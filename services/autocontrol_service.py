"""Auto-control service with Away Mode for sophisticated SOC-based power management."""
import time
import threading
from datetime import datetime, date, timezone
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo

# Try to import astral for sunset calculations
try:
    from astral import LocationInfo
    from astral.sun import sun
    ASTRAL_AVAILABLE = True
except ImportError:
    ASTRAL_AVAILABLE = False
    print("[AutoControl] WARNING: astral library not available, using fixed sunset time")

from utils.state_manager import StateManager


class AutoControlService:
    """
    Automatic power control with Away Mode.

    Away Mode implements sophisticated control logic:
    1. Emergency shutdown at SOC < 30%
    2. Full power (100%) when SOC > 99% AND PV > 3600W
    3. High SOC conservative (90%) when SOC > 90% AND PV < 3600W
    4. After sunset startup when time > sunset AND SOC > 40% AND miner off
    5. Normal discharge tiers (SOC% → power%)
    """

    def __init__(self,
                 miner_service,
                 battery_service,
                 state_manager: StateManager,
                 base_watts: int,
                 min_interval_sec: int,
                 mode: str,
                 away_config: dict,
                 location_config: dict,
                 weather_service=None,
                 weather_gate=None,
                 **kwargs):
        """
        Initialize auto-control service.

        Args:
            miner_service: Miner service instance
            battery_service: Battery service instance
            state_manager: State manager for persistence
            base_watts: Maximum miner power
            min_interval_sec: Minimum time between power adjustments
            mode: Control mode ("away" or "present")
            away_config: Away mode configuration dict
            location_config: Location configuration for sunset
            weather_service: Optional WeatherService instance for forecast data
            weather_gate: Optional WeatherGate instance owning the pre-sunrise
                evaluation. When None, the weather gate is bypassed entirely.
        """
        self.miner = miner_service
        self.battery = battery_service
        self.state = state_manager
        self.base_watts = base_watts
        self.min_interval_sec = min_interval_sec
        self.mode = mode
        self.weather = weather_service
        self.weather_gate = weather_gate

        # Away mode configuration — emergency_soc may be overridden by persisted runtime value
        config_emergency_soc = away_config.get("emergency_soc", 30)
        saved = self.state.load()
        persisted = saved.get("emergency_soc")
        self.emergency_soc = int(persisted) if persisted is not None else config_emergency_soc
        self.max_pv_power = away_config.get("max_pv_power", 3600)
        self.after_sunset_min_soc = away_config.get("after_sunset_min_soc", 40)

        # Location for sunset calculation
        self.latitude = location_config.get("latitude", 40.0)
        self.longitude = location_config.get("longitude", -74.0)
        self.timezone_str = location_config.get("timezone", "America/New_York")

        # Sunset calculation setup
        if ASTRAL_AVAILABLE:
            self.location = LocationInfo("Home", "Region", self.timezone_str, self.latitude, self.longitude)
            print(f"[AutoControl] Using astral for sunset at lat={self.latitude}, lon={self.longitude}")
        else:
            self.location = None
            print(f"[AutoControl] Install 'astral' for accurate sunset times: pip install astral")

        # Fallback sunset (used if astral not available)
        self.fallback_sunset_hour = kwargs.get("sunset_hour", 19)
        self.fallback_sunset_minute = kwargs.get("sunset_minute", 0)

        # Load state
        saved_state = self.state.load()
        self.enabled = saved_state.get("autocontrol", False)

        # Control state
        self.last_set_w: Optional[int] = None
        self.last_set_ts = 0.0
        self.latched_floor_w: Optional[int] = None
        self.target_w: Optional[int] = None
        self.target_pct: Optional[int] = None
        self.current_state_description = "Initializing"

        # Stop-reason tracking (surfaced to the dashboard)
        # Values: "emergency_soc", "emergency_unverified", "manual_off", "ramping", "normal"
        self.stop_reason: str = "normal"
        self.resume_at_soc: Optional[int] = None

        # Emergency latch state — persisted so it survives service restart
        self.emergency_active: bool = bool(saved_state.get("emergency_active", False))
        self.emergency_verified_off: bool = False
        self.emergency_attempts_this_latch: int = 0

        # Cache today's sunset
        self._cached_sunset_date: Optional[date] = None
        self._cached_sunset_time: Optional[datetime] = None

        # Grace period after any privileged command. Prevents autocontrol from
        # re-issuing power_on during the ~60-90s firmware chip recalibration that
        # follows adjust_power_limit, during which Power Limit and MHS 5s both
        # read 0 (making is_off appear True even though the miner is reconfiguring).
        _POST_CMD_GRACE_SEC = 300
        self._post_cmd_grace_sec: int = _POST_CMD_GRACE_SEC

        # Last power tier we successfully enqueued. Power commands are only sent when
        # this changes — the firmware resets on every adjust_power_limit regardless of
        # whether the value changed, so resending the same tier causes an unnecessary
        # recalibration cycle. Cleared to None on power_on (so a fresh start re-syncs)
        # and on enable() (in case tier changed while autocontrol was disabled).
        self._last_sent_pct: Optional[int] = None

        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Start auto-control thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._control_loop, daemon=True)
        self._thread.start()
        print(f"[AutoControl] Started (enabled={self.enabled}, mode={self.mode})")

    def stop(self):
        """Stop auto-control thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        print("[AutoControl] Stopped")

    def enable(self):
        """Enable auto-control."""
        self.enabled = True
        self.state.save(autocontrol=True)
        self.last_set_ts = 0.0  # Allow immediate action
        self._last_sent_pct = None  # Re-sync tier on first tick
        print(f"[AutoControl] Enabled ({self.mode} mode)")

    def disable(self):
        """Disable auto-control."""
        self.enabled = False
        self.state.save(autocontrol=False)
        print("[AutoControl] Disabled")

    def set_mode(self, mode: str) -> bool:
        """
        Set control mode.

        Args:
            mode: "away" or "present"

        Returns:
            True if successful, False if mode not implemented
        """
        if mode not in ["away", "present"]:
            return False

        if mode == "present":
            print("[AutoControl] Present mode not yet implemented")
            return False

        self.mode = mode
        self.last_set_ts = 0.0  # Allow immediate re-evaluation
        print(f"[AutoControl] Mode changed to: {mode}")
        return True

    def get_emergency_soc(self) -> int:
        """Return current emergency SOC threshold (%)."""
        return self.emergency_soc

    def set_emergency_soc(self, percent: int) -> None:
        """Update the emergency SOC threshold and persist it to wm_state.json."""
        if not (5 <= percent <= 95):
            raise ValueError(f"emergency_soc must be between 5 and 95, got {percent}")
        self.emergency_soc = percent
        self.state.save(emergency_soc=percent)
        print(f"[AutoControl] emergency_soc updated to {percent}%")

    def _get_sunset_time(self) -> Optional[datetime]:
        """
        Get today's sunset time.
        Uses cached value if already calculated for today.

        Returns:
            Sunset datetime in local timezone, or None if unavailable
        """
        today = date.today()

        # Return cached value if still valid
        if self._cached_sunset_date == today and self._cached_sunset_time:
            return self._cached_sunset_time

        # Calculate new sunset
        try:
            if ASTRAL_AVAILABLE and self.location:
                # Use astral for accurate calculation
                tz = ZoneInfo(self.timezone_str)
                s = sun(self.location.observer, date=today, tzinfo=tz)
                sunset_time = s['sunset']

                # Cache the result
                self._cached_sunset_date = today
                self._cached_sunset_time = sunset_time

                print(f"[AutoControl] Today's sunset: {sunset_time.strftime('%H:%M:%S')}")
                return sunset_time
            else:
                # Use fallback fixed time
                tz = ZoneInfo(self.timezone_str)
                now = datetime.now(tz)
                sunset_time = now.replace(
                    hour=self.fallback_sunset_hour,
                    minute=self.fallback_sunset_minute,
                    second=0,
                    microsecond=0
                )

                # Cache the result
                self._cached_sunset_date = today
                self._cached_sunset_time = sunset_time

                print(f"[AutoControl] Using fallback sunset: {sunset_time.strftime('%H:%M:%S')}")
                return sunset_time

        except Exception as e:
            print(f"[AutoControl] Error calculating sunset: {e}")
            return None

    def _is_past_sunset(self) -> bool:
        """Check if current time is past today's sunset."""
        sunset = self._get_sunset_time()
        if not sunset:
            return False

        try:
            tz = ZoneInfo(self.timezone_str)
            now = datetime.now(tz)
            return now > sunset
        except Exception:
            return False

    def _control_loop(self):
        """Main control loop - evaluates conditions every min_interval_sec."""

        while self._running:
            try:
                if not self.enabled:
                    time.sleep(self.min_interval_sec)
                    continue

                # Only Away Mode is implemented
                if self.mode == "away":
                    self._away_mode_control()
                else:
                    print(f"[AutoControl] Mode '{self.mode}' not implemented")

            except Exception as e:
                print(f"[AutoControl] Error in control loop: {e}")
                import traceback
                traceback.print_exc()

            time.sleep(self.min_interval_sec)

    def _away_mode_control(self):
        """
        Away Mode control logic with priority conditions.

        Priority order:
        1. Emergency shutdown (SOC < 30%)
        2. Full power (SOC > 99% AND PV > 3600W)
        3. High SOC conservative (SOC > 90% AND PV < 3600W)
        4. After sunset startup (after sunset AND SOC > 40% AND miner off)
        5. Normal discharge tiers (SOC → power%)
        """

        # SAFETY GATE: If battery telemetry is stale, stop the miner and do nothing else.
        if not self.battery.is_fresh():
            age = self.battery.get_battery_age_seconds()
            age_str = f"{age:.0f}s" if age is not None else "unknown"
            print(f"[AutoControl] WARNING: Battery telemetry stale ({age_str}) — stopping miner for safety")
            self.current_state_description = f"Battery telemetry stale ({age_str}s) — miner stopped for safety."
            self.stop_reason = "battery_stale"
            self.resume_at_soc = None
            # Only enqueue power_off if the miner is not already confirmed off.
            if not self.miner.is_off:
                self.miner.power_off()
            return

        # Get current state
        battery_snap = self.battery.get_status()
        soc = battery_snap.get("soc_percent")
        pv_power = battery_snap.get("pv_power_w", 0) or 0

        is_miner_off = self.miner.is_off

        # WEATHER GATE (additive): pre-sunrise check decides whether the day's
        # forecast solar harvest can refill the battery. If not, autocontrol is
        # held off for the day with stop_reason="weather_disabled". Recovery
        # may re-enable later if SOC climbs back in time. The battery_stale
        # gate above always wins.
        if self._evaluate_weather_gate(soc):
            self._apply_weather_disabled(soc)
            return

        if not isinstance(soc, (int, float)):
            print("[AutoControl] No valid SOC data")
            return

        # EMERGENCY LATCH: if already latched in emergency, only run the stop loop.
        # All normal priority branches are short-circuited until SOC >= 90% and
        # the miner has been confirmed off at least once this latch period.
        if self.emergency_active:
            self._run_emergency_latch_tick(soc)
            return

        # Get sunset info
        sunset_time = self._get_sunset_time()
        is_past_sunset = self._is_past_sunset()

        # Log decision context
        print(f"\n{'='*60}")
        print(f"[AutoControl] === AWAY MODE DECISION ===")
        if sunset_time:
            print(f"[AutoControl] Sunset today: {sunset_time.strftime('%H:%M:%S')}")
        tz = ZoneInfo(self.timezone_str)
        now = datetime.now(tz)
        print(f"[AutoControl] Current time: {now.strftime('%H:%M:%S')} ({'after' if is_past_sunset else 'before'} sunset)")
        print(f"[AutoControl] Current state: SOC={soc:.1f}%, PV={pv_power:.0f}W, Miner={'OFF' if is_miner_off else 'ON'}")

        # PRIORITY 1: Emergency Shutdown (trips the emergency latch)
        if soc < self.emergency_soc:
            self._trip_emergency(soc)
            return

        # PRIORITY 2: Full Power (SOC > 99% AND PV > max_pv_power)
        if soc > 99 and pv_power > self.max_pv_power:
            self.target_pct = 100
            self.target_w = self.base_watts
            self.current_state_description = "Full power - battery full with excess solar"
            self.latched_floor_w = None  # Reset latch
            self.stop_reason = "normal"
            self.resume_at_soc = None

            print(f"[AutoControl] Condition met: Full power opportunity (SOC > 99% AND PV > {self.max_pv_power}W)")
            print(f"[AutoControl] Target power: 100% ({self.base_watts}W)")

            # Ensure miner is on. If we have to power it on, do ONLY that this tick:
            # firmware locks the privileged session for ~180s per get_token, so
            # enqueueing power_on + set_power_pct on the same tick self-contends.
            # The next tick will re-evaluate; once is_off=False, we fall through to
            # the rate-limited power adjustment below.
            if is_miner_off:
                if self._in_post_cmd_grace_period():
                    elapsed = int(time.time() - self.last_set_ts)
                    print(f"[AutoControl] Post-command grace period ({elapsed}s of {self._post_cmd_grace_sec}s) — miner may be recalibrating, skipping power_on")
                    print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
                    print(f"{'='*60}\n")
                    return
                print(f"[AutoControl] Powering on miner (deferring power adjust to next tick)...")
                self.miner.power_on()
                self.last_set_ts = time.time()
                self.state.save(miner_power_state="running")
                time.sleep(2)  # Brief delay for power-on
                print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
                print(f"{'='*60}\n")
                return

            self._set_power_with_rate_limit(100)
            print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
            print(f"{'='*60}\n")
            return

        # PRIORITY 3: High SOC Conservative (SOC > 90% AND PV < max_pv_power)
        if soc > 90 and pv_power < self.max_pv_power:
            self.target_pct = 90
            self.target_w = int(self.base_watts * 0.9)
            self.current_state_description = "High SOC conservative - limited solar"
            self.stop_reason = "normal"
            self.resume_at_soc = None

            print(f"[AutoControl] Condition met: High SOC conservative (SOC > 90% AND PV < {self.max_pv_power}W)")
            print(f"[AutoControl] Target power: 90% ({self.target_w}W)")

            # Ensure miner is on. If we have to power it on, do ONLY that this tick
            # (see PRIORITY 2 comment for rationale).
            if is_miner_off:
                if self._in_post_cmd_grace_period():
                    elapsed = int(time.time() - self.last_set_ts)
                    print(f"[AutoControl] Post-command grace period ({elapsed}s of {self._post_cmd_grace_sec}s) — miner may be recalibrating, skipping power_on")
                    print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
                    print(f"{'='*60}\n")
                    return
                print(f"[AutoControl] Powering on miner (deferring power adjust to next tick)...")
                self.miner.power_on()
                self.last_set_ts = time.time()
                self.state.save(miner_power_state="running")
                time.sleep(2)
                print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
                print(f"{'='*60}\n")
                return

            self._set_power_with_rate_limit(90)
            print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
            print(f"{'='*60}\n")
            return

        # PRIORITY 4: After Sunset Startup
        if is_past_sunset and soc > self.after_sunset_min_soc and is_miner_off:
            # Calculate appropriate power tier based on current SOC
            tier_pct = self._calculate_soc_tier(soc)
            self.target_pct = tier_pct
            self.target_w = int(self.base_watts * (tier_pct / 100.0))
            self.current_state_description = f"After sunset startup at {tier_pct}%"
            self.stop_reason = "normal"
            self.resume_at_soc = None

            print(f"[AutoControl] Condition met: After sunset startup")
            print(f"[AutoControl] Time is after sunset AND SOC > {self.after_sunset_min_soc}% AND miner is off")
            print(f"[AutoControl] Target power: {tier_pct}% ({self.target_w}W) based on SOC tier")

            # One privileged op per tick when miner is off — see PRIORITY 2 comment.
            if self._in_post_cmd_grace_period():
                elapsed = int(time.time() - self.last_set_ts)
                print(f"[AutoControl] Post-command grace period ({elapsed}s of {self._post_cmd_grace_sec}s) — miner may be recalibrating, skipping power_on")
                print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
                print(f"{'='*60}\n")
                return
            print(f"[AutoControl] Powering on miner (deferring power adjust to next tick)...")
            self._last_sent_pct = None  # Force tier sync after power-on
            self.miner.power_on()
            self.last_set_ts = time.time()
            self.state.save(miner_power_state="running")
            time.sleep(2)  # Brief delay
            print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
            print(f"{'='*60}\n")
            return

        # PRIORITY 5: Normal Discharge Tiers
        tier_pct = self._calculate_soc_tier(soc)
        self.target_pct = tier_pct
        self.target_w = int(self.base_watts * (tier_pct / 100.0))
        self.current_state_description = f"Normal discharge at {tier_pct}%"
        self.stop_reason = "normal"
        self.resume_at_soc = None

        print(f"[AutoControl] Condition met: Normal discharge")
        print(f"[AutoControl] SOC tier: {tier_pct}% for SOC={soc:.1f}%")
        print(f"[AutoControl] Target power: {tier_pct}% ({self.target_w}W)")

        # Ensure miner is on if target > 0. If we have to power it on, do ONLY that
        # this tick — see PRIORITY 2 comment for rationale (firmware lock contention).
        if tier_pct > 0 and is_miner_off:
            if self._in_post_cmd_grace_period():
                elapsed = int(time.time() - self.last_set_ts)
                print(f"[AutoControl] Post-command grace period ({elapsed}s of {self._post_cmd_grace_sec}s) — miner may be recalibrating, skipping power_on")
                print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
                print(f"{'='*60}\n")
                return
            print(f"[AutoControl] Powering on miner (deferring power adjust to next tick)...")
            self._last_sent_pct = None  # Force tier sync after power-on
            self.miner.power_on()
            self.last_set_ts = time.time()
            self.state.save(miner_power_state="running")
            time.sleep(2)
            print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
            print(f"{'='*60}\n")
            return

        self._set_power_with_rate_limit(tier_pct)
        print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
        print(f"{'='*60}\n")

    # ---- Emergency helpers ----

    def _trip_emergency(self, soc: float):
        """Trip emergency latch on first SOC-below-threshold detection."""
        self.target_pct = 0
        self.target_w = 0
        self.current_state_description = f"Emergency shutdown - SOC below {self.emergency_soc}%"
        self.stop_reason = "emergency_soc"
        self.resume_at_soc = self.emergency_soc
        self.last_set_ts = time.time()
        self._last_sent_pct = None  # Force tier re-sync when miner recovers from emergency

        print(f"[AutoControl] Condition met: Emergency shutdown (SOC={soc:.1f}% < {self.emergency_soc}%)")

        if not self.emergency_active:
            self.emergency_active = True
            self.emergency_verified_off = False
            self.emergency_attempts_this_latch = 0
            self.state.save(emergency_active=True)
            print(f"[AutoControl] Emergency latch SET")

        print(f"[AutoControl] Draining queue and sending emergency power off...")
        self.miner.controller.drain_queue()
        verified = self._emergency_stop_with_verify()

        if verified:
            self.state.save(miner_power_state="stopped", target_power_pct=0)
        else:
            self.stop_reason = "emergency_unverified"

        print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
        print(f"{'='*60}\n")

    def _run_emergency_latch_tick(self, soc: float):
        """One tick while emergency latch is active: re-check miner, re-stop if needed."""
        print(f"[AutoControl] Emergency latch active — checking miner state (SOC={soc:.1f}%)")

        if not self._check_verified_off():
            print(f"[AutoControl] Miner not confirmed off — draining queue and re-stopping...")
            self.miner.controller.drain_queue()
            verified = self._emergency_stop_with_verify()
            if not verified:
                self.stop_reason = "emergency_unverified"
        else:
            self.stop_reason = "emergency_soc"

        # Clear latch only when: verified-off succeeded this latch period AND SOC >= 90%
        if self.emergency_verified_off and soc >= 90:
            self.emergency_active = False
            self.state.save(emergency_active=False)
            print(f"[AutoControl] Emergency latch CLEARED (SOC={soc:.1f}% >= 90% and verified off)")

    # ---- Weather gate helpers ----

    def _evaluate_weather_gate(self, soc: Optional[float]) -> bool:
        """Run one weather-gate tick. Return True iff caller should hold the
        miner off with stop_reason="weather_disabled" this tick.
        """
        if self.weather_gate is None or self.weather is None:
            return False
        try:
            forecast = self.weather.get_today_forecast()
            outcome = self.weather_gate.evaluate(
                soc_pct=soc,
                battery_fresh=self.battery.is_fresh(),
                forecast=forecast,
            )
        except Exception as exc:
            print(f"[AutoControl] Weather gate evaluation error: {exc}")
            # On error, do not force-disable — defer to existing logic.
            return False
        # The single source of truth is gate.disabled (set by evaluate()).
        # The outcome string is informational; gate.disabled drives behavior.
        return bool(self.weather_gate.disabled)

    def force_evaluate_weather_gate(self) -> dict:
        """Force an immediate gate evaluation outside the normal window guard.

        Used by /api/weather/evaluate_now. Returns the gate's snapshot after
        running. If no weather_gate is wired, returns an empty dict with a note.
        """
        if self.weather_gate is None or self.weather is None:
            return {"error": "weather_gate_not_configured"}

        # Bypass the once-per-day guard so the operator can re-run the
        # decision on demand.
        self.weather_gate.evaluated_date = None
        soc = None
        try:
            snap = self.battery.get_status()
            soc = snap.get("soc_percent")
        except Exception:
            pass
        forecast = self.weather.get_today_forecast()
        outcome = self.weather_gate.evaluate(
            soc_pct=soc,
            battery_fresh=self.battery.is_fresh(),
            forecast=forecast,
        )
        state = self.weather_gate.get_state()
        state["outcome"] = outcome
        return state

    def _apply_weather_disabled(self, soc: Optional[float]):
        """Force-stop the miner with stop_reason='weather_disabled'."""
        gate_state = self.weather_gate.get_state() if self.weather_gate else {}
        reason_detail = gate_state.get("reason") or "weather_disabled"
        expected = gate_state.get("expected_kwh")
        deficit = gate_state.get("deficit_kwh")
        self.target_pct = 0
        self.target_w = 0
        self.current_state_description = (
            f"Weather gate disabled autocontrol ({reason_detail})."
        )
        self.stop_reason = "weather_disabled"
        self.resume_at_soc = None
        soc_str = f"{soc:.1f}%" if isinstance(soc, (int, float)) else "?"
        exp_str = f"{expected:.2f}" if isinstance(expected, (int, float)) else "?"
        def_str = f"{deficit:.2f}" if isinstance(deficit, (int, float)) else "?"
        print(
            f"[AutoControl] Weather gate active ({reason_detail}) — "
            f"SOC={soc_str}, expected={exp_str}kWh, deficit={def_str}kWh"
        )
        if not self.miner.is_off:
            self.miner.power_off()

    def _emergency_stop_with_verify(self) -> bool:
        """Send emergency_power_off and verify via re-poll. Up to 5 attempts.
        Returns True if verified-off condition is confirmed, False otherwise.
        Logs success only after verification is confirmed.
        """
        for attempt in range(5):
            self.emergency_attempts_this_latch += 1
            accepted = self.miner.emergency_power_off()
            print(f"[AutoControl] Emergency stop attempt {attempt + 1}/5 — command accepted: {accepted}")

            # Wait up to 10s for miner to respond to the power-limit command
            for _ in range(10):
                time.sleep(1)
                if self._check_verified_off():
                    print(f"[AutoControl] ✓ Miner powered off (verified after attempt {attempt + 1})")
                    self.emergency_verified_off = True
                    return True

            if attempt < 4:
                print(f"[AutoControl] Not yet off — waiting 5s before retry...")
                time.sleep(5)

        print(f"[AutoControl] ✗✗✗ EMERGENCY STOP UNVERIFIED — MINER STILL RUNNING ✗✗✗")
        return False

    def _check_verified_off(self) -> bool:
        """Check verified-off condition: MHS 5s==0, is_off==true.
        Uses direct nc_api.summary() and miner_status_cmd() (both non-privileged, no token lock).
        NOTE: Power Limit is always 0 on this firmware regardless of miner state;
        do not use it as a stop indicator.
        """
        try:
            nc_api = self.miner.api
            summ = nc_api.summary()
            item = ((summ or {}).get("SUMMARY") or [{}])[0]
            mhs_5s = float(item.get("MHS 5s") or 0)

            if mhs_5s != 0.0:
                print(f"[AutoControl] Verified-off check FAILED: MHS 5s={mhs_5s} (still hashing)")
                return False

            # MHS 5s is 0 — confirm is_off via status cmd if available
            is_off = True
            try:
                status_reply = nc_api.miner_status_cmd()
                mining_list = (status_reply or {}).get("MINING") or []
                mining_info = mining_list[0] if mining_list and isinstance(mining_list[0], dict) else {}
                mineroff_val = str(mining_info.get("mineroff", "")).lower()
                if mineroff_val == "false":
                    is_off = False
                # If mineroff absent from response, trust MHS 5s == 0 above
            except Exception:
                pass

            print(f"[AutoControl] Verified-off check: MHS 5s={mhs_5s}, is_off={is_off}")
            return is_off
        except Exception as e:
            print(f"[AutoControl] _check_verified_off error: {e}")
            return False

    def _calculate_soc_tier(self, soc: float) -> int:
        """
        Calculate power tier based on SOC.

        SOC ranges map to power percentages:
        80-100% → 80%
        70-79% → 70%
        60-69% → 60%
        50-59% → 50%
        40-49% → 40%
        30-39% → 30%
        < 30% → 0%

        Args:
            soc: Battery state of charge percentage

        Returns:
            Power percentage (0-100)
        """
        if soc >= 80:
            return 80
        elif soc >= 70:
            return 70
        elif soc >= 60:
            return 60
        elif soc >= 50:
            return 50
        elif soc >= 40:
            return 40
        elif soc >= 30:
            return 30
        else:
            return 0

    def _in_post_cmd_grace_period(self) -> bool:
        """True within _post_cmd_grace_sec of the last queued privileged command.
        During firmware chip recalibration after adjust_power_limit, the miner's
        Power Limit and MHS 5s both read 0, making is_off appear True. This guard
        prevents autocontrol from issuing a redundant power_on during that window.
        """
        return self.last_set_ts > 0 and (time.time() - self.last_set_ts) < self._post_cmd_grace_sec

    def _set_power_with_rate_limit(self, target_pct: int):
        """
        Set miner power, but only when the SOC tier has changed.

        Args:
            target_pct: Target power percentage
        """
        # Skip if tier hasn't changed — adjust_power_limit always causes a full
        # firmware recalibration, even when the value is identical to what's running.
        if self._last_sent_pct is not None and target_pct == self._last_sent_pct:
            print(f"[AutoControl] Tier unchanged at {target_pct}% — no power command needed")
            return

        wall_now = time.time()

        # Check rate limit
        if (wall_now - self.last_set_ts) < self.min_interval_sec:
            print(f"[AutoControl] Rate limited - last adjustment {int(wall_now - self.last_set_ts)}s ago")
            return

        print(f"[AutoControl] Sending power command: {target_pct}% (was {self._last_sent_pct}%)...")

        def _on_verified():
            print(f"[AutoControl] ✓ Power verified at {target_pct}%")

        try:
            self.miner.set_power_pct(target_pct, on_verified=_on_verified)
            self.last_set_w = int(self.base_watts * (target_pct / 100.0))
            self.last_set_ts = wall_now
            self._last_sent_pct = target_pct
            self.state.save(target_power_pct=target_pct)

            print(f"[AutoControl] Power command queued ({target_pct}%)")
        except Exception as e:
            print(f"[AutoControl] ✗ Power adjustment failed: {e}")

    def force_tick(self):
        """Force a single _away_mode_control evaluation outside the normal loop.
        Used by test endpoints only. Not thread-safe with the running control loop,
        but the loop sleeps min_interval_sec between ticks, making collision unlikely.
        """
        if self.mode == "away":
            self._away_mode_control()

    def get_state(self) -> Dict[str, Any]:
        """Get current auto-control state for debugging and display."""
        sunset_time = self._get_sunset_time()

        # Derive the effective stop_reason for the dashboard.
        # When autocontrol is disabled, the miner state is purely manual.
        miner_latest = self.miner.get_status()
        upfreq_complete = miner_latest.get("upfreq_complete", 0) if miner_latest else 0
        if upfreq_complete is None:
            upfreq_complete = 0

        if not self.enabled:
            effective_stop_reason = "manual_off" if self.miner.is_off else "normal"
            effective_resume_at_soc = None
        elif self.stop_reason == "emergency_unverified":
            effective_stop_reason = "emergency_unverified"
            effective_resume_at_soc = self.resume_at_soc
        elif self.stop_reason == "emergency_soc":
            effective_stop_reason = "emergency_soc"
            effective_resume_at_soc = self.resume_at_soc
        elif not self.miner.is_off and upfreq_complete == 0:
            # Miner is on but still ramping up
            effective_stop_reason = "ramping"
            effective_resume_at_soc = None
        elif not self.miner.is_off and upfreq_complete == 1:
            effective_stop_reason = "normal"
            effective_resume_at_soc = None
        else:
            effective_stop_reason = self.stop_reason
            effective_resume_at_soc = self.resume_at_soc

        # If the weather gate is holding the miner off, override the derived
        # stop_reason so the dashboard banner can display the new state.
        if self.weather_gate is not None and self.weather_gate.disabled and self.enabled:
            effective_stop_reason = "weather_disabled"
            effective_resume_at_soc = None

        weather_gate_state = (
            self.weather_gate.get_state() if self.weather_gate is not None else None
        )

        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "target_w": self.target_w,
            "target_pct": self.target_pct,
            "last_set_w": self.last_set_w,
            "latched_floor_w": self.latched_floor_w,
            "min_interval_sec": self.min_interval_sec,
            "current_state_description": self.current_state_description,
            "sunset_time": sunset_time.strftime('%H:%M:%S') if sunset_time else "Unknown",
            "is_past_sunset": self._is_past_sunset(),
            "emergency_soc": self.emergency_soc,
            "battery_fresh": self.battery.is_fresh(),
            "battery_age_seconds": self.battery.get_battery_age_seconds(),
            "stop_reason": effective_stop_reason,
            "resume_at_soc": effective_resume_at_soc,
            "emergency_active": self.emergency_active,
            "emergency_verified_off": self.emergency_verified_off,
            "emergency_attempts_this_latch": self.emergency_attempts_this_latch,
            "weather_gate": weather_gate_state,
        }
