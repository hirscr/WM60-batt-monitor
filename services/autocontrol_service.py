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
        """
        self.miner = miner_service
        self.battery = battery_service
        self.state = state_manager
        self.base_watts = base_watts
        self.min_interval_sec = min_interval_sec
        self.mode = mode

        # Away mode configuration
        self.emergency_soc = away_config.get("emergency_soc", 30)
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

        # Cache today's sunset
        self._cached_sunset_date: Optional[date] = None
        self._cached_sunset_time: Optional[datetime] = None

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

        # Get current state
        battery_snap = self.battery.get_status()
        soc = battery_snap.get("soc_percent")
        pv_power = battery_snap.get("pv_power_w", 0) or 0

        miner_status = self.miner.get_status()
        miner_power = miner_status.get("Power", 0) or 0
        is_miner_off = (miner_power == 0) or (miner_power < 100)

        if not isinstance(soc, (int, float)):
            print("[AutoControl] No valid SOC data")
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
        print(f"[AutoControl] Current state: SOC={soc:.1f}%, PV={pv_power:.0f}W, Miner={'OFF' if is_miner_off else f'{miner_power}W'}")

        # PRIORITY 1: Emergency Shutdown
        if soc < self.emergency_soc:
            self.target_pct = 0
            self.target_w = 0
            self.current_state_description = f"Emergency shutdown - SOC below {self.emergency_soc}%"

            print(f"[AutoControl] Condition met: Emergency shutdown (SOC < {self.emergency_soc}%)")
            print(f"[AutoControl] Target power: 0% (0W)")
            print(f"[AutoControl] Sending power off command...")

            self.miner.power_off()
            self.state.save(miner_power_state="stopped", target_power_pct=0)
            self.last_set_ts = time.time()

            print(f"[AutoControl] ✓ Miner powered off")
            print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
            print(f"{'='*60}\n")
            return

        # PRIORITY 2: Full Power (SOC > 99% AND PV > max_pv_power)
        if soc > 99 and pv_power > self.max_pv_power:
            self.target_pct = 100
            self.target_w = self.base_watts
            self.current_state_description = "Full power - battery full with excess solar"
            self.latched_floor_w = None  # Reset latch

            print(f"[AutoControl] Condition met: Full power opportunity (SOC > 99% AND PV > {self.max_pv_power}W)")
            print(f"[AutoControl] Target power: 100% ({self.base_watts}W)")

            # Ensure miner is on
            if is_miner_off:
                print(f"[AutoControl] Powering on miner...")
                self.miner.power_on()
                self.state.save(miner_power_state="running")
                time.sleep(2)  # Brief delay for power-on

            self._set_power_with_rate_limit(100)
            print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
            print(f"{'='*60}\n")
            return

        # PRIORITY 3: High SOC Conservative (SOC > 90% AND PV < max_pv_power)
        if soc > 90 and pv_power < self.max_pv_power:
            self.target_pct = 90
            self.target_w = int(self.base_watts * 0.9)
            self.current_state_description = "High SOC conservative - limited solar"

            print(f"[AutoControl] Condition met: High SOC conservative (SOC > 90% AND PV < {self.max_pv_power}W)")
            print(f"[AutoControl] Target power: 90% ({self.target_w}W)")

            # Ensure miner is on
            if is_miner_off:
                print(f"[AutoControl] Powering on miner...")
                self.miner.power_on()
                self.state.save(miner_power_state="running")
                time.sleep(2)

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

            print(f"[AutoControl] Condition met: After sunset startup")
            print(f"[AutoControl] Time is after sunset AND SOC > {self.after_sunset_min_soc}% AND miner is off")
            print(f"[AutoControl] Target power: {tier_pct}% ({self.target_w}W) based on SOC tier")
            print(f"[AutoControl] Powering on miner...")

            self.miner.power_on()
            self.state.save(miner_power_state="running")
            time.sleep(2)  # Brief delay

            self._set_power_with_rate_limit(tier_pct)
            print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
            print(f"{'='*60}\n")
            return

        # PRIORITY 5: Normal Discharge Tiers
        tier_pct = self._calculate_soc_tier(soc)
        self.target_pct = tier_pct
        self.target_w = int(self.base_watts * (tier_pct / 100.0))
        self.current_state_description = f"Normal discharge at {tier_pct}%"

        print(f"[AutoControl] Condition met: Normal discharge")
        print(f"[AutoControl] SOC tier: {tier_pct}% for SOC={soc:.1f}%")
        print(f"[AutoControl] Target power: {tier_pct}% ({self.target_w}W)")

        # Ensure miner is on if target > 0
        if tier_pct > 0 and is_miner_off:
            print(f"[AutoControl] Powering on miner...")
            self.miner.power_on()
            self.state.save(miner_power_state="running")
            time.sleep(2)

        self._set_power_with_rate_limit(tier_pct)
        print(f"[AutoControl] Next evaluation in {self.min_interval_sec}s")
        print(f"{'='*60}\n")

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

    def _set_power_with_rate_limit(self, target_pct: int):
        """
        Set miner power with rate limiting.

        Args:
            target_pct: Target power percentage
        """
        wall_now = time.time()

        # Check rate limit
        if (wall_now - self.last_set_ts) < self.min_interval_sec:
            print(f"[AutoControl] Rate limited - last adjustment {int(wall_now - self.last_set_ts)}s ago")
            return

        print(f"[AutoControl] Sending power command: {target_pct}%...")

        try:
            self.miner.set_power_pct(target_pct)
            self.last_set_w = int(self.base_watts * (target_pct / 100.0))
            self.last_set_ts = wall_now
            self.state.save(target_power_pct=target_pct)

            print(f"[AutoControl] ✓ Power adjusted successfully")
        except Exception as e:
            print(f"[AutoControl] ✗ Power adjustment failed: {e}")

    def get_state(self) -> Dict[str, Any]:
        """Get current auto-control state for debugging and display."""
        sunset_time = self._get_sunset_time()

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
        }
