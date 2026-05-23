"""Pre-sunrise weather-forecast autocontrol gate.

Decides once per local day whether the forecast solar harvest can refill the
battery; if not, autocontrol is force-disabled for the day and the miner is
held off with stop_reason="weather_disabled". A recovery path can re-enable
autocontrol later in the day if SOC climbs back fast enough.

This module owns all gate state and decision rules. AutoControlService just
calls evaluate() on each tick and reads the gate's state.

Pure decision rule (no I/O) lives in WeatherGate.decide_after_evaluation —
that's what the unit tests exercise.

Logging follows the project's two-layer gate pattern:
    if __debug__:
        log("WEATHER_GATE", "...")
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from utils.log_config import log
from utils.solar_model import expected_energy_kwh, max_daily_energy_kwh


# ----------------------------------------------------------------------
# Outcome values returned by evaluate()
# ----------------------------------------------------------------------

OUTCOME_DISABLED_FOR_DAY = "disabled_for_day"  # gate decided "insufficient"
OUTCOME_KEPT_ENABLED = "kept_enabled"  # gate decided "sufficient"
OUTCOME_RECOVERED = "recovered"  # SOC recovered, lifted the gate
OUTCOME_RECOVERY_WINDOW_TOO_SHORT = "recovery_window_too_short"
OUTCOME_MIDNIGHT_RESET = "midnight_reset"
OUTCOME_OUTSIDE_WINDOW = "outside_window"  # nothing to do this tick
OUTCOME_ALREADY_EVALUATED = "already_evaluated"
OUTCOME_BATTERY_STALE = "battery_stale"  # skip, don't advance date
OUTCOME_FORECAST_STALE = "forecast_stale"  # skip, don't advance date
OUTCOME_GATE_DISABLED = "gate_disabled"  # master switch off

# Decision-source labels recorded on the decision dict and in persisted state.
# "eg4_predict" wins when the EG4 portal prediction is fresh; otherwise we
# fall back to the sinusoidal seasonal model attenuated by cloud cover.
DECISION_SOURCE_EG4 = "eg4_predict"
DECISION_SOURCE_FALLBACK = "solar_model_fallback"


@dataclass
class WeatherGateConfigSnapshot:
    """Immutable per-tick view of the editable gate parameters."""
    enabled: bool
    battery_total_kwh: float
    summer_max_kwh: float
    winter_max_kwh: float
    pre_sunrise_window_minutes: int
    recovery_soc_threshold_pct: int
    recovery_min_hours_before_sunset: float
    # Conservative multiplier applied to EG4's portal-provided PV prediction.
    # Default 0.8 — user-tunable from the dashboard so the multiplier can be
    # calibrated against the daily prediction-vs-actual log.
    eg4_predict_multiplier: float = 0.8


class WeatherGate:
    """Owns the weather-gate runtime state and the once-per-day decision flow.

    State is persisted via the provided state_manager so it survives restarts.
    """

    # Persisted state keys (kept namespaced to avoid colliding with autocontrol's keys)
    _KEY_DISABLED = "weather_gate_disabled"
    _KEY_REASON = "weather_gate_reason"
    _KEY_EXPECTED_KWH = "weather_gate_expected_kwh"
    _KEY_DEFICIT_KWH = "weather_gate_deficit_kwh"
    _KEY_MAX_FOR_DAY_KWH = "weather_gate_max_for_day_kwh"
    _KEY_EVALUATED_DATE = "weather_gate_evaluated_date"  # ISO string or None
    _KEY_EVALUATED_AT = "weather_gate_evaluated_at"  # ISO timestamp or None
    # New decision-context keys — let the dashboard show "which source"
    # produced the day's expected_kwh and what raw EG4 number fed it.
    _KEY_EG4_TODAY_KWH_RAW = "weather_gate_eg4_today_kwh_raw"
    _KEY_MULTIPLIER_APPLIED = "weather_gate_multiplier_applied"
    _KEY_DECISION_SOURCE = "weather_gate_decision_source"

    def __init__(
        self,
        state_manager,
        timezone_str: str,
        config_provider,
    ):
        """
        Args:
            state_manager: project StateManager — used for persistence.
            timezone_str: IANA timezone name (e.g. "America/New_York").
            config_provider: zero-arg callable returning a
                WeatherGateConfigSnapshot. AutoControlService passes a closure
                so live edits via /api/weather/config are picked up each tick.
        """
        self._state = state_manager
        self._tz = ZoneInfo(timezone_str)
        self._config_provider = config_provider

        saved = state_manager.load()
        self.disabled: bool = bool(saved.get(self._KEY_DISABLED, False))
        self.reason: str = saved.get(self._KEY_REASON) or "init"
        self.expected_kwh: Optional[float] = saved.get(self._KEY_EXPECTED_KWH)
        self.deficit_kwh: Optional[float] = saved.get(self._KEY_DEFICIT_KWH)
        self.max_for_day_kwh: Optional[float] = saved.get(self._KEY_MAX_FOR_DAY_KWH)
        self.evaluated_at: Optional[str] = saved.get(self._KEY_EVALUATED_AT)
        self.eg4_today_kwh_raw: Optional[float] = saved.get(self._KEY_EG4_TODAY_KWH_RAW)
        self.multiplier_applied: Optional[float] = saved.get(self._KEY_MULTIPLIER_APPLIED)
        self.decision_source: Optional[str] = saved.get(self._KEY_DECISION_SOURCE)

        evaluated_date_str = saved.get(self._KEY_EVALUATED_DATE)
        self.evaluated_date: Optional[date] = (
            date.fromisoformat(evaluated_date_str) if evaluated_date_str else None
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(
        self,
        soc_pct: Optional[float],
        battery_fresh: bool,
        forecast: dict,
        force: bool = False,
    ) -> str:
        """Run one tick of the gate. Returns one of the OUTCOME_* constants.

        Caller must call this on every autocontrol tick BEFORE the normal
        decision tree. If outcome is OUTCOME_DISABLED_FOR_DAY (or self.disabled
        is True after the call), the caller force-stops the miner with
        stop_reason="weather_disabled" and skips its normal logic.

        Args:
            soc_pct: current battery SOC (0..100) or None when unknown.
            battery_fresh: True iff battery telemetry is fresh enough to trust.
            forecast: dict from WeatherService.get_today_forecast().
            force: if True, bypass the window and disabled guards (operator
                   re-evaluation via /api/weather/evaluate_now).
        """
        cfg = self._config_provider()
        if not cfg.enabled:
            # Master switch off: disable the gate but DO NOT clear past evaluation
            # so re-enabling mid-day picks up where we left off.
            if self.disabled:
                self._set_state(disabled=False, reason="gate_master_disabled")
            return OUTCOME_GATE_DISABLED

        now_local = datetime.now(self._tz)
        today_local = now_local.date()

        # Midnight reset — first tick after local midnight clears the disabled
        # flag so normal overnight autocontrol can run, but leaves
        # evaluated_date pointing at the previous day so the next pre-sunrise
        # window will rearm evaluation.
        if self.evaluated_date is not None and self.evaluated_date < today_local:
            if self.disabled:
                self._set_state(disabled=False, reason="midnight_reset")
                return OUTCOME_MIDNIGHT_RESET

        sunrise_dt = forecast.get("sunrise_dt")
        sunset_dt = forecast.get("sunset_dt")
        forecast_fresh = bool(forecast.get("is_fresh"))
        cloud_cover = forecast.get("cloud_cover_pct")
        eg4_today_kwh = forecast.get("eg4_today_kwh")
        eg4_is_fresh = bool(forecast.get("eg4_is_fresh"))

        # If we're already disabled, run the recovery check on every tick. It
        # cannot re-enable based on stale data, so freshness gates still apply.
        if self.disabled:
            return self._maybe_recover(
                soc_pct=soc_pct,
                battery_fresh=battery_fresh,
                forecast_fresh=forecast_fresh,
                sunset_dt=sunset_dt,
                cfg=cfg,
                now_local=now_local,
            )

        # Not currently disabled: only evaluate inside the pre-sunrise window
        # and only once per local day. evaluated_date == today means "already
        # decided", regardless of whether the decision was on/off.
        if self.evaluated_date == today_local:
            return OUTCOME_ALREADY_EVALUATED

        if sunrise_dt is None:
            # No sunrise yet — we can't determine the window. Skip silently.
            return OUTCOME_OUTSIDE_WINDOW

        window_start = sunrise_dt - timedelta(minutes=cfg.pre_sunrise_window_minutes)
        if not force and not (window_start <= now_local <= sunrise_dt):
            return OUTCOME_OUTSIDE_WINDOW

        # Inside the pre-sunrise window. Check freshness preconditions; if
        # stale, skip and DO NOT advance evaluated_date so we retry next tick.
        if not battery_fresh or soc_pct is None:
            if __debug__:
                log("WEATHER_GATE", "skip eval: battery stale; will retry next tick")
            return OUTCOME_BATTERY_STALE

        # The EG4 path requires only a fresh EG4 prediction; the solar_model
        # fallback path requires fresh Open-Meteo cloud cover. We only need
        # to declare "forecast stale" when BOTH sources are unavailable —
        # otherwise the EG4 path can still produce a decision even on days
        # when Open-Meteo is down.
        eg4_path_ready = eg4_is_fresh and eg4_today_kwh is not None
        fallback_path_ready = forecast_fresh and cloud_cover is not None
        if not eg4_path_ready and not fallback_path_ready:
            if __debug__:
                log("WEATHER_GATE", "skip eval: no fresh forecast source; will retry next tick")
            return OUTCOME_FORECAST_STALE

        # All preconditions met. Run the pure decision and commit.
        decision = self.decide_after_evaluation(
            soc_pct=soc_pct,
            cloud_cover_pct=float(cloud_cover) if cloud_cover is not None else None,
            day_of_year=now_local.timetuple().tm_yday,
            cfg=cfg,
            eg4_today_kwh=eg4_today_kwh if eg4_path_ready else None,
        )
        self._commit_evaluation(
            decision=decision,
            evaluated_date=today_local,
            evaluated_at=now_local.isoformat(timespec="seconds"),
        )
        if __debug__:
            log(
                "WEATHER_GATE",
                f"eval committed date={today_local} "
                f"source={decision['decision_source']} "
                f"expected_kwh={decision['expected_kwh']:.2f} "
                f"deficit_kwh={decision['deficit_kwh']:.2f} "
                f"ratio={decision['ratio']:.2f} "
                f"decision={decision['outcome']}",
            )
        return decision["outcome"]

    # ------------------------------------------------------------------
    # Pure decision rule — exercised directly by unit tests
    # ------------------------------------------------------------------

    @staticmethod
    def decide_after_evaluation(
        soc_pct: float,
        cloud_cover_pct: Optional[float],
        day_of_year: int,
        cfg: WeatherGateConfigSnapshot,
        eg4_today_kwh: Optional[float] = None,
    ) -> dict:
        """Pure: given preconditions are met, decide on/off and return numbers.

        Returns::

            {
                "outcome": OUTCOME_KEPT_ENABLED | OUTCOME_DISABLED_FOR_DAY,
                "expected_kwh": float,
                "deficit_kwh": float,
                "max_for_day_kwh": float,
                "ratio": float,
                "decision_source": "eg4_predict" | "solar_model_fallback",
                "eg4_today_kwh_raw": float | None,   # raw EG4 prediction (kWh) before multiplier
                "multiplier_applied": float | None,  # multiplier in effect at decision time
            }

        Source-selection rule:
          - If eg4_today_kwh is not None, the decision uses
            expected_kwh = eg4_today_kwh * cfg.eg4_predict_multiplier
            and the source is "eg4_predict". Zero is a valid EG4 prediction
            (pessimistic overcast day) and propagates to expected_kwh = 0.0.
          - Otherwise, expected_kwh comes from the seasonal solar_model
            attenuated by cloud_cover_pct, and the source is
            "solar_model_fallback". This branch requires cloud_cover_pct to
            be non-None; callers must not invoke it without one.

        The threshold is `expected_kwh >= deficit_kwh`. The EG4 predict
        multiplier already embeds the desired conservatism.
        """
        max_for_day = max_daily_energy_kwh(
            day_of_year, cfg.summer_max_kwh, cfg.winter_max_kwh
        )

        if eg4_today_kwh is not None:
            # EG4 path: raw prediction * conservative multiplier.
            multiplier = float(cfg.eg4_predict_multiplier)
            expected = float(eg4_today_kwh) * multiplier
            decision_source = DECISION_SOURCE_EG4
            eg4_today_kwh_raw = float(eg4_today_kwh)
            multiplier_applied: Optional[float] = multiplier
        else:
            # Fallback path: solar_model attenuated by cloud cover.
            if cloud_cover_pct is None:
                raise ValueError(
                    "decide_after_evaluation: cloud_cover_pct is required "
                    "when eg4_today_kwh is not provided"
                )
            expected = expected_energy_kwh(max_for_day, cloud_cover_pct)
            decision_source = DECISION_SOURCE_FALLBACK
            eg4_today_kwh_raw = None
            multiplier_applied = None

        deficit = (100.0 - max(0.0, min(100.0, soc_pct))) / 100.0 * cfg.battery_total_kwh
        # guard div-by-zero; battery already full → always enable
        if deficit <= 0:
            ratio = float("inf")
            outcome = OUTCOME_KEPT_ENABLED
        else:
            ratio = expected / deficit
            outcome = OUTCOME_KEPT_ENABLED if expected >= deficit else OUTCOME_DISABLED_FOR_DAY

        return {
            "outcome": outcome,
            "expected_kwh": expected,
            "deficit_kwh": deficit,
            "max_for_day_kwh": max_for_day,
            "ratio": ratio,
            "decision_source": decision_source,
            "eg4_today_kwh_raw": eg4_today_kwh_raw,
            "multiplier_applied": multiplier_applied,
        }

    # ------------------------------------------------------------------
    # Snapshot for API / dashboard
    # ------------------------------------------------------------------

    def get_state(self) -> dict:
        return {
            "disabled": self.disabled,
            "reason": self.reason,
            "expected_kwh": self.expected_kwh,
            "deficit_kwh": self.deficit_kwh,
            "max_for_day_kwh": self.max_for_day_kwh,
            "evaluated_at": self.evaluated_at,
            "evaluated_date": self.evaluated_date.isoformat() if self.evaluated_date else None,
            "eg4_today_kwh_raw": self.eg4_today_kwh_raw,
            "multiplier_applied": self.multiplier_applied,
            "decision_source": self.decision_source,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _maybe_recover(
        self,
        soc_pct: Optional[float],
        battery_fresh: bool,
        forecast_fresh: bool,
        sunset_dt: Optional[datetime],
        cfg: WeatherGateConfigSnapshot,
        now_local: datetime,
    ) -> str:
        """Recovery rule: lift disabled flag if SOC climbed in time."""
        if not battery_fresh or soc_pct is None:
            return OUTCOME_BATTERY_STALE
        if soc_pct < cfg.recovery_soc_threshold_pct:
            return OUTCOME_DISABLED_FOR_DAY  # stay disabled; nothing changed
        if sunset_dt is None or not forecast_fresh:
            # Without a sunset we can't measure "time left in day". Stay safe.
            return OUTCOME_DISABLED_FOR_DAY
        hours_left = (sunset_dt - now_local).total_seconds() / 3600.0
        if hours_left >= cfg.recovery_min_hours_before_sunset:
            self._set_state(disabled=False, reason="recovered_soc_in_time")
            if __debug__:
                log(
                    "WEATHER_GATE",
                    f"recovered: SOC={soc_pct:.1f}% hours_left={hours_left:.2f}",
                )
            return OUTCOME_RECOVERED
        # SOC reached threshold but too late to be useful.
        if self.reason != "recovery_window_too_short":
            self._set_state(disabled=True, reason="recovery_window_too_short")
        return OUTCOME_RECOVERY_WINDOW_TOO_SHORT

    def _commit_evaluation(
        self,
        decision: dict,
        evaluated_date: date,
        evaluated_at: str,
    ) -> None:
        if decision["outcome"] == OUTCOME_DISABLED_FOR_DAY:
            self.disabled = True
            self.reason = "insufficient_solar_expected"
        else:
            self.disabled = False
            self.reason = "sufficient_solar_expected"

        self.expected_kwh = float(decision["expected_kwh"])
        self.deficit_kwh = float(decision["deficit_kwh"])
        self.max_for_day_kwh = float(decision["max_for_day_kwh"])
        self.evaluated_date = evaluated_date
        self.evaluated_at = evaluated_at
        # Decision context — used by the dashboard and the prediction logger.
        eg4_raw = decision.get("eg4_today_kwh_raw")
        self.eg4_today_kwh_raw = float(eg4_raw) if eg4_raw is not None else None
        mult = decision.get("multiplier_applied")
        self.multiplier_applied = float(mult) if mult is not None else None
        self.decision_source = decision.get("decision_source")

        self._persist_all()

    def _set_state(self, disabled: bool, reason: str) -> None:
        self.disabled = disabled
        self.reason = reason
        self._persist_all()

    def _persist_all(self) -> None:
        self._state.save(
            **{
                self._KEY_DISABLED: self.disabled,
                self._KEY_REASON: self.reason,
                self._KEY_EXPECTED_KWH: self.expected_kwh,
                self._KEY_DEFICIT_KWH: self.deficit_kwh,
                self._KEY_MAX_FOR_DAY_KWH: self.max_for_day_kwh,
                self._KEY_EVALUATED_AT: self.evaluated_at,
                self._KEY_EVALUATED_DATE: (
                    self.evaluated_date.isoformat() if self.evaluated_date else None
                ),
                self._KEY_EG4_TODAY_KWH_RAW: self.eg4_today_kwh_raw,
                self._KEY_MULTIPLIER_APPLIED: self.multiplier_applied,
                self._KEY_DECISION_SOURCE: self.decision_source,
            }
        )
