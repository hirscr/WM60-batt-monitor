import json, os, tempfile, threading, time

DEFAULT_STATE = {
    "autocontrol": False,           # whether autocontrol is enabled
    "miner_power_state": "stopped", # "stopped", "running", or "pending"
    "target_power_pct": 0,          # e.g., 40 for 40 percent
    "emergency_soc": None,          # runtime override for emergency SOC %; None means use config value
    # User-commanded master switch state. True means "user wants the miner
    # available". False means the user has clicked Power-OFF on the dashboard.
    # Only HTTP endpoints triggered by user clicks write to this field; service-
    # side code (AutoControl safety stops, verification failures, etc.) must
    # NEVER touch it. Defaults to True so existing deploys preserve current
    # behavior on first load after this field was introduced.
    "user_power_intent": True,
    # Tier-promotion state — owned by services/tier_promotion.py. Timestamps
    # are monotonic-clock samples; only meaningful within a single process.
    # They are persisted to avoid spurious post-restart promotions, even though
    # the cooldown math compares against the new monotonic clock.
    # NOTE: last_seen_soc is deliberately NOT persisted — see
    # services/tier_promotion.py::to_state_dict.
    "weather_promotion_tier": None,        # None | 90 | 100
    "last_demotion_from_90_ts": 0.0,
    "last_demotion_from_100_ts": 0.0,
    # Emergency latch state. emergency_active is the latch boolean (was already
    # persisted via state.save calls). emergency_latch_set_at is the wallclock
    # timestamp the latch was tripped — used for observability via get_state().
    "emergency_active": False,             # bool — latch state
    "emergency_latch_set_at": None,        # float | None — time.time() when latch tripped
    # Weather gate auxiliary fields — populated by WeatherGate when an
    # evaluation commits. The gate already persists its primary decision
    # fields (disabled, reason, expected_kwh, deficit_kwh, evaluated_date)
    # under its own _KEY_ namespace.
    "weather_gate_eg4_today_kwh_raw": None,    # float | None — raw EG4 prediction before multiplier
    "weather_gate_multiplier_applied": None,   # float | None — multiplier in effect at decision time
    "weather_gate_decision_source": None,      # "eg4_predict" | "solar_model_fallback" | None
    # PVPredictionLogger state — owned by services/pv_prediction_logger.py.
    # The local-date ISO string (YYYY-MM-DD) of the last day for which a
    # prediction-log row was written. Prevents double-logging across restarts.
    "last_pv_log_date": None,
    "last_updated": 0               # unix time seconds
}

class StateManager:
    def __init__(self, path="wm_state.json"):
        self.path = path
        self._lock = threading.Lock()
        if not os.path.exists(self.path):
            self._atomic_write(DEFAULT_STATE)

    def load(self):
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
            # fill any missing fields for forward compatibility
            merged = DEFAULT_STATE.copy()
            merged.update(data or {})
            return merged
        except Exception:
            return DEFAULT_STATE.copy()

    def save(self, **kwargs):
        with self._lock:
            state = self.load()
            state.update(kwargs)
            state["last_updated"] = int(time.time())
            self._atomic_write(state)
            return state

    def _atomic_write(self, state):
        d = os.path.dirname(os.path.abspath(self.path)) or "."
        fd, tmp = tempfile.mkstemp(prefix=".wmstate.", dir=d)
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(state, f, separators=(",", ":"), sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)  # atomic on POSIX
        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
