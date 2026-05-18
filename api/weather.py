"""Weather gate observability + control endpoints.

  GET  /api/weather/status         — current forecast + gate decision + config
  POST /api/weather/config         — update editable gate parameters
  POST /api/weather/evaluate_now   — force a one-off gate evaluation

The blueprint wires three collaborators provided at register time:
  - weather_service: WeatherService instance (forecast cache)
  - autocontrol_service: AutoControlService (owns the gate)
  - settings: project Settings — used to read config defaults and persist edits

Config persistence: edits land in config.local.yaml (never the committed
config.yaml). On a successful POST we also mutate the in-memory
settings.weather_gate so subsequent requests see the new value immediately.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

import yaml
from flask import Blueprint, jsonify, request


# Editable operational parameters. Anything outside this set in a POST body
# is rejected with HTTP 400 — explicitly including the forecast refresh /
# freshness keys, which are deliberately config-only.
EDITABLE_KEYS: dict = {
    # key: (type, validator(value) -> Optional[error_str])
    "enabled":                          (bool,  lambda v: None if isinstance(v, bool) else "must be bool"),
    "battery_total_kwh":                (float, lambda v: None if v > 0 else "must be > 0"),
    "summer_max_kwh":                   (float, lambda v: None if v > 0 else "must be > 0"),
    "winter_max_kwh":                   (float, lambda v: None if v > 0 else "must be > 0"),
    "safety_factor":                    (float, lambda v: None if 1.0 <= v <= 2.0 else "must be in [1.0, 2.0]"),
    "pre_sunrise_window_minutes":       (int,   lambda v: None if 0 <= v <= 240 else "must be in [0, 240]"),
    "recovery_soc_threshold_pct":       (int,   lambda v: None if 0 <= v <= 100 else "must be in [0, 100]"),
    "recovery_min_hours_before_sunset": (float, lambda v: None if 0.0 <= v <= 12.0 else "must be in [0.0, 12.0]"),
}

# Keys present in the dataclass but explicitly forbidden via the UI.
FORBIDDEN_KEYS = {"forecast_refresh_seconds", "forecast_freshness_seconds"}

LOCAL_CONFIG_FILENAME = "config.local.yaml"


def create_blueprint(weather_service, autocontrol_service, settings) -> Blueprint:
    """Build the /api/weather/* blueprint bound to the given collaborators."""
    bp = Blueprint("weather", __name__)

    @bp.get("/api/weather/status")
    def weather_status():
        forecast = weather_service.get_today_forecast() if weather_service else {}
        gate_state = autocontrol_service.weather_gate.get_state() if autocontrol_service.weather_gate else None
        wg = settings.weather_gate

        return jsonify({
            "forecast": _serialize_forecast(forecast),
            "gate": gate_state,
            "config": _serialize_editable(wg),
            "config_meta": {
                "forecast_refresh_seconds": wg.forecast_refresh_seconds,
                "forecast_freshness_seconds": wg.forecast_freshness_seconds,
            },
        })

    @bp.post("/api/weather/config")
    def weather_config():
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return jsonify({"error": "body must be a JSON object"}), 400

        # Reject forbidden keys explicitly so the caller knows why.
        bad = sorted(k for k in body.keys() if k in FORBIDDEN_KEYS)
        if bad:
            return jsonify({
                "error": "config-only keys cannot be set via API",
                "rejected_keys": bad,
            }), 400

        # Unknown keys are rejected too — keeps the surface tight.
        unknown = sorted(k for k in body.keys() if k not in EDITABLE_KEYS)
        if unknown:
            return jsonify({
                "error": "unknown keys",
                "unknown_keys": unknown,
            }), 400

        # Type + range validation.
        coerced: dict = {}
        for key, raw in body.items():
            type_, validator = EDITABLE_KEYS[key]
            try:
                if type_ is bool:
                    if not isinstance(raw, bool):
                        return jsonify({"error": f"{key}: must be bool"}), 400
                    value = raw
                else:
                    value = type_(raw)
            except (TypeError, ValueError):
                return jsonify({"error": f"{key}: not coercible to {type_.__name__}"}), 400
            err = validator(value)
            if err is not None:
                return jsonify({"error": f"{key}: {err}"}), 400
            coerced[key] = value

        if not coerced:
            return jsonify({"error": "no editable keys supplied"}), 400

        # Persist to config.local.yaml (merging with anything already there).
        try:
            _persist_local_config(coerced)
        except Exception as exc:
            return jsonify({"error": f"failed to persist config: {exc}"}), 500

        # Apply to the in-memory settings so subsequent ticks see the change
        # immediately without waiting for a restart.
        for key, value in coerced.items():
            setattr(settings.weather_gate, key, value)

        wg = settings.weather_gate
        return jsonify({
            "ok": True,
            "config": _serialize_editable(wg),
        })

    @bp.post("/api/weather/evaluate_now")
    def evaluate_now():
        result = autocontrol_service.force_evaluate_weather_gate()
        return jsonify(result)

    return bp


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _serialize_editable(wg) -> dict:
    """Project the editable keys from a WeatherGateConfig dataclass."""
    return {key: getattr(wg, key) for key in EDITABLE_KEYS}


def _serialize_forecast(forecast: dict) -> dict:
    """Render datetime fields as ISO strings for the JSON payload."""
    def _iso(val):
        return val.isoformat() if hasattr(val, "isoformat") else val

    return {
        "cloud_cover_pct": forecast.get("cloud_cover_pct"),
        "sunrise": _iso(forecast.get("sunrise_dt")),
        "sunset": _iso(forecast.get("sunset_dt")),
        "for_date": _iso(forecast.get("for_date")),
        "fetched_at": _iso(forecast.get("fetched_at")),
        "age_seconds": forecast.get("age_seconds"),
        "is_fresh": forecast.get("is_fresh", False),
        "last_error": forecast.get("last_error"),
    }


def _persist_local_config(coerced: dict, path: Optional[str] = None) -> None:
    """Merge the new weather_gate fields into config.local.yaml.

    Creates the file if missing. Leaves all other top-level sections untouched.
    """
    target = Path(path) if path else Path(LOCAL_CONFIG_FILENAME)
    if target.exists():
        with target.open("r") as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}
    if not isinstance(data, dict):
        raise RuntimeError("config.local.yaml root must be a mapping")

    section = dict(data.get("weather_gate") or {})
    section.update(coerced)
    data["weather_gate"] = section

    tmp = target.with_suffix(target.suffix + ".tmp")
    with tmp.open("w") as f:
        yaml.safe_dump(data, f, sort_keys=False)
    tmp.replace(target)
