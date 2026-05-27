"""Unit tests for POST /api/weather/config.

The blueprint depends on three collaborators; we pass minimal fakes. Config
persistence is exercised by pointing LOCAL_CONFIG_FILENAME at a temp file via
monkeypatch.

Coverage:
  - happy path: valid partial update applies + persists
  - rejects forecast refresh/freshness (forbidden keys)
  - rejects unknown keys
  - rejects out-of-range values
  - rejects wrong types
  - empty body is rejected
  - non-dict body is rejected
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml
from flask import Flask

from api import weather as weather_api


def _settings_with_defaults():
    """Build a minimal settings object with a writable weather_gate namespace."""
    wg = SimpleNamespace(
        enabled=True,
        battery_total_kwh=75.0,
        summer_max_kwh=75.0,
        winter_max_kwh=30.0,
        pre_sunrise_window_minutes=30,
        recovery_soc_threshold_pct=90,
        recovery_min_hours_before_sunset=3.0,
        eg4_predict_multiplier=0.8,
        forecast_refresh_seconds=3600,
        forecast_freshness_seconds=7200,
    )
    return SimpleNamespace(weather_gate=wg)


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Flask test client wired to in-memory fakes and a temp config file."""
    local_yaml = tmp_path / "config.local.yaml"
    monkeypatch.setattr(weather_api, "LOCAL_CONFIG_FILENAME", str(local_yaml))

    settings = _settings_with_defaults()
    weather_service = MagicMock()
    weather_service.get_today_forecast.return_value = {}
    autocontrol_service = MagicMock()
    autocontrol_service.weather_gate.get_state.return_value = {"disabled": False}
    autocontrol_service.force_evaluate_weather_gate.return_value = {"outcome": "kept_enabled"}

    app = Flask(__name__)
    app.register_blueprint(
        weather_api.create_blueprint(weather_service, autocontrol_service, settings)
    )
    test_client = app.test_client()
    return test_client, settings, local_yaml


# ----------------------------------------------------------------------
# Happy path
# ----------------------------------------------------------------------


def test_valid_partial_update_applies_and_persists(client):
    test_client, settings, local_yaml = client

    resp = test_client.post(
        "/api/weather/config",
        json={"battery_total_kwh": 100.0, "eg4_predict_multiplier": 0.9},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert settings.weather_gate.battery_total_kwh == 100.0
    assert settings.weather_gate.eg4_predict_multiplier == 0.9
    assert local_yaml.exists()
    with local_yaml.open() as f:
        data = yaml.safe_load(f)
    assert data["weather_gate"]["battery_total_kwh"] == 100.0
    assert data["weather_gate"]["eg4_predict_multiplier"] == 0.9


def test_bool_master_switch_round_trips(client):
    test_client, settings, _ = client
    resp = test_client.post("/api/weather/config", json={"enabled": False})
    assert resp.status_code == 200
    assert settings.weather_gate.enabled is False


# ----------------------------------------------------------------------
# Rejections
# ----------------------------------------------------------------------


def test_rejects_forecast_refresh_key(client):
    test_client, settings, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"forecast_refresh_seconds": 1800}
    )
    assert resp.status_code == 400
    body = resp.get_json()
    assert "rejected_keys" in body
    assert body["rejected_keys"] == ["forecast_refresh_seconds"]
    # Setting was NOT mutated
    assert settings.weather_gate.forecast_refresh_seconds == 3600


def test_rejects_forecast_freshness_key(client):
    test_client, _, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"forecast_freshness_seconds": 1800}
    )
    assert resp.status_code == 400
    body = resp.get_json()
    assert body["rejected_keys"] == ["forecast_freshness_seconds"]


def test_rejects_unknown_key(client):
    test_client, _, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"bogus_key": 1}
    )
    assert resp.status_code == 400
    body = resp.get_json()
    assert "unknown_keys" in body
    assert body["unknown_keys"] == ["bogus_key"]


def test_rejects_negative_battery_kwh(client):
    test_client, _, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"battery_total_kwh": -5.0}
    )
    assert resp.status_code == 400


def test_rejects_multiplier_above_range(client):
    test_client, _, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"eg4_predict_multiplier": 1.5}
    )
    assert resp.status_code == 400


def test_rejects_multiplier_below_range(client):
    test_client, _, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"eg4_predict_multiplier": 0.0}
    )
    assert resp.status_code == 400


def test_rejects_pct_over_100(client):
    test_client, _, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"recovery_soc_threshold_pct": 150}
    )
    assert resp.status_code == 400


def test_rejects_hours_above_12(client):
    test_client, _, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"recovery_min_hours_before_sunset": 13.0}
    )
    assert resp.status_code == 400


def test_rejects_non_bool_for_enabled(client):
    test_client, _, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"enabled": "yes"}
    )
    assert resp.status_code == 400


def test_rejects_empty_body(client):
    test_client, _, _ = client
    resp = test_client.post("/api/weather/config", json={})
    assert resp.status_code == 400


def test_rejects_array_body(client):
    test_client, _, _ = client
    resp = test_client.post("/api/weather/config", json=[1, 2, 3])
    assert resp.status_code == 400


# ----------------------------------------------------------------------
# Persistence merge behavior
# ----------------------------------------------------------------------


def test_persist_merges_with_existing_local_yaml(tmp_path, monkeypatch, client):
    """A partial POST should not blow away unrelated sections in config.local.yaml."""
    test_client, settings, local_yaml = client
    # Pre-seed the local YAML with a foreign section.
    local_yaml.parent.mkdir(parents=True, exist_ok=True)
    local_yaml.write_text(yaml.safe_dump({
        "miner": {"host": "10.0.0.5"},
        "weather_gate": {"battery_total_kwh": 50.0},
    }))

    resp = test_client.post(
        "/api/weather/config", json={"eg4_predict_multiplier": 0.7}
    )
    assert resp.status_code == 200

    with local_yaml.open() as f:
        data = yaml.safe_load(f)
    assert data["miner"]["host"] == "10.0.0.5"  # untouched
    assert data["weather_gate"]["eg4_predict_multiplier"] == 0.7
    assert data["weather_gate"]["battery_total_kwh"] == 50.0  # preserved


# ----------------------------------------------------------------------
# Status + evaluate_now
# ----------------------------------------------------------------------


def test_status_returns_forecast_gate_and_config(client):
    test_client, _, _ = client
    resp = test_client.get("/api/weather/status")
    assert resp.status_code == 200
    body = resp.get_json()
    assert "forecast" in body
    assert "gate" in body
    assert "config" in body
    assert "config_meta" in body
    # config has all editable keys, config_meta has the two non-editable ones
    assert "battery_total_kwh" in body["config"]
    assert "forecast_refresh_seconds" in body["config_meta"]


def test_evaluate_now_delegates_to_service(client):
    test_client, _, _ = client
    resp = test_client.post("/api/weather/evaluate_now")
    assert resp.status_code == 200
    assert resp.get_json() == {"outcome": "kept_enabled"}


def test_prediction_history_returns_503_without_logger(client):
    """The blueprint defaults to no pv_prediction_logger — endpoint must
    fail loudly with 503 in that case rather than crash with a 500."""
    test_client, _, _ = client
    resp = test_client.get("/api/weather/prediction_history?days=7")
    assert resp.status_code == 503
    body = resp.get_json()
    assert "error" in body


def test_prediction_history_with_logger_returns_rows(tmp_path, monkeypatch):
    """When wired, the endpoint returns the logger's read_recent_rows output."""
    from unittest.mock import MagicMock
    from flask import Flask
    from api import weather as weather_api

    settings = _settings_with_defaults()
    weather_service = MagicMock()
    weather_service.get_today_forecast.return_value = {}
    autocontrol_service = MagicMock()
    autocontrol_service.weather_gate.get_state.return_value = {"disabled": False}
    pv_logger = MagicMock()
    pv_logger.read_recent_rows.return_value = [
        {"date": "2026-05-22", "actual_kwh": "40.0", "eg4_today_kwh_raw": "50.0",
         "ratio_actual_to_eg4_raw": "0.8", "decision_source": "eg4_predict"},
    ]

    app = Flask(__name__)
    app.register_blueprint(
        weather_api.create_blueprint(
            weather_service, autocontrol_service, settings,
            pv_prediction_logger=pv_logger,
        )
    )
    test_client = app.test_client()
    resp = test_client.get("/api/weather/prediction_history?days=7")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["days"] == 7
    assert len(body["rows"]) == 1
    pv_logger.read_recent_rows.assert_called_once_with(7)


def test_prediction_history_clamps_days(tmp_path):
    """days outside [1, 365] is clamped, not rejected."""
    from unittest.mock import MagicMock
    from flask import Flask
    from api import weather as weather_api

    settings = _settings_with_defaults()
    weather_service = MagicMock()
    autocontrol_service = MagicMock()
    pv_logger = MagicMock()
    pv_logger.read_recent_rows.return_value = []

    app = Flask(__name__)
    app.register_blueprint(
        weather_api.create_blueprint(
            weather_service, autocontrol_service, settings,
            pv_prediction_logger=pv_logger,
        )
    )
    tc = app.test_client()
    # Out of range high
    tc.get("/api/weather/prediction_history?days=9999")
    pv_logger.read_recent_rows.assert_called_with(365)
    # Out of range low
    tc.get("/api/weather/prediction_history?days=0")
    pv_logger.read_recent_rows.assert_called_with(1)
    # Bad input -> default 14
    tc.get("/api/weather/prediction_history?days=garbage")
    pv_logger.read_recent_rows.assert_called_with(14)


# ----------------------------------------------------------------------
# Smoke test: AutoControlService.get_state() includes tier_promotion block.
#
# The /api/autocontrol/status route in app.py is a one-line jsonify of
# autocontrol_service.get_state(). Importing app.py at test time is too
# heavy (it spins up background threads); instead we wire a real
# AutoControlService against minimal fakes and call get_state() directly.
# This guarantees /api/autocontrol/status will include the tier_promotion
# block without standing up the full app.
# ----------------------------------------------------------------------


def test_autocontrol_state_includes_tier_promotion(tmp_path, monkeypatch):
    """get_state() — the body of /api/autocontrol/status — exposes the
    tier_promotion block with the expected shape."""
    from services.autocontrol_service import AutoControlService
    from utils.state_manager import StateManager

    state_file = tmp_path / "wm_state.json"
    state_mgr = StateManager(path=str(state_file))

    miner = MagicMock()
    miner.is_off = False
    miner.get_status.return_value = {"upfreq_complete": 1}
    battery = MagicMock()
    battery.is_fresh.return_value = True
    battery.get_battery_age_seconds.return_value = 5.0
    battery.get_status.return_value = {"soc_percent": None}

    weather_service = MagicMock()
    weather_service.get_today_forecast.return_value = {
        "cloud_cover_remaining_daylight_pct": None,
        "is_fresh": False,
        "sunset_dt": None,
    }

    svc = AutoControlService(
        miner_service=miner,
        battery_service=battery,
        state_manager=state_mgr,
        base_watts=4000,
        min_interval_sec=60,
        mode="away",
        away_config={
            "emergency_soc": 30,
            "max_pv_power": 3600,
            "after_sunset_min_soc": 40,
        },
        location_config={
            "latitude": 40.0,
            "longitude": -74.0,
            "timezone": "America/New_York",
        },
        weather_service=weather_service,
        weather_gate=None,
    )

    state = svc.get_state()
    assert "tier_promotion" in state
    tp = state["tier_promotion"]
    assert set(tp.keys()) == {
        "tier",
        "cooldown_remaining_90_sec",
        "cooldown_remaining_100_sec",
        "tier_baseline_soc",
    }
    assert tp["tier"] is None
    assert tp["cooldown_remaining_90_sec"] == 0
    assert tp["cooldown_remaining_100_sec"] == 0
    assert tp["tier_baseline_soc"] is None


# ----------------------------------------------------------------------
# Regression: tier_baseline_soc never falls back to live battery SOC.
#
# An earlier implementation papered over a None tier baseline by reading
# live battery SOC. That fallback was misleading — it surfaced a live
# reading as if it were the recorded tier baseline. The current contract
# is: tier_baseline_soc is exactly self.tier_promotion.last_seen_soc, or
# None. Never a substitute.
# ----------------------------------------------------------------------


def test_tier_baseline_soc_returns_none_when_unset_even_with_live_soc(tmp_path):
    """When TierPromotion.last_seen_soc is None (first tick after restart),
    tier_baseline_soc must be None — even if live battery SOC is available.
    """
    from services.autocontrol_service import AutoControlService
    from utils.state_manager import StateManager

    state_file = tmp_path / "wm_state.json"
    state_mgr = StateManager(path=str(state_file))

    miner = MagicMock()
    miner.is_off = False
    miner.get_status.return_value = {"upfreq_complete": 1}
    battery = MagicMock()
    battery.is_fresh.return_value = True
    battery.get_battery_age_seconds.return_value = 5.0
    # Live SOC IS available — but tier_baseline_soc must NOT use it.
    battery.get_status.return_value = {"soc_percent": 55.0}

    weather_service = MagicMock()
    weather_service.get_today_forecast.return_value = {
        "cloud_cover_remaining_daylight_pct": None,
        "is_fresh": False,
        "sunset_dt": None,
    }

    svc = AutoControlService(
        miner_service=miner,
        battery_service=battery,
        state_manager=state_mgr,
        base_watts=4000,
        min_interval_sec=60,
        mode="away",
        away_config={
            "emergency_soc": 30,
            "max_pv_power": 3600,
            "after_sunset_min_soc": 40,
        },
        location_config={
            "latitude": 40.0,
            "longitude": -74.0,
            "timezone": "America/New_York",
        },
        weather_service=weather_service,
        weather_gate=None,
    )

    state = svc.get_state()
    tp = state["tier_promotion"]
    # Must be None — no fallback to live battery SOC.
    assert tp["tier_baseline_soc"] is None
    # The old key must not appear on the external surface.
    assert "last_seen_soc" not in tp
