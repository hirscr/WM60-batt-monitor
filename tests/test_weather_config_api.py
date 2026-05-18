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
        safety_factor=1.1,
        pre_sunrise_window_minutes=30,
        recovery_soc_threshold_pct=90,
        recovery_min_hours_before_sunset=3.0,
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
        json={"battery_total_kwh": 100.0, "safety_factor": 1.25},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    # In-memory settings updated
    assert settings.weather_gate.battery_total_kwh == 100.0
    assert settings.weather_gate.safety_factor == 1.25
    # File written
    assert local_yaml.exists()
    with local_yaml.open() as f:
        data = yaml.safe_load(f)
    assert data["weather_gate"]["battery_total_kwh"] == 100.0
    assert data["weather_gate"]["safety_factor"] == 1.25


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


def test_rejects_safety_factor_above_range(client):
    test_client, _, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"safety_factor": 3.0}
    )
    assert resp.status_code == 400


def test_rejects_safety_factor_below_range(client):
    test_client, _, _ = client
    resp = test_client.post(
        "/api/weather/config", json={"safety_factor": 0.5}
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
        "/api/weather/config", json={"safety_factor": 1.3}
    )
    assert resp.status_code == 200

    with local_yaml.open() as f:
        data = yaml.safe_load(f)
    assert data["miner"]["host"] == "10.0.0.5"  # untouched
    assert data["weather_gate"]["safety_factor"] == 1.3
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
