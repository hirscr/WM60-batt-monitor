"""Unit tests for the EG4 prediction path through the WeatherGate decision.

Two layers exercised:
  1. The pure decide_after_evaluation() — does it pick the right source and
     apply the multiplier correctly?
  2. The state machine wrapper evaluate() — does the precondition split
     work (EG4 path doesn't require cloud cover; fallback path does)?
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from services import weather_gate as wg
from services.weather_gate import (
    DECISION_SOURCE_EG4,
    DECISION_SOURCE_FALLBACK,
    OUTCOME_DISABLED_FOR_DAY,
    OUTCOME_FORECAST_STALE,
    OUTCOME_KEPT_ENABLED,
    WeatherGate,
    WeatherGateConfigSnapshot,
)
from utils.state_manager import StateManager


TZ = ZoneInfo("America/New_York")


def _cfg(**overrides) -> WeatherGateConfigSnapshot:
    base = dict(
        enabled=True,
        battery_total_kwh=75.0,
        summer_max_kwh=75.0,
        winter_max_kwh=30.0,
        pre_sunrise_window_minutes=30,
        recovery_soc_threshold_pct=90,
        recovery_min_hours_before_sunset=3.0,
        eg4_predict_multiplier=0.8,
    )
    base.update(overrides)
    return WeatherGateConfigSnapshot(**base)


def _make_gate(tmp_path, cfg=None):
    state_path = str(tmp_path / "wm_state.json")
    sm = StateManager(path=state_path)
    cfg = cfg or _cfg()
    return WeatherGate(state_manager=sm, timezone_str="America/New_York", config_provider=lambda: cfg), sm


def _forecast(
    cloud_cover_pct=None,
    sunrise_dt=None,
    sunset_dt=None,
    is_fresh=True,
    eg4_today_kwh=None,
    eg4_is_fresh=False,
):
    return {
        "cloud_cover_pct": cloud_cover_pct,
        "sunrise_dt": sunrise_dt,
        "sunset_dt": sunset_dt,
        "is_fresh": is_fresh,
        "eg4_today_kwh": eg4_today_kwh,
        "eg4_is_fresh": eg4_is_fresh,
    }


def _freeze_now(monkeypatch, when: datetime) -> None:
    """Pin datetime.now(tz) inside weather_gate to a fixed value."""

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return when.replace(tzinfo=None)
            return when.astimezone(tz)

    monkeypatch.setattr(wg, "datetime", _FrozenDatetime)


# ----------------------------------------------------------------------
# Pure decision rule — source selection + multiplier math
# ----------------------------------------------------------------------


def test_decide_uses_eg4_path_and_applies_multiplier():
    """raw 71.2 kWh × 0.8 = 56.96 expected; deficit 30 kWh → KEPT_ENABLED."""
    cfg = _cfg(eg4_predict_multiplier=0.8, battery_total_kwh=75.0)
    res = WeatherGate.decide_after_evaluation(
        soc_pct=60.0,  # deficit = 30 kWh
        cloud_cover_pct=None,  # MUST be allowed when EG4 path is taken
        day_of_year=172,
        cfg=cfg,
        eg4_today_kwh=71.2,
    )
    assert res["decision_source"] == DECISION_SOURCE_EG4
    assert res["expected_kwh"] == pytest.approx(56.96, abs=1e-6)
    assert res["deficit_kwh"] == pytest.approx(30.0, abs=1e-6)
    assert res["outcome"] == OUTCOME_KEPT_ENABLED
    assert res["eg4_today_kwh_raw"] == 71.2
    assert res["multiplier_applied"] == 0.8


def test_decide_eg4_path_disables_under_low_multiplier():
    """30 × 0.5 = 15 expected; deficit 50 kWh → DISABLED."""
    cfg = _cfg(eg4_predict_multiplier=0.5, battery_total_kwh=100.0)
    res = WeatherGate.decide_after_evaluation(
        soc_pct=50.0,  # deficit = 50 kWh
        cloud_cover_pct=None,
        day_of_year=172,
        cfg=cfg,
        eg4_today_kwh=30.0,
    )
    assert res["expected_kwh"] == pytest.approx(15.0, abs=1e-6)
    assert res["outcome"] == OUTCOME_DISABLED_FOR_DAY


def test_decide_eg4_zero_prediction_disables_under_positive_deficit():
    """A confident EG4 zero must propagate to expected_kwh = 0.0 and disable
    the day for any positive deficit. This is the safety-critical case."""
    cfg = _cfg(eg4_predict_multiplier=0.8)
    res = WeatherGate.decide_after_evaluation(
        soc_pct=60.0,  # deficit > 0
        cloud_cover_pct=None,
        day_of_year=172,
        cfg=cfg,
        eg4_today_kwh=0.0,
    )
    assert res["expected_kwh"] == 0.0
    assert res["outcome"] == OUTCOME_DISABLED_FOR_DAY
    assert res["decision_source"] == DECISION_SOURCE_EG4


def test_decide_fallback_path_uses_solar_model():
    """eg4_today_kwh=None falls through to the cloud-cover-attenuated model."""
    cfg = _cfg()
    res = WeatherGate.decide_after_evaluation(
        soc_pct=70.0,
        cloud_cover_pct=10.0,
        day_of_year=172,
        cfg=cfg,
        eg4_today_kwh=None,
    )
    assert res["decision_source"] == DECISION_SOURCE_FALLBACK
    assert res["eg4_today_kwh_raw"] is None
    assert res["multiplier_applied"] is None
    # solar_model: summer max 75 × (1 - 0.10) = 67.5
    assert res["expected_kwh"] == pytest.approx(67.5, abs=1e-6)


def test_decide_fallback_without_cloud_cover_raises():
    """Calling the fallback path without cloud_cover is a programmer error."""
    cfg = _cfg()
    with pytest.raises(ValueError):
        WeatherGate.decide_after_evaluation(
            soc_pct=50.0, cloud_cover_pct=None, day_of_year=172, cfg=cfg,
        )


# ----------------------------------------------------------------------
# State machine — precondition split
# ----------------------------------------------------------------------


def test_evaluate_uses_eg4_when_cloud_cover_missing(tmp_path, monkeypatch):
    """The headline behavior: a fresh EG4 prediction lets the gate decide
    even when Open-Meteo is unavailable."""
    gate, _ = _make_gate(tmp_path)
    now = datetime(2026, 6, 21, 5, 30, tzinfo=TZ)
    sunrise = datetime(2026, 6, 21, 5, 45, tzinfo=TZ)
    sunset = datetime(2026, 6, 21, 20, 30, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(
        cloud_cover_pct=None,    # Open-Meteo MISSING
        sunrise_dt=sunrise,
        sunset_dt=sunset,
        is_fresh=False,          # forecast cache stale
        eg4_today_kwh=71.2,
        eg4_is_fresh=True,       # but EG4 IS fresh
    )
    outcome = gate.evaluate(soc_pct=60.0, battery_fresh=True, forecast=forecast)
    # The decision goes through; source is EG4.
    assert outcome == OUTCOME_KEPT_ENABLED
    assert gate.decision_source == DECISION_SOURCE_EG4
    assert gate.eg4_today_kwh_raw == 71.2
    assert gate.multiplier_applied == 0.8


def test_evaluate_falls_back_to_solar_model_when_eg4_stale(tmp_path, monkeypatch):
    """Stale or missing EG4 → solar_model with cloud cover."""
    gate, _ = _make_gate(tmp_path)
    now = datetime(2026, 6, 21, 5, 30, tzinfo=TZ)
    sunrise = datetime(2026, 6, 21, 5, 45, tzinfo=TZ)
    sunset = datetime(2026, 6, 21, 20, 30, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(
        cloud_cover_pct=10.0,
        sunrise_dt=sunrise,
        sunset_dt=sunset,
        is_fresh=True,
        eg4_today_kwh=None,
        eg4_is_fresh=False,
    )
    outcome = gate.evaluate(soc_pct=70.0, battery_fresh=True, forecast=forecast)
    assert outcome == OUTCOME_KEPT_ENABLED
    assert gate.decision_source == DECISION_SOURCE_FALLBACK


def test_evaluate_skips_when_both_sources_unavailable(tmp_path, monkeypatch):
    """No EG4 and no fresh cloud cover → skip, do NOT advance evaluated_date."""
    gate, _ = _make_gate(tmp_path)
    now = datetime(2026, 6, 21, 5, 30, tzinfo=TZ)
    sunrise = datetime(2026, 6, 21, 5, 45, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(
        cloud_cover_pct=None,
        sunrise_dt=sunrise,
        is_fresh=False,
        eg4_today_kwh=None,
        eg4_is_fresh=False,
    )
    outcome = gate.evaluate(soc_pct=60.0, battery_fresh=True, forecast=forecast)
    assert outcome == OUTCOME_FORECAST_STALE
    assert gate.evaluated_date is None  # NOT advanced — retry next tick


def test_evaluate_eg4_zero_prediction_disables_for_day(tmp_path, monkeypatch):
    """End-to-end: zero EG4 forecast → expected_kwh 0 → disabled for day."""
    gate, _ = _make_gate(tmp_path)
    now = datetime(2026, 6, 21, 5, 30, tzinfo=TZ)
    sunrise = datetime(2026, 6, 21, 5, 45, tzinfo=TZ)
    sunset = datetime(2026, 6, 21, 20, 30, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(
        cloud_cover_pct=None,
        sunrise_dt=sunrise,
        sunset_dt=sunset,
        is_fresh=False,
        eg4_today_kwh=0.0,
        eg4_is_fresh=True,
    )
    outcome = gate.evaluate(soc_pct=60.0, battery_fresh=True, forecast=forecast)
    assert outcome == OUTCOME_DISABLED_FOR_DAY
    assert gate.expected_kwh == 0.0
    assert gate.decision_source == DECISION_SOURCE_EG4


def test_persisted_state_includes_decision_source(tmp_path, monkeypatch):
    """A fresh gate loaded from disk sees the prior decision context."""
    gate, sm = _make_gate(tmp_path)
    now = datetime(2026, 6, 21, 5, 30, tzinfo=TZ)
    sunrise = datetime(2026, 6, 21, 5, 45, tzinfo=TZ)
    sunset = datetime(2026, 6, 21, 20, 30, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(
        sunrise_dt=sunrise,
        sunset_dt=sunset,
        is_fresh=False,
        eg4_today_kwh=71.2,
        eg4_is_fresh=True,
    )
    gate.evaluate(soc_pct=60.0, battery_fresh=True, forecast=forecast)

    cfg = _cfg()
    fresh = WeatherGate(state_manager=sm, timezone_str="America/New_York", config_provider=lambda: cfg)
    assert fresh.decision_source == DECISION_SOURCE_EG4
    assert fresh.eg4_today_kwh_raw == 71.2
    assert fresh.multiplier_applied == 0.8
