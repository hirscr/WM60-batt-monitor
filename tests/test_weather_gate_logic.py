"""Unit tests for WeatherGate decision rule and state machine.

No live network. Battery and forecast inputs are passed directly to evaluate().
The pure decision rule (decide_after_evaluation) is exercised independently.

Coverage:
  - sufficient solar keeps gate enabled
  - gap zone (between expected and threshold) keeps gate disabled (conservative)
  - strictly insufficient disables gate
  - successful eval advances evaluated_date so subsequent ticks skip
  - stale battery skips eval and does NOT advance evaluated_date
  - stale forecast skips eval and does NOT advance evaluated_date
  - outside pre-sunrise window skips eval (no state change)
  - midnight reset clears disabled flag and rearms next-day evaluation
  - recovery: SOC threshold met with time remaining lifts disabled flag
  - recovery: SOC threshold met but too late in day stays disabled
  - master switch off bypasses all evaluation
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from services import weather_gate as wg
from services.weather_gate import (
    OUTCOME_ALREADY_EVALUATED,
    OUTCOME_BATTERY_STALE,
    OUTCOME_DISABLED_FOR_DAY,
    OUTCOME_FORECAST_STALE,
    OUTCOME_GATE_DISABLED,
    OUTCOME_KEPT_ENABLED,
    OUTCOME_MIDNIGHT_RESET,
    OUTCOME_OUTSIDE_WINDOW,
    OUTCOME_RECOVERED,
    OUTCOME_RECOVERY_WINDOW_TOO_SHORT,
    WeatherGate,
    WeatherGateConfigSnapshot,
)
from utils.state_manager import StateManager


TZ = ZoneInfo("America/New_York")
SUMMER_DAY_OF_YEAR = 172  # June 21


def _cfg(**overrides) -> WeatherGateConfigSnapshot:
    base = dict(
        enabled=True,
        battery_total_kwh=75.0,
        summer_max_kwh=75.0,
        winter_max_kwh=30.0,
        pre_sunrise_window_minutes=30,
        recovery_soc_threshold_pct=90,
        recovery_min_hours_before_sunset=3.0,
    )
    base.update(overrides)
    return WeatherGateConfigSnapshot(**base)


def _make_gate(tmp_path, cfg=None):
    """Build a WeatherGate backed by a temp state file."""
    state_path = str(tmp_path / "wm_state.json")
    sm = StateManager(path=state_path)
    cfg = cfg or _cfg()
    return WeatherGate(state_manager=sm, timezone_str="America/New_York", config_provider=lambda: cfg), sm


def _forecast(
    cloud_cover_pct=20.0,
    sunrise_dt=None,
    sunset_dt=None,
    is_fresh=True,
):
    """Build a forecast snapshot dict in the shape WeatherService produces."""
    return {
        "cloud_cover_pct": cloud_cover_pct,
        "sunrise_dt": sunrise_dt,
        "sunset_dt": sunset_dt,
        "is_fresh": is_fresh,
    }


# ----------------------------------------------------------------------
# Pure decision rule
# ----------------------------------------------------------------------


def test_pure_decision_sufficient_solar_keeps_enabled():
    cfg = _cfg()
    res = WeatherGate.decide_after_evaluation(
        soc_pct=70.0, cloud_cover_pct=10.0, day_of_year=SUMMER_DAY_OF_YEAR, cfg=cfg
    )
    assert res["outcome"] == OUTCOME_KEPT_ENABLED
    assert res["expected_kwh"] >= res["deficit_kwh"]


def test_pure_decision_strictly_insufficient_disables():
    cfg = _cfg()
    res = WeatherGate.decide_after_evaluation(
        soc_pct=30.0, cloud_cover_pct=95.0, day_of_year=SUMMER_DAY_OF_YEAR, cfg=cfg
    )
    assert res["outcome"] == OUTCOME_DISABLED_FOR_DAY


def test_pure_decision_full_battery_treats_as_sufficient():
    # No deficit -> ratio infinite -> kept enabled regardless of clouds
    cfg = _cfg()
    res = WeatherGate.decide_after_evaluation(
        soc_pct=100.0, cloud_cover_pct=100.0, day_of_year=SUMMER_DAY_OF_YEAR, cfg=cfg
    )
    assert res["outcome"] == OUTCOME_KEPT_ENABLED


# ----------------------------------------------------------------------
# State machine: stale-input behavior
# ----------------------------------------------------------------------


def test_stale_battery_does_not_advance_evaluated_date(tmp_path, monkeypatch):
    gate, _ = _make_gate(tmp_path)
    # Pin now-local to inside the pre-sunrise window.
    now = datetime(2026, 6, 21, 5, 30, tzinfo=TZ)
    sunrise = datetime(2026, 6, 21, 5, 45, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    outcome = gate.evaluate(
        soc_pct=50.0, battery_fresh=False, forecast=_forecast(sunrise_dt=sunrise)
    )
    assert outcome == OUTCOME_BATTERY_STALE
    assert gate.evaluated_date is None  # NOT advanced
    assert gate.disabled is False


def test_stale_forecast_does_not_advance_evaluated_date(tmp_path, monkeypatch):
    gate, _ = _make_gate(tmp_path)
    now = datetime(2026, 6, 21, 5, 30, tzinfo=TZ)
    sunrise = datetime(2026, 6, 21, 5, 45, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(sunrise_dt=sunrise, is_fresh=False)
    outcome = gate.evaluate(soc_pct=50.0, battery_fresh=True, forecast=forecast)
    assert outcome == OUTCOME_FORECAST_STALE
    assert gate.evaluated_date is None  # NOT advanced
    assert gate.disabled is False


# ----------------------------------------------------------------------
# State machine: window guard + once-per-day
# ----------------------------------------------------------------------


def test_outside_pre_sunrise_window_skips(tmp_path, monkeypatch):
    gate, _ = _make_gate(tmp_path)
    # 09:00 local is well outside any 30-minute pre-sunrise window
    now = datetime(2026, 6, 21, 9, 0, tzinfo=TZ)
    sunrise = datetime(2026, 6, 21, 5, 30, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    outcome = gate.evaluate(
        soc_pct=50.0, battery_fresh=True, forecast=_forecast(sunrise_dt=sunrise)
    )
    assert outcome == OUTCOME_OUTSIDE_WINDOW
    assert gate.evaluated_date is None


def test_successful_eval_advances_date_and_skips_next_tick(tmp_path, monkeypatch):
    gate, _ = _make_gate(tmp_path)
    now = datetime(2026, 6, 21, 5, 30, tzinfo=TZ)
    sunrise = datetime(2026, 6, 21, 5, 45, tzinfo=TZ)
    sunset = datetime(2026, 6, 21, 20, 30, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(cloud_cover_pct=10.0, sunrise_dt=sunrise, sunset_dt=sunset)
    first = gate.evaluate(soc_pct=70.0, battery_fresh=True, forecast=forecast)
    assert first == OUTCOME_KEPT_ENABLED
    assert gate.evaluated_date == date(2026, 6, 21)

    # Second tick (later same morning, still in window): already evaluated.
    now2 = datetime(2026, 6, 21, 5, 35, tzinfo=TZ)
    _freeze_now(monkeypatch, now2)
    second = gate.evaluate(soc_pct=70.0, battery_fresh=True, forecast=forecast)
    assert second == OUTCOME_ALREADY_EVALUATED


def test_insufficient_eval_disables_for_day_and_persists(tmp_path, monkeypatch):
    gate, sm = _make_gate(tmp_path)
    now = datetime(2026, 12, 21, 7, 0, tzinfo=TZ)
    sunrise = datetime(2026, 12, 21, 7, 15, tzinfo=TZ)
    sunset = datetime(2026, 12, 21, 16, 30, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(cloud_cover_pct=90.0, sunrise_dt=sunrise, sunset_dt=sunset)
    outcome = gate.evaluate(soc_pct=40.0, battery_fresh=True, forecast=forecast)
    assert outcome == OUTCOME_DISABLED_FOR_DAY
    assert gate.disabled is True
    assert gate.reason == "insufficient_solar_expected"
    # Persistence check: a fresh gate loaded from same file sees disabled=True
    cfg = _cfg()
    fresh = WeatherGate(state_manager=sm, timezone_str="America/New_York", config_provider=lambda: cfg)
    assert fresh.disabled is True
    assert fresh.evaluated_date == date(2026, 12, 21)


# ----------------------------------------------------------------------
# State machine: midnight reset
# ----------------------------------------------------------------------


def test_midnight_reset_clears_disabled_and_rearms(tmp_path, monkeypatch):
    gate, _ = _make_gate(tmp_path)

    # Step 1: disable on day 1
    now1 = datetime(2026, 12, 21, 7, 0, tzinfo=TZ)
    sunrise1 = datetime(2026, 12, 21, 7, 15, tzinfo=TZ)
    sunset1 = datetime(2026, 12, 21, 16, 30, tzinfo=TZ)
    _freeze_now(monkeypatch, now1)
    forecast1 = _forecast(cloud_cover_pct=95.0, sunrise_dt=sunrise1, sunset_dt=sunset1)
    gate.evaluate(soc_pct=40.0, battery_fresh=True, forecast=forecast1)
    assert gate.disabled is True
    assert gate.evaluated_date == date(2026, 12, 21)

    # Step 2: first tick after midnight on day 2 -> reset
    now2 = datetime(2026, 12, 22, 0, 5, tzinfo=TZ)
    _freeze_now(monkeypatch, now2)
    # forecast still references prior day; the reset doesn't depend on it
    forecast2 = _forecast(cloud_cover_pct=95.0, sunrise_dt=sunrise1, sunset_dt=sunset1)
    outcome = gate.evaluate(soc_pct=40.0, battery_fresh=True, forecast=forecast2)
    assert outcome == OUTCOME_MIDNIGHT_RESET
    assert gate.disabled is False

    # Step 3: same day 2 during pre-sunrise window -> fresh eval allowed
    now3 = datetime(2026, 12, 22, 7, 0, tzinfo=TZ)
    sunrise3 = datetime(2026, 12, 22, 7, 15, tzinfo=TZ)
    sunset3 = datetime(2026, 12, 22, 16, 30, tzinfo=TZ)
    _freeze_now(monkeypatch, now3)
    forecast3 = _forecast(cloud_cover_pct=10.0, sunrise_dt=sunrise3, sunset_dt=sunset3)
    outcome3 = gate.evaluate(soc_pct=70.0, battery_fresh=True, forecast=forecast3)
    assert outcome3 == OUTCOME_KEPT_ENABLED
    assert gate.evaluated_date == date(2026, 12, 22)


# ----------------------------------------------------------------------
# State machine: recovery rule
# ----------------------------------------------------------------------


def test_recovery_with_time_remaining_lifts_gate(tmp_path, monkeypatch):
    gate, _ = _make_gate(tmp_path)
    # Pre-disable the gate by directly persisting state to simulate a prior eval.
    gate._set_state(disabled=True, reason="insufficient_solar_expected")
    gate.evaluated_date = date(2026, 12, 21)
    gate._persist_all()

    # Mid-morning on the same disabled day. SOC has climbed to 90%, sunset is
    # 4 hours away (>= 3.0h threshold) -> recover.
    now = datetime(2026, 12, 21, 12, 0, tzinfo=TZ)
    sunset = datetime(2026, 12, 21, 16, 30, tzinfo=TZ)
    sunrise = datetime(2026, 12, 21, 7, 15, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(cloud_cover_pct=20.0, sunrise_dt=sunrise, sunset_dt=sunset)
    outcome = gate.evaluate(soc_pct=92.0, battery_fresh=True, forecast=forecast)
    assert outcome == OUTCOME_RECOVERED
    assert gate.disabled is False
    assert gate.reason == "recovered_soc_in_time"


def test_recovery_too_late_in_day_stays_disabled(tmp_path, monkeypatch):
    gate, _ = _make_gate(tmp_path)
    gate._set_state(disabled=True, reason="insufficient_solar_expected")
    gate.evaluated_date = date(2026, 12, 21)
    gate._persist_all()

    # SOC reaches 92% but sunset is only 1 hour away -> too late
    now = datetime(2026, 12, 21, 15, 30, tzinfo=TZ)
    sunset = datetime(2026, 12, 21, 16, 30, tzinfo=TZ)
    sunrise = datetime(2026, 12, 21, 7, 15, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(cloud_cover_pct=20.0, sunrise_dt=sunrise, sunset_dt=sunset)
    outcome = gate.evaluate(soc_pct=92.0, battery_fresh=True, forecast=forecast)
    assert outcome == OUTCOME_RECOVERY_WINDOW_TOO_SHORT
    assert gate.disabled is True
    assert gate.reason == "recovery_window_too_short"


def test_recovery_blocked_by_stale_battery(tmp_path, monkeypatch):
    gate, _ = _make_gate(tmp_path)
    gate._set_state(disabled=True, reason="insufficient_solar_expected")
    gate.evaluated_date = date(2026, 12, 21)
    gate._persist_all()

    now = datetime(2026, 12, 21, 12, 0, tzinfo=TZ)
    sunset = datetime(2026, 12, 21, 16, 30, tzinfo=TZ)
    sunrise = datetime(2026, 12, 21, 7, 15, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    forecast = _forecast(cloud_cover_pct=20.0, sunrise_dt=sunrise, sunset_dt=sunset)
    outcome = gate.evaluate(soc_pct=92.0, battery_fresh=False, forecast=forecast)
    assert outcome == OUTCOME_BATTERY_STALE
    assert gate.disabled is True


# ----------------------------------------------------------------------
# Master switch
# ----------------------------------------------------------------------


def test_master_switch_off_bypasses_everything(tmp_path, monkeypatch):
    gate, _ = _make_gate(tmp_path, cfg=_cfg(enabled=False))
    now = datetime(2026, 12, 21, 7, 0, tzinfo=TZ)
    sunrise = datetime(2026, 12, 21, 7, 15, tzinfo=TZ)
    _freeze_now(monkeypatch, now)

    # Even with terrible forecast we don't disable when the master switch is off.
    forecast = _forecast(cloud_cover_pct=100.0, sunrise_dt=sunrise)
    outcome = gate.evaluate(soc_pct=10.0, battery_fresh=True, forecast=forecast)
    assert outcome == OUTCOME_GATE_DISABLED
    assert gate.disabled is False


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _freeze_now(monkeypatch, when: datetime) -> None:
    """Pin datetime.now(tz) inside weather_gate to a fixed value."""

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return when.replace(tzinfo=None)
            return when.astimezone(tz)

    monkeypatch.setattr(wg, "datetime", _FrozenDatetime)
