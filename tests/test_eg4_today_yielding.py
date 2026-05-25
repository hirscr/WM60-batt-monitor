"""Tests for EG4Client.get_today_yielding_kwh_blocking and its validator.

The portal reports todayYielding in tenths of kWh on the inverter Energy
endpoint. PVPredictionLogger uses this value as the primary source for
actual_kwh once per day at the post-sunset trigger.

The pure validator/conversion function _validate_today_yielding is the
load-bearing piece of logic. Covering it directly avoids spinning up the
event loop and the EG4 library for unit tests.

The blocking method itself is exercised for the "no loop running" guard
path; the network-dependent path is covered by integration tests at
deploy time (and by injecting a fake EG4 client in PVPredictionLogger
tests in test_pv_prediction_logger_eg4_source.py).
"""
from __future__ import annotations

import pytest

from eg4_client import EG4Client, _validate_today_yielding


# ---------------------------------------------------------------------
# _validate_today_yielding — pure conversion + validation
# ---------------------------------------------------------------------

def test_validate_converts_tenths_to_kwh_positive():
    """1234 (tenths of kWh) -> 123.4 kWh."""
    assert _validate_today_yielding(1234) == pytest.approx(123.4, abs=1e-9)


def test_validate_converts_tenths_to_kwh_float_input():
    """Floats are accepted too; the library reports ints in practice but
    we don't assume."""
    assert _validate_today_yielding(500.0) == pytest.approx(50.0, abs=1e-9)


def test_validate_zero_is_valid_and_returns_zero_kwh():
    """A fully cloudy / overnight reading is legitimately 0. Must not
    be treated as missing data — that's a load-bearing rule for the
    PVPredictionLogger fallback decision."""
    result = _validate_today_yielding(0)
    assert result == 0.0
    assert result is not None  # explicit: 0 is a value, not None


def test_validate_zero_float_is_valid():
    assert _validate_today_yielding(0.0) == 0.0
    assert _validate_today_yielding(0.0) is not None


def test_validate_string_zero_is_valid():
    """The library may marshal numeric fields as strings; cover that path."""
    assert _validate_today_yielding("0") == 0.0


def test_validate_string_positive_converts():
    assert _validate_today_yielding("250") == pytest.approx(25.0, abs=1e-9)


def test_validate_none_returns_none():
    assert _validate_today_yielding(None) is None


def test_validate_missing_attribute_returns_none():
    """getattr(energy, 'todayYielding', None) returns None when missing.
    Validator should propagate that as None."""
    assert _validate_today_yielding(None) is None


def test_validate_non_numeric_returns_none():
    assert _validate_today_yielding("not_a_number") is None
    assert _validate_today_yielding(object()) is None


def test_validate_empty_string_returns_none():
    assert _validate_today_yielding("") is None


def test_validate_negative_returns_none():
    """Negative readings are impossible; reject them rather than feed a
    bogus value into the prediction log."""
    assert _validate_today_yielding(-10) is None
    assert _validate_today_yielding(-0.1) is None
    assert _validate_today_yielding("-5") is None


# ---------------------------------------------------------------------
# EG4Client.get_today_yielding_kwh_blocking — guard paths
# ---------------------------------------------------------------------

def test_blocking_method_returns_none_when_loop_not_running(monkeypatch):
    """The post-sunset trigger may fire before the EG4 background loop
    is ready (e.g. fresh boot, or after a stop). The method must return
    None — never raise — so the caller falls back to CSV."""
    monkeypatch.setenv("EG4_USER", "test_user")
    monkeypatch.setenv("EG4_PASS", "test_pass")
    client = EG4Client()
    # _loop starts as None until start() is called and the thread spins up.
    assert client._loop is None
    assert client.get_today_yielding_kwh_blocking() is None
