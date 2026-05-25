"""Tests for PVPredictionLogger's source-selection between EG4 and CSV.

The post-sunset trigger calls _resolve_actual_kwh which must:
  - prefer EG4 todayYielding when the get_eg4_client callback returns a
    client whose get_today_yielding_kwh_blocking() returns a float
  - treat 0.0 as a real reading and NOT fall back to CSV (a fully cloudy
    day legitimately yields 0; the existing CSV integration could read
    "0 W constant" and either return 0 or, with sensor noise, a small
    positive value that would corrupt the calibration log)
  - fall back to CSV trapezoidal integration when the EG4 call returns
    None for any reason (callback missing, client None, loop not running,
    validation reject, exception)
  - tolerate embedded NUL bytes in eg4_battery_log.csv so a single
    corrupted line (Pi power loss mid-flush) does not abort the entire
    day's integration

These cover the GOAL bullets directly and they exercise the new
constructor parameter `get_eg4_client`.
"""
from __future__ import annotations

import csv
import os
from datetime import date, datetime, timedelta, timezone

import pytest

from services.pv_prediction_logger import PVPredictionLogger
from utils.state_manager import StateManager


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

class _FakeEG4Client:
    """Minimal stand-in for EG4Client.get_today_yielding_kwh_blocking().

    Returns whatever the test configures, including None to simulate the
    "not available, fall back to CSV" path.
    """

    def __init__(self, value):
        self._value = value
        self.calls = 0

    def get_today_yielding_kwh_blocking(self, timeout: float = 30.0):
        self.calls += 1
        return self._value


def _make_logger(tmp_path, *, get_eg4_client=None):
    state_path = str(tmp_path / "wm_state.json")
    sm = StateManager(path=state_path)
    battery_path = str(tmp_path / "miner_logs" / "eg4_battery_log.csv")
    prediction_path = str(tmp_path / "miner_logs" / "pv_prediction_log.csv")
    os.makedirs(os.path.dirname(prediction_path), exist_ok=True)
    logger = PVPredictionLogger(
        state_manager=sm,
        weather_service=None,
        battery_log_path=battery_path,
        prediction_log_path=prediction_path,
        timezone_str="America/New_York",
        get_eg4_client=get_eg4_client,
    )
    return logger, battery_path, prediction_path, sm


def _write_battery_csv(path: str, rows: list[tuple[datetime, float]]) -> None:
    """Write a minimal battery CSV with ts and pv_power_w columns."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "pv_power_w"])
        for ts, w in rows:
            writer.writerow([ts.isoformat(), w])


def _write_battery_csv_with_nul(path: str, rows: list[tuple[datetime, float]],
                                 corrupt_row_index: int) -> None:
    """Write a battery CSV where one full line is corrupted with NUL bytes.

    The NULs appear in the middle of an otherwise-well-formed row so a
    naive csv.DictReader call would crash with "_csv.Error: line contains
    NUL"; the hardened reader must strip them and keep going.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lines = ["ts,pv_power_w\n"]
    for idx, (ts, w) in enumerate(rows):
        if idx == corrupt_row_index:
            # Inject NUL bytes mid-line. The whole line will still parse
            # as 'ts,pv_power_w' after the strip (good row) instead of
            # raising on read.
            lines.append(f"{ts.isoformat()}\x00,{w}\x00\n")
        else:
            lines.append(f"{ts.isoformat()},{w}\n")
    with open(path, "w", newline="") as f:
        f.writelines(lines)


# Local tz the logger uses.
TZ_OFFSET = timezone(timedelta(hours=-4))  # America/New_York EDT, close enough
                                            # for samples explicitly stamped EDT.


# ---------------------------------------------------------------------
# Source-selection logic
# ---------------------------------------------------------------------

def test_resolve_prefers_eg4_when_positive(tmp_path):
    """A valid EG4 float beats the CSV fallback even when both exist."""
    fake = _FakeEG4Client(value=42.5)
    logger, battery_path, _, _ = _make_logger(tmp_path, get_eg4_client=lambda: fake)
    # CSV also has data, but EG4 must win.
    t0 = datetime(2026, 5, 20, 12, 0, tzinfo=TZ_OFFSET)
    _write_battery_csv(battery_path, [
        (t0, 5000.0),
        (t0 + timedelta(seconds=10), 5000.0),
    ])
    value, source = logger._resolve_actual_kwh(date(2026, 5, 20))
    assert value == pytest.approx(42.5, abs=1e-9)
    assert source == "eg4_today_yielding"
    assert fake.calls == 1


def test_resolve_uses_eg4_zero_directly_and_does_not_fall_back(tmp_path):
    """0.0 from EG4 is a real reading — not a 'missing value' signal.

    This is the core GOAL bullet: a fully cloudy day must be recorded as
    0.0 actual_kwh. Falling back to CSV here would either still be 0 (best
    case) or a small positive value from sensor noise — both wrong from a
    calibration standpoint because the inverter is the authority.
    """
    fake = _FakeEG4Client(value=0.0)
    logger, battery_path, _, _ = _make_logger(tmp_path, get_eg4_client=lambda: fake)
    # CSV deliberately has nonzero PV samples — if fallback fires, the
    # test fails. This proves the None-vs-zero distinction is preserved.
    t0 = datetime(2026, 5, 20, 12, 0, tzinfo=TZ_OFFSET)
    _write_battery_csv(battery_path, [
        (t0, 5000.0),
        (t0 + timedelta(seconds=10), 5000.0),
    ])
    value, source = logger._resolve_actual_kwh(date(2026, 5, 20))
    assert value == 0.0
    assert source == "eg4_today_yielding"
    assert fake.calls == 1


def test_resolve_falls_back_to_csv_when_eg4_returns_none(tmp_path):
    """None from EG4 means 'unavailable' — CSV integration is the fallback."""
    fake = _FakeEG4Client(value=None)
    logger, battery_path, _, _ = _make_logger(tmp_path, get_eg4_client=lambda: fake)
    # Set up a known CSV integration: 1000 W constant for 20s = 1000*20/3.6e6 kWh.
    t0 = datetime(2026, 5, 20, 12, 0, tzinfo=TZ_OFFSET)
    _write_battery_csv(battery_path, [
        (t0 + timedelta(seconds=i * 10), 1000.0)
        for i in range(3)
    ])
    value, source = logger._resolve_actual_kwh(date(2026, 5, 20))
    expected = (1000.0 * 20) / 3_600_000.0
    assert value == pytest.approx(expected, abs=1e-9)
    assert source == "csv_integration"
    assert fake.calls == 1


def test_resolve_falls_back_to_csv_when_callback_returns_none_client(tmp_path):
    """get_eg4_client may return None during early boot (BatteryService
    not yet started). Must fall back without crashing or raising."""
    logger, battery_path, _, _ = _make_logger(tmp_path, get_eg4_client=lambda: None)
    t0 = datetime(2026, 5, 20, 12, 0, tzinfo=TZ_OFFSET)
    _write_battery_csv(battery_path, [
        (t0 + timedelta(seconds=i * 10), 2000.0)
        for i in range(3)
    ])
    value, source = logger._resolve_actual_kwh(date(2026, 5, 20))
    assert source == "csv_integration"
    assert value > 0


def test_resolve_falls_back_to_csv_when_callback_raises(tmp_path):
    """A raised exception from get_eg4_client must not crash the tick."""
    def bad_callback():
        raise RuntimeError("simulated callback failure")
    logger, battery_path, _, _ = _make_logger(tmp_path, get_eg4_client=bad_callback)
    t0 = datetime(2026, 5, 20, 12, 0, tzinfo=TZ_OFFSET)
    _write_battery_csv(battery_path, [
        (t0 + timedelta(seconds=i * 10), 1500.0)
        for i in range(3)
    ])
    value, source = logger._resolve_actual_kwh(date(2026, 5, 20))
    assert source == "csv_integration"
    assert value >= 0


def test_resolve_falls_back_to_csv_when_eg4_method_raises(tmp_path):
    """The blocking method is documented as not raising, but defend in depth."""
    class _BoomClient:
        def get_today_yielding_kwh_blocking(self, timeout: float = 30.0):
            raise RuntimeError("boom")
    logger, battery_path, _, _ = _make_logger(tmp_path, get_eg4_client=lambda: _BoomClient())
    t0 = datetime(2026, 5, 20, 12, 0, tzinfo=TZ_OFFSET)
    _write_battery_csv(battery_path, [
        (t0 + timedelta(seconds=i * 10), 1000.0)
        for i in range(3)
    ])
    value, source = logger._resolve_actual_kwh(date(2026, 5, 20))
    assert source == "csv_integration"


def test_resolve_falls_back_to_csv_when_callback_missing(tmp_path):
    """Backwards compatibility: a logger constructed without the new
    parameter still works via the CSV-only path."""
    logger, battery_path, _, _ = _make_logger(tmp_path, get_eg4_client=None)
    t0 = datetime(2026, 5, 20, 12, 0, tzinfo=TZ_OFFSET)
    _write_battery_csv(battery_path, [
        (t0 + timedelta(seconds=i * 10), 1000.0)
        for i in range(3)
    ])
    value, source = logger._resolve_actual_kwh(date(2026, 5, 20))
    assert source == "csv_integration"


# ---------------------------------------------------------------------
# CSV NUL-byte tolerance
# ---------------------------------------------------------------------

def test_csv_fallback_tolerates_embedded_nul_bytes(tmp_path):
    """A corrupted row with NULs must not abort the day. Surrounding
    valid rows continue to integrate correctly."""
    fake = _FakeEG4Client(value=None)  # force CSV fallback
    logger, battery_path, _, _ = _make_logger(tmp_path, get_eg4_client=lambda: fake)
    t0 = datetime(2026, 5, 20, 12, 0, tzinfo=TZ_OFFSET)
    rows = [
        (t0 + timedelta(seconds=0), 1000.0),
        (t0 + timedelta(seconds=10), 1000.0),  # corrupt this one
        (t0 + timedelta(seconds=20), 1000.0),
        (t0 + timedelta(seconds=30), 1000.0),
    ]
    _write_battery_csv_with_nul(battery_path, rows, corrupt_row_index=1)
    # The strip should keep the corrupt row parseable as a normal row
    # (its values still make sense after NUL removal). The integration
    # should not crash and should produce a positive kWh value.
    value, source = logger._resolve_actual_kwh(date(2026, 5, 20))
    assert source == "csv_integration"
    # Four valid samples × 1000 W × 30 s span = 30000 Ws = 30000/3.6e6 kWh
    expected = (1000.0 * 30.0) / 3_600_000.0
    assert value == pytest.approx(expected, abs=1e-9)


def test_csv_fallback_does_not_crash_on_pure_nul_garbage(tmp_path):
    """Even an entire line of NULs must not blow up the integration —
    the row is stripped to empty, which DictReader skips."""
    fake = _FakeEG4Client(value=None)
    logger, battery_path, _, _ = _make_logger(tmp_path, get_eg4_client=lambda: fake)
    os.makedirs(os.path.dirname(battery_path), exist_ok=True)
    t0 = datetime(2026, 5, 20, 12, 0, tzinfo=TZ_OFFSET)
    with open(battery_path, "w", newline="") as f:
        f.write("ts,pv_power_w\n")
        f.write(f"{t0.isoformat()},1000\n")
        f.write("\x00\x00\x00\x00\x00\n")
        f.write(f"{(t0 + timedelta(seconds=10)).isoformat()},1000\n")
    value, source = logger._resolve_actual_kwh(date(2026, 5, 20))
    assert source == "csv_integration"
    assert value >= 0  # must not raise and must produce a finite value
