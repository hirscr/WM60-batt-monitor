"""Unit tests for PVPredictionLogger's CSV-handling surface.

These tests cover the persistent-storage primitives plus the tick-level
self-healing policy:

  - schema reconcile archives a legacy header and starts fresh
  - read_recent_rows returns reverse-chronological rows bounded by `days`
  - _append_row is an upsert keyed by date (replaces an existing same-day
    row in place, leaves other rows untouched)
  - the atomic-write pattern leaves no partial file on disk after a
    write completes
  - the tick re-runs after sunset+30min when today's row is incomplete,
    even when last_pv_log_date is already today; it no-ops once the row
    is fully populated
"""
from __future__ import annotations

import csv
import os
from datetime import date, timedelta

import pytest

from services.pv_prediction_logger import CSV_FIELDNAMES, PVPredictionLogger
from utils.state_manager import StateManager


def _make_logger(tmp_path, log_filename="pv_prediction_log.csv"):
    state_path = str(tmp_path / "wm_state.json")
    sm = StateManager(path=state_path)
    battery_path = str(tmp_path / "miner_logs" / "eg4_battery_log.csv")
    prediction_path = str(tmp_path / "miner_logs" / log_filename)
    os.makedirs(os.path.dirname(prediction_path), exist_ok=True)
    logger = PVPredictionLogger(
        state_manager=sm,
        weather_service=None,
        battery_log_path=battery_path,
        prediction_log_path=prediction_path,
        timezone_str="America/New_York",
    )
    return logger, prediction_path, sm


def _write_header(path, header_fields):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header_fields)


def _write_row(logger, day, ctx, actual_kwh):
    """Use the private writer to exercise the append path in isolation."""
    logger._append_row(day, actual_kwh, ctx)


# ----------------------------------------------------------------------
# Schema reconcile
# ----------------------------------------------------------------------


def test_reconcile_keeps_matching_schema(tmp_path):
    logger, path, _ = _make_logger(tmp_path)
    _write_header(path, CSV_FIELDNAMES)
    logger._reconcile_csv_schema()
    # File still exists with the canonical header.
    assert os.path.exists(path)
    with open(path, "r", newline="") as f:
        header = next(csv.reader(f))
    assert header == CSV_FIELDNAMES


def test_reconcile_archives_mismatched_schema(tmp_path):
    logger, path, _ = _make_logger(tmp_path)
    _write_header(path, ["wrong", "schema", "here"])
    logger._reconcile_csv_schema()
    # Original file moved aside; main path is now missing.
    assert not os.path.exists(path)
    # A legacy_* file should exist beside it.
    archives = [
        p for p in os.listdir(os.path.dirname(path))
        if p.startswith("pv_prediction_log_legacy_") and p.endswith(".csv")
    ]
    assert len(archives) == 1


def test_reconcile_noop_on_missing_file(tmp_path):
    logger, path, _ = _make_logger(tmp_path)
    if os.path.exists(path):
        os.remove(path)
    logger._reconcile_csv_schema()  # must not raise
    assert not os.path.exists(path)


# ----------------------------------------------------------------------
# read_recent_rows
# ----------------------------------------------------------------------


def test_read_recent_rows_returns_reverse_chronological(tmp_path):
    logger, _, _ = _make_logger(tmp_path)
    ctx_eg4 = {
        "eg4_today_kwh_raw": 50.0,
        "multiplier_applied": 0.8,
        "expected_kwh_used": 40.0,
        "decision_source": "eg4_predict",
    }
    _write_row(logger, date(2026, 5, 18), ctx_eg4, actual_kwh=42.0)
    _write_row(logger, date(2026, 5, 19), ctx_eg4, actual_kwh=39.5)
    _write_row(logger, date(2026, 5, 20), ctx_eg4, actual_kwh=44.0)

    rows = logger.read_recent_rows(2)
    assert len(rows) == 2
    assert rows[0]["date"] == "2026-05-20"
    assert rows[1]["date"] == "2026-05-19"


def test_read_recent_rows_empty_when_file_missing(tmp_path):
    logger, _, _ = _make_logger(tmp_path)
    assert logger.read_recent_rows(7) == []


def test_read_recent_rows_clamps_to_at_least_one(tmp_path):
    logger, _, _ = _make_logger(tmp_path)
    ctx = {
        "eg4_today_kwh_raw": 50.0, "multiplier_applied": 0.8,
        "expected_kwh_used": 40.0, "decision_source": "eg4_predict",
    }
    _write_row(logger, date(2026, 5, 19), ctx, actual_kwh=39.5)
    # days=0 must not return zero rows; we clamp to at least 1.
    rows = logger.read_recent_rows(0)
    assert len(rows) == 1


# ----------------------------------------------------------------------
# Idempotent append
# ----------------------------------------------------------------------


def test_append_is_idempotent_within_a_day(tmp_path):
    """Writing today's row twice in a row must produce only one entry."""
    logger, path, _ = _make_logger(tmp_path)
    ctx = {
        "eg4_today_kwh_raw": 50.0, "multiplier_applied": 0.8,
        "expected_kwh_used": 40.0, "decision_source": "eg4_predict",
    }
    today = date(2026, 5, 20)
    _write_row(logger, today, ctx, actual_kwh=42.0)
    _write_row(logger, today, ctx, actual_kwh=42.0)  # should be a no-op

    rows = logger.read_recent_rows(7)
    assert len(rows) == 1
    assert rows[0]["date"] == today.isoformat()


def test_append_writes_ratio_when_raw_is_positive(tmp_path):
    """ratio_actual_to_eg4_raw should populate when eg4_today_kwh_raw > 0."""
    logger, _, _ = _make_logger(tmp_path)
    ctx = {
        "eg4_today_kwh_raw": 50.0, "multiplier_applied": 0.8,
        "expected_kwh_used": 40.0, "decision_source": "eg4_predict",
    }
    _write_row(logger, date(2026, 5, 20), ctx, actual_kwh=40.0)
    rows = logger.read_recent_rows(1)
    ratio_str = rows[0]["ratio_actual_to_eg4_raw"]
    assert ratio_str != ""
    assert float(ratio_str) == pytest.approx(0.8, abs=1e-3)


def test_append_blanks_ratio_when_raw_is_none(tmp_path):
    """ratio is empty when there is no EG4 raw value to divide by."""
    logger, _, _ = _make_logger(tmp_path)
    ctx = {
        "eg4_today_kwh_raw": None, "multiplier_applied": None,
        "expected_kwh_used": 30.0, "decision_source": "solar_model_fallback",
    }
    _write_row(logger, date(2026, 5, 20), ctx, actual_kwh=35.0)
    rows = logger.read_recent_rows(1)
    assert rows[0]["ratio_actual_to_eg4_raw"] == ""


def test_append_blanks_ratio_when_raw_is_zero(tmp_path):
    """ratio is empty when raw is 0 (division would explode)."""
    logger, _, _ = _make_logger(tmp_path)
    ctx = {
        "eg4_today_kwh_raw": 0.0, "multiplier_applied": 0.8,
        "expected_kwh_used": 0.0, "decision_source": "eg4_predict",
    }
    _write_row(logger, date(2026, 5, 20), ctx, actual_kwh=5.0)
    rows = logger.read_recent_rows(1)
    assert rows[0]["ratio_actual_to_eg4_raw"] == ""


def test_append_emits_canonical_header_on_first_write(tmp_path):
    logger, path, _ = _make_logger(tmp_path)
    if os.path.exists(path):
        os.remove(path)
    ctx = {
        "eg4_today_kwh_raw": 50.0, "multiplier_applied": 0.8,
        "expected_kwh_used": 40.0, "decision_source": "eg4_predict",
    }
    _write_row(logger, date(2026, 5, 20), ctx, actual_kwh=42.0)
    with open(path, "r", newline="") as f:
        header = next(csv.reader(f))
    assert header == CSV_FIELDNAMES


# ----------------------------------------------------------------------
# Upsert semantics
# ----------------------------------------------------------------------


def test_upsert_replaces_existing_same_day_row_in_place(tmp_path):
    """A second write for the same date must overwrite the existing
    row's non-key fields, NOT append a duplicate. Other dates' rows
    must remain untouched. The header is preserved.

    This is the core motivator for this rewrite — a row written before
    the gate finished evaluating (blank gate-context columns) must be
    upgrade-able when the gate values become available later that day.
    """
    logger, path, _ = _make_logger(tmp_path)
    ctx_a = {
        "eg4_today_kwh_raw": 50.0, "multiplier_applied": 0.8,
        "expected_kwh_used": 40.0, "decision_source": "eg4_predict",
    }
    _write_row(logger, date(2026, 5, 18), ctx_a, actual_kwh=42.0)
    _write_row(logger, date(2026, 5, 19), ctx_a, actual_kwh=39.5)
    # Now write a SECOND row for May 19 with completely different
    # context — the upsert must replace fields in place, not append.
    ctx_b = {
        "eg4_today_kwh_raw": 11.1, "multiplier_applied": 0.9,
        "expected_kwh_used": 9.99, "decision_source": "eg4_predict",
    }
    _write_row(logger, date(2026, 5, 19), ctx_b, actual_kwh=12.34)

    # Read raw — exactly two data rows, in original insertion order.
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        header = reader.fieldnames

    assert header == CSV_FIELDNAMES
    assert len(rows) == 2

    # May 18 untouched.
    assert rows[0]["date"] == "2026-05-18"
    assert float(rows[0]["actual_kwh"]) == pytest.approx(42.0, abs=1e-3)
    assert float(rows[0]["eg4_today_kwh_raw"]) == pytest.approx(50.0, abs=1e-3)

    # May 19 fully replaced with the new context.
    assert rows[1]["date"] == "2026-05-19"
    assert float(rows[1]["actual_kwh"]) == pytest.approx(12.34, abs=1e-3)
    assert float(rows[1]["eg4_today_kwh_raw"]) == pytest.approx(11.1, abs=1e-3)
    assert float(rows[1]["multiplier_applied"]) == pytest.approx(0.9, abs=1e-3)
    assert float(rows[1]["expected_kwh_used"]) == pytest.approx(9.99, abs=1e-3)
    assert rows[1]["decision_source"] == "eg4_predict"
    # Ratio recomputed from the new values.
    assert float(rows[1]["ratio_actual_to_eg4_raw"]) == pytest.approx(
        12.34 / 11.1, abs=1e-3
    )


def test_upsert_keeps_row_position_for_replaced_day(tmp_path):
    """An in-place replacement must NOT reorder rows. The replaced row
    stays in its original position (sandwiched between other dates).
    """
    logger, path, _ = _make_logger(tmp_path)
    ctx = {
        "eg4_today_kwh_raw": 50.0, "multiplier_applied": 0.8,
        "expected_kwh_used": 40.0, "decision_source": "eg4_predict",
    }
    _write_row(logger, date(2026, 5, 18), ctx, actual_kwh=42.0)
    _write_row(logger, date(2026, 5, 19), ctx, actual_kwh=39.5)
    _write_row(logger, date(2026, 5, 20), ctx, actual_kwh=44.0)
    # Replace the MIDDLE row.
    _write_row(logger, date(2026, 5, 19), ctx, actual_kwh=12.34)

    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["date"] for r in rows] == ["2026-05-18", "2026-05-19", "2026-05-20"]
    assert float(rows[1]["actual_kwh"]) == pytest.approx(12.34, abs=1e-3)


# ----------------------------------------------------------------------
# Atomic-write pattern
# ----------------------------------------------------------------------


def test_atomic_write_leaves_no_temp_file_on_disk(tmp_path):
    """After a successful write, no .pv_prediction_log.*.tmp sibling
    should be left behind. The temp file is created in the same dir
    as the target so the rename stays on one filesystem.
    """
    logger, path, _ = _make_logger(tmp_path)
    ctx = {
        "eg4_today_kwh_raw": 50.0, "multiplier_applied": 0.8,
        "expected_kwh_used": 40.0, "decision_source": "eg4_predict",
    }
    _write_row(logger, date(2026, 5, 20), ctx, actual_kwh=42.0)

    parent = os.path.dirname(path)
    leftovers = [
        name for name in os.listdir(parent)
        if name.startswith(".pv_prediction_log.") and name.endswith(".tmp")
    ]
    assert leftovers == []


def test_atomic_write_failure_does_not_truncate_existing_file(tmp_path, monkeypatch):
    """If os.replace raises mid-write, the existing CSV must remain
    intact at its previous content. The orphaned temp file is cleaned
    up in the finally block.
    """
    logger, path, _ = _make_logger(tmp_path)
    ctx = {
        "eg4_today_kwh_raw": 50.0, "multiplier_applied": 0.8,
        "expected_kwh_used": 40.0, "decision_source": "eg4_predict",
    }
    # Establish a known-good initial state.
    _write_row(logger, date(2026, 5, 20), ctx, actual_kwh=42.0)
    with open(path, "r", newline="") as f:
        original_bytes = f.read()

    # Force os.replace to blow up on the next write.
    import services.pv_prediction_logger as pv_mod

    def boom(*args, **kwargs):
        raise OSError("simulated disk failure")
    monkeypatch.setattr(pv_mod.os, "replace", boom)

    with pytest.raises(OSError):
        _write_row(logger, date(2026, 5, 21), ctx, actual_kwh=99.0)

    # Existing file must be byte-identical to its pre-attempt state.
    with open(path, "r", newline="") as f:
        post_bytes = f.read()
    assert post_bytes == original_bytes

    # No temp leftovers in the target directory.
    parent = os.path.dirname(path)
    leftovers = [
        name for name in os.listdir(parent)
        if name.startswith(".pv_prediction_log.") and name.endswith(".tmp")
    ]
    assert leftovers == []


# ----------------------------------------------------------------------
# Tick completeness / self-healing
# ----------------------------------------------------------------------


class _FakeWeatherService:
    """Minimal stand-in returning a fixed sunset_dt via get_today_forecast()."""

    def __init__(self, sunset_dt):
        self._sunset_dt = sunset_dt

    def get_today_forecast(self):
        return {"sunset_dt": self._sunset_dt}


class _FakeEG4Client:
    def __init__(self, value):
        self._value = value

    def get_today_yielding_kwh_blocking(self, timeout: float = 30.0):
        return self._value


def _build_logger_for_tick(tmp_path, *, sunset_dt, eg4_value, frozen_now):
    """Construct a logger primed for a deterministic _tick() call.

    Patches the module-level `datetime.now(self._tz)` lookup via
    monkeypatching the class used inside _tick. Returns the logger plus
    the prediction-log path plus the state manager.
    """
    from zoneinfo import ZoneInfo

    state_path = str(tmp_path / "wm_state.json")
    sm = StateManager(path=state_path)
    battery_path = str(tmp_path / "miner_logs" / "eg4_battery_log.csv")
    prediction_path = str(tmp_path / "miner_logs" / "pv_prediction_log.csv")
    os.makedirs(os.path.dirname(prediction_path), exist_ok=True)

    logger = PVPredictionLogger(
        state_manager=sm,
        weather_service=_FakeWeatherService(sunset_dt),
        battery_log_path=battery_path,
        prediction_log_path=prediction_path,
        timezone_str="America/New_York",
        get_eg4_client=lambda: _FakeEG4Client(eg4_value),
    )
    return logger, prediction_path, sm


def test_tick_rewrites_incomplete_row_after_sunset(tmp_path, monkeypatch):
    """The May-25-style scenario: a row already exists for today with
    actual_kwh=0.0 and blank gate-context columns; wm_state has the
    gate fields set and last_pv_log_date already advanced to today.

    The tick (run after sunset+30min) must STILL upsert the row,
    populating actual_kwh from EG4 and the four gate-context columns
    from wm_state. There must remain exactly one row for today.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 25)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 25, 20, 0, tzinfo=tz)
    frozen_now = sunset_dt + timedelta(minutes=45)  # past sunset+30min

    logger, path, sm = _build_logger_for_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        eg4_value=10.5,
        frozen_now=frozen_now,
    )

    # Pre-existing partial row + state matching the live Pi May-25 case.
    ctx_blank = {
        "eg4_today_kwh_raw": None,
        "multiplier_applied": None,
        "expected_kwh_used": None,
        "decision_source": None,
    }
    logger._append_row(today_local, 0.0, ctx_blank)
    sm.save(
        last_pv_log_date=today_local.isoformat(),
        weather_gate_eg4_today_kwh_raw=11.1,
        weather_gate_multiplier_applied=0.9,
        weather_gate_expected_kwh=9.99,
        weather_gate_decision_source="eg4_predict",
        weather_gate_evaluated_date=today_local.isoformat(),
    )

    # Freeze "now" for the tick.
    import services.pv_prediction_logger as pv_mod

    class _FrozenDateTime(_dt):
        @classmethod
        def now(cls, tz_=None):
            if tz_ is not None:
                return frozen_now.astimezone(tz_)
            return frozen_now
    monkeypatch.setattr(pv_mod, "datetime", _FrozenDateTime)

    logger._tick()

    # Exactly one row, fully populated.
    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    assert row["date"] == today_local.isoformat()
    assert float(row["actual_kwh"]) == pytest.approx(10.5, abs=1e-3)
    assert float(row["eg4_today_kwh_raw"]) == pytest.approx(11.1, abs=1e-3)
    assert float(row["multiplier_applied"]) == pytest.approx(0.9, abs=1e-3)
    assert float(row["expected_kwh_used"]) == pytest.approx(9.99, abs=1e-3)
    assert row["decision_source"] == "eg4_predict"
    # Ratio recomputed: 10.5 / 11.1.
    assert float(row["ratio_actual_to_eg4_raw"]) == pytest.approx(
        10.5 / 11.1, abs=1e-3
    )

    # last_pv_log_date set/kept since row is now complete.
    assert sm.load().get("last_pv_log_date") == today_local.isoformat()


def test_tick_noops_when_row_complete_and_flag_set(tmp_path, monkeypatch):
    """Once today's row is fully populated AND last_pv_log_date == today,
    the tick must do nothing — not even touch the disk.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 25)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 25, 20, 0, tzinfo=tz)
    frozen_now = sunset_dt + timedelta(minutes=45)

    logger, path, sm = _build_logger_for_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        eg4_value=99.9,  # if the tick fired, this would be written
        frozen_now=frozen_now,
    )

    ctx_full = {
        "eg4_today_kwh_raw": 11.1,
        "multiplier_applied": 0.9,
        "expected_kwh_used": 9.99,
        "decision_source": "eg4_predict",
    }
    logger._append_row(today_local, 10.5, ctx_full)
    sm.save(
        last_pv_log_date=today_local.isoformat(),
        weather_gate_eg4_today_kwh_raw=11.1,
        weather_gate_multiplier_applied=0.9,
        weather_gate_expected_kwh=9.99,
        weather_gate_decision_source="eg4_predict",
        weather_gate_evaluated_date=today_local.isoformat(),
    )

    # Snapshot file contents BEFORE the tick.
    with open(path, "r", newline="") as f:
        before = f.read()

    # Freeze "now" so we're definitely past sunset+30min.
    import services.pv_prediction_logger as pv_mod

    class _FrozenDateTime(_dt):
        @classmethod
        def now(cls, tz_=None):
            if tz_ is not None:
                return frozen_now.astimezone(tz_)
            return frozen_now
    monkeypatch.setattr(pv_mod, "datetime", _FrozenDateTime)

    logger._tick()

    with open(path, "r", newline="") as f:
        after = f.read()
    # Tick was a no-op; file untouched.
    assert before == after
    # The 99.9 EG4 value must NOT have been written.
    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert float(rows[0]["actual_kwh"]) == pytest.approx(10.5, abs=1e-3)


def test_tick_does_not_advance_flag_when_row_stays_incomplete(tmp_path, monkeypatch):
    """If actual_kwh comes back as 0.0 (cloudy day from EG4), the row
    is "incomplete" by definition. last_pv_log_date must NOT advance —
    the next tick will re-attempt. (Wasteful on a truly cloudy day, but
    bounded by date rollover, and necessary to keep the May-25 fix.)
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 25)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 25, 20, 0, tzinfo=tz)
    frozen_now = sunset_dt + timedelta(minutes=45)

    logger, path, sm = _build_logger_for_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        eg4_value=0.0,
        frozen_now=frozen_now,
    )

    # Fresh state: no last_pv_log_date yet.
    sm.save(
        weather_gate_eg4_today_kwh_raw=11.1,
        weather_gate_multiplier_applied=0.9,
        weather_gate_expected_kwh=9.99,
        weather_gate_decision_source="eg4_predict",
        weather_gate_evaluated_date=today_local.isoformat(),
    )

    import services.pv_prediction_logger as pv_mod

    class _FrozenDateTime(_dt):
        @classmethod
        def now(cls, tz_=None):
            if tz_ is not None:
                return frozen_now.astimezone(tz_)
            return frozen_now
    monkeypatch.setattr(pv_mod, "datetime", _FrozenDateTime)

    logger._tick()

    # Row was written (with 0.0 actual, full gate ctx) but flag NOT advanced.
    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert float(rows[0]["actual_kwh"]) == pytest.approx(0.0, abs=1e-9)
    assert sm.load().get("last_pv_log_date") is None
