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


# ----------------------------------------------------------------------
# Morning prediction-column write (Part 1)
# ----------------------------------------------------------------------

def test_morning_write_populates_only_prediction_columns(tmp_path, monkeypatch):
    """Pre-sunset tick: when the gate has committed today's decision and
    today's row is missing prediction columns, the morning write upserts
    exactly the four prediction columns. actual_kwh stays blank — the
    dashboard's Actual cell must continue to render '—' until sunset+30min.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 27)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    # 09:00 local — well before sunset.
    frozen_now = _dt(2026, 5, 27, 9, 0, tzinfo=tz)

    logger, path, sm = _build_logger_for_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        eg4_value=42.0,  # if a sunset write fires, this would be written
        frozen_now=frozen_now,
    )

    # State: gate has evaluated today, full prediction context present.
    sm.save(
        weather_gate_eg4_today_kwh_raw=11.1,
        weather_gate_multiplier_applied=0.9,
        weather_gate_expected_kwh=9.99,
        weather_gate_decision_source="eg4_predict",
        weather_gate_evaluated_date=today_local.isoformat(),
    )

    # Freeze "now" so the sunset+30min gate definitely has NOT fired.
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
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    assert row["date"] == today_local.isoformat()
    # Prediction columns populated.
    assert float(row["eg4_today_kwh_raw"]) == pytest.approx(11.1, abs=1e-3)
    assert float(row["multiplier_applied"]) == pytest.approx(0.9, abs=1e-3)
    assert float(row["expected_kwh_used"]) == pytest.approx(9.99, abs=1e-3)
    assert row["decision_source"] == "eg4_predict"
    # actual_kwh, ratio, end_reason all blank.
    assert (row.get("actual_kwh") or "").strip() == ""
    assert (row.get("ratio_actual_to_eg4_raw") or "").strip() == ""
    assert (row.get("actual_end_reason") or "").strip() == ""
    # last_pv_log_date NOT advanced — the day isn't done.
    assert sm.load().get("last_pv_log_date") is None


def test_morning_write_is_idempotent(tmp_path, monkeypatch):
    """A second morning-tick on the same day must be a no-op when today's
    row already has decision_source populated. The on-disk file bytes are
    identical before and after.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 27)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    frozen_now = _dt(2026, 5, 27, 9, 0, tzinfo=tz)

    logger, path, sm = _build_logger_for_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        eg4_value=None,
        frozen_now=frozen_now,
    )
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
    with open(path, "rb") as f:
        bytes_after_first = f.read()

    # Mutate state to a value the morning write would normally pick up;
    # if the idempotency guard fails, the row will be rewritten with these
    # new numbers and the file bytes will differ.
    sm.save(
        weather_gate_eg4_today_kwh_raw=999.0,
        weather_gate_multiplier_applied=0.5,
        weather_gate_expected_kwh=500.0,
        weather_gate_decision_source="solar_model_fallback",
        weather_gate_evaluated_date=today_local.isoformat(),
    )

    logger._tick()
    with open(path, "rb") as f:
        bytes_after_second = f.read()

    assert bytes_after_first == bytes_after_second, (
        "Second tick rewrote the row — morning write is not idempotent."
    )
    # And the row content reflects only the first state, not the second.
    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert float(rows[0]["eg4_today_kwh_raw"]) == pytest.approx(11.1, abs=1e-3)


def test_sunset_write_preserves_morning_prediction_columns(tmp_path, monkeypatch):
    """When the morning write has already populated prediction columns
    on disk, the sunset write must NOT overwrite them — even if state
    has been mutated to different values in the meantime.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 27)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    frozen_now = sunset_dt + timedelta(minutes=45)  # past sunset+30min

    logger, path, sm = _build_logger_for_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        eg4_value=10.5,
        frozen_now=frozen_now,
    )

    # Pre-seed the on-disk row as if a morning write had already run.
    ctx_morning = {
        "eg4_today_kwh_raw": 11.1,
        "multiplier_applied": 0.9,
        "expected_kwh_used": 9.99,
        "decision_source": "eg4_predict",
    }
    logger._upsert_row_partial(today_local, {
        "eg4_today_kwh_raw": "11.1000",
        "multiplier_applied": "0.9000",
        "expected_kwh_used": "9.9900",
        "decision_source": "eg4_predict",
    })

    # Mutate state to a different prediction context — sunset write should
    # IGNORE it (fallback fill only applies when the column is blank).
    sm.save(
        weather_gate_eg4_today_kwh_raw=99.0,
        weather_gate_multiplier_applied=0.5,
        weather_gate_expected_kwh=49.5,
        weather_gate_decision_source="solar_model_fallback",
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

    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    # Morning prediction columns preserved verbatim.
    assert float(row["eg4_today_kwh_raw"]) == pytest.approx(11.1, abs=1e-3)
    assert float(row["multiplier_applied"]) == pytest.approx(0.9, abs=1e-3)
    assert float(row["expected_kwh_used"]) == pytest.approx(9.99, abs=1e-3)
    assert row["decision_source"] == "eg4_predict"
    # Sunset write filled actual_kwh + ratio.
    assert float(row["actual_kwh"]) == pytest.approx(10.5, abs=1e-3)
    assert float(row["ratio_actual_to_eg4_raw"]) == pytest.approx(
        10.5 / 11.1, abs=1e-3
    )


def test_no_placeholder_actual_kwh_zero_written_before_sunset(tmp_path, monkeypatch):
    """Pre-sunset ticks must NEVER write actual_kwh — even when the
    underlying _resolve_actual_kwh path would return 0.0. The signature
    bug being fixed: today's row used to appear with date + actual_kwh=0
    and all other columns blank before sunset+30min ever happened.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 27)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    # Midday — well before sunset+30min.
    frozen_now = _dt(2026, 5, 27, 12, 0, tzinfo=tz)

    # EG4 client returns 0.0 — what used to produce the placeholder row.
    logger, path, sm = _build_logger_for_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        eg4_value=0.0,
        frozen_now=frozen_now,
    )

    # No gate context — morning write also won't fire. Tick should be a
    # complete no-op: no row written at all.
    import services.pv_prediction_logger as pv_mod

    class _FrozenDateTime(_dt):
        @classmethod
        def now(cls, tz_=None):
            if tz_ is not None:
                return frozen_now.astimezone(tz_)
            return frozen_now
    monkeypatch.setattr(pv_mod, "datetime", _FrozenDateTime)

    logger._tick()

    # File must not exist (no writes happened). The morning write would
    # have created the file only with a populated gate decision_source.
    if os.path.exists(path):
        with open(path, "r", newline="") as f:
            rows = list(csv.DictReader(f))
        assert rows == [], (
            "Pre-sunset tick wrote rows when it should have been a no-op."
        )

    # Reinforce: even when run repeatedly, no placeholder row appears.
    logger._tick()
    if os.path.exists(path):
        with open(path, "r", newline="") as f:
            rows = list(csv.DictReader(f))
        assert rows == []


# ----------------------------------------------------------------------
# actual_end_reason classifier (Part 2)
# ----------------------------------------------------------------------


def _write_battery_log_with_soc(path: str, samples: list[tuple[datetime, float]]) -> None:
    """Write a battery log with the columns the classifier reads.

    Schema mirrors the production eg4_battery_log.csv: ts + soc_percent
    (plus pv_power_w to keep the parser happy if some other code reads it).
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "soc_percent", "pv_power_w"])
        for ts, soc in samples:
            writer.writerow([ts.isoformat(), soc, 0])


def test_classifier_returns_sunset_when_no_full_window(tmp_path):
    """A typical day: SOC climbs but never sustains >=99% for 30 min
    before sunset. Classification: sunset."""
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo
    from services.pv_prediction_logger import classify_end_reason

    tz = ZoneInfo("America/New_York")
    day = date(2026, 5, 27)
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)

    battery_path = str(tmp_path / "miner_logs" / "eg4_battery_log.csv")
    # SOC climbs from 50% to 95% across the day, never crossing 99%.
    samples = [
        (_dt(2026, 5, 27, h, 0, tzinfo=tz), 50.0 + h * 2.5)
        for h in range(8, 20)
    ]
    _write_battery_log_with_soc(battery_path, samples)

    reason = classify_end_reason(
        battery_log_path=battery_path,
        day=day,
        sunset_dt=sunset_dt,
        tz=tz,
    )
    assert reason == "sunset"


def test_classifier_returns_battery_full_for_long_window_before_sunset(tmp_path):
    """SOC sits at >=99% for >30 minutes ending more than an hour before
    sunset. Classification: battery_full (curtailment)."""
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo
    from services.pv_prediction_logger import classify_end_reason

    tz = ZoneInfo("America/New_York")
    day = date(2026, 5, 27)
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)

    battery_path = str(tmp_path / "miner_logs" / "eg4_battery_log.csv")
    # Climb to 100 by 14:00, stay there through 17:00 (3-hour window),
    # then drop slightly. Window ends 3 hours before sunset.
    samples = []
    for h in range(8, 14):
        samples.append((_dt(2026, 5, 27, h, 0, tzinfo=tz), 70.0))
    # Long contiguous SOC>=99% window.
    for minute_offset in range(0, 180, 10):  # 14:00 through 17:00, every 10 min
        ts = _dt(2026, 5, 27, 14, 0, tzinfo=tz) + timedelta(minutes=minute_offset)
        samples.append((ts, 100.0))
    # Then SOC declines into evening.
    for h in range(17, 20):
        samples.append((_dt(2026, 5, 27, h, 30, tzinfo=tz), 90.0))
    _write_battery_log_with_soc(battery_path, samples)

    reason = classify_end_reason(
        battery_log_path=battery_path,
        day=day,
        sunset_dt=sunset_dt,
        tz=tz,
    )
    assert reason == "battery_full"


def test_classifier_returns_unknown_when_no_battery_log(tmp_path):
    """No file -> unknown. (And: no exception.)"""
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo
    from services.pv_prediction_logger import classify_end_reason

    tz = ZoneInfo("America/New_York")
    day = date(2026, 5, 27)
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    missing_path = str(tmp_path / "miner_logs" / "does_not_exist.csv")

    reason = classify_end_reason(
        battery_log_path=missing_path,
        day=day,
        sunset_dt=sunset_dt,
        tz=tz,
    )
    assert reason == "unknown"


def test_classifier_returns_unknown_when_no_samples_in_day(tmp_path):
    """File exists but has no rows in the target day's window -> unknown."""
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo
    from services.pv_prediction_logger import classify_end_reason

    tz = ZoneInfo("America/New_York")
    day = date(2026, 5, 27)
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)

    battery_path = str(tmp_path / "miner_logs" / "eg4_battery_log.csv")
    # All samples are from a DIFFERENT day.
    samples = [
        (_dt(2026, 5, 20, 12, 0, tzinfo=tz), 100.0),
        (_dt(2026, 5, 20, 13, 0, tzinfo=tz), 100.0),
    ]
    _write_battery_log_with_soc(battery_path, samples)

    reason = classify_end_reason(
        battery_log_path=battery_path,
        day=day,
        sunset_dt=sunset_dt,
        tz=tz,
    )
    assert reason == "unknown"


def test_classifier_treats_full_window_ending_at_sunset_as_sunset(tmp_path):
    """A SOC>=99% window whose end is at (or after) sunset is NOT
    curtailment — the battery sat full because the day was ending,
    which is normal. Must classify as sunset."""
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo
    from services.pv_prediction_logger import classify_end_reason

    tz = ZoneInfo("America/New_York")
    day = date(2026, 5, 27)
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)

    battery_path = str(tmp_path / "miner_logs" / "eg4_battery_log.csv")
    samples = [
        (_dt(2026, 5, 27, 12, 0, tzinfo=tz), 80.0),
        # Long >=99% window ending right at sunset.
        (_dt(2026, 5, 27, 19, 0, tzinfo=tz), 100.0),
        (_dt(2026, 5, 27, 19, 30, tzinfo=tz), 100.0),
        (_dt(2026, 5, 27, 19, 59, tzinfo=tz), 100.0),
        (_dt(2026, 5, 27, 20, 0, tzinfo=tz), 100.0),
    ]
    _write_battery_log_with_soc(battery_path, samples)

    reason = classify_end_reason(
        battery_log_path=battery_path,
        day=day,
        sunset_dt=sunset_dt,
        tz=tz,
    )
    assert reason == "sunset"


# ----------------------------------------------------------------------
# CSV header migration (Part 2)
# ----------------------------------------------------------------------


def test_header_migration_adds_actual_end_reason_column_to_legacy_csv(tmp_path):
    """A legacy 6-column CSV must be upgraded in place to 7 columns,
    preserving every existing row's data. The new column is blank for
    legacy rows."""
    from services.pv_prediction_logger import (
        CSV_FIELDNAMES, _LEGACY_CSV_FIELDNAMES_V1, PVPredictionLogger
    )

    logger, path, _ = _make_logger(tmp_path)
    # Write a legacy CSV: 6-column header + two real data rows.
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_LEGACY_CSV_FIELDNAMES_V1)
        writer.writerow([
            "2026-05-18", "50.0000", "0.8000", "40.0000", "42.0000",
            "0.8400", "eg4_predict",
        ])
        writer.writerow([
            "2026-05-19", "55.0000", "0.8000", "44.0000", "39.5000",
            "0.7182", "eg4_predict",
        ])

    # Reconcile (this is what start() runs).
    logger._reconcile_csv_schema()

    # File still exists at the canonical path.
    assert os.path.exists(path)
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    # Header upgraded.
    assert header == CSV_FIELDNAMES
    # Both legacy rows preserved with the new column blank.
    assert len(rows) == 2
    assert rows[0][:7] == [
        "2026-05-18", "50.0000", "0.8000", "40.0000", "42.0000",
        "0.8400", "eg4_predict",
    ]
    assert rows[0][7] == ""  # actual_end_reason blank for legacy row
    assert rows[1][:7] == [
        "2026-05-19", "55.0000", "0.8000", "44.0000", "39.5000",
        "0.7182", "eg4_predict",
    ]
    assert rows[1][7] == ""


def test_header_migration_writes_canonical_header_for_new_row_after_migration(tmp_path):
    """After legacy migration, a fresh upsert under the new schema must
    work — the canonical header on disk is honored end-to-end."""
    from services.pv_prediction_logger import _LEGACY_CSV_FIELDNAMES_V1, CSV_FIELDNAMES

    logger, path, _ = _make_logger(tmp_path)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_LEGACY_CSV_FIELDNAMES_V1)
        writer.writerow([
            "2026-05-18", "50.0000", "0.8000", "40.0000", "42.0000",
            "0.8400", "eg4_predict",
        ])

    logger._reconcile_csv_schema()
    # Write a new row via the partial upsert with end_reason populated.
    logger._upsert_row_partial(date(2026, 5, 19), {
        "actual_kwh": "44.0000",
        "actual_end_reason": "battery_full",
    })

    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        header = reader.fieldnames

    assert header == CSV_FIELDNAMES
    assert len(rows) == 2
    # Legacy row untouched.
    assert rows[0]["date"] == "2026-05-18"
    assert rows[0]["actual_end_reason"] == ""
    # New row written through with end_reason.
    assert rows[1]["date"] == "2026-05-19"
    assert rows[1]["actual_kwh"] == "44.0000"
    assert rows[1]["actual_end_reason"] == "battery_full"


# ----------------------------------------------------------------------
# Start-of-day battery energy capture (this change)
# ----------------------------------------------------------------------


def _build_logger_for_start_energy_tick(
    tmp_path,
    *,
    sunset_dt,
    frozen_now,
    eg4_value=None,
    battery_status=None,
    battery_is_fresh=True,
    battery_capacity_kwh=75.0,
):
    """Construct a logger wired with battery callbacks for start-of-day tests.

    battery_status is the dict returned by the get_battery_status callback.
    Pass None to simulate "no snapshot available" (empty dict). Pass
    battery_is_fresh=False to simulate stale telemetry. Pass
    battery_capacity_kwh=None to simulate "no rated capacity configured".
    """
    from utils.state_manager import StateManager

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
        get_battery_status=lambda: (battery_status or {}),
        get_battery_is_fresh=lambda: battery_is_fresh,
        get_battery_capacity_kwh=lambda: battery_capacity_kwh,
    )
    return logger, prediction_path, sm


def test_morning_write_captures_start_soc_and_kwh_when_fresh(tmp_path, monkeypatch):
    """When SOC is fresh, both start columns are written together with the
    prediction columns in a single upsert. kWh derives from soc/100 * capacity.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 27)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    frozen_now = _dt(2026, 5, 27, 5, 30, tzinfo=tz)  # pre-sunrise

    logger, path, sm = _build_logger_for_start_energy_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        frozen_now=frozen_now,
        battery_status={"soc_percent": 42.5},
        battery_is_fresh=True,
        battery_capacity_kwh=75.0,
    )
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

    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    assert row["date"] == today_local.isoformat()
    # Prediction columns written.
    assert float(row["eg4_today_kwh_raw"]) == pytest.approx(11.1, abs=1e-3)
    assert row["decision_source"] == "eg4_predict"
    # Start-of-day columns also written in the same upsert.
    assert float(row["start_soc_pct"]) == pytest.approx(42.5, abs=1e-3)
    expected_kwh = 42.5 / 100.0 * 75.0
    assert float(row["start_battery_kwh"]) == pytest.approx(expected_kwh, abs=1e-3)


def test_morning_write_leaves_start_columns_blank_when_stale(tmp_path, monkeypatch):
    """When the battery freshness gate is False, both start columns must
    be blank for that day — never synthesise a stale value. Prediction
    columns are still populated.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 27)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    frozen_now = _dt(2026, 5, 27, 5, 30, tzinfo=tz)

    logger, path, sm = _build_logger_for_start_energy_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        frozen_now=frozen_now,
        battery_status={"soc_percent": 88.8},  # would be a valid SOC if fresh
        battery_is_fresh=False,                 # but freshness gate says no
        battery_capacity_kwh=75.0,
    )
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

    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    # Prediction columns present.
    assert row["decision_source"] == "eg4_predict"
    # Start columns blank.
    assert (row.get("start_soc_pct") or "").strip() == ""
    assert (row.get("start_battery_kwh") or "").strip() == ""


def test_morning_write_leaves_start_columns_blank_when_soc_none(tmp_path, monkeypatch):
    """When soc_percent is None (e.g. battery dict has the key but value
    not yet populated), both start columns must be blank.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 27)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    frozen_now = _dt(2026, 5, 27, 5, 30, tzinfo=tz)

    logger, path, sm = _build_logger_for_start_energy_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        frozen_now=frozen_now,
        battery_status={"soc_percent": None},
        battery_is_fresh=True,
        battery_capacity_kwh=75.0,
    )
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

    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    assert (row.get("start_soc_pct") or "").strip() == ""
    assert (row.get("start_battery_kwh") or "").strip() == ""


def test_morning_write_start_columns_idempotent(tmp_path, monkeypatch):
    """A second morning-write tick on the same day must NOT change the
    captured start values, even if the live SOC has drifted in the
    meantime. The morning write only fires once per day.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 27)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    frozen_now = _dt(2026, 5, 27, 5, 30, tzinfo=tz)

    # Start the first tick with SOC=42.5.
    battery_snapshot = {"soc_percent": 42.5}
    logger, path, sm = _build_logger_for_start_energy_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        frozen_now=frozen_now,
        battery_status=battery_snapshot,
        battery_is_fresh=True,
        battery_capacity_kwh=75.0,
    )
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
    with open(path, "rb") as f:
        bytes_after_first = f.read()

    # Drift the live SOC and tick again — should be a no-op because the
    # decision_source on disk is already populated (morning write idempotency).
    battery_snapshot["soc_percent"] = 99.9

    logger._tick()
    with open(path, "rb") as f:
        bytes_after_second = f.read()

    assert bytes_after_first == bytes_after_second
    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    # SOC frozen at first-tick value, not the drifted 99.9.
    assert float(rows[0]["start_soc_pct"]) == pytest.approx(42.5, abs=1e-3)


def test_morning_write_kwh_blank_when_capacity_unknown(tmp_path, monkeypatch):
    """When capacity_kwh callable returns None, start_battery_kwh is
    blank but start_soc_pct is still captured. The SOC column is the
    authoritative one; kWh is derived context.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 27)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    frozen_now = _dt(2026, 5, 27, 5, 30, tzinfo=tz)

    logger, path, sm = _build_logger_for_start_energy_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        frozen_now=frozen_now,
        battery_status={"soc_percent": 60.0},
        battery_is_fresh=True,
        battery_capacity_kwh=None,
    )
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

    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    row = rows[0]
    assert float(row["start_soc_pct"]) == pytest.approx(60.0, abs=1e-3)
    assert (row.get("start_battery_kwh") or "").strip() == ""


def test_morning_write_kwh_blank_when_capacity_non_positive(tmp_path, monkeypatch):
    """Capacity values <= 0 are treated as 'unknown' — start_battery_kwh
    blank, start_soc_pct captured.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    today_local = date(2026, 5, 27)
    tz = ZoneInfo("America/New_York")
    sunset_dt = _dt(2026, 5, 27, 20, 0, tzinfo=tz)
    frozen_now = _dt(2026, 5, 27, 5, 30, tzinfo=tz)

    logger, path, sm = _build_logger_for_start_energy_tick(
        tmp_path,
        sunset_dt=sunset_dt,
        frozen_now=frozen_now,
        battery_status={"soc_percent": 60.0},
        battery_is_fresh=True,
        battery_capacity_kwh=0.0,
    )
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

    with open(path, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert float(rows[0]["start_soc_pct"]) == pytest.approx(60.0, abs=1e-3)
    assert (rows[0].get("start_battery_kwh") or "").strip() == ""


# ----------------------------------------------------------------------
# CSV header migration — 7/8/9-col legacy schemas (this change)
# ----------------------------------------------------------------------


def test_header_migration_v1_seven_to_ten(tmp_path):
    """V1 = 7-col (no actual_end_reason, no start_*). Migration must keep
    every row, add the three new columns blank, preserve order.
    """
    from services.pv_prediction_logger import (
        CSV_FIELDNAMES, _LEGACY_CSV_FIELDNAMES_V1
    )

    logger, path, _ = _make_logger(tmp_path)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_LEGACY_CSV_FIELDNAMES_V1)
        writer.writerow([
            "2026-05-18", "50.0000", "0.8000", "40.0000",
            "42.0000", "0.8400", "eg4_predict",
        ])
        writer.writerow([
            "2026-05-19", "55.0000", "0.8000", "44.0000",
            "39.5000", "0.7182", "eg4_predict",
        ])

    logger._reconcile_csv_schema()

    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    assert header == CSV_FIELDNAMES
    assert len(rows) == 2
    # Existing values preserved.
    assert rows[0][:7] == [
        "2026-05-18", "50.0000", "0.8000", "40.0000",
        "42.0000", "0.8400", "eg4_predict",
    ]
    # actual_end_reason, start_soc_pct, start_battery_kwh blank.
    assert rows[0][7] == ""
    assert rows[0][8] == ""
    assert rows[0][9] == ""


def test_header_migration_v2_eight_to_ten(tmp_path):
    """V2 = 8-col (has actual_end_reason, no start_*). Migration must
    preserve every existing column, add only the two start_* columns blank.
    """
    from services.pv_prediction_logger import (
        CSV_FIELDNAMES, _LEGACY_CSV_FIELDNAMES_V2
    )

    logger, path, _ = _make_logger(tmp_path)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_LEGACY_CSV_FIELDNAMES_V2)
        writer.writerow([
            "2026-05-18", "50.0000", "0.8000", "40.0000",
            "42.0000", "0.8400", "eg4_predict", "battery_full",
        ])
        writer.writerow([
            "2026-05-19", "55.0000", "0.8000", "44.0000",
            "39.5000", "0.7182", "eg4_predict", "sunset",
        ])

    logger._reconcile_csv_schema()

    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    assert header == CSV_FIELDNAMES
    assert len(rows) == 2
    # Eight existing columns preserved verbatim.
    assert rows[0][:8] == [
        "2026-05-18", "50.0000", "0.8000", "40.0000",
        "42.0000", "0.8400", "eg4_predict", "battery_full",
    ]
    # Two new columns blank.
    assert rows[0][8] == ""
    assert rows[0][9] == ""
    assert rows[1][7] == "sunset"
    assert rows[1][8] == ""
    assert rows[1][9] == ""


def test_header_migration_v3_nine_to_ten(tmp_path):
    """V3 = 9-col (has actual_end_reason + start_soc_pct, missing
    start_battery_kwh). Migration must keep start_soc_pct values and add
    only start_battery_kwh blank.
    """
    from services.pv_prediction_logger import (
        CSV_FIELDNAMES, _LEGACY_CSV_FIELDNAMES_V3
    )

    logger, path, _ = _make_logger(tmp_path)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(_LEGACY_CSV_FIELDNAMES_V3)
        writer.writerow([
            "2026-05-18", "50.0000", "0.8000", "40.0000",
            "42.0000", "0.8400", "eg4_predict", "battery_full", "33.3000",
        ])

    logger._reconcile_csv_schema()

    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    assert header == CSV_FIELDNAMES
    assert len(rows) == 1
    # start_soc_pct preserved.
    assert rows[0][8] == "33.3000"
    # start_battery_kwh blank (the only new column).
    assert rows[0][9] == ""


# ----------------------------------------------------------------------
# read_recent_rows / API passthrough for start columns (this change)
# ----------------------------------------------------------------------


def test_read_recent_rows_includes_start_columns(tmp_path):
    """read_recent_rows must surface start_soc_pct and start_battery_kwh
    so /api/weather/prediction_history can pass them through unchanged.
    """
    logger, path, _ = _make_logger(tmp_path)
    # Use the partial upsert to seed a row with both start columns populated.
    logger._upsert_row_partial(date(2026, 5, 20), {
        "eg4_today_kwh_raw": "50.0000",
        "multiplier_applied": "0.8000",
        "expected_kwh_used": "40.0000",
        "actual_kwh": "42.0000",
        "ratio_actual_to_eg4_raw": "0.8400",
        "decision_source": "eg4_predict",
        "actual_end_reason": "sunset",
        "start_soc_pct": "37.5000",
        "start_battery_kwh": "28.1250",
    })

    rows = logger.read_recent_rows(7)
    assert len(rows) == 1
    r = rows[0]
    assert r["start_soc_pct"] == "37.5000"
    assert r["start_battery_kwh"] == "28.1250"


# ----------------------------------------------------------------------
# Backfill tool (tools/backfill_start_energy.py)
# ----------------------------------------------------------------------


def _seed_battery_log(path: str, samples: list) -> None:
    """Write a minimal eg4_battery_log.csv (ts + soc_percent + pv_power_w).

    samples is a list of (datetime, soc) tuples. pv_power_w is set to 0.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ts", "soc_percent", "pv_power_w"])
        for ts, soc in samples:
            writer.writerow([ts.isoformat(), soc, 0])


def _seed_prediction_log(path: str, rows: list[dict]) -> None:
    """Write pv_prediction_log.csv at the current schema with the given
    rows. Each row should be a dict keyed by CSV_FIELDNAMES."""
    from services.pv_prediction_logger import CSV_FIELDNAMES as FIELDS
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            full = {col: "" for col in FIELDS}
            full.update(row)
            writer.writerow(full)


def _import_backfill_module(tmp_path, monkeypatch):
    """Import tools/backfill_start_energy.py with its PROJECT-relative paths
    rerouted to tmp_path. Patches the module globals after import so the
    real config / log files are never touched.
    """
    import importlib
    # Force fresh import each test so global path patches don't leak.
    import sys as _sys
    if "backfill_start_energy" in _sys.modules:
        del _sys.modules["backfill_start_energy"]
    project_root = os.path.abspath(os.path.join(
        os.path.dirname(__file__), ".."
    ))
    tools_dir = os.path.join(project_root, "tools")
    if tools_dir not in _sys.path:
        _sys.path.insert(0, tools_dir)
    mod = importlib.import_module("backfill_start_energy")

    pred_log = str(tmp_path / "miner_logs" / "pv_prediction_log.csv")
    batt_log = str(tmp_path / "miner_logs" / "eg4_battery_log.csv")
    # Reroute the module's paths to the test sandbox.
    monkeypatch.setattr(mod, "PREDICTION_LOG", pred_log)
    monkeypatch.setattr(mod, "BATTERY_LOG", batt_log)
    return mod, pred_log, batt_log


def _patch_location_and_capacity(monkeypatch, mod, sunrise_dt, capacity_kwh):
    """Bypass YAML loading. Force _load_location_and_capacity to return
    fixed values and _compute_sunrise to return a fixed datetime, so the
    backfill tests are independent of real config files.
    """
    tz = sunrise_dt.tzinfo
    tz_name = str(tz)

    def fake_load():
        return 40.0, -74.0, tz_name, capacity_kwh
    monkeypatch.setattr(mod, "_load_location_and_capacity", fake_load)

    def fake_sunrise(day, lat, lon, tz_name_arg):
        # Build sunrise on the requested day at the same time-of-day as
        # the supplied reference. This lets the test fix one "sunrise"
        # but still pass for whatever date the row carries.
        from datetime import datetime as _dt
        from zoneinfo import ZoneInfo as _ZoneInfo
        return _dt(
            day.year, day.month, day.day,
            sunrise_dt.hour, sunrise_dt.minute, sunrise_dt.second,
            tzinfo=_ZoneInfo(tz_name_arg),
        )
    monkeypatch.setattr(mod, "_compute_sunrise", fake_sunrise)


def test_backfill_uses_row_at_exact_sunrise(tmp_path, monkeypatch):
    """A battery-log row whose timestamp is exactly at sunrise on the
    target day is used. Both columns are written.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("America/New_York")
    sunrise_dt = _dt(2026, 5, 20, 5, 30, 0, tzinfo=tz)

    mod, pred_log, batt_log = _import_backfill_module(tmp_path, monkeypatch)
    _patch_location_and_capacity(monkeypatch, mod, sunrise_dt, capacity_kwh=75.0)

    _seed_battery_log(batt_log, [
        (sunrise_dt, 42.5),
        (sunrise_dt + timedelta(minutes=10), 43.0),
    ])
    _seed_prediction_log(pred_log, [
        {"date": "2026-05-20", "actual_kwh": "30.0"},
    ])

    rc = mod.backfill(dry_run=False)
    assert rc == 0

    with open(pred_log, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert float(rows[0]["start_soc_pct"]) == pytest.approx(42.5, abs=1e-3)
    expected_kwh = 42.5 / 100.0 * 75.0
    assert float(rows[0]["start_battery_kwh"]) == pytest.approx(expected_kwh, abs=1e-3)


def test_backfill_uses_row_within_30_min_after_sunrise(tmp_path, monkeypatch):
    """If no row sits exactly at sunrise, the closest row within the
    window is used. A row 20 minutes after sunrise is well within ±2h.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("America/New_York")
    sunrise_dt = _dt(2026, 5, 20, 5, 30, 0, tzinfo=tz)

    mod, pred_log, batt_log = _import_backfill_module(tmp_path, monkeypatch)
    _patch_location_and_capacity(monkeypatch, mod, sunrise_dt, capacity_kwh=75.0)

    later = sunrise_dt + timedelta(minutes=20)
    _seed_battery_log(batt_log, [(later, 41.0)])
    _seed_prediction_log(pred_log, [
        {"date": "2026-05-20", "actual_kwh": "30.0"},
    ])

    rc = mod.backfill(dry_run=False)
    assert rc == 0

    with open(pred_log, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert float(rows[0]["start_soc_pct"]) == pytest.approx(41.0, abs=1e-3)


def test_backfill_skips_when_no_row_within_window(tmp_path, monkeypatch):
    """A row >2h after sunrise (and no row before) leaves both columns
    blank — the day is unobservable, no synthesised value.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("America/New_York")
    sunrise_dt = _dt(2026, 5, 20, 5, 30, 0, tzinfo=tz)

    mod, pred_log, batt_log = _import_backfill_module(tmp_path, monkeypatch)
    _patch_location_and_capacity(monkeypatch, mod, sunrise_dt, capacity_kwh=75.0)

    # Only a sample 3 hours after sunrise — outside the ±2h window.
    far = sunrise_dt + timedelta(hours=3)
    _seed_battery_log(batt_log, [(far, 50.0)])
    _seed_prediction_log(pred_log, [
        {"date": "2026-05-20", "actual_kwh": "30.0"},
    ])

    rc = mod.backfill(dry_run=False)
    assert rc == 0

    with open(pred_log, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert (rows[0].get("start_soc_pct") or "").strip() == ""
    assert (rows[0].get("start_battery_kwh") or "").strip() == ""


def test_backfill_does_not_use_previous_day_late_night_reading(tmp_path, monkeypatch):
    """A late-night reading from the previous day (well before the window
    around sunrise) must NOT be used. The day is treated as unobservable.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("America/New_York")
    sunrise_dt = _dt(2026, 5, 20, 5, 30, 0, tzinfo=tz)

    mod, pred_log, batt_log = _import_backfill_module(tmp_path, monkeypatch)
    _patch_location_and_capacity(monkeypatch, mod, sunrise_dt, capacity_kwh=75.0)

    # Reading at 22:00 on 5/19 — 7.5 hours before sunrise on 5/20. Far
    # outside the ±2h window.
    prev_night = _dt(2026, 5, 19, 22, 0, 0, tzinfo=tz)
    _seed_battery_log(batt_log, [(prev_night, 88.0)])
    _seed_prediction_log(pred_log, [
        {"date": "2026-05-20", "actual_kwh": "30.0"},
    ])

    rc = mod.backfill(dry_run=False)
    assert rc == 0

    with open(pred_log, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert (rows[0].get("start_soc_pct") or "").strip() == ""


def test_backfill_dry_run_does_not_write(tmp_path, monkeypatch):
    """--dry-run must not touch the prediction log on disk."""
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("America/New_York")
    sunrise_dt = _dt(2026, 5, 20, 5, 30, 0, tzinfo=tz)

    mod, pred_log, batt_log = _import_backfill_module(tmp_path, monkeypatch)
    _patch_location_and_capacity(monkeypatch, mod, sunrise_dt, capacity_kwh=75.0)

    _seed_battery_log(batt_log, [(sunrise_dt, 42.5)])
    _seed_prediction_log(pred_log, [
        {"date": "2026-05-20", "actual_kwh": "30.0"},
    ])

    with open(pred_log, "rb") as f:
        before = f.read()
    rc = mod.backfill(dry_run=True)
    assert rc == 0
    with open(pred_log, "rb") as f:
        after = f.read()
    assert before == after


def test_backfill_skips_rows_already_populated(tmp_path, monkeypatch):
    """A row that already has start_soc_pct must not be re-touched —
    re-running is safe."""
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("America/New_York")
    sunrise_dt = _dt(2026, 5, 20, 5, 30, 0, tzinfo=tz)

    mod, pred_log, batt_log = _import_backfill_module(tmp_path, monkeypatch)
    _patch_location_and_capacity(monkeypatch, mod, sunrise_dt, capacity_kwh=75.0)

    # SOC=42 on disk vs. SOC=80 in the battery log — backfill must NOT
    # overwrite the 42.
    _seed_battery_log(batt_log, [(sunrise_dt, 80.0)])
    _seed_prediction_log(pred_log, [
        {"date": "2026-05-20", "start_soc_pct": "42.0000",
         "start_battery_kwh": "31.5000"},
    ])

    rc = mod.backfill(dry_run=False)
    assert rc == 0

    with open(pred_log, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert float(rows[0]["start_soc_pct"]) == pytest.approx(42.0, abs=1e-3)
    assert float(rows[0]["start_battery_kwh"]) == pytest.approx(31.5, abs=1e-3)


def test_backfill_kwh_blank_when_capacity_unknown(tmp_path, monkeypatch):
    """When config has no usable battery_total_kwh, the tool still writes
    start_soc_pct but leaves start_battery_kwh blank."""
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("America/New_York")
    sunrise_dt = _dt(2026, 5, 20, 5, 30, 0, tzinfo=tz)

    mod, pred_log, batt_log = _import_backfill_module(tmp_path, monkeypatch)
    _patch_location_and_capacity(monkeypatch, mod, sunrise_dt, capacity_kwh=None)

    _seed_battery_log(batt_log, [(sunrise_dt, 42.5)])
    _seed_prediction_log(pred_log, [
        {"date": "2026-05-20", "actual_kwh": "30.0"},
    ])

    rc = mod.backfill(dry_run=False)
    assert rc == 0

    with open(pred_log, "r", newline="") as f:
        rows = list(csv.DictReader(f))
    assert float(rows[0]["start_soc_pct"]) == pytest.approx(42.5, abs=1e-3)
    assert (rows[0].get("start_battery_kwh") or "").strip() == ""
