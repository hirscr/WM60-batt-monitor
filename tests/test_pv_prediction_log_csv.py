"""Unit tests for PVPredictionLogger's CSV-handling surface.

The background thread is NOT exercised here — the tick policy (wait for
sunset, etc.) is straightforward and gets verified at deploy time. These
tests cover the persistent-storage primitives:

  - schema reconcile archives a legacy header and starts fresh
  - read_recent_rows returns reverse-chronological rows bounded by `days`
  - _append_row is idempotent within a day (no duplicate today rows)
"""
from __future__ import annotations

import csv
import os
from datetime import date

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
