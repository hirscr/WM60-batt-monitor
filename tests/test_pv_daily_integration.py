"""Unit tests for utils/pv_integration.py — the trapezoidal-integration math
that converts pv_power_w time series into daily kWh.

Covers:
  - clean integration of evenly-spaced samples
  - None / empty / negative power values handled per spec
  - malformed timestamps reject the row (no crash)
  - gap > 30 min discards the pair's contribution
  - single-sample input → 0 kWh
  - day-boundary respects local timezone
"""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from utils.pv_integration import (
    DEFAULT_GAP_THRESHOLD_SEC,
    parse_battery_row,
    trapezoidal_kwh,
)


TZ_NY = ZoneInfo("America/New_York")


# ----------------------------------------------------------------------
# parse_battery_row
# ----------------------------------------------------------------------


def test_parse_row_with_aware_iso_timestamp():
    row = {"ts": "2026-06-21T12:00:00-04:00", "pv_power_w": "5000"}
    parsed = parse_battery_row(row, tz=TZ_NY)
    assert parsed is not None
    dt, watts = parsed
    assert watts == 5000.0
    assert dt.tzinfo is not None


def test_parse_row_with_naive_timestamp_attaches_tz():
    row = {"ts": "2026-06-21T12:00:00", "pv_power_w": "1000"}
    parsed = parse_battery_row(row, tz=TZ_NY)
    assert parsed is not None
    dt, _ = parsed
    assert dt.tzinfo is not None


def test_parse_row_negative_power_clamped_to_zero():
    row = {"ts": "2026-06-21T12:00:00-04:00", "pv_power_w": "-50"}
    parsed = parse_battery_row(row, tz=TZ_NY)
    assert parsed == (parsed[0], 0.0)


def test_parse_row_none_power_treated_as_zero():
    row = {"ts": "2026-06-21T12:00:00-04:00", "pv_power_w": None}
    parsed = parse_battery_row(row, tz=TZ_NY)
    assert parsed == (parsed[0], 0.0)


def test_parse_row_empty_string_power_treated_as_zero():
    row = {"ts": "2026-06-21T12:00:00-04:00", "pv_power_w": ""}
    parsed = parse_battery_row(row, tz=TZ_NY)
    assert parsed == (parsed[0], 0.0)


def test_parse_row_non_numeric_power_returns_none():
    row = {"ts": "2026-06-21T12:00:00-04:00", "pv_power_w": "not_a_number"}
    assert parse_battery_row(row, tz=TZ_NY) is None


def test_parse_row_bad_timestamp_returns_none():
    row = {"ts": "not-a-timestamp", "pv_power_w": "1000"}
    assert parse_battery_row(row, tz=TZ_NY) is None


def test_parse_row_missing_ts_returns_none():
    row = {"pv_power_w": "1000"}
    assert parse_battery_row(row, tz=TZ_NY) is None


# ----------------------------------------------------------------------
# trapezoidal_kwh
# ----------------------------------------------------------------------


def test_trapezoidal_single_sample_yields_zero():
    """A single point can't integrate. Empty input is also 0."""
    t0 = datetime(2026, 6, 21, 12, 0, tzinfo=TZ_NY)
    assert trapezoidal_kwh([(t0, 1000.0)]) == 0.0
    assert trapezoidal_kwh([]) == 0.0


def test_trapezoidal_constant_power_matches_hand_calculation():
    """1000 W constant for 3600 s = 1.0 kWh exactly.

    Trapezoidal on a flat line is the same as the rectangle rule.
    """
    t0 = datetime(2026, 6, 21, 12, 0, tzinfo=TZ_NY)
    samples = [(t0 + timedelta(seconds=i * 10), 1000.0) for i in range(361)]
    # 360 intervals × 10s × 1000W = 3,600,000 Ws = 1.0 kWh
    kwh = trapezoidal_kwh(samples)
    assert kwh == pytest.approx(1.0, abs=1e-9)


def test_trapezoidal_linear_ramp_matches_hand_calculation():
    """Ramp from 0W to 1000W over 1h. Average power = 500W → 0.5 kWh."""
    t0 = datetime(2026, 6, 21, 12, 0, tzinfo=TZ_NY)
    n = 361  # 10s spacing for 3600s
    samples = []
    for i in range(n):
        ts = t0 + timedelta(seconds=i * 10)
        w = 1000.0 * (i / (n - 1))
        samples.append((ts, w))
    kwh = trapezoidal_kwh(samples)
    assert kwh == pytest.approx(0.5, abs=1e-9)


def test_trapezoidal_long_gap_discards_pair():
    """A 31-minute gap between two samples must NOT contribute any kWh.

    Without this rule a single multi-hour EG4 outage would push the
    daily total into the stratosphere.
    """
    t0 = datetime(2026, 6, 21, 6, 0, tzinfo=TZ_NY)
    samples = [
        (t0, 5000.0),
        (t0 + timedelta(seconds=1900), 5000.0),  # 31m 40s gap > threshold
    ]
    assert trapezoidal_kwh(samples) == 0.0


def test_trapezoidal_short_gap_contributes_normally():
    """Just under the threshold (29m) still contributes."""
    t0 = datetime(2026, 6, 21, 6, 0, tzinfo=TZ_NY)
    dt_sec = 29 * 60  # 1740s
    samples = [(t0, 4000.0), (t0 + timedelta(seconds=dt_sec), 4000.0)]
    expected = (4000.0 * dt_sec) / 3_600_000.0
    assert trapezoidal_kwh(samples) == pytest.approx(expected, abs=1e-9)


def test_trapezoidal_out_of_order_pair_skipped():
    """Out-of-order timestamps (dt<=0) skip the contribution."""
    t0 = datetime(2026, 6, 21, 12, 0, tzinfo=TZ_NY)
    samples = [
        (t0, 1000.0),
        (t0 - timedelta(seconds=10), 1000.0),  # earlier
    ]
    assert trapezoidal_kwh(samples) == 0.0


# ----------------------------------------------------------------------
# Day-boundary behavior — integration with parse_battery_row
# ----------------------------------------------------------------------


def test_day_boundary_in_local_timezone_separates_samples():
    """Samples at 23:55 EDT and 00:05 EDT belong to different days.

    The test parses both and then a typical filter
    (day_start <= ts_local < day_end) would put them in adjacent days.
    """
    row_day1 = {"ts": "2026-06-21T23:55:00-04:00", "pv_power_w": "100"}
    row_day2 = {"ts": "2026-06-22T00:05:00-04:00", "pv_power_w": "100"}
    parsed1 = parse_battery_row(row_day1, tz=TZ_NY)
    parsed2 = parse_battery_row(row_day2, tz=TZ_NY)
    assert parsed1 is not None and parsed2 is not None
    local1 = parsed1[0].astimezone(TZ_NY)
    local2 = parsed2[0].astimezone(TZ_NY)
    assert local1.date() == datetime(2026, 6, 21).date()
    assert local2.date() == datetime(2026, 6, 22).date()
    assert local1.date() != local2.date()


def test_full_pipeline_skips_malformed_rows_without_aborting():
    """A bad row in the middle of the day must not poison the rest of the integration."""
    t0 = datetime(2026, 6, 21, 12, 0, tzinfo=TZ_NY)
    rows = [
        {"ts": (t0 + timedelta(seconds=0)).isoformat(), "pv_power_w": "1000"},
        {"ts": "garbage", "pv_power_w": "1000"},  # skipped
        {"ts": (t0 + timedelta(seconds=10)).isoformat(), "pv_power_w": "1000"},
        {"ts": (t0 + timedelta(seconds=20)).isoformat(), "pv_power_w": "1000"},
    ]
    parsed = []
    for r in rows:
        p = parse_battery_row(r, tz=TZ_NY)
        if p is not None:
            parsed.append(p)
    # 3 valid samples × 1000W with 10s spacing → 20s of 1000W = 1000*20/3.6e6 kWh
    kwh = trapezoidal_kwh(parsed)
    assert kwh == pytest.approx(20.0 * 1000.0 / 3_600_000.0, abs=1e-9)
