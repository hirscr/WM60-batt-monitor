"""Pure helpers for daily PV-energy integration.

Extracted out of services/pv_prediction_logger.py so the math is unit-testable
without touching the file system or a background thread.

Two public functions:

    parse_battery_row(row, tz)
        Convert one row of eg4_battery_log.csv into a (datetime, watts) tuple
        or None when the row is malformed. Negative power values are clamped
        to 0; None power is treated as 0; bad timestamps are rejected.

    trapezoidal_kwh(samples, gap_threshold_sec)
        Trapezoidal integration of (datetime, watts) samples into kWh. Gaps
        longer than gap_threshold_sec discard that pair's contribution
        (one long outage must not dominate the daily total). Samples are
        assumed to be sorted ascending; the integration is robust to a single
        unsorted pair (it's just skipped via the gap rule).

Both functions are deliberately small and side-effect-free.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo


# Gap-discard policy: any sample interval greater than this many seconds is
# treated as an outage; the pair contributes 0 kWh to the daily integral.
# 30 minutes catches long EG4 portal outages without sacrificing normal
# 10-minute logging cadence.
DEFAULT_GAP_THRESHOLD_SEC = 1800.0


def parse_battery_row(row: dict, tz: Optional[ZoneInfo] = None) -> Optional[Tuple[datetime, float]]:
    """Parse one row of eg4_battery_log.csv.

    Returns (timestamp_aware_datetime, watts) on success, or None on any
    parse failure. Power-value rules:
      - missing / empty string / None -> 0.0
      - negative -> 0.0 (clamp; PV harvest is never negative)
      - non-numeric string -> None (row is malformed)
      - numeric -> float(value)

    Timestamp rules:
      - column "ts" must be present and parseable by datetime.fromisoformat
      - if the parsed datetime is naive, attach the supplied tz (UTC fallback
        when tz is None) — matches the project's DataLoader behavior
    """
    ts_str = row.get("ts")
    if not ts_str or not isinstance(ts_str, str):
        return None
    try:
        # Tolerate the trailing Z that some upstream tooling emits.
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz or ZoneInfo("UTC"))

    raw = row.get("pv_power_w")
    if raw is None or raw == "":
        watts = 0.0
    else:
        try:
            watts = float(raw)
        except (TypeError, ValueError):
            return None
        if watts < 0.0:
            watts = 0.0
    return (dt, watts)


def trapezoidal_kwh(
    samples: Iterable[Tuple[datetime, float]],
    gap_threshold_sec: float = DEFAULT_GAP_THRESHOLD_SEC,
) -> float:
    """Integrate (timestamp, watts) samples to kWh via the trapezoidal rule.

    Skips any adjacent pair whose timestamp delta exceeds gap_threshold_sec
    so a single multi-hour outage doesn't dominate the daily total. Samples
    must be tz-aware datetimes. A single-sample (or empty) input integrates
    to 0 kWh.

    Returns: kWh as a non-negative float.
    """
    pairs: List[Tuple[datetime, float]] = list(samples)
    if len(pairs) < 2:
        return 0.0

    total_watt_seconds = 0.0
    for i in range(len(pairs) - 1):
        t0, w0 = pairs[i]
        t1, w1 = pairs[i + 1]
        dt_sec = (t1 - t0).total_seconds()
        if dt_sec <= 0:
            # Out-of-order or duplicate timestamp; skip the contribution.
            continue
        if dt_sec > gap_threshold_sec:
            # Outage: discard this pair's contribution per project policy.
            continue
        total_watt_seconds += 0.5 * (w0 + w1) * dt_sec

    return total_watt_seconds / 3_600_000.0
