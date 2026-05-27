"""One-shot: backfill start_soc_pct and start_battery_kwh in
pv_prediction_log.csv.

Iterates over every row where start_soc_pct is blank. For each such row,
finds the SOC reading in miner_logs/eg4_battery_log.csv whose timestamp
is closest to (but not strictly after) that date's sunrise, and writes
both the SOC and the derived kWh.

Sunrise for historical dates is computed via astral using the latitude,
longitude, and timezone from config.yaml + config.local.yaml — same
source the autocontrol service uses. If sunrise computation fails, the
target timestamp falls back to 05:30 local time on that date.

A ±2 hour search window is enforced around the target time. Rows with no
battery-log sample inside that window are skipped (left blank) and
reported in the summary.

Battery capacity (kWh) is read from weather_gate.battery_total_kwh in
config.yaml / config.local.yaml — the same value the deficit calc uses.
When that config key is missing or non-positive, start_battery_kwh is
left blank; start_soc_pct is still written when the search succeeds.

Usage:

    python3 tools/backfill_start_energy.py            # write
    python3 tools/backfill_start_energy.py --dry-run  # show changes only

Safety: rewrite is atomic (temp-sibling + rename), so a crash or kill
mid-run leaves the existing log untouched. The tool refuses to touch
rows that are already populated (start_soc_pct non-blank), so re-running
it after a partial run is harmless.

This tool is intended to be run manually after deploy. Nothing in the
running service imports it.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import tempfile
from datetime import date, datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML not installed — run `pip install -r requirements.txt` first.")
    sys.exit(2)

try:
    from astral import LocationInfo
    from astral.sun import sun as astral_sun
except ImportError:
    print("ERROR: astral not installed — required for historical sunrise times.")
    sys.exit(2)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

# Importing the service module pulls in only pure helpers — no threads
# or file-system side effects fire at import time.
from services.pv_prediction_logger import CSV_FIELDNAMES  # noqa: E402

PREDICTION_LOG = os.path.join(PROJECT_ROOT, "miner_logs", "pv_prediction_log.csv")
BATTERY_LOG = os.path.join(PROJECT_ROOT, "miner_logs", "eg4_battery_log.csv")
CONFIG_YAML = os.path.join(PROJECT_ROOT, "config.yaml")
CONFIG_LOCAL_YAML = os.path.join(PROJECT_ROOT, "config.local.yaml")

# ±2 hour search window around the target time. Outside this window, we
# consider the day "unobservable" rather than guess from a distant reading.
SEARCH_WINDOW_SEC = 2 * 3600

# Fallback if astral cannot compute sunrise for a date (e.g. polar latitude).
# Empirically close to mid-spring sunrise at the project's mid-latitude install.
_FALLBACK_TARGET = time(5, 30, 0)


def _load_location_and_capacity() -> tuple[float, float, str, Optional[float]]:
    """Read latitude / longitude / timezone / battery_total_kwh from config.

    config.local.yaml overrides config.yaml when both are present. Returns
    (40.0, -74.0, America/New_York, None) when both files are missing.
    """
    lat = 40.0
    lon = -74.0
    tz_name = "America/New_York"
    capacity_kwh: Optional[float] = None
    for path in (CONFIG_YAML, CONFIG_LOCAL_YAML):
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r") as f:
                data = yaml.safe_load(f) or {}
        except Exception as exc:
            print(f"WARNING: cannot parse {path}: {exc}")
            continue
        location = (
            (data.get("autocontrol") or {}).get("location") or {}
        )
        if "latitude" in location:
            lat = float(location["latitude"])
        if "longitude" in location:
            lon = float(location["longitude"])
        if "timezone" in location:
            tz_name = str(location["timezone"])
        wg = data.get("weather_gate") or {}
        if "battery_total_kwh" in wg:
            try:
                val = float(wg["battery_total_kwh"])
                if val > 0:
                    capacity_kwh = val
            except (TypeError, ValueError):
                pass
    return lat, lon, tz_name, capacity_kwh


def _compute_sunrise(day: date, lat: float, lon: float, tz_name: str) -> Optional[datetime]:
    """Compute astronomical sunrise for `day` at the project location.

    Returns None when astral cannot determine sunrise (e.g. polar regions,
    or a malformed location). Callers fall back to a fixed time-of-day.
    """
    tz = ZoneInfo(tz_name)
    try:
        location = LocationInfo("Site", "Region", tz_name, lat, lon)
        s = astral_sun(location.observer, date=day, tzinfo=tz)
        sunrise = s.get("sunrise")
        if isinstance(sunrise, datetime):
            return sunrise
    except Exception as exc:
        print(f"  Sunrise compute failed for {day}: {exc}")
    return None


def _target_time(day: date, lat: float, lon: float, tz_name: str) -> datetime:
    """Return the timestamp at which we'd ideally observe the start-of-day SOC.

    Prefers sunrise (so the SOC is captured after the overnight rest and
    just as PV begins). Falls back to 05:30 local on the same date when
    sunrise cannot be computed.
    """
    tz = ZoneInfo(tz_name)
    sunrise = _compute_sunrise(day, lat, lon, tz_name)
    if sunrise is not None:
        return sunrise
    return datetime.combine(day, _FALLBACK_TARGET, tzinfo=tz)


def _row_needs_backfill(row: dict) -> bool:
    """Eligible iff start_soc_pct is blank.

    start_battery_kwh is treated as derived from start_soc_pct + capacity;
    we never backfill the kWh column without also writing SOC, so the
    presence of SOC is the only gate.
    """
    soc = (row.get("start_soc_pct") or "").strip()
    return not soc


def _nul_stripped_lines(iterable):
    """Drop embedded NULs from each line (eg4_battery_log.csv has been
    seen with NUL bytes after Pi power loss mid-flush)."""
    for line in iterable:
        if "\x00" in line:
            line = line.replace("\x00", "")
        yield line


def _scan_battery_log_for_target(
    battery_log_path: str,
    target_ts: datetime,
    tz: ZoneInfo,
    window_sec: int,
) -> Optional[tuple[datetime, float]]:
    """Return (ts, soc_percent) for the battery-log row closest to but not
    strictly after `target_ts`, within ±window_sec.

    The "not strictly after" rule keeps the captured SOC representative of
    the moment the day starts. If no row precedes target_ts within the
    window, the closest row AFTER target_ts is used instead — necessary
    for days where the first poll landed a few minutes past sunrise.

    Returns None when no eligible row exists in the window.
    """
    if not os.path.exists(battery_log_path):
        return None

    window_start = target_ts - timedelta(seconds=window_sec)
    window_end = target_ts + timedelta(seconds=window_sec)

    best_before: Optional[tuple[datetime, float]] = None  # nearest <= target
    best_after: Optional[tuple[datetime, float]] = None   # nearest > target

    try:
        with open(battery_log_path, "r", newline="") as f:
            reader = csv.DictReader(_nul_stripped_lines(f))
            for raw in reader:
                ts_str = raw.get("ts") or ""
                soc_str = raw.get("soc_percent") or ""
                if not ts_str or not soc_str:
                    continue
                try:
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (TypeError, ValueError):
                    continue
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=tz)
                ts_local = dt.astimezone(tz)
                if ts_local < window_start or ts_local > window_end:
                    continue
                try:
                    soc = float(soc_str)
                except (TypeError, ValueError):
                    continue
                if ts_local <= target_ts:
                    if best_before is None or ts_local > best_before[0]:
                        best_before = (ts_local, soc)
                else:
                    if best_after is None or ts_local < best_after[0]:
                        best_after = (ts_local, soc)
    except Exception as exc:
        print(f"  Battery log scan failed: {exc}")
        return None

    if best_before is not None:
        return best_before
    return best_after


def _fmt_num(v) -> str:
    """Match the on-disk formatting used by services/pv_prediction_logger.py."""
    if v is None:
        return ""
    try:
        return f"{float(v):.4f}"
    except (TypeError, ValueError):
        return ""


def _atomic_rewrite(target_path: str, rows: list[dict]) -> None:
    """Write the full CSV (header + rows) atomically via temp + rename."""
    target_dir = os.path.dirname(target_path) or "."
    os.makedirs(target_dir, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=".pv_prediction_log.",
        suffix=".tmp",
        dir=target_dir,
    )
    try:
        with os.fdopen(fd, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, target_path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def backfill(dry_run: bool) -> int:
    if not os.path.exists(PREDICTION_LOG):
        print(f"ERROR: prediction log not found at {PREDICTION_LOG}")
        return 1

    lat, lon, tz_name, capacity_kwh = _load_location_and_capacity()
    tz = ZoneInfo(tz_name)
    print(f"Location: lat={lat}, lon={lon}, tz={tz_name}")
    if capacity_kwh is not None:
        print(f"Battery capacity: {capacity_kwh:.2f} kWh "
              f"(from weather_gate.battery_total_kwh)")
    else:
        print("Battery capacity: unknown — start_battery_kwh will be left blank.")

    # Read existing rows. Use DictReader so a partially-migrated CSV (missing
    # columns) still yields rows; the missing columns arrive as None and the
    # _atomic_rewrite writer normalises them.
    rows: list[dict] = []
    with open(PREDICTION_LOG, "r", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = {col: (raw.get(col) or "") for col in CSV_FIELDNAMES}
            rows.append(row)

    proposed = 0
    skipped_no_window = 0
    skipped_bad_date = 0
    for row in rows:
        if not _row_needs_backfill(row):
            continue
        date_str = row.get("date") or ""
        try:
            day = date.fromisoformat(date_str)
        except (TypeError, ValueError):
            print(f"  Skip row with unparseable date={date_str!r}")
            skipped_bad_date += 1
            continue

        target_ts = _target_time(day, lat, lon, tz_name)
        hit = _scan_battery_log_for_target(
            BATTERY_LOG, target_ts, tz, SEARCH_WINDOW_SEC
        )
        if hit is None:
            print(
                f"  Skip {day}: no battery-log sample within "
                f"±{SEARCH_WINDOW_SEC//3600}h of {target_ts.strftime('%H:%M:%S')}"
            )
            skipped_no_window += 1
            continue
        ts, soc = hit
        soc_str = _fmt_num(soc)
        if capacity_kwh is not None:
            kwh_val = soc / 100.0 * capacity_kwh
            kwh_str = _fmt_num(kwh_val)
        else:
            kwh_str = ""

        proposed += 1
        action = "WOULD SET" if dry_run else "SET"
        print(
            f"  {day} target={target_ts.strftime('%H:%M:%S')} "
            f"using {ts.strftime('%H:%M:%S')} "
            f"-> {action} start_soc_pct={soc_str} start_battery_kwh={kwh_str or '-'}"
        )
        if not dry_run:
            row["start_soc_pct"] = soc_str
            row["start_battery_kwh"] = kwh_str

    print()
    print(f"Summary:")
    print(f"  Rows updated: {proposed}")
    print(f"  Skipped (no window match): {skipped_no_window}")
    print(f"  Skipped (bad date): {skipped_bad_date}")

    if proposed == 0:
        if skipped_no_window == 0 and skipped_bad_date == 0:
            print("Nothing to backfill — every row already has start_soc_pct.")
        return 0

    if dry_run:
        print()
        print("Dry-run — no changes written.")
        return 0

    _atomic_rewrite(PREDICTION_LOG, rows)
    print()
    print(f"Wrote {PREDICTION_LOG}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill start_soc_pct and start_battery_kwh for historical "
            "rows in pv_prediction_log.csv. Run manually after deploy. "
            "Re-running is safe — already-populated rows are left alone."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the proposed values without writing the CSV.",
    )
    args = parser.parse_args(argv)
    return backfill(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
