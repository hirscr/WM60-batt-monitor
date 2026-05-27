"""One-shot: backfill actual_end_reason in pv_prediction_log.csv.

Iterates over every row that has actual_kwh populated but
actual_end_reason blank. For each such row, classifies the day using
the same battery-log scan that the in-process sunset write uses
(services.pv_prediction_logger.classify_end_reason), and rewrites the
CSV in place with the new values.

Sunset for historical dates is computed via astral using the latitude,
longitude, and timezone from config.yaml + config.local.yaml — the same
location data the autocontrol service uses for its own sunset gating.
This avoids depending on the weather service (which only knows today's
sunset) and keeps the tool fully offline.

Usage:

    python3 tools/backfill_end_reason.py            # write
    python3 tools/backfill_end_reason.py --dry-run  # show changes only

Safety: the rewrite is atomic (temp-sibling + rename), so a crash or
kill mid-run leaves the existing log untouched. The tool refuses to
touch rows that are already populated, so re-running it after a
partial run is harmless.

This tool is intended to be run manually after deploy. Nothing in the
running service imports it.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import tempfile
from datetime import date, datetime
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
    print("ERROR: astral not installed — required for historical sunset times.")
    sys.exit(2)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

# Importing the service module pulls in only pure helpers — no threads
# or file-system side effects fire at import time.
from services.pv_prediction_logger import (  # noqa: E402
    CSV_FIELDNAMES,
    classify_end_reason,
)

PREDICTION_LOG = os.path.join(PROJECT_ROOT, "miner_logs", "pv_prediction_log.csv")
BATTERY_LOG = os.path.join(PROJECT_ROOT, "miner_logs", "eg4_battery_log.csv")
CONFIG_YAML = os.path.join(PROJECT_ROOT, "config.yaml")
CONFIG_LOCAL_YAML = os.path.join(PROJECT_ROOT, "config.local.yaml")


def _load_location() -> tuple[float, float, str]:
    """Read latitude/longitude/timezone from config files.

    config.local.yaml overrides config.yaml when both present. Falls
    back to (40.0, -74.0, America/New_York) only when both files are
    missing — same defaults the autocontrol service uses.
    """
    lat = 40.0
    lon = -74.0
    tz_name = "America/New_York"
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
    return lat, lon, tz_name


def _compute_sunset(day: date, lat: float, lon: float, tz_name: str) -> datetime:
    """Compute astronomical sunset for `day` at the project location."""
    tz = ZoneInfo(tz_name)
    location = LocationInfo("Site", "Region", tz_name, lat, lon)
    s = astral_sun(location.observer, date=day, tzinfo=tz)
    return s["sunset"]


def _row_needs_backfill(row: dict) -> bool:
    """A row is eligible iff actual_kwh is populated AND actual_end_reason is blank."""
    actual = (row.get("actual_kwh") or "").strip()
    end_reason = (row.get("actual_end_reason") or "").strip()
    return bool(actual) and not end_reason


def _atomic_rewrite(target_path: str, rows: list[dict]) -> None:
    """Write the full CSV (header + rows) atomically via temp + rename.

    Same durability pattern as services/pv_prediction_logger.py — keeps
    the existing on-disk log untouched if the rewrite fails mid-way.
    """
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

    lat, lon, tz_name = _load_location()
    tz = ZoneInfo(tz_name)
    print(f"Location: lat={lat}, lon={lon}, tz={tz_name}")

    # Read existing rows.
    rows: list[dict] = []
    legacy_header_seen = False
    with open(PREDICTION_LOG, "r", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = {col: (raw.get(col) or "") for col in CSV_FIELDNAMES}
            # If the file is still on the legacy schema, the new column
            # will be missing from `raw` — that's fine, it stays blank.
            if "actual_end_reason" not in raw:
                legacy_header_seen = True
            rows.append(row)

    if legacy_header_seen:
        print(
            "NOTE: legacy 6-column header detected. The next service restart will "
            "migrate it in place; this backfill writes the 7-column schema regardless."
        )

    proposed = 0
    classified_counts = {"sunset": 0, "battery_full": 0, "unknown": 0}
    for row in rows:
        if not _row_needs_backfill(row):
            continue
        date_str = row.get("date") or ""
        try:
            day = date.fromisoformat(date_str)
        except (TypeError, ValueError):
            print(f"  Skip row with unparseable date={date_str!r}")
            continue
        try:
            sunset_dt = _compute_sunset(day, lat, lon, tz_name)
        except Exception as exc:
            print(f"  Skip {day}: sunset compute failed: {exc}")
            continue
        reason = classify_end_reason(
            battery_log_path=BATTERY_LOG,
            day=day,
            sunset_dt=sunset_dt,
            tz=tz,
        )
        proposed += 1
        classified_counts[reason] += 1
        action = "WOULD SET" if dry_run else "SET"
        print(
            f"  {day} sunset={sunset_dt.strftime('%H:%M:%S')} "
            f"-> {action} actual_end_reason={reason!r}"
        )
        if not dry_run:
            row["actual_end_reason"] = reason

    if proposed == 0:
        print("Nothing to backfill — every row with actual_kwh already has an end_reason.")
        return 0

    print()
    print(f"Summary: {proposed} rows classified")
    for label, count in classified_counts.items():
        print(f"  {label}: {count}")

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
            "Backfill actual_end_reason for historical rows in "
            "pv_prediction_log.csv. Run manually after deploy. "
            "Re-running is safe — already-populated rows are left alone."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the proposed classifications without writing the CSV.",
    )
    args = parser.parse_args(argv)
    return backfill(dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
