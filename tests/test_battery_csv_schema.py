"""Tests for BatteryService CSV schema pinning and legacy file archival.

These tests construct a BatteryService with dummy credentials pointing at a
temp directory. start() is never called — _reconcile_csv_schema() and
_log_to_csv() are invoked directly to exercise the schema logic without
spinning up the EG4 client, network, or background threads.
"""
import csv
import glob
import os
import tempfile

import pytest

from services.battery_service import BatteryService, CSV_FIELDNAMES


# Representative legacy header — 27 columns. The first 10 happen to overlap
# with the canonical schema, which is exactly why DictReader-based consumers
# silently mismapped columns (positional mapping on a header mismatch).
LEGACY_HEADER = [
    "ts",
    "soc_percent",
    "pack_voltage_v",
    "pack_current_a",
    "pv_power_w",
    "load_power_w",
    "grid_power_w",
    "ac_couple_w",
    "battery_net_w",
    "units",
    "Battery_01_sn",
    "Battery_01_soc",
    "Battery_01_voltage_v",
    "Battery_01_current_a",
    "Battery_02_sn",
    "Battery_02_soc",
    "Battery_02_voltage_v",
    "Battery_02_current_a",
    "Battery_03_sn",
    "Battery_03_soc",
    "Battery_03_voltage_v",
    "Battery_03_current_a",
    "Battery_04_sn",
    "Battery_04_soc",
    "Battery_04_voltage_v",
    "Battery_04_current_a",
    "extra_col",
]


def _make_service(log_file: str) -> BatteryService:
    """Build a BatteryService pointed at a specific log file, without starting it."""
    return BatteryService(
        username="dummy",
        password="dummy",
        base_url="http://localhost",
        poll_seconds=60,
        log_interval_sec=600,
        log_file=log_file,
        session_refresh_hours=168,
    )


def _sample_snap() -> dict:
    """Canonical snap dict matching CSV_FIELDNAMES."""
    return {
        "ts": "2026-05-12T12:00:00+00:00",
        "soc_percent": 87,
        "pack_voltage_v": 52.4,
        "pack_current_a": 12.3,
        "pv_power_w": 4200,
        "load_power_w": 850,
        "grid_power_w": 0,
        "ac_couple_w": 0,
        "battery_net_w": 3350,
        "units": "W",
    }


def test_writer_creates_file_with_canonical_header(tmp_path):
    """Writing to an empty directory creates the file with the canonical header."""
    log_file = str(tmp_path / "eg4_battery_log.csv")
    svc = _make_service(log_file)

    svc._log_to_csv(_sample_snap())

    assert os.path.exists(log_file)
    with open(log_file, "r", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    assert len(rows) == 2, f"expected header + 1 data row, got {len(rows)} rows"
    assert rows[0] == CSV_FIELDNAMES
    assert len(rows[1]) == len(CSV_FIELDNAMES)


def test_writer_appends_when_header_matches(tmp_path):
    """A pre-existing file with the canonical header is reused without archiving."""
    log_file = str(tmp_path / "eg4_battery_log.csv")

    # Pre-create file with canonical header and one row.
    with open(log_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_FIELDNAMES)
        writer.writerow(["2026-05-11T00:00:00+00:00", 50, 51.0, 0, 0, 0, 0, 0, 0, "W"])

    svc = _make_service(log_file)
    svc._reconcile_csv_schema()

    # No archive should have been created.
    archives = glob.glob(str(tmp_path / "eg4_battery_log_legacy_*.csv"))
    assert archives == [], f"unexpected archive(s): {archives}"

    # Append a new row.
    svc._log_to_csv(_sample_snap())

    with open(log_file, "r", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == CSV_FIELDNAMES
    assert len(rows) == 3, f"expected header + 2 rows, got {len(rows)}"


def test_legacy_header_archived(tmp_path):
    """A legacy 27-column header triggers an atomic rename to a timestamped archive."""
    log_file = str(tmp_path / "eg4_battery_log.csv")

    # Pre-create file with the legacy header and one data row (10 cells — the
    # exact length-mismatch pattern that broke DictReader in production).
    with open(log_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(LEGACY_HEADER)
        writer.writerow(_sample_snap_values())

    svc = _make_service(log_file)
    svc._reconcile_csv_schema()

    # Original file gone, exactly one archive present.
    assert not os.path.exists(log_file), "original file should have been renamed"
    archives = glob.glob(str(tmp_path / "eg4_battery_log_legacy_*.csv"))
    assert len(archives) == 1, f"expected exactly 1 archive, got {archives}"


def test_extra_keys_ignored(tmp_path):
    """Snap dicts with extra keys not in CSV_FIELDNAMES are silently ignored."""
    log_file = str(tmp_path / "eg4_battery_log.csv")
    svc = _make_service(log_file)

    snap = _sample_snap()
    snap["extra_one"] = "noise"
    snap["extra_two"] = 99

    # Must not raise.
    svc._log_to_csv(snap)

    with open(log_file, "r", newline="") as f:
        rows = list(csv.reader(f))

    assert rows[0] == CSV_FIELDNAMES
    assert len(rows[1]) == len(CSV_FIELDNAMES), (
        f"data row should have exactly {len(CSV_FIELDNAMES)} columns, got {len(rows[1])}"
    )


def test_missing_keys_blank(tmp_path):
    """Snap dicts missing canonical keys produce empty-string columns, no exception."""
    log_file = str(tmp_path / "eg4_battery_log.csv")
    svc = _make_service(log_file)

    snap = _sample_snap()
    del snap["ac_couple_w"]
    del snap["units"]

    # Must not raise.
    svc._log_to_csv(snap)

    with open(log_file, "r", newline="") as f:
        rows = list(csv.reader(f))

    header = rows[0]
    data = rows[1]
    assert header == CSV_FIELDNAMES
    assert data[header.index("ac_couple_w")] == ""
    assert data[header.index("units")] == ""


def _sample_snap_values() -> list:
    """Return the values of _sample_snap() as a positional row for the canonical schema."""
    snap = _sample_snap()
    return [snap[k] for k in CSV_FIELDNAMES]
