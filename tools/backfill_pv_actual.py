"""One-shot: patch pv_prediction_log.csv for a specified date.

Two independent modes, run separately or combined:

  Default (no flag) — recompute actual_kwh from the battery log
    Reads eg4_battery_log.csv with NUL-byte stripping (same logic as
    the hardened CSV fallback in pv_prediction_logger.py), integrates
    pv_power_w for the requested calendar day (America/New_York,
    midnight-to-midnight) using the trapezoidal rule, and writes the
    result into the actual_kwh column of that date's row.

  --from-state — patch gate-context columns from wm_state.json
    Reads wm_state.json from the project root and patches the four
    gate-context columns (eg4_today_kwh_raw, multiplier_applied,
    expected_kwh_used, decision_source) into that date's row from the
    current weather_gate_* state values. Refuses if wm_state's
    weather_gate_evaluated_date does not match the target date (would
    otherwise write stale gate context).

  --from-state without recompute — leaves actual_kwh untouched. Use
    this for a day not yet complete (e.g. today's row that was
    written before the gate finished evaluating).

  --from-state --actual — runs both modes in one invocation. Useful
    for completed past days.

ratio_actual_to_eg4_raw is recomputed whenever either actual_kwh or
eg4_today_kwh_raw is touched in this run.

Usage on the Pi (service can stay running — only reads battery log,
rewrites pred log):

    # Recompute actual_kwh from the battery log (legacy behavior)
    python3 tools/backfill_pv_actual.py 2026-05-24

    # Patch gate context only (today's row, EG4 will fill actual later)
    python3 tools/backfill_pv_actual.py 2026-05-25 --from-state

    # Both — for a completed past day that's missing gate context too
    python3 tools/backfill_pv_actual.py 2026-05-24 --from-state --actual
"""
import argparse
import csv
import json
import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BATTERY_LOG = os.path.join(PROJECT_ROOT, "miner_logs", "eg4_battery_log.csv")
PREDICTION_LOG = os.path.join(PROJECT_ROOT, "miner_logs", "pv_prediction_log.csv")
WM_STATE = os.path.join(PROJECT_ROOT, "wm_state.json")
TZ = ZoneInfo("America/New_York")
GAP_THRESHOLD_SEC = 1800.0

CSV_FIELDNAMES = [
    "date",
    "eg4_today_kwh_raw",
    "multiplier_applied",
    "expected_kwh_used",
    "actual_kwh",
    "ratio_actual_to_eg4_raw",
    "decision_source",
]

# Mapping from CSV column name to wm_state.json key. The actual_kwh
# column has no wm_state counterpart and is handled separately.
GATE_CTX_MAPPING = [
    ("eg4_today_kwh_raw", "weather_gate_eg4_today_kwh_raw"),
    ("multiplier_applied", "weather_gate_multiplier_applied"),
    ("expected_kwh_used", "weather_gate_expected_kwh"),
    ("decision_source", "weather_gate_decision_source"),
]


def nul_stripped_lines(iterable):
    for line in iterable:
        if "\x00" in line:
            line = line.replace("\x00", "")
        yield line


def compute_actual_kwh(day: date) -> float:
    day_start = datetime.combine(day, datetime.min.time(), tzinfo=TZ)
    day_end = day_start + timedelta(days=1)

    samples = []
    battery_log = os.path.abspath(BATTERY_LOG)
    if not os.path.exists(battery_log):
        print(f"ERROR: battery log not found at {battery_log}")
        return 0.0

    with open(battery_log, "r", newline="", errors="replace") as f:
        reader = csv.DictReader(nul_stripped_lines(f))
        for row in reader:
            ts_str = row.get("ts", "")
            if not ts_str:
                continue
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except (TypeError, ValueError):
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
            ts_local = dt.astimezone(TZ)
            if not (day_start <= ts_local < day_end):
                continue
            raw = row.get("pv_power_w", "")
            if raw is None or raw == "":
                watts = 0.0
            else:
                try:
                    watts = float(raw)
                except (TypeError, ValueError):
                    continue
                if watts < 0.0:
                    watts = 0.0
            samples.append((ts_local, watts))

    samples.sort(key=lambda p: p[0])
    print(f"  Found {len(samples)} battery log rows for {day}")
    if not samples:
        return 0.0

    total_ws = 0.0
    for i in range(len(samples) - 1):
        t0, w0 = samples[i]
        t1, w1 = samples[i + 1]
        dt_sec = (t1 - t0).total_seconds()
        if dt_sec <= 0 or dt_sec > GAP_THRESHOLD_SEC:
            continue
        total_ws += 0.5 * (w0 + w1) * dt_sec

    return total_ws / 3_600_000.0


def _fmt_num(v) -> str:
    """Match the on-disk formatting used by services/pv_prediction_logger.py."""
    if v is None:
        return ""
    try:
        return f"{float(v):.4f}"
    except (TypeError, ValueError):
        return ""


def _recompute_ratio(actual_str: str, raw_str: str) -> str:
    """Return the ratio cell value as a formatted string, or '' when
    division is impossible (missing/blank/zero raw)."""
    try:
        raw_val = float(raw_str) if raw_str else 0.0
    except (TypeError, ValueError):
        return ""
    if raw_val <= 0.0:
        return ""
    try:
        actual_val = float(actual_str) if actual_str else 0.0
    except (TypeError, ValueError):
        return ""
    return f"{actual_val / raw_val:.4f}"


def _load_wm_state() -> dict:
    """Read wm_state.json. Refuses (exits non-zero) on missing/corrupt file
    rather than guessing — the gate-context patch path is too important to
    silently apply partial data."""
    if not os.path.exists(WM_STATE):
        print(f"ERROR: wm_state.json not found at {WM_STATE}")
        sys.exit(2)
    try:
        with open(WM_STATE, "r") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"ERROR: cannot read wm_state.json: {exc}")
        sys.exit(2)


def _validate_state_evaluated_date(state: dict, day: date) -> None:
    """Refuse to write stale gate context.

    The weather gate stamps weather_gate_evaluated_date when it commits a
    decision. If that stamp does not match the day we are patching, the
    gate context is from another day and would corrupt the calibration
    log. Exit non-zero with a clear message rather than write bad data.
    """
    eval_str = state.get("weather_gate_evaluated_date")
    if eval_str != day.isoformat():
        print(
            f"ERROR: --from-state refused: wm_state.json "
            f"weather_gate_evaluated_date={eval_str!r} does not match "
            f"target date {day.isoformat()!r}. Refusing to write stale "
            f"gate context."
        )
        sys.exit(3)


def patch_prediction_log(
    day: date,
    *,
    actual_kwh: float | None,
    state_ctx: dict | None,
) -> bool:
    """Patch the row for `day` in pv_prediction_log.csv.

    Args:
        day: target date.
        actual_kwh: if not None, overwrite the actual_kwh column with
            this value. If None, leave the actual_kwh column alone.
        state_ctx: if not None, a dict of {csv_column: value} to patch
            the four gate-context columns. None values become blanks.
            If None as the parameter (not the inner values), the gate-
            context columns are not touched.

    Recomputes ratio_actual_to_eg4_raw whenever either actual_kwh or
    eg4_today_kwh_raw was touched in this run.
    """
    pred_log = os.path.abspath(PREDICTION_LOG)
    if not os.path.exists(pred_log):
        print(f"ERROR: prediction log not found at {pred_log}")
        return False

    rows = []
    patched = False
    with open(pred_log, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("date") == day.isoformat():
                if actual_kwh is not None:
                    old = row.get("actual_kwh", "")
                    row["actual_kwh"] = f"{actual_kwh:.4f}"
                    print(
                        f"  Patching {day}: actual_kwh "
                        f"{old!r} -> {row['actual_kwh']!r}"
                    )
                if state_ctx is not None:
                    for csv_col in (
                        "eg4_today_kwh_raw",
                        "multiplier_applied",
                        "expected_kwh_used",
                        "decision_source",
                    ):
                        old = row.get(csv_col, "")
                        if csv_col == "decision_source":
                            new = state_ctx.get(csv_col) or ""
                        else:
                            new = _fmt_num(state_ctx.get(csv_col))
                        row[csv_col] = new
                        print(
                            f"  Patching {day}: {csv_col} "
                            f"{old!r} -> {new!r}"
                        )

                # Recompute ratio whenever either side was touched.
                if actual_kwh is not None or state_ctx is not None:
                    row["ratio_actual_to_eg4_raw"] = _recompute_ratio(
                        row.get("actual_kwh", ""),
                        row.get("eg4_today_kwh_raw", ""),
                    )
                    print(
                        f"  Patching {day}: ratio_actual_to_eg4_raw -> "
                        f"{row['ratio_actual_to_eg4_raw']!r}"
                    )
                patched = True
            rows.append(dict(row))

    if not patched:
        print(f"  No row found for {day} in prediction log — nothing to patch.")
        return False

    with open(pred_log, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return True


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Patch pv_prediction_log.csv for a date. Default behavior "
            "recomputes actual_kwh from the battery log. --from-state "
            "patches the four gate-context columns from wm_state.json. "
            "Use both flags to do both in one run."
        ),
    )
    parser.add_argument(
        "date",
        help="Target date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--from-state",
        action="store_true",
        help=(
            "Patch the four gate-context columns from wm_state.json. "
            "By itself this leaves actual_kwh untouched; combine with "
            "--actual to also recompute actual_kwh."
        ),
    )
    parser.add_argument(
        "--actual",
        action="store_true",
        help=(
            "Force actual_kwh recomputation from the battery log. "
            "Only meaningful alongside --from-state; without --from-state "
            "the default already recomputes actual_kwh."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    try:
        day = date.fromisoformat(args.date)
    except ValueError:
        print(f"ERROR: invalid date {args.date!r}, expected YYYY-MM-DD")
        return 1

    # Decide what to patch.
    # - default (no flags):   recompute actual, no state
    # - --from-state alone:   no actual recompute, patch state
    # - --from-state --actual: both
    # - --actual alone:        treated as default (it's the implicit
    #   behavior anyway); harmless redundancy
    do_state = args.from_state
    do_actual = (not args.from_state) or args.actual

    state_ctx: dict | None = None
    if do_state:
        state = _load_wm_state()
        _validate_state_evaluated_date(state, day)
        state_ctx = {csv_col: state.get(key) for csv_col, key in GATE_CTX_MAPPING}
        print(
            f"  Loaded gate context from wm_state.json for {day}: "
            f"eg4_raw={state_ctx['eg4_today_kwh_raw']}, "
            f"mult={state_ctx['multiplier_applied']}, "
            f"expected={state_ctx['expected_kwh_used']}, "
            f"source={state_ctx['decision_source']!r}"
        )

    actual_kwh: float | None = None
    if do_actual:
        print(f"Computing actual_kwh for {day} from battery log...")
        actual_kwh = compute_actual_kwh(day)
        print(f"  Computed actual_kwh: {actual_kwh:.4f} kWh")

    if patch_prediction_log(day, actual_kwh=actual_kwh, state_ctx=state_ctx):
        print("Done.")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
