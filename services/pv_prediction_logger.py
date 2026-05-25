"""Daily PV prediction-vs-actual logger.

Background-thread service. Once per local day, about 30 minutes after sunset,
this service:

  1. Computes the day's actual PV energy harvest. EG4 inverter's cumulative
     todayYielding is the primary source; trapezoidal integration of the
     pv_power_w column in miner_logs/eg4_battery_log.csv is the fallback.
  2. Reads the gate's persisted decision context (raw EG4 prediction, applied
     multiplier, expected_kwh used, decision source).
  3. UPSERTS one row keyed by date into miner_logs/pv_prediction_log.csv with
     the canonical schema (date, eg4_today_kwh_raw, multiplier_applied,
     expected_kwh_used, actual_kwh, ratio_actual_to_eg4_raw, decision_source).
     Writes go through a temp-sibling + atomic rename so a crash mid-write
     cannot truncate the log.
  4. Updates last_pv_log_date in wm_state.json ONLY when today's row is
     fully populated (non-blank, non-zero actual_kwh AND every gate-context
     column whose wm_state counterpart is set today). An incomplete row
     leaves the flag untouched, so the next tick after sunset+30min self-
     heals the missing fields.

Side-effect free helpers live in utils/pv_integration.py; this file owns the
file-system and threading concerns only.

Logging follows the two-layer gate pattern with tag "PV_LOG".
"""
from __future__ import annotations

import csv
import os
import tempfile
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from utils.log_config import log
from utils.pv_integration import parse_battery_row, trapezoidal_kwh

# Canonical column order for the prediction log. Schema reconcile compares
# the on-disk header to this list; mismatch -> archive + create fresh.
CSV_FIELDNAMES = [
    "date",
    "eg4_today_kwh_raw",
    "multiplier_applied",
    "expected_kwh_used",
    "actual_kwh",
    "ratio_actual_to_eg4_raw",
    "decision_source",
]

# How long to wait after sunset before logging the day. 30 minutes is enough
# to capture late-day cleanup PV harvest without colliding with sunrise on
# the next day.
DEFAULT_POST_SUNSET_SEC = 30 * 60


class PVPredictionLogger:
    """Background service that writes one row per day to pv_prediction_log.csv."""

    def __init__(
        self,
        state_manager,
        weather_service,
        battery_log_path: str,
        prediction_log_path: str,
        timezone_str: str,
        tick_seconds: int = 60,
        post_sunset_seconds: int = DEFAULT_POST_SUNSET_SEC,
        get_eg4_client: Optional[Callable[[], object]] = None,
    ):
        """
        Args:
            state_manager: project StateManager for last_pv_log_date persistence.
            weather_service: WeatherService — used only for today's sunset_dt
                via get_today_forecast(). Cache-only; never triggers network.
            battery_log_path: absolute path to miner_logs/eg4_battery_log.csv.
            prediction_log_path: absolute path to miner_logs/pv_prediction_log.csv.
            timezone_str: IANA tz for "calendar day" boundaries.
            tick_seconds: how often the background loop wakes to check.
            post_sunset_seconds: how long after sunset to trigger logging.
            get_eg4_client: optional zero-arg callable returning the current
                EG4Client (or None if not yet started). Used as the primary
                source for actual_kwh via EG4Client.get_today_yielding_kwh_blocking();
                CSV trapezoidal integration is the fallback. The callback shape
                exists because BatteryService.refresh_session() replaces its
                EG4Client, so capturing a direct reference would silently go
                stale.
        """
        self._state = state_manager
        self._weather = weather_service
        self._battery_log_path = battery_log_path
        self._prediction_log_path = prediction_log_path
        self._tz = ZoneInfo(timezone_str)
        self._tick_seconds = int(tick_seconds)
        self._post_sunset_seconds = int(post_sunset_seconds)
        self._get_eg4_client = get_eg4_client

        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._running:
            return
        try:
            self._reconcile_csv_schema()
        except Exception as exc:
            print(f"[PVPredictionLogger] CSV schema reconciliation failed: {exc}")
        self._running = True
        self._thread = threading.Thread(
            target=self._loop, name="pv-prediction-logger", daemon=True
        )
        self._thread.start()
        print("[PVPredictionLogger] Started")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        print("[PVPredictionLogger] Stopped")

    # ------------------------------------------------------------------
    # Public test/utility surface — small, side-effect-free where possible
    # ------------------------------------------------------------------

    def read_recent_rows(self, days: int) -> list:
        """Return the last `days` rows of the prediction log, reverse-chronological.

        Used by GET /api/weather/prediction_history. Returns an empty list
        when the file does not exist or has no data rows.
        """
        days = max(1, min(365, int(days)))
        if not os.path.exists(self._prediction_log_path):
            return []
        rows = []
        try:
            with open(self._prediction_log_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(dict(row))
        except Exception as exc:
            print(f"[PVPredictionLogger] read_recent_rows failed: {exc}")
            return []
        rows.reverse()
        return rows[:days]

    # ------------------------------------------------------------------
    # Background loop
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        if __debug__:
            log("PV_LOG", "loop started")
        while self._running:
            try:
                self._tick()
            except Exception as exc:
                print(f"[PVPredictionLogger] tick error: {exc}")
                if __debug__:
                    log("PV_LOG", f"tick error: {exc}")
            for _ in range(self._tick_seconds):
                if not self._running:
                    break
                time.sleep(1)
        if __debug__:
            log("PV_LOG", "loop exiting")

    def _tick(self) -> None:
        """One pass: upsert today's row if it's time and the row is incomplete.

        Self-healing semantics: the tick does NOT early-return purely on the
        last_pv_log_date flag. It re-runs after sunset+30min whenever today's
        row is incomplete (missing/blank/zero actual_kwh, or any gate-context
        column blank while the wm_state has a matching value for today). Only
        once the row is fully populated AND last_pv_log_date == today does the
        tick no-op. This lets a partially-written row self-correct on the next
        sunset+30min pass — e.g. when the gate evaluated AFTER an earlier
        premature log write, leaving the gate-context columns blank on disk.
        """
        now_local = datetime.now(self._tz)
        today_local = now_local.date()

        saved = self._state.load()
        last_logged_str = saved.get("last_pv_log_date")

        # The "complete and done" no-op condition: row is fully populated
        # AND the persisted flag matches today. We compute completeness
        # before any sunset gating because if we're already done, no need
        # to ask the weather service or do any work at all.
        row_complete = self._is_today_row_complete(today_local, saved)
        last_logged_is_today = False
        if last_logged_str:
            try:
                last_logged_is_today = (
                    date.fromisoformat(last_logged_str) == today_local
                )
            except (TypeError, ValueError):
                last_logged_is_today = False
        if row_complete and last_logged_is_today:
            return

        # We need sunset to know when the day is "done". WeatherService
        # exposes today's sunset via get_today_forecast(); this is cache-only.
        forecast = self._weather.get_today_forecast() if self._weather else {}
        sunset_dt = forecast.get("sunset_dt")
        if sunset_dt is None:
            if __debug__:
                log("PV_LOG", "no sunset_dt yet; skip")
            return

        # Only log once we are past sunset + post_sunset window.
        trigger_at = sunset_dt + timedelta(seconds=self._post_sunset_seconds)
        if now_local < trigger_at:
            return

        # Compute the day's actual PV harvest and upsert the row. EG4
        # todayYielding is the primary source; CSV trapezoidal integration
        # is the fallback when EG4 is unavailable.
        actual_kwh, actual_source = self._resolve_actual_kwh(today_local)
        decision_ctx = self._read_gate_decision_context(saved, today_local)
        self._append_row(today_local, actual_kwh, decision_ctx)

        # Persist last_pv_log_date ONLY when the row is fully populated.
        # An incomplete write (zero/missing actual, or gate ctx still blank
        # while wm_state has it) leaves the flag at its previous value so
        # the next tick re-attempts.
        if self._is_today_row_complete(today_local, saved):
            self._state.save(last_pv_log_date=today_local.isoformat())
            if __debug__:
                log(
                    "PV_LOG",
                    f"logged date={today_local} actual_kwh={actual_kwh:.2f} "
                    f"actual_source={actual_source} "
                    f"raw={decision_ctx.get('eg4_today_kwh_raw')} "
                    f"source={decision_ctx.get('decision_source')} "
                    f"complete=True",
                )
        else:
            if __debug__:
                log(
                    "PV_LOG",
                    f"upserted incomplete row date={today_local} "
                    f"actual_kwh={actual_kwh:.2f} actual_source={actual_source} "
                    f"raw={decision_ctx.get('eg4_today_kwh_raw')} "
                    f"source={decision_ctx.get('decision_source')} "
                    f"flag_not_advanced",
                )

    def _resolve_actual_kwh(self, day: date) -> tuple[float, str]:
        """Return (actual_kwh, source_label) for `day`.

        Primary source is the EG4 inverter's cumulative todayYielding,
        fetched via the get_eg4_client callback. Fallback is trapezoidal
        integration of pv_power_w in eg4_battery_log.csv.

        The EG4 result is used iff the call returns a float (including 0.0
        — a legitimate cloudy-day reading). CSV fallback is taken ONLY when
        the EG4 call returns None (callback missing, client not yet started,
        loop not running, validation reject, exception). The explicit
        `result is None` check is load-bearing: treating 0.0 as missing
        would silently overwrite a real zero-production day with a CSV
        integration (which itself might be zero or, worse, slightly
        positive from sensor noise).
        """
        if self._get_eg4_client is not None:
            try:
                client = self._get_eg4_client()
            except Exception as exc:
                client = None
                if __debug__:
                    log("PV_LOG", f"get_eg4_client callback raised: {exc}")
            if client is not None:
                try:
                    eg4_value = client.get_today_yielding_kwh_blocking()
                except Exception as exc:
                    eg4_value = None
                    if __debug__:
                        log("PV_LOG", f"eg4 today_yielding call raised: {exc}")
                if eg4_value is not None:
                    if __debug__:
                        log("PV_LOG", f"actual_kwh from eg4: {eg4_value:.4f}")
                    return float(eg4_value), "eg4_today_yielding"

        csv_value = self._compute_actual_kwh(day)
        if __debug__:
            log("PV_LOG", f"actual_kwh from csv fallback: {csv_value:.4f}")
        return csv_value, "csv_integration"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _read_gate_decision_context(self, saved_state: dict, today: date) -> dict:
        """Pull the gate's persisted decision context for today.

        Returns the values used in the prediction log row. All four keys are
        always present; values default to None when the gate didn't evaluate
        today (e.g. weather gate disabled, or evaluation skipped).
        """
        # Only trust the context if the gate evaluated today; otherwise the
        # values are from a previous day and would corrupt the calibration log.
        evaluated_date_str = saved_state.get("weather_gate_evaluated_date")
        ctx_valid = False
        if evaluated_date_str:
            try:
                ctx_valid = date.fromisoformat(evaluated_date_str) == today
            except (TypeError, ValueError):
                ctx_valid = False

        if not ctx_valid:
            return {
                "eg4_today_kwh_raw": None,
                "multiplier_applied": None,
                "expected_kwh_used": None,
                "decision_source": None,
            }

        return {
            "eg4_today_kwh_raw": saved_state.get("weather_gate_eg4_today_kwh_raw"),
            "multiplier_applied": saved_state.get("weather_gate_multiplier_applied"),
            "expected_kwh_used": saved_state.get("weather_gate_expected_kwh"),
            "decision_source": saved_state.get("weather_gate_decision_source"),
        }

    def _compute_actual_kwh(self, day: date) -> float:
        """Trapezoidal integration of pv_power_w over `day` (local tz).

        Day window: local midnight to next local midnight, exclusive.
        Returns 0.0 when the CSV is missing or no rows fall in the window.

        Embedded NUL bytes in the underlying file are stripped before the
        DictReader parses them. NULs have appeared in eg4_battery_log.csv
        after disk-write interruptions (Pi power loss mid-flush); without
        the strip, csv.DictReader raises _csv.Error("line contains NUL")
        and would abort the integration for the entire day. Malformed rows
        are skipped at the parse_battery_row level — they remain non-fatal.
        """
        if not os.path.exists(self._battery_log_path):
            return 0.0

        day_start = datetime.combine(day, datetime.min.time(), tzinfo=self._tz)
        day_end = day_start + timedelta(days=1)

        samples = []
        try:
            with open(self._battery_log_path, "r", newline="") as f:
                lines = _nul_stripped_lines(f)
                reader = csv.DictReader(lines)
                for raw_row in reader:
                    try:
                        parsed = parse_battery_row(raw_row, tz=self._tz)
                    except Exception:
                        # parse_battery_row is defensive but be doubly safe:
                        # a single corrupt row must not abort the whole day.
                        continue
                    if parsed is None:
                        continue
                    ts, watts = parsed
                    # Convert to local tz for the day-window comparison.
                    ts_local = ts.astimezone(self._tz)
                    if day_start <= ts_local < day_end:
                        samples.append((ts_local, watts))
        except Exception as exc:
            print(f"[PVPredictionLogger] read battery csv failed: {exc}")
            return 0.0

        samples.sort(key=lambda p: p[0])
        return trapezoidal_kwh(samples)

    def _append_row(self, day: date, actual_kwh: float, ctx: dict) -> None:
        """Upsert today's row in pv_prediction_log.csv keyed by date.

        Behavior:
          - If a row for `day` exists, replace all six non-key fields in
            place. Other rows are preserved untouched.
          - Otherwise, append a new row at the tail.
          - Write goes via a temp-sibling + atomic rename so a crash mid-
            write never truncates the log.

        The name "_append_row" is retained for backwards compatibility
        with the test surface (tests call it directly to exercise the
        write path in isolation). The semantic, however, is upsert.
        """
        os.makedirs(os.path.dirname(self._prediction_log_path), exist_ok=True)

        new_row = self._build_row(day, actual_kwh, ctx)
        existing_rows = self._read_all_rows()

        replaced = False
        merged_rows = []
        for row in existing_rows:
            if row.get("date") == day.isoformat():
                merged_rows.append(new_row)
                replaced = True
            else:
                merged_rows.append(row)
        if not replaced:
            merged_rows.append(new_row)

        self._atomic_rewrite(merged_rows)
        if __debug__:
            action = "replaced" if replaced else "appended"
            log("PV_LOG", f"row {action} for {day.isoformat()}")

    def _build_row(self, day: date, actual_kwh: float, ctx: dict) -> dict:
        """Build a row dict from the gate-context inputs.

        Recomputes ratio_actual_to_eg4_raw whenever both actual_kwh and a
        positive eg4_today_kwh_raw are present; blank otherwise.
        """
        raw = ctx.get("eg4_today_kwh_raw")
        ratio: str = ""
        if isinstance(raw, (int, float)) and raw > 0:
            ratio = f"{actual_kwh / float(raw):.4f}"

        return {
            "date": day.isoformat(),
            "eg4_today_kwh_raw": _fmt_num(raw),
            "multiplier_applied": _fmt_num(ctx.get("multiplier_applied")),
            "expected_kwh_used": _fmt_num(ctx.get("expected_kwh_used")),
            "actual_kwh": f"{actual_kwh:.4f}",
            "ratio_actual_to_eg4_raw": ratio,
            "decision_source": ctx.get("decision_source") or "",
        }

    def _read_all_rows(self) -> list[dict]:
        """Return every row from the prediction log as a list of dicts.

        Empty list when the file does not exist or has no data rows. The
        header is not returned; the writer recreates it on rewrite.
        """
        if not os.path.exists(self._prediction_log_path):
            return []
        rows: list[dict] = []
        try:
            with open(self._prediction_log_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(dict(row))
        except Exception as exc:
            if __debug__:
                log("PV_LOG", f"_read_all_rows failed: {exc}")
            return []
        return rows

    def _atomic_rewrite(self, rows: list[dict]) -> None:
        """Write the full CSV (header + rows) atomically via temp + rename.

        Crash safety: a power loss or kill between the open and the rename
        leaves the existing file untouched; only the orphaned temp sibling
        is left behind. os.replace is atomic on POSIX, which is the deploy
        target. The temp file is created in the same directory as the
        prediction log so the rename stays within a single filesystem.
        """
        target_dir = os.path.dirname(self._prediction_log_path) or "."
        os.makedirs(target_dir, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".pv_prediction_log.",
            suffix=".tmp",
            dir=target_dir,
        )
        try:
            with os.fdopen(fd, "w", newline="") as f:
                writer = csv.DictWriter(
                    f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore"
                )
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self._prediction_log_path)
        finally:
            # If we never made it to os.replace, clean up the temp file.
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    def _is_today_row_complete(self, today: date, saved_state: dict) -> bool:
        """Return True iff today's on-disk row is fully populated.

        Completeness criteria (must all hold):
          - A row exists for `today` in pv_prediction_log.csv.
          - actual_kwh is parseable, non-blank, and STRICTLY POSITIVE.
            Zero counts as incomplete per the spec — a row with 0.00
            actual_kwh is the signature of a premature pre-sunset write
            and should self-heal at the next sunset+30min tick.
          - For each of the four gate-context columns (eg4_today_kwh_raw,
            multiplier_applied, expected_kwh_used, decision_source):
            if wm_state has the corresponding weather_gate_* value present
            AND weather_gate_evaluated_date == today, then the column must
            be non-blank on disk. If wm_state does not have it, a blank
            column does NOT count as incomplete (nothing to fill in).

        Tradeoff: treating actual_kwh == 0 as "incomplete" causes the tick
        to re-attempt the write on a legitimately cloudy day (EG4 truly
        reports 0.0). That repeats every tick interval until midnight,
        when the date rollover quiets it. The on-disk row stays at 0.0000
        — the wasted writes are bounded and harmless. The alternative,
        treating 0 as complete, would never self-heal the May 25 case
        that motivated this rewrite.
        """
        rows = self._read_all_rows()
        target_iso = today.isoformat()
        row = None
        for r in rows:
            if r.get("date") == target_iso:
                row = r
                break
        if row is None:
            return False

        # actual_kwh must be parseable AND > 0.
        actual_str = (row.get("actual_kwh") or "").strip()
        if not actual_str:
            return False
        try:
            if float(actual_str) <= 0.0:
                return False
        except (TypeError, ValueError):
            return False

        # Per-column gate-context completeness, conditional on wm_state
        # having the corresponding value and an evaluated_date matching today.
        evaluated_date_str = saved_state.get("weather_gate_evaluated_date")
        gate_today = False
        if evaluated_date_str:
            try:
                gate_today = date.fromisoformat(evaluated_date_str) == today
            except (TypeError, ValueError):
                gate_today = False

        if gate_today:
            checks = [
                ("eg4_today_kwh_raw", "weather_gate_eg4_today_kwh_raw"),
                ("multiplier_applied", "weather_gate_multiplier_applied"),
                ("expected_kwh_used", "weather_gate_expected_kwh"),
                ("decision_source", "weather_gate_decision_source"),
            ]
            for csv_col, state_key in checks:
                state_val = saved_state.get(state_key)
                if state_val is None:
                    continue
                # wm_state has a value for this column; disk row must
                # have a non-blank entry to count as complete.
                csv_val = (row.get(csv_col) or "").strip()
                if not csv_val:
                    return False

        return True

    def _reconcile_csv_schema(self) -> None:
        """Ensure the prediction log uses CSV_FIELDNAMES.

        Same pattern as BatteryService._reconcile_csv_schema:
          - file missing or empty -> nothing to do, first write creates it
          - header matches -> keep using it
          - header differs -> archive to a timestamped sibling and create fresh
        """
        path = self._prediction_log_path
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return

        with open(path, "r", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                header = []

        if header == CSV_FIELDNAMES:
            return

        stem, ext = os.path.splitext(os.path.basename(path))
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archive = os.path.join(os.path.dirname(path), f"{stem}_legacy_{stamp}{ext}")
        os.rename(path, archive)
        print(
            f"[PVPredictionLogger] WARNING: legacy CSV schema detected — "
            f"archived to {archive}. Starting fresh log with canonical schema."
        )


def _fmt_num(v) -> str:
    """Format a numeric value for the CSV. Empty string for None."""
    if v is None:
        return ""
    try:
        return f"{float(v):.4f}"
    except (TypeError, ValueError):
        return ""


def _nul_stripped_lines(iterable):
    """Yield lines from `iterable` with embedded NUL bytes (\\x00) removed.

    csv.DictReader raises _csv.Error("line contains NUL") on any NUL byte,
    which would otherwise abort an entire day's integration when a single
    corrupted line (e.g. partial write during a Pi power loss) sits in the
    middle of eg4_battery_log.csv. Stripping NULs at the line level keeps
    the valid surrounding rows usable; rows that become structurally
    malformed after the strip are dropped by parse_battery_row's normal
    None-on-error contract.
    """
    for line in iterable:
        if "\x00" in line:
            line = line.replace("\x00", "")
        yield line
