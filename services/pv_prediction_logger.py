"""Daily PV prediction-vs-actual logger.

Background-thread service. Once per local day, about 30 minutes after sunset,
this service:

  1. Computes the day's actual PV energy harvest by trapezoidal integration of
     the pv_power_w column in miner_logs/eg4_battery_log.csv for the calendar
     day in the configured local timezone (midnight-to-midnight).
  2. Reads the gate's persisted decision context (raw EG4 prediction, applied
     multiplier, expected_kwh used, decision source).
  3. Appends one row to miner_logs/pv_prediction_log.csv with the canonical
     schema (date, eg4_today_kwh_raw, multiplier_applied, expected_kwh_used,
     actual_kwh, ratio_actual_to_eg4_raw, decision_source).
  4. Updates last_pv_log_date in wm_state.json so a restart doesn't double-log.

Side-effect free helpers live in utils/pv_integration.py; this file owns the
file-system and threading concerns only.

Logging follows the two-layer gate pattern with tag "PV_LOG".
"""
from __future__ import annotations

import csv
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional
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
        """
        self._state = state_manager
        self._weather = weather_service
        self._battery_log_path = battery_log_path
        self._prediction_log_path = prediction_log_path
        self._tz = ZoneInfo(timezone_str)
        self._tick_seconds = int(tick_seconds)
        self._post_sunset_seconds = int(post_sunset_seconds)

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
        """One pass: log today's row if it's time."""
        now_local = datetime.now(self._tz)
        today_local = now_local.date()

        # Already logged today? If so, nothing to do.
        saved = self._state.load()
        last_logged_str = saved.get("last_pv_log_date")
        if last_logged_str:
            try:
                last_logged = date.fromisoformat(last_logged_str)
                if last_logged >= today_local:
                    return
            except (TypeError, ValueError):
                # Corrupt value — treat as "never logged" and let the
                # idempotent CSV append handle dedup.
                pass

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

        # Compute the day's actual PV harvest and append the row.
        actual_kwh = self._compute_actual_kwh(today_local)
        decision_ctx = self._read_gate_decision_context(saved, today_local)
        self._append_row(today_local, actual_kwh, decision_ctx)

        # Persist last_pv_log_date so we don't re-run if the service restarts.
        self._state.save(last_pv_log_date=today_local.isoformat())
        if __debug__:
            log(
                "PV_LOG",
                f"logged date={today_local} actual_kwh={actual_kwh:.2f} "
                f"raw={decision_ctx.get('eg4_today_kwh_raw')} "
                f"source={decision_ctx.get('decision_source')}",
            )

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
        """
        if not os.path.exists(self._battery_log_path):
            return 0.0

        day_start = datetime.combine(day, datetime.min.time(), tzinfo=self._tz)
        day_end = day_start + timedelta(days=1)

        samples = []
        try:
            with open(self._battery_log_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for raw_row in reader:
                    parsed = parse_battery_row(raw_row, tz=self._tz)
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
        """Append one row to pv_prediction_log.csv. Idempotent within a day.

        If the last existing row already has today's date, this is a no-op
        (defense against the state_manager flag being out of sync with the
        on-disk file).
        """
        os.makedirs(os.path.dirname(self._prediction_log_path), exist_ok=True)
        file_exists = os.path.exists(self._prediction_log_path)

        # Idempotent: if today's row is already there, skip.
        if file_exists:
            existing = self._tail_date()
            if existing == day.isoformat():
                if __debug__:
                    log("PV_LOG", f"row for {day.isoformat()} already present; skip append")
                return

        raw = ctx.get("eg4_today_kwh_raw")
        ratio: Optional[str] = ""
        if isinstance(raw, (int, float)) and raw > 0:
            ratio = f"{actual_kwh / float(raw):.4f}"

        row = {
            "date": day.isoformat(),
            "eg4_today_kwh_raw": _fmt_num(raw),
            "multiplier_applied": _fmt_num(ctx.get("multiplier_applied")),
            "expected_kwh_used": _fmt_num(ctx.get("expected_kwh_used")),
            "actual_kwh": f"{actual_kwh:.4f}",
            "ratio_actual_to_eg4_raw": ratio,
            "decision_source": ctx.get("decision_source") or "",
        }

        with open(self._prediction_log_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

    def _tail_date(self) -> Optional[str]:
        """Return the date field of the last row, or None if file is empty/missing."""
        if not os.path.exists(self._prediction_log_path):
            return None
        try:
            with open(self._prediction_log_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                last_row = None
                for row in reader:
                    last_row = row
                if last_row is None:
                    return None
                return last_row.get("date")
        except Exception:
            return None

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
