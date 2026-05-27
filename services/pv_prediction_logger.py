"""Daily PV prediction-vs-actual logger.

Background-thread service that writes one row per local day to
miner_logs/pv_prediction_log.csv using two independent, idempotent writes:

  1. Morning write (prediction columns) — fires the first time the
     weather gate has committed a decision for today and the on-disk row
     is still missing prediction columns. Populates eg4_today_kwh_raw,
     multiplier_applied, expected_kwh_used, decision_source. Does NOT
     touch actual_kwh — the dashboard will show "Actual: —" for today
     until sunset+30min.
  2. Sunset write (actual columns) — fires once per day, about 30 minutes
     after sunset. Computes actual_kwh from EG4's cumulative todayYielding
     (with trapezoidal integration of pv_power_w as a fallback) and
     classifies actual_end_reason (sunset / battery_full / unknown).
     Preserves prediction columns when already populated by the morning
     write; fills them in as a fallback if morning did not run.

Updates last_pv_log_date in wm_state.json ONLY when today's row is fully
populated (every prediction column present AND a strictly positive
actual_kwh). An incomplete row leaves the flag untouched so the next
tick after sunset+30min self-heals the missing fields.

Writes go through a temp-sibling + atomic rename so a crash mid-write
cannot truncate the log. Side-effect-free helpers live in
utils/pv_integration.py; this file owns the file-system and threading
concerns only.

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
# the on-disk header to this list; mismatch -> migrate-in-place against one
# of the known legacy schemas below, or archive + create fresh on unknown
# mismatch.
CSV_FIELDNAMES = [
    "date",
    "eg4_today_kwh_raw",
    "multiplier_applied",
    "expected_kwh_used",
    "actual_kwh",
    "ratio_actual_to_eg4_raw",
    "decision_source",
    "actual_end_reason",
    "start_soc_pct",
    "start_battery_kwh",
]

# Known legacy schemas. Each is the canonical column order at the historical
# point when the schema was last canonical. _reconcile_csv_schema migrates
# any of these in place to CSV_FIELDNAMES, preserving every existing row.
# All new columns introduced by a migration default to blank for legacy rows.
#
# V1 (7 cols): pre-actual_end_reason. The original canonical schema.
# V2 (8 cols): adds actual_end_reason. Was canonical until this change.
# V3 (9 cols): intermediate state if someone runs a half-rolled-out build that
#   adds start_soc_pct but not start_battery_kwh. Defensive — should not
#   occur in practice, but listed so a mixed-version deploy migrates cleanly.
_LEGACY_CSV_FIELDNAMES_V1 = [
    "date",
    "eg4_today_kwh_raw",
    "multiplier_applied",
    "expected_kwh_used",
    "actual_kwh",
    "ratio_actual_to_eg4_raw",
    "decision_source",
]

_LEGACY_CSV_FIELDNAMES_V2 = [
    "date",
    "eg4_today_kwh_raw",
    "multiplier_applied",
    "expected_kwh_used",
    "actual_kwh",
    "ratio_actual_to_eg4_raw",
    "decision_source",
    "actual_end_reason",
]

_LEGACY_CSV_FIELDNAMES_V3 = [
    "date",
    "eg4_today_kwh_raw",
    "multiplier_applied",
    "expected_kwh_used",
    "actual_kwh",
    "ratio_actual_to_eg4_raw",
    "decision_source",
    "actual_end_reason",
    "start_soc_pct",
]

# Ordered registry of legacy schemas the reconcile path can migrate in place.
# Newest-first because that's the most likely on-disk state during a rolling
# upgrade.
_KNOWN_LEGACY_SCHEMAS = (
    _LEGACY_CSV_FIELDNAMES_V3,
    _LEGACY_CSV_FIELDNAMES_V2,
    _LEGACY_CSV_FIELDNAMES_V1,
)

# Columns owned by the morning (prediction-context) write. The sunset write
# preserves these when they are already non-blank on disk.
_PREDICTION_COLUMNS = (
    "eg4_today_kwh_raw",
    "multiplier_applied",
    "expected_kwh_used",
    "decision_source",
)

# SOC threshold and minimum duration used by the actual_end_reason classifier.
# SOC at or above _BATTERY_FULL_SOC_PCT for at least _BATTERY_FULL_MIN_SEC,
# with the window ending strictly before sunset, classifies the day as
# battery_full (curtailment).
_BATTERY_FULL_SOC_PCT = 99.0
_BATTERY_FULL_MIN_SEC = 30 * 60

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
        get_battery_status: Optional[Callable[[], dict]] = None,
        get_battery_is_fresh: Optional[Callable[[], bool]] = None,
        get_battery_capacity_kwh: Optional[Callable[[], Optional[float]]] = None,
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
            get_battery_status: optional zero-arg callable returning the
                latest battery snapshot dict (BatteryService.get_status()).
                Used at morning-write time to capture start_soc_pct. Callback
                shape mirrors get_eg4_client — the underlying snapshot dict
                is replaced on every poll, so capturing a direct reference
                would silently go stale.
            get_battery_is_fresh: optional zero-arg callable returning the
                current battery-freshness gate (BatteryService.is_fresh()).
                Morning write captures start_soc_pct only when this returns
                True AND get_battery_status() yields a non-None soc_percent;
                a stale or missing snapshot leaves both start columns blank
                for that day rather than synthesizing a stale value.
            get_battery_capacity_kwh: optional zero-arg callable returning the
                rated battery capacity in kWh (or None if unknown). The
                derivation of start_battery_kwh = start_soc_pct / 100 *
                capacity uses this. When the callable returns None,
                start_battery_kwh is written blank while start_soc_pct is
                still captured.
        """
        self._state = state_manager
        self._weather = weather_service
        self._battery_log_path = battery_log_path
        self._prediction_log_path = prediction_log_path
        self._tz = ZoneInfo(timezone_str)
        self._tick_seconds = int(tick_seconds)
        self._post_sunset_seconds = int(post_sunset_seconds)
        self._get_eg4_client = get_eg4_client
        self._get_battery_status = get_battery_status
        self._get_battery_is_fresh = get_battery_is_fresh
        self._get_battery_capacity_kwh = get_battery_capacity_kwh

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
        """One pass: morning + sunset writes, each independently gated.

        The tick is split into two independent write paths, each idempotent:

          1. Morning write — runs whenever the weather gate has committed a
             decision for today (state.weather_gate_evaluated_date == today)
             AND today's on-disk row is missing prediction columns. Writes
             only the four prediction-context columns (eg4_today_kwh_raw,
             multiplier_applied, expected_kwh_used, decision_source).
             Does NOT touch actual_kwh.
          2. Sunset write — runs only past sunset + post_sunset_seconds.
             Writes actual_kwh, actual_end_reason, and recomputes
             ratio_actual_to_eg4_raw. Preserves prediction columns when
             they are already populated; only fills them in as a fallback
             if the morning write somehow didn't run.

        Self-healing semantics on the sunset path: the tick re-runs whenever
        the row is incomplete (per _is_today_row_complete). last_pv_log_date
        only advances once the row is fully populated.
        """
        now_local = datetime.now(self._tz)
        today_local = now_local.date()

        saved = self._state.load()
        last_logged_str = saved.get("last_pv_log_date")

        # The "complete and done" no-op condition: row is fully populated
        # AND the persisted flag matches today. Compute completeness before
        # any sunset gating so we can skip both write paths entirely.
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

        # ----- Morning write -----
        # Independent of sunset timing. As soon as the gate evaluates today,
        # the prediction columns become visible on the dashboard.
        self._maybe_write_morning_prediction(today_local, saved)

        # ----- Sunset gate -----
        forecast = self._weather.get_today_forecast() if self._weather else {}
        sunset_dt = forecast.get("sunset_dt")
        if sunset_dt is None:
            if __debug__:
                log("PV_LOG", "no sunset_dt yet; skip sunset write")
            return

        trigger_at = sunset_dt + timedelta(seconds=self._post_sunset_seconds)
        if now_local < trigger_at:
            return

        # ----- Sunset write (actual columns) -----
        actual_kwh, actual_source = self._resolve_actual_kwh(today_local)
        end_reason = self._classify_end_reason(today_local, sunset_dt)
        # Reload state in case the morning write or another writer touched it.
        saved = self._state.load()
        # Build the partial update: actual_kwh + end_reason + ratio.
        # Prediction columns are filled in as a FALLBACK only if missing on
        # disk and present in state (the morning write should have already
        # taken care of them).
        decision_ctx = self._read_gate_decision_context(saved, today_local)
        self._write_actual_columns(
            today_local,
            actual_kwh=actual_kwh,
            end_reason=end_reason,
            fallback_ctx=decision_ctx,
        )

        # Persist last_pv_log_date ONLY when the row is fully populated.
        if self._is_today_row_complete(today_local, saved):
            self._state.save(last_pv_log_date=today_local.isoformat())
            if __debug__:
                log(
                    "PV_LOG",
                    f"logged date={today_local} actual_kwh={actual_kwh:.2f} "
                    f"actual_source={actual_source} end_reason={end_reason} "
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
                    f"end_reason={end_reason} "
                    f"raw={decision_ctx.get('eg4_today_kwh_raw')} "
                    f"source={decision_ctx.get('decision_source')} "
                    f"flag_not_advanced",
                )

    def _maybe_write_morning_prediction(self, today: date, saved_state: dict) -> None:
        """Morning prediction-column write. Idempotent.

        Runs at most once per day. The trigger is:
          - state.weather_gate_evaluated_date == today, AND
          - today's row on disk is missing decision_source (i.e. the
            morning write has not happened yet today).

        Only the four prediction columns are written. actual_kwh and
        actual_end_reason are NOT touched here — they remain blank until
        the sunset+30min write. ratio_actual_to_eg4_raw is recomputed when
        actual_kwh is already non-blank on disk (this is the only time the
        ratio can be computed from prediction-column data alone).

        No-op when the gate has not evaluated today, when state's gate
        context is empty, or when the row already has decision_source.
        """
        ctx = self._read_gate_decision_context(saved_state, today)
        # If the gate has not committed a decision for today, nothing to write.
        if ctx.get("decision_source") is None:
            return

        # Idempotency guard: if today's on-disk row already has a non-blank
        # decision_source, the morning write already ran. Do nothing.
        existing = self._get_row(today)
        if existing is not None:
            on_disk_source = (existing.get("decision_source") or "").strip()
            if on_disk_source:
                return

        updates: dict[str, str] = {
            "eg4_today_kwh_raw": _fmt_num(ctx.get("eg4_today_kwh_raw")),
            "multiplier_applied": _fmt_num(ctx.get("multiplier_applied")),
            "expected_kwh_used": _fmt_num(ctx.get("expected_kwh_used")),
            "decision_source": ctx.get("decision_source") or "",
        }
        # Capture start-of-day battery state. Single atomic upsert with the
        # prediction columns — the morning write site is one call, not two,
        # per the schema-change spec. Blank when SOC is missing/stale or no
        # capacity is configured (see _capture_start_of_day_energy).
        start_soc_str, start_kwh_str = self._capture_start_of_day_energy()
        updates["start_soc_pct"] = start_soc_str
        updates["start_battery_kwh"] = start_kwh_str
        # If actual_kwh is already on disk (e.g. a backfill ran first),
        # recompute the ratio now using the new prediction value.
        if existing is not None:
            actual_str = (existing.get("actual_kwh") or "").strip()
            if actual_str:
                updates["ratio_actual_to_eg4_raw"] = _compute_ratio(
                    actual_str, updates["eg4_today_kwh_raw"]
                )

        self._upsert_row_partial(today, updates)
        if __debug__:
            log(
                "PV_LOG",
                f"morning write date={today} source={ctx.get('decision_source')} "
                f"raw={ctx.get('eg4_today_kwh_raw')} mult={ctx.get('multiplier_applied')} "
                f"expected={ctx.get('expected_kwh_used')} "
                f"start_soc={start_soc_str or '-'} start_kwh={start_kwh_str or '-'}",
            )

    def _capture_start_of_day_energy(self) -> tuple[str, str]:
        """Return (start_soc_pct_str, start_battery_kwh_str) for the morning write.

        Reads SOC from get_battery_status() at the moment of capture — the
        weather gate's persisted snapshot is not used because the gate runs
        once per day and the morning-write tick may be hours later. The
        battery freshness gate (get_battery_is_fresh) is consulted: stale or
        missing telemetry yields ("", "") so no synthesized value lands in
        the log.

        Capacity comes from get_battery_capacity_kwh(); when None or non-
        positive, start_battery_kwh is left blank while start_soc_pct is
        still captured. Capacity must remain a single source-of-truth read
        per write so a config change during a tick can't produce a
        mismatched (SOC, kWh) pair.
        """
        # No status callback configured (older tests, or a deploy that did
        # not wire battery_service) -> both columns blank.
        if self._get_battery_status is None:
            return ("", "")

        # Freshness gate, when supplied. A missing freshness callback
        # defaults to "trust the snapshot" to keep legacy callers working,
        # but production wiring should always supply both.
        if self._get_battery_is_fresh is not None:
            try:
                fresh = bool(self._get_battery_is_fresh())
            except Exception as exc:
                if __debug__:
                    log("PV_LOG", f"battery is_fresh callback raised: {exc}")
                return ("", "")
            if not fresh:
                if __debug__:
                    log("PV_LOG", "start-of-day capture skipped: battery stale")
                return ("", "")

        try:
            status = self._get_battery_status() or {}
        except Exception as exc:
            if __debug__:
                log("PV_LOG", f"battery status callback raised: {exc}")
            return ("", "")

        soc_raw = status.get("soc_percent") if isinstance(status, dict) else None
        if soc_raw is None:
            if __debug__:
                log("PV_LOG", "start-of-day capture skipped: soc_percent is None")
            return ("", "")
        try:
            soc_val = float(soc_raw)
        except (TypeError, ValueError):
            if __debug__:
                log("PV_LOG", f"start-of-day capture skipped: soc non-numeric ({soc_raw!r})")
            return ("", "")

        soc_str = _fmt_num(soc_val)

        # Capacity is optional — start_battery_kwh stays blank if missing.
        capacity_kwh: Optional[float] = None
        if self._get_battery_capacity_kwh is not None:
            try:
                cap_raw = self._get_battery_capacity_kwh()
            except Exception as exc:
                if __debug__:
                    log("PV_LOG", f"battery capacity callback raised: {exc}")
                cap_raw = None
            if cap_raw is not None:
                try:
                    cap_val = float(cap_raw)
                    if cap_val > 0:
                        capacity_kwh = cap_val
                except (TypeError, ValueError):
                    capacity_kwh = None

        if capacity_kwh is None:
            return (soc_str, "")

        start_kwh = soc_val / 100.0 * capacity_kwh
        return (soc_str, _fmt_num(start_kwh))

    def _write_actual_columns(
        self,
        today: date,
        *,
        actual_kwh: float,
        end_reason: str,
        fallback_ctx: dict,
    ) -> None:
        """Sunset-write path. Updates actual_kwh, actual_end_reason, and
        ratio_actual_to_eg4_raw. Preserves prediction columns when already
        populated on disk; only fills them from `fallback_ctx` when blank.
        """
        actual_str = f"{actual_kwh:.4f}"
        updates: dict[str, str] = {
            "actual_kwh": actual_str,
            "actual_end_reason": end_reason or "",
        }

        existing = self._get_row(today)
        # Recompute ratio from (effective) eg4_today_kwh_raw, which is whatever
        # ends up on disk after this write (existing value, fallback, or blank).
        existing_raw = (existing.get("eg4_today_kwh_raw") if existing else "") or ""

        # Prediction-column fallback fill. Only fills disk cells that are
        # currently blank — never overwrites a value the morning write put
        # there. Skip when fallback_ctx has nothing to offer.
        if fallback_ctx.get("decision_source") is not None:
            fallback_map = {
                "eg4_today_kwh_raw": _fmt_num(fallback_ctx.get("eg4_today_kwh_raw")),
                "multiplier_applied": _fmt_num(fallback_ctx.get("multiplier_applied")),
                "expected_kwh_used": _fmt_num(fallback_ctx.get("expected_kwh_used")),
                "decision_source": fallback_ctx.get("decision_source") or "",
            }
            for col, fallback_val in fallback_map.items():
                if not fallback_val:
                    continue
                on_disk_val = (existing.get(col) if existing else "") or ""
                if not on_disk_val.strip():
                    updates[col] = fallback_val
                    if col == "eg4_today_kwh_raw":
                        existing_raw = fallback_val

        # Recompute ratio against whichever raw value will be on disk after
        # this write — either the preserved morning value or the fallback fill.
        updates["ratio_actual_to_eg4_raw"] = _compute_ratio(actual_str, existing_raw)

        self._upsert_row_partial(today, updates)

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
        """Full-row upsert. Writes every non-key column from the supplied
        inputs, replacing any existing row for `day` in place.

        Retained for backwards compatibility with the test surface — tests
        call it directly to seed deterministic fixtures. Production code
        paths now use _upsert_row_partial (morning + sunset writes) to
        avoid clobbering columns that were written by the other path.
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
        positive eg4_today_kwh_raw are present; blank otherwise. Columns
        not present in `ctx` (actual_end_reason, start_soc_pct,
        start_battery_kwh) default to blank — _append_row callers (tests)
        need not provide them.
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
            "actual_end_reason": ctx.get("actual_end_reason") or "",
            "start_soc_pct": _fmt_num(ctx.get("start_soc_pct")),
            "start_battery_kwh": _fmt_num(ctx.get("start_battery_kwh")),
        }

    def _upsert_row_partial(self, day: date, updates: dict[str, str]) -> None:
        """Merge `updates` into today's row, preserving every column the
        caller did not specify.

        `updates` is a dict of column name -> formatted string value. Only
        these columns are written into the merged row; existing non-blank
        values for other columns are preserved verbatim.

        If no row for `day` exists yet, one is created with `updates`
        applied and all other columns blank.

        Write goes via temp-sibling + atomic rename — same durability
        guarantees as the full-row writer.
        """
        os.makedirs(os.path.dirname(self._prediction_log_path), exist_ok=True)

        existing_rows = self._read_all_rows()
        target_iso = day.isoformat()

        replaced = False
        merged_rows = []
        for row in existing_rows:
            if row.get("date") == target_iso:
                merged = self._blank_row(day)
                # Carry forward every column already on disk.
                for col in CSV_FIELDNAMES:
                    val = row.get(col, "")
                    if val is None:
                        val = ""
                    merged[col] = val
                # Apply the caller's updates over the top.
                for col, val in updates.items():
                    merged[col] = val if val is not None else ""
                merged_rows.append(merged)
                replaced = True
            else:
                merged_rows.append(row)
        if not replaced:
            new_row = self._blank_row(day)
            for col, val in updates.items():
                new_row[col] = val if val is not None else ""
            merged_rows.append(new_row)

        self._atomic_rewrite(merged_rows)
        if __debug__:
            cols = ",".join(sorted(updates.keys()))
            action = "replaced" if replaced else "appended"
            log("PV_LOG", f"partial {action} for {target_iso} cols=[{cols}]")

    def _blank_row(self, day: date) -> dict:
        """Return a fresh row dict with date set and all other columns blank."""
        return {col: ("" if col != "date" else day.isoformat()) for col in CSV_FIELDNAMES}

    def _get_row(self, day: date) -> Optional[dict]:
        """Return today's on-disk row dict (or None if no row exists)."""
        target_iso = day.isoformat()
        for row in self._read_all_rows():
            if row.get("date") == target_iso:
                return row
        return None

    def _classify_end_reason(self, day: date, sunset_dt: datetime) -> str:
        """Classify how the day's PV gathering ended.

        Returns one of:
          - "sunset" — PV gathering ended at sunset (normal).
          - "battery_full" — SOC sat at >=99% for >=30 consecutive minutes
            ending before sunset (curtailment).
          - "unknown" — insufficient battery-log data to classify.

        See module-level constants _BATTERY_FULL_SOC_PCT and
        _BATTERY_FULL_MIN_SEC for the exact thresholds.
        """
        return classify_end_reason(
            battery_log_path=self._battery_log_path,
            day=day,
            sunset_dt=sunset_dt,
            tz=self._tz,
        )

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

        Cases:
          - File missing or empty -> nothing to do; first write creates it.
          - Header matches CSV_FIELDNAMES -> keep using it.
          - Header matches a known legacy schema (V1/V2/V3 in
            _KNOWN_LEGACY_SCHEMAS) -> migrate in place, preserving every
            existing data row. Columns added by the migration default
            blank for legacy rows.
          - Any other header mismatch -> archive to a timestamped sibling
            and start a fresh canonical log.

        Migration is additive only — existing rows keep every previous
        column unchanged. New columns appear blank. _read_all_rows uses
        DictReader so missing-column values arrive as None and are
        normalised to "" by _upsert_row_partial / _atomic_rewrite.
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

        for legacy in _KNOWN_LEGACY_SCHEMAS:
            if header == legacy:
                existing_rows = self._read_all_rows()
                self._atomic_rewrite(existing_rows)
                added_cols = [c for c in CSV_FIELDNAMES if c not in legacy]
                added_str = ", ".join(repr(c) for c in added_cols) or "(none)"
                print(
                    f"[PVPredictionLogger] Migrated legacy CSV in place: "
                    f"added columns [{added_str}] (default blank) to "
                    f"{len(existing_rows)} existing rows."
                )
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


def _compute_ratio(actual_str: str, raw_str: str) -> str:
    """Return ratio_actual_to_eg4_raw as a formatted string, or '' when
    the division is impossible (missing/blank/non-positive raw)."""
    actual_str = (actual_str or "").strip()
    raw_str = (raw_str or "").strip()
    if not actual_str or not raw_str:
        return ""
    try:
        raw_val = float(raw_str)
    except (TypeError, ValueError):
        return ""
    if raw_val <= 0.0:
        return ""
    try:
        actual_val = float(actual_str)
    except (TypeError, ValueError):
        return ""
    return f"{actual_val / raw_val:.4f}"


def classify_end_reason(
    *,
    battery_log_path: str,
    day: date,
    sunset_dt: datetime,
    tz: ZoneInfo,
) -> str:
    """Classify how PV gathering ended on `day`.

    Scans miner_logs/eg4_battery_log.csv (path supplied) for rows in
    `day`'s local calendar window. Returns:
      - "sunset" — no contiguous SOC>=99% window of >=30 min ended before
        sunset.
      - "battery_full" — at least one contiguous SOC>=99% window of
        >=30 min ended strictly before sunset.
      - "unknown" — no battery-log coverage for the day, or no SOC samples
        before sunset.

    The classifier is pure (depends only on the supplied path and inputs)
    and is shared between the in-process sunset write and the backfill
    tool.

    Implementation note: rows are streamed in disk order, then sorted by
    parsed timestamp before window detection — the battery log can contain
    out-of-order rows after a session-refresh re-fetches a historical
    sample, and "longest contiguous window" only makes sense in time order.
    """
    if not os.path.exists(battery_log_path):
        return "unknown"

    day_start = datetime.combine(day, datetime.min.time(), tzinfo=tz)
    day_end = day_start + timedelta(days=1)
    sunset_local = sunset_dt.astimezone(tz)

    samples: list[tuple[datetime, float]] = []
    try:
        with open(battery_log_path, "r", newline="") as f:
            lines = _nul_stripped_lines(f)
            reader = csv.DictReader(lines)
            for raw_row in reader:
                ts_str = raw_row.get("ts") or ""
                soc_str = raw_row.get("soc_percent") or ""
                if not ts_str or not soc_str:
                    continue
                try:
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (TypeError, ValueError):
                    continue
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=tz)
                ts_local = dt.astimezone(tz)
                if not (day_start <= ts_local < day_end):
                    continue
                try:
                    soc = float(soc_str)
                except (TypeError, ValueError):
                    continue
                samples.append((ts_local, soc))
    except Exception:
        # A read failure is indistinguishable from missing coverage for
        # classification purposes.
        return "unknown"

    if not samples:
        return "unknown"

    samples.sort(key=lambda p: p[0])

    # No SOC samples before sunset means we can't observe the end-of-day
    # behavior at all.
    pre_sunset = [s for s in samples if s[0] <= sunset_local]
    if not pre_sunset:
        return "unknown"

    # Walk pre-sunset samples and find the longest contiguous SOC>=threshold
    # window. "Contiguous" is defined by adjacent samples both crossing the
    # threshold — gaps in the battery log do NOT split a window (gaps just
    # mean we did not observe SOC during that interval; both endpoints
    # already qualify).
    longest_start: Optional[datetime] = None
    longest_end: Optional[datetime] = None
    longest_dur = 0.0
    cur_start: Optional[datetime] = None
    cur_end: Optional[datetime] = None
    for ts, soc in pre_sunset:
        if soc >= _BATTERY_FULL_SOC_PCT:
            if cur_start is None:
                cur_start = ts
            cur_end = ts
        else:
            if cur_start is not None and cur_end is not None:
                dur = (cur_end - cur_start).total_seconds()
                if dur > longest_dur:
                    longest_dur = dur
                    longest_start = cur_start
                    longest_end = cur_end
            cur_start = None
            cur_end = None
    # Close the trailing window, if any.
    if cur_start is not None and cur_end is not None:
        dur = (cur_end - cur_start).total_seconds()
        if dur > longest_dur:
            longest_dur = dur
            longest_start = cur_start
            longest_end = cur_end

    if (
        longest_end is not None
        and longest_dur >= _BATTERY_FULL_MIN_SEC
        and longest_end < sunset_local
    ):
        return "battery_full"
    return "sunset"


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
