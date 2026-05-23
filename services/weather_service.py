"""Weather forecast service backed by the keyless Open-Meteo API.

Polls https://api.open-meteo.com/v1/forecast on a slow (hourly) cadence in a
background thread and caches today's forecast. The autocontrol weather gate
reads the snapshot via get_today_forecast() to decide whether the day's
expected solar harvest can refill the battery.

Open-Meteo requires no API key and has no auth header. The only inputs are
latitude / longitude / timezone, which are read from the existing
autocontrol.location config.

Threadsafety: all shared state is protected by self._lock. Network I/O happens
only on the background thread; the Flask request thread only reads the cache.

Logging follows the project's two-layer gate pattern:
    if __debug__:
        log("WEATHER", "...")
"""
from __future__ import annotations

import json as json_mod
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

from utils.log_config import log

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


class WeatherService:
    """Hourly-polled Open-Meteo forecast cache.

    Public surface (thread-safe):
        start() / stop()        — lifecycle
        get_today_forecast()    — most recent snapshot dict
        is_fresh()              — bool: snapshot inside freshness window
        age_seconds()           — float or None
    """

    def __init__(
        self,
        latitude: float,
        longitude: float,
        timezone_str: str,
        refresh_seconds: int = 3600,
        freshness_seconds: int = 7200,
        get_eg4_client: Optional[Callable] = None,
        eg4_freshness_seconds: Optional[int] = None,
    ):
        if refresh_seconds <= 0:
            raise ValueError("refresh_seconds must be positive")
        if freshness_seconds <= 0:
            raise ValueError("freshness_seconds must be positive")

        self._latitude = float(latitude)
        self._longitude = float(longitude)
        self._timezone_str = str(timezone_str)
        self._refresh_seconds = int(refresh_seconds)
        self._freshness_seconds = int(freshness_seconds)

        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Cached snapshot. Populated after first successful poll.
        # Shape: {"cloud_cover_pct": float, "sunrise_dt": datetime,
        #         "sunset_dt": datetime, "for_date": date, "fetched_at": datetime,
        #         "hourly_times": list[datetime], "hourly_cloud_cover": list[float]}
        self._snapshot: Optional[dict] = None
        self._last_error: Optional[str] = None

        # EG4 PV-predict cache. Separate from the Open-Meteo snapshot so a
        # failure on either side doesn't poison the other. The callback
        # pattern (rather than holding a direct EG4Client reference) is
        # essential: BatteryService.refresh_session() replaces the EG4Client
        # instance when the session is recovered, and a captured reference
        # would silently go stale.
        self._get_eg4_client = get_eg4_client
        self._eg4_freshness_seconds = int(eg4_freshness_seconds or freshness_seconds)
        self._eg4_lock = threading.Lock()
        self._eg4_refresh_lock = threading.Lock()  # serializes refresh attempts
        # Shape mirrors EG4Client.get_latest_pv_predict() plus a `for_date_obj`
        # parsed once at refresh time so the freshness check doesn't reparse.
        self._eg4_snapshot: dict = {
            "today_kwh": None,
            "tomorrow_kwh": None,
            "for_date": None,
            "tomorrow_date": None,
            "fetched_at": None,
            "last_error": None,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop, name="weather-poller", daemon=True
        )
        self._thread.start()
        print(
            f"[WeatherService] Started (refresh_seconds={self._refresh_seconds}, "
            f"freshness_seconds={self._freshness_seconds})"
        )

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        print("[WeatherService] Stopped")

    # ------------------------------------------------------------------
    # Public read interface
    # ------------------------------------------------------------------

    def get_today_forecast(self) -> dict:
        """Return today's forecast snapshot plus derived freshness info.

        Shape::

            {
                "cloud_cover_pct": float | None,
                "cloud_cover_remaining_daylight_pct": float | None,
                "sunrise_dt": datetime | None,
                "sunset_dt": datetime | None,
                "for_date": date | None,
                "fetched_at": datetime | None,
                "age_seconds": float | None,
                "is_fresh": bool,
                "last_error": str | None,
            }

        `cloud_cover_remaining_daylight_pct` is the arithmetic mean of hourly
        cloud cover from the current hour through the sunset hour, inclusive.
        It is None when we're past sunset, hourly data is missing/malformed,
        or no snapshot exists yet.
        """
        with self._lock:
            snap = dict(self._snapshot) if self._snapshot else None
            err = self._last_error

        eg4 = self._eg4_view()

        if snap is None:
            return {
                "cloud_cover_pct": None,
                "cloud_cover_remaining_daylight_pct": None,
                "sunrise_dt": None,
                "sunset_dt": None,
                "for_date": None,
                "fetched_at": None,
                "age_seconds": None,
                "is_fresh": False,
                "last_error": err,
                **eg4,
            }

        age = self._age_seconds_from(snap.get("fetched_at"))
        remaining = self._remaining_daylight_cloud_cover(
            snap.get("hourly_times"),
            snap.get("hourly_cloud_cover"),
            snap.get("sunset_dt"),
        )
        return {
            "cloud_cover_pct": snap.get("cloud_cover_pct"),
            "cloud_cover_remaining_daylight_pct": remaining,
            "sunrise_dt": snap.get("sunrise_dt"),
            "sunset_dt": snap.get("sunset_dt"),
            "for_date": snap.get("for_date"),
            "fetched_at": snap.get("fetched_at"),
            "age_seconds": age,
            "is_fresh": age is not None and age <= self._freshness_seconds,
            "last_error": err,
            **eg4,
        }

    def _eg4_view(self) -> dict:
        """Project the EG4 prediction cache into the get_today_forecast shape.

        Returns a dict with these keys (always present):
          eg4_today_kwh, eg4_tomorrow_kwh, eg4_fetched_at, eg4_is_fresh,
          eg4_last_error, eg4_for_date, eg4_tomorrow_date.
        """
        with self._eg4_lock:
            snap = dict(self._eg4_snapshot)
        age = self._age_seconds_from(snap.get("fetched_at"))
        return {
            "eg4_today_kwh": snap.get("today_kwh"),
            "eg4_tomorrow_kwh": snap.get("tomorrow_kwh"),
            "eg4_fetched_at": snap.get("fetched_at"),
            "eg4_is_fresh": (
                age is not None and age <= self._eg4_freshness_seconds
                and snap.get("today_kwh") is not None
            ),
            "eg4_last_error": snap.get("last_error"),
            "eg4_for_date": snap.get("for_date"),
            "eg4_tomorrow_date": snap.get("tomorrow_date"),
        }

    def is_fresh(self) -> bool:
        return self.get_today_forecast()["is_fresh"]

    def age_seconds(self) -> Optional[float]:
        return self.get_today_forecast()["age_seconds"]

    # ------------------------------------------------------------------
    # Internal: polling loop
    # ------------------------------------------------------------------

    def _poll_loop(self) -> None:
        if __debug__:
            log("WEATHER", "poll loop started")
        # Refresh immediately so the cache is warm before the first autocontrol tick.
        self._do_refresh()
        self._maybe_refresh_eg4_prediction()
        while self._running:
            for _ in range(self._refresh_seconds):
                if not self._running:
                    break
                time.sleep(1)
            if not self._running:
                break
            self._do_refresh()
            self._maybe_refresh_eg4_prediction()
        if __debug__:
            log("WEATHER", "poll loop exiting")

    def _maybe_refresh_eg4_prediction(self) -> None:
        """Refresh the EG4 cache on each poll pass when stale.

        "Stale" means: no successful fetch yet, OR the cache's fetched_at is
        older than _eg4_freshness_seconds. Once-per-pass is enough because
        the poll loop already runs at the Open-Meteo cadence (default 1h)
        which is well below the freshness window (default 2h).
        """
        if self._get_eg4_client is None:
            return
        view = self._eg4_view()
        if view.get("eg4_is_fresh"):
            return
        try:
            self.refresh_eg4_prediction()
        except Exception as exc:
            # Defensive: refresh_eg4_prediction() already swallows errors, but
            # keep the poll loop alive even if something unexpected escapes.
            print(f"[WeatherService] EG4 refresh raised: {exc}")
            if __debug__:
                log("WEATHER", f"eg4 refresh exception: {exc}")

    def refresh_eg4_prediction(self, force: bool = False) -> dict:
        """Synchronously refresh the EG4 PV-predict cache and return the view.

        This is the ONLY public path that performs the EG4 network call.
        Safe to call from any thread; serializes via _eg4_refresh_lock so
        concurrent callers don't pile up on the EG4 portal.

        Args:
            force: When True, refresh even if the cache is currently fresh.
                Used by /api/weather/evaluate_now so the operator can re-run
                the gate against the latest EG4 reading on demand.

        Returns the same dict shape as _eg4_view().
        """
        if self._get_eg4_client is None:
            if __debug__:
                log("WEATHER", "eg4 refresh skipped: no client callback wired")
            return self._eg4_view()

        if not force:
            view = self._eg4_view()
            if view.get("eg4_is_fresh"):
                return view

        # Serialize concurrent refreshes. Using a lock here (not just on the
        # snapshot) so the network round-trip doesn't fan out — the EG4
        # portal has rate-limits we don't want to test.
        acquired = self._eg4_refresh_lock.acquire(blocking=False)
        if not acquired:
            if __debug__:
                log("WEATHER", "eg4 refresh skipped: another refresh in flight")
            return self._eg4_view()

        try:
            client = self._get_eg4_client()
            if client is None:
                self._update_eg4_snapshot(last_error="eg4_client_unavailable")
                return self._eg4_view()
            try:
                fresh = client.refresh_pv_predict_blocking()
            except Exception as exc:
                err = f"{type(exc).__name__}: {exc}"[:200]
                self._update_eg4_snapshot(last_error=err)
                if __debug__:
                    log("WEATHER", f"eg4 refresh failed: {err}")
                return self._eg4_view()

            # client.refresh_pv_predict_blocking returns a dict snapshot with
            # the same keys we cache. Copy them through; fetched_at may be
            # None on failure — in that case keep our last good fetched_at
            # so freshness math doesn't suddenly flip just because the most
            # recent refresh hit an error.
            update = {
                "today_kwh": fresh.get("today_kwh"),
                "tomorrow_kwh": fresh.get("tomorrow_kwh"),
                "for_date": fresh.get("for_date"),
                "tomorrow_date": fresh.get("tomorrow_date"),
                "last_error": fresh.get("last_error"),
            }
            if fresh.get("fetched_at") is not None:
                update["fetched_at"] = fresh["fetched_at"]
            self._update_eg4_snapshot(**update)
            if __debug__:
                log(
                    "WEATHER",
                    f"eg4 refresh ok today_kwh={update['today_kwh']} "
                    f"tomorrow_kwh={update['tomorrow_kwh']} "
                    f"for_date={update['for_date']} "
                    f"last_error={update.get('last_error')}",
                )
            return self._eg4_view()
        finally:
            self._eg4_refresh_lock.release()

    def _update_eg4_snapshot(self, **fields) -> None:
        """Atomic replacement of the EG4 snapshot under _eg4_lock."""
        with self._eg4_lock:
            merged = dict(self._eg4_snapshot)
            merged.update(fields)
            self._eg4_snapshot = merged

    def _do_refresh(self) -> None:
        try:
            payload = self._fetch_forecast()
            snap = self._parse_payload(payload)
        except Exception as exc:
            err = self._safe_error_str(exc)
            with self._lock:
                self._last_error = err
            print(f"[WeatherService] Refresh failed: {err}")
            if __debug__:
                log("WEATHER", f"refresh failed: {err}")
            return

        with self._lock:
            self._snapshot = snap
            self._last_error = None

        print(
            f"[WeatherService] Forecast OK for {snap['for_date']}: "
            f"cloud_cover_mean={snap['cloud_cover_pct']}%"
        )
        if __debug__:
            log(
                "WEATHER",
                f"refresh ok for_date={snap['for_date']} "
                f"cloud_cover_pct={snap['cloud_cover_pct']} "
                f"sunrise={snap['sunrise_dt']} sunset={snap['sunset_dt']}",
            )

    # ------------------------------------------------------------------
    # Internal: HTTP + parse
    # ------------------------------------------------------------------

    def _fetch_forecast(self) -> dict:
        params = {
            "latitude": f"{self._latitude}",
            "longitude": f"{self._longitude}",
            "timezone": self._timezone_str,
            "daily": "cloud_cover_mean,sunrise,sunset",
            "hourly": "cloud_cover",
            "forecast_days": "1",
        }
        url = OPEN_METEO_URL + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.status
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"upstream {exc.code}") from exc
        except urllib.error.URLError as exc:
            reason = str(exc.reason)
            if "timed out" in reason.lower():
                raise RuntimeError("timeout") from exc
            raise RuntimeError(f"network error: {reason}") from exc

        if status < 200 or status >= 300:
            raise RuntimeError(f"upstream {status}")

        return json_mod.loads(body)

    def _parse_payload(self, payload: dict) -> dict:
        daily = payload.get("daily") or {}
        try:
            for_date_str = daily["time"][0]
            cloud_cover = daily["cloud_cover_mean"][0]
            sunrise_str = daily["sunrise"][0]
            sunset_str = daily["sunset"][0]
        except (KeyError, IndexError, TypeError) as exc:
            raise RuntimeError(f"unexpected payload shape: {exc}") from exc

        tz = ZoneInfo(self._timezone_str)
        for_date = date.fromisoformat(for_date_str)
        sunrise_dt = self._iso_local(sunrise_str, tz)
        sunset_dt = self._iso_local(sunset_str, tz)
        fetched_at = datetime.now(timezone.utc)

        # Hourly arrays are optional. Open-Meteo always returns them when
        # `hourly=` is in the query, but a defensive parse keeps the daily
        # snapshot intact when the hourly block is missing or malformed.
        hourly_times, hourly_cloud_cover = self._parse_hourly(payload, tz)

        return {
            "cloud_cover_pct": float(cloud_cover) if cloud_cover is not None else None,
            "sunrise_dt": sunrise_dt,
            "sunset_dt": sunset_dt,
            "for_date": for_date,
            "fetched_at": fetched_at,
            "hourly_times": hourly_times,
            "hourly_cloud_cover": hourly_cloud_cover,
        }

    @staticmethod
    def _parse_hourly(payload: dict, tz: ZoneInfo) -> tuple:
        """Pull hourly time + cloud cover arrays. Returns (None, None) when
        the hourly block is missing or malformed — callers treat that as
        'unknown remaining-daylight cloud cover'.
        """
        hourly = payload.get("hourly")
        if not isinstance(hourly, dict):
            return None, None
        times = hourly.get("time")
        clouds = hourly.get("cloud_cover")
        if not isinstance(times, list) or not isinstance(clouds, list):
            return None, None
        if len(times) != len(clouds):
            return None, None
        parsed_times: list = []
        for entry in times:
            try:
                dt = datetime.fromisoformat(entry)
            except (TypeError, ValueError):
                return None, None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz)
            parsed_times.append(dt)
        parsed_clouds: list = []
        for v in clouds:
            try:
                parsed_clouds.append(float(v))
            except (TypeError, ValueError):
                return None, None
        return parsed_times, parsed_clouds

    def _remaining_daylight_cloud_cover(
        self,
        hourly_times,
        hourly_cloud_cover,
        sunset_dt,
    ) -> Optional[float]:
        """Mean hourly cloud cover from the current hour through the sunset
        hour, inclusive. Returns None when past sunset, missing inputs, or
        malformed data leaves no eligible hours.

        Rule details:
          - Bin the current local time down to the top of the hour.
          - Include every hourly bucket whose timestamp is in
            [current_hour, sunset_hour] inclusive.
          - "Sunset hour" is the floor-hour of sunset_dt (so a 19:42 sunset
            keeps the 19:00 bucket in the average).
        """
        if not hourly_times or not hourly_cloud_cover or sunset_dt is None:
            return None
        if len(hourly_times) != len(hourly_cloud_cover):
            return None
        try:
            tz = sunset_dt.tzinfo or ZoneInfo(self._timezone_str)
        except Exception:
            return None
        now_local = self._now_local(tz)
        if now_local >= sunset_dt:
            return None
        current_hour = now_local.replace(minute=0, second=0, microsecond=0)
        sunset_hour = sunset_dt.replace(minute=0, second=0, microsecond=0)
        if sunset_hour < current_hour:
            return None
        values = []
        for ts, val in zip(hourly_times, hourly_cloud_cover):
            if not isinstance(ts, datetime):
                continue
            if current_hour <= ts <= sunset_hour:
                values.append(float(val))
        if not values:
            return None
        return sum(values) / len(values)

    @staticmethod
    def _iso_local(iso_str: str, tz: ZoneInfo) -> datetime:
        """Parse an Open-Meteo local-time ISO string and attach tz."""
        # Open-Meteo returns local times like "2026-05-17T05:42" (no offset)
        # when timezone= is set. fromisoformat handles either form.
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        return dt

    @staticmethod
    def _now_local(tz: ZoneInfo) -> datetime:
        """Wallclock 'now' in the given timezone. Wrapped so tests can patch
        a single method instead of mocking the datetime module wholesale.
        """
        return datetime.now(tz)

    @staticmethod
    def _age_seconds_from(fetched_at: Optional[datetime]) -> Optional[float]:
        if fetched_at is None:
            return None
        return (datetime.now(timezone.utc) - fetched_at).total_seconds()

    @staticmethod
    def _safe_error_str(exc: Exception) -> str:
        msg = str(exc)
        return msg[:200] if msg else type(exc).__name__
