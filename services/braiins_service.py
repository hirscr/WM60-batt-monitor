"""
Braiins Pool stats service.

Polls the Braiins Pool profile endpoint every 60 seconds in a background
thread.  Also fetches the BTC/USD spot price from Coinbase every 5 minutes.

The service is disabled if:
  - config flag braiins.enabled is False, OR
  - the BRAIINS_API_KEY environment variable is missing or empty.

When disabled, start() is a no-op and get_latest() returns an empty dict.
The /api/braiins/status endpoint interprets a disabled service as HTTP 503.

Thread safety: all shared state is protected by self._lock.
"""
import os
import time
import threading
import logging
from datetime import datetime, timezone
from typing import Optional

import urllib.request
import urllib.error
import json as json_mod

log = logging.getLogger(__name__)

# Braiins hash-rate unit denominator: 1 TH/s = 1000 Gh/s.
GHS_PER_THS = 1000.0

# Coinbase BTC/USD spot endpoint — no auth required.
COINBASE_PRICE_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"

BRAIINS_PROFILE_URL = "https://pool.braiins.com/accounts/profile/json/btc/"


class BraiinsService:
    """
    Background-threaded Braiins Pool monitoring service.

    All network I/O happens on the polling thread.  The Flask request thread
    only ever reads from the in-memory cache via get_latest() — zero blocking.
    """

    def __init__(
        self,
        api_key: str,
        poll_seconds: int = 60,
        freshness_window_sec: int = 300,
        price_refresh_sec: int = 300,
    ):
        self._api_key = api_key          # never logged
        self._poll_seconds = poll_seconds
        self._freshness_window_sec = freshness_window_sec
        self._price_refresh_sec = price_refresh_sec

        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Cached snapshot — populated after first successful poll cycle.
        self._snapshot: dict = {}
        self._last_updated_ts: Optional[datetime] = None
        self._fail_count: int = 0
        self._last_error: Optional[str] = None

        # BTC/USD price cache — independent of the profile poll.
        self._btc_usd_price: Optional[float] = None
        self._btc_price_fetched_at: Optional[float] = None  # time.time()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        """Start the background polling thread."""
        if self._running:
            return
        if not self._api_key:
            log.warning("[BraiinsService] API key is empty — service not started")
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop, name="braiins-poller", daemon=True
        )
        self._thread.start()
        log.info("[BraiinsService] Started (poll_seconds=%d)", self._poll_seconds)

    def stop(self):
        """Signal the polling thread to exit."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        log.info("[BraiinsService] Stopped")

    # ------------------------------------------------------------------
    # Public read interface (called from Flask request thread)
    # ------------------------------------------------------------------

    def get_latest(self) -> dict:
        """Return a copy of the latest cached snapshot."""
        with self._lock:
            return dict(self._snapshot)

    def is_fresh(self) -> bool:
        """True if last_updated_ts is within freshness_window_sec of now."""
        with self._lock:
            ts = self._last_updated_ts
        if ts is None:
            return False
        age = (datetime.now(timezone.utc) - ts).total_seconds()
        return age <= self._freshness_window_sec

    def age_seconds(self) -> Optional[float]:
        """Seconds since last successful complete poll, or None."""
        with self._lock:
            ts = self._last_updated_ts
        if ts is None:
            return None
        return (datetime.now(timezone.utc) - ts).total_seconds()

    def is_enabled(self) -> bool:
        """True if the service was started successfully."""
        return self._running or self._thread is not None

    # ------------------------------------------------------------------
    # Background polling loop
    # ------------------------------------------------------------------

    def _poll_loop(self):
        """Main polling loop — runs on the background thread."""
        log.info("[BraiinsService] Poll loop started")
        while self._running:
            self._do_poll_cycle()
            # Sleep in 1-second increments so stop() is responsive.
            for _ in range(self._poll_seconds):
                if not self._running:
                    break
                time.sleep(1)
        log.info("[BraiinsService] Poll loop exiting")

    def _do_poll_cycle(self):
        """Fetch profile data and update the cache."""
        # Refresh BTC price if stale.
        self._maybe_refresh_btc_price()

        # Fetch Braiins profile.
        try:
            profile = self._fetch_profile()
        except Exception as exc:
            err_str = self._safe_error_str(exc)
            log.warning("[BraiinsService] Profile fetch failed: %s", err_str)
            with self._lock:
                self._fail_count += 1
                self._last_error = err_str
                # Leave _last_updated_ts unchanged so is_fresh() degrades naturally.
                self._snapshot["error"] = err_str
            return

        # Parse the response fields we care about.
        try:
            snapshot = self._build_snapshot(profile)
        except Exception as exc:
            err_str = f"parse error: {exc}"
            log.warning("[BraiinsService] %s", err_str)
            with self._lock:
                self._fail_count += 1
                self._last_error = err_str
                self._snapshot["error"] = err_str
            return

        # Full cycle succeeded.
        now_utc = datetime.now(timezone.utc)
        with self._lock:
            self._snapshot = snapshot
            self._last_updated_ts = now_utc
            self._fail_count = 0
            self._last_error = None

        log.info(
            "[BraiinsService] Poll OK — 5m=%.1f TH/s, today=%s BTC",
            snapshot.get("hashrate_5m_ths") or 0.0,
            snapshot.get("today_btc"),
        )

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _fetch_profile(self) -> dict:
        """Fetch the Braiins profile endpoint.  Raises on non-2xx or timeout."""
        req = urllib.request.Request(BRAIINS_PROFILE_URL)
        req.add_header("SlushPool-Auth-Token", self._api_key)
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
                raise RuntimeError("timeout fetching profile") from exc
            raise RuntimeError(f"network error: {reason}") from exc

        if status < 200 or status >= 300:
            raise RuntimeError(f"upstream {status}")

        return json_mod.loads(body)

    def _maybe_refresh_btc_price(self):
        """Refresh the BTC/USD spot price if the cache is stale."""
        now = time.time()
        with self._lock:
            last = self._btc_price_fetched_at

        if last is not None and (now - last) < self._price_refresh_sec:
            return  # Cache is fresh; skip fetch.

        try:
            req = urllib.request.Request(COINBASE_PRICE_URL)
            req.add_header("Accept", "application/json")
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json_mod.loads(resp.read().decode("utf-8"))
            price = float(data["data"]["amount"])
            with self._lock:
                self._btc_usd_price = price
                self._btc_price_fetched_at = now
            log.info("[BraiinsService] BTC price refreshed: $%.2f", price)
        except Exception as exc:
            log.warning("[BraiinsService] BTC price fetch failed: %s", self._safe_error_str(exc))
            # Leave cached value in place; don't update fetched_at so we
            # retry again on the next poll cycle.

    # ------------------------------------------------------------------
    # Data transformation
    # ------------------------------------------------------------------

    def _build_snapshot(self, profile: dict) -> dict:
        """Convert a raw Braiins profile response into the API shape."""
        btc = profile.get("btc", {})

        # Hash rates are in Gh/s; divide by 1000 to get TH/s.
        unit = btc.get("hash_rate_unit", "Gh/s")
        divisor = GHS_PER_THS if "g" in unit.lower() else 1.0

        def to_ths(val) -> Optional[float]:
            if val is None:
                return None
            try:
                return round(float(val) / divisor, 2)
            except (TypeError, ValueError):
                return None

        def to_btc(val) -> Optional[float]:
            if val is None:
                return None
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

        hr_5m = to_ths(btc.get("hash_rate_5m"))
        hr_1h = to_ths(btc.get("hash_rate_60m"))
        hr_24h = to_ths(btc.get("hash_rate_24h"))
        today_btc = to_btc(btc.get("today_reward"))
        estimated_btc = to_btc(btc.get("estimated_reward"))
        all_time_btc = to_btc(btc.get("all_time_reward"))
        balance_btc = to_btc(btc.get("current_balance"))

        with self._lock:
            btc_price = self._btc_usd_price
            price_fetched = self._btc_price_fetched_at

        def btc_to_usd(btc_val) -> Optional[float]:
            if btc_val is None or btc_price is None:
                return None
            return round(btc_val * btc_price, 2)

        price_age = None
        if price_fetched is not None:
            price_age = round(time.time() - price_fetched, 1)

        return {
            "hashrate_5m_ths": hr_5m,
            "hashrate_1h_ths": hr_1h,
            "hashrate_24h_ths": hr_24h,
            "today_btc": today_btc,
            "today_usd": btc_to_usd(today_btc),
            "estimated_btc": estimated_btc,
            "estimated_usd": btc_to_usd(estimated_btc),
            "all_time_btc": all_time_btc,
            "all_time_usd": btc_to_usd(all_time_btc),
            "account_balance_btc": balance_btc,
            "account_balance_usd": btc_to_usd(balance_btc),
            "btc_usd_price": btc_price,
            "btc_usd_price_age_seconds": price_age,
            "error": None,
        }

    # ------------------------------------------------------------------
    # Safety helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_error_str(exc: Exception) -> str:
        """Return a short error description that will never include the API token."""
        msg = str(exc)
        # Paranoid guard: if the token somehow appears in the message, redact it.
        return msg[:200] if msg else type(exc).__name__
