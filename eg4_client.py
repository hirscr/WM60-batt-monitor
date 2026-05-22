#!/usr/bin/env python3
"""
eg4_client.py — background EG4 battery runtime fetcher with a simple sync API.

- Starts an asyncio event loop in a dedicated thread
- Logs in once and keeps the session alive
- Polls battery + runtime every BATTERY_POLL_SEC seconds
- Caches the latest merged snapshot (pack SOC/voltage/current, PV/load/grid/battery power, per-unit SOCs)
- Exposes sync methods: start(), stop(), get_latest(), get_history()
"""

import os, sys, json, time, threading, asyncio
from dataclasses import is_dataclass, asdict
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from eg4_inverter_api import EG4InverterAPI

# Half the 600s battery-freshness gate, so at least one recovery attempt is
# possible before the staleness threshold fires and autocontrol stops the miner.
RELOGIN_COOLDOWN_SEC = 300


def _is_empty_response(resp) -> bool:
    """Single authoritative rule for what counts as an empty EG4 response.

    A response is considered empty (i.e. the session is effectively dead and
    re-login is warranted) when:
      - resp is None or otherwise falsy
      - resp is a dict whose "success" field is explicitly False
      - resp is a dict whose "data" field is None

    All other dict responses with real data are considered non-empty.
    This rule is encoded here once; callers must not re-check it inline.
    """
    if not resp:
        return True
    if isinstance(resp, dict):
        if resp.get("success") is False:
            return True
        if resp.get("data") is None:
            return True
    return False


def _is_merged_snapshot_empty(merged: dict) -> bool:
    """Single authoritative rule for the "zombie session" silent-failure mode.

    Some EG4 portal failures (server-side session expiry that leaves the
    selected-inverter binding orphaned) return structurally-valid objects
    where every meaningful field is None or zero. _is_empty_response cannot
    catch this because the raw response is technically non-empty.

    A merged snapshot is considered empty when ALL of the following hold:
      - soc_percent is None
      - units is empty/falsy
      - pv_power_w is None or 0
      - load_power_w is None or 0
      - grid_power_w is None or 0
      - battery_net_w is None or 0

    Any single real reading (even a partial one) means the snapshot has
    useful data and must be merged normally. This rule is encoded here once;
    callers must not re-check it inline.
    """
    if not isinstance(merged, dict):
        return True
    if merged.get("soc_percent") is not None:
        return False
    if merged.get("units"):
        return False
    for k in ("pv_power_w", "load_power_w", "grid_power_w", "battery_net_w"):
        v = merged.get(k)
        if v is not None and v != 0 and v != 0.0:
            return False
    return True


def _to_plain(o):
    if o is None or isinstance(o, (str,int,float,bool)): return o
    if is_dataclass(o):  # handles nested dataclasses
        return {k: _to_plain(v) for k, v in asdict(o).items()}
    if hasattr(o, "model_dump"): return _to_plain(o.model_dump())
    if hasattr(o, "dict"): return _to_plain(o.dict())
    if isinstance(o, dict): return {str(k): _to_plain(v) for k,v in o.items()}
    if isinstance(o, (list,tuple,set)): return [_to_plain(v) for v in o]
    if hasattr(o, "__dict__"): return {k: _to_plain(v) for k,v in o.__dict__.items() if not k.startswith("_")}
    return str(o)

def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

class EG4Client:
    def __init__(self,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 base_url: str = "https://monitor.eg4electronics.com",
                 poll_seconds: int = 60,
                 history_max: int = 24*60):  # ~24h @ 60s
        self.username = username or os.getenv("EG4_USER") or os.getenv("USERNAME")
        self.password = password or os.getenv("EG4_PASS") or os.getenv("PASSWORD")
        self.base_url = base_url or os.getenv("EG4_BASE_URL", base_url)
        self.poll_seconds = int(os.getenv("BATTERY_POLL_SEC", str(poll_seconds)))
        self.history = deque(maxlen=history_max)

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()
        self._api: Optional[EG4InverterAPI] = None

        self._latest: Dict[str, Any] = {}
        self._last_error: Optional[str] = None
        self._lock = threading.Lock()

        # Monotonic timestamp of last re-login attempt; 0.0 = eligible immediately.
        self._last_relogin_attempt: float = 0.0

        if not self.username or not self.password:
            raise RuntimeError("EG4 creds not set (EG4_USER / EG4_PASS).")

    # ---------- Public sync API ----------
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._thread_main, name="EG4ClientThread", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0):
        self._stop_evt.set()
        if self._loop:
            try:
                asyncio.run_coroutine_threadsafe(self._shutdown(), self._loop).result(timeout)
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=timeout)

    def get_latest(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._latest) if self._latest else {}

    def get_history(self, limit: int = 120) -> list[Dict[str, Any]]:
        with self._lock:
            return list(self.history)[-limit:]

    def last_error(self) -> Optional[str]:
        return self._last_error

    def last_snapshot_ts(self) -> Optional[datetime]:
        """Return the timestamp of the most recent accepted snapshot, or None if no poll yet."""
        with self._lock:
            ts_str = self._latest.get("ts")
        if not ts_str:
            return None
        try:
            return datetime.fromisoformat(ts_str)
        except Exception:
            return None

    # ---------- Thread/async internals ----------
    def _thread_main(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        finally:
            try:
                self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            except Exception:
                pass
            self._loop.close()

    async def _run(self):
        # init API and login
        self._api = EG4InverterAPI(username=self.username, password=self.password, base_url=self.base_url)
        try:
            await self._api.login(ignore_ssl=True)
        except Exception as e:
            self._last_error = f"login_failed: {e}"
            # Keep trying every poll interval
        # pick first inverter if available (sync methods in lib)
        try:
            invs = self._api.get_inverters()
            if invs:
                self._api.set_selected_inverter(inverterIndex=0)
        except Exception as e:
            # non-fatal; will still try runtime/battery which may set inverter implicitly
            self._last_error = f"select_inverter_failed: {e}"

        # polling loop
        while not self._stop_evt.is_set():
            try:
                await self._poll_once()
                self._last_error = None
            except Exception as e:
                self._last_error = f"poll_error: {e}"
                # if auth expired, re-login once
                try:
                    await self._api.login(ignore_ssl=True)
                except Exception:
                    pass
            await asyncio.sleep(self.poll_seconds)

        await self._shutdown()

    async def _shutdown(self):
        try:
            if self._api:
                await self._api.close()
        except Exception:
            pass

    async def _attempt_recovery(self, reason_label: str):
        """Cooldown-gated re-login + inverter re-select + re-fetch.

        Returns (batt, runtime) on success — caller falls through to merge.
        Returns None if the cooldown is still active or if the re-login
        attempt still returns empty responses — caller must return early
        without advancing the snapshot ts (freshness gate handles safety).

        Centralizes the recovery sequence so the raw-empty and merged-empty
        paths share the same cooldown timer and the same inverter-reselect
        step (the latter is essential after a session expiry: the EG4
        library's selected-inverter binding is lost on re-login and must
        be re-established or subsequent fetches return all-null objects).
        """
        elapsed = time.monotonic() - self._last_relogin_attempt
        if elapsed < RELOGIN_COOLDOWN_SEC:
            self._last_error = "empty_response_cooldown"
            remaining = int(RELOGIN_COOLDOWN_SEC - elapsed)
            print(
                f"[EG4Client] {reason_label} — "
                f"in cooldown, skipping re-login (next eligible in {remaining}s)"
            )
            return None

        self._last_error = "empty_response_relogin_pending"
        print(f"[EG4Client] {reason_label} — attempting re-login")
        self._last_relogin_attempt = time.monotonic()
        try:
            await self._api.login(ignore_ssl=True)
        except Exception as e:
            self._last_error = f"Re-login failed: {e}"
            print(
                f"[EG4Client] Re-login attempted but still no data — "
                f"next retry in {RELOGIN_COOLDOWN_SEC}s"
            )
            return None

        # Re-select inverter — the binding is lost on session expiry.
        # Non-fatal if it errors; subsequent fetches may still succeed.
        try:
            invs = self._api.get_inverters()
            if invs:
                self._api.set_selected_inverter(inverterIndex=0)
        except Exception as e:
            print(f"[EG4Client] Re-select inverter failed (non-fatal): {e}")

        batt = await self._api.get_inverter_battery_async()
        runtime = await self._api.get_inverter_runtime_async()
        if _is_empty_response(batt) or _is_empty_response(runtime):
            self._last_error = "empty_response_relogin_pending"
            print(
                f"[EG4Client] Re-login attempted but still no data — "
                f"next retry in {RELOGIN_COOLDOWN_SEC}s"
            )
            return None
        print("[EG4Client] Recovered — session re-established, data flowing")
        return (batt, runtime)

    def _merge_response(self, batt, runtime) -> Dict[str, Any]:
        """Translate raw EG4 battery + runtime objects into the merged snapshot."""
        b = _to_plain(batt)
        r = _to_plain(runtime)

        # Derive PV/Load/Grid/Battery powers from runtime
        pv_candidates = [_num(r.get(k)) for k in ("ppv1","ppv2","ppv3")]
        pv_vals = [v for v in pv_candidates if v is not None]
        pv_power_w = sum(pv_vals) if pv_vals else _num(r.get("ppv"))

        # EPS (backup) power fields pEpsL1N and pEpsL2N give load power
        eps_l1 = _num(r.get("pEpsL1N")) or 0.0
        eps_l2 = _num(r.get("pEpsL2N")) or 0.0
        load_power_w = eps_l1 + eps_l2

        # Fallback to old fields if EPS fields not available
        if load_power_w == 0.0:
            load_power_w = _num(r.get("pToUser"))
            if load_power_w is None:
                load_power_w = _num(r.get("consumptionPower"))

        grid_power_w = _num(r.get("pToGrid"))
        ac_couple_w = _num(r.get("acCouplePower"))

        p_chg  = _num(r.get("pCharge"))
        p_dchg = _num(r.get("pDisCharge"))
        bat_net_w = (p_chg or 0.0) - (p_dchg or 0.0) if (p_chg is not None or p_dchg is not None) else None

        # Pack metrics
        remain = b.get("remainCapacity")
        full   = b.get("fullCapacity")
        units  = b.get("battery_units") or []

        if isinstance(remain,(int,float)) and isinstance(full,(int,float)) and full>0:
            soc_pct = round((remain/full)*100, 1)
        else:
            socs = [u.get("soc") for u in units if isinstance(u.get("soc"), (int,float))]
            soc_pct = round(sum(socs)/len(socs), 1) if socs else None

        pack_voltage = None
        if "totalVoltageText" in b:
            try: pack_voltage = float(b["totalVoltageText"])
            except: pass
        if pack_voltage is None and units:
            try:
                pack_voltage = round(sum(u.get("totalVoltage",0) for u in units)/100.0, 1)  # mV→V rough
            except: pass

        pack_current = None
        if "currentText" in b:
            try: pack_current = float(b["currentText"])
            except: pass

        return {
            "ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
            "soc_percent": soc_pct,
            "pack_voltage_v": pack_voltage,
            "pack_current_a": pack_current,
            "pv_power_w": pv_power_w,
            "load_power_w": load_power_w,
            "grid_power_w": grid_power_w,
            "ac_couple_w": ac_couple_w,
            "battery_net_w": bat_net_w,
            "units": [
                {"sn": u.get("batterySn"), "soc": u.get("soc"),
                 "voltage_mv": u.get("totalVoltage"), "current_a": u.get("current")}
                for u in units
            ]
        }

    async def _poll_once(self):
        assert self._api is not None
        # Fetch battery & runtime via async endpoints
        batt = await self._api.get_inverter_battery_async()
        runtime = await self._api.get_inverter_runtime_async()

        # Path 1: structurally empty raw response (success=False / data=None /
        # falsy). Classic silent session expiry — handled by _attempt_recovery.
        batt_empty = _is_empty_response(batt)
        runtime_empty = _is_empty_response(runtime)
        if batt_empty or runtime_empty:
            which = []
            if batt_empty:
                which.append("battery")
            if runtime_empty:
                which.append("runtime")
            endpoints_desc = "+".join(which)
            recovered = await self._attempt_recovery(
                f"Empty response from {endpoints_desc}"
            )
            if recovered is None:
                return  # cooldown or re-login still empty; freshness gate handles safety
            batt, runtime = recovered

        merged = self._merge_response(batt, runtime)

        # Path 2: "zombie session" — raw response was structurally valid but
        # every meaningful field is None/0 after merge. Typically caused by a
        # lost inverter-selection binding after server-side session expiry.
        # _attempt_recovery re-logs in AND re-selects the inverter.
        if _is_merged_snapshot_empty(merged):
            recovered = await self._attempt_recovery("Zombie session (merged snapshot all-null)")
            if recovered is None:
                return
            batt, runtime = recovered
            merged = self._merge_response(batt, runtime)
            if _is_merged_snapshot_empty(merged):
                self._last_error = "merged_snapshot_empty_after_recovery"
                print(
                    f"[EG4Client] Re-login attempted but data still all-null — "
                    f"next retry in {RELOGIN_COOLDOWN_SEC}s"
                )
                return

        with self._lock:
            self._latest = merged
            self.history.append(merged)

        # Terse one-line poll summary (replaces the prior verbose dumps)
        soc_pct = merged.get("soc_percent")
        pv_power_w = merged.get("pv_power_w")
        load_power_w = merged.get("load_power_w")
        soc_s = f"{soc_pct}%" if soc_pct is not None else "?"
        pv_s = f"{int(pv_power_w)}w" if pv_power_w is not None else "?"
        load_s = f"{int(load_power_w)}w" if load_power_w is not None else "?"
        print(f"[EG4Client] poll ok SOC={soc_s} PV={pv_s} load={load_s}")