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

    async def _poll_once(self):
        assert self._api is not None
        # Fetch battery & runtime via async endpoints
        batt = await self._api.get_inverter_battery_async()
        runtime = await self._api.get_inverter_runtime_async()

        # If no data, try re-login
        if not batt or not runtime:
            try:
                await self._api.login(ignore_ssl=True)
                # Retry fetch after re-login
                batt = await self._api.get_inverter_battery_async()
                runtime = await self._api.get_inverter_runtime_async()
                if not batt or not runtime:
                    self._last_error = "Still no data after re-login"
                    return
            except Exception as e:
                self._last_error = f"Re-login failed: {e}"
                return

        b = _to_plain(batt)
        r = _to_plain(runtime)
        # Rest of function remains the same...

        # Derive PV/Load/Grid/Battery powers from runtime
        pv_candidates = [_num(r.get(k)) for k in ("ppv1","ppv2","ppv3")]
        pv_vals = [v for v in pv_candidates if v is not None]
        pv_power_w = sum(pv_vals) if pv_vals else _num(r.get("ppv"))

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

        merged = {
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

        with self._lock:
            self._latest = merged
            self.history.append(merged)