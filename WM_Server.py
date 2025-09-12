#!/usr/bin/env python3
# WM_server.py — Flask server that exposes WhatsMiner status & control APIs
# Requires: wm_controller.py in the same folder with class WhatsMinerClientPlain

import hashlib
import os, csv
from collections import deque
import math
import logging
from datetime import datetime, timezone
import tzlocal

from flask import Flask, jsonify, request, send_from_directory

from typing import Optional

# --- EG4 battery client ---
from eg4_client import EG4Client

# ---- BTMiner routines
from pyasic.rpc.btminer import BTMinerRPCAPI

# ----- routines to maintain state of server through apower cycle
from WM_State import StateManager
state_mgr = StateManager(path="./wm_state.json")  # choose a durable path

# Config from env
EG4_USER = os.environ.get("EG4_USER") or os.environ.get("USERNAME")
EG4_PASS = os.environ.get("EG4_PASS") or os.environ.get("PASSWORD")
EG4_BASE = os.environ.get("EG4_BASE_URL", "https://monitor.eg4electronics.com")

# Poll cadence
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "10"))

# --- Auto control config (place near other constants/env reads) ---
AUTO_MIN_INTERVAL_SEC = int(os.environ.get("AUTO_MIN_INTERVAL_SEC", "120"))
BASE_WATTS = int(os.getenv("WM_BASE_WATTS", "3600"))
MIN_WATT = 0
AUTO_LOW_CAP_W = int(os.environ.get("AUTO_LOW_CAP_W", "3200"))  # cap before full recharge
AUTO_TARGET_W: Optional[int] = None
AUTO_TARGET_PCT: Optional[int] = None
AUTO_MINER_OFF_DUE_TO_SOC: bool = False

# Ratchet floor: only decreases with SOC; resets to BASE_WATTS once SOC == 100%
AUTO_FLOOR_W = BASE_WATTS
COOLDOWN_TARGET_C = 35.0
COOLDOWN_POLL_S = 5.0  # poll only every 5 seconds, per requirement

# Latched-floor state and last set tracking
AUTO_LAST_SET_W: Optional[int] = None
AUTO_LAST_SET_TS: float = 0.0
LATCHED_FLOOR_W: Optional[int] = None

# State
battery_latest = {}   # last good snapshot (normalized)
battery_history = deque()  # keep all rows ever loaded


# ---------- Config ----------

HISTORY_MAX = int(os.environ.get("HISTORY_MAX", str(24*60*60 // max(1, POLL_SECONDS))))  # ~24h default
DEFAULT_LIMIT = int(os.environ.get("DEFAULT_LIMIT", "3000"))  # used for resume if no last nonzero limit

DASHBOARD_FILE = "WM_Dashboard.html"

POLL_SEC = 10
BASE_DIR = os.path.dirname(__file__)
LOG_DIR  = os.path.join(BASE_DIR, "miner_logs")
LOG_FILE = os.path.join(LOG_DIR, "wm_status_log.csv")
# Battery log (EG4) lives alongside miner logs
BATT_LOG_FILE = os.path.join(LOG_DIR, "eg4_battery_log.csv")

# Global logging interval (seconds)
LOG_INTERVAL_SEC = int(os.environ.get("LOG_INTERVAL_SEC", str(3600)))  # default: 1 hour

_history = deque(maxlen=10000)   # browser history window
_latest  = {}                    # last good sample

# ---------- App/state ----------
app = Flask(__name__, static_folder=".", static_url_path="")

WM_HOST = os.environ.get("WM_HOST", "").strip()
WM_PASS = os.environ.get("WM_PASS", "admin")
if not WM_HOST:
    raise SystemExit("Set WM_HOST (e.g., export WM_HOST=192.168.86.52)")

# SINGLE shared client
api = BTMinerRPCAPI(WM_HOST)
api.pwd = WM_PASS

# Restore saved values on startup
state = state_mgr.load()
AUTOCONTROL = state.get("autocontrol", False)
TARGET_POWER_PCT = state.get("target_power_pct", 0)
MINER_POWER_STATE = state.get("miner_power_state", "stopped")

# Make the runtime switch match persisted state
AUTO_ENABLED = bool(AUTOCONTROL)

def restore_runtime():
    if AUTOCONTROL:
        if MINER_POWER_STATE == "stopped":
            # stay stopped
            pass
        else:
            set_autocontrol_target(TARGET_POWER_PCT)
    else:
        pass

def set_autocontrol_enabled(enabled: bool):
    global AUTOCONTROL
    AUTOCONTROL = enabled
    state_mgr.save(autocontrol=AUTOCONTROL)

def set_autocontrol_target(pct: int):
    global TARGET_POWER_PCT
    TARGET_POWER_PCT = max(0, min(100, int(pct)))
    state_mgr.save(target_power_pct=TARGET_POWER_PCT)

def set_miner_power_state(state_str: str):
    global MINER_POWER_STATE
    MINER_POWER_STATE = state_str
    state_mgr.save(miner_power_state=MINER_POWER_STATE)


# --- BEGIN Miner control queue integration ---
import asyncio
import threading, queue, time
from typing import Optional, Dict, Any

class MinerController:
    def __init__(self, api: BTMinerRPCAPI):
        self.api = api
        self.q = queue.Queue()
        # keep "error" a string, not None
        self.state: dict[str, object] = {
            "op_state": "idle",
            "op_kind": None,
            "last_sent_command": None,
            "error": "",           # <- string, not None
            "started_at": None,
            "request": None,
        }
        t = threading.Thread(target=self._worker, daemon=True)
        t.start()

    def status_snapshot(self) -> Dict[str, Any]:
        return dict(self.state)

    # enqueue APIs
    def enqueue_stop(self):
        self.q.put(("stop", {}))

    def enqueue_resume(self):
        self.q.put(("resume", {}))

    def enqueue_set_power_limit(self, watts: int):
        self.q.put(("power_limit", {"watts": int(watts)}))

    def enqueue_set_power_pct(self, percent: int):
        self.q.put(("power_pct", {"percent": int(percent)}))

    # worker helpers
    def _verify(self, kind: str, req: Dict[str, Any]) -> bool:
        try:
            summary = asyncio.run(self.api.summary())
            if not isinstance(summary, dict):
                return False
            lst = summary.get("SUMMARY") or []
            s = lst[0] if lst and isinstance(lst[0], dict) else {}
            if kind == "stop":
                return s.get("is_mining") is False
            if kind == "resume":
                return s.get("is_mining") is True
            if kind == "power_limit":
                return str(s.get("Power Limit")) == str(req["watts"])
            if kind == "power_pct":
                return True
        except Exception:
            return False
        return True

    def _run_op(self, kind: str, req: Dict[str, Any]):
        self.state.update({
            "op_state": "applying",
            "op_kind": kind,
            "error": None,
            "started_at": time.time(),
            "request": dict(req),
            "last_sent_command": kind,
        })
        try:
            if kind == "stop":
                asyncio.run(self.api.power_off())
                set_miner_power_state("stopped")
            elif kind == "resume":
                asyncio.run(self.api.power_on())
                set_miner_power_state("running")
            elif kind == "power_limit":
                asyncio.run(
                    self.api.send_privileged_command(
                        "set_power_limit", power_limit=str(req["watts"])
                    )
                )
            elif kind == "power_pct":
                asyncio.run(self.api.set_power_pct(req["percent"]))
                set_autocontrol_target(req["percent"])  # persist target percent
            else:
                raise ValueError(f"Unknown op {kind}")

            self.state["op_state"] = "verifying"
            ok = self._verify(kind, req)
            if not ok:
                raise RuntimeError(f"verification failed for {kind}")

            self.state["op_state"] = "idle"
        except Exception as e:
            self.state["op_state"] = "error"
            self.state["error"] = str(e)

    def _worker(self):
        while True:
            kind, req = self.q.get()
            try:
                self._run_op(kind, req)
            finally:
                self.q.task_done()

# instantiate controller
miner_ctrl = MinerController(api)

# --- BEGIN control endpoints ---

def _json_ok(**kwargs):
    return jsonify({"ok": True, **kwargs})

def _json_err(msg, code=400):
    return jsonify({"ok": False, "error": str(msg)}), code

@app.post("/set_power_limit")
def set_power_limit():
    """Plaintext path: set Max Power (W). Button should turn yellow until verified."""
    try:
        data = request.get_json(force=True) or {}
        watts = int(data.get("watts"))
        if watts <= 0:
            return _json_err("watts must be > 0")
    except Exception as e:
        return _json_err(f"invalid payload: {e}")
    miner_ctrl.enqueue_set_power_limit(watts)
    return _json_ok(queued=True, op="power_limit", watts=watts)

@app.post("/set_power_pct")
def set_power_pct():
    """Ciphertext path via WM_Cipher: set power percent (0..100).
       Firmware will stop & cool to 35C before applying; our queue polls every 5s.
    """
    try:
        data = request.get_json(force=True) or {}
        percent = int(data.get("percent"))
        if not 0 <= percent <= 100:
            return _json_err("percent must be 0..100")
    except Exception as e:
        return _json_err(f"invalid payload: {e}")
    miner_ctrl.enqueue_set_power_pct(percent)
    return _json_ok(queued=True, op="power_pct", percent=percent)

@app.post("/stop")
def stop_mining():
    miner_ctrl.enqueue_stop()
    set_miner_power_state("stopped")  # persist
    return _json_ok(queued=True, op="stop")

@app.post("/resume")
def resume_mining():
    miner_ctrl.enqueue_resume()
    set_miner_power_state("running")  # persist
    return _json_ok(queued=True, op="resume")

@app.get("/op_status")
def op_status():
    """Lightweight status for the UI to track apply/cooldown/verify phases."""
    return jsonify(miner_ctrl.status_snapshot())

# --- END control endpoints ---

# Ensure EG4 creds exist and pass them explicitly
if not EG4_USER or not EG4_PASS:
    raise SystemExit("Set EG4_USER and EG4_PASS in your Run Configuration (Environment variables).")
print(f"[BOOT] EG4 username detected: {EG4_USER!r} (password not shown)")
eg4 = EG4Client(username=EG4_USER, password=EG4_PASS, base_url=EG4_BASE, poll_seconds=60)
eg4.start()

HISTORY: deque = deque()  # keep all rows ever loaded
latest: Dict[str, Any] = {}
last_nonzero_limit: Optional[int] = None
standby_flag = False

def _load_existing_csv(file_path: str, target_deque: deque):
    """Load existing CSV into memory at startup."""
    if not os.path.exists(file_path):
        print(f"[LOAD] no file {file_path}")
        return
    try:
        with open(file_path, "r", newline="") as f:
            rdr = csv.DictReader(f)
            rows = list(rdr)
            for row in rows:
                # add each row as-is
                target_deque.append(row)
        print(f"[LOAD] loaded {len(rows)} rows from {file_path}")
    except Exception as e:
        print(f"[LOAD] error reading {file_path}: {e}")

def _now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

def _extract_summary_item(reply: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not reply or "SUMMARY" not in reply or not reply["SUMMARY"]:
        return {}
    item = reply["SUMMARY"][0]
    return item if isinstance(item, dict) else {}

def _safe_int(d: Dict[str, Any], key: str) -> Optional[int]:
    try:
        return int(d.get(key)) if key in d else None
    except Exception:
        return None

def _watts_for_soc(
    soc: Optional[float],
    last_set_w: Optional[int],
    latched_floor_w: Optional[int],
) -> int:
    """
    Decile mapping with a 'latched floor':
      - Map SOC to power in 10% steps, rounded UP to the next decile.
        E.g., 55% -> 60%, 83% -> 90%, 96% -> 100%.
      - Once power has been reduced below 100%, we latch the lowest power ever set
        and do NOT allow increases above that latched floor until SOC == 100%.
      - When SOC == 100%, allow 100% power again (release the latch).
      - If SOC is None, hold last_set_w if available; otherwise default to BASE_WATTS.
    """
    if soc is None:
        return last_set_w if last_set_w is not None else BASE_WATTS

    # Compute decile percent rounded UP, clamp 0..100
    pct = min(100, int(math.ceil(soc / 10.0) * 10))
    candidate = int(round(BASE_WATTS * (pct / 100.0)))

    # Apply the latched floor while SOC is below 100%
    if latched_floor_w is not None and soc < 100.0:
        candidate = min(candidate, latched_floor_w)

    # Clamp to global bounds
    return max(MIN_WATT, min(BASE_WATTS, candidate))

def _extract_hashrate_ths_from_summary(item: Dict[str, Any]) -> Optional[float]:
    """Derive hashrate in TH/s from a WhatsMiner SUMMARY item."""
    if not isinstance(item, dict):
        return None

    # 1) Direct TH/s fields (some firmwares)
    for k in ("THS 5s", "THS av", "TH/s", "THS"):
        if k in item:
            try:
                v = float(item[k])
                return v if v >= 0 else None
            except Exception:
                pass

    # 2) GH/s fields (most cgminer-style summaries)
    for k in ("GHS 5s", "GHS av", "GH/s", "GHS"):
        if k in item:
            try:
                v = float(item[k])  # GH/s
                return v / 1000.0 if v >= 0 else None  # TH/s
            except Exception:
                pass

    # 3) MH/s fields (rare on SHA256, but handle just in case)
    for k in ("MHS 5s", "MHS av", "MH/s", "MHS"):
        if k in item:
            try:
                v = float(item[k])  # MH/s
                return v / 1_000_000.0 if v >= 0 else None  # TH/s
            except Exception:
                pass

    # 4) Fallback: sometimes a flat 'Hashrate' exists (assume TH/s)
    if "Hashrate" in item:
        try:
            v = float(item["Hashrate"])
            return v if v >= 0 else None
        except Exception:
            pass

    return None

def poller():
    """Background: poll summary every POLL_SECONDS, keep latest/history fresh."""
    global latest, last_nonzero_limit
    while True:
        try:
            reply = asyncio.run(api.summary())
            item = _extract_summary_item(reply)
            if item:
                pl = _safe_int(item, "Power Limit")
                if pl and pl > 0:
                    last_nonzero_limit = pl
                row = {"ts": _now_iso()}
                for k, v in item.items():
                    row[str(k)] = v

                # Compute Hashrate (TH/s) from SUMMARY and add to the row
                hr_ths = _extract_hashrate_ths_from_summary(item)
                row["Hashrate"] = round(hr_ths, 1) if hr_ths is not None else None

                # Compute Efficiency (W/TH) if we have both Power and Hashrate
                try:
                    pwr = row.get("Power")
                    if isinstance(pwr, (int, float)) or (isinstance(pwr, str) and pwr.isdigit()):
                        pwr_val = float(pwr)
                    else:
                        pwr_val = None
                except Exception:
                    pwr_val = None

                if pwr_val is not None and hr_ths and hr_ths > 0:
                    row["Efficiency"] = round(pwr_val / hr_ths, 1)
                else:
                    row["Efficiency"] = None

                latest = row
                HISTORY.append(row)
                now_ts = time.time()
                if not hasattr(poller, "_last_log_ts"):
                    poller._last_log_ts = 0.0
                if (now_ts - poller._last_log_ts) >= LOG_INTERVAL_SEC:
                    _append_csv(row)
                    poller._last_log_ts = now_ts
        except Exception:
            pass
        time.sleep(POLL_SECONDS)

def battery_poller():
    """Background thread: poll EG4 battery snapshot and log CSV using the running 'eg4' client."""
    global battery_latest

    while True:
        try:
            snap = eg4.get_latest()  # use the already-running EG4Client instance
            if isinstance(snap, dict) and snap:
                # keep in-memory state
                battery_latest = snap.copy()
                battery_history.append(snap)

                # build flat CSV row
                row = {
                    "ts": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
                    "pv_power_w": snap.get("pv_power_w"),
                    "load_power_w": snap.get("load_power_w"),
                    "battery_net_w": (
                        snap.get("battery_net_w")
                        if snap.get("battery_net_w") is not None
                        else (
                            round(snap["pack_voltage_v"] * snap["pack_current_a"])
                            if (snap.get("pack_voltage_v") is not None and snap.get("pack_current_a") is not None)
                            else None
                        )
                    ),
                    "soc_percent": snap.get("soc_percent"),
                    "pack_voltage_v": snap.get("pack_voltage_v"),
                    "pack_current_a": snap.get("pack_current_a"),
                }
                # per-battery fields
                units = snap.get("units") or []
                for idx, u in enumerate(units, start=1):
                    key = f"Battery_{idx:02d}"
                    row[f"{key}_sn"] = u.get("sn")
                    row[f"{key}_soc"] = u.get("soc")
                    v_mv = u.get("voltage_mv")
                    # library reports 0.01V units (e.g., 5295 == 52.95V) → divide by 100
                    row[f"{key}_voltage_v"] = (round(v_mv / 100.0, 2) if isinstance(v_mv, (int, float)) else None)
                    row[f"{key}_current_a"] = u.get("current_a")

                # Log only once per LOG_INTERVAL_S
                now_ts = time.time()
                if not hasattr(battery_poller, "_last_log_ts"):
                    battery_poller._last_log_ts = 0.0
                if (now_ts - battery_poller._last_log_ts) >= 3600:  # once per hour
                    _append_csv_generic(BATT_LOG_FILE, row)
                    battery_poller._last_log_ts = now_ts
        except Exception:
            # swallow hiccups and keep polling
            pass

        time.sleep(POLL_SECONDS)

def _html_sig():
    """Return (mtime_sec, sha1) for the dashboard file so the browser can auto-reload when it changes."""
    try:
        path = os.path.join(".", DASHBOARD_FILE)
        mtime = int(os.path.getmtime(path))
        with open(path, "rb") as f:
            sha = hashlib.sha1(f.read()).hexdigest()
        return mtime, sha
    except Exception:
        return 0, "missing"

#-----helpers--------
def _flatten_summary(summary_obj):
    """
    Accepts the JSON dict you already build from the miner (the one that has SUMMARY[0]).
    Returns a flat dict with a timestamp plus all key/values.
    """
    ts = time.strftime("%Y-%m-%d %H:%M:%S")  # server-side timestamp
    row = {"ts": ts}
    if not summary_obj:   # defensive
        return row
    s = summary_obj.get("SUMMARY", [{}])[0] if isinstance(summary_obj, dict) else {}
    # merge all top-level fields
    for k, v in s.items():
        row[k] = v
    return row


def _append_csv(row):
    os.makedirs(LOG_DIR, exist_ok=True)
    file_exists = os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def _append_csv_generic(file_path: str, row: dict):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_exists = os.path.exists(file_path)
    with open(file_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

def _load_battery_history_from_csv():
    """Load all EG4 rows from BATT_LOG_FILE into battery_history on startup."""
    try:
        if not os.path.exists(BATT_LOG_FILE):
            print(f"[LOAD] battery log not found: {BATT_LOG_FILE}")
            return
        count = 0
        with open(BATT_LOG_FILE, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if isinstance(row, dict) and row.get("ts"):
                    battery_history.append(row)
                    count += 1
        print(f"[LOAD] battery history loaded: {count} rows from {BATT_LOG_FILE}")
    except Exception as e:
        print(f"[LOAD] battery history error: {e}")

def _fresh_batt(max_age_s: int = 90) -> Optional[dict]:
    """Return the latest EG4 snapshot from the running client if fresh; else None."""
    snap = eg4.get_latest() if ("eg4" in globals() and eg4 is not None) else None
    if not isinstance(snap, dict) or not snap:
        return None
    ts = snap.get("ts")
    try:
        age = time.time() - datetime.fromisoformat(ts).timestamp()
        if age > max_age_s:
            return None
    except Exception:
        # If ts is missing or malformed, treat as stale
        return None
    return snap

def _load_miner_history_from_csv():
    """Load all miner rows from LOG_FILE into HISTORY on startup."""
    try:
        if not os.path.exists(LOG_FILE):
            print(f"[LOAD] miner log not found: {LOG_FILE}")
            return
        count = 0
        with open(LOG_FILE, "r", newline="") as f:
            reader: csv.DictReader[str] = csv.DictReader(f)
            for row in reader:
                row: dict[str, str]  # <-- tell linter explicitly
                if row.get("ts"):
                    HISTORY.append(row)
                    count += 1
        print(f"[LOAD] miner history loaded: {count} rows from {LOG_FILE}")
    except Exception as e:
        print(f"[LOAD] miner history error: {e}")

def is_past_sunset():
    # Example: hardcoded sunset at 19:00 local time, adjust as needed

    local_tz = tzlocal.get_localzone()
    now = datetime.now(local_tz)
    sunset_hour = int(os.environ.get("SUNSET_HOUR", "19"))
    sunset_minute = int(os.environ.get("SUNSET_MINUTE", "0"))
    sunset = now.replace(hour=sunset_hour, minute=sunset_minute, second=0, microsecond=0)
    return now > sunset

def auto_controller():
    """
    Non-blocking, time-gated control loop:
      - Never sleeps; uses elapsed-time checks to decide when to poll and when to act.
      - Switch (AUTO_ENABLED) is checked every iteration for instant responsiveness.
      - Battery snapshot is refreshed at most every BATT_REFRESH_S seconds.
      - Control decisions are evaluated at most every CTRL_EVAL_S seconds.
      - Miner setpoint updates are still rate-limited by AUTO_MIN_INTERVAL_SEC.
    """
    global AUTO_LAST_SET_W, AUTO_LAST_SET_TS, LATCHED_FLOOR_W, AUTO_ENABLED
    global AUTO_ENABLED, AUTO_TARGET_W, AUTO_TARGET_PCT, AUTO_MINER_OFF_DUE_TO_SOC
    # --- Tunables for loop cadence (no sleeps; these are *max* cadences) ---
    BATT_REFRESH_S = 10.0   # how often to ask EG4 client for a fresh snapshot
    CTRL_EVAL_S    = 10  # how often to re-evaluate target power

    MIN_DELTA_W = 100      # ignore tiny changes to prevent churn

    # --- Time anchors (monotonic for robustness) ---
    last_batt_poll = time.monotonic() - BATT_REFRESH_S
    last_ctrl_eval = time.monotonic() - CTRL_EVAL_S

    # --- Cached state from last successful battery read ---
    cached_snap: Optional[dict] = None

    while True:
        now = time.monotonic()

        # --- 1) Refresh battery snapshot on schedule (non-blocking) ---
        # --- 1) If it is not time to re-evaluate control, loop immediately ---
        if (now - last_ctrl_eval) < CTRL_EVAL_S:
            continue
        last_ctrl_eval = now

        snap = None

        if (now - last_batt_poll) >= BATT_REFRESH_S:
            last_batt_poll = now
            snap = _fresh_batt(max_age_s=90)
            if not(isinstance(snap, dict) and snap):
                continue  # only replace cache on good data

        soc = snap.get("soc_percent")

        if not isinstance(soc, (int, float)):
            # malformed data; keep looping, wait for next good cache
            continue

        print(f"[AUTO] loop sees AUTO_ENABLED={AUTO_ENABLED}")

        # --- 4) Compute / enforce on/off + targets (instant response to AUTO_ENABLED) ---
        if not AUTO_ENABLED:
            print(f"[AUTO] evaluating soc={soc} AUTO_ENABLED={AUTO_ENABLED}")
            continue

        target_w: Optional[int] = None  # <-- ensure defined

        # >>> FULL-CHARGE ENSURE-ON <<<
        # Always ensure the miner is ON at full charge when Auto is enabled.
        if soc >= 100.0:
            # Clear any floor and make full power the target
            LATCHED_FLOOR_W = None
            target_w = BASE_WATTS
            AUTO_TARGET_W = BASE_WATTS
            AUTO_TARGET_PCT = 100

            # If miner is not hashing, explicitly power it on
            should_power_on = False
            try:
                _sum = asyncio.run(api.summary())
                _s = (_sum.get("SUMMARY") or [{}])[0] if isinstance(_sum, dict) else {}
                is_mining = bool(_s.get("is_mining"))
                should_power_on = not is_mining
            except Exception:
                # If we can't tell, be conservative and try to power on
                should_power_on = False
                print("[AUTO] Miner not on, Power Full, Charge: unable to verify mining state")

            if should_power_on:
                try:
                    print("[AUTO] SOC 100% → ensuring miner power_on()")
                    asyncio.run(api.power_on())
                    set_miner_power_state("running")
                    AUTO_MINER_OFF_DUE_TO_SOC = False
                except Exception as e:
                    import traceback
                    print(f"[AUTO][ERROR] power_on at 100% failed: {type(e).__name__}: {e}")
                    traceback.print_exc()

            # Push 100% power percent (respecting your rate limiter)
            wall_now = time.time()
            # Directly set miner to 100% power and update globals, skipping rate limiting
            try:
                asyncio.run(api.power_on())
                asyncio.run(api.set_power_pct(100))
                set_miner_power_state("running")
                set_autocontrol_target(100)
                AUTO_LAST_SET_W = BASE_WATTS
                AUTO_LAST_SET_TS = wall_now
                print("[AUTO] Miner power_on() and set_power_pct(100) applied")
            except Exception as e:
                import traceback
                print(f"[AUTO][ERROR] power_on/set_power_pct(100) failed: {type(e).__name__}: {e}")
                traceback.print_exc()

            # Full-charge branch is complete; skip the rest of this loop iteration
            continue

        # SOC-driven hard cutoff at 30%: force miner OFF and latch this state.
        if soc <= 30.0:
            AUTO_TARGET_W = 0
            AUTO_TARGET_PCT = 0
            print("[AUTO] SOC ≤ 30% → power_off()")
            asyncio.run(api.power_off())
            set_miner_power_state("stopped")
            AUTO_MINER_OFF_DUE_TO_SOC = True
            AUTO_LAST_SET_TS = time.time()  # reset cadence timer
            continue

# --- New condition: enable autocontrol after sunset if battery > 60% and autocontrol is off --
        if is_past_sunset() and soc > 60.0 and MINER_POWER_STATE == "stopped":
            print("[AUTO] Past sunset, battery > 60%, turning miner power ON")
            asyncio.run(api.power_on())
            set_miner_power_state("running")
            continue

        # Only reduce or maintain miner power percent based on SOC decile
        current_pct = AUTO_TARGET_PCT if AUTO_TARGET_PCT is not None else 100
        decile_pct = min(100, int(math.ceil(soc / 10.0) * 10))

        if decile_pct < current_pct:
            # Only reduce power percent if decile is lower than current
            target_w = int(round(BASE_WATTS * (decile_pct / 100.0)))
            AUTO_TARGET_W = target_w
            AUTO_TARGET_PCT = decile_pct
        else:
            # Keep current target, do not increase
            target_w = int(round(BASE_WATTS * (current_pct / 100.0)))
            AUTO_TARGET_W = target_w
            AUTO_TARGET_PCT = current_pct

        # --- DEBUG: show decision inputs and candidate target ---
        print(f"[AUTO] enabled={AUTO_ENABLED} soc={soc:.1f} "
              f"last_set={AUTO_LAST_SET_W} floor={LATCHED_FLOOR_W} "
              f"candidate={target_w}")

        # --- 5) Rate-limit outbound commands and avoid tiny nudges ---
        if target_w is None:
            # Defensive: shouldn't happen due to continues above, but keep guard.
            continue
        wall_now = time.time()
        pct = 0 if BASE_WATTS <= 0 else int(round((target_w / float(BASE_WATTS)) * 100))
        pct = max(0, min(100, pct))

        try:
            resp = asyncio.run(api.set_power_pct(pct))
            set_autocontrol_target(pct)  # persist target percent
            print(f"[AUTO] set_power_pct({pct}%) -> {resp}")
            AUTO_LAST_SET_W = int(target_w)
            AUTO_LAST_SET_TS = wall_now
            if target_w < BASE_WATTS:
                if LATCHED_FLOOR_W is None or target_w < LATCHED_FLOOR_W:
                    LATCHED_FLOOR_W = target_w
        except Exception as e:
            import traceback
            print(f"[AUTO][ERROR] set_power_pct({pct}%) failed: {type(e).__name__}: {e}")
            traceback.print_exc()
        else:
            print(
                f"[AUTO] SKIP send: "
                f"delta={None if AUTO_LAST_SET_W is None else abs(target_w - AUTO_LAST_SET_W)} "
                f"age={wall_now - AUTO_LAST_SET_TS:.1f}s "
                f"min_delta={MIN_DELTA_W} "
                f"min_interval={AUTO_MIN_INTERVAL_SEC}s"
            )

# ---------- Routes ----------
@app.get("/")
def index():
    return send_from_directory(".", DASHBOARD_FILE)

@app.get("/version")
def version():
    mtime, sha = _html_sig()
    return jsonify({"mtime": mtime, "sha": sha})


@app.post("/set_limit")
def set_limit():
    data = request.get_json(silent=True) or {}
    try:
        watts = int(data.get("watts"))
        if watts < 0 or watts > 99999:
            return jsonify({"ok": False, "error": "watts_out_of_range"}), 400
    except Exception:
        return jsonify({"ok": False, "error": "bad_watts"}), 400

    global standby_flag, last_nonzero_limit
    if watts > 0:
        last_nonzero_limit = watts
        standby_flag = False
    else:
        standby_flag = True

    asyncio.run(api.send_privileged_command("set_power_limit", power_limit=str(watts)))
    return jsonify({"ok": True, "watts": watts})

@app.post("/standby")
def to_standby():
    """Stop hashing via ciphertext (hashboards off)."""
    miner_ctrl.enqueue_stop()
    return jsonify({"ok": True, "queued": True, "op": "stop"})

@app.post("/resume")
def resume():
    """Resume hashing via ciphertext (hashboards on)."""
    miner_ctrl.enqueue_resume()
    return jsonify({"ok": True, "queued": True, "op": "resume"})


@app.get("/status")
def status():
    # Make a shallow copy of latest and force string keys (defensive for jsonify)
    base_latest = latest if isinstance(latest, dict) else {}
    safe_latest = {str(k): v for k, v in base_latest.items()}

    # Ensure timestamp
    safe_latest.setdefault("ts", datetime.now(timezone.utc).isoformat())

    # --- Hashrate/Efficiency (no change to your logic) ---
    hr_ths = None
    h = safe_latest.get("hashrate")
    if isinstance(h, dict) and h.get("rate") is not None:
        try:
            hr_ths = float(h["rate"])
        except Exception:
            hr_ths = None
    if hr_ths is None and safe_latest.get("Hashrate") is not None:
        try:
            hr_ths = float(safe_latest["Hashrate"])
        except Exception:
            hr_ths = None
    safe_latest["Hashrate"] = round(hr_ths, 1) if hr_ths is not None else None

    p = safe_latest.get("Power")
    if p is not None and hr_ths and hr_ths > 0:
        try:
            safe_latest["Efficiency"] = round(float(p) / float(hr_ths), 1)
        except Exception:
            safe_latest["Efficiency"] = None
    else:
        safe_latest["Efficiency"] = None

    # --- Power Percent: actual Power / Power Limit * 100 ---
    pct = None
    try:
        pwr = safe_latest.get("Power")
        pl = safe_latest.get("Power Limit")
        if pwr is not None and pl not in (None, 0):
            pct = max(0, min(100, int(round(float(pwr) / float(pl) * 100))))
    except Exception:
        pct = None
    safe_latest["Power Percent"] = pct

    # recent: also coerce keys to strings to avoid any None/Non-str keys from older rows
    recent = [{str(k): v for k, v in row.items()} for row in list(HISTORY)[-50:]]

    # Current % of BASE_WATTS (for display like “now 79%”)
    curr_pct_base = None
    try:
        pwr = safe_latest.get("Power")
        if pwr is not None and BASE_WATTS > 0:
            curr_pct_base = max(0, min(100, int(round(float(pwr) / float(BASE_WATTS) * 100))))
    except Exception:
        curr_pct_base = None

    return jsonify({
        "latest": safe_latest,
        "recent": recent,
        "auto": {
            "enabled": AUTO_ENABLED,
            "last_set_w": AUTO_LAST_SET_W,
            "min_interval_s": AUTO_MIN_INTERVAL_SEC,
            "target_w": AUTO_TARGET_W,
            "target_pct": AUTO_TARGET_PCT,
            "current_pct_base": curr_pct_base
        }
    })

@app.get("/history")
def api_history():
    rows = list(HISTORY)
    # Coerce all dict keys to strings so jsonify never compares None vs str
    safe_rows = []
    for r in rows:
        if isinstance(r, dict):
            safe_rows.append({str(k): v for k, v in r.items()})
        else:
            safe_rows.append(r)
    return jsonify(safe_rows)

@app.get("/battery_page")
def battery_page():
    return send_from_directory(".", "EG4_Battery.html")

@app.get("/battery")
def battery_api():
    # Serve from preloaded battery_history so charts have data at startup,
    # and coerce CSV strings to numbers for charting.
    latest_snap = eg4.get_latest()
    all_rows = list(battery_history)  # CSV-preloaded + live

    NUM_KEYS = {
        "pv_power_w",
        "load_power_w",
        "battery_net_w",
        "soc_percent",
        "pack_voltage_v",
        "pack_current_a",
    }

    def _coerce_num(v):
        try:
            if v is None or v == "":
                return None
            # allow ints to stay ints if integral
            f = float(v)
            return int(f) if f.is_integer() else f
        except Exception:
            return v  # leave as-is if not numeric

    def _normalize(row: dict) -> dict:
        out = dict(row)
        for k in NUM_KEYS:
            if k in out:
                out[k] = _coerce_num(out[k])
        # ensure ts is a plain string
        if "ts" in out and out["ts"] is not None:
            out["ts"] = str(out["ts"])
        return out

    # Normalize lists
    all_rows_norm = [_normalize(r) for r in all_rows]
    recent_rows_norm = all_rows_norm[-360:] if len(all_rows_norm) > 360 else all_rows_norm
    print(f"[BATTERY API] latest_ok={isinstance(latest_snap, dict)} recent_len={len(recent_rows_norm)} all_len={len(all_rows_norm)}")
    return jsonify({
        "latest": latest_snap,
        "recent": recent_rows_norm,
        "all": all_rows_norm,
    })

@app.get("/health")
def health():
    recent = list(HISTORY)[-50:]
    return jsonify({
        "latest": latest,
        "recent": recent,
        "auto": {
            "enabled": AUTO_ENABLED,
            "last_set_w": AUTO_LAST_SET_W,
            "min_interval_s": AUTO_MIN_INTERVAL_SEC
        }
    })

@app.get("/autocontrol")
def get_autocontrol():
    print(f"[AUTOCONTROL][GET] enabled={AUTO_ENABLED}")
    return jsonify({"enabled": AUTO_ENABLED})

@app.post("/autocontrol")
def set_autocontrol():
    global AUTO_ENABLED, AUTO_LAST_SET_TS
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled", False))

    # persist to wm_state.json
    set_autocontrol_enabled(enabled)

    # keep the control loop’s live flag in sync
    AUTO_ENABLED = enabled
    if enabled:
        AUTO_LAST_SET_TS = 0.0  # allow immediate first action

    print(f"[AUTOCONTROL][POST] enabled={AUTO_ENABLED}")
    return jsonify({"ok": True, "enabled": AUTO_ENABLED})

@app.get("/auto_state")
def auto_state():
    # Expose minimal auto-control state for debugging
    snap = eg4.get_latest() if ("eg4" in globals() and eg4 is not None) else None
    soc = None
    if isinstance(snap, dict):
        soc = snap.get("soc_percent")
    return jsonify({
        "auto_enabled": AUTO_ENABLED,
        "last_set_w": AUTO_LAST_SET_W,
        "latched_floor_w": LATCHED_FLOOR_W,
        "soc_percent": soc,
        "max_watt": BASE_WATTS,
        "min_interval_s": AUTO_MIN_INTERVAL_SEC,
    })

@app.get("/logs/miner.csv")
def download_miner_csv():
    # Serve the miner CSV file as a download
    return send_from_directory(LOG_DIR, os.path.basename(LOG_FILE), as_attachment=True)

@app.get("/logs/battery.csv")
def download_battery_csv():
    # Serve the EG4 battery CSV file as a download
    return send_from_directory(os.path.dirname(BATT_LOG_FILE), os.path.basename(BATT_LOG_FILE), as_attachment=True)

@app.get("/debug/miner_logs")
def debug_miner_logs():
    import glob
    out = []
    try:
        for path in sorted(glob.glob(os.path.join(LOG_DIR, "*.csv"))):
            try:
                sz = os.path.getsize(path)
                # count rows (cheap scan)
                n = 0
                with open(path, "r", newline="") as f:
                    rdr = csv.reader(f)
                    for _ in rdr:
                        n += 1
                # subtract header if present
                if n > 0:
                    n_data = n - 1
                else:
                    n_data = 0
                out.append({"file": os.path.basename(path), "size_bytes": sz, "rows_data": n_data})
            except Exception as e:
                out.append({"file": os.path.basename(path), "error": str(e)})
    except Exception as e:
        return jsonify({"error": str(e), "LOG_DIR": LOG_DIR})
    return jsonify({"LOG_DIR": LOG_DIR, "files": out, "LOG_FILE": LOG_FILE})

@app.get("/debug/history_counts")
def debug_history_counts():
    # Numbers the server currently holds in memory
    miner_n = len(HISTORY)
    batt_n = len(battery_history)

    # Peek first/last timestamps if present
    def peek_ts(seq):
        try:
            first = seq[0].get("ts") if seq else None
            last = seq[-1].get("ts") if seq else None
            return first, last
        except Exception:
            return None, None

    m_first, m_last = peek_ts(list(HISTORY))
    b_first, b_last = peek_ts(list(battery_history))

    # What /battery would return right now
    all_rows = list(battery_history)
    recent_rows = all_rows[-360:] if len(all_rows) > 360 else all_rows

    return jsonify({
        "miner_history_count": miner_n,
        "miner_first_ts": m_first,
        "miner_last_ts": m_last,
        "battery_history_count": batt_n,
        "battery_first_ts": b_first,
        "battery_last_ts": b_last,
        "battery_recent_count": len(recent_rows),
        "battery_all_count": len(all_rows)
    })

@app.get("/debug/miner_sample")
def debug_miner_sample():
    import itertools, datetime as _dt

    rows = list(HISTORY)
    n = len(rows)

    def parse_ok(ts):
        try:
            # JS Date accepts ISO 8601; this mirrors that
            _ = _dt.datetime.fromisoformat(str(ts))
            return True
        except Exception:
            return False

    with_ts = sum(1 for r in rows if r.get("ts"))
    parseable = sum(1 for r in rows if r.get("ts") and parse_ok(r.get("ts")))
    head = list(itertools.islice(rows, 0, 3))
    tail = list(itertools.islice(rows, max(0, n-3), n))

    return jsonify({
        "total_rows": n,
        "with_ts": with_ts,
        "parseable_ts": parseable,
        "head": head,
        "tail": tail,
    })

# ---------- Main ----------
if __name__ == "__main__":

    # Preload historical data so charts have range immediately
    print(f"[LOAD] paths: LOG_FILE={LOG_FILE}  BATT_LOG_FILE={BATT_LOG_FILE}")
    _load_miner_history_from_csv()
    _load_battery_history_from_csv()

    # --- DEBUG: verify CSV preload happens on startup ---
    print(f"[LOAD] paths: LOG_FILE={LOG_FILE}  BATT_LOG_FILE={BATT_LOG_FILE}")
    try:
        print(f"[LOAD] before: HISTORY={len(HISTORY)} battery_history={len(battery_history)}")
    except Exception:
        pass

    # If you have _load_existing_csv, use it; otherwise use your per-file loaders
    try:
        _load_existing_csv(LOG_FILE, HISTORY)
        _load_existing_csv(BATT_LOG_FILE, battery_history)
    except NameError:
        _load_miner_history_from_csv()
        _load_battery_history_from_csv()

    print(f"[LOAD] after:  HISTORY={len(HISTORY)} battery_history={len(battery_history)}")

    # >>> Restore desired runtime BEFORE background threads start <<<
    restore_runtime()

    # Start background workers
    t = threading.Thread(target=poller, daemon=True)
    t.start()

    tb = threading.Thread(target=battery_poller, daemon=True)
    tb.start()

    ta = threading.Thread(target=auto_controller, daemon=True)
    ta.start()

    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    # Listen on all interfaces so Tailscale clients can reach it
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))