#!/usr/bin/env python3
# WM_server.py — Flask server that exposes WhatsMiner status & control APIs
# Requires: wm_controller.py in the same folder with class WhatsMinerClientPlain

import hashlib
from typing import Any, Dict, Optional
import os, threading, time, csv
from collections import deque
from datetime import datetime, timezone
import math
import logging

from flask import Flask, jsonify, request, send_from_directory

# --- EG4 battery client ---
from eg4_client import EG4Client
from wm_controller import WhatsMinerClientPlain  # <- your existing plaintext client

# Config from env
EG4_USER = os.environ.get("EG4_USER") or os.environ.get("USERNAME")
EG4_PASS = os.environ.get("EG4_PASS") or os.environ.get("PASSWORD")
EG4_BASE = os.environ.get("EG4_BASE_URL", "https://monitor.eg4electronics.com")

# Poll cadence for battery page
BATTERY_POLL_SECONDS = int(os.environ.get("BATTERY_POLL_SECONDS", "10"))

# --- Auto Control state ---
AUTO_ENABLED = False

# --- Auto control config (place near other constants/env reads) ---
AUTO_MIN_INTERVAL_SEC = int(os.environ.get("AUTO_MIN_INTERVAL_SEC", "120"))
MAX_WATT = int(os.environ.get("MAX_WATT", "3600"))
MIN_WATT = 0
AUTO_LOW_CAP_W = int(os.environ.get("AUTO_LOW_CAP_W", "3200"))  # cap before full recharge
# Ratchet floor: only decreases with SOC; resets to MAX_WATT once SOC == 100%
AUTO_FLOOR_W = MAX_WATT

# Latched-floor state and last set tracking
AUTO_LAST_SET_W: Optional[int] = None
AUTO_LAST_SET_TS: float = 0.0
LATCHED_FLOOR_W: Optional[int] = None

# State
battery_latest = {}   # last good snapshot (normalized)
battery_history = deque()  # keep all rows ever loaded


# ---------- Config ----------
MINER_IP = os.environ.get("MINER_IP", "").strip()
if not MINER_IP:
    raise SystemExit("Set MINER_IP (e.g., export MINER_IP=192.168.86.47)")

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "10"))
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
client = WhatsMinerClientPlain(MINER_IP, timeout=2.5)
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
      - If SOC is None, hold last_set_w if available; otherwise default to MAX_WATT.
    """
    if soc is None:
        return last_set_w if last_set_w is not None else MAX_WATT

    # Compute decile percent rounded UP, clamp 0..100
    pct = min(100, int(math.ceil(soc / 10.0) * 10))
    candidate = int(round(MAX_WATT * (pct / 100.0)))

    # Apply the latched floor while SOC is below 100%
    if latched_floor_w is not None and soc < 100.0:
        candidate = min(candidate, latched_floor_w)

    # Clamp to global bounds
    return max(MIN_WATT, min(MAX_WATT, candidate))

def poller():
    """Background: poll summary every POLL_SECONDS, keep latest/history fresh."""
    global latest, last_nonzero_limit
    while True:
        try:
            reply = client.get_summary()
            item = _extract_summary_item(reply)
            if item:
                pl = _safe_int(item, "Power Limit")
                if pl and pl > 0:
                    last_nonzero_limit = pl
                row = {"ts": _now_iso()}
                for k, v in item.items():
                    row[str(k)] = v
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

        time.sleep(BATTERY_POLL_SECONDS)

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
                if row.get("ts"):
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
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("ts"):
                    HISTORY.append(row)
                    count += 1
        print(f"[LOAD] miner history loaded: {count} rows from {LOG_FILE}")
    except Exception as e:
        print(f"[LOAD] miner history error: {e}")

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
    # --- Tunables for loop cadence (no sleeps; these are *max* cadences) ---
    BATT_REFRESH_S = 10.0   # how often to ask EG4 client for a fresh snapshot
    CTRL_EVAL_S    = 20  # how often to re-evaluate target power

    MIN_DELTA_W = 100      # ignore tiny changes to prevent churn

    # --- Time anchors (monotonic for robustness) ---
    last_batt_poll = time.monotonic() - BATT_REFRESH_S
    last_ctrl_eval = time.monotonic() - CTRL_EVAL_S

    # --- Cached state from last successful battery read ---
    cached_snap: Optional[dict] = None

    while True:
        now = time.monotonic()

        # --- 1) Refresh battery snapshot on schedule (non-blocking) ---
        if (now - last_batt_poll) >= BATT_REFRESH_S:
            last_batt_poll = now
            snap = _fresh_batt(max_age_s=90)
            if isinstance(snap, dict) and snap:
                cached_snap = snap  # only replace cache on good data

        # --- 2) If it is not time to re-evaluate control, loop immediately ---
        if (now - last_ctrl_eval) < CTRL_EVAL_S:
            continue
        last_ctrl_eval = now

        # --- 3) If we have no cached battery data, we cannot decide; loop ---
        if not cached_snap:
            continue

        soc = cached_snap.get("soc_percent")
        if not isinstance(soc, (int, float)):
            # malformed data; keep looping, wait for next good cache
            continue

        print(f"[AUTO] loop sees AUTO_ENABLED={AUTO_ENABLED}")

        # --- 4) Compute target setpoint (instant response to AUTO_ENABLED) ---
        if not AUTO_ENABLED:
            # Auto is off: do not change miner, but keep tracking state
            # (Leave AUTO_LAST_SET_W as-is. No commands issued.)
            print(f"[AUTO] evaluating soc={soc} AUTO_ENABLED={AUTO_ENABLED}")
            continue

        # Hard cutoff below 20% SOC
        if soc < 20.0:
            target_w = 0
        else:
            target_w = _watts_for_soc(
                soc=soc,
                last_set_w=AUTO_LAST_SET_W,
                latched_floor_w=LATCHED_FLOOR_W,
            )

        # On full charge, restore full power and clear the floor latch
        if soc >= 100.0:
            target_w = MAX_WATT
            LATCHED_FLOOR_W = None

        # --- DEBUG: show decision inputs and candidate target ---
        print(f"[AUTO] enabled={AUTO_ENABLED} soc={soc:.1f} "
              f"last_set={AUTO_LAST_SET_W} floor={LATCHED_FLOOR_W} "
              f"candidate={target_w}")

        # --- 5) Rate-limit outbound commands and avoid tiny nudges ---
        wall_now = time.time()
        if AUTO_LAST_SET_W is None or (
                abs(target_w - AUTO_LAST_SET_W) >= MIN_DELTA_W
                and (wall_now - AUTO_LAST_SET_TS) >= AUTO_MIN_INTERVAL_SEC
        ):
            try:
                # Send the command to the miner
                client.set_power_limit_w(int(target_w))
                AUTO_LAST_SET_W = int(target_w)
                AUTO_LAST_SET_TS = wall_now
                print(f"[AUTO] SENT target_w={target_w}")

                # Update latched floor when we set < 100%
                if target_w < MAX_WATT:
                    if LATCHED_FLOOR_W is None or target_w < LATCHED_FLOOR_W:
                        LATCHED_FLOOR_W = target_w
            except Exception:
                # Ignore transient miner errors; loop continues immediately
                pass
        else:
            # Explain why we didn't send a command this cycle
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

    reply = client.set_power_limit_w(watts)
    return jsonify({"ok": True, "reply": reply})

@app.post("/standby")
def to_standby():
    global standby_flag
    standby_flag = True
    reply = client.set_power_limit_w(0)
    return jsonify({"ok": True, "reply": reply})

@app.post("/resume")
def resume():
    global standby_flag
    target = last_nonzero_limit or DEFAULT_LIMIT
    standby_flag = False
    reply = client.set_power_limit_w(target)
    return jsonify({"ok": True, "target": target, "reply": reply})

@app.get("/status")
def status():
    recent = list(HISTORY)[-50:]  # or _history if that's your var
    return jsonify({
        "latest": latest,
        "recent": recent,
        "auto": {
            "enabled": AUTO_ENABLED,
            "last_set_w": AUTO_LAST_SET_W,
            "min_interval_s": AUTO_MIN_INTERVAL_SEC
        }
    })

@app.get("/history")
def api_history():
    return jsonify(list(HISTORY))

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
    global AUTO_ENABLED, AUTO_LAST_SET_TS  # reset timer so we can act quickly on enable
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled"))
    AUTO_ENABLED = enabled
    if enabled:
        AUTO_LAST_SET_TS = 0.0  # allow immediate first set
    print(f"[AUTOCONTROL][POST] requested_body={request.get_json(silent=True)}")
    print(f"[AUTOCONTROL][POST] new enabled={AUTO_ENABLED}")
    print(f"[AUTOCONTROL][POST] responding enabled={AUTO_ENABLED}")
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
        "max_watt": MAX_WATT,
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
    import itertools, json, datetime as _dt

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
        _load_existing_csv(LOG_FILE, HISTORY)  # <-- use this if present
        _load_existing_csv(BATT_LOG_FILE, battery_history)
    except NameError:
        # fallback to the two loaders you added earlier
        _load_miner_history_from_csv()
        _load_battery_history_from_csv()

    print(f"[LOAD] after:  HISTORY={len(HISTORY)} battery_history={len(battery_history)}")

    t = threading.Thread(target=poller, daemon=True)
    t.start()

    tb = threading.Thread(target=battery_poller, daemon=True)
    tb.start()

    ta = threading.Thread(target=auto_controller, daemon=True);
    ta.start()

    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    # Listen on all interfaces so Tailscale clients can reach it
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))