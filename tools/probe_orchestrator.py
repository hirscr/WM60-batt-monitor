#!/usr/bin/env python3
"""Pi-resident power-tuning probe orchestrator.

Runs as a `nohup`-detached background process on the Pi, independent of
`whatsminer.service`. Empirically finds (if it exists) a WhatsMiner
privileged-API command that moves operating Power between 50% (1800W)
and 60% (2160W) on an M60S without triggering chip recalibration
(actual Power dropping to 0 or Upfreq Complete dropping to 0).

Cross-process communication with the main Flask app:
  - Reads:  GET http://127.0.0.1:8080/api/battery/status (SOC + freshness)
  - Writes: tools/probe_state.json                       (Flask /api/probe reads)
  - Reads:  tools/probe_stop.flag                        (Flask /api/probe/stop writes)
  - Calls:  POST /api/autocontrol/{disable,enable}       (release/restore control)

See `old prompts/pi_resident_probe_orchestrator.md` for the full spec.
"""
import argparse
import contextlib
import datetime as dt
import fcntl
import json
import os
import socket
import subprocess
import sys
import time
import traceback
from typing import Any, Dict, Optional, Tuple

import yaml
from passlib.hash import md5_crypt

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import tools.probe_candidates as probe_candidates

# ---------------------------------------------------------------------------
# Module constants — tunable knobs in one place
# ---------------------------------------------------------------------------

# Tolerance bands (fraction of base_watts).
SUCCESS_BAND_FRAC = 0.05          # ±5% of base_watts around target Power
NOOP_MIN_MOVEMENT_FRAC = 0.02     # ±2% of base_watts max swing => No-op

# Timing.
OBSERVATION_SECONDS = 600         # 10 min per candidate
POLL_INTERVAL_SECONDS = 10        # SUMMARY poll cadence
SUCCESS_STREAK_SECONDS = 90       # must hold target band this long
RESET_RECOVERY_MAX_SECONDS = 35 * 60  # wait up to 35 min for Power > 0 after Reset
UPFREQ_WAIT_MAX_SECONDS = 90 * 60     # wait up to 90 min for Upfreq Complete == 1
STALE_BATTERY_THRESHOLD_SECONDS = 600  # 10 min — match autocontrol's freshness gate
HEARTBEAT_INTERVAL_SECONDS = 5    # how often state file's heartbeat is refreshed
SOC_RESUME_DELTA = 5              # resume only when SOC >= floor + this
GET_TOKEN_RATE_LIMIT_SECONDS = 185
TOKEN_KEEPALIVE_SECONDS = 150     # refresh token cache after this age

# Power levels (only these two are allowed anywhere in the probe).
LEVEL_LOW_PCT = 50
LEVEL_HIGH_PCT = 60

# Networking.
MINER_PORT = 4028
MAIN_API_BASE = "http://127.0.0.1:8080"

# File paths.
STATE_FILE = os.path.join(PROJECT_ROOT, "tools", "probe_state.json")
STOP_FLAG_FILE = os.path.join(PROJECT_ROOT, "tools", "probe_stop.flag")
PID_FILE = os.path.join(PROJECT_ROOT, "tools", "probe.pid")
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")


# ---------------------------------------------------------------------------
# Config / credential loading (mirrors tools/probe_power_tuning_sweep.py)
# ---------------------------------------------------------------------------

ENV_KEYS_FOR_PWD = ("MINER_PWD", "MINER_PASSWORD", "WM_PASSWORD", "WHATSMINER_PASSWORD", "WM_PASS")


def load_config() -> Dict[str, Any]:
    """Load miner host, password, base_watts, and emergency_soc floor."""
    cfg = {}
    for p in (
        os.path.join(PROJECT_ROOT, "config.local.yaml"),
        os.path.join(PROJECT_ROOT, "config.yaml"),
    ):
        if os.path.exists(p):
            with open(p) as f:
                data = yaml.safe_load(f) or {}
            for k, v in data.items():
                cfg.setdefault(k, v)

    miner = cfg.get("miner") or {}
    host = miner.get("host") or miner.get("ip")
    base_watts = int(miner.get("base_watts") or 3600)
    pwd = miner.get("password")

    env_path = os.path.join(PROJECT_ROOT, ".wm_env")
    env_host = None
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                # Tolerate leading "export " prefix.
                if k.startswith("export "):
                    k = k[7:].strip()
                v = v.strip().strip('"').strip("'")
                if k == "WM_HOST" and v:
                    env_host = v
                if k in ENV_KEYS_FOR_PWD and not pwd:
                    pwd = v
    if host in (None, "", "192.168.1.100") and env_host:
        host = env_host

    autocontrol = (cfg.get("autocontrol") or {})
    away = autocontrol.get("away_mode") or {}
    emergency_soc_floor = int(away.get("emergency_soc", 30))

    if not host:
        raise RuntimeError("Miner host not configured")
    if not pwd:
        raise RuntimeError("Miner password not configured (set WM_PASS in .wm_env)")

    return {
        "host": host,
        "password": pwd,
        "base_watts": base_watts,
        "emergency_soc_floor": emergency_soc_floor,
    }


# ---------------------------------------------------------------------------
# Miner I/O — raw socket nc-equivalent.
# Same code path as tools/probe_power_tuning_sweep.py, lifted intact.
# ---------------------------------------------------------------------------


def nc_send_raw(host: str, port: int, payload: bytes, timeout: int = 6) -> bytes:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    chunks = []
    try:
        s.connect((host, port))
        s.sendall(payload)
        with contextlib.suppress(OSError):
            s.shutdown(socket.SHUT_WR)
        while True:
            try:
                buf = s.recv(4096)
            except socket.timeout:
                break
            if not buf:
                break
            chunks.append(buf)
    finally:
        with contextlib.suppress(Exception):
            s.close()
    return b"".join(chunks)


def nc_send_json(host: str, port: int, obj: dict, timeout: int = 6) -> Tuple[Optional[dict], str]:
    payload = json.dumps(obj).encode("utf-8")
    raw = nc_send_raw(host, port, payload, timeout=timeout)
    text = raw.decode("utf-8", errors="replace").strip()
    try:
        return json.loads(text), text
    except json.JSONDecodeError:
        return None, text


def summary(host: str) -> dict:
    obj, _ = nc_send_json(host, MINER_PORT, {"command": "summary"})
    if not obj:
        return {}
    items = obj.get("SUMMARY") or [{}]
    return items[0] if items else {}


def snapshot_fields(s: dict) -> dict:
    """Extract the fields we care about, coerced to plain Python types.

    Returns: power_w, power_limit_w, mhs_5s, mhs_av, upfreq_complete, power_mode.
    """
    def _num(v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    return {
        "power_w": _num(s.get("Power")),
        "power_5s_w": _num(s.get("Power 5s")),
        "power_limit_w": _num(s.get("Power Limit")),
        "mhs_5s": _num(s.get("MHS 5s")),
        "mhs_av": _num(s.get("MHS av")),
        "upfreq_complete": s.get("Upfreq Complete"),
        "power_mode": s.get("Power Mode"),
    }


def snapshot_str(snap: dict) -> str:
    return (
        f"PL={snap.get('power_limit_w')} P={snap.get('power_w')}W "
        f"P5s={snap.get('power_5s_w')}W MHS5s={snap.get('mhs_5s')} "
        f"Upfreq={snap.get('upfreq_complete')} Mode={snap.get('power_mode')}"
    )


# ---------------------------------------------------------------------------
# Token / AES helpers — same as tools/probe_power_tuning_sweep.py.
# ---------------------------------------------------------------------------


def fetch_token_raw(host: str) -> dict:
    obj, raw = nc_send_json(host, MINER_PORT, {"command": "get_token"})
    if not obj:
        raise RuntimeError(f"get_token returned no JSON: {raw!r}")
    msg = obj.get("Msg")
    if isinstance(msg, str):
        raise RuntimeError(f"get_token error: {msg}")
    if not isinstance(msg, dict):
        raise RuntimeError(f"get_token unexpected response: {obj}")
    salt, time_str, newsalt = msg.get("salt"), msg.get("time"), msg.get("newsalt")
    if not (salt and time_str and newsalt):
        raise RuntimeError(f"get_token missing fields: {obj}")
    return {"salt": salt, "time": time_str, "newsalt": newsalt}


def build_token_data(pwd: str, token: dict) -> dict:
    """Derive host_passwd_md5 and host_sign per pyasic's algorithm."""
    from pyasic.rpc.btminer import _crypt

    salt, time_str, newsalt = token["salt"], token["time"], token["newsalt"]
    pwd_crypt = _crypt(pwd, "$1$" + salt + "$")
    host_passwd_md5 = pwd_crypt.split("$")[3]
    tmp_crypt = _crypt(host_passwd_md5 + time_str, "$1$" + newsalt + "$")
    host_sign = tmp_crypt.split("$")[3]
    return {"host_sign": host_sign, "host_passwd_md5": host_passwd_md5}


def send_aes(host: str, token_data: dict, cmd_dict: dict, timeout: int = 6) -> Tuple[Optional[dict], str]:
    """Send AES-enveloped privileged command. Returns (decrypted_dict_or_None, display_text)."""
    from pyasic.rpc.btminer import create_privileged_cmd, parse_btminer_priviledge_data

    envelope = create_privileged_cmd(dict(token_data), dict(cmd_dict))
    raw = nc_send_raw(host, MINER_PORT, envelope, timeout=timeout)
    text = raw.decode("utf-8", errors="replace").strip()
    try:
        resp = json.loads(text)
    except json.JSONDecodeError:
        return None, text

    if isinstance(resp, dict) and "enc" in resp:
        try:
            decrypted = parse_btminer_priviledge_data(dict(token_data), resp)
            return decrypted, json.dumps(decrypted)
        except Exception as e:
            return resp, f"(decrypt failed: {e}) {text}"

    return resp, text


def send_md5crypt_adjust_power_limit(host: str, pwd: str, watts: int, timeout: int = 6) -> Tuple[Optional[dict], str]:
    """Wind-down path: matches production's MD5-crypt inline auth."""
    tok = fetch_token_raw(host)
    salt, time_str = tok["salt"], tok["time"]
    enc_pwd = md5_crypt.using(salt=salt).hash(pwd)
    cmd = {
        "command": "adjust_power_limit",
        "enc": "1",
        "time": time_str,
        "power_limit": str(watts),
        "enc_pwd": enc_pwd,
    }
    raw = nc_send_raw(host, MINER_PORT, json.dumps(cmd).encode(), timeout=timeout)
    text = raw.decode("utf-8", errors="replace").strip()
    try:
        return json.loads(text), text
    except json.JSONDecodeError:
        return None, text


class TokenCache:
    """Refreshes a token at most once per ~150s to amortize the 185s rate limit."""

    def __init__(self, host: str, pwd: str):
        self.host = host
        self.pwd = pwd
        self._data: Optional[dict] = None
        self._acquired_at: float = 0.0

    def get(self, force_refresh: bool = False) -> dict:
        age = time.time() - self._acquired_at
        if not force_refresh and self._data and age < TOKEN_KEEPALIVE_SECONDS:
            return self._data

        # Respect the 185s rate limit.
        if self._acquired_at:
            elapsed = time.time() - self._acquired_at
            if elapsed < GET_TOKEN_RATE_LIMIT_SECONDS:
                wait = int(GET_TOKEN_RATE_LIMIT_SECONDS - elapsed) + 2
                _log(f"Sleeping {wait}s to respect 185s get_token rate limit")
                time.sleep(wait)

        tok = fetch_token_raw(self.host)
        self._data = build_token_data(self.pwd, tok)
        self._acquired_at = time.time()
        return self._data

    def invalidate(self):
        self._acquired_at = 0.0


# ---------------------------------------------------------------------------
# Main-service HTTP helpers.
# ---------------------------------------------------------------------------


def _http_request(method: str, path: str, timeout: int = 5) -> Optional[dict]:
    """Tiny urllib-only HTTP client. No external deps."""
    import urllib.request

    url = f"{MAIN_API_BASE}{path}"
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if not body:
                return {}
            try:
                return json.loads(body)
            except json.JSONDecodeError:
                return {"_raw": body}
    except Exception as e:  # noqa: BLE001 — caller logs
        _log(f"HTTP {method} {path} failed: {e}")
        return None


def get_battery_status() -> Optional[dict]:
    """Read live battery snapshot via the main Flask app.

    Returns the parsed JSON dict or None on transport failure.
    The orchestrator never talks to EG4 directly — too risky to fight
    the main service for the session.
    """
    return _http_request("GET", "/api/battery/status", timeout=5)


def disable_autocontrol() -> bool:
    resp = _http_request("POST", "/api/autocontrol/disable", timeout=5)
    return bool(resp and resp.get("ok"))


def enable_autocontrol() -> bool:
    resp = _http_request("POST", "/api/autocontrol/enable", timeout=5)
    return bool(resp and resp.get("ok"))


# ---------------------------------------------------------------------------
# SOC safety extraction.
# ---------------------------------------------------------------------------


def evaluate_soc_safety(emergency_floor: int) -> dict:
    """Read battery_status and decide the safety state.

    Returns: dict {safety_state, soc, age_seconds, detail}
      safety_state in {"running", "paused_low_soc", "paused_stale_battery",
                       "paused_no_data"}.
    """
    resp = get_battery_status()
    if not resp:
        return {"safety_state": "paused_no_data", "soc": None,
                "age_seconds": None,
                "detail": "main service did not respond to /api/battery/status"}

    status = (resp.get("status") or {}) if isinstance(resp, dict) else {}
    connection = (resp.get("connection") or {}) if isinstance(resp, dict) else {}

    soc = status.get("soc_percent")
    if soc is None:
        # Some payloads put soc directly at top level — try that as a fallback.
        soc = resp.get("soc_percent")

    # Freshness — try several fields. The current main service exposes:
    #   connection.connected   bool
    #   connection.last_seen   ISO timestamp string of latest successful poll
    # Older variants also expose battery_fresh / battery_age_seconds.
    is_fresh = status.get("battery_fresh")
    if is_fresh is None:
        is_fresh = status.get("is_fresh")

    age_seconds = (
        status.get("battery_age_seconds")
        or status.get("age_seconds")
        or connection.get("age_seconds")
    )

    # Derive age from last_seen if we don't have an explicit age field.
    if age_seconds is None:
        last_seen = connection.get("last_seen") or status.get("last_seen")
        if isinstance(last_seen, str):
            try:
                ts = dt.datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=dt.timezone.utc)
                age_seconds = (dt.datetime.now(dt.timezone.utc) - ts).total_seconds()
            except ValueError:
                age_seconds = None

    # Defensive: if we have an age and it's beyond the threshold, treat as stale.
    if isinstance(age_seconds, (int, float)) and age_seconds > STALE_BATTERY_THRESHOLD_SECONDS:
        is_fresh = False
    elif is_fresh is None:
        # No explicit fresh flag — infer from connection.connected and age.
        connected = connection.get("connected")
        if connected is False:
            is_fresh = False
        elif connected is True and (age_seconds is None or age_seconds <= STALE_BATTERY_THRESHOLD_SECONDS):
            is_fresh = True

    if is_fresh is False:
        return {"safety_state": "paused_stale_battery", "soc": soc,
                "age_seconds": age_seconds,
                "detail": f"battery telemetry stale (age={age_seconds}s)"}

    if soc is None:
        return {"safety_state": "paused_no_data", "soc": None,
                "age_seconds": age_seconds,
                "detail": "battery status returned but soc_percent missing"}

    if soc < emergency_floor:
        return {"safety_state": "paused_low_soc", "soc": soc,
                "age_seconds": age_seconds,
                "detail": f"SOC {soc}% below emergency floor {emergency_floor}%"}

    return {"safety_state": "running", "soc": soc,
            "age_seconds": age_seconds, "detail": ""}


def is_safe_to_resume(safety: dict, emergency_floor: int) -> bool:
    """When recovering from paused_low_soc, require SOC >= floor + delta."""
    if safety["safety_state"] != "running":
        return False
    soc = safety.get("soc")
    if soc is None:
        return False
    return soc >= (emergency_floor + SOC_RESUME_DELTA)


# ---------------------------------------------------------------------------
# State file IO — fcntl-locked atomic writes.
# ---------------------------------------------------------------------------


def write_state_file(state: dict) -> None:
    """Atomic, fcntl-locked write of probe_state.json."""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            state = dict(state)
            state["last_write_at"] = time.time()
            json.dump(state, f, separators=(",", ":"), sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    os.replace(tmp, STATE_FILE)


def read_state_file() -> Optional[dict]:
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE) as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                return json.load(f)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except Exception:  # noqa: BLE001
        return None


def stop_requested() -> bool:
    return os.path.exists(STOP_FLAG_FILE)


def clear_stop_flag() -> None:
    with contextlib.suppress(FileNotFoundError):
        os.remove(STOP_FLAG_FILE)


# ---------------------------------------------------------------------------
# Logging — both to stdout (captured by nohup -> probe.out) and to the
# Markdown experiment log.
# ---------------------------------------------------------------------------


def _log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%SZ")
    print(f"[orch {ts}] {msg}", flush=True)


class ExperimentLog:
    """Append-only Markdown log."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a") as f:
            f.write(f"# Power-Tuning Probe Log\n\nStarted: {dt.datetime.now(dt.timezone.utc).isoformat()}\n\n")

    def write(self, text: str) -> None:
        with open(self.path, "a") as f:
            f.write(text)
            if not text.endswith("\n"):
                f.write("\n")

    def header(self, text: str) -> None:
        self.write(f"\n## {text}\n")

    def subheader(self, text: str) -> None:
        self.write(f"\n### {text}\n")

    def kv(self, key: str, value: Any) -> None:
        self.write(f"- **{key}**: {value}")


# ---------------------------------------------------------------------------
# Orchestrator core.
# ---------------------------------------------------------------------------


class ProbeOrchestrator:
    """Sole owner of probe_state.json. Single instance per Pi."""

    def __init__(self, config: dict, log: ExperimentLog, run_id: str):
        self.config = config
        self.host = config["host"]
        self.pwd = config["password"]
        self.base_watts = config["base_watts"]
        self.emergency_floor = config["emergency_soc_floor"]
        self.log = log
        self.run_id = run_id

        self.token_cache = TokenCache(self.host, self.pwd)

        # Persistent state mirror — written on every transition.
        self.state: Dict[str, Any] = {
            "run_id": run_id,
            "pid": os.getpid(),
            "started_at": time.time(),
            "phase": "phase_0_setup",
            "safety_state": "running",
            "stop_requested": False,
            "current_candidate": None,
            "current_candidate_index": None,
            "current_target_pct": None,
            "current_level_pct": LEVEL_LOW_PCT,
            "baseline_pl": None,
            "candidates": [
                {"label": c["label"], "value_type": c["value_type"],
                 "phase_a_outcome": None, "phase_a_confirm_outcome": None,
                 "phase_b_cycle_count": 0, "phase_b_last_outcome": None}
                for c in probe_candidates.CANDIDATES
            ],
            "verdict": None,
            "heartbeat_at": time.time(),
            "last_summary_snapshot": None,
            "last_safety_detail": "",
            "wind_down_failed": False,
            "wind_down_held_low_soc": False,
        }

    # ---- state mirror ----

    def _save_state(self, **updates) -> None:
        if updates:
            self.state.update(updates)
        self.state["heartbeat_at"] = time.time()
        try:
            write_state_file(self.state)
        except Exception as e:  # noqa: BLE001
            _log(f"WARN: state file write failed: {e}")

    def _heartbeat(self, last_snapshot: Optional[dict] = None) -> None:
        if last_snapshot is not None:
            self.state["last_summary_snapshot"] = last_snapshot
        self._save_state()

    # ---- safety ----

    def safety_check(self) -> dict:
        """Evaluate safety and update state. Returns safety dict."""
        safety = evaluate_soc_safety(self.emergency_floor)
        self.state["safety_state"] = safety["safety_state"]
        self.state["last_safety_detail"] = safety.get("detail", "")
        self._save_state()
        return safety

    def hold_for_safety(self) -> None:
        """If safety is paused, AES power_off the miner once and loop until clear."""
        powered_off_for_safety = False
        while True:
            if stop_requested():
                _log("Stop requested while held for safety — exiting safety loop")
                self.state["stop_requested"] = True
                self._save_state()
                return
            safety = self.safety_check()
            if safety["safety_state"] == "running":
                if is_safe_to_resume(safety, self.emergency_floor) or not powered_off_for_safety:
                    # Either we never powered off (transient stale window) OR
                    # SOC has now climbed past floor+delta.
                    return
                # SOC just barely cleared the floor — keep waiting for floor+delta.
            if not powered_off_for_safety:
                _log(f"Safety: {safety['safety_state']} — {safety['detail']}; sending AES power_off")
                self.log.write(f"- SAFETY: {safety['safety_state']} — {safety['detail']} — powering off")
                try:
                    token_data = self.token_cache.get()
                    send_aes(self.host, token_data, {"cmd": "power_off"})
                except Exception as e:  # noqa: BLE001
                    _log(f"WARN: power_off during safety hold failed: {e}")
                powered_off_for_safety = True
            time.sleep(30)

    # ---- baseline & upfreq ----

    def wait_for_upfreq_one(self, ctx_label: str) -> bool:
        """Wait up to UPFREQ_WAIT_MAX_SECONDS for Upfreq Complete == 1 and MHS 5s > 0.

        Returns True on success, False on timeout or stop.
        """
        self.log.write(f"- Waiting for Upfreq Complete = 1 ({ctx_label})")
        deadline = time.time() + UPFREQ_WAIT_MAX_SECONDS
        last_safety_check = 0.0
        while time.time() < deadline:
            if stop_requested():
                return False
            now = time.time()
            if now - last_safety_check >= 30:
                safety = self.safety_check()
                if safety["safety_state"] != "running":
                    self.hold_for_safety()
                last_safety_check = now
            s = summary(self.host)
            snap = snapshot_fields(s)
            self._heartbeat(last_snapshot=snap)
            upfreq = snap.get("upfreq_complete")
            mhs5 = snap.get("mhs_5s") or 0
            if upfreq == 1 and mhs5 > 0:
                self.log.write(f"- Upfreq Complete = 1 (PL={snap.get('power_limit_w')}W, P={snap.get('power_w')}W)")
                return True
            time.sleep(POLL_INTERVAL_SECONDS)
        self.log.write(f"- TIMEOUT waiting for Upfreq Complete = 1 ({ctx_label})")
        return False

    def phase_0_setup(self) -> Optional[dict]:
        """Disable autocontrol, set PL=1800W if needed, wait for Upfreq=1.

        Returns the steady-state snapshot dict or None on abort.
        """
        self.log.header("Phase 0 — Setup")
        self._save_state(phase="phase_0_setup")

        # Check main service is reachable.
        health = _http_request("GET", "/api/system/health", timeout=5) or {}
        if not health.get("ok"):
            self.log.write("- Main service /api/system/health did not respond OK — aborting")
            return None
        self.log.write("- Main service reachable")

        # Battery safety at startup — must be above floor+delta.
        safety = self.safety_check()
        self.log.write(f"- Initial safety: {safety['safety_state']} SOC={safety.get('soc')}% age={safety.get('age_seconds')}s")
        if safety["safety_state"] != "running":
            self.log.write(f"- Refusing to start: {safety['detail']}")
            return None
        if not is_safe_to_resume(safety, self.emergency_floor):
            self.log.write(
                f"- Refusing to start: SOC {safety['soc']}% within {SOC_RESUME_DELTA}% of floor "
                f"({self.emergency_floor}%). Wait for SOC to climb before launching probe."
            )
            return None

        # Disable autocontrol via API.
        if not disable_autocontrol():
            self.log.write("- FATAL: could not disable autocontrol via API — aborting")
            return None
        self.log.write("- Autocontrol disabled")

        # Current PL.
        s = summary(self.host)
        snap = snapshot_fields(s)
        self.log.write(f"- Initial miner state: {snapshot_str(snap)}")
        current_pl = snap.get("power_limit_w")
        target_low_watts = int(round(self.base_watts * LEVEL_LOW_PCT / 100.0))

        if current_pl is None or abs(current_pl - target_low_watts) > 50:
            self.log.write(f"- Current PL {current_pl}W != target {target_low_watts}W; sending adjust_power_limit")
            try:
                _, raw = send_md5crypt_adjust_power_limit(self.host, self.pwd, target_low_watts)
                self.log.write(f"- adjust_power_limit response: `{raw[:200]}`")
            except Exception as e:  # noqa: BLE001
                self.log.write(f"- ERROR adjust_power_limit: {e}")
                return None
        else:
            self.log.write(f"- PL already at target {target_low_watts}W")

        # Wait for Upfreq Complete = 1.
        if not self.wait_for_upfreq_one("Phase 0 baseline"):
            return None

        s = summary(self.host)
        baseline = snapshot_fields(s)
        self.log.kv("baseline", snapshot_str(baseline))
        self._save_state(baseline_pl=baseline.get("power_limit_w"),
                         current_level_pct=LEVEL_LOW_PCT)
        return baseline

    # ---- classifier ----

    def classify_attempt(self, attempt_label: str, target_pct: int,
                         start_snap: dict) -> Tuple[str, list]:
        """Poll for OBSERVATION_SECONDS and classify the outcome.

        Returns (outcome, polls) where outcome in {Success, Reset, No-op,
        Error} and polls is the list of recorded snapshots.
        """
        target_w = int(round(self.base_watts * target_pct / 100.0))
        band_w = max(50, int(round(self.base_watts * SUCCESS_BAND_FRAC)))
        min_movement_w = max(20, int(round(self.base_watts * NOOP_MIN_MOVEMENT_FRAC)))
        start_power = start_snap.get("power_w") or 0.0

        self.log.write(
            f"  - Target: {target_w}W ±{band_w}W. No-op threshold: |ΔP|<{min_movement_w}W for full window"
        )

        polls: list = []
        deadline = time.time() + OBSERVATION_SECONDS
        success_streak_start: Optional[float] = None
        zero_power_polls = 0
        zero_upfreq_polls = 0
        max_movement_w = 0.0
        observed_meaningful_movement = False
        last_safety_check = 0.0

        while time.time() < deadline:
            if stop_requested():
                return "Stopped", polls
            now = time.time()
            if now - last_safety_check >= 30:
                safety = self.safety_check()
                if safety["safety_state"] != "running":
                    self.hold_for_safety()
                last_safety_check = now

            time.sleep(POLL_INTERVAL_SECONDS)
            s = summary(self.host)
            snap = snapshot_fields(s)
            elapsed = int(time.time() - (deadline - OBSERVATION_SECONDS))
            polls.append({"t": elapsed, **snap})
            self._heartbeat(last_snapshot=snap)
            self.log.write(f"    - t+{elapsed}s: {snapshot_str(snap)}")

            p = snap.get("power_w")
            upfreq = snap.get("upfreq_complete")

            # Reset detection: Power==0 OR Upfreq dropped to 0 (chip recalibration
            # keeps drawing idle power so Power rarely hits 0, but Upfreq==0 is unambiguous).
            if p is not None and p <= 1.0:
                zero_power_polls += 1
                if zero_power_polls >= 2:
                    self.log.write(f"  - RESET: Power=0 for 2 consecutive polls")
                    return "Reset", polls
            else:
                zero_power_polls = 0

            if upfreq == 0:
                zero_upfreq_polls += 1
                if zero_upfreq_polls >= 2:
                    self.log.write(f"  - RESET: Upfreq Complete=0 for 2 consecutive polls (chip recalibration)")
                    return "Reset", polls
            else:
                zero_upfreq_polls = 0

            # Movement tracking.
            if p is not None:
                movement = abs(p - start_power)
                if movement > max_movement_w:
                    max_movement_w = movement
                if movement >= min_movement_w:
                    observed_meaningful_movement = True

            # Success: P within ±band of target AND streak >= 90s AND we did
            # see some movement (so a coincidental match from a stationary
            # baseline doesn't trigger Success).
            if p is not None and abs(p - target_w) <= band_w:
                if success_streak_start is None:
                    success_streak_start = time.time()
                elif time.time() - success_streak_start >= SUCCESS_STREAK_SECONDS:
                    if observed_meaningful_movement:
                        self.log.write(
                            f"  - SUCCESS: Power held in band {target_w}±{band_w}W for "
                            f"{SUCCESS_STREAK_SECONDS}s, max movement {max_movement_w:.0f}W"
                        )
                        return "Success", polls
                    # In band but no movement — keep watching.
            else:
                success_streak_start = None

        # Window elapsed.
        if max_movement_w < min_movement_w:
            self.log.write(
                f"  - NO-OP: max |ΔPower| = {max_movement_w:.0f}W < {min_movement_w}W threshold "
                f"over full {OBSERVATION_SECONDS}s window"
            )
            return "No-op", polls
        self.log.write(
            f"  - RESET: window elapsed, Power moved (max ΔP={max_movement_w:.0f}W) but never reached target band"
        )
        return "Reset", polls

    def wait_for_recovery(self) -> bool:
        """After a Reset, wait for power to stabilise (same ±50W for 2 consecutive polls and > 0).

        This firmware keeps drawing idle power through recalibration so Power never
        hits 0 — but the miner will return 'API command ERROR' if a privileged command
        is sent while it is still in the acute phase of a reset. Waiting for stable
        power (not full Upfreq=1) is enough to let the firmware accept the next candidate.
        """
        self.log.write(f"  - Waiting for power to stabilise after reset (cap {RESET_RECOVERY_MAX_SECONDS//60} min)")
        deadline = time.time() + RESET_RECOVERY_MAX_SECONDS
        last_safety_check = 0.0
        prev_power: Optional[float] = None
        while time.time() < deadline:
            if stop_requested():
                return False
            now = time.time()
            if now - last_safety_check >= 30:
                safety = self.safety_check()
                if safety["safety_state"] != "running":
                    self.hold_for_safety()
                last_safety_check = now
            time.sleep(POLL_INTERVAL_SECONDS)
            s = summary(self.host)
            snap = snapshot_fields(s)
            self._heartbeat(last_snapshot=snap)
            p = snap.get("power_w") or 0.0
            if p > 1.0 and prev_power is not None and abs(p - prev_power) <= 50:
                self.log.write(f"  - Power stable at {p}W — proceeding to next candidate")
                return True
            prev_power = p if p > 1.0 else None
        self.log.write(f"  - Recovery cap reached after {RESET_RECOVERY_MAX_SECONDS//60} min")
        return False

    # ---- sending candidates ----

    def issue_candidate(self, candidate_idx: int, target_pct: int) -> Tuple[Optional[dict], str, bool]:
        """Send one candidate command.

        Returns (parsed_resp, raw_text, hard_error).
        hard_error=True means we should mark Outcome=Error and continue.
        """
        cand = probe_candidates.CANDIDATES[candidate_idx]
        cmd_dict = probe_candidates.render(cand["cmd"], target_pct, self.base_watts)
        self.log.write(f"- Request cmd_dict (token redacted): `{json.dumps(cmd_dict)}`")
        try:
            token_data = self.token_cache.get()
        except RuntimeError as e:
            self.log.write(f"- ERROR token: {e}")
            return None, str(e), True

        time.sleep(0.5)
        resp_obj, resp_text = send_aes(self.host, token_data, cmd_dict)
        self.log.write(f"- Response (decrypted): `{resp_text[:300]}`")

        if isinstance(resp_obj, dict):
            status_top = resp_obj.get("STATUS")
            err_msg = ""
            if status_top == "E":
                err_msg = resp_obj.get("Msg", "")
            elif isinstance(status_top, list) and status_top:
                first = status_top[0]
                if isinstance(first, dict) and first.get("STATUS") == "E":
                    err_msg = first.get("Msg", "")
            if err_msg:
                self.log.write(f"- Miner returned error: {err_msg}")
                low = err_msg.lower()
                if "token" in low or "auth" in low or "time" in low or "enc" in low:
                    self.token_cache.invalidate()
                    self.log.write("- Token invalidated, will refresh next attempt")
                return resp_obj, resp_text, True

        return resp_obj, resp_text, False

    # ---- Phase A ----

    def phase_a_sweep(self) -> Optional[int]:
        """Sweep candidates from current baseline (LOW=50%). Returns winner index or None."""
        self.log.header("Phase A — Candidate Sweep")
        self._save_state(phase="phase_a")
        winner_idx: Optional[int] = None
        current_level_pct = self.state["current_level_pct"]

        for idx, cand in enumerate(probe_candidates.CANDIDATES):
            if stop_requested():
                self.log.write("- Stop flag set; aborting sweep")
                return None
            self.hold_for_safety()
            target_pct = LEVEL_HIGH_PCT if current_level_pct == LEVEL_LOW_PCT else LEVEL_LOW_PCT
            self.log.subheader(f"Attempt {idx+1}/{len(probe_candidates.CANDIDATES)}: {cand['label']} ({current_level_pct}% -> {target_pct}%)")
            self.log.kv("timestamp_utc", dt.datetime.now(dt.timezone.utc).isoformat())

            self._save_state(
                phase="phase_a",
                current_candidate=cand["label"],
                current_candidate_index=idx,
                current_target_pct=target_pct,
            )

            start_snap = snapshot_fields(summary(self.host))
            self.log.write(f"- Start snapshot: {snapshot_str(start_snap)}")

            _, _, hard_error = self.issue_candidate(idx, target_pct)
            if hard_error:
                self.state["candidates"][idx]["phase_a_outcome"] = "Error"
                self._save_state()
                continue

            outcome, _ = self.classify_attempt(cand["label"], target_pct, start_snap)
            self.state["candidates"][idx]["phase_a_outcome"] = outcome
            self.log.kv("Outcome", f"**{outcome}**")
            self._save_state()

            if outcome == "Stopped":
                return None
            if outcome == "Success":
                winner_idx = idx
                # Update current_level — the candidate took us to target_pct.
                self._save_state(current_level_pct=target_pct)
                break
            if outcome == "Reset":
                recovered = self.wait_for_recovery()
                if not recovered:
                    self.log.write("- Did not recover in time — aborting sweep")
                    return None
                # After recovery the miner re-tunes to whatever PL it lands at.
                # Refresh current_level from PL/base_watts.
                snap = snapshot_fields(summary(self.host))
                pl = snap.get("power_limit_w") or self.base_watts * (LEVEL_LOW_PCT / 100.0)
                inferred_pct = int(round(100.0 * pl / self.base_watts))
                # Snap to nearest allowed level.
                current_level_pct = LEVEL_HIGH_PCT if abs(inferred_pct - LEVEL_HIGH_PCT) < abs(inferred_pct - LEVEL_LOW_PCT) else LEVEL_LOW_PCT
                self._save_state(current_level_pct=current_level_pct)
                continue
            # No-op / Error — leave current_level unchanged.
        return winner_idx

    def phase_a_confirm(self, winner_idx: int) -> bool:
        """After a Phase A win, issue the same command in the opposite direction.

        Returns True if confirmed bidirectional.
        """
        cand = probe_candidates.CANDIDATES[winner_idx]
        self.log.header(f"Phase A confirmation: {cand['label']}")
        self._save_state(phase="phase_a_confirm",
                         current_candidate=cand["label"],
                         current_candidate_index=winner_idx)

        # Wait for Upfreq Complete = 1 first.
        if not self.wait_for_upfreq_one("Phase A confirmation pre-wait"):
            self.log.write("- Confirmation aborted: did not reach Upfreq=1")
            return False

        current_level_pct = self.state["current_level_pct"]
        target_pct = LEVEL_LOW_PCT if current_level_pct == LEVEL_HIGH_PCT else LEVEL_HIGH_PCT
        self.log.subheader(f"Confirm: {cand['label']} ({current_level_pct}% -> {target_pct}%)")
        self._save_state(current_target_pct=target_pct)

        start_snap = snapshot_fields(summary(self.host))
        self.log.write(f"- Start snapshot: {snapshot_str(start_snap)}")

        _, _, hard_error = self.issue_candidate(winner_idx, target_pct)
        if hard_error:
            self.state["candidates"][winner_idx]["phase_a_confirm_outcome"] = "Error"
            self._save_state()
            return False

        outcome, _ = self.classify_attempt(cand["label"], target_pct, start_snap)
        self.state["candidates"][winner_idx]["phase_a_confirm_outcome"] = outcome
        self.log.kv("Outcome", f"**{outcome}**")
        if outcome == "Success":
            self._save_state(current_level_pct=target_pct)
            return True

        if outcome == "Reset":
            self.wait_for_recovery()
        return False

    # ---- Phase B ----

    def phase_b_alternate(self, winner_idx: int) -> None:
        """Cycle the confirmed command indefinitely between 50% and 60%."""
        cand = probe_candidates.CANDIDATES[winner_idx]
        self.log.header(f"Phase B — Sustained alternation: {cand['label']}")
        self._save_state(phase="phase_b")

        while True:
            if stop_requested():
                self.log.write("- Stop requested; exiting Phase B")
                return
            self.hold_for_safety()
            if not self.wait_for_upfreq_one("Phase B cycle pre-wait"):
                self.log.write("- Phase B aborted: did not reach Upfreq=1")
                return
            current_level_pct = self.state["current_level_pct"]
            target_pct = LEVEL_LOW_PCT if current_level_pct == LEVEL_HIGH_PCT else LEVEL_HIGH_PCT
            self.state["candidates"][winner_idx]["phase_b_cycle_count"] += 1
            cycle_num = self.state["candidates"][winner_idx]["phase_b_cycle_count"]
            self.log.subheader(f"Cycle {cycle_num}: {current_level_pct}% -> {target_pct}%")
            self._save_state(current_target_pct=target_pct)

            start_snap = snapshot_fields(summary(self.host))
            self.log.write(f"- Start snapshot: {snapshot_str(start_snap)}")

            _, _, hard_error = self.issue_candidate(winner_idx, target_pct)
            if hard_error:
                self.state["candidates"][winner_idx]["phase_b_last_outcome"] = "Error"
                self._save_state()
                self.log.write("- Phase B aborted on Error")
                return

            outcome, _ = self.classify_attempt(cand["label"], target_pct, start_snap)
            self.state["candidates"][winner_idx]["phase_b_last_outcome"] = outcome
            self.log.kv(f"Cycle {cycle_num} Outcome", f"**{outcome}**")
            if outcome == "Success":
                self._save_state(current_level_pct=target_pct)
                continue
            self.log.write(f"- Phase B stopped on outcome={outcome}")
            if outcome == "Reset":
                self.wait_for_recovery()
            return

    # ---- Phase Z ----

    def phase_z_winddown(self) -> None:
        """Always-run wind-down: PL=1800W, autocontrol enabled, verdict written."""
        self.log.header("Phase Z — Wind-down")
        self._save_state(phase="wind_down")
        target_watts = int(round(self.base_watts * LEVEL_LOW_PCT / 100.0))

        # Wait briefly if miner is mid-reset (Power=0). Bail after 30 min.
        deadline = time.time() + 30 * 60
        while time.time() < deadline:
            snap = snapshot_fields(summary(self.host))
            self._heartbeat(last_snapshot=snap)
            p = snap.get("power_w") or 0.0
            if p > 1.0:
                break
            self.log.write(f"  - Wind-down: Power={p}W; waiting up to 30 min for it to recover")
            time.sleep(20)

        # Send adjust_power_limit 1800W.
        try:
            _, raw = send_md5crypt_adjust_power_limit(self.host, self.pwd, target_watts)
            self.log.write(f"- adjust_power_limit({target_watts}) response: `{raw[:200]}`")
        except Exception as e:  # noqa: BLE001
            self.log.write(f"- ERROR adjust_power_limit during wind-down: {e}")

        # Check SOC — if low, leave autocontrol disabled.
        safety = self.safety_check()
        if safety["safety_state"] != "running":
            self.log.write(f"- SOC unsafe at wind-down ({safety['detail']}); leaving autocontrol DISABLED")
            self._save_state(wind_down_held_low_soc=True)
        else:
            ok = enable_autocontrol()
            if ok:
                self.log.write("- Autocontrol re-enabled")
            else:
                self.log.write("- WARN: failed to re-enable autocontrol via API — manual intervention required")
                self._save_state(wind_down_failed=True)

        clear_stop_flag()

    # ---- verdict ----

    def write_verdict(self, winner_idx: Optional[int], confirmed: bool) -> None:
        self.log.header("Verdict")
        if winner_idx is not None and confirmed:
            cand = probe_candidates.CANDIDATES[winner_idx]
            cycles = self.state["candidates"][winner_idx]["phase_b_cycle_count"]
            verdict = f"WINNER (bidirectional): `{cand['label']}` — {cycles} successful Phase B cycle(s)"
            self.log.write(f"- {verdict}")
            self.log.write(
                "- Recommended operational rule: use this command for in-flight power changes "
                f"between {LEVEL_LOW_PCT}% and {LEVEL_HIGH_PCT}% on this miner. Continue using "
                "adjust_power_limit for any other transitions until further testing."
            )
        elif winner_idx is not None and not confirmed:
            cand = probe_candidates.CANDIDATES[winner_idx]
            verdict = f"PARTIAL: `{cand['label']}` succeeded one-way but failed bidirectional confirmation"
            self.log.write(f"- {verdict}")
        else:
            verdict = "NO CANDIDATE QUALIFIED — every shape was Reset, No-op, or Error"
            self.log.write(f"- {verdict}")
            self.log.write("- Per-candidate outcomes:")
            for c in self.state["candidates"]:
                self.log.write(
                    f"  - {c['label']}: phase_a={c['phase_a_outcome']} "
                    f"confirm={c['phase_a_confirm_outcome']}"
                )
        self._save_state(phase="complete", verdict=verdict)

    # ---- top-level run ----

    def run(self) -> None:
        try:
            baseline = self.phase_0_setup()
            if baseline is None:
                self.log.write("\nPhase 0 aborted — running wind-down only.")
                self.phase_z_winddown()
                self.write_verdict(None, False)
                return

            if stop_requested():
                self.log.write("\nStop requested before Phase A — winding down.")
                self.phase_z_winddown()
                self.write_verdict(None, False)
                return

            winner_idx = self.phase_a_sweep()
            confirmed = False
            if winner_idx is not None and not stop_requested():
                confirmed = self.phase_a_confirm(winner_idx)
                if confirmed and not stop_requested():
                    self.phase_b_alternate(winner_idx)

            self.phase_z_winddown()
            self.write_verdict(winner_idx, confirmed)
        except Exception as e:  # noqa: BLE001
            self.log.header("FATAL ERROR")
            self.log.write(f"```\n{traceback.format_exc()}\n```")
            self._save_state(phase="error", verdict=f"fatal: {e}")
            with contextlib.suppress(Exception):
                self.phase_z_winddown()


# ---------------------------------------------------------------------------
# Single-instance lock.
# ---------------------------------------------------------------------------


def check_existing_instance() -> Optional[int]:
    """Return a live PID if another orchestrator is already running, else None."""
    existing = read_state_file()
    if not existing:
        return None
    pid = existing.get("pid")
    heartbeat_at = existing.get("heartbeat_at") or 0
    phase = existing.get("phase")
    if phase == "complete":
        return None
    if not pid:
        return None
    # PID alive check.
    try:
        os.kill(int(pid), 0)
    except (ProcessLookupError, ValueError):
        return None
    except PermissionError:
        # Process exists but owned by another user — still counts as live.
        pass
    # Heartbeat freshness — anything older than 5 min is considered dead.
    if time.time() - heartbeat_at > 300:
        return None
    return int(pid)


# ---------------------------------------------------------------------------
# Main entry point.
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="Override single-instance lock (use with care)")
    args = parser.parse_args()

    if not args.force:
        existing_pid = check_existing_instance()
        if existing_pid:
            print(f"[orch] Another orchestrator is already running (PID {existing_pid}). Exiting.",
                  file=sys.stderr)
            sys.exit(3)

    clear_stop_flag()

    try:
        config = load_config()
    except RuntimeError as e:
        print(f"[orch] FATAL: {e}", file=sys.stderr)
        sys.exit(2)

    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = os.path.join(LOG_DIR, f"probe_{run_id}.md")
    log = ExperimentLog(log_path)
    log.kv("Miner host", config["host"])
    log.kv("base_watts", config["base_watts"])
    log.kv("emergency_soc_floor", config["emergency_soc_floor"])
    log.kv("Candidates", ", ".join(c["label"] for c in probe_candidates.CANDIDATES))
    log.kv("Levels", f"{LEVEL_LOW_PCT}% / {LEVEL_HIGH_PCT}%")
    log.kv("PID", os.getpid())

    # Write PID file for the launch script.
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    _log(f"Probe starting. PID={os.getpid()} log={log_path}")
    orch = ProbeOrchestrator(config, log, run_id)
    orch.run()
    _log("Probe finished.")


if __name__ == "__main__":
    main()
