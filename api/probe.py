"""Probe orchestrator observability endpoints.

Two endpoints:
  GET  /api/probe/status — returns probe_state.json + pid_alive + last summary
  POST /api/probe/stop   — creates the stop flag; orchestrator picks it up

There is no /api/probe/start endpoint — the orchestrator is launched
manually via tools/_launch_probe.sh over SSH. This keeps the Flask app
out of the business of spawning long-running OS processes.
"""
from __future__ import annotations

import errno
import fcntl
import json
import os
import time
from typing import Any, Optional

from flask import Blueprint, jsonify


probe_bp = Blueprint("probe", __name__)


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE_FILE = os.path.join(PROJECT_ROOT, "tools", "probe_state.json")
STOP_FLAG_FILE = os.path.join(PROJECT_ROOT, "tools", "probe_stop.flag")

# Same heartbeat-staleness threshold the orchestrator uses internally for
# the single-instance check. Past this, we report pid_alive=false.
HEARTBEAT_STALE_THRESHOLD_SEC = 60


def _read_state() -> Optional[dict]:
    """Read probe_state.json under a shared lock. Returns None if absent."""
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE) as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            try:
                return json.load(f)
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except (OSError, json.JSONDecodeError):
        return None


def _pid_alive(pid: Optional[Any]) -> bool:
    if pid is None:
        return False
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        return False
    if pid_int <= 0:
        return False
    try:
        os.kill(pid_int, 0)
        return True
    except OSError as e:
        if e.errno == errno.EPERM:
            # Process exists but we can't signal it — still alive.
            return True
        return False


@probe_bp.get("/api/probe/status")
def probe_status():
    """Return current orchestrator state, or a sentinel if no run exists."""
    state = _read_state()
    if state is None:
        return jsonify({
            "present": False,
            "message": "No probe has been run yet. Launch via tools/_launch_probe.sh on the Pi.",
        })

    pid = state.get("pid")
    heartbeat_at = state.get("heartbeat_at") or 0
    heartbeat_age = time.time() - heartbeat_at if heartbeat_at else None
    process_alive = _pid_alive(pid)
    heartbeat_fresh = (
        heartbeat_age is not None and heartbeat_age <= HEARTBEAT_STALE_THRESHOLD_SEC
    )
    pid_alive = process_alive and heartbeat_fresh

    out = dict(state)
    out["present"] = True
    out["pid_alive"] = pid_alive
    out["process_alive"] = process_alive
    out["heartbeat_age_seconds"] = heartbeat_age
    out["stop_flag_present"] = os.path.exists(STOP_FLAG_FILE)
    return jsonify(out)


@probe_bp.post("/api/probe/stop")
def probe_stop():
    """Create the stop flag file. Orchestrator polls for it and exits cleanly."""
    os.makedirs(os.path.dirname(STOP_FLAG_FILE), exist_ok=True)
    # Touch the file (idempotent).
    with open(STOP_FLAG_FILE, "w") as f:
        f.write(str(int(time.time())))
    return jsonify({"queued": True, "stop_flag_path": STOP_FLAG_FILE})
