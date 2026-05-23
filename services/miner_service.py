"""Miner service for polling and control."""
import asyncio
import threading
import queue
import time
import platform
import csv
import os
from collections import deque
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from pyasic.rpc.btminer import BTMinerRPCAPI
from models.device import ConnectionStatus
from utils.nc_miner_api import NCMinerAPI
from utils.log_config import log as debug_log


class MinerController:
    """Async command queue for miner operations."""

    def __init__(self, api, priv_api=None):
        """
        Initialize controller.

        Args:
            api: API for polling (NCMinerAPI or BTMinerRPCAPI).
            priv_api: API for privileged commands (always NCMinerAPI). If None,
                      falls back to api (for backward compatibility when api is NCMinerAPI).
        """
        self.api = api
        # Use dedicated privileged API if provided; otherwise fall back to polling API.
        # NCMinerAPI is subprocess-based and stateless, so it is always safe to reuse.
        self._priv_api = priv_api if priv_api is not None else api
        self.q = queue.Queue()
        self.state: dict[str, object] = {
            "op_state": "idle",
            "op_kind": None,
            "last_sent_command": None,
            "error": "",
            "started_at": None,
            "request": None,
        }
        t = threading.Thread(target=self._worker, daemon=True)
        t.start()

    def status_snapshot(self) -> Dict[str, Any]:
        return dict(self.state)

    def enqueue_stop(self, on_verified=None):
        self.q.put(("stop", {}, on_verified))

    def enqueue_resume(self, on_verified=None):
        self.q.put(("resume", {}, on_verified))

    def enqueue_set_power_limit(self, watts: int, on_verified=None):
        self.q.put(("power_limit", {"watts": int(watts)}, on_verified))

    def enqueue_set_power_pct(self, percent: int, on_verified=None):
        self.q.put(("power_pct", {"percent": int(percent)}, on_verified))

    def drain_queue(self):
        """Remove all pending ops from the queue without executing them.
        LIMITATION: cannot cancel an op already executing on the worker thread.
        If a power_on was in flight when drain is called, it may still complete.
        The Layer 3 verify-and-retry loop in AutoControlService handles this case
        by detecting the miner is still on and issuing another emergency stop.
        """
        drained = 0
        while True:
            try:
                self.q.get_nowait()
                drained += 1
            except queue.Empty:
                break
        if drained:
            print(f"[MinerController] Drained {drained} pending op(s) from queue")

    def _verify(self, kind: str, req: Dict[str, Any]) -> bool:
        """Verify that a command succeeded by polling the miner.

        For stop: checks SUMMARY Power Limit == 0 AND MHS 5s == 0 (MD5-crypt shutdown path).
        For resume: uses the 'status' command and inspects MINING[0].mineroff.
        For power_limit: checks SUMMARY Power Limit field.
        For power_pct: checks SUMMARY Power Limit matches the target watts.
        A fresh API instance is used to avoid shared-state issues.
        """
        try:
            if kind == "stop":
                # Verify MHS 5s == 0 via summary (non-privileged).
                # NOTE: Power Limit is always 0 on this firmware regardless of state;
                # do not use it as a stop indicator. MHS 5s is the reliable metric.
                for _ in range(10):
                    try:
                        fresh_api = self._make_fresh_api()
                        if isinstance(fresh_api, NCMinerAPI):
                            summary = fresh_api.summary()
                        else:
                            summary = asyncio.run(fresh_api.summary())
                        lst = (summary or {}).get("SUMMARY") or []
                        s = lst[0] if lst and isinstance(lst[0], dict) else {}
                        mhs_5s = float(s.get("MHS 5s") or 0)
                        if mhs_5s == 0.0:
                            return True
                    except Exception as ve:
                        print(f"[MinerController] _verify stop poll error: {ve}")
                    time.sleep(1)
                return False

            if kind == "resume":
                # Use status command to check mineroff field — retry up to 10 seconds.
                for _ in range(10):
                    try:
                        fresh_api = self._make_fresh_api()
                        if isinstance(fresh_api, NCMinerAPI):
                            status_reply = fresh_api.miner_status_cmd()
                        else:
                            status_reply = asyncio.run(fresh_api.send_command("status"))
                        if status_reply:
                            mining_list = status_reply.get("MINING") or []
                            mining_info = mining_list[0] if mining_list and isinstance(mining_list[0], dict) else {}
                            mineroff_val = str(mining_info.get("mineroff", "")).lower()
                            if mineroff_val == "false":
                                return True
                    except Exception as ve:
                        print(f"[MinerController] _verify resume poll error: {ve}")
                    time.sleep(1)
                return False

            if kind == "power_limit":
                # Check SUMMARY Power Limit equals requested watts — retry up to 5 seconds.
                for _ in range(5):
                    try:
                        fresh_api = self._make_fresh_api()
                        if isinstance(fresh_api, NCMinerAPI):
                            summary = fresh_api.summary()
                        else:
                            summary = asyncio.run(fresh_api.summary())
                        lst = (summary or {}).get("SUMMARY") or []
                        s = lst[0] if lst and isinstance(lst[0], dict) else {}
                        if str(s.get("Power Limit")) == str(req["watts"]):
                            return True
                    except Exception as ve:
                        print(f"[MinerController] _verify power_limit poll error: {ve}")
                    time.sleep(1)
                return False

            if kind == "power_pct":
                # Verify Power Limit matches the target watts calculated from percent.
                target_watts = int(3600 * (req["percent"] / 100))
                for _ in range(5):
                    try:
                        fresh_api = self._make_fresh_api()
                        if isinstance(fresh_api, NCMinerAPI):
                            summary = fresh_api.summary()
                        else:
                            summary = asyncio.run(fresh_api.summary())
                        lst = (summary or {}).get("SUMMARY") or []
                        s = lst[0] if lst and isinstance(lst[0], dict) else {}
                        if str(s.get("Power Limit")) == str(target_watts):
                            return True
                    except Exception as ve:
                        print(f"[MinerController] _verify power_pct poll error: {ve}")
                    time.sleep(1)
                return False

        except Exception:
            return False
        return True

    def _make_fresh_api(self):
        """Return the privileged command API instance.

        NCMinerAPI is subprocess-based and stateless — it is safe to reuse across
        calls. It uses MD5-crypt authentication (not pyasic's AES handshake), which
        works with firmware 20240605.01.REL on both macOS and Linux.
        """
        return self._priv_api

    def _run_op(self, kind: str, req: Dict[str, Any], on_verified=None):
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
                # Use AES power_off (no respbefore — that causes enc json load err on this
                # firmware). adjust_power_limit(0) does NOT stop the miner; the firmware
                # treats 0 as unconstrained and keeps hashing.
                priv_api = self._make_fresh_api()
                if isinstance(priv_api, NCMinerAPI):
                    priv_api.power_off()
                else:
                    asyncio.run(priv_api.power_off())
            elif kind == "resume":
                priv_api = self._make_fresh_api()
                if isinstance(priv_api, NCMinerAPI):
                    priv_api.power_on()
                else:
                    asyncio.run(priv_api.power_on())
            elif kind == "power_limit":
                priv_api = self._make_fresh_api()
                if isinstance(priv_api, NCMinerAPI):
                    priv_api.send_privileged_command(
                        "adjust_power_limit", power_limit=str(req["watts"])
                    )
                else:
                    asyncio.run(
                        priv_api.send_privileged_command(
                            "adjust_power_limit", power_limit=str(req["watts"])
                        )
                    )
            elif kind == "power_pct":
                # Convert percentage to watts (max 3600W)
                percent = req['percent']
                watts = int(3600 * (percent / 100))
                print(f"[MinerController] Setting power to {percent}% ({watts}W)...")
                priv_api = self._make_fresh_api()
                print(f"[MinerController] Using API: {type(priv_api).__name__}")
                print(f"[MinerController] Using password: {bool(priv_api.pwd)}")

                # Use adjust_power_limit (permanent) instead of set_power_pct (temporary)
                if isinstance(priv_api, NCMinerAPI):
                    result = priv_api.send_privileged_command("adjust_power_limit", power_limit=str(watts))
                else:
                    result = asyncio.run(
                        priv_api.send_privileged_command("adjust_power_limit", power_limit=str(watts))
                    )
                print(f"[MinerController] Response: {result}")
                status_list = result.get("STATUS") if isinstance(result, dict) else None
                if isinstance(status_list, list) and status_list:
                    status_code = status_list[0].get("STATUS", "")
                elif isinstance(status_list, str):
                    status_code = status_list
                else:
                    status_code = ""
                if status_code == "S":
                    print(f"[MinerController] ✓ Power limit set to {watts}W ({percent}%)")
                else:
                    print(f"[MinerController] ✗ Power limit command failed (will verify current state)")
            else:
                raise ValueError(f"Unknown op {kind}")

            self.state["op_state"] = "verifying"
            ok = self._verify(kind, req)
            if not ok:
                print(f"[MinerController] ✗ Verification failed for {kind}")
                raise RuntimeError(f"verification failed for {kind}")
            print(f"[MinerController] ✓ Verification passed for {kind}")

            if on_verified:
                try:
                    on_verified()
                except Exception as cb_err:
                    print(f"[MinerController] on_verified callback error: {cb_err}")

            self.state["op_state"] = "idle"
        except Exception as e:
            import traceback
            print(f"[MinerController] ✗ Op '{kind}' failed: {type(e).__name__}: {e}")
            traceback.print_exc()
            self.state["op_state"] = "error"
            self.state["error"] = str(e)

    def _worker(self):
        while True:
            item = self.q.get()
            kind = item[0]
            req = item[1]
            on_verified = item[2] if len(item) > 2 else None
            try:
                self._run_op(kind, req, on_verified=on_verified)
            finally:
                self.q.task_done()


class MinerService:
    """Service for miner polling and control."""

    def __init__(self, host: str, password: str, poll_seconds: int, log_interval_sec: int, log_file: str = None, state_manager=None):
        # Always use NCMinerAPI for both polling and privileged commands.
        # NCMinerAPI uses nc (subprocess) which closes the TCP connection immediately
        # after each command, avoiding the miner's "over max connect" limit.
        # BTMinerRPCAPI (pyasic asyncio) holds connections longer and exhausts
        # the miner's connection limit, preventing auth sessions from succeeding.
        print(f"[MinerService] Using NCMinerAPI for polling and privileged commands")
        self.api = NCMinerAPI(host, password=password)
        self.use_async = False

        # Polling API + privileged command API (same instance — NCMinerAPI is stateless)
        self.controller = MinerController(self.api, priv_api=self.api)

        self.poll_seconds = poll_seconds
        self.log_interval_sec = log_interval_sec
        self.log_file = log_file or os.path.join("miner_logs", "wm_status_log.csv")

        self.latest = {}
        self.history: deque = deque()
        self.last_nonzero_limit: Optional[int] = None

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_log_ts = 0.0  # Start at 0 so first data point logs immediately

        # is_off: True until confirmed hashing. Starts conservative (safe default).
        self._is_off: bool = True

        # User-commanded master switch state. Loaded from persisted state at init
        # so the dashboard toggle reflects the user's last explicit choice across
        # restarts. Only the user-facing HTTP endpoints (power_on / power_off)
        # write to this — service-side code (AutoControl, MinerController) MUST
        # NOT touch it. See docstring for set_user_power_intent.
        self._state_mgr = state_manager
        if state_manager is not None:
            try:
                persisted = state_manager.load()
                intent = persisted.get("user_power_intent")
                self._user_power_intent: bool = True if intent is None else bool(intent)
            except Exception as e:
                print(f"[MinerService] Could not load user_power_intent from state ({e}); defaulting to True")
                self._user_power_intent = True
        else:
            # No state manager (e.g. test instantiation) — default to True.
            self._user_power_intent = True

        # Connection status tracking
        self.connection_status = ConnectionStatus(connected=False)
        self._first_connect_time: Optional[float] = None

    def start(self):
        """Start polling thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        print("[MinerService] Started polling")

    def stop(self):
        """Stop polling thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        print("[MinerService] Stopped polling")

    def _poll_loop(self):
        """Background polling loop."""
        ip = getattr(self.api, 'ip', 'unknown')
        print(f"[MinerService] Starting poll loop for {ip}")

        while self._running:
            try:
                print(f"[MinerService] Polling miner at {ip}...")

                # Use sync or async based on API type
                if self.use_async:
                    reply = asyncio.run(self.api.summary())
                else:
                    reply = self.api.summary()

                print(f"[MinerService] RAW RESPONSE: {reply}")
                print(f"[MinerService] Response type: {type(reply)}")
                item = self._extract_summary_item(reply)

                if item:
                    # Update connection status
                    now = time.time()
                    if not self.connection_status.connected:
                        self._first_connect_time = now
                        print(f"[MinerService] ✓ Connected to miner at {self.api.ip}")

                    self.connection_status.connected = True
                    self.connection_status.last_seen = datetime.now(timezone.utc)
                    if self._first_connect_time:
                        self.connection_status.uptime_seconds = now - self._first_connect_time
                    self.connection_status.error = None

                    # Process data
                    pl = self._safe_int(item, "Power Limit")
                    if pl and pl > 0:
                        self.last_nonzero_limit = pl

                    row = {"ts": self._now_iso()}
                    for k, v in item.items():
                        row[str(k)] = v

                    # --- Hashrate 5s (MHS 5s / 1e6) ---
                    try:
                        mhs_5s = float(item.get("MHS 5s") or 0)
                        row["Hashrate 5s"] = round(mhs_5s / 1_000_000.0, 1) if mhs_5s > 0 else None
                    except (TypeError, ValueError):
                        row["Hashrate 5s"] = None

                    # --- Hashrate (prefer MHS 5m for chart smoothness; fallback to best available) ---
                    try:
                        mhs_5m = float(item.get("MHS 5m") or 0)
                        hashrate_5m = round(mhs_5m / 1_000_000.0, 1) if mhs_5m > 0 else None
                    except (TypeError, ValueError):
                        hashrate_5m = None
                    hr_ths = hashrate_5m if hashrate_5m is not None else self._extract_hashrate_ths(item)
                    row["Hashrate"] = round(hr_ths, 1) if hr_ths is not None else None

                    # --- Power 5s (real-time PSU pin reading) ---
                    power_5s = None
                    try:
                        if not self.use_async:
                            psu_reply = self.api.get_psu()
                        else:
                            psu_reply = asyncio.run(self.api.get_psu())
                        psu_msg = psu_reply.get("Msg", {}) if isinstance(psu_reply, dict) else {}
                        pin_val = psu_msg.get("pin") if isinstance(psu_msg, dict) else None
                        if pin_val is not None:
                            power_5s = round(float(pin_val), 0)
                    except Exception as e:
                        print(f"[MinerService] get_psu failed (non-fatal): {e}")
                    row["Power 5s"] = power_5s

                    # --- is_off: authoritative composite detection ---
                    # 1. Try 'status' command for mineroff field (most authoritative)
                    is_off_from_status = None
                    try:
                        if not self.use_async:
                            status_reply = self.api.miner_status_cmd()
                        else:
                            status_reply = asyncio.run(self.api.send_command("status"))
                        if status_reply:
                            mining_list = status_reply.get("MINING") or []
                            mining_info = mining_list[0] if mining_list and isinstance(mining_list[0], dict) else {}
                            mineroff_val = str(mining_info.get("mineroff", "")).lower()
                            if mineroff_val == "true":
                                is_off_from_status = True
                            elif mineroff_val == "false":
                                is_off_from_status = False
                    except Exception as e:
                        print(f"[MinerService] status command failed (using fallback): {e}")

                    if is_off_from_status is not None:
                        self._is_off = is_off_from_status
                    elif item.get("is_mining") is not None:
                        # pyasic-normalized field
                        self._is_off = item.get("is_mining") is False
                    elif self._safe_int(item, "Power Limit") == 0:
                        self._is_off = True
                    else:
                        try:
                            mhs_5s_check = float(item.get("MHS 5s") or 0)
                            self._is_off = (mhs_5s_check == 0.0)
                        except (TypeError, ValueError):
                            self._is_off = False

                    row["is_off"] = self._is_off

                    # --- Upfreq Complete (0 = still ramping, 1 = full speed) ---
                    try:
                        upfreq_raw = item.get("Upfreq Complete")
                        row["upfreq_complete"] = int(upfreq_raw) if upfreq_raw is not None else 0
                    except (TypeError, ValueError):
                        row["upfreq_complete"] = 0

                    # Compute Efficiency (W/TH)
                    pwr = row.get("Power")
                    if isinstance(pwr, (int, float)) or (isinstance(pwr, str) and str(pwr).isdigit()):
                        pwr_val = float(pwr)
                    else:
                        pwr_val = None

                    if pwr_val is not None and hr_ths and hr_ths > 0:
                        row["Efficiency"] = round(pwr_val / hr_ths, 1)
                    else:
                        row["Efficiency"] = None

                    self.latest = row
                    self.history.append(row)

                    # CSV logging check (log first data immediately, then every log_interval_sec)
                    now_ts = time.time()
                    if (now_ts - self._last_log_ts) >= self.log_interval_sec:
                        self._log_to_csv(row)
                        self._last_log_ts = now_ts

                    # Safe logging with None checks
                    hr_display = f"{hr_ths:.1f}" if hr_ths is not None else "—"
                    pwr_display = f"{pwr_val:.0f}" if pwr_val is not None else "—"
                    print(f"[MinerService] Poll successful: {hr_display}TH/s, {pwr_display}W")
                    print(f"[MinerService] Sending update to UI: {row.get('Hashrate')}TH/s, {row.get('Hashrate 5s')}TH/s(5s), {row.get('Power')}W, {row.get('Power 5s')}W(5s), is_off={self._is_off}")
                else:
                    print(f"[MinerService] No data in summary response")

            except Exception as e:
                # Connection failed
                was_connected = self.connection_status.connected
                self.connection_status.connected = False
                self.connection_status.error = str(e)

                if was_connected:
                    print(f"[MinerService] ✗ Lost connection to miner: {e}")
                else:
                    print(f"[MinerService] ✗ Cannot connect to miner at {self.api.ip}: {e}")

                import traceback
                traceback.print_exc()

            time.sleep(self.poll_seconds)

    def get_status(self) -> dict:
        """Get current miner status.

        Includes `user_power_intent`, the persisted user-commanded master switch
        state. The dashboard's Power toggle is driven by this field (NOT `is_off`,
        which is observed miner state and flips during transient safety stops).
        """
        snapshot = self.latest.copy() if self.latest else {}
        snapshot["user_power_intent"] = self._user_power_intent
        return snapshot

    def get_history(self) -> list:
        """Get history."""
        return list(self.history)

    def get_connection_status(self) -> dict:
        """Get connection status."""
        return {
            "connected": self.connection_status.connected,
            "last_seen": self.connection_status.last_seen.isoformat() if self.connection_status.last_seen else None,
            "uptime_seconds": self.connection_status.uptime_seconds,
            "error": self.connection_status.error,
        }

    # Control methods
    def set_power_limit(self, watts: int, on_verified=None):
        """Set power limit in watts."""
        self.controller.enqueue_set_power_limit(watts, on_verified=on_verified)

    def set_power_pct(self, percent: int, on_verified=None):
        """Set power percent (0-100)."""
        self.controller.enqueue_set_power_pct(percent, on_verified=on_verified)

    def power_on(self, on_verified=None):
        """Turn miner on."""
        self.controller.enqueue_resume(on_verified=on_verified)

    def power_off(self, on_verified=None):
        """Turn miner off via queue."""
        self.controller.enqueue_stop(on_verified=on_verified)

    def emergency_power_off(self) -> bool:
        """Emergency stop: call AES power_off directly on the calling thread.
        Bypasses the FIFO queue entirely — no queueing, no MinerController.
        Uses _token_max_attempts=1 to fail fast if the session is locked, instead
        of sleeping 185s. The caller's retry loop handles repeated attempts.
        Returns True if the AES command was accepted (encrypted response received).
        This is NOT proof the miner is off; verified-off requires the caller to
        re-poll and confirm MHS 5s==0, is_off==true.
        NOTE: adjust_power_limit(0) does NOT stop this firmware — it treats 0 as
        unconstrained and keeps hashing. AES power_off is the only reliable path.
        """
        result = self.api.send_aes_privileged_command("power_off", _token_max_attempts=1)
        return isinstance(result, dict) and "enc" in result

    def get_op_status(self) -> dict:
        """Get operation status."""
        return self.controller.status_snapshot()

    @property
    def is_off(self) -> bool:
        """True if miner is confirmed off (conservative: defaults to True until first poll)."""
        return self._is_off

    @property
    def user_power_intent(self) -> bool:
        """User-commanded master switch state.

        True means the user wants the miner available (default).
        False means the user has explicitly clicked Power-OFF.

        This is USER INTENT, not observed miner state. The two can diverge:
        if AutoControl issues a safety-driven shutdown, the miner physically
        powers down (is_off becomes True) but user_power_intent stays True
        because the user did not click anything. The dashboard's Power toggle
        is driven by this field.
        """
        return self._user_power_intent

    def set_user_power_intent(self, value: bool) -> None:
        """Set the user-commanded master switch state and persist it.

        ONLY HTTP endpoints triggered by user clicks should call this. Service-
        side code (AutoControl, MinerController, verification loops) MUST NOT
        call this — they manipulate miner power via power_on()/power_off() which
        do not touch intent.
        """
        new_value = bool(value)
        self._user_power_intent = new_value
        if __debug__:
            debug_log("POWER_INTENT", f"user_power_intent set to {new_value}")
        if self._state_mgr is not None:
            try:
                self._state_mgr.save(user_power_intent=new_value)
            except Exception as e:
                print(f"[MinerService] Failed to persist user_power_intent: {e}")

    # Helper methods
    def _now_iso(self):
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    def _log_to_csv(self, row: dict):
        """Append row to CSV log file."""
        try:
            os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
            file_exists = os.path.exists(self.log_file)

            with open(self.log_file, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=row.keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)

            print(f"[MinerService] Logged to CSV: {row.get('ts', 'no timestamp')}")
        except Exception as e:
            print(f"[MinerService] CSV logging error: {e}")

    def _extract_summary_item(self, reply: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not reply or "SUMMARY" not in reply or not reply["SUMMARY"]:
            return {}
        item = reply["SUMMARY"][0]
        return item if isinstance(item, dict) else {}

    def _safe_int(self, d: Dict[str, Any], key: str) -> Optional[int]:
        try:
            return int(d.get(key)) if key in d else None
        except Exception:
            return None

    def _extract_hashrate_ths(self, item: Dict[str, Any]) -> Optional[float]:
        """Derive hashrate in TH/s from a WhatsMiner SUMMARY item."""
        if not isinstance(item, dict):
            return None

        # Direct TH/s fields
        for k in ("THS 5s", "THS av", "TH/s", "THS"):
            if k in item:
                try:
                    v = float(item[k])
                    return v if v >= 0 else None
                except Exception:
                    pass

        # GH/s fields (convert to TH/s)
        for k in ("GHS 5s", "GHS av", "GH/s", "GHS"):
            if k in item:
                try:
                    v = float(item[k])
                    return v / 1000.0 if v >= 0 else None
                except Exception:
                    pass

        # MH/s fields (convert to TH/s)
        for k in ("MHS 5s", "MHS av", "MH/s", "MHS", "MHS 1m", "MHS 5m", "MHS 15m"):
            if k in item:
                try:
                    v = float(item[k])
                    return v / 1000000.0 if v >= 0 else None
                except Exception:
                    pass

        return None
