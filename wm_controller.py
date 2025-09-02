# wm_controller.py (plaintext client)
import json, socket
from typing import Any, Dict

class WhatsMinerClientPlain:
    def __init__(self, ip: str, timeout: float = 3.0):
        self.ip = ip
        self.timeout = timeout

    def _send_one(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Send one JSON command and read a single JSON line reply."""
        line = json.dumps(obj, separators=(",", ":")).encode() + b"\n"
        with socket.create_connection((self.ip, 4028), timeout=self.timeout) as s:
            s.settimeout(self.timeout)
            f = s.makefile("rwb", buffering=0)
            f.write(line); f.flush()
            # Read exactly one line; some writes may not reply before restart
            try:
                raw = f.readline().decode("utf-8", errors="ignore").strip()
            except TimeoutError:
                raw = ""
        if not raw:
            # Return a benign status so callers can poll summary
            return {"STATUS": "S", "Code": 131, "Msg": "No immediate reply (miner may be restarting)", "Description": ""}
        return json.loads(raw)

    def get_version(self) -> Dict[str, Any]:
        return self._send_one({"cmd": "get_version"})

    def get_summary(self) -> Dict[str, Any]:
        return self._send_one({"cmd": "summary"})

    def set_power_limit_w(self, watts: int) -> Dict[str, Any]:
        if not (0 <= watts <= 99999):
            raise ValueError("watts out of range")
        # Many firmwares restart btminer after applying this; reply may be empty
        return self._send_one({"cmd": "adjust_power_limit", "power_limit": str(watts)})

    # Optional presets; some builds return Code:14
    def set_low_power(self) -> Dict[str, Any]:
        return self._send_one({"cmd": "set_low_power"})
    def set_normal_power(self) -> Dict[str, Any]:
        return self._send_one({"cmd": "set_normal_power"})
    def set_high_power(self) -> Dict[str, Any]:
        return self._send_one({"cmd": "set_high_power"})