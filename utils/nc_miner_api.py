#!/usr/bin/env python3
"""
NC-based miner API - workaround for macOS socket issues.
Uses nc command via subprocess instead of Python sockets.
Supports two encrypted privileged command formats:
  - MD5-crypt inline (enc_pwd field) — works for adjust_power_limit
  - AES envelope (pyasic format) — required for power_off and power_on
"""
import subprocess
import json
from typing import Optional, Dict, Any
from passlib.hash import md5_crypt


class NCMinerAPI:
    """Miner API using nc command as workaround for macOS socket restrictions."""

    def __init__(self, ip: str, port: int = 4028, timeout: int = 5, password: str = None):
        self.ip = ip
        self.port = port
        self.timeout = timeout
        self.pwd = password  # Store password for privileged commands

    def _send_command(self, command: str) -> Optional[Dict[str, Any]]:
        """Send command to miner via nc."""
        cmd = {"command": command}
        cmd_json = json.dumps(cmd)

        try:
            result = subprocess.run(
                ['nc', '-w', str(self.timeout), self.ip, str(self.port)],
                input=cmd_json.encode('utf-8'),
                capture_output=True,
                timeout=self.timeout + 2
            )

            if result.returncode == 0:
                response = result.stdout.decode('utf-8', errors='replace').strip()
                data = json.loads(response)
                return data
            else:
                print(f"[NCMinerAPI] nc failed: {result.stderr.decode()}")
                return None

        except subprocess.TimeoutExpired:
            print(f"[NCMinerAPI] Timeout connecting to {self.ip}:{self.port}")
            return None
        except json.JSONDecodeError as e:
            print(f"[NCMinerAPI] JSON decode error: {e}")
            return None
        except Exception as e:
            print(f"[NCMinerAPI] Error: {e}")
            return None

    def summary(self) -> Dict[str, Any]:
        """Get miner summary (synchronous, no async needed)."""
        result = self._send_command("summary")
        return result if result else {}

    def version(self) -> Dict[str, Any]:
        """Get miner version."""
        result = self._send_command("version")
        return result if result else {}

    def power_off(self):
        """Power off miner using AES-encrypted privileged command."""
        return self.send_aes_privileged_command("power_off", respbefore="true")

    def power_on(self):
        """Power on miner using AES-encrypted privileged command."""
        return self.send_aes_privileged_command("power_on")

    def _get_token_with_retry(self, max_attempts: int = 3) -> Optional[Dict[str, Any]]:
        """
        Fetch authentication token, waiting if the miner reports "over max connect".

        The WhatsMiner firmware allows only one privileged session at a time with a
        ~180s timeout. CRITICAL: each get_token call (success OR failure) starts/resets
        the timer. So we MUST NOT poll get_token repeatedly while blocked — doing so
        extends the lock indefinitely and the session never expires.

        Strategy: on first "over max connect", sleep 185s (enough for the session to
        expire), then try once more. Repeat up to max_attempts total.

        Args:
            max_attempts: Maximum number of get_token attempts (default 3).

        Returns:
            Token response dict on success, None if all attempts fail.
        """
        import time as _time
        SESSION_TIMEOUT_SEC = 185  # Slightly more than observed ~180s firmware timeout

        for attempt in range(max_attempts):
            token_response = self.get_token()
            if not token_response:
                print(f"[NCMinerAPI] get_token returned None (attempt {attempt+1}/{max_attempts})")
                if attempt + 1 < max_attempts:
                    print(f"[NCMinerAPI] Sleeping {SESSION_TIMEOUT_SEC}s for session to expire...")
                    _time.sleep(SESSION_TIMEOUT_SEC)
                continue

            msg = token_response.get("Msg", {})
            if isinstance(msg, dict) and msg.get("salt"):
                # Success
                if attempt > 0:
                    print(f"[NCMinerAPI] ✓ Token acquired on attempt {attempt+1}")
                return token_response

            # Error case — "over max connect" or other
            err_msg = msg if isinstance(msg, str) else token_response.get("Msg", "unknown")
            if attempt + 1 < max_attempts:
                print(f"[NCMinerAPI] Token blocked ({err_msg}), sleeping {SESSION_TIMEOUT_SEC}s for session to expire (attempt {attempt+1}/{max_attempts})...")
                _time.sleep(SESSION_TIMEOUT_SEC)
            else:
                print(f"[NCMinerAPI] ✗ Token blocked ({err_msg}) on final attempt {attempt+1}/{max_attempts}")

        print(f"[NCMinerAPI] ✗ Failed to acquire token after {max_attempts} attempts")
        return None

    def get_token(self) -> Optional[Dict[str, Any]]:
        """Get authentication token from miner."""
        return self._send_command("get_token")

    def _encrypt_password(self, salt: str) -> str:
        """Encrypt password using MD5 crypt with salt."""
        if not self.pwd:
            raise ValueError("Password not set")
        # Use MD5 crypt (same format as pyasic: $1$salt$hash)
        return md5_crypt.using(salt=salt).hash(self.pwd)

    def send_aes_privileged_command(self, command: str, **params) -> Dict[str, Any]:
        """
        Send a privileged command using pyasic's AES envelope format.

        Required for power_off and power_on on firmware 20240605.01.REL.
        The MD5-crypt inline format (send_privileged_command) returns "invalid data"
        for those commands; the AES envelope is the only accepted format.

        The command is:
          1. Get token (get_token) for salt/time/newsalt
          2. Derive host_passwd_md5 and host_sign via MD5-crypt chains
          3. AES-encrypt the command body using sha256(host_passwd_md5) as key
          4. Send {"enc": 1, "data": "<base64>"} envelope via nc
        """
        if not self.pwd:
            return {"STATUS": [{"STATUS": "E", "Msg": "Password not configured"}]}

        print(f"[NCMinerAPI] Sending AES privileged command: {command}")

        try:
            # Import pyasic helpers (available in miner-venv)
            from pyasic.rpc.btminer import _crypt, create_privileged_cmd
        except ImportError as e:
            print(f"[NCMinerAPI] ✗ Cannot import pyasic for AES encryption: {e}")
            return {"STATUS": [{"STATUS": "E", "Msg": f"pyasic not available: {e}"}]}

        try:
            # Step 1: Get token (with retry if session is busy)
            print(f"[NCMinerAPI] Step 1: Getting authentication token...")
            token_response = self._get_token_with_retry()

            if not token_response:
                return {"STATUS": [{"STATUS": "E", "Msg": "Timed out waiting for auth session"}]}

            msg = token_response.get("Msg", {})
            if isinstance(msg, str):
                print(f"[NCMinerAPI] ✗ Token error: {msg}")
                return {"STATUS": [{"STATUS": "E", "Msg": msg}]}

            salt = msg.get("salt")
            time_str = msg.get("time")
            newsalt = msg.get("newsalt")

            if not salt or not time_str or not newsalt:
                print(f"[NCMinerAPI] ✗ Invalid token response: {token_response}")
                return {"STATUS": [{"STATUS": "E", "Msg": "Invalid token response"}]}

            print(f"[NCMinerAPI] ✓ Got token (salt: {salt}, time: {time_str})")

            # Step 2: Derive host_passwd_md5 and host_sign (pyasic algorithm)
            pwd_crypt = _crypt(self.pwd, "$1$" + salt + "$")
            host_passwd_md5 = pwd_crypt.split("$")[3]
            tmp_crypt = _crypt(host_passwd_md5 + time_str, "$1$" + newsalt + "$")
            host_sign = tmp_crypt.split("$")[3]

            token_data = {
                "host_sign": host_sign,
                "host_passwd_md5": host_passwd_md5,
            }

            # Step 3: Build command dict and AES-encrypt it
            cmd_dict = {"cmd": command}
            for key, value in params.items():
                cmd_dict[key] = str(value)

            enc_payload = create_privileged_cmd(token_data, cmd_dict)
            print(f"[NCMinerAPI] AES payload built ({len(enc_payload)} bytes)")

            # Brief pause to allow get_token connection to close before opening new one
            import time as _time
            _time.sleep(0.5)

            # Step 4: Send via nc
            result = subprocess.run(
                ['nc', '-w', str(self.timeout), self.ip, str(self.port)],
                input=enc_payload,
                capture_output=True,
                timeout=self.timeout + 2
            )

            if result.returncode == 0:
                response = result.stdout.decode('utf-8', errors='replace').strip()
                print(f"[NCMinerAPI] AES response received ({len(response)} bytes)")
                try:
                    data = json.loads(response)
                    # AES responses are encrypted — presence of "enc" key indicates success
                    if "enc" in data:
                        print(f"[NCMinerAPI] ✓ AES command accepted (encrypted response received)")
                    elif data.get("STATUS") == "E":
                        print(f"[NCMinerAPI] ✗ ERROR: {data.get('Msg')}")
                    return data
                except json.JSONDecodeError:
                    print(f"[NCMinerAPI] ✗ JSON decode error on response: {response[:100]}")
                    return {"STATUS": [{"STATUS": "E", "Msg": "JSON decode error"}]}
            else:
                error_msg = result.stderr.decode()
                print(f"[NCMinerAPI] ✗ nc failed: {error_msg}")
                return {"STATUS": [{"STATUS": "E", "Msg": f"nc failed: {error_msg}"}]}

        except subprocess.TimeoutExpired:
            print(f"[NCMinerAPI] ✗ Timeout")
            return {"STATUS": [{"STATUS": "E", "Msg": "Timeout"}]}
        except Exception as e:
            print(f"[NCMinerAPI] ✗ AES command error: {e}")
            import traceback
            traceback.print_exc()
            return {"STATUS": [{"STATUS": "E", "Msg": str(e)}]}

    def send_privileged_command(self, command: str, **params) -> Dict[str, Any]:
        """
        Send privileged command with encrypted authentication.

        Args:
            command: Command name (e.g., "set_power_limit")
            **params: Command parameters (e.g., power_limit="2700")

        Returns:
            Response dict from miner
        """
        if not self.pwd:
            return {"STATUS": [{"STATUS": "E", "Msg": "Password not configured"}]}

        print(f"[NCMinerAPI] Sending privileged command: {command}")

        try:
            # Step 1: Get token (with retry if session is busy)
            print(f"[NCMinerAPI] Step 1: Getting authentication token...")
            token_response = self._get_token_with_retry()

            if not token_response:
                return {"STATUS": [{"STATUS": "E", "Msg": "Timed out waiting for auth session"}]}

            msg = token_response.get("Msg", {})
            if isinstance(msg, str):
                print(f"[NCMinerAPI] ✗ Token error: {msg}")
                return {"STATUS": [{"STATUS": "E", "Msg": msg}]}

            salt = msg.get("salt")
            time_str = msg.get("time")

            if not salt or not time_str:
                print(f"[NCMinerAPI] ✗ Invalid token response: {token_response}")
                return {"STATUS": [{"STATUS": "E", "Msg": "Invalid token response"}]}

            print(f"[NCMinerAPI] ✓ Got token (salt: {salt}, time: {time_str})")

            # Step 2: Encrypt password
            print(f"[NCMinerAPI] Step 2: Encrypting password...")
            enc_pwd = self._encrypt_password(salt)
            print(f"[NCMinerAPI] ✓ Password encrypted")

            # Step 3: Build and send privileged command
            print(f"[NCMinerAPI] Step 3: Sending command '{command}'...")
            cmd = {
                "command": command,
                "enc": "1",
                "time": time_str
            }

            # Add parameters
            for key, value in params.items():
                cmd[key] = str(value)

            # Add encrypted password
            cmd["enc_pwd"] = enc_pwd

            cmd_json = json.dumps(cmd)
            print(f"[NCMinerAPI] Command payload: {json.dumps({k: v for k, v in cmd.items() if k != 'enc_pwd'})}")

            result = subprocess.run(
                ['nc', '-w', str(self.timeout), self.ip, str(self.port)],
                input=cmd_json.encode('utf-8'),
                capture_output=True,
                timeout=self.timeout + 2
            )

            if result.returncode == 0:
                response = result.stdout.decode('utf-8', errors='replace').strip()
                print(f"[NCMinerAPI] Response: {response}")
                data = json.loads(response)

                # Check status
                if data:
                    # Handle both old and new API response formats
                    if "STATUS" in data and isinstance(data["STATUS"], list) and data["STATUS"]:
                        status = data["STATUS"][0]
                        status_code = status.get("STATUS", "")
                        msg = status.get("Msg", "")
                    else:
                        # Simple format: {"STATUS":"E","Msg":"..."}
                        status_code = data.get("STATUS", "")
                        msg = data.get("Msg", "")

                    if status_code == "S":
                        print(f"[NCMinerAPI] ✓ SUCCESS: {msg}")
                    elif status_code == "E":
                        print(f"[NCMinerAPI] ✗ ERROR: {msg}")
                    else:
                        print(f"[NCMinerAPI] ? Unknown status: {status_code}")

                return data
            else:
                error_msg = result.stderr.decode()
                print(f"[NCMinerAPI] ✗ nc failed: {error_msg}")
                return {"STATUS": [{"STATUS": "E", "Msg": f"Command failed: {error_msg}"}]}

        except subprocess.TimeoutExpired:
            print(f"[NCMinerAPI] ✗ Timeout")
            return {"STATUS": [{"STATUS": "E", "Msg": "Timeout"}]}
        except json.JSONDecodeError as e:
            print(f"[NCMinerAPI] ✗ JSON decode error: {e}")
            return {"STATUS": [{"STATUS": "E", "Msg": f"JSON decode error: {e}"}]}
        except Exception as e:
            print(f"[NCMinerAPI] ✗ Error: {e}")
            import traceback
            traceback.print_exc()
            return {"STATUS": [{"STATUS": "E", "Msg": str(e)}]}

    def miner_status_cmd(self) -> Dict[str, Any]:
        """Send 'status' command — returns MINING[0].mineroff for authoritative is_off detection."""
        result = self._send_command("status")
        return result if result else {}

    def get_psu(self) -> Dict[str, Any]:
        """Send 'get_psu' command — returns PSU[0].pin for real-time power reading."""
        result = self._send_command("get_psu")
        return result if result else {}

    def set_power_pct(self, percent: int) -> Dict[str, Any]:
        """Set power percentage (requires password) - TEMPORARY."""
        if not 0 < percent <= 100:
            return {"STATUS": [{"STATUS": "E", "Msg": f"Invalid percent: {percent}. Must be 1-100"}]}

        print(f"[NCMinerAPI] Setting power to {percent}% (temporary) on {self.ip}...")
        return self.send_privileged_command("set_power_pct", parameter=str(percent))

    def set_power_limit(self, watts: int) -> Dict[str, Any]:
        """Set power limit in watts (requires password) - PERMANENT."""
        print(f"[NCMinerAPI] Setting power limit to {watts}W (permanent) on {self.ip}...")
        return self.send_privileged_command("adjust_power_limit", power_limit=str(watts))
