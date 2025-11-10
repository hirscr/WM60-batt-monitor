#!/usr/bin/env python3
"""
NC-based miner API - workaround for macOS socket issues.
Uses nc command via subprocess instead of Python sockets.
Supports encrypted privileged commands using MD5 crypt authentication.
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
        """Power off miner."""
        return self._send_command("power_off")

    def power_on(self):
        """Power on miner."""
        return self._send_command("power_on")

    def get_token(self) -> Optional[Dict[str, Any]]:
        """Get authentication token from miner."""
        return self._send_command("get_token")

    def _encrypt_password(self, salt: str) -> str:
        """Encrypt password using MD5 crypt with salt."""
        if not self.pwd:
            raise ValueError("Password not set")
        # Use MD5 crypt (same format as pyasic: $1$salt$hash)
        return md5_crypt.using(salt=salt).hash(self.pwd)

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
            # Step 1: Get token
            print(f"[NCMinerAPI] Step 1: Getting authentication token...")
            token_response = self.get_token()

            if not token_response or "Msg" not in token_response:
                print(f"[NCMinerAPI] ✗ Failed to get token: {token_response}")
                return {"STATUS": [{"STATUS": "E", "Msg": "Failed to get authentication token"}]}

            msg = token_response.get("Msg", {})
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
