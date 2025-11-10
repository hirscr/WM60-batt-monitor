"""Battery service with automatic session management."""
import time
import threading
import csv
import os
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from eg4_client import EG4Client
from models.device import ConnectionStatus


class BatteryService:
    """
    Service for battery monitoring with automatic session refresh.

    Features:
    - Auto-refreshes session every N hours
    - Retries on auth failures
    - Tracks connection status
    """

    def __init__(self,
                 username: str,
                 password: str,
                 base_url: str,
                 poll_seconds: int,
                 log_interval_sec: int = 600,
                 log_file: str = None,
                 session_refresh_hours: int = 168):  # 1 week default
        """
        Initialize battery service.

        Args:
            username: EG4 username
            password: EG4 password
            base_url: EG4 portal URL
            poll_seconds: Polling interval
            log_interval_sec: Seconds between CSV logs
            log_file: Path to CSV log file
            session_refresh_hours: Hours between forced session refreshes
        """
        self.username = username
        self.password = password
        self.base_url = base_url
        self.poll_seconds = poll_seconds
        self.log_interval_sec = log_interval_sec
        self.log_file = log_file or os.path.join("miner_logs", "eg4_battery_log.csv")
        self.session_refresh_hours = session_refresh_hours
        self.session_refresh_interval = session_refresh_hours * 3600

        self.client: Optional[EG4Client] = None
        self.latest = {}
        self.history: deque = deque()

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_auth_time = 0.0
        self._last_log_ts = 0.0  # Start at 0 so first data point logs immediately

        # Connection status tracking
        self.connection_status = ConnectionStatus(connected=False)
        self._first_connect_time: Optional[float] = None

    def start(self):
        """Start battery service."""
        if self._running:
            return

        print("[BatteryService] Starting...")
        self._running = True

        # Create and start EG4 client
        self.client = EG4Client(
            username=self.username,
            password=self.password,
            base_url=self.base_url,
            poll_seconds=self.poll_seconds
        )
        self.client.start()
        self._last_auth_time = time.time()

        # Start session management thread
        self._thread = threading.Thread(target=self._session_manager_loop, daemon=True)
        self._thread.start()

        print("[BatteryService] Started")

    def stop(self):
        """Stop battery service."""
        self._running = False
        if self.client:
            self.client.stop()
        if self._thread:
            self._thread.join(timeout=5)
        print("[BatteryService] Stopped")

    def _session_manager_loop(self):
        """Background thread to manage session health."""
        print("[BatteryService] Session manager loop started")

        while self._running:
            try:
                # Get latest data
                print("[BatteryService] Getting latest battery data...")
                snap = self.client.get_latest()
                print(f"[BatteryService] RAW RESPONSE: {snap}")
                print(f"[BatteryService] Available keys: {list(snap.keys()) if snap else 'None'}")

                if snap and snap.get("soc_percent") is not None:
                    # Connection successful
                    now = time.time()
                    if not self.connection_status.connected:
                        self._first_connect_time = now
                        print("[BatteryService] ✓ Connected to battery API")

                    self.connection_status.connected = True
                    self.connection_status.last_seen = datetime.now(timezone.utc)
                    if self._first_connect_time:
                        self.connection_status.uptime_seconds = now - self._first_connect_time
                    self.connection_status.error = None

                    self.latest = snap
                    self.history.append(snap)

                    # CSV logging check (log first data immediately, then every log_interval_sec)
                    if (now - self._last_log_ts) >= self.log_interval_sec:
                        self._log_to_csv(snap)
                        self._last_log_ts = now

                    soc = snap.get("soc_percent")
                    pv = snap.get("pv_power_w")
                    load = snap.get("load_power_w")
                    print(f"[BatteryService] Poll successful: SOC={soc}%, PV={pv}W, Load={load}W")

                    # Check if session needs refresh
                    session_age = now - self._last_auth_time
                    if session_age >= self.session_refresh_interval:
                        print(f"[BatteryService] Session {session_age/3600:.1f}h old, refreshing...")
                        if self.refresh_session():
                            print("[BatteryService] Session refreshed successfully")
                        else:
                            print("[BatteryService] Session refresh failed")

                else:
                    # No data - might be auth issue
                    error = self.client.last_error() if self.client else "No client"
                    print(f"[BatteryService] ✗ No data from battery API")
                    if error and ("auth" in error.lower() or "login" in error.lower() or "401" in error):
                        print(f"[BatteryService] Auth error detected: {error}, refreshing session...")
                        if self.refresh_session():
                            print("[BatteryService] Session refreshed after auth error")
                        else:
                            print("[BatteryService] Failed to refresh session after auth error")
                            self.connection_status.connected = False
                            self.connection_status.error = f"Auth failed: {error}"
                    else:
                        # Other error
                        self.connection_status.connected = False
                        self.connection_status.error = error or "Unknown error"
                        print(f"[BatteryService] Error: {error}")

            except Exception as e:
                print(f"[BatteryService] Session manager error: {e}")
                self.connection_status.connected = False
                self.connection_status.error = str(e)
                import traceback
                traceback.print_exc()

            time.sleep(self.poll_seconds)

    def refresh_session(self) -> bool:
        """
        Force session refresh by restarting the client.

        Returns:
            True if refresh successful, False otherwise
        """
        try:
            print("[BatteryService] Refreshing session...")

            # Stop old client
            if self.client:
                self.client.stop()

            # Create new client (forces re-auth)
            self.client = EG4Client(
                username=self.username,
                password=self.password,
                base_url=self.base_url,
                poll_seconds=self.poll_seconds
            )
            self.client.start()
            self._last_auth_time = time.time()

            # Wait a bit for first poll
            time.sleep(5)

            # Check if it worked
            snap = self.client.get_latest()
            if snap and snap.get("soc_percent") is not None:
                print("[BatteryService] Session refresh successful")
                return True
            else:
                print("[BatteryService] Session refresh failed - no data")
                return False

        except Exception as e:
            print(f"[BatteryService] Session refresh error: {e}")
            return False

    def get_status(self) -> dict:
        """Get current battery status."""
        return self.latest.copy() if self.latest else {}

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
            "session_age_hours": (time.time() - self._last_auth_time) / 3600 if self._last_auth_time else None,
        }

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

            print(f"[BatteryService] Logged to CSV: {row.get('ts', 'no timestamp')}")
        except Exception as e:
            print(f"[BatteryService] CSV logging error: {e}")
