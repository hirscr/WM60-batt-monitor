"""Data loading service with proper 3-day default and lazy loading."""
import csv
import os
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Deque, Optional


class DataLoader:
    """Manages CSV data loading with proper date filtering."""

    def __init__(self, log_dir: str, default_days: int = 3, max_days: int = 30):
        """
        Initialize data loader.

        Args:
            log_dir: Directory containing log files
            default_days: Default number of days to load (fixes broken 3-day loading)
            max_days: Maximum number of days to load
        """
        self.log_dir = log_dir
        self.default_days = default_days
        self.max_days = max_days
        self.miner_loaded_days = 0
        self.battery_loaded_days = 0

        self.miner_log_file = os.path.join(log_dir, "wm_status_log.csv")
        self.battery_log_file = os.path.join(log_dir, "eg4_battery_log.csv")

    def load_miner_data(self, days: Optional[int] = None) -> list[dict]:
        """
        Load miner data for the last N days.

        Args:
            days: Number of days to load (default: self.default_days)

        Returns:
            List of data rows as dicts
        """
        if days is None:
            days = self.default_days

        days = max(1, min(self.max_days, days))
        rows = self._load_csv_by_days(self.miner_log_file, days)
        self.miner_loaded_days = days
        print(f"[DataLoader] Loaded {len(rows)} miner rows from last {days} days")
        return rows

    def load_battery_data(self, days: Optional[int] = None) -> list[dict]:
        """
        Load battery data for the last N days.

        Args:
            days: Number of days to load (default: self.default_days)

        Returns:
            List of data rows as dicts
        """
        if days is None:
            days = self.default_days

        days = max(1, min(self.max_days, days))
        rows = self._load_csv_by_days(self.battery_log_file, days)
        self.battery_loaded_days = days
        print(f"[DataLoader] Loaded {len(rows)} battery rows from last {days} days")
        return rows

    def extend_miner_data(self, days: int) -> list[dict]:
        """
        Extend miner data to N days.

        Args:
            days: Total number of days to load

        Returns:
            List of data rows as dicts
        """
        if days <= self.miner_loaded_days:
            print(f"[DataLoader] Miner already has {self.miner_loaded_days} days, skipping")
            return []

        days = min(self.max_days, days)
        rows = self._load_csv_by_days(self.miner_log_file, days)
        self.miner_loaded_days = days
        print(f"[DataLoader] Extended miner data to {days} days ({len(rows)} rows)")
        return rows

    def extend_battery_data(self, days: int) -> list[dict]:
        """
        Extend battery data to N days.

        Args:
            days: Total number of days to load

        Returns:
            List of data rows as dicts
        """
        if days <= self.battery_loaded_days:
            print(f"[DataLoader] Battery already has {self.battery_loaded_days} days, skipping")
            return []

        days = min(self.max_days, days)
        rows = self._load_csv_by_days(self.battery_log_file, days)
        self.battery_loaded_days = days
        print(f"[DataLoader] Extended battery data to {days} days ({len(rows)} rows)")
        return rows

    def _load_csv_by_days(self, file_path: str, days: int) -> list[dict]:
        """
        Load CSV file and filter to last N days.

        This FIXES the broken 3-day loading - it actually filters by date now!

        Args:
            file_path: Path to CSV file
            days: Number of days to load

        Returns:
            List of rows as dicts
        """
        if not os.path.exists(file_path):
            print(f"[DataLoader] File not found: {file_path}")
            return []

        # Calculate cutoff timestamp
        now = datetime.now(timezone.utc)
        cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)
        print(f"[DataLoader] Loading data from {file_path}, cutoff date: {cutoff.isoformat()}, current time: {now.isoformat()}")

        loaded_rows = []
        skipped = 0

        try:
            with open(file_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Handle multiple timestamp formats:
                    # 1. "ts" field (battery CSV)
                    # 2. separate "date" and "time" fields (old miner CSV)
                    # 3. first column contains full ISO timestamp (new miner CSV)
                    ts_str = None

                    if row.get("ts"):
                        ts_str = row["ts"]
                    elif row.get("date") and row.get("time"):
                        # Check if "date" column actually contains a full timestamp
                        date_val = row["date"]
                        if "T" in date_val or ":" in date_val:
                            # First column is actually a full timestamp
                            ts_str = date_val
                            row["ts"] = ts_str  # Add ts field for compatibility
                        else:
                            # Separate date and time columns
                            ts_str = f"{date_val}T{row['time']}"
                            row["ts"] = ts_str  # Add ts field for compatibility

                    if not ts_str:
                        skipped += 1
                        continue

                    try:
                        # Parse timestamp - ensure timezone aware for comparison
                        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))

                        # CRITICAL: If timezone-naive, add UTC timezone
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)

                        # Filter by cutoff date
                        if dt >= cutoff:
                            loaded_rows.append(row)
                        else:
                            skipped += 1

                    except Exception as e:
                        print(f"[DataLoader] Skipping row with bad timestamp: {ts_str} - {e}")
                        skipped += 1
                        continue

        except Exception as e:
            print(f"[DataLoader] Error reading {file_path}: {e}")
            return []

        # Log summary with date details
        print(f"[DataLoader] ===== CSV LOAD SUMMARY =====")
        print(f"[DataLoader] File: {os.path.basename(file_path)}")
        print(f"[DataLoader] Cutoff date: {cutoff.date()} (loading last {days} days)")
        print(f"[DataLoader] Total rows in CSV: {len(loaded_rows) + skipped}")
        print(f"[DataLoader] Rows loaded: {len(loaded_rows)}")
        print(f"[DataLoader] Rows skipped (too old): {skipped}")

        if loaded_rows:
            # Show date range of loaded data
            first_ts = loaded_rows[0].get('ts', 'unknown')
            last_ts = loaded_rows[-1].get('ts', 'unknown')
            print(f"[DataLoader] Date range: {first_ts} to {last_ts}")
            print(f"[DataLoader] ✓ Data loaded successfully!")
        else:
            print(f"[DataLoader] ⚠️  NO DATA LOADED!")
            print(f"[DataLoader] All CSV data is older than {cutoff.date()}")
        print(f"[DataLoader] ==============================")

        # DEBUG: Log first 3 rows to see format
        if loaded_rows:
            print(f"[DataLoader] === FIRST 3 ROWS ===")
            for i, row in enumerate(loaded_rows[:3]):
                print(f"[DataLoader] Row {i+1}: ts={row.get('ts')}, type={type(row.get('ts'))}")
                # Show a few data fields
                keys = list(row.keys())[:5]
                print(f"[DataLoader]   Fields: {', '.join([f'{k}={row.get(k)}' for k in keys])}")

        return loaded_rows

    def get_stats(self) -> dict:
        """Get statistics about loaded data."""
        return {
            "miner_loaded_days": self.miner_loaded_days,
            "battery_loaded_days": self.battery_loaded_days,
            "default_days": self.default_days,
            "max_days": self.max_days,
        }
