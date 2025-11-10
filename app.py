#!/usr/bin/env python3
"""
WhatsMiner Controller - Unified Application

Refactored modular architecture with:
- Configuration management
- Service-based architecture
- Connection status tracking
- Automatic session management
- Network discovery
- Fixed 3-day data loading
"""
import os
import sys
import logging
import threading
from collections import deque
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory

# Load configuration
from config import load_settings
from utils.state_manager import StateManager

# Services
from services.data_loader import DataLoader
from services.miner_service import MinerService
from services.battery_service import BatteryService
from services.autocontrol_service import AutoControlService
from services.network_scanner import NetworkScanner

# ========== LOG BUFFER ==========
class LogBuffer:
    """Thread-safe buffer for capturing backend logs."""

    def __init__(self, maxlen=500):
        self.logs = deque(maxlen=maxlen)
        self.lock = threading.Lock()

    def add(self, message: str, level: str = "info"):
        """Add a log message with timestamp."""
        with self.lock:
            self.logs.append({
                "timestamp": datetime.now().isoformat(),
                "message": message,
                "level": level
            })

    def get_recent(self, count: int = 200):
        """Get recent log messages."""
        with self.lock:
            return list(self.logs)[-count:]

    def clear(self):
        """Clear all logs."""
        with self.lock:
            self.logs.clear()

# Global log buffer
log_buffer = LogBuffer()

class LogCapture:
    """Captures stdout/stderr and routes to log buffer."""

    def __init__(self, original, level="info"):
        self.original = original
        self.level = level

    def write(self, text):
        """Capture write calls."""
        # Handle both str and bytes
        if isinstance(text, bytes):
            # Just write bytes directly to original
            self.original.write(text)
            self.original.flush()
            return

        if text and text.strip():
            # Still write to original
            self.original.write(text)
            self.original.flush()

            # Also add to log buffer
            text = text.strip()

            # Determine log level from content
            level = self.level
            if "error" in text.lower() or "✗" in text:
                level = "error"
            elif "warning" in text.lower():
                level = "warning"
            elif "✓" in text or "success" in text.lower():
                level = "success"

            log_buffer.add(text, level)

    def flush(self):
        """Flush the stream."""
        self.original.flush()

# Capture stdout and stderr
sys.stdout = LogCapture(sys.stdout, "info")
sys.stderr = LogCapture(sys.stderr, "error")

# Initialize Flask app
app = Flask(__name__, static_folder="static", static_url_path="/static")

# Load settings
print("[APP] Loading configuration...")
settings = load_settings()
print(f"[APP] Config loaded: default_days={settings.data.default_days}, max_days={settings.data.max_days}")

# Initialize state manager
state_mgr = StateManager(path="./wm_state.json")

# Initialize services
print("[APP] Initializing services...")

# Data loader
data_loader = DataLoader(
    log_dir="./miner_logs",
    default_days=settings.data.default_days,
    max_days=settings.data.max_days
)

# Miner service
miner_service = MinerService(
    host=settings.miner.host,
    password=settings.miner.password,
    poll_seconds=settings.miner.poll_seconds,
    log_interval_sec=settings.data.log_interval_sec
)

# Battery service
battery_service = BatteryService(
    username=settings.battery.user,
    password=settings.battery.password,
    base_url=settings.battery.base_url,
    poll_seconds=settings.battery.poll_seconds,
    log_interval_sec=settings.data.log_interval_sec,
    session_refresh_hours=settings.battery.session_refresh_hours
)

# Auto-control service
autocontrol_service = AutoControlService(
    miner_service=miner_service,
    battery_service=battery_service,
    state_manager=state_mgr,
    base_watts=settings.miner.base_watts,
    min_interval_sec=settings.autocontrol.min_interval_sec,
    mode=settings.autocontrol.mode,
    away_config={
        "emergency_soc": settings.autocontrol.away_mode.emergency_soc,
        "max_pv_power": settings.autocontrol.away_mode.max_pv_power,
        "after_sunset_min_soc": settings.autocontrol.away_mode.after_sunset_min_soc
    },
    location_config={
        "latitude": settings.autocontrol.location.latitude,
        "longitude": settings.autocontrol.location.longitude,
        "timezone": settings.autocontrol.location.timezone
    },
    sunset_hour=settings.autocontrol.sunset_hour,
    sunset_minute=settings.autocontrol.sunset_minute
)

# Network scanner
network_scanner = NetworkScanner(subnet="192.168.86")

# ========== API ROUTES ==========

@app.route("/")
def index():
    """Serve unified dashboard."""
    return send_from_directory("static", "index.html")

# --- Miner Routes ---
@app.get("/api/miner/status")
def miner_status():
    """Get current miner status."""
    status = miner_service.get_status()
    connection = miner_service.get_connection_status()
    return jsonify({
        "status": status,
        "connection": connection
    })

@app.get("/api/miner/test_communication")
def test_miner_communication():
    """Test direct communication with miner - returns raw summary."""
    import asyncio
    from pyasic.rpc.btminer import BTMinerRPCAPI

    print("\n" + "="*60)
    print("[TEST] DIRECT MINER COMMUNICATION TEST")
    print(f"[TEST] Miner IP: {settings.miner.host}")
    print(f"[TEST] Password configured: {bool(settings.miner.password)}")
    print("="*60)

    try:
        api = BTMinerRPCAPI(settings.miner.host)
        api.pwd = settings.miner.password

        print("[TEST] Step 1: Getting summary...")
        summary = asyncio.run(api.summary())
        print(f"[TEST] Summary response: {summary}")

        print("[TEST] Step 2: Getting token...")
        token = asyncio.run(api.get_token())
        print(f"[TEST] Token response: {token}")

        print("[TEST] Step 3: Sending test power limit command (3500W)...")
        result = asyncio.run(api.send_privileged_command("set_power_limit", power_limit="3500"))
        print(f"[TEST] Power limit response: {result}")

        print("="*60 + "\n")

        return jsonify({
            "success": True,
            "summary": summary,
            "token": str(token),
            "power_command_result": result,
            "message": "Communication test complete - check server logs"
        })

    except Exception as e:
        print(f"[TEST] ✗ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        print("="*60 + "\n")

        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })

@app.get("/api/miner/history")
def miner_history():
    """Get miner history."""
    try:
        days = int(request.args.get('days', settings.data.default_days))
        days = max(1, min(settings.data.max_days, days))
    except (ValueError, TypeError):
        days = settings.data.default_days

    # Load data if needed
    if days > data_loader.miner_loaded_days:
        rows = data_loader.extend_miner_data(days)
    else:
        rows = data_loader.load_miner_data(days)

    # DEBUG: Log what we're returning
    print(f"[API /api/miner/history] Returning {len(rows)} rows")
    if rows:
        print(f"[API] First row timestamp: {rows[0].get('ts')} (type: {type(rows[0].get('ts'))})")
        print(f"[API] First row sample: {list(rows[0].items())[:5]}")

    return jsonify({
        "data": rows,
        "meta": {
            "loaded_days": data_loader.miner_loaded_days,
            "requested_days": days
        }
    })

@app.post("/api/miner/power_limit")
def set_miner_power_limit():
    """Set miner power limit (watts)."""
    data = request.get_json() or {}
    watts = int(data.get("watts", 0))
    miner_service.set_power_limit(watts)
    return jsonify({"ok": True, "watts": watts})

@app.post("/api/miner/power_pct")
def set_miner_power_pct():
    """Set miner power percent (0-100)."""
    data = request.get_json() or {}
    percent = int(data.get("percent", 0))
    watts = int(3600 * (percent / 100))

    print("\n" + "="*60)
    print(f"[API] POWER CONTROL REQUEST")
    print(f"[API] Target: {percent}% = {watts}W")
    print(f"[API] Time: {datetime.now().isoformat()}")
    print("="*60)

    miner_service.set_power_pct(percent)
    state_mgr.save(target_power_pct=percent)

    print(f"[API] ✓ Command enqueued to miner service")
    print("="*60 + "\n")

    return jsonify({"ok": True, "percent": percent, "watts": watts})

@app.post("/api/miner/power_on")
def miner_power_on():
    """Turn miner on."""
    miner_service.power_on()
    state_mgr.save(miner_power_state="running")
    return jsonify({"ok": True})

@app.post("/api/miner/power_off")
def miner_power_off():
    """Turn miner off."""
    miner_service.power_off()
    state_mgr.save(miner_power_state="stopped")
    return jsonify({"ok": True})

@app.get("/api/miner/op_status")
def miner_op_status():
    """Get miner operation status."""
    return jsonify(miner_service.get_op_status())

# --- Battery Routes ---
@app.get("/api/battery/status")
def battery_status():
    """Get current battery status."""
    status = battery_service.get_status()
    connection = battery_service.get_connection_status()
    return jsonify({
        "status": status,
        "connection": connection
    })

@app.get("/api/battery/history")
def battery_history():
    """Get battery history."""
    try:
        days = int(request.args.get('days', settings.data.default_days))
        days = max(1, min(settings.data.max_days, days))
    except (ValueError, TypeError):
        days = settings.data.default_days

    # Load data if needed
    if days > data_loader.battery_loaded_days:
        rows = data_loader.extend_battery_data(days)
    else:
        rows = data_loader.load_battery_data(days)

    # DEBUG: Log what we're returning
    print(f"[API /api/battery/history] Returning {len(rows)} rows")
    if rows:
        print(f"[API] First row timestamp: {rows[0].get('ts')} (type: {type(rows[0].get('ts'))})")
        print(f"[API] First row sample: {list(rows[0].items())[:5]}")

    return jsonify({
        "data": rows,
        "meta": {
            "loaded_days": data_loader.battery_loaded_days,
            "requested_days": days
        }
    })

@app.post("/api/battery/refresh_session")
def battery_refresh_session():
    """Force battery session refresh."""
    success = battery_service.refresh_session()
    return jsonify({"ok": success})

# --- Chart Data Route ---
@app.get("/api/chart-data")
def chart_data():
    """Get unified chart data with all metrics."""
    print("\n" + "="*50)
    print("[ChartAPI] === CHART DATA REQUEST ===")

    try:
        hours = int(request.args.get('hours', 72))  # Default 3 days
        hours = max(1, min(hours, 8760))  # Cap at 1 year
        days = max(1, (hours + 23) // 24)  # Convert to days, round up
    except (ValueError, TypeError):
        hours = 72
        days = 3

    # Helper function to normalize timestamps
    def normalize_timestamp(ts_str):
        """
        Convert any timestamp format to ISO 8601 with UTC timezone.
        Handles:
        - ISO with timezone: 2025-08-30T17:30:16-04:00
        - ISO without timezone: 2025-08-30T04:47:24
        - Various other formats
        Returns ISO string with UTC: 2025-08-30T12:47:24+00:00
        """
        if not ts_str:
            return None

        from datetime import datetime, timezone

        try:
            # Parse timestamp - handle various formats
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))

            # If no timezone, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            # Convert to UTC
            dt_utc = dt.astimezone(timezone.utc)

            # Return ISO string
            return dt_utc.isoformat()
        except Exception as e:
            print(f"[ChartAPI] ⚠️  Failed to parse timestamp '{ts_str}': {e}")
            return None

    try:
        print(f"[ChartAPI] Requested: {hours} hours = {days} days")
        print(f"[ChartAPI] Currently loaded: miner={data_loader.miner_loaded_days} days, battery={data_loader.battery_loaded_days} days")

        # Load miner and battery data
        if days > data_loader.miner_loaded_days:
            print(f"[ChartAPI] Extending miner data to {days} days...")
            miner_rows = data_loader.extend_miner_data(days)
        else:
            print(f"[ChartAPI] Using existing miner data ({data_loader.miner_loaded_days} days)...")
            miner_rows = data_loader.load_miner_data(days)

        if days > data_loader.battery_loaded_days:
            print(f"[ChartAPI] Extending battery data to {days} days...")
            battery_rows = data_loader.extend_battery_data(days)
        else:
            print(f"[ChartAPI] Using existing battery data ({data_loader.battery_loaded_days} days)...")
            battery_rows = data_loader.load_battery_data(days)

        print(f"[ChartAPI] Miner CSV rows: {len(miner_rows)}")
        print(f"[ChartAPI] Battery CSV rows: {len(battery_rows)}")

        # ALSO get in-memory live data from services
        miner_live = miner_service.get_history()  # Get live polling data
        battery_live = battery_service.get_history()  # Get live polling data
        print(f"[ChartAPI] Miner live rows: {len(miner_live)}")
        print(f"[ChartAPI] Battery live rows: {len(battery_live)}")

        # Combine CSV + live data
        all_miner_rows = miner_rows + miner_live
        all_battery_rows = battery_rows + battery_live
        print(f"[ChartAPI] Total miner (CSV+live): {len(all_miner_rows)}")
        print(f"[ChartAPI] Total battery (CSV+live): {len(all_battery_rows)}")

        # Log sample timestamps BEFORE normalization
        if all_miner_rows:
            print(f"[ChartAPI] Sample miner timestamp (before): {all_miner_rows[0].get('ts')}")
        if all_battery_rows:
            print(f"[ChartAPI] Sample battery timestamp (before): {all_battery_rows[0].get('ts')}")

        # Create timestamp index for miner data with NORMALIZED timestamps
        miner_by_ts = {}
        for row in all_miner_rows:
            ts = row.get('ts')
            if ts:
                normalized_ts = normalize_timestamp(ts)
                if normalized_ts:
                    miner_by_ts[normalized_ts] = row

        # Create timestamp index for battery data with NORMALIZED timestamps
        battery_by_ts = {}
        for row in all_battery_rows:
            ts = row.get('ts')
            if ts:
                normalized_ts = normalize_timestamp(ts)
                if normalized_ts:
                    battery_by_ts[normalized_ts] = row

        # Log sample normalized timestamps
        if miner_by_ts:
            first_normalized = list(miner_by_ts.keys())[0]
            print(f"[ChartAPI] Sample miner timestamp (after): {first_normalized}")
        if battery_by_ts:
            first_normalized = list(battery_by_ts.keys())[0]
            print(f"[ChartAPI] Sample battery timestamp (after): {first_normalized}")

        # Merge data by timestamp (union of all timestamps)
        all_timestamps = sorted(set(list(miner_by_ts.keys()) + list(battery_by_ts.keys())))

        # Build unified data array
        # Helper to safely convert to float (handles None and empty strings)
        def safe_float(val):
            if val is None or val == '':
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        def safe_int(val):
            if val is None or val == '':
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        data = []
        for ts in all_timestamps:
            miner = miner_by_ts.get(ts, {})
            battery = battery_by_ts.get(ts, {})

            # Extract miner data
            hashrate = miner.get('Hashrate')
            miner_power = miner.get('Power')
            fan_speed = None
            for k in miner.keys():
                # Add None check before calling .lower()
                if k and (('fan' in k.lower()) or ('rpm' in k.lower())):
                    try:
                        fan_speed = int(miner[k])
                        break
                    except:
                        pass
            env_temp = miner.get('Env Temp') or miner.get('Env Temperature')
            miner_temp = miner.get('Temperature') or miner.get('Chip Temp Avg')

            # Extract battery data
            battery_soc = battery.get('soc_percent')
            pv_power = battery.get('pv_power_w')
            eps_power = battery.get('load_power_w')
            net_power = battery.get('battery_net_w')

            data.append({
                "timestamp": ts,
                "hash_rate": safe_float(hashrate),
                "miner_power": safe_float(miner_power),
                "fan_speed": safe_int(fan_speed),
                "env_temp": safe_float(env_temp),
                "miner_temp": safe_float(miner_temp),
                "battery_soc": safe_float(battery_soc),
                "pv_power": safe_float(pv_power),
                "eps_power": safe_float(eps_power),
                "net_power": safe_float(net_power),
            })

        print(f"[ChartAPI] === FINAL RESULTS ===")
        print(f"[ChartAPI] Merged timestamps: {len(all_timestamps)}")
        print(f"[ChartAPI] Final data points: {len(data)}")
        if len(data) > 0:
            print(f"[ChartAPI] First timestamp: {data[0]['timestamp']}")
            print(f"[ChartAPI] Last timestamp: {data[-1]['timestamp']}")
            print(f"[ChartAPI] Sample row: hash_rate={data[0]['hash_rate']}, miner_power={data[0]['miner_power']}, battery_soc={data[0]['battery_soc']}")
        else:
            print(f"[ChartAPI] ⚠️  NO DATA IN FINAL RESULT!")
        print("="*50 + "\n")

        return jsonify({"data": data, "hours": hours, "count": len(data)})

    except Exception as e:
        print(f"[ChartAPI] ❌ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        print("="*50 + "\n")
        return jsonify({"error": str(e), "data": [], "count": 0}), 500

# --- Auto-Control Routes ---
@app.get("/api/autocontrol/status")
def autocontrol_status():
    """Get auto-control status."""
    state = autocontrol_service.get_state()

    # If auto-control is disabled, get the manually set target from state manager
    if not state.get("enabled"):
        saved_state = state_mgr.load()
        manual_target_pct = saved_state.get("target_power_pct")
        if manual_target_pct is not None:
            state["target_pct"] = manual_target_pct
            state["target_w"] = int(round(settings.miner.base_watts * (manual_target_pct / 100.0)))

    return jsonify(state)

@app.post("/api/autocontrol/enable")
def autocontrol_enable():
    """Enable auto-control."""
    autocontrol_service.enable()
    return jsonify({"ok": True, "enabled": True})

@app.post("/api/autocontrol/disable")
def autocontrol_disable():
    """Disable auto-control."""
    autocontrol_service.disable()
    return jsonify({"ok": True, "enabled": False})

@app.post("/api/autocontrol/set-mode")
def autocontrol_set_mode():
    """Set auto-control mode (away or present)."""
    data = request.get_json()
    mode = data.get("mode")

    if not mode:
        return jsonify({"ok": False, "error": "Mode parameter required"}), 400

    success = autocontrol_service.set_mode(mode)
    if success:
        return jsonify({"ok": True, "mode": mode})
    else:
        return jsonify({"ok": False, "error": "Invalid mode or mode not implemented"}), 400

# --- Network Routes ---
@app.get("/api/network/devices")
def network_devices():
    """Get discovered network devices."""
    return jsonify(network_scanner.get_scan_info())

@app.post("/api/network/scan")
def network_scan():
    """Trigger immediate network scan."""
    devices = network_scanner.scan_network()
    return jsonify({
        "ok": True,
        "device_count": len(devices),
        "devices": [
            {
                "ip": d.ip,
                "type": d.device_type.value,
                "hashrate_ths": d.hashrate_ths,
                "power_w": d.power_w,
            }
            for d in devices
        ]
    })

# --- System Routes ---
@app.get("/api/system/status")
def system_status():
    """Get overall system status."""
    return jsonify({
        "miner": miner_service.get_connection_status(),
        "battery": battery_service.get_connection_status(),
        "autocontrol": autocontrol_service.get_state(),
        "network": network_scanner.get_scan_info(),
        "data_loader": data_loader.get_stats()
    })

@app.get("/api/system/health")
def system_health():
    """Health check endpoint."""
    return jsonify({"ok": True, "status": "healthy"})

@app.get("/api/system/logs")
def system_logs():
    """Get recent system logs."""
    try:
        count = int(request.args.get('count', 200))
        count = max(1, min(500, count))
    except (ValueError, TypeError):
        count = 200

    logs = log_buffer.get_recent(count)
    return jsonify({"logs": logs})

@app.post("/api/system/logs/clear")
def clear_system_logs():
    """Clear system logs."""
    log_buffer.clear()
    return jsonify({"ok": True})

# ========== MAIN ==========
if __name__ == "__main__":
    # Load initial data
    print("[APP] Loading initial data...")
    data_loader.load_miner_data(settings.data.default_days)
    data_loader.load_battery_data(settings.data.default_days)

    # Start services
    print("[APP] Starting services...")
    miner_service.start()
    battery_service.start()
    autocontrol_service.start()
    network_scanner.start_background_scan()

    # Configure logging
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    print(f"[APP] Starting Flask server on {settings.app.host}:{settings.app.port}")
    print(f"[APP] Dashboard: http://localhost:{settings.app.port}/")

    # Run Flask app
    app.run(
        host=settings.app.host,
        port=settings.app.port,
        debug=settings.app.debug
    )
