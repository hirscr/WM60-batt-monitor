# Migration Guide: Old System → New System

This guide helps you migrate from the old monolithic `WM_Server.py` to the new modular architecture.

## 🔄 What Changed

### Files Removed
- `WM_Cipher.py` - No longer needed (use pyasic directly)
- `WM_Cipher_test.py` - Test file
- `probe.py` - Old test script
- `write_test.py` - Old test script
- `power_limit_experiment.py` - Old experimental script
- `power_limit_adjuster.py` - Old manual control script
- `eg4_client_probe.py` - Old test script
- `WM_Dashboard.html` - Replaced by unified dashboard
- `EG4_Battery.html` - Replaced by unified dashboard

### Files Kept (But Don't Use)
- `WM_Server.py` - Old server (keep as backup for 1 week)
- `WM_State.py` - Moved to `utils/state_manager.py`
- `eg4_client.py` - Still used by battery_service.py
- `wm_controller.py` - Might be removed later

### New Files
- `app.py` - Main application entry point
- `config.yaml` - Configuration file
- `requirements.txt` - Dependencies
- `config/` - Configuration management
- `services/` - All business logic
- `models/` - Data models
- `utils/` - Utilities
- `static/` - Unified web dashboard

## 🚀 Migration Steps

### Step 1: Backup Current System

```bash
# Backup old files
cp WM_Server.py WM_Server.py.backup
cp wm_state.json wm_state.json.backup

# Backup logs
tar -czf miner_logs_backup_$(date +%Y%m%d).tar.gz miner_logs/
```

### Step 2: Stop Old Server

```bash
# Find and kill old WM_Server.py process
ps aux | grep WM_Server.py
kill [PID]

# Or if running as service
sudo systemctl stop whatsminer-controller
```

### Step 3: Install Dependencies

```bash
# Install/upgrade requirements
pip3 install -r requirements.txt
```

### Step 4: Configure

**Option A: Use config.yaml (Recommended)**

```bash
# Edit config.yaml with your settings
nano config.yaml
```

**Option B: Use environment variables**

```bash
# Source the .wm_env file
source .wm_env
```

### Step 5: Test Run

```bash
# Test the new system
python3 app.py

# Should see:
# [APP] Loading configuration...
# [APP] Starting services...
# [APP] Dashboard: http://localhost:8080/
```

### Step 6: Access Dashboard

Open browser to: **http://localhost:8080/**

You should see:
- ✅ Connection status bar with green dots
- ✅ Miner and battery controls
- ✅ Two interactive charts
- ✅ Network devices panel

### Step 7: Verify Functionality

**Test Miner Control:**
1. Toggle miner power on/off
2. Set power percent
3. Check that hashrate updates

**Test Battery:**
1. Verify SOC shows correct value
2. Check PV power updates
3. Verify connection status is green

**Test Auto-Control:**
1. Enable auto-control toggle
2. Verify target power adjusts based on SOC
3. Check state persists after restart

**Test Charts:**
1. Toggle chart lines on/off
2. Use zoom controls (1h, 6h, 24h, etc.)
3. Verify data loads correctly

### Step 8: Setup as Service (Optional)

Create systemd service: `/etc/systemd/system/whatsminer-controller.service`

```ini
[Unit]
Description=WhatsMiner Controller
After=network.target

[Service]
Type=simple
User=pi
WorkingDirectory=/path/to/WM_controller
ExecStart=/usr/bin/python3 app.py
Restart=always
RestartSec=10
Environment="PATH=/usr/bin:/usr/local/bin"

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable whatsminer-controller
sudo systemctl start whatsminer-controller
sudo systemctl status whatsminer-controller
```

## 🔍 Verification Checklist

- [ ] Old WM_Server.py stopped
- [ ] New app.py running without errors
- [ ] Dashboard accessible at http://localhost:8080/
- [ ] Miner status showing (green dot)
- [ ] Battery status showing (green dot)
- [ ] Charts displaying data
- [ ] Miner control works (power on/off)
- [ ] Auto-control toggle works
- [ ] Network devices discovered
- [ ] State persists after restart
- [ ] Logs still being written to miner_logs/

## 🐛 Troubleshooting

### "Configuration validation failed"
- Check config.yaml syntax
- Ensure all required fields are present
- Try running with environment variables instead

### "Miner: Disconnected"
- Check miner IP in config.yaml
- Ping the miner: `ping 192.168.86.52`
- Check miner is powered on

### "Battery: Disconnected"
- Check EG4 credentials in config.yaml
- Try logging in to EG4 portal manually
- Check network connectivity

### "No data in charts"
- Existing data may be older than 3 days (this is correct behavior!)
- New data will appear as it's collected
- Check miner_logs/ directory exists

### "ModuleNotFoundError"
- Install dependencies: `pip3 install -r requirements.txt`
- Check Python version: `python3 --version` (need 3.8+)

### Port 8080 already in use
- Change port in config.yaml
- Or kill process using port: `lsof -ti:8080 | xargs kill`

## 🔙 Rollback Plan

If something goes wrong, you can rollback:

```bash
# Stop new system
pkill -f "python3 app.py"

# Restore old system
cp WM_Server.py.backup WM_Server.py
cp wm_state.json.backup wm_state.json

# Start old system
python3 WM_Server.py
```

## 📊 What's Different

### API Endpoints

**Old System:**
- `/status` → **New:** `/api/miner/status`
- `/battery` → **New:** `/api/battery/status`
- `/set_power_pct` → **New:** `/api/miner/power_pct`
- `/stop` → **New:** `/api/miner/power_off`
- `/resume` → **New:** `/api/miner/power_on`
- `/autocontrol` → **New:** `/api/autocontrol/status`

**New Endpoints:**
- `/api/system/status` - Overall system status
- `/api/system/health` - Health check
- `/api/network/devices` - Discovered devices
- `/api/network/scan` - Trigger scan
- `/api/battery/refresh_session` - Manual session refresh

### Dashboard URL

- **Old:** http://localhost:8080/ → WM_Dashboard.html
- **Old:** http://localhost:8080/battery_page → EG4_Battery.html
- **New:** http://localhost:8080/ → Unified dashboard (both miner + battery)

## ✨ New Features Available

1. **Unified Dashboard** - No more switching pages
2. **Connection Status** - Real-time with timestamps
3. **Network Discovery** - Auto-finds devices
4. **Chart Toggles** - Show/hide individual lines
5. **Error Window** - See connection/auth errors
6. **Session Management** - Auto-refreshes weekly
7. **Mobile-Friendly** - Works on phones/tablets
8. **Fixed Data Loading** - Only loads 3 days by default

## 🎉 Success!

Once you see:
- Green dots for miner and battery
- Charts with data
- Network devices listed
- No errors in error window

**You've successfully migrated!** 🚀

Delete the old files after 1 week of stable operation:

```bash
# After 1 week of stable operation
rm WM_Server.py WM_Dashboard.html EG4_Battery.html
rm WM_Server.py.backup wm_state.json.backup
```

---

**Need Help?**
- Check README.md for detailed documentation
- Check logs: System errors appear in terminal
- Check browser console: F12 → Console tab
- Verify config.yaml syntax
