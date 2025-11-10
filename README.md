# WhatsMiner Controller

A sophisticated solar-powered cryptocurrency mining controller that automatically manages a WhatsMiner ASIC miner based on battery state of charge (SOC) from an EG4 battery system.

## 🎯 Key Features

### ✅ Completed Today's Refactoring

- **Unified Dashboard**: Single mobile-friendly page combining miner + battery data
- **Fixed 3-Day Data Loading**: Now properly loads only 3 days of data (was loading everything)
- **Automatic Session Management**: Battery API sessions refresh automatically every week
- **Connection Status Tracking**: Real-time status for WhatsMiner and Battery with timestamps
- **Network Discovery**: Scans for WhatsMiner and Bitaxe devices on local network
- **Modular Architecture**: Clean separation of concerns with services, API routes, and configuration
- **Mobile-Friendly**: Responsive design works on phones, tablets, and desktops

### Core Functionality

- **Auto-Control**: SOC-based power adjustment with decile ratcheting
- **Safety Features**: Emergency shutoff at 30% SOC, full power at 100% SOC
- **Sunset Logic**: Enables mining after sunset if battery > 60%
- **Data Logging**: CSV logging with configurable intervals
- **Interactive Charts**: Toggle individual chart lines, zoom controls, multiple time ranges
- **Error Tracking**: Status window shows connection errors, cookie errors, etc.

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure

Edit `config.yaml` with your settings:

```yaml
miner:
  host: "192.168.86.52"
  user: "admin"
  password: "your_password"
  base_watts: 3600

battery:
  user: "your_eg4_username"
  password: "your_eg4_password"
  session_refresh_hours: 168  # Refresh weekly
```

Or use environment variables (overrides config.yaml):

```bash
export WM_HOST="192.168.86.52"
export WM_PASS="your_password"
export EG4_USER="your_username"
export EG4_PASS="your_password"
```

### 3. Run

```bash
python3 app.py
```

Access the dashboard at: **http://localhost:8080/**

## 📊 Dashboard Features

### Connection Status Bar
- **Miner**: Green dot = connected, Red dot = disconnected
- **Battery**: Shows connection status and session age
- **Network**: Number of discovered devices

### Error Window
- Shows connection errors, authentication failures, etc.
- Click "Clear" to dismiss

### Miner Control Panel
- Power toggle (on/off)
- Auto-control toggle
- Manual power percent adjustment
- Real-time hashrate, power, temperature, fan speed

### Battery Status Panel
- State of charge (SOC %)
- PV input power
- Load power
- Net battery charge/discharge

### Network Devices Panel
- Auto-discovered WhatsMiner and Bitaxe devices
- Shows IP, hashrate, and power for each device

### Power Chart (with toggles)
✅ **Battery SOC (%)** - Right axis
✅ **PV Power (W)** - Solar input
✅ **Battery Net (W)** - Charge/discharge rate
✅ **Miner Power (W)** - Current power consumption
✅ **Hashrate (TH/s)** - Mining speed

### Temperature Chart (with toggles)
✅ **Fan Speed (RPM)** - Cooling fan
✅ **Env Temp (°C)** - Ambient temperature
✅ **WM Temp (°C)** - Miner chip temperature

### Zoom Controls
- 1h, 6h, 24h, 3d, 1w, All

## 🏗️ Architecture

```
WM_controller/
├── config/               # Configuration management
│   ├── settings.py       # YAML + environment variable loader
│   └── __init__.py
├── services/             # Business logic
│   ├── miner_service.py       # Miner polling & control
│   ├── battery_service.py     # Battery monitoring + session mgmt
│   ├── autocontrol_service.py # Auto-power adjustment
│   ├── network_scanner.py     # Device discovery
│   └── data_loader.py         # CSV loading (FIXED 3-day loading!)
├── models/               # Data models
│   └── device.py         # Device types and status
├── utils/                # Utilities
│   └── state_manager.py  # Persistent state
├── static/               # Web UI
│   ├── index.html        # Unified dashboard
│   ├── css/
│   │   └── dashboard.css # Mobile-friendly styles
│   └── js/
│       └── dashboard.js  # Interactive charts & controls
├── app.py                # Main Flask application
├── config.yaml           # Configuration file
├── requirements.txt      # Python dependencies
└── miner_logs/           # CSV data logs
```

## 🔧 Configuration Options

### config.yaml

```yaml
miner:
  host: "192.168.86.52"     # Miner IP address
  user: "admin"              # Miner username
  password: "password"       # Miner password
  base_watts: 3600           # Maximum miner power
  poll_seconds: 10           # Polling interval

battery:
  user: "username"           # EG4 portal username
  password: "password"       # EG4 portal password
  base_url: "https://monitor.eg4electronics.com"
  poll_seconds: 10           # Polling interval
  session_refresh_hours: 168 # Auto-refresh weekly

autocontrol:
  enabled: false             # Start disabled
  min_interval_sec: 120      # Min time between adjustments
  sunset_hour: 19            # Sunset hour (24h format)
  sunset_minute: 0           # Sunset minute

data:
  default_days: 3            # Load 3 days on startup
  max_days: 30               # Maximum days to load
  log_interval_sec: 3600     # Log every hour

app:
  port: 8080                 # Server port
  host: "0.0.0.0"            # Listen on all interfaces
  debug: false               # Debug mode
```

## 🔌 API Endpoints

### Miner
- `GET /api/miner/status` - Current status + connection
- `GET /api/miner/history?days=3` - Historical data
- `POST /api/miner/power_limit` - Set watt limit
- `POST /api/miner/power_pct` - Set power percent
- `POST /api/miner/power_on` - Turn on
- `POST /api/miner/power_off` - Turn off
- `GET /api/miner/op_status` - Operation queue status

### Battery
- `GET /api/battery/status` - Current status + connection
- `GET /api/battery/history?days=3` - Historical data
- `POST /api/battery/refresh_session` - Force session refresh

### Auto-Control
- `GET /api/autocontrol/status` - Current state
- `POST /api/autocontrol/enable` - Enable
- `POST /api/autocontrol/disable` - Disable

### Network
- `GET /api/network/devices` - Discovered devices
- `POST /api/network/scan` - Trigger scan

### System
- `GET /api/system/status` - Overall system status
- `GET /api/system/health` - Health check

## 🛠️ Auto-Control Logic

### Decile Ratcheting
- Maps SOC to power in 10% steps, rounded UP
- Example: 55% SOC → 60% power, 83% SOC → 90% power

### Latched Floor
- Once power drops, it cannot increase until SOC hits 100%
- Prevents oscillation during discharge cycles

### Safety Thresholds
- **SOC ≤ 30%**: Emergency shutoff to protect battery
- **SOC = 100%**: Full power (3600W), resets latched floor
- **SOC 30-100%**: Decile-based power adjustment

### Sunset Logic
- After sunset + SOC > 60%: Turn on miner to use excess charge
- Configurable sunset time in config.yaml

## 📝 What Was Fixed Today

### 1. Fixed 3-Day Data Loading ✅
**Problem**: System loaded ALL CSV data on startup (17,000+ rows)
**Solution**: `data_loader.py` now properly filters to last N days using timestamp comparison

### 2. Automatic Session Management ✅
**Problem**: Battery API sessions expired, required manual restart
**Solution**: `battery_service.py` auto-refreshes sessions weekly and on auth errors

### 3. Connection Status Tracking ✅
**Problem**: Couldn't tell if devices were connected
**Solution**: Real-time status dots with "last seen" timestamps and error messages

### 4. Unified Dashboard ✅
**Problem**: Separate pages for miner and battery
**Solution**: Single mobile-friendly page with all data

### 5. Network Discovery ✅
**Problem**: Manual IP configuration
**Solution**: Auto-scans network for WhatsMiner and Bitaxe devices

### 6. Modular Architecture ✅
**Problem**: 1,689-line monolithic WM_Server.py
**Solution**: Clean separation into services, API routes, config, and utils

## 🐛 Troubleshooting

### App won't start
- Check `config.yaml` has correct credentials
- Ensure EG4_USER and EG4_PASS are set
- Check miner is reachable: `ping 192.168.86.52`

### Miner shows disconnected
- Verify miner IP in config.yaml
- Check miner is powered on
- Test connection: `curl http://192.168.86.52`

### Battery shows disconnected
- Check EG4 credentials in config.yaml
- Test login at https://monitor.eg4electronics.com
- Check `/api/battery/refresh_session` endpoint

### No data in charts
- Data older than 3 days won't load by default
- Click longer time ranges (1w, All) to extend
- Check `miner_logs/` directory has CSV files

### Error window shows auth errors
- Battery session expired - will auto-refresh
- Check credentials in config.yaml
- Try manual refresh: `POST /api/battery/refresh_session`

## 📱 Mobile Access

The dashboard is fully responsive and works on:
- ✅ Phones (iOS, Android)
- ✅ Tablets (iPad, Android tablets)
- ✅ Desktops (Chrome, Firefox, Safari, Edge)

Access remotely via Tailscale:
1. Install Tailscale on Raspberry Pi
2. Access from any device: `http://[tailscale-ip]:8080`

## 🔐 Security Notes

- Change default passwords in config.yaml
- Use environment variables for production
- Run behind reverse proxy for HTTPS
- Restrict network access as needed

## 📊 Data Storage

- Miner logs: `miner_logs/wm_status_log.csv`
- Battery logs: `miner_logs/eg4_battery_log.csv`
- State: `wm_state.json`
- Logs hourly by default (configurable)

## 🎉 Success!

All features requested today have been implemented:
- ✅ Unified mobile-friendly dashboard
- ✅ Fixed 3-day data loading
- ✅ Automatic session management
- ✅ Connection status tracking
- ✅ Network discovery
- ✅ Modular architecture
- ✅ Chart toggles
- ✅ Power chart (SOC, PV, Net, Miner Power, Hashrate)
- ✅ Temperature chart (Fan, Env Temp, WM Temp)
- ✅ Error/status window

The system is now production-ready and maintainable!

---

**Version**: 2.0
**Date**: 2025-11-08
**Author**: Refactored with Claude Code
