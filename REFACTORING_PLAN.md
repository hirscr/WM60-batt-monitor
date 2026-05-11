# WhatsMiner Controller Refactoring Plan

## Executive Summary

This document outlines the comprehensive refactoring of the WhatsMiner controller system from a monolithic 1,689-line Flask server into a well-organized, modular architecture with enhanced features.

## Current State Analysis

### Problems Identified

1. **Monolithic WM_Server.py** (1,689 lines)
   - All functionality in a single file
   - Difficult to test, maintain, and extend
   - Mixed concerns: API routes, business logic, data access, threading

2. **Broken 3-Day Data Loading**
   - System attempts to load 3 days by default but loads everything
   - No lazy loading mechanism
   - Memory inefficient for long-running deployments

3. **Cookie/Session Management Issues**
   - Battery API authentication fails silently
   - No automatic cookie refresh
   - Manual re-login required

4. **Separate Dashboards**
   - WM_Dashboard.html and EG4_Battery.html are completely separate
   - User must navigate between pages
   - No unified view of system state

5. **No Connection Status Visibility**
   - Can't tell if devices are connected without checking data
   - No timestamps for last successful connection
   - No uptime tracking

6. **No Network Discovery**
   - Manual IP configuration required
   - Can't find Bitaxe or WhatsMiner automatically
   - No network scanning capability

## Proposed Architecture

### New Project Structure

```
WM_controller/
├── config/
│   ├── __init__.py
│   ├── settings.py          # Configuration management (YAML + env vars)
│   └── default.yaml         # Default configuration
│
├── models/
│   ├── __init__.py
│   ├── miner.py             # Miner data models
│   ├── battery.py           # Battery data models
│   └── device.py            # Network device models
│
├── services/
│   ├── __init__.py
│   ├── miner_service.py     # Miner control & polling logic
│   ├── battery_service.py   # Battery monitoring & session management
│   ├── autocontrol_service.py  # Auto-control algorithm
│   ├── network_scanner.py   # Network discovery service
│   └── data_loader.py       # CSV loading with lazy loading
│
├── api/
│   ├── __init__.py
│   ├── routes.py            # Route registration
│   ├── miner_routes.py      # Miner control endpoints
│   ├── battery_routes.py    # Battery endpoints
│   ├── data_routes.py       # History/data endpoints
│   └── system_routes.py     # Status, health, debug endpoints
│
├── utils/
│   ├── __init__.py
│   ├── csv_logger.py        # CSV logging utilities
│   ├── state_manager.py     # State persistence (move from WM_State.py)
│   └── time_utils.py        # Timestamp formatting utilities
│
├── static/
│   ├── css/
│   │   └── dashboard.css    # Unified dashboard styles
│   ├── js/
│   │   ├── dashboard.js     # Main dashboard logic
│   │   ├── miner.js         # Miner-specific UI
│   │   ├── battery.js       # Battery-specific UI
│   │   └── network.js       # Network status UI
│   └── index.html           # Unified dashboard
│
├── tests/
│   ├── __init__.py
│   ├── test_miner_service.py
│   ├── test_battery_service.py
│   ├── test_autocontrol.py
│   ├── test_network_scanner.py
│   ├── test_api.py
│   └── fixtures/            # Test data fixtures
│
├── scripts/
│   ├── probe.py             # Keep existing utility scripts
│   ├── eg4_client_probe.py
│   └── ...
│
├── app.py                   # Main Flask application (slim entry point)
├── requirements.txt         # Python dependencies
├── config.yaml              # User configuration
├── .env.example             # Environment variable template
├── README.md                # Comprehensive documentation
└── miner_logs/              # Log directory
```

## Module Breakdown

### 1. config/settings.py
**Purpose**: Centralized configuration management

**Responsibilities**:
- Load config from YAML file
- Override with environment variables
- Validate all configuration on startup
- Provide type-safe config access
- Handle defaults

**Key Classes**:
```python
class MinerConfig:
    host: str
    user: str
    password: str
    base_watts: int
    poll_seconds: int

class BatteryConfig:
    user: str
    password: str
    base_url: str
    poll_seconds: int
    session_refresh_hours: int  # NEW: auto-refresh interval

class AutoControlConfig:
    enabled: bool
    min_interval_sec: int
    sunset_hour: int
    sunset_minute: int

class DataConfig:
    default_days: int = 3       # NEW: enforce 3-day default
    max_days: int = 30
    log_interval_sec: int

class AppConfig:
    port: int
    host: str
    debug: bool

class Settings:
    miner: MinerConfig
    battery: BatteryConfig
    autocontrol: AutoControlConfig
    data: DataConfig
    app: AppConfig

    @classmethod
    def load(cls) -> Settings:
        """Load from config.yaml + environment variables"""

    def validate(self):
        """Validate all settings, raise if invalid"""
```

### 2. services/miner_service.py
**Purpose**: All miner-related logic

**Responsibilities**:
- Miner polling thread
- Command queue (MinerController)
- Power control commands
- Status reporting

**Key Classes**:
```python
class MinerService:
    def __init__(self, config: MinerConfig, state_manager: StateManager):
        self.api = BTMinerRPCAPI(config.host)
        self.controller = MinerController(self.api)
        self.latest = {}
        self.history = deque()

    def start_polling(self):
        """Start background polling thread"""

    def stop(self):
        """Stop polling and cleanup"""

    def get_status(self) -> dict:
        """Get current miner status"""

    def set_power_limit(self, watts: int):
        """Queue power limit command"""

    def set_power_pct(self, percent: int):
        """Queue power percent command"""

    def power_on(self):
        """Queue power on command"""

    def power_off(self):
        """Queue power off command"""

    def get_history(self, hours: int = 24) -> list:
        """Get history for last N hours"""

    def get_connection_status(self) -> dict:
        """NEW: Return connection status with timestamps"""
        # Returns: {'connected': True, 'last_seen': timestamp, 'uptime_seconds': 123}
```

### 3. services/battery_service.py
**Purpose**: Battery monitoring with session management

**Responsibilities**:
- EG4 battery polling
- **NEW: Automatic session refresh**
- **NEW: Cookie management**
- Connection status tracking

**Key Classes**:
```python
class BatteryService:
    def __init__(self, config: BatteryConfig):
        self.client = EG4Client(...)
        self.latest = {}
        self.history = deque()
        self.last_auth_time = None
        self.session_refresh_interval = config.session_refresh_hours * 3600

    def start_polling(self):
        """Start background polling thread"""

    def stop(self):
        """Stop polling and cleanup"""

    def get_status(self) -> dict:
        """Get current battery status"""

    def get_history(self, hours: int = 24) -> list:
        """Get history for last N hours"""

    def refresh_session(self) -> bool:
        """NEW: Force session refresh"""
        # Re-authenticate and get new cookies

    def check_session_health(self):
        """NEW: Check if session needs refresh"""
        # Called periodically in polling thread
        # If data fetch fails due to auth, auto-refresh

    def get_connection_status(self) -> dict:
        """NEW: Return connection status with timestamps"""
```

### 4. services/autocontrol_service.py
**Purpose**: Automated power control based on battery SOC

**Responsibilities**:
- SOC-based power adjustment
- Decile ratcheting logic
- Sunset detection
- State tracking (latched floor, last set power)

**Key Classes**:
```python
class AutoControlService:
    def __init__(self,
                 config: AutoControlConfig,
                 miner: MinerService,
                 battery: BatteryService,
                 state: StateManager):
        self.config = config
        self.miner = miner
        self.battery = battery
        self.state = state
        self.enabled = state.get('autocontrol', False)
        self.last_set_w = None
        self.latched_floor_w = None

    def start(self):
        """Start auto-control thread"""

    def stop(self):
        """Stop auto-control thread"""

    def enable(self):
        """Enable auto-control"""

    def disable(self):
        """Disable auto-control"""

    def evaluate_and_adjust(self):
        """Main control loop iteration"""

    def get_state(self) -> dict:
        """Get current auto-control state for debugging"""
```

### 5. services/network_scanner.py
**Purpose**: Network device discovery

**Responsibilities**:
- Scan 192.168.86.0/24 subnet
- Identify WhatsMiner devices
- Identify Bitaxe devices
- Track discovered devices

**Key Classes**:
```python
class NetworkScanner:
    def __init__(self):
        self.discovered_devices = []
        self.last_scan_time = None

    async def scan_network(self, subnet: str = "192.168.86") -> list[Device]:
        """NEW: Scan network for devices"""
        # Use ping + port scanning
        # Check common miner ports: 4028, 4029, 80

    async def identify_whatsminer(self, ip: str) -> Optional[Device]:
        """NEW: Try to identify WhatsMiner at IP"""
        # Try RPC summary command

    async def identify_bitaxe(self, ip: str) -> Optional[Device]:
        """NEW: Try to identify Bitaxe at IP"""
        # Check for Bitaxe-specific HTTP endpoints

    def get_devices(self) -> list[Device]:
        """Get list of discovered devices"""

    def start_background_scan(self):
        """NEW: Start periodic network scanning"""
        # Scan every 5 minutes
```

### 6. services/data_loader.py
**Purpose**: Efficient CSV data loading with lazy loading

**Responsibilities**:
- Load initial 3 days of data
- **FIX: Actually limit to 3 days, not load all**
- Lazy load additional data on demand
- Manage memory limits

**Key Classes**:
```python
class DataLoader:
    def __init__(self, config: DataConfig):
        self.config = config
        self.miner_loaded_days = 0
        self.battery_loaded_days = 0

    def load_miner_data(self, days: int = 3) -> list[dict]:
        """Load miner CSV data for last N days"""
        # FIX: Actually filter to N days
        # Calculate cutoff timestamp
        # Only load rows after cutoff

    def extend_miner_data(self, days: int) -> list[dict]:
        """Extend loaded miner data"""
        # Only load if days > currently loaded

    def load_battery_data(self, days: int = 3) -> list[dict]:
        """Load battery CSV data for last N days"""

    def extend_battery_data(self, days: int) -> list[dict]:
        """Extend loaded battery data"""

    def get_loaded_stats(self) -> dict:
        """Get statistics about loaded data"""
```

### 7. api/ modules
**Purpose**: Clean separation of API routes

**Structure**:
- `api/routes.py` - Registers all blueprints
- `api/miner_routes.py` - Miner control endpoints
- `api/battery_routes.py` - Battery endpoints
- `api/data_routes.py` - History/data endpoints
- `api/system_routes.py` - Status, health, debug

Each route module is a Flask Blueprint with dependency injection of services.

### 8. Unified Dashboard (static/index.html)

**Layout**:
```
┌─────────────────────────────────────────────────────────────┐
│ Status Bar: [Miner: ● Connected] [Battery: ● Connected]    │
│             [Network: 2 devices found]                      │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────┬─────────────────────────────────────────┐ │
│ │   MINER     │           BATTERY                       │ │
│ │  Controls   │           Summary                       │ │
│ │             │                                         │ │
│ │ Power: 79%  │  PV: 2400W    SOC: 87%                 │ │
│ │ Hashrate:   │  Load: 1800W  Charge: +600W            │ │
│ │ 315 TH/s    │                                         │ │
│ └─────────────┴─────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                      POWER CHART                            │
│  [Miner Power, Battery SOC, PV Power, Load - Combined]     │
├─────────────────────────────────────────────────────────────┤
│                  TEMPERATURE CHART                          │
│  [Miner Temp, Env Temp]                                     │
├─────────────────────────────────────────────────────────────┤
│ ┌────────────────────────────────────────────────────────┐ │
│ │  NETWORK DEVICES                                       │ │
│ │  ● WhatsMiner (192.168.86.52) - 315 TH/s              │ │
│ │  ● Bitaxe (192.168.86.147) - 0.7 TH/s                 │ │
│ └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

**Features**:
- Single page with tabs/sections for different views
- Connection status indicators with green/red dots
- Last connected timestamps
- Network device panel
- Combined charts showing miner + battery data
- Responsive layout for mobile/tablet

## Implementation Roadmap

### Phase 1: Foundation (Week 1)
1. Create new directory structure
2. Implement config/settings.py with YAML support
3. Move existing code into service modules (no new features)
4. Implement utils/ (csv_logger, state_manager, time_utils)
5. Create models/ (data classes for miner, battery)

### Phase 2: Services Refactor (Week 2)
6. Extract MinerService from WM_Server.py
7. Extract BatteryService from WM_Server.py
8. Extract AutoControlService from WM_Server.py
9. Implement DataLoader service
10. **FIX: 3-day data loading** - actually limit to 3 days

### Phase 3: API Refactor (Week 2)
11. Create Flask blueprints for API routes
12. Move all routes from WM_Server.py to api/ modules
13. Implement dependency injection pattern
14. Create new app.py as slim entry point

### Phase 4: New Features (Week 3)
15. **Implement NetworkScanner service**
    - Network discovery
    - Device identification (WhatsMiner, Bitaxe)
    - Background scanning thread

16. **Implement connection status tracking**
    - Track last successful connection for each device
    - Calculate uptime
    - Expose via API

17. **Implement battery session management**
    - Automatic cookie refresh
    - Weekly refresh schedule
    - Retry logic on auth failure

### Phase 5: Unified Dashboard (Week 3-4)
18. Design unified dashboard layout
19. Implement combined charts (Plotly.js)
20. Add connection status panel
21. Add network devices panel
22. Implement responsive design
23. Add date range selector with lazy loading

### Phase 6: Testing & Documentation (Week 4)
24. Write unit tests for services
25. Write integration tests for API
26. Create test fixtures
27. Write comprehensive README
28. Document API endpoints
29. Create setup/deployment guide

### Phase 7: Polish & Deploy (Week 5)
30. Add error handling improvements
31. Performance optimization
32. Memory usage optimization (deque limits)
33. Add logging configuration
34. Production deployment on Raspberry Pi
35. Verify Tailscale remote access

## Technical Decisions

### Configuration Management
- **Primary**: YAML file (`config.yaml`)
- **Override**: Environment variables (for secrets)
- **Template**: `.env.example` for easy setup
- **Validation**: Pydantic models for type safety

### Data Loading Strategy
- **Default**: 3 days loaded on startup
- **Lazy**: Load more only when user requests
- **Max**: Cap at 30 days to prevent memory issues
- **Filter**: Use timestamp filtering, not row counting

### Session Management
- **Auto-refresh**: Check session health every poll
- **Scheduled**: Force refresh every week
- **Retry**: On auth failure, refresh and retry once
- **Logging**: Log all auth attempts for debugging

### Network Discovery
- **Scan**: Every 5 minutes
- **Method**: Async ping + port check
- **Ports**: 4028 (miner RPC), 80 (HTTP)
- **Timeout**: 2 seconds per host
- **Cache**: Store discovered devices

### Testing Strategy
- **Unit tests**: All service methods
- **Integration tests**: API endpoints
- **Mocks**: External API calls (miner, battery)
- **Fixtures**: CSV data samples
- **Coverage target**: 80%+

## Migration Strategy

### Backward Compatibility
- Keep old routes working during transition
- Deprecate, don't remove immediately
- Add deprecation warnings in logs

### Rollback Plan
- Keep WM_Server.py as backup
- Tag releases in git
- Easy rollback to previous version

### Deployment Steps
1. Test on development machine
2. Deploy to Raspberry Pi test environment
3. Run parallel for 24 hours (old + new)
4. Monitor for issues
5. Cut over to new version
6. Keep old version as backup for 1 week

## Risk Mitigation

### High Risk Items
1. **Data loading changes** - Could break existing data access
   - Mitigation: Extensive testing with real CSV files

2. **Session management** - Could cause auth failures
   - Mitigation: Fallback to manual re-auth

3. **Dashboard rewrite** - Could break user workflows
   - Mitigation: Keep old pages as `/legacy/`

### Medium Risk Items
1. **Network scanning** - Could impact performance
   - Mitigation: Run in low-priority thread, limit scan frequency

2. **Module refactoring** - Could introduce bugs
   - Mitigation: Comprehensive testing, gradual rollout

## Success Metrics

1. **Code Quality**
   - Lines per file: < 300
   - Test coverage: > 80%
   - No cyclomatic complexity > 15

2. **Performance**
   - Startup time: < 5 seconds
   - Memory usage: < 200MB
   - API response time: < 100ms

3. **User Experience**
   - Single dashboard (no page switching)
   - Connection status visible at all times
   - Auto-discovery works on first launch

4. **Reliability**
   - Battery session stays active for 1 week+
   - 3-day data loading works correctly
   - No memory leaks over 30 days

## Open Questions

1. **Q**: Should we support multiple WhatsMiner devices?
   **A**: TBD - Add if user requests

2. **Q**: Should Bitaxe have its own control interface?
   **A**: TBD - Research Bitaxe API capabilities

3. **Q**: Should we add email/SMS alerts for low battery?
   **A**: TBD - Nice to have, but not MVP

4. **Q**: Should we add historical SOC-based power optimization?
   **A**: TBD - Future ML feature

## Next Steps

1. Review this plan with user
2. Get approval on architecture
3. Set up development branch
4. Begin Phase 1 implementation
5. Create detailed task list in todo system

---

**Document Version**: 1.0
**Created**: 2025-11-08
**Author**: Claude Code
**Status**: Draft - Awaiting Approval
