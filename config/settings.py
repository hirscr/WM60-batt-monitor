"""Configuration management with YAML + environment variable support."""
import os
import yaml
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path


@dataclass
class MinerConfig:
    """WhatsMiner configuration."""
    host: str = "192.168.86.52"
    user: str = "admin"
    password: str = "admin"
    base_watts: int = 3600
    poll_seconds: int = 10


@dataclass
class BatteryConfig:
    """EG4 Battery configuration."""
    user: str = ""
    password: str = ""
    base_url: str = "https://monitor.eg4electronics.com"
    poll_seconds: int = 10
    session_refresh_hours: int = 168  # 1 week


@dataclass
class AwayModeConfig:
    """Away mode specific configuration."""
    emergency_soc: int = 30
    max_pv_power: int = 3600
    after_sunset_min_soc: int = 40


@dataclass
class LocationConfig:
    """Location configuration for sunset calculation."""
    latitude: float = 40.0
    longitude: float = -74.0
    timezone: str = "America/New_York"


@dataclass
class AutoControlConfig:
    """Auto-control configuration."""
    enabled: bool = False
    min_interval_sec: int = 60
    mode: str = "away"
    away_mode: AwayModeConfig = field(default_factory=AwayModeConfig)
    location: LocationConfig = field(default_factory=LocationConfig)
    # Legacy fallback values
    sunset_hour: int = 19
    sunset_minute: int = 0


@dataclass
class BraiinsConfig:
    """Braiins Pool integration configuration."""
    enabled: bool = False
    poll_seconds: int = 60
    freshness_window_sec: int = 300


@dataclass
class WeatherGateConfig:
    """Weather-gated autocontrol configuration.

    The pre-sunrise gate evaluates once per local day inside a configurable
    window and decides whether the day's expected solar harvest can refill
    the battery; if not, autocontrol is disabled for the day so the battery
    can charge fully from solar.

    Operational decision parameters (the first eight fields) are editable
    from the dashboard at runtime via POST /api/weather/config. Forecast
    refresh/freshness fields are config-only.
    """
    enabled: bool = True
    battery_total_kwh: float = 75.0
    summer_max_kwh: float = 75.0
    winter_max_kwh: float = 30.0
    pre_sunrise_window_minutes: int = 30
    recovery_soc_threshold_pct: int = 90
    recovery_min_hours_before_sunset: float = 3.0
    eg4_predict_multiplier: float = 0.8
    forecast_refresh_seconds: int = 3600
    forecast_freshness_seconds: int = 7200


@dataclass
class DataConfig:
    """Data loading configuration."""
    default_days: int = 3
    max_days: int = 30
    log_interval_sec: int = 3600


@dataclass
class AppConfig:
    """Application configuration."""
    port: int = 8080
    host: str = "0.0.0.0"
    debug: bool = False


@dataclass
class Settings:
    """Complete application settings."""
    miner: MinerConfig = field(default_factory=MinerConfig)
    battery: BatteryConfig = field(default_factory=BatteryConfig)
    autocontrol: AutoControlConfig = field(default_factory=AutoControlConfig)
    data: DataConfig = field(default_factory=DataConfig)
    app: AppConfig = field(default_factory=AppConfig)
    braiins: BraiinsConfig = field(default_factory=BraiinsConfig)
    weather_gate: WeatherGateConfig = field(default_factory=WeatherGateConfig)

    def validate(self):
        """Validate settings and raise if invalid."""
        if not self.miner.host:
            raise ValueError("Miner host is required")
        if not self.battery.user or not self.battery.password:
            raise ValueError("Battery credentials are required")
        if self.data.default_days > self.data.max_days:
            raise ValueError("default_days cannot exceed max_days")


def load_settings(config_path: Optional[str] = None) -> Settings:
    """
    Load settings from YAML file and environment variables.
    Environment variables override YAML values.

    Priority order:
    1. config.local.yaml (if exists) - for local secrets
    2. config.yaml (fallback) - safe template
    """
    settings = Settings()

    # Load from YAML if exists
    if config_path is None:
        # Prefer config.local.yaml if it exists (contains actual secrets)
        if Path("config.local.yaml").exists():
            config_path = "config.local.yaml"
            print("[Config] Using config.local.yaml (local secrets)")
        else:
            config_path = "config.yaml"
            print("[Config] Using config.yaml (template)")

    config_file = Path(config_path)
    if config_file.exists():
        print(f"[Config] Loading from {config_file}")
        with open(config_file, 'r') as f:
            data = yaml.safe_load(f) or {}
        print(f"[Config] YAML data section: {data.get('data', {})}")

        # Miner config
        if 'miner' in data:
            m = data['miner']
            settings.miner = MinerConfig(
                host=m.get('host', settings.miner.host),
                user=m.get('user', settings.miner.user),
                password=m.get('password', settings.miner.password),
                base_watts=m.get('base_watts', settings.miner.base_watts),
                poll_seconds=m.get('poll_seconds', settings.miner.poll_seconds),
            )

        # Battery config
        if 'battery' in data:
            b = data['battery']
            settings.battery = BatteryConfig(
                user=b.get('user', settings.battery.user),
                password=b.get('password', settings.battery.password),
                base_url=b.get('base_url', settings.battery.base_url),
                poll_seconds=b.get('poll_seconds', settings.battery.poll_seconds),
                session_refresh_hours=b.get('session_refresh_hours', settings.battery.session_refresh_hours),
            )

        # Auto-control config
        if 'autocontrol' in data:
            a = data['autocontrol']

            # Parse away_mode section
            away_mode_data = a.get('away_mode', {})
            away_mode = AwayModeConfig(
                emergency_soc=away_mode_data.get('emergency_soc', 30),
                max_pv_power=away_mode_data.get('max_pv_power', 3600),
                after_sunset_min_soc=away_mode_data.get('after_sunset_min_soc', 40)
            )

            # Parse location section
            location_data = a.get('location', {})
            location = LocationConfig(
                latitude=location_data.get('latitude', 40.0),
                longitude=location_data.get('longitude', -74.0),
                timezone=location_data.get('timezone', 'America/New_York')
            )

            settings.autocontrol = AutoControlConfig(
                enabled=a.get('enabled', settings.autocontrol.enabled),
                min_interval_sec=a.get('min_interval_sec', settings.autocontrol.min_interval_sec),
                mode=a.get('mode', 'away'),
                away_mode=away_mode,
                location=location,
                sunset_hour=a.get('sunset_hour', settings.autocontrol.sunset_hour),
                sunset_minute=a.get('sunset_minute', settings.autocontrol.sunset_minute),
            )

        # Data config
        if 'data' in data:
            d = data['data']
            settings.data = DataConfig(
                default_days=d.get('default_days', settings.data.default_days),
                max_days=d.get('max_days', settings.data.max_days),
                log_interval_sec=d.get('log_interval_sec', settings.data.log_interval_sec),
            )
            print(f"[Config] Set default_days={settings.data.default_days}, max_days={settings.data.max_days}")

        # App config
        if 'app' in data:
            app = data['app']
            settings.app = AppConfig(
                port=app.get('port', settings.app.port),
                host=app.get('host', settings.app.host),
                debug=app.get('debug', settings.app.debug),
            )

        # Braiins Pool config
        if 'braiins' in data:
            br = data['braiins']
            settings.braiins = BraiinsConfig(
                enabled=br.get('enabled', settings.braiins.enabled),
                poll_seconds=br.get('poll_seconds', settings.braiins.poll_seconds),
                freshness_window_sec=br.get('freshness_window_sec', settings.braiins.freshness_window_sec),
            )

        # Weather gate config
        if 'weather_gate' in data:
            wg = data['weather_gate'] or {}
            d = settings.weather_gate
            eg4_mult = float(wg.get('eg4_predict_multiplier', d.eg4_predict_multiplier))
            if eg4_mult <= 0:
                raise ValueError(
                    f"weather_gate.eg4_predict_multiplier must be > 0, got {eg4_mult}"
                )
            settings.weather_gate = WeatherGateConfig(
                enabled=wg.get('enabled', d.enabled),
                battery_total_kwh=float(wg.get('battery_total_kwh', d.battery_total_kwh)),
                summer_max_kwh=float(wg.get('summer_max_kwh', d.summer_max_kwh)),
                winter_max_kwh=float(wg.get('winter_max_kwh', d.winter_max_kwh)),
                pre_sunrise_window_minutes=int(wg.get('pre_sunrise_window_minutes', d.pre_sunrise_window_minutes)),
                recovery_soc_threshold_pct=int(wg.get('recovery_soc_threshold_pct', d.recovery_soc_threshold_pct)),
                recovery_min_hours_before_sunset=float(wg.get('recovery_min_hours_before_sunset', d.recovery_min_hours_before_sunset)),
                eg4_predict_multiplier=eg4_mult,
                forecast_refresh_seconds=int(wg.get('forecast_refresh_seconds', d.forecast_refresh_seconds)),
                forecast_freshness_seconds=int(wg.get('forecast_freshness_seconds', d.forecast_freshness_seconds)),
            )

    # Override with environment variables
    if os.getenv('WM_HOST'):
        settings.miner.host = os.getenv('WM_HOST')
    if os.getenv('WM_USER'):
        settings.miner.user = os.getenv('WM_USER')
    if os.getenv('WM_PASS'):
        settings.miner.password = os.getenv('WM_PASS')
    if os.getenv('WM_BASE_WATTS'):
        settings.miner.base_watts = int(os.getenv('WM_BASE_WATTS'))

    if os.getenv('EG4_USER'):
        settings.battery.user = os.getenv('EG4_USER')
    if os.getenv('EG4_PASS'):
        settings.battery.password = os.getenv('EG4_PASS')
    if os.getenv('EG4_BASE_URL'):
        settings.battery.base_url = os.getenv('EG4_BASE_URL')

    if os.getenv('POLL_SECONDS'):
        settings.miner.poll_seconds = int(os.getenv('POLL_SECONDS'))
        settings.battery.poll_seconds = int(os.getenv('POLL_SECONDS'))

    if os.getenv('PORT'):
        settings.app.port = int(os.getenv('PORT'))

    # Validate settings
    settings.validate()

    return settings
