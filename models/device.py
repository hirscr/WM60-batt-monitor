"""Device models for network discovery."""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class DeviceType(Enum):
    """Type of mining device."""
    WHATSMINER = "whatsminer"
    BITAXE = "bitaxe"
    UNKNOWN = "unknown"


@dataclass
class ConnectionStatus:
    """Connection status for a device."""
    connected: bool
    last_seen: Optional[datetime] = None
    uptime_seconds: Optional[float] = None
    error: Optional[str] = None


@dataclass
class Device:
    """Network device information."""
    ip: str
    hostname: Optional[str] = None
    device_type: DeviceType = DeviceType.UNKNOWN
    hashrate_ths: Optional[float] = None
    power_w: Optional[int] = None
    status: Optional[ConnectionStatus] = None
