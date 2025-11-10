"""Network scanner for discovering mining devices."""
import socket
import threading
import time
import subprocess
import re
from typing import List, Optional
import platform

from models.device import Device, DeviceType


class NetworkScanner:
    """
    Network scanner for discovering devices on local network.

    Two-phase approach:
    1. Find ALL alive hosts (using ARP or ping)
    2. Identify device types (WhatsMiner, Bitaxe, etc.)
    """

    def __init__(self, subnet: str = "192.168.86"):
        """
        Initialize network scanner.

        Args:
            subnet: First 3 octets of subnet to scan (e.g., "192.168.86")
        """
        self.subnet = subnet
        self.discovered_devices: List[Device] = []
        self.last_scan_time: float = 0.0
        self.scanning: bool = False  # Track if scan is in progress

        self._running = False
        self._thread = None

    def start_background_scan(self):
        """Start background scanning thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._scan_loop, daemon=True)
        self._thread.start()
        print("[NetworkScanner] Started background scanning")

    def stop(self):
        """Stop background scanning."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        print("[NetworkScanner] Stopped")

    def _scan_loop(self):
        """Background scan loop."""
        import sys
        SCAN_INTERVAL = 300  # 5 minutes

        # Scan immediately on startup
        print("[NetworkScanner] Starting initial scan...", flush=True)
        sys.stdout.flush()
        try:
            self.scanning = True
            devices = self.scan_network()
            self.discovered_devices = devices
            self.last_scan_time = time.time()
            self.scanning = False
            print(f"[NetworkScanner] ✓ Initial scan complete: Found {len(devices)} devices")
            for d in devices:
                print(f"  - {d.device_type.value} at {d.ip}: {d.hashrate_ths}TH/s, {d.power_w}W")
        except Exception as e:
            self.scanning = False
            print(f"[NetworkScanner] ✗ Initial scan error: {e}")
            import traceback
            traceback.print_exc()

        while self._running:
            time.sleep(SCAN_INTERVAL)
            try:
                print("[NetworkScanner] Starting periodic scan...")
                self.scanning = True
                devices = self.scan_network()
                self.discovered_devices = devices
                self.last_scan_time = time.time()
                self.scanning = False
                print(f"[NetworkScanner] ✓ Scan complete: Found {len(devices)} devices")
            except Exception as e:
                self.scanning = False
                print(f"[NetworkScanner] ✗ Scan error: {e}")
                import traceback
                traceback.print_exc()

    def scan_network(self) -> List[Device]:
        """
        Scan network for devices using 2-phase approach.

        Returns:
            List of discovered devices
        """
        print(f"[NetworkScanner] Phase 1: Finding alive hosts on {self.subnet}.x...")

        # Phase 1: Find ALL alive hosts
        alive_ips = self._find_alive_hosts()
        print(f"[NetworkScanner] ✓ Found {len(alive_ips)} alive hosts: {', '.join(alive_ips)}")

        # Phase 2: Identify device types
        devices = []
        for ip in alive_ips:
            hostname = self._hostname_map.get(ip)
            print(f"[NetworkScanner] Phase 2: Identifying {ip}...")
            device = self._identify_device(ip)
            if device:
                # Add hostname to identified device
                device.hostname = hostname
                devices.append(device)
                print(f"[NetworkScanner]   ✓ {ip} = {device.device_type.value}")
            else:
                # Still add unknown devices
                devices.append(Device(
                    ip=ip,
                    hostname=hostname,
                    device_type=DeviceType.UNKNOWN,
                    hashrate_ths=0.0,
                    power_w=0
                ))
                print(f"[NetworkScanner]   - {ip} = unknown device")

        return devices

    def _find_alive_hosts(self) -> List[str]:
        """
        Find all alive hosts on the subnet.

        Uses multiple methods:
        1. ARP table (fastest on macOS)
        2. Ping sweep
        3. TCP connect to common ports

        Returns:
            List of IP addresses
        """
        alive_hosts = set()
        self._hostname_map = {}  # Store IP -> hostname mapping

        # Method 1: Use ARP table (Mac/Linux)
        try:
            if platform.system() == "Darwin":
                # macOS: Use arp -a (with hostnames)
                # Takes ~6 seconds but scans are infrequent (startup + every 5 min)
                result = subprocess.run(
                    ['arp', '-a'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )

                if result.returncode == 0:
                    # Parse ARP output: "whatsminer.lan (192.168.86.52) at aa:bb:cc:dd:ee:ff on en0"
                    # or: "? (192.168.86.52) at aa:bb:cc:dd:ee:ff on en0"
                    # Skip "(incomplete)" entries - those have no MAC address
                    for line in result.stdout.split('\n'):
                        if "(incomplete)" in line:
                            continue  # Skip incomplete ARP entries

                        # Extract hostname (if present) and IP
                        hostname = None
                        hostname_match = re.search(r'^(\S+)\s+\(', line)
                        if hostname_match:
                            hostname = hostname_match.group(1)
                            if hostname == '?':
                                hostname = None

                        ip_match = re.search(r'\((\d+\.\d+\.\d+\.\d+)\)', line)
                        if ip_match:
                            ip = ip_match.group(1)
                            if ip.startswith(self.subnet):
                                # Skip network (.0) and broadcast (.255) addresses
                                last_octet = int(ip.split('.')[-1])
                                if last_octet == 0 or last_octet == 255:
                                    continue
                                alive_hosts.add(ip)
                                if hostname:
                                    self._hostname_map[ip] = hostname
                                    print(f"[NetworkScanner]   ARP: {ip} ({hostname}) alive")
                                else:
                                    print(f"[NetworkScanner]   ARP: {ip} alive")
            else:
                # Linux: Use ip neigh
                result = subprocess.run(
                    ['ip', 'neigh'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )

                if result.returncode == 0:
                    for line in result.stdout.split('\n'):
                        if "INCOMPLETE" in line or "FAILED" in line:
                            continue  # Skip incomplete entries
                        match = re.search(r'^(\d+\.\d+\.\d+\.\d+)', line)
                        if match:
                            ip = match.group(1)
                            if ip.startswith(self.subnet):
                                alive_hosts.add(ip)
                                print(f"[NetworkScanner]   ARP: {ip} alive")

        except Exception as e:
            print(f"[NetworkScanner]   ARP lookup failed: {e}")

        # Method 2: If ARP didn't find many hosts, try ping sweep on common IPs
        if len(alive_hosts) < 5:
            print("[NetworkScanner]   ARP found few hosts, trying ping sweep...")
            common_ips = list(range(1, 256))  # Scan all .1-.255

            for last_octet in common_ips:
                ip = f"{self.subnet}.{last_octet}"
                if self._ping_host(ip):
                    alive_hosts.add(ip)
                    print(f"[NetworkScanner]   PING: {ip} alive")

        return sorted(list(alive_hosts))

    def _ping_host(self, ip: str, timeout: float = 0.5) -> bool:
        """Check if host responds to ping."""
        try:
            # Use system ping command (works on Mac/Linux)
            if platform.system() == "Darwin":
                # macOS: ping -c 1 -W timeout_ms
                result = subprocess.run(
                    ['ping', '-c', '1', '-W', str(int(timeout * 1000)), ip],
                    capture_output=True,
                    timeout=timeout + 1
                )
            else:
                # Linux: ping -c 1 -W timeout_sec
                result = subprocess.run(
                    ['ping', '-c', '1', '-W', str(int(timeout)), ip],
                    capture_output=True,
                    timeout=timeout + 1
                )

            return result.returncode == 0
        except Exception:
            return False

    def _identify_device(self, ip: str) -> Optional[Device]:
        """
        Try to identify device type.

        Args:
            ip: IP address to identify

        Returns:
            Device if identified, None otherwise
        """
        # Try WhatsMiner (port 4028)
        if self._check_port(ip, 4028, timeout=1.0):
            device = self._identify_whatsminer(ip)
            if device:
                return device

        # Try Bitaxe (port 80 with API)
        if self._check_port(ip, 80, timeout=1.0):
            device = self._identify_bitaxe(ip)
            if device:
                return device

        return None

    def _check_port(self, ip: str, port: int, timeout: float = 1.0) -> bool:
        """Check if port is open."""
        # Use nc command on macOS (Python sockets don't work due to security restrictions)
        if platform.system() == "Darwin":
            try:
                # Use nc with -z flag (scan mode) and -w timeout
                result = subprocess.run(
                    ['nc', '-z', '-w', '1', ip, str(port)],
                    capture_output=True,
                    timeout=timeout + 1
                )
                return result.returncode == 0
            except Exception:
                return False
        else:
            # Use Python sockets on Linux
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                result = sock.connect_ex((ip, port))
                sock.close()
                return result == 0
            except Exception:
                return False

    def _identify_whatsminer(self, ip: str) -> Optional[Device]:
        """Try to identify WhatsMiner device."""
        try:
            # Use NC-based API on macOS
            if platform.system() == "Darwin":
                from utils.nc_miner_api import NCMinerAPI
                api = NCMinerAPI(ip)
                summary = api.summary()
            else:
                from pyasic.rpc.btminer import BTMinerRPCAPI
                import asyncio
                api = BTMinerRPCAPI(ip)
                summary = asyncio.run(api.summary())

            if summary and "SUMMARY" in summary:
                item = summary["SUMMARY"][0] if summary["SUMMARY"] else {}

                # Extract hashrate (try MHS, GHS, THS)
                hashrate = 0.0
                if "MHS 5s" in item:
                    hashrate = item.get("MHS 5s", 0) / 1000000.0  # MH/s to TH/s
                elif "GHS 5s" in item:
                    hashrate = item.get("GHS 5s", 0) / 1000.0  # GH/s to TH/s
                elif "THS 5s" in item:
                    hashrate = item.get("THS 5s", 0)  # Already TH/s

                power = item.get("Power", 0)

                return Device(
                    ip=ip,
                    device_type=DeviceType.WHATSMINER,
                    hashrate_ths=hashrate,
                    power_w=power
                )
        except Exception as e:
            print(f"[NetworkScanner]     Error identifying WhatsMiner at {ip}: {e}")

        return None

    def _identify_bitaxe(self, ip: str) -> Optional[Device]:
        """Try to identify Bitaxe device."""
        try:
            import requests
            # Bitaxe typically has a /api/system/info endpoint
            response = requests.get(f"http://{ip}/api/system/info", timeout=2)
            if response.status_code == 200:
                data = response.json()
                if "ASICModel" in data or "bitaxe" in str(data).lower():
                    # This is likely a Bitaxe
                    hashrate = data.get("hashRate", 0) / 1000000000000.0  # Convert to TH/s
                    power = data.get("power", 0)

                    return Device(
                        ip=ip,
                        device_type=DeviceType.BITAXE,
                        hashrate_ths=hashrate,
                        power_w=power
                    )
        except Exception:
            pass

        return None

    def get_devices(self) -> List[Device]:
        """Get list of discovered devices."""
        return self.discovered_devices.copy()

    def get_scan_info(self) -> dict:
        """Get scan information."""
        return {
            "scanning": self.scanning,
            "last_scan_time": self.last_scan_time,
            "device_count": len(self.discovered_devices),
            "devices": [
                {
                    "ip": d.ip,
                    "hostname": d.hostname,
                    "type": d.device_type.value,
                    "hashrate_ths": d.hashrate_ths,
                    "power_w": d.power_w,
                }
                for d in self.discovered_devices
            ]
        }
