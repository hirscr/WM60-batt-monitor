"""
WM_Cipher.py
Thin control wrapper around pyasic for WhatsMiner devices.

Configuration:
- Set the following environment variables:
  WM_HOST   : miner IP or hostname
  WM_USER   : miner username
  WM_PASS   : miner password

Security:
- This module never logs or prints the password.
"""

from __future__ import annotations

import os
import asyncio
from typing import Optional, Any, Dict

from pyasic import get_miner, settings


class WMError(RuntimeError):
    pass


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise WMError(f"Missing required environment variable: {name}")
    return val


class WMCipher:
    def __init__(
        self,
        host: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """
        If args are not provided, they are read from environment:
        WM_HOST, WM_USER, WM_PASS.
        """
        # Pull from environment unless explicitly provided.
        self.host = host or _require_env("WM_HOST")
        self.username = username or _require_env("WM_USER")
        self._password = password or _require_env("WM_PASS")  # keep private

        self._miner = None  # pyasic miner instance

    async def _ensure(self) -> None:
        if self._miner is not None:
            return
        # Provide credentials to pyasic's global settings that its backends use.
        # WhatsMiner RPC password:
        try:
            settings.update("default_whatsminer_rpc_password", self._password)
        except Exception:
            pass
        # Some operations may use SSH or HTTP auth depending on backend capabilities.
        try:
            settings.update("default_ssh_user", self.username)
            settings.update("default_ssh_password", self._password)
        except Exception:
            pass
        # Identify and create the correct miner class for this IP.
        self._miner = await get_miner(self.host)
        if self._miner is None:
            raise WMError(f"Failed to initialize miner at {self.host} for user {self.username}")

    # -------------------------
    # Read / status utilities
    # -------------------------
    async def a_get_data(self) -> Dict[str, Any]:
        await self._ensure()
        data = await self._miner.get_data()
        # pyasic returns a MinerData object with .as_dict()
        return data.as_dict() if hasattr(data, "as_dict") else dict(data)

    def get_data(self) -> Dict[str, Any]:
        return asyncio.run(self.a_get_data())

    # -------------------------
    # Core power controls
    # -------------------------
    async def _wait_is_mining(self, target: bool, timeout: int = 30, interval: float = 1.0) -> bool:
        """Poll until is_mining equals target, or timeout."""
        import asyncio as _asyncio  # local import to avoid top-level pollution
        deadline = _asyncio.get_event_loop().time() + timeout
        while _asyncio.get_event_loop().time() < deadline:
            try:
                data = await self._miner.get_data()
                state = getattr(data, "as_dict", lambda: dict(data))().get("is_mining")
                if state is target:
                    return True
            except Exception:
                # ignore transient RPC errors while state is changing
                pass
            await _asyncio.sleep(interval)
        return False

    async def a_power_off(self) -> None:
        """Stop mining, with fallbacks and verification."""
        await self._ensure()

        # Nudge cool-down behavior first; ignore if unsupported.
        try:
            if hasattr(self._miner, "set_poweroff_cool"):
                await self._miner.set_poweroff_cool(1)
        except Exception:
            pass

        # Try preferred stop call.
        tried = False
        if hasattr(self._miner, "stop_mining"):
            tried = True
            try:
                await self._miner.stop_mining()
            except Exception:
                pass

        # Fallback to alternate backend names if present.
        if hasattr(self._miner, "power_off"):
            try:
                await self._miner.power_off()
            except Exception:
                pass

        # Verify by polling.
        ok = await self._wait_is_mining(False, timeout=30, interval=1.0)
        if not ok:
            raise WMError("Failed to stop mining: miner still reports is_mining=True after 30s")

    async def a_power_on(self) -> None:
        """Start or resume mining, with fallbacks and verification."""
        await self._ensure()

        started = False
        if hasattr(self._miner, "resume_mining"):
            try:
                await self._miner.resume_mining()
                started = True
            except Exception:
                pass

        if not started and hasattr(self._miner, "start_mining"):
            try:
                await self._miner.start_mining()
                started = True
            except Exception:
                pass

        if not started and hasattr(self._miner, "power_on"):
            try:
                await self._miner.power_on()
                started = True
            except Exception:
                pass

        ok = await self._wait_is_mining(True, timeout=45, interval=1.5)
        if not ok:
            raise WMError("Failed to start mining: miner still reports is_mining=False after 45s")


    def power_off(self) -> None:
        asyncio.run(self.a_power_off())

    def power_on(self) -> None:
        asyncio.run(self.a_power_on())

    # -------------------------
    # Useful adjunct controls
    # -------------------------
    async def a_restart_btminer(self) -> None:
        await self._ensure()
        if hasattr(self._miner, "restart_backend"):
            await self._miner.restart_backend()
        elif hasattr(self._miner, "restart_btminer"):
            await self._miner.restart_btminer()
        else:
            raise WMError("pyasic backend does not expose restart btminer")

    def restart_btminer(self) -> None:
        asyncio.run(self.a_restart_btminer())

    async def a_adjust_power_limit(self, watts: int) -> None:
        """Set upper power limit in watts."""
        await self._ensure()
        if hasattr(self._miner, "set_power_limit"):
            await self._miner.set_power_limit(int(watts))
        else:
            raise WMError("pyasic backend does not expose set_power_limit")

    def adjust_power_limit(self, watts: int) -> None:
        asyncio.run(self.a_adjust_power_limit(watts))

    async def a_set_power_pct(self, percent: int) -> None:
        """Set temporary power percentage 0..100.
        If backend lacks a native 'set_power_percent/pct', emulate via 'set_power_limit'
        using a reasonable base (env override -> wattage_limit -> config.mining_mode.power -> current wattage).
        """
        await self._ensure()

        # validate
        try:
            p = int(percent)
        except Exception:
            raise WMError(f"power percent must be an integer, got {percent!r}")
        if p < 0 or p > 100:
            raise WMError(f"power percent must be between 0 and 100, got {p}")

        # 1) Try native percent methods first
        for meth in ("set_power_percent", "set_power_pct"):
            if hasattr(self._miner, meth):
                try:
                    await getattr(self._miner, meth)(p)
                    return
                except Exception:
                    # fall through to wattage emulation
                    pass

        # 2) Emulate percent via set_power_limit if available
        if hasattr(self._miner, "set_power_limit"):
            # Read current data to pick a sane base
            try:
                data = await self._miner.get_data()
                d = data.as_dict() if hasattr(data, "as_dict") else dict(data)
            except Exception:
                d = {}

            # Allow explicit override via env
            base_env = os.getenv("WM_BASE_WATTS")
            base = None
            if base_env:
                try:
                    base = int(base_env)
                except Exception:
                    base = None

            # Prefer miner’s configured limit, then configured power, then current draw
            if base is None:
                base = d.get("wattage_limit")
            if base is None:
                base = (d.get("config") or {}).get("mining_mode", {}).get("power")
            if base is None:
                base = d.get("wattage")
            if base is None:
                base = 2000  # conservative fallback

            # Compute target watts from percent of base
            target_watts = max(100, int(round((p / 100.0) * int(base))))

            try:
                await self._miner.set_power_limit(target_watts)
                return
            except Exception as e:
                raise WMError(f"failed to set power via wattage limit emulation at {target_watts} W") from e

        # 3) Nothing worked
    def set_power_pct(self, percent: int) -> None:
        asyncio.run(self.a_set_power_pct(percent))

    async def a_set_poweroff_cool(self, enabled: bool) -> None:
        """Enable or disable cool down fans when stopping mining."""
        await self._ensure()
        if hasattr(self._miner, "set_poweroff_cool"):
            await self._miner.set_poweroff_cool(int(bool(enabled)))
        else:
            raise WMError("pyasic backend does not expose set_poweroff_cool")

    def set_poweroff_cool(self, enabled: bool) -> None:
        asyncio.run(self.a_set_poweroff_cool(enabled))

    async def a_set_led(
        self,
        mode: str = "auto",
        color: Optional[str] = None,
        period_ms: Optional[int] = None,
        duration_ms: Optional[int] = None,
        start_ms: Optional[int] = None,
    ) -> None:
        """Manage locator LED. mode='auto' or 'manual' with color/period/duration/start."""
        await self._ensure()
        if not hasattr(self._miner, "set_led"):
            raise WMError("pyasic backend does not expose set_led")
        if mode == "auto":
            await self._miner.set_led({"param": "auto"})
            return
        if mode != "manual":
            raise WMError("mode must be 'auto' or 'manual'")
        if color not in {"red", "green"}:
            raise WMError("color must be 'red' or 'green'")
        if period_ms is None or duration_ms is None or start_ms is None:
            raise WMError("manual mode requires period_ms, duration_ms, start_ms")
        await self._miner.set_led(
            {
                "color": color,
                "period": int(period_ms),
                "duration": int(duration_ms),
                "start": int(start_ms),
            }
        )

    def set_led(self, *args, **kwargs) -> None:
        asyncio.run(self.a_set_led(*args, **kwargs))

    # -------------------------
    # Convenience aliases
    # -------------------------
    def off(self) -> None:
        self.power_off()

    def on(self) -> None:
        self.power_on()