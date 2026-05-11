"""Two-layer gated logger per global standard.

Outer gate: __debug__ (stripped from optimized builds via -O).
Inner gate: per-tag boolean flag in logs/logging.yaml.

Usage:
    from utils.log_config import log

    if __debug__:
        log("AUTOCONTROL", "battery stale, stopping miner")

The outer `if __debug__:` keeps release builds clean. The `log()` function
no-ops if the tag is missing or set to false in logs/logging.yaml.
"""

from __future__ import annotations

import os
import threading
from datetime import datetime
from pathlib import Path

import yaml

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_PATH = _PROJECT_ROOT / "logs" / "logging.yaml"
_LOG_FILE = _PROJECT_ROOT / "logs" / "debug.log"

_lock = threading.Lock()
_cache: dict[str, bool] | None = None
_cache_mtime: float = 0.0


def _load_tags() -> dict[str, bool]:
    global _cache, _cache_mtime
    try:
        mtime = _CONFIG_PATH.stat().st_mtime
    except FileNotFoundError:
        return {}
    if _cache is None or mtime != _cache_mtime:
        with _CONFIG_PATH.open("r") as f:
            data = yaml.safe_load(f) or {}
        _cache = data.get("tags") or {}
        _cache_mtime = mtime
    return _cache


def is_enabled(tag: str) -> bool:
    return bool(_load_tags().get(tag, False))


def log(tag: str, message: str) -> None:
    if not is_enabled(tag):
        return
    line = f"{datetime.now().isoformat(timespec='seconds')} [{tag}] {message}\n"
    with _lock:
        _LOG_FILE.parent.mkdir(exist_ok=True)
        with _LOG_FILE.open("a") as f:
            f.write(line)
