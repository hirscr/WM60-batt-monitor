"""Tests for the emergency latch restart bug fix.

When the service restarts with emergency_active=True persisted in state,
emergency_verified_off resets to False. If the miner is already off,
_check_verified_off() returns True and takes the else branch — which
previously never set emergency_verified_off=True, so the clear condition
`self.emergency_verified_off and soc >= 90` could never be satisfied.

Coverage:
  - Restart with emergency_active=True, miner already off, SOC=100 → latch clears
  - Restart with emergency_active=True, miner already off, SOC=80 → latch stays
  - Restart with emergency_active=True, miner still running → re-stop loop runs,
    latch does not clear prematurely
"""
from __future__ import annotations

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from services.autocontrol_service import AutoControlService
from utils.state_manager import StateManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_service(state_path: str, emergency_active: bool = True) -> AutoControlService:
    """Build a minimal AutoControlService with all external deps mocked."""
    state_mgr = StateManager(path=state_path)
    state_mgr.save(
        autocontrol=True,
        emergency_active=emergency_active,
        emergency_soc=40,
    )

    miner = MagicMock()
    miner.is_off = True
    miner.api = MagicMock()

    battery = MagicMock()
    battery.is_fresh.return_value = True
    battery.get_status.return_value = {"soc_percent": 100.0, "pv_power_w": 500}
    battery.get_battery_age_seconds.return_value = 30

    svc = AutoControlService(
        miner_service=miner,
        battery_service=battery,
        state_manager=state_mgr,
        base_watts=3600,
        min_interval_sec=60,
        mode="away",
        away_config={"emergency_soc": 40, "max_pv_power": 3600, "after_sunset_min_soc": 40},
        location_config={"latitude": 40.0, "longitude": -74.0, "timezone": "America/New_York"},
    )
    return svc


@pytest.fixture
def state_path():
    fd, path = tempfile.mkstemp(prefix="test_emerg_", suffix=".json")
    os.close(fd)
    os.remove(path)
    yield path
    if os.path.exists(path):
        os.remove(path)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_restart_latch_clears_when_miner_off_and_soc_at_100(state_path):
    """After restart with persisted emergency latch, confirmed-off miner + SOC=100 must clear it."""
    svc = _make_service(state_path, emergency_active=True)

    assert svc.emergency_active is True
    assert svc.emergency_verified_off is False

    with patch.object(svc, "_check_verified_off", return_value=True):
        svc._run_emergency_latch_tick(soc=100.0)

    assert svc.emergency_verified_off is True
    assert svc.emergency_active is False

    # Confirm the cleared state was persisted.
    persisted = svc.state.load()
    assert persisted.get("emergency_active") is False


def test_restart_latch_stays_when_soc_below_threshold(state_path):
    """Miner already off but SOC=80 — latch must not clear (SOC threshold is 90)."""
    svc = _make_service(state_path, emergency_active=True)

    with patch.object(svc, "_check_verified_off", return_value=True):
        svc._run_emergency_latch_tick(soc=80.0)

    assert svc.emergency_verified_off is True   # flag is set this tick
    assert svc.emergency_active is True          # but latch stays (SOC < 90)


def test_restart_latch_stays_when_miner_still_running(state_path):
    """Miner NOT confirmed off → re-stop loop runs; latch does not clear prematurely."""
    svc = _make_service(state_path, emergency_active=True)

    # _emergency_stop_with_verify sets emergency_verified_off=True internally.
    # Simulate that via side_effect so downstream assertion is meaningful.
    def _fake_stop_verify():
        svc.emergency_verified_off = True
        return True

    with patch.object(svc, "_check_verified_off", return_value=False), \
         patch.object(svc, "_emergency_stop_with_verify", side_effect=_fake_stop_verify) as mock_stop:
        svc._run_emergency_latch_tick(soc=80.0)

    mock_stop.assert_called_once()
    assert svc.emergency_verified_off is True
    assert svc.emergency_active is True   # SOC=80 < 90, so latch stays

    # Second tick with SOC=95 and miner confirmed off → latch clears.
    with patch.object(svc, "_check_verified_off", return_value=True):
        svc._run_emergency_latch_tick(soc=95.0)

    assert svc.emergency_active is False


# ---------------------------------------------------------------------------
# Emergency-latch timestamp (set + clear)
# ---------------------------------------------------------------------------

def test_emergency_latch_set_at_records_time_and_clears_on_release(state_path):
    """Tripping the latch must record time.time() into both the live attribute
    and persisted state. Clearing the latch must reset both back to None.
    """
    # Fresh service with no prior latch.
    state_mgr = StateManager(path=state_path)
    state_mgr.save(autocontrol=True, emergency_soc=40)

    miner = MagicMock()
    miner.is_off = True
    miner.api = MagicMock()
    miner.controller = MagicMock()

    battery = MagicMock()
    battery.is_fresh.return_value = True
    battery.get_battery_age_seconds.return_value = 5

    svc = AutoControlService(
        miner_service=miner,
        battery_service=battery,
        state_manager=state_mgr,
        base_watts=3600,
        min_interval_sec=60,
        mode="away",
        away_config={"emergency_soc": 40, "max_pv_power": 3600, "after_sunset_min_soc": 40},
        location_config={"latitude": 40.0, "longitude": -74.0, "timezone": "America/New_York"},
    )

    # Sanity: no latch yet.
    assert svc.emergency_active is False
    assert svc.emergency_latch_set_at is None

    pinned_set_ts = 1_700_000_000.0

    # _emergency_stop_with_verify normally sets emergency_verified_off=True
    # when it succeeds. Replicate that side-effect via the mock so the CLEAR
    # path further down can satisfy its precondition.
    def _fake_stop_verify():
        svc.emergency_verified_off = True
        return True

    # Trip the latch. Patch time.time inside autocontrol_service so the SET
    # site records the pinned value. Patch stop-with-verify and verified-off
    # check so no real miner work happens.
    with patch("services.autocontrol_service.time.time", return_value=pinned_set_ts), \
         patch.object(svc, "_emergency_stop_with_verify", side_effect=_fake_stop_verify), \
         patch.object(svc, "_check_verified_off", return_value=True):
        svc._trip_emergency(soc=20.0)

    # Live attribute and persisted state both reflect the pinned timestamp.
    assert svc.emergency_active is True
    assert svc.emergency_latch_set_at == pinned_set_ts
    persisted_after_set = svc.state.load()
    assert persisted_after_set.get("emergency_active") is True
    assert persisted_after_set.get("emergency_latch_set_at") == pinned_set_ts

    # Now run the CLEAR path: miner confirmed off + SOC=100 → latch clears.
    # emergency_verified_off was set True by the side_effect during _trip_emergency.
    with patch.object(svc, "_check_verified_off", return_value=True):
        svc._run_emergency_latch_tick(soc=100.0)

    assert svc.emergency_active is False
    assert svc.emergency_latch_set_at is None
    persisted_after_clear = svc.state.load()
    assert persisted_after_clear.get("emergency_active") is False
    assert persisted_after_clear.get("emergency_latch_set_at") is None
