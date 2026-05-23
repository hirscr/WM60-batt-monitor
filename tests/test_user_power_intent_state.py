"""Tests for persistent user_power_intent state.

The user_power_intent flag represents the user-commanded master switch state
for the dashboard's Power toggle. These tests exercise the persistence layer
in isolation — no service instantiation, no network, no asyncio. Pure
StateManager round-trips through a tempfile.

Coverage:
  - Default user_power_intent is True when no prior state file exists.
  - save(user_power_intent=False) followed by load() returns False.
  - Saving an unrelated key after setting intent=False does not overwrite
    the intent flag (the merge in StateManager.load preserves it).
  - Loading a state file that omits user_power_intent returns the default
    True (forward compatibility for pre-feature deploys).
"""
from __future__ import annotations

import json
import os
import tempfile

import pytest

from utils.state_manager import StateManager, DEFAULT_STATE


@pytest.fixture
def state_path():
    """Provide a fresh temp path for each test; remove it on teardown."""
    fd, path = tempfile.mkstemp(prefix="test_intent_", suffix=".json")
    os.close(fd)
    os.remove(path)  # StateManager creates the file on first init
    yield path
    if os.path.exists(path):
        os.remove(path)


def test_default_user_power_intent_is_true(state_path):
    """A fresh StateManager initializes user_power_intent to True.

    Existing deploys lift this default on first start after the feature
    ships, preserving the previous always-available behavior until the
    user explicitly clicks Power-OFF.
    """
    mgr = StateManager(path=state_path)
    state = mgr.load()
    assert state["user_power_intent"] is True


def test_persistence_round_trip_false(state_path):
    """save(user_power_intent=False) followed by load() returns False."""
    mgr = StateManager(path=state_path)
    mgr.save(user_power_intent=False)
    state = mgr.load()
    assert state["user_power_intent"] is False


def test_unrelated_save_preserves_intent(state_path):
    """Saving an unrelated key after intent=False must not clobber intent.

    StateManager.save() does a read-modify-write under lock: it loads the
    current state, updates the supplied kwargs, and writes back. A bug
    here (e.g. resetting to DEFAULT_STATE before update) would silently
    re-enable the master switch — which would defeat the entire purpose
    of the persisted intent flag.
    """
    mgr = StateManager(path=state_path)
    mgr.save(user_power_intent=False)
    # Now write something else — emergency_soc is an unrelated key.
    mgr.save(emergency_soc=42)
    state = mgr.load()
    assert state["user_power_intent"] is False
    assert state["emergency_soc"] == 42


def test_forward_compat_missing_key_defaults_true(state_path):
    """Loading a pre-feature state file (no user_power_intent key) returns True.

    Simulates an existing deploy whose wm_state.json predates this feature.
    The merge in StateManager.load() must supply the DEFAULT_STATE value
    (True) for the missing key so the dashboard does not render the master
    switch as OFF after upgrading.
    """
    # Write a state file that omits user_power_intent entirely.
    legacy = {
        "autocontrol": True,
        "miner_power_state": "running",
        "target_power_pct": 40,
        # No user_power_intent — pre-feature deploy.
    }
    with open(state_path, "w") as f:
        json.dump(legacy, f)

    mgr = StateManager(path=state_path)
    state = mgr.load()
    assert state["user_power_intent"] is True
    # Other persisted values should be preserved.
    assert state["autocontrol"] is True
    assert state["target_power_pct"] == 40
