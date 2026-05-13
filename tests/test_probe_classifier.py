"""Unit tests for the probe orchestrator's classifier + state file IO.

These tests are hermetic — no network, no AES, no miner. They exercise:
  - classify_attempt outcomes (Success / Reset / No-op / Timeout) by stubbing
    the SUMMARY poll and time.sleep
  - probe_candidates.render placeholder substitution
  - state file fcntl-locked atomic writes (presence + parseability)
  - the API blueprint's /api/probe/status response shape
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
from typing import List
from unittest import mock

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from tools import probe_candidates  # noqa: E402
from tools import probe_orchestrator as orch  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_orchestrator(tmp_path, monkeypatch):
    """Build an orchestrator that writes state to a tmp dir and has no I/O.

    All network calls (summary, AES, HTTP, sleep) are stubbed.
    """
    # Redirect state-file and stop-flag paths into tmp_path.
    monkeypatch.setattr(orch, "STATE_FILE", str(tmp_path / "probe_state.json"))
    monkeypatch.setattr(orch, "STOP_FLAG_FILE", str(tmp_path / "probe_stop.flag"))
    monkeypatch.setattr(orch, "PID_FILE", str(tmp_path / "probe.pid"))

    cfg = {
        "host": "10.0.0.1",
        "password": "test-pwd",
        "base_watts": 3600,
        "emergency_soc_floor": 30,
    }

    # Write log to tmp file.
    log_path = str(tmp_path / "probe_log.md")
    log = orch.ExperimentLog(log_path)

    o = orch.ProbeOrchestrator(cfg, log, run_id="testrun")
    return o


def _make_snap(power_w, power_limit_w=None, mhs_5s=None, upfreq=1):
    """Build a snapshot dict matching what snapshot_fields() returns."""
    return {
        "power_w": power_w,
        "power_5s_w": power_w,
        "power_limit_w": power_limit_w,
        "mhs_5s": mhs_5s if mhs_5s is not None else (12_000_000 if power_w else 0),
        "mhs_av": 12_000_000,
        "upfreq_complete": upfreq,
        "power_mode": "Normal",
    }


class FakeClock:
    """Synthetic monotonically-increasing clock for hermetic timing tests.

    time.sleep(n) advances the clock by n seconds.
    time.time()  returns the current synthetic time.
    """
    def __init__(self, start=1_000_000.0):
        self.now = start

    def time(self):
        return self.now

    def sleep(self, secs):
        # Even a zero-arg sleep nudges the clock so tight inner loops terminate.
        self.now += max(float(secs), 0.001)


def _patch_summary_sequence(monkeypatch, snapshots):
    """Make orch.summary() yield from the given list in order, repeating last."""
    state = {"i": 0}

    def fake_summary(host):
        i = min(state["i"], len(snapshots) - 1)
        state["i"] += 1
        return snapshots[i]

    monkeypatch.setattr(orch, "summary", fake_summary)
    monkeypatch.setattr(orch, "snapshot_fields", lambda s: s)


def _patch_clock(monkeypatch, clock):
    """Redirect both time.time() and time.sleep() inside orch to a fake clock."""
    monkeypatch.setattr(orch.time, "sleep", clock.sleep)
    monkeypatch.setattr(orch.time, "time", clock.time)


def _patch_safety_ok(monkeypatch):
    monkeypatch.setattr(
        orch,
        "evaluate_soc_safety",
        lambda _floor: {"safety_state": "running", "soc": 70.0,
                        "age_seconds": 30, "detail": ""},
    )


# ---------------------------------------------------------------------------
# probe_candidates.render
# ---------------------------------------------------------------------------


def test_render_substitutes_percent_string():
    rendered = probe_candidates.render(
        {"cmd": "set_power_pct", "percent": "{percent}"}, percent=50, base_watts=3600
    )
    assert rendered == {"cmd": "set_power_pct", "percent": "50"}


def test_render_substitutes_percent_int():
    rendered = probe_candidates.render(
        {"cmd": "set_power_pct", "percent": "{percent_int}"}, percent=60, base_watts=3600
    )
    assert rendered == {"cmd": "set_power_pct", "percent": 60}


def test_render_substitutes_watts():
    rendered = probe_candidates.render(
        {"cmd": "set_power_limit", "power_limit": "{watts}"}, percent=50, base_watts=3600
    )
    assert rendered == {"cmd": "set_power_limit", "power_limit": "1800"}


def test_render_does_not_mutate_template():
    template = {"cmd": "set_power_pct_v2", "percent": "{percent}"}
    probe_candidates.render(template, percent=50, base_watts=3600)
    # Template unchanged.
    assert template == {"cmd": "set_power_pct_v2", "percent": "{percent}"}


def test_candidates_list_excludes_set_target_freq_neg():
    """The known catastrophic candidate must never be in the run list."""
    labels = [c["label"] for c in probe_candidates.CANDIDATES]
    for label in labels:
        assert "set_target_freq_neg" not in label


def test_candidates_list_excludes_adjust_power_limit():
    """adjust_power_limit is the baseline we're trying to replace, not a candidate."""
    for c in probe_candidates.CANDIDATES:
        assert c["cmd"]["cmd"] != "adjust_power_limit"


# ---------------------------------------------------------------------------
# Classifier — Success
# ---------------------------------------------------------------------------


def test_classifier_success_when_power_holds_in_band(fake_orchestrator, monkeypatch):
    """Power moves from 1800W (50%) to 2160W (60%) and holds for >=90s."""
    # base_watts=3600. Target=60%=2160W. Band=±5%=±180W.
    # Start at 1800W (out of band), then 12 polls at ~2160W (90s+ at 10s polls).
    snaps = (
        [_make_snap(1800)] * 1            # t=10: still below target band
        + [_make_snap(2150)] * 30         # 300s in band -> well past 90s streak
        + [_make_snap(2150)] * 100        # padding
    )
    _patch_summary_sequence(monkeypatch, snaps)
    _patch_clock(monkeypatch, FakeClock())
    _patch_safety_ok(monkeypatch)

    start_snap = _make_snap(1800)
    outcome, polls = fake_orchestrator.classify_attempt("test", target_pct=60, start_snap=start_snap)
    assert outcome == "Success"
    assert len(polls) >= 10


# ---------------------------------------------------------------------------
# Classifier — Reset
# ---------------------------------------------------------------------------


def test_classifier_reset_when_power_zero_two_polls(fake_orchestrator, monkeypatch):
    """Power drops to 0 for 2 consecutive polls -> Reset."""
    snaps = [
        _make_snap(1800),  # t=10
        _make_snap(0),     # t=20 — first zero
        _make_snap(0),     # t=30 — second zero -> Reset
        _make_snap(0),     # padding
    ]
    _patch_summary_sequence(monkeypatch, snaps)
    _patch_clock(monkeypatch, FakeClock())
    _patch_safety_ok(monkeypatch)

    outcome, _ = fake_orchestrator.classify_attempt(
        "test", target_pct=60, start_snap=_make_snap(1800)
    )
    assert outcome == "Reset"


def test_classifier_single_zero_poll_is_not_reset(fake_orchestrator, monkeypatch):
    """A single transient zero doesn't trigger Reset (real firmware glitches)."""
    # 1 zero followed by recovery; rest in target band for >=90s.
    snaps = (
        [_make_snap(1800)]                # t=10
        + [_make_snap(0)]                 # t=20 — single zero
        + [_make_snap(2160)] * 30         # 300s in band -> Success
        + [_make_snap(2160)] * 100        # padding
    )
    _patch_summary_sequence(monkeypatch, snaps)
    _patch_clock(monkeypatch, FakeClock())
    _patch_safety_ok(monkeypatch)

    outcome, _ = fake_orchestrator.classify_attempt(
        "test", target_pct=60, start_snap=_make_snap(1800)
    )
    # Not Reset — should reach Success after streak completes.
    assert outcome == "Success"


# ---------------------------------------------------------------------------
# Classifier — No-op
# ---------------------------------------------------------------------------


def test_classifier_noop_when_power_never_moves(fake_orchestrator, monkeypatch):
    """Power stays within ±2% of base_watts (72W) of start for entire window."""
    # base_watts=3600, no-op band = ±72W. Start 1800W; all polls 1810..1830W.
    # OBSERVATION_SECONDS=600, poll every 10s -> 60 polls.
    snaps = [_make_snap(1800 + (i % 20)) for i in range(120)]
    _patch_summary_sequence(monkeypatch, snaps)
    _patch_clock(monkeypatch, FakeClock())
    _patch_safety_ok(monkeypatch)

    outcome, _ = fake_orchestrator.classify_attempt(
        "test", target_pct=60, start_snap=_make_snap(1800)
    )
    assert outcome == "No-op"


# ---------------------------------------------------------------------------
# Classifier — Timeout
# ---------------------------------------------------------------------------


def test_classifier_timeout_when_power_moves_but_misses_target(fake_orchestrator, monkeypatch):
    """Power moves significantly but never settles inside target band."""
    # Start 1800W. Move up but stop at 1950W (well outside 2160±180 band).
    # Movement = 150W > 72W no-op threshold -> not No-op.
    snaps = [_make_snap(1800)] + [_make_snap(1950)] * 100
    _patch_summary_sequence(monkeypatch, snaps)
    _patch_clock(monkeypatch, FakeClock())
    _patch_safety_ok(monkeypatch)

    outcome, _ = fake_orchestrator.classify_attempt(
        "test", target_pct=60, start_snap=_make_snap(1800)
    )
    assert outcome == "Timeout"


# ---------------------------------------------------------------------------
# Classifier — Stop flag respected
# ---------------------------------------------------------------------------


def test_classifier_stops_on_stop_flag(fake_orchestrator, monkeypatch, tmp_path):
    """If the stop flag appears mid-window, classify_attempt returns Stopped."""
    snaps = [_make_snap(1800)] * 100
    _patch_summary_sequence(monkeypatch, snaps)
    _patch_clock(monkeypatch, FakeClock())
    _patch_safety_ok(monkeypatch)
    # Touch the stop flag before calling.
    with open(orch.STOP_FLAG_FILE, "w") as f:
        f.write("1")

    outcome, _ = fake_orchestrator.classify_attempt(
        "test", target_pct=60, start_snap=_make_snap(1800)
    )
    assert outcome == "Stopped"


# ---------------------------------------------------------------------------
# State file IO
# ---------------------------------------------------------------------------


def test_state_file_write_and_read_roundtrip(tmp_path, monkeypatch):
    """write_state_file produces a JSON file readable by read_state_file."""
    monkeypatch.setattr(orch, "STATE_FILE", str(tmp_path / "probe_state.json"))
    state = {"phase": "phase_a", "pid": 42, "candidates": []}
    orch.write_state_file(state)
    loaded = orch.read_state_file()
    assert loaded is not None
    assert loaded["phase"] == "phase_a"
    assert loaded["pid"] == 42
    assert "last_write_at" in loaded


def test_state_file_atomic_no_partial_file(tmp_path, monkeypatch):
    """The .tmp file should not remain after a successful write."""
    monkeypatch.setattr(orch, "STATE_FILE", str(tmp_path / "probe_state.json"))
    orch.write_state_file({"phase": "test"})
    files = list(tmp_path.glob("probe_state.json*"))
    # Only the final file should exist, no .tmp leftover.
    assert any(f.name == "probe_state.json" for f in files)
    assert not any(f.name.endswith(".tmp") for f in files)


# ---------------------------------------------------------------------------
# SOC safety evaluation
# ---------------------------------------------------------------------------


def test_safety_running_when_soc_above_floor_and_fresh(monkeypatch):
    monkeypatch.setattr(
        orch,
        "get_battery_status",
        lambda: {"status": {"soc_percent": 70.0, "battery_fresh": True, "battery_age_seconds": 12}},
    )
    safety = orch.evaluate_soc_safety(emergency_floor=30)
    assert safety["safety_state"] == "running"
    assert safety["soc"] == 70.0


def test_safety_stale_when_battery_age_exceeds_threshold(monkeypatch):
    monkeypatch.setattr(
        orch,
        "get_battery_status",
        lambda: {"status": {"soc_percent": 70.0, "battery_fresh": True, "battery_age_seconds": 700}},
    )
    safety = orch.evaluate_soc_safety(emergency_floor=30)
    assert safety["safety_state"] == "paused_stale_battery"


def test_safety_paused_low_soc(monkeypatch):
    monkeypatch.setattr(
        orch,
        "get_battery_status",
        lambda: {"status": {"soc_percent": 25.0, "battery_fresh": True, "battery_age_seconds": 30}},
    )
    safety = orch.evaluate_soc_safety(emergency_floor=30)
    assert safety["safety_state"] == "paused_low_soc"


def test_safety_paused_no_data_when_main_service_down(monkeypatch):
    monkeypatch.setattr(orch, "get_battery_status", lambda: None)
    safety = orch.evaluate_soc_safety(emergency_floor=30)
    assert safety["safety_state"] == "paused_no_data"


def test_safety_running_from_live_shape_with_last_seen(monkeypatch):
    """Verify the orchestrator handles the actual /api/battery/status payload shape
    (connection.last_seen ISO timestamp, no explicit battery_fresh flag)."""
    now_iso = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=12)).isoformat()
    monkeypatch.setattr(
        orch,
        "get_battery_status",
        lambda: {
            "connection": {"connected": True, "last_seen": now_iso, "error": None},
            "status": {"soc_percent": 70.0},
        },
    )
    safety = orch.evaluate_soc_safety(emergency_floor=30)
    assert safety["safety_state"] == "running"
    assert safety["soc"] == 70.0
    assert safety["age_seconds"] is not None and safety["age_seconds"] < 30


def test_safety_stale_from_live_shape_with_old_last_seen(monkeypatch):
    """When last_seen is older than 10 min, the orchestrator must pause as stale."""
    old_iso = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=900)).isoformat()
    monkeypatch.setattr(
        orch,
        "get_battery_status",
        lambda: {
            "connection": {"connected": True, "last_seen": old_iso, "error": None},
            "status": {"soc_percent": 70.0},
        },
    )
    safety = orch.evaluate_soc_safety(emergency_floor=30)
    assert safety["safety_state"] == "paused_stale_battery"


def test_is_safe_to_resume_requires_delta(monkeypatch):
    """Resume requires SOC >= floor + SOC_RESUME_DELTA."""
    floor = 30
    delta = orch.SOC_RESUME_DELTA
    # Exactly at floor — not safe yet.
    s = {"safety_state": "running", "soc": float(floor)}
    assert not orch.is_safe_to_resume(s, floor)
    # At floor + delta - 1 — still not safe.
    s = {"safety_state": "running", "soc": float(floor + delta - 1)}
    assert not orch.is_safe_to_resume(s, floor)
    # At floor + delta — safe.
    s = {"safety_state": "running", "soc": float(floor + delta)}
    assert orch.is_safe_to_resume(s, floor)
