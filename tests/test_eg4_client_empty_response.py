"""Tests for the EG4 empty-response classifiers.

These tests exercise the two authoritative rules that decide whether an
EG4 portal response is "empty" (i.e. the session is effectively dead and
re-login is warranted):

  - _is_empty_response — for the raw response (structurally empty)
  - _is_merged_snapshot_empty — for the merged snapshot (zombie session:
    raw response is structurally valid but every meaningful field is
    None or 0, usually after the inverter selection is lost)

No network, no EG4 library, no event loop — just the pure classifier
functions. These are the only places the empty-response rules are
encoded, so covering them here is the same as covering the recovery
decision logic.
"""

from eg4_client import _is_empty_response, _is_merged_snapshot_empty


# ---------- _is_empty_response ----------

def test_silent_session_expiry_is_empty():
    """The 2026-05-11 incident shape: HTTP 200 with success=False, data=None."""
    resp = {"success": False, "data": None, "error_message": None}
    assert _is_empty_response(resp) is True


def test_normal_response_with_real_fields_is_not_empty():
    """A populated runtime/battery response must pass through untouched."""
    resp = {
        "success": True,
        "data": {"soc": 54.6, "ppv1": 3200, "ppv2": 4300},
        "ppv1": 3200,
        "ppv2": 4300,
        "soc": 54.6,
    }
    assert _is_empty_response(resp) is False


def test_none_response_is_empty():
    """A None response (network error fallback in some libs) counts as empty."""
    assert _is_empty_response(None) is True


def test_success_true_with_data_object_is_not_empty():
    """Bare success/data wrapper without unwrap is still a non-empty response."""
    resp = {"success": True, "data": {"k": "v"}}
    assert _is_empty_response(resp) is False


# ---------- _is_merged_snapshot_empty ----------

def test_all_none_merged_snapshot_is_empty():
    """The 2026-05-22 zombie-session shape: every field None after merge."""
    merged = {
        "ts": "2026-05-22T18:07:26-04:00",
        "soc_percent": None,
        "pack_voltage_v": None,
        "pack_current_a": None,
        "pv_power_w": None,
        "load_power_w": None,
        "grid_power_w": None,
        "ac_couple_w": None,
        "battery_net_w": None,
        "units": [],
    }
    assert _is_merged_snapshot_empty(merged) is True


def test_merged_with_valid_soc_is_not_empty():
    """A real SOC reading means the snapshot has data, even if powers are 0."""
    merged = {
        "ts": "2026-05-22T18:16:45-04:00",
        "soc_percent": 100.0,
        "pack_voltage_v": 55.1,
        "pack_current_a": 0.9,
        "pv_power_w": 0,
        "load_power_w": 0,
        "grid_power_w": 0,
        "ac_couple_w": 0,
        "battery_net_w": 0,
        "units": [],
    }
    assert _is_merged_snapshot_empty(merged) is False


def test_merged_with_pv_power_but_no_soc_is_not_empty():
    """Partial readings (e.g. PV but battery side temporarily unavailable)
    must NOT trigger recovery — any single real value means data is flowing."""
    merged = {
        "ts": "2026-05-22T12:00:00-04:00",
        "soc_percent": None,
        "pack_voltage_v": None,
        "pack_current_a": None,
        "pv_power_w": 7500.0,
        "load_power_w": 0,
        "grid_power_w": 0,
        "ac_couple_w": 0,
        "battery_net_w": 0,
        "units": [],
    }
    assert _is_merged_snapshot_empty(merged) is False


def test_merged_with_units_but_no_scalars_is_not_empty():
    """Per-unit data populated but pack/runtime scalars None: still real data."""
    merged = {
        "ts": "2026-05-22T18:00:00-04:00",
        "soc_percent": None,
        "pack_voltage_v": None,
        "pack_current_a": None,
        "pv_power_w": None,
        "load_power_w": None,
        "grid_power_w": None,
        "ac_couple_w": None,
        "battery_net_w": None,
        "units": [
            {"sn": "Battery_ID_01", "soc": 100, "voltage_mv": 5512, "current_a": 0},
        ],
    }
    assert _is_merged_snapshot_empty(merged) is False
