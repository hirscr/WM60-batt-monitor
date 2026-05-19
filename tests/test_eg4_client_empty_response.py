"""Tests for the EG4 empty-response classifier.

These tests exercise the single authoritative rule that decides whether
an EG4 portal response is "empty" (i.e. the session is effectively dead
and re-login is warranted). No network, no EG4 library, no event loop —
just the pure classifier function.

The classifier is the only place the empty-response rule is encoded, so
covering it here is the same as covering the recovery decision logic.
"""

from eg4_client import _is_empty_response


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
