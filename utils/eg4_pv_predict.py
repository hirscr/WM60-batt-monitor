"""Pure helpers for the EG4 portal /api/weather/forecast endpoint.

These functions are extracted out of eg4_client.py so they can be unit-tested
without any network, asyncio loop, or EG4 SDK dependency. The I/O wrapper
lives in eg4_client.EG4Client; this module owns:

    - parse_pv_predict_kwh(raw_value)
        Convert one of the integer fields under ePvPredict (todayPvEnergy /
        tomorrowPvEnergy) to a kWh float per the project rules:
          - missing / non-numeric / negative -> None
          - zero -> 0.0  (valid pessimistic forecast — NOT missing)
          - positive integer -> value / 10.0

    - extract_inverter_serial(inverter)
        Pull a serial number off whatever shape the eg4_inverter_api library
        returned. Handles object attribute access (Inverter.serialNum) and
        dict-style access ({"serialNum": ...}). Returns None when the field
        is genuinely missing.

    - classify_pv_predict_response(http_status, parsed_json)
        Single authoritative rule for whether a forecast response is OK or
        whether the caller should trigger _attempt_recovery. Returns one of:
          ("ok", reason_label)        — proceed; use the parsed values
          ("recovery", reason_label)  — trigger session re-login + retry once
          ("error", reason_label)     — non-recoverable; cache the error
        The reason_label is suitable for passing to _attempt_recovery so the
        log line names which failure mode tripped.
"""
from __future__ import annotations

from typing import Any, Optional, Tuple


def parse_pv_predict_kwh(raw_value: Any) -> Optional[float]:
    """Convert a single ePvPredict integer (tenths of kWh) to kWh.

    Rules (from FOREMAN_PROMPT_EG4_PV_PREDICT.md):
      - None / missing / non-numeric -> None
      - Negative numbers -> None (physically impossible for a daily prediction)
      - Zero -> 0.0 (valid pessimistic forecast; MUST NOT be conflated with missing)
      - Positive number -> value / 10.0

    Booleans are explicitly rejected as non-numeric. The EG4 server returns
    integers, not bools; but Python's isinstance(True, int) is True, so the
    bool guard prevents a "True" value from sneaking through as 1.0 kWh.
    """
    if raw_value is None:
        return None
    if isinstance(raw_value, bool):
        return None
    if not isinstance(raw_value, (int, float)):
        # Reject strings, lists, etc. The EG4 endpoint emits a plain integer;
        # any other shape is malformed and treated as missing.
        return None
    if raw_value < 0:
        return None
    return float(raw_value) / 10.0


def extract_inverter_serial(inverter: Any) -> Optional[str]:
    """Return the serial number of an EG4 Inverter object/dict, or None.

    The eg4_inverter_api library returns Inverter objects whose serial number
    is exposed via the `serialNum` attribute (confirmed in the probe). Some
    code paths in the library may return dict-like shapes, so we accept both.

    Returns:
        The serial number as a string, stripped of surrounding whitespace,
        or None when the field is missing or empty.
    """
    if inverter is None:
        return None

    # Object-style access first (the common case for EG4InverterAPI).
    serial = getattr(inverter, "serialNum", None)

    # Fallback: dict-style access for any code path that returns raw JSON.
    if serial is None and isinstance(inverter, dict):
        serial = inverter.get("serialNum")

    if serial is None:
        return None

    serial_str = str(serial).strip()
    if not serial_str:
        return None
    return serial_str


def classify_pv_predict_response(
    http_status: int,
    parsed_json: Any,
) -> Tuple[str, str]:
    """Decide whether a /api/weather/forecast response is usable.

    Returns one of:
      ("ok", "ok")
        Proceed; the caller should parse todayPvEnergy / tomorrowPvEnergy.
      ("recovery", reason_label)
        Likely session expiry or lost inverter-binding. Caller must invoke
        _attempt_recovery and retry once. The reason_label is human-readable
        and suitable for the recovery log line.
      ("error", reason_label)
        Non-recoverable (malformed JSON, etc.). Caller caches the error and
        does NOT retry.

    Decision rules (per spec):
      - http_status >= 400 -> recovery (treated like the zombie-session case)
      - parsed_json is not a dict -> error (malformed)
      - parsed_json.success is explicitly False -> recovery
      - parsed_json.ePvPredict missing -> recovery (the block we need is gone)
      - parsed_json.ePvPredict.success != "True" (case-insensitive) -> recovery
      - otherwise -> ok
    """
    if http_status >= 400:
        return ("recovery", f"HTTP {http_status} from /api/weather/forecast")

    if not isinstance(parsed_json, dict):
        return ("error", "non-JSON response body")

    # Top-level success flag is a real boolean per probe.
    if parsed_json.get("success") is False:
        return ("recovery", "top-level success=false")

    predict = parsed_json.get("ePvPredict")
    if not isinstance(predict, dict):
        return ("recovery", "ePvPredict block missing")

    # ePvPredict.success is a STRING "True" / "False" per probe — compare
    # case-insensitively so we are tolerant of any casing.
    success_str = str(predict.get("success", "")).strip().lower()
    if success_str != "true":
        return ("recovery", f"ePvPredict.success={predict.get('success')!r}")

    return ("ok", "ok")
