"""Unit tests for the EG4 PV-predict parsing/classification helpers.

These cover the three pure functions in utils/eg4_pv_predict.py:

    parse_pv_predict_kwh(raw_value)
    extract_inverter_serial(inverter)
    classify_pv_predict_response(http_status, parsed_json)

No network, no asyncio, no EG4 SDK — pure parsers only. The I/O wrapper in
eg4_client.py is exercised at integration time on the Pi.
"""
from __future__ import annotations

from types import SimpleNamespace

from utils.eg4_pv_predict import (
    classify_pv_predict_response,
    extract_inverter_serial,
    parse_pv_predict_kwh,
)


# ----------------------------------------------------------------------
# parse_pv_predict_kwh
# ----------------------------------------------------------------------


def test_parse_positive_integer_yields_tenths_of_kwh():
    """Raw 712 → 71.2 kWh — the canonical conversion case."""
    assert parse_pv_predict_kwh(712) == 71.2


def test_parse_zero_is_a_valid_pessimistic_forecast():
    """0 must produce 0.0, NOT None.

    This is the safety-critical distinction: a confident "EG4 expects nothing
    today" prediction must propagate to expected_kwh = 0.0 so the day is
    disabled under any positive deficit. Conflating zero with missing would
    silently flip the safety logic and let the miner run on an empty
    forecast.
    """
    result = parse_pv_predict_kwh(0)
    assert result == 0.0
    assert result is not None


def test_parse_none_is_missing():
    assert parse_pv_predict_kwh(None) is None


def test_parse_negative_is_missing():
    """Negative is non-physical for a PV harvest prediction."""
    assert parse_pv_predict_kwh(-5) is None
    assert parse_pv_predict_kwh(-0.1) is None


def test_parse_non_numeric_is_missing():
    assert parse_pv_predict_kwh("144") is None
    assert parse_pv_predict_kwh([144]) is None
    assert parse_pv_predict_kwh({"v": 144}) is None


def test_parse_bool_is_rejected():
    """isinstance(True, int) is True; the bool guard prevents a sneaky 1.0."""
    assert parse_pv_predict_kwh(True) is None
    assert parse_pv_predict_kwh(False) is None


def test_parse_float_input_is_accepted():
    """Server is documented to send integers; floats still convert cleanly."""
    assert parse_pv_predict_kwh(144.0) == 14.4


# ----------------------------------------------------------------------
# extract_inverter_serial
# ----------------------------------------------------------------------


def test_extract_serial_from_object_attribute():
    """The eg4_inverter_api library exposes serialNum as an attribute."""
    inv = SimpleNamespace(serialNum="4392670077", plantId="P1")
    assert extract_inverter_serial(inv) == "4392670077"


def test_extract_serial_from_dict_shape():
    """Some code paths may return raw JSON dicts — handle both."""
    inv = {"serialNum": "ABC123", "plantId": "P1"}
    assert extract_inverter_serial(inv) == "ABC123"


def test_extract_serial_returns_none_when_missing():
    """A missing or empty serial must be None, never crash."""
    assert extract_inverter_serial(None) is None
    assert extract_inverter_serial(SimpleNamespace(plantId="P1")) is None
    assert extract_inverter_serial({"plantId": "P1"}) is None
    assert extract_inverter_serial(SimpleNamespace(serialNum=None)) is None
    assert extract_inverter_serial(SimpleNamespace(serialNum="   ")) is None
    assert extract_inverter_serial({"serialNum": ""}) is None


def test_extract_serial_coerces_to_str_and_strips():
    """Defensive coercion — server has been observed returning ints."""
    inv = SimpleNamespace(serialNum=4392670077)
    assert extract_inverter_serial(inv) == "4392670077"


# ----------------------------------------------------------------------
# classify_pv_predict_response
# ----------------------------------------------------------------------


def _ok_payload():
    return {
        "success": True,
        "ePvPredict": {
            "success": "True",
            "todayPvEnergy": 144,
            "tomorrowPvEnergy": 114,
        },
        "localDate": "2026/05/23",
    }


def test_classify_ok_payload_returns_ok():
    verdict, _ = classify_pv_predict_response(200, _ok_payload())
    assert verdict == "ok"


def test_classify_http_4xx_triggers_recovery():
    """Session expiry / unbound inverter typically surfaces as 4xx or 5xx."""
    verdict, reason = classify_pv_predict_response(401, None)
    assert verdict == "recovery"
    assert "401" in reason


def test_classify_http_5xx_triggers_recovery():
    verdict, reason = classify_pv_predict_response(503, None)
    assert verdict == "recovery"
    assert "503" in reason


def test_classify_non_dict_body_is_error():
    """Malformed JSON is non-recoverable — re-login won't fix a server bug."""
    verdict, _ = classify_pv_predict_response(200, "not json")
    assert verdict == "error"


def test_classify_top_level_success_false_triggers_recovery():
    payload = {"success": False, "ePvPredict": {"success": "True", "todayPvEnergy": 100}}
    verdict, _ = classify_pv_predict_response(200, payload)
    assert verdict == "recovery"


def test_classify_missing_epvpredict_triggers_recovery():
    payload = {"success": True}
    verdict, _ = classify_pv_predict_response(200, payload)
    assert verdict == "recovery"


def test_classify_epvpredict_success_false_triggers_recovery():
    payload = {
        "success": True,
        "ePvPredict": {"success": "False"},
    }
    verdict, _ = classify_pv_predict_response(200, payload)
    assert verdict == "recovery"


def test_classify_epvpredict_success_case_insensitive_true():
    """ePvPredict.success is a STRING per probe; tolerate any casing."""
    payload = {
        "success": True,
        "ePvPredict": {"success": "true", "todayPvEnergy": 144},
    }
    verdict, _ = classify_pv_predict_response(200, payload)
    assert verdict == "ok"
