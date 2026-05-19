"""Unit tests for WeatherService.cloud_cover_remaining_daylight_pct.

No live network — the Open-Meteo HTTP call is mocked via _fetch_forecast.
Pure parsing + mean computation is exercised through _parse_payload and
_remaining_daylight_cloud_cover.

The wall-clock "now" used by the mean computation is mocked via
WeatherService._now_local — a single seam that keeps tests deterministic
without monkey-patching the datetime module.

Coverage:
  - typical mid-day case: mean spans current hour through sunset hour inclusive
  - past sunset returns None
  - missing hourly block returns None
  - malformed hourly arrays return None
  - boundary correctness: current-hour and sunset-hour are both included
  - get_today_forecast surfaces the new key
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from services.weather_service import WeatherService


TZ_NAME = "America/New_York"


def _make_service() -> WeatherService:
    return WeatherService(
        latitude=40.0,
        longitude=-74.0,
        timezone_str=TZ_NAME,
        refresh_seconds=3600,
        freshness_seconds=24 * 3600,
    )


def _payload_with_hourly(times: list, clouds: list) -> dict:
    """Build a payload similar to what Open-Meteo returns.

    `times` is a list of ISO strings (local times, no offset, as Open-Meteo
    returns when `timezone=` is set). `clouds` is the matching cloud_cover list.
    """
    return {
        "daily": {
            "time": ["2026-05-18"],
            "cloud_cover_mean": [30],
            "sunrise": ["2026-05-18T05:42"],
            "sunset": ["2026-05-18T19:42"],
        },
        "hourly": {
            "time": times,
            "cloud_cover": clouds,
        },
    }


def _hourly_times_full_day(date_str: str = "2026-05-18") -> list:
    """Open-Meteo returns 24 hourly entries; we mimic that here."""
    return [f"{date_str}T{h:02d}:00" for h in range(24)]


def _fixed_now(hour: int, minute: int = 0, day: int = 18) -> datetime:
    return datetime(2026, 5, day, hour, minute, 0, tzinfo=ZoneInfo(TZ_NAME))


# ----------------------------------------------------------------------
# Typical mid-day case
# ----------------------------------------------------------------------


def test_typical_midday_returns_mean_over_remaining_hours():
    svc = _make_service()
    times = _hourly_times_full_day()
    # Set cloud_cover[h] = h so the average is easy to predict.
    clouds = [float(h) for h in range(24)]
    snap = svc._parse_payload(_payload_with_hourly(times, clouds))

    # Sunset is 19:42 -> sunset_hour = 19:00. Current hour 12 -> [12..19].
    with patch.object(WeatherService, "_now_local", return_value=_fixed_now(12)):
        result = svc._remaining_daylight_cloud_cover(
            snap["hourly_times"], snap["hourly_cloud_cover"], snap["sunset_dt"]
        )
    expected = sum(range(12, 20)) / 8
    assert result == pytest.approx(expected)


# ----------------------------------------------------------------------
# Past sunset
# ----------------------------------------------------------------------


def test_past_sunset_returns_none():
    svc = _make_service()
    times = _hourly_times_full_day()
    clouds = [50.0] * 24
    snap = svc._parse_payload(_payload_with_hourly(times, clouds))

    with patch.object(WeatherService, "_now_local", return_value=_fixed_now(20)):
        result = svc._remaining_daylight_cloud_cover(
            snap["hourly_times"], snap["hourly_cloud_cover"], snap["sunset_dt"]
        )
    assert result is None


def test_exactly_at_sunset_returns_none():
    svc = _make_service()
    times = _hourly_times_full_day()
    clouds = [50.0] * 24
    snap = svc._parse_payload(_payload_with_hourly(times, clouds))

    with patch.object(WeatherService, "_now_local", return_value=_fixed_now(19, 42)):
        result = svc._remaining_daylight_cloud_cover(
            snap["hourly_times"], snap["hourly_cloud_cover"], snap["sunset_dt"]
        )
    assert result is None


# ----------------------------------------------------------------------
# Missing / malformed hourly
# ----------------------------------------------------------------------


def test_missing_hourly_returns_none_at_parse():
    svc = _make_service()
    payload = {
        "daily": {
            "time": ["2026-05-18"],
            "cloud_cover_mean": [30],
            "sunrise": ["2026-05-18T05:42"],
            "sunset": ["2026-05-18T19:42"],
        },
        # No hourly block at all
    }
    snap = svc._parse_payload(payload)
    assert snap["hourly_times"] is None
    assert snap["hourly_cloud_cover"] is None

    with patch.object(WeatherService, "_now_local", return_value=_fixed_now(12)):
        result = svc._remaining_daylight_cloud_cover(
            snap["hourly_times"], snap["hourly_cloud_cover"], snap["sunset_dt"]
        )
    assert result is None


def test_mismatched_array_lengths_return_none():
    svc = _make_service()
    times = ["2026-05-18T12:00", "2026-05-18T13:00"]
    clouds = [50.0]  # mismatched
    snap = svc._parse_payload(_payload_with_hourly(times, clouds))
    assert snap["hourly_times"] is None
    assert snap["hourly_cloud_cover"] is None


def test_malformed_time_string_returns_none():
    svc = _make_service()
    times = ["not-a-time", "2026-05-18T13:00"]
    clouds = [50.0, 60.0]
    snap = svc._parse_payload(_payload_with_hourly(times, clouds))
    assert snap["hourly_times"] is None
    assert snap["hourly_cloud_cover"] is None


def test_malformed_cloud_value_returns_none():
    svc = _make_service()
    times = ["2026-05-18T12:00", "2026-05-18T13:00"]
    clouds = [50.0, "cloudy"]
    snap = svc._parse_payload(_payload_with_hourly(times, clouds))
    assert snap["hourly_times"] is None
    assert snap["hourly_cloud_cover"] is None


def test_non_dict_hourly_returns_none():
    svc = _make_service()
    payload = {
        "daily": {
            "time": ["2026-05-18"],
            "cloud_cover_mean": [30],
            "sunrise": ["2026-05-18T05:42"],
            "sunset": ["2026-05-18T19:42"],
        },
        "hourly": "not a dict",
    }
    snap = svc._parse_payload(payload)
    assert snap["hourly_times"] is None


# ----------------------------------------------------------------------
# Boundary correctness
# ----------------------------------------------------------------------


def test_current_hour_inclusive():
    """The bucket at the current hour must be part of the average."""
    svc = _make_service()
    times = _hourly_times_full_day()
    # Every hour 100% cloud except 12:00 = 0%. If 12:00 is included the
    # mean drops below 100; if excluded the mean stays at 100.
    clouds = [100.0] * 24
    clouds[12] = 0.0
    snap = svc._parse_payload(_payload_with_hourly(times, clouds))

    with patch.object(WeatherService, "_now_local", return_value=_fixed_now(12, 30)):
        result = svc._remaining_daylight_cloud_cover(
            snap["hourly_times"], snap["hourly_cloud_cover"], snap["sunset_dt"]
        )
    # Buckets 12..19 inclusive: [0, 100, 100, 100, 100, 100, 100, 100] / 8 = 87.5
    assert result == pytest.approx(700.0 / 8)


def test_sunset_hour_inclusive():
    """The bucket at the sunset hour must be part of the average."""
    svc = _make_service()
    times = _hourly_times_full_day()
    clouds = [100.0] * 24
    clouds[19] = 0.0  # only the sunset-hour bucket differs
    snap = svc._parse_payload(_payload_with_hourly(times, clouds))

    with patch.object(WeatherService, "_now_local", return_value=_fixed_now(12)):
        result = svc._remaining_daylight_cloud_cover(
            snap["hourly_times"], snap["hourly_cloud_cover"], snap["sunset_dt"]
        )
    # If 19 is included, mean = (7*100 + 0) / 8 = 87.5; if excluded mean = 100.
    assert result == pytest.approx(700.0 / 8)


# ----------------------------------------------------------------------
# get_today_forecast surfaces the value
# ----------------------------------------------------------------------


def test_get_today_forecast_includes_remaining_key_when_no_snapshot():
    svc = _make_service()
    out = svc.get_today_forecast()
    assert "cloud_cover_remaining_daylight_pct" in out
    assert out["cloud_cover_remaining_daylight_pct"] is None


def test_get_today_forecast_returns_remaining_after_refresh():
    """End-to-end through the cache: _do_refresh stores the snapshot, then
    get_today_forecast computes the remaining-daylight mean for 'now'."""
    svc = _make_service()
    times = _hourly_times_full_day()
    clouds = [10.0] * 24
    payload = _payload_with_hourly(times, clouds)

    with patch.object(svc, "_fetch_forecast", return_value=payload):
        svc._do_refresh()

    with patch.object(WeatherService, "_now_local", return_value=_fixed_now(12)):
        out = svc.get_today_forecast()
    assert out["cloud_cover_remaining_daylight_pct"] == pytest.approx(10.0)
