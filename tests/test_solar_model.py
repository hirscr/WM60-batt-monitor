"""Unit tests for utils.solar_model.

Pure-function tests — no fixtures, no mocks. Covers:
  - max_daily_energy_kwh at both solstices and near each equinox
  - sinusoidal symmetry around the summer solstice
  - expected_energy_kwh at 0%, 50%, 100% cloud cover
  - cloud-cover clamp behavior for out-of-range inputs
"""
from utils.solar_model import expected_energy_kwh, max_daily_energy_kwh


SUMMER = 75.0
WINTER = 30.0


# ----------------------------------------------------------------------
# max_daily_energy_kwh
# ----------------------------------------------------------------------


def test_summer_solstice_returns_summer_max():
    # Day 172 = June 21
    assert max_daily_energy_kwh(172, SUMMER, WINTER) == SUMMER


def test_winter_solstice_returns_close_to_winter_max():
    # Day 355 = Dec 21. cos(2π * (355-172)/365) = cos(2π * 183/365) which
    # is essentially -1 (the model rounds to winter_max within floating-point
    # tolerance — the half-day offset gives a tiny residual).
    val = max_daily_energy_kwh(355, SUMMER, WINTER)
    assert abs(val - WINTER) < 0.05


def test_spring_equinox_lands_near_midpoint():
    # Day 80 ≈ Mar 21 — roughly a quarter cycle from the summer solstice,
    # so cos is near 0 and the result hugs the midpoint.
    mid = (SUMMER + WINTER) / 2.0
    val = max_daily_energy_kwh(80, SUMMER, WINTER)
    assert abs(val - mid) < 5.0


def test_autumn_equinox_lands_near_midpoint():
    # Day 266 ≈ Sept 23
    mid = (SUMMER + WINTER) / 2.0
    val = max_daily_energy_kwh(266, SUMMER, WINTER)
    assert abs(val - mid) < 5.0


def test_symmetry_around_summer_solstice():
    # The cosine model is symmetric around the summer solstice in day-of-year
    # space, so days equidistant from day 172 produce the same value.
    for delta in (10, 30, 60):
        a = max_daily_energy_kwh(172 - delta, SUMMER, WINTER)
        b = max_daily_energy_kwh(172 + delta, SUMMER, WINTER)
        assert abs(a - b) < 0.01, f"asymmetry at delta={delta}: {a} vs {b}"


def test_equal_summer_and_winter_flatlines():
    # When summer == winter the year is constant.
    for d in (1, 80, 172, 266, 355):
        assert max_daily_energy_kwh(d, 50.0, 50.0) == 50.0


# ----------------------------------------------------------------------
# expected_energy_kwh
# ----------------------------------------------------------------------


def test_zero_cloud_returns_max():
    assert expected_energy_kwh(60.0, 0.0) == 60.0


def test_half_cloud_returns_half_max():
    assert expected_energy_kwh(60.0, 50.0) == 30.0


def test_full_cloud_returns_zero():
    assert expected_energy_kwh(60.0, 100.0) == 0.0


def test_negative_cloud_clamps_to_zero():
    # Defensive clamp — Open-Meteo should never return negatives but we
    # don't trust upstream blindly.
    assert expected_energy_kwh(60.0, -10.0) == 60.0


def test_over_100_cloud_clamps_to_100():
    assert expected_energy_kwh(60.0, 150.0) == 0.0
