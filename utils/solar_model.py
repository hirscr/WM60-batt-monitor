"""Seasonal solar energy model.

Pure functions. No I/O. Suitable for unit-testing without mocks.

Two functions:

- max_daily_energy_kwh(day_of_year, summer_max_kwh, winter_max_kwh)
    Sinusoidal interpolation between solstices. Northern Hemisphere only:
    summer solstice = day 172 (Jun 21), winter solstice = day 355 (Dec 21).
    On day 172 the function returns summer_max_kwh; on day 355 it returns
    winter_max_kwh; equinoxes land near the midpoint.

- expected_energy_kwh(max_for_day_kwh, cloud_cover_pct)
    Linear attenuation: 0% cloud -> full max, 100% cloud -> zero.

Both functions are intentionally trivial so the gating logic stays auditable.
"""
from __future__ import annotations

import math

# Day-of-year (1..365) for the Northern Hemisphere solstices.
_SUMMER_SOLSTICE_DOY = 172  # ~June 21
_DAYS_PER_YEAR = 365


def max_daily_energy_kwh(
    day_of_year: int,
    summer_max_kwh: float,
    winter_max_kwh: float,
) -> float:
    """Sinusoidal seasonal maximum harvest in kWh for the given day-of-year.

    Returns summer_max_kwh on the summer solstice and winter_max_kwh on the
    winter solstice; smoothly interpolates between via a cosine. Pure function.

    Args:
        day_of_year: 1..366
        summer_max_kwh: peak summer harvest (e.g. cloud-free June day)
        winter_max_kwh: peak winter harvest (e.g. cloud-free December day)
    """
    mid = (summer_max_kwh + winter_max_kwh) / 2.0
    amp = (summer_max_kwh - winter_max_kwh) / 2.0
    phase = 2.0 * math.pi * (day_of_year - _SUMMER_SOLSTICE_DOY) / _DAYS_PER_YEAR
    return mid + amp * math.cos(phase)


def expected_energy_kwh(max_for_day_kwh: float, cloud_cover_pct: float) -> float:
    """Linear cloud-cover attenuation of max_for_day_kwh.

    cloud_cover_pct is clamped to [0, 100] before computation so callers can
    pass forecast values without revalidating the bound.
    """
    pct = max(0.0, min(100.0, cloud_cover_pct))
    return max_for_day_kwh * (1.0 - pct / 100.0)
