"""Pure-logic tests for services.tier_promotion.TierPromotion.

No I/O — the monotonic clock is injectable so cooldown tests run instantly.

Coverage:
  - SOC crosses 90% with clear skies and 4h to sunset → promote to 90
  - SOC crosses 90% with cloudy skies → no promote
  - SOC crosses 90% with 2h to sunset → no promote (sunset too close)
  - SOC crosses 99% from 90% tier with clear skies → promote to 100
  - SOC drops 99→98.5% while at 100% tier → demote to 90, arm cooldown
  - SOC drops 90→89.5% while at 90% tier → demote to None, arm cooldown
  - SOC re-crosses 90% inside cooldown → no promote
  - SOC re-crosses 90% after cooldown expires → promote
  - Stale cloud cover → no promote, no forced demote
  - Past sunset (cloud None) → no promote, tier preserved
  - Restart: first tick after restart cannot promote
"""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from services.tier_promotion import TierPromotion, COOLDOWN_SEC


TZ = ZoneInfo("America/New_York")


class FakeClock:
    """Tiny injectable monotonic clock. .advance() simulates wall time."""
    def __init__(self, start: float = 1000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _now_local(hour: int = 12, minute: int = 0) -> datetime:
    return datetime(2026, 5, 18, hour, minute, 0, tzinfo=TZ)


def _sunset(hour: int = 19, minute: int = 42) -> datetime:
    return datetime(2026, 5, 18, hour, minute, 0, tzinfo=TZ)


def _new_tp(*, clock: FakeClock, tier=None, last90=0.0, last100=0.0, last_soc=None) -> TierPromotion:
    return TierPromotion(
        tier=tier,
        last_demotion_from_90_ts=last90,
        last_demotion_from_100_ts=last100,
        last_seen_soc=last_soc,
        now_fn=clock,
    )


# ----------------------------------------------------------------------
# 90% promotion
# ----------------------------------------------------------------------


def test_promote_to_90_with_clear_skies_and_4h_to_sunset():
    clock = FakeClock()
    tp = _new_tp(clock=clock, last_soc=89.0)  # last seen below 90
    result = tp.evaluate(
        soc_pct=90.5,
        cloud_cover_remaining_pct=5.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(15),  # 15:00 -> ~4.7h to 19:42
    )
    assert result.tier == 90
    assert result.target_pct == 90
    assert result.tier_changed is True
    assert tp.tier == 90


def test_promote_blocked_when_cloudy():
    clock = FakeClock()
    tp = _new_tp(clock=clock, last_soc=89.0)
    result = tp.evaluate(
        soc_pct=91.0,
        cloud_cover_remaining_pct=60.0,  # cloudy
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(12),
    )
    assert result.tier is None
    assert result.tier_changed is False


def test_promote_blocked_when_only_2h_to_sunset():
    clock = FakeClock()
    tp = _new_tp(clock=clock, last_soc=89.0)
    result = tp.evaluate(
        soc_pct=91.0,
        cloud_cover_remaining_pct=5.0,
        forecast_fresh=True,
        sunset_dt=_sunset(19, 0),
        now_local=_now_local(17),  # 2h before sunset, fails > 3h gate
    )
    assert result.tier is None
    assert result.tier_changed is False


# ----------------------------------------------------------------------
# 100% promotion
# ----------------------------------------------------------------------


def test_promote_to_100_from_90_with_clear_skies():
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=90, last_soc=98.0)
    result = tp.evaluate(
        soc_pct=99.5,
        cloud_cover_remaining_pct=2.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(13),  # ~6.7h to sunset
    )
    assert result.tier == 100
    assert result.target_pct == 100
    assert result.tier_changed is True


def test_promote_to_100_unconditional_when_battery_full_even_if_cloudy():
    """SOC >= 99% overrides cloud cover — full battery promotes regardless."""
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=90, last_soc=98.0)
    result = tp.evaluate(
        soc_pct=99.5,
        cloud_cover_remaining_pct=80.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(13),
    )
    assert result.tier == 100
    assert result.tier_changed is True


def test_promote_to_100_crossing_still_blocked_when_cloudy_and_soc_below_99():
    """Clouds still block the weather-gated crossing path when SOC < 99%."""
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=90, last_soc=97.5)
    result = tp.evaluate(
        soc_pct=98.5,  # below 99 — unconditional override does not fire
        cloud_cover_remaining_pct=80.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(13),
    )
    assert result.tier == 90  # unchanged — cloud blocks crossing-based path
    assert result.tier_changed is False


# ----------------------------------------------------------------------
# Demotion
# ----------------------------------------------------------------------


def test_demote_from_100_to_90_when_soc_drops_below_99():
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=100, last_soc=99.5)
    result = tp.evaluate(
        soc_pct=98.5,
        cloud_cover_remaining_pct=5.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(12),
    )
    assert result.tier == 90
    assert result.target_pct == 90
    assert result.tier_changed is True
    # 100% cooldown should now be armed (full 30 min)
    assert tp.cooldown_remaining_100_sec() == COOLDOWN_SEC


def test_demote_from_90_to_none_arms_cooldown():
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=90, last_soc=90.5)
    result = tp.evaluate(
        soc_pct=89.5,
        cloud_cover_remaining_pct=5.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(12),
    )
    assert result.tier is None
    assert result.target_pct is None
    assert result.tier_changed is True
    assert tp.cooldown_remaining_90_sec() == COOLDOWN_SEC


# ----------------------------------------------------------------------
# Cooldown semantics
# ----------------------------------------------------------------------


def test_recross_90_within_cooldown_does_not_promote():
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=90, last_soc=90.5)
    # Demote first to arm the 90% cooldown.
    tp.evaluate(
        soc_pct=89.5,
        cloud_cover_remaining_pct=5.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(12),
    )
    assert tp.tier is None

    # Advance only a few minutes — still inside the 30m cooldown.
    clock.advance(600)
    result = tp.evaluate(
        soc_pct=90.5,
        cloud_cover_remaining_pct=5.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(12, 10),
    )
    assert result.tier is None
    assert result.tier_changed is False


def test_recross_90_after_cooldown_promotes():
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=90, last_soc=90.5)
    tp.evaluate(
        soc_pct=89.5,
        cloud_cover_remaining_pct=5.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(12),
    )
    # Advance past the cooldown.
    clock.advance(COOLDOWN_SEC + 1)
    # last_seen_soc is now 89.5; the next call must establish prev<90, new>=90.
    result = tp.evaluate(
        soc_pct=90.5,
        cloud_cover_remaining_pct=5.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(13),
    )
    assert result.tier == 90
    assert result.tier_changed is True


# ----------------------------------------------------------------------
# Fallbacks
# ----------------------------------------------------------------------


def test_stale_cloud_cover_does_not_promote_or_force_demote():
    """Stale forecast blocks the weather-gated crossing path (SOC < 99)
    and does not force a demotion."""
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=90, last_soc=94.0)
    # Forecast missing entirely; SOC below 99 so unconditional override skips.
    result = tp.evaluate(
        soc_pct=95.5,
        cloud_cover_remaining_pct=None,
        forecast_fresh=False,
        sunset_dt=None,
        now_local=_now_local(12),
    )
    # Stale forecast blocks crossing-based promotion; no forced demote.
    assert result.tier == 90
    assert result.tier_changed is False


def test_past_sunset_returns_none_cloud_and_preserves_tier():
    """Past-sunset is represented by cloud_remaining=None upstream. The tier
    promotion service must preserve the current tier and not promote.
    """
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=90, last_soc=95.0)
    result = tp.evaluate(
        soc_pct=95.5,
        cloud_cover_remaining_pct=None,  # past sunset upstream
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(20),  # past sunset
    )
    assert result.tier == 90
    assert result.tier_changed is False


def test_stale_forecast_blocks_promotion_even_if_cloud_value_present():
    clock = FakeClock()
    tp = _new_tp(clock=clock, last_soc=89.0)
    result = tp.evaluate(
        soc_pct=91.0,
        cloud_cover_remaining_pct=5.0,
        forecast_fresh=False,  # stale
        sunset_dt=_sunset(),
        now_local=_now_local(12),
    )
    assert result.tier is None
    assert result.tier_changed is False


# ----------------------------------------------------------------------
# Restart safety
# ----------------------------------------------------------------------


def test_first_tick_after_restart_cannot_promote():
    """On the first evaluate() after process start, last_seen_soc is None.
    Even with clear skies and a SOC above 90, the first tick MUST NOT
    promote — it just initializes last_seen_soc.
    """
    clock = FakeClock()
    tp = _new_tp(clock=clock, last_soc=None)
    result = tp.evaluate(
        soc_pct=92.0,
        cloud_cover_remaining_pct=3.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(12),
    )
    assert result.tier is None
    assert result.tier_changed is False
    assert result.is_first_tick is True
    assert tp.last_seen_soc == 92.0


def test_second_tick_after_restart_needs_real_crossing():
    """After initialization, the SOC must cross upward through 90 — having
    SOC stay above 90 is not enough."""
    clock = FakeClock()
    tp = _new_tp(clock=clock, last_soc=None)
    # First tick initializes last_seen_soc=92
    tp.evaluate(
        soc_pct=92.0,
        cloud_cover_remaining_pct=3.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(12),
    )
    # Second tick: still 92 — no crossing, must not promote.
    result = tp.evaluate(
        soc_pct=92.0,
        cloud_cover_remaining_pct=3.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(12, 1),
    )
    assert result.tier is None
    assert result.tier_changed is False


def test_first_tick_at_100_soc_promotes_immediately():
    """SOC >= 99 on the very first tick after restart must promote unconditionally.
    The restart-safety init skip does not apply to the full-battery override."""
    clock = FakeClock()
    tp = _new_tp(clock=clock, last_soc=None)  # fresh restart
    result = tp.evaluate(
        soc_pct=100.0,
        cloud_cover_remaining_pct=None,  # no cloud data available
        forecast_fresh=False,
        sunset_dt=None,
        now_local=_now_local(12),
    )
    assert result.tier == 100
    assert result.tier_changed is True
    assert result.is_first_tick is True
    assert tp.last_seen_soc == 100.0


def test_no_soc_returns_unchanged_without_clobbering_state():
    """Missing SOC mid-run must not modify last_seen_soc or tier."""
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=90, last_soc=95.0)
    result = tp.evaluate(
        soc_pct=None,
        cloud_cover_remaining_pct=5.0,
        forecast_fresh=True,
        sunset_dt=_sunset(),
        now_local=_now_local(12),
    )
    assert result.tier == 90
    assert result.tier_changed is False
    assert tp.last_seen_soc == 95.0


# ----------------------------------------------------------------------
# State serialization
# ----------------------------------------------------------------------


def test_to_state_dict_contains_persistable_fields():
    clock = FakeClock()
    tp = _new_tp(clock=clock, tier=100, last90=10.0, last100=20.0, last_soc=99.9)
    snap = tp.to_state_dict()
    assert snap["weather_promotion_tier"] == 100
    assert snap["last_demotion_from_90_ts"] == 10.0
    assert snap["last_demotion_from_100_ts"] == 20.0
    assert snap["last_seen_soc"] == 99.9
