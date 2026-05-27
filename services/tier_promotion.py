"""Weather-aware tier promotion logic.

Drives the 90%/100% boost tier above the decile table. Pure decision
logic with NO I/O — state persists via caller's state_manager.

Promotion (one-shot, upward SOC crossing required):
  - prev<90 and new>=90, clear skies, >3h before sunset, no cooldown -> 90
  - At 90, prev<99 and new>=99, same conditions -> 100

Unconditional full-power override:
  - SOC >= 99% -> 100%, regardless of cloud cover, crossing, or time.
    A full battery has nowhere to store more energy; run at full power.
    Applies on the first tick too, bypassing the restart-safety init skip.

Demotion (SOC-only):
  - At 100, SOC<99 -> 90, arms 100% cooldown
  - At 90,  SOC<90 -> None (fall through to decile), arms 90% cooldown

Fallbacks block promotion but never force demotion: missing cloud,
stale forecast, past sunset, missing sunset_dt, weather unreachable.

Restart safety: last_seen_soc starts as None; the first evaluate()
records SOC and skips the crossing check (except the SOC>=99 override).
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


# Promotion thresholds
SOC_PROMOTE_90 = 90.0
SOC_PROMOTE_100 = 99.0

# Demotion thresholds (SOC drops below these)
SOC_DEMOTE_100 = 99.0
SOC_DEMOTE_90 = 90.0

# Cloud / time gates
CLOUD_COVER_MAX_PCT = 10.0
MIN_HOURS_BEFORE_SUNSET = 3.0

# Cooldown after each demotion (seconds). 30 minutes is the spec.
COOLDOWN_SEC = 30 * 60


@dataclass
class TierEvaluation:
    """Result of one evaluate() call.

    `tier_changed` is True iff this call moved the tier (in either direction).
    Callers use this to decide whether to issue a power command — power is
    only sent on a tier change.

    `target_pct` is the recommended power percentage for this tier:
        100 -> 100%
         90 -> 90%
       None -> fall through to Priority 5 (decile table)
    `description` is a short human-readable status for the dashboard.
    `is_first_tick` is True iff this is the very first evaluate() after
    restart (last_seen_soc was None entering the call).
    """
    tier: Optional[int]
    target_pct: Optional[int]
    tier_changed: bool
    description: str
    is_first_tick: bool


class TierPromotion:
    """Holds tier state + cooldowns and decides one tick at a time.

    Pure logic. The clock is injectable via `now_monotonic` so tests can
    drive cooldowns without sleeping. State persistence is the caller's
    responsibility — TierPromotion.to_state_dict() / from_state_dict()
    serialize the persistable fields.
    """

    def __init__(
        self,
        *,
        tier: Optional[int] = None,
        last_demotion_from_90_ts: float = 0.0,
        last_demotion_from_100_ts: float = 0.0,
        last_seen_soc: Optional[float] = None,
        now_fn=time.time,
    ):
        if tier not in (None, 90, 100):
            raise ValueError(f"tier must be None, 90, or 100; got {tier!r}")
        self.tier: Optional[int] = tier
        self.last_demotion_from_90_ts: float = float(last_demotion_from_90_ts)
        self.last_demotion_from_100_ts: float = float(last_demotion_from_100_ts)
        self.last_seen_soc: Optional[float] = (
            float(last_seen_soc) if last_seen_soc is not None else None
        )
        self._now = now_fn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(
        self,
        *,
        soc_pct: Optional[float],
        cloud_cover_remaining_pct: Optional[float],
        forecast_fresh: bool,
        sunset_dt: Optional[datetime],
        now_local: Optional[datetime],
    ) -> TierEvaluation:
        """Run one tier-promotion tick.

        Args:
            soc_pct: current battery SOC (0..100) or None.
            cloud_cover_remaining_pct: mean cloud cover from now through
                sunset, or None when past sunset / data missing.
            forecast_fresh: True iff the weather snapshot is inside its
                freshness window.
            sunset_dt: today's sunset in local time, or None.
            now_local: wallclock 'now' in the same timezone as sunset_dt.

        Returns:
            TierEvaluation describing the new tier + whether it changed.
        """
        prev_tier = self.tier
        is_first_tick = self.last_seen_soc is None

        # No SOC -> we cannot decide anything safely. Block promotion AND
        # block demotion (we have no evidence SOC dropped). Leave state
        # untouched; report no change.
        if soc_pct is None:
            return self._unchanged(
                description=self._describe(prev_tier, suffix=" (no SOC)"),
                is_first_tick=is_first_tick,
            )

        # Full-battery override: SOC >= 99% means the battery can't absorb
        # more energy, so run at full power unconditionally. Bypasses cloud
        # cover, crossing requirement, time-before-sunset, and the first-tick
        # init skip. Sets last_seen_soc so the init guard is satisfied for
        # subsequent ticks.
        if float(soc_pct) >= SOC_PROMOTE_100 and self.tier != 100:
            self.last_seen_soc = float(soc_pct)
            return TierEvaluation(
                tier=100,
                target_pct=100,
                tier_changed=(prev_tier != 100),
                description="Full power — battery full",
                is_first_tick=is_first_tick,
            )

        # Initialize the running SOC on the first tick. Skip the crossing
        # check this tick so a restart cannot trigger a false promotion.
        if is_first_tick:
            self.last_seen_soc = float(soc_pct)
            return TierEvaluation(
                tier=prev_tier,
                target_pct=self._tier_to_pct(prev_tier),
                tier_changed=False,
                description=self._describe(prev_tier, suffix=" (init)"),
                is_first_tick=True,
            )

        prev_soc = self.last_seen_soc  # never None past the guard above
        # Update last_seen_soc AFTER we use prev_soc for the crossing test.
        new_soc = float(soc_pct)

        # ------------------------------------------------------------------
        # DEMOTION first (SOC-driven, takes precedence over promotion).
        # ------------------------------------------------------------------
        if self.tier == 100 and new_soc < SOC_DEMOTE_100:
            self.tier = 90
            self.last_demotion_from_100_ts = self._now()
            self.last_seen_soc = new_soc
            return TierEvaluation(
                tier=self.tier,
                target_pct=90,
                tier_changed=True,
                description="Solar boost at 90% (demoted from 100%)",
                is_first_tick=False,
            )

        if self.tier == 90 and new_soc < SOC_DEMOTE_90:
            self.tier = None
            self.last_demotion_from_90_ts = self._now()
            self.last_seen_soc = new_soc
            return TierEvaluation(
                tier=None,
                target_pct=None,
                tier_changed=True,
                description="Tier promotion released (fall through to Priority 5)",
                is_first_tick=False,
            )

        # ------------------------------------------------------------------
        # PROMOTION (one-shot at upward SOC crossings).
        # ------------------------------------------------------------------
        cloud_ok = (
            cloud_cover_remaining_pct is not None
            and forecast_fresh
            and cloud_cover_remaining_pct < CLOUD_COVER_MAX_PCT
        )
        time_ok = self._hours_before_sunset(now_local, sunset_dt) > MIN_HOURS_BEFORE_SUNSET

        # 90 -> 100 crossing
        if (
            self.tier == 90
            and prev_soc < SOC_PROMOTE_100 <= new_soc
            and cloud_ok
            and time_ok
            and not self._cooldown_active(self.last_demotion_from_100_ts)
        ):
            self.tier = 100
            self.last_seen_soc = new_soc
            return TierEvaluation(
                tier=100,
                target_pct=100,
                tier_changed=True,
                description="Full power on clear day",
                is_first_tick=False,
            )

        # None -> 90 crossing (only fires when not already at 90 or 100)
        if (
            self.tier is None
            and prev_soc < SOC_PROMOTE_90 <= new_soc
            and cloud_ok
            and time_ok
            and not self._cooldown_active(self.last_demotion_from_90_ts)
        ):
            self.tier = 90
            self.last_seen_soc = new_soc
            return TierEvaluation(
                tier=90,
                target_pct=90,
                tier_changed=True,
                description="Solar boost at 90% (clear skies)",
                is_first_tick=False,
            )

        # No transition: just record the SOC and report unchanged.
        self.last_seen_soc = new_soc
        return self._unchanged(
            description=self._describe(self.tier),
            is_first_tick=False,
        )

    def cooldown_remaining_90_sec(self) -> int:
        return self._cooldown_remaining(self.last_demotion_from_90_ts)

    def cooldown_remaining_100_sec(self) -> int:
        return self._cooldown_remaining(self.last_demotion_from_100_ts)

    def to_state_dict(self) -> dict:
        """Persistable snapshot for state_manager.save()."""
        return {
            "weather_promotion_tier": self.tier,
            "last_demotion_from_90_ts": self.last_demotion_from_90_ts,
            "last_demotion_from_100_ts": self.last_demotion_from_100_ts,
            "last_seen_soc": self.last_seen_soc,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _unchanged(self, *, description: str, is_first_tick: bool) -> TierEvaluation:
        return TierEvaluation(
            tier=self.tier,
            target_pct=self._tier_to_pct(self.tier),
            tier_changed=False,
            description=description,
            is_first_tick=is_first_tick,
        )

    @staticmethod
    def _tier_to_pct(tier: Optional[int]) -> Optional[int]:
        if tier == 100:
            return 100
        if tier == 90:
            return 90
        return None

    @staticmethod
    def _describe(tier: Optional[int], suffix: str = "") -> str:
        if tier == 100:
            return "Full power on clear day" + suffix
        if tier == 90:
            return "Solar boost at 90% (clear skies)" + suffix
        return "Tier promotion inactive" + suffix

    @staticmethod
    def _hours_before_sunset(
        now_local: Optional[datetime],
        sunset_dt: Optional[datetime],
    ) -> float:
        if now_local is None or sunset_dt is None:
            return -1.0
        try:
            return (sunset_dt - now_local).total_seconds() / 3600.0
        except Exception:
            return -1.0

    def _cooldown_active(self, last_ts: float) -> bool:
        if last_ts <= 0:
            return False
        return (self._now() - last_ts) < COOLDOWN_SEC

    def _cooldown_remaining(self, last_ts: float) -> int:
        if last_ts <= 0:
            return 0
        remaining = COOLDOWN_SEC - (self._now() - last_ts)
        return int(remaining) if remaining > 0 else 0
