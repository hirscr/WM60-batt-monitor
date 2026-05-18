# Foreman Prompt — Weather-aware tier promotion (replace Priorities 2 & 3)

**Status**: Drafted 2026-05-18 in a FIXER session. Awaiting user approval to
dispatch.

**Background**: The weather_gate (pre-sunrise daily decision) is already
shipped (commit `f5705d0`). This prompt covers the in-tick complement:
weather-aware promotion above the 80% decile cap when conditions warrant it.

---

## CONTEXT

`services/autocontrol_service.py` currently uses three priorities to decide
target power on each 60s tick:

- **Priority 2 — Full Power**: fires when `SOC > 99% AND PV > 3600W` → 100%
- **Priority 3 — High SOC Conservative**: fires when `SOC > 90% AND PV < 3600W` → 90%
- **Priority 5 — Normal Discharge Tiers**: decile table, **maxes at 80%**

Result today: at SOC 93.6% with strong solar (PV 9 kW), neither Priority 2
nor Priority 3 fires (SOC isn't > 99, PV is too high for Priority 3's
"weak-solar conservative" semantics), so the system falls through to
Priority 5 and caps at 80%. The user wants 90% in this situation.

Priorities 2 and 3 also use **instantaneous PV power** as the gate, which is
volatile minute-to-minute. The user wants the gate to use a forecast-based
signal (cloud cover) so the tier doesn't flap with passing clouds.

## SCOPE OF THIS TASK

Replace Priorities 2 and 3 with new weather-aware promotion logic, driven
by remaining-daylight mean cloud cover from the existing WeatherService.
Extend WeatherService to fetch and expose that value. Add the necessary
state, cooldown, and persistence. Update tests.

## PART 1 — EXTEND THE WEATHER SERVICE

In `services/weather_service.py`:

- Add a second Open-Meteo query (or extend the existing query) to fetch
  hourly `cloud_cover` for today (the hourly endpoint parameter is the same
  family of calls).
- Add a derived field: `cloud_cover_remaining_daylight_pct`. Definition:
  the simple arithmetic mean of hourly cloud cover for every full hour
  from "now" through "today's sunset hour" inclusive. If "now" is past
  sunset, this value is None.
- Include this field in the dict returned by `get_today_forecast()`.
- Refresh on the same cadence as the existing daily forecast. Reuse the
  same freshness window.
- If the hourly data is missing or malformed, set
  `cloud_cover_remaining_daylight_pct` to None (do not raise).

In `api/weather.py`:

- Include `cloud_cover_remaining_daylight_pct` in the `forecast` block of
  the `GET /api/weather/status` response.

## PART 2 — REPLACE PRIORITIES 2 AND 3

In `services/autocontrol_service.py`:

Delete the existing Priority 2 and Priority 3 code blocks. Replace with a
single new priority block (call it Priority 2/3 unified or rename
appropriately) that implements the rules below.

### Tier-promotion rules

The promotion check runs on every autocontrol tick, after the
battery-freshness check and weather_gate check, but before the
existing Priority 5 (decile tiers) fallthrough.

**Promotion (one-shot at SOC upward crossing):**
- On any tick where SOC has just crossed upward through 90% (was below
  90% on the previous tick or last-known value, is at or above 90% now),
  AND `cloud_cover_remaining_daylight_pct` is fresh AND `< 10`,
  AND current local time is more than 3 hours before today's sunset,
  AND the 90% cooldown is not active (see below):
    → promote to 90% tier.

- On any tick where the system is at 90% tier AND SOC has just crossed
  upward through 99% (was below 99% on the previous tick, is at or above
  99% now), AND `cloud_cover_remaining_daylight_pct` is fresh AND `< 10`,
  AND current local time is more than 3 hours before today's sunset,
  AND the 100% cooldown is not active:
    → promote to 100% tier.

**Demotion (SOC-driven, no cloud cover check):**
- If at 100% tier AND SOC drops below 99% → demote to 90% tier; set the
  100% cooldown timestamp to now.
- If at 90% tier AND SOC drops below 90% → demote to Priority 5 (which
  will resolve to 80% if SOC ≥ 80, etc.); set the 90% cooldown timestamp
  to now.

**Cooldown:**
- After demoting from 90%, do not re-promote to 90% for 30 minutes.
- After demoting from 100%, do not re-promote to 100% for 30 minutes.

**Fallbacks (no forced demotion in any of these cases):**
- If `cloud_cover_remaining_daylight_pct` is None or stale, or
- If the WeatherService is unreachable, or
- If "now" is past today's sunset (remaining-daylight window empty):
  → block promotion; tier already in effect is unchanged.

**Boundary semantics:**
- "SOC crosses upward through 90%" means `prev_soc < 90 AND new_soc >= 90`.
- "SOC crosses upward through 99%" means `prev_soc < 99 AND new_soc >= 99`.
- "More than 3 hours before sunset" means
  `(sunset_dt - now_dt).total_seconds() > 10800`.

### State to add to AutoControlService

- `weather_promotion_tier` — int or None: `None` / `90` / `100`. Source
  of truth for the current weather-driven tier override. `None` means
  the system is on Priority 5.
- `last_demotion_from_90_ts` — float (monotonic seconds) or 0.0.
- `last_demotion_from_100_ts` — float or 0.0.
- `last_seen_soc` — float, used to detect upward crossings.

All four must persist across restarts via `utils/state_manager.py`
alongside existing autocontrol state. After a restart, treat
`last_seen_soc` as the value at restart (so the first tick after restart
cannot trigger a promotion by definition — promotion requires an
*upward crossing*).

### Computing the target

When `weather_promotion_tier` is set, override Priority 5's decile lookup:
- `weather_promotion_tier == 100` → `target_pct = 100`, `target_w = base_watts`
- `weather_promotion_tier == 90` → `target_pct = 90`, `target_w = int(base_watts * 0.9)`
- `weather_promotion_tier is None` → fall through to Priority 5 (decile table)

`current_state_description` should reflect the active tier — e.g.
"Solar boost at 90% (clear skies)" or "Full power on clear day."

### Logging

One concise log line per tick when the tier-promotion code path makes a
*decision change* (promote, demote, or cooldown-blocked). Use tag
`[AutoControl][TIER_PROMO]`. Do NOT log on every tick — only on state
transitions. Use the project's two-layer gate (compile-time + yaml).
Add the tag to `logs/logging.yaml`, default true while feature is new.

## PART 3 — API + DASHBOARD

In `/api/autocontrol/status`, add a `tier_promotion` block:

```
"tier_promotion": {
  "tier": null | 90 | 100,
  "cooldown_remaining_90_sec": int,
  "cooldown_remaining_100_sec": int,
  "last_seen_soc": float
}
```

In the dashboard weather card (or miner control card — Foreman's call,
whichever fits best visually), surface the active tier-promotion status:
"Tier: 80% / 90% / 100%" with a note like "promoted at 14:32 — clear sky,
4h to sunset." When demoted with cooldown active, show "Cooldown: Nm
remaining before re-promotion."

## PART 4 — TESTS

In `tests/`:

- Extend `tests/test_weather_gate_logic.py` (or add a new
  `tests/test_tier_promotion_logic.py`) with pure-logic tests for the
  promotion / demotion / cooldown rules. Cover:
  - SOC crossing 90% with clear skies and 4h to sunset → promote
  - SOC crossing 90% with cloudy skies → no promote
  - SOC crossing 90% with 2h to sunset → no promote
  - SOC crossing 99% from 90% tier with clear skies and 4h to sunset → promote
  - SOC drops 99% → 98.5% while at 100% tier → demote to 90%
  - SOC drops 90% → 89.5% while at 90% tier → demote to 80%, cooldown armed
  - SOC re-crosses 90% within cooldown → no promote
  - SOC re-crosses 90% after cooldown expires → promote
  - Stale cloud cover data → no promote, no forced demote
  - Past sunset → no promote, current tier preserved
  - Restart: first tick after restart does not promote (crossing not detected)

- Mock the WeatherService and time.monotonic in these tests; no live network.

- Extend `tests/test_weather_config_api.py` if any new config keys are
  added (none planned, but check). At minimum, add a smoke test that
  `GET /api/autocontrol/status` includes the `tier_promotion` block.

## PART 5 — VERIFICATION

Before declaring done:

1. `pytest tests/` — all existing tests still pass; new tests pass.
2. `grep -nE "Priority 2|Priority 3|max_pv_power.*Priority" services/autocontrol_service.py` returns no obsolete references to the old PV-gated priorities (or they have been clearly relabeled).
3. With Flask running locally on Mac:
   - `GET /api/weather/status` returns `cloud_cover_remaining_daylight_pct`.
   - `GET /api/autocontrol/status` returns the new `tier_promotion` block.
4. Code-path inspection: verify that promotion only fires on upward SOC
   crossings (not on every tick where SOC > 90%), and demotion is symmetric.
5. Confirm no changes to the battery-stale safety gate, the emergency_soc
   gate, or the weather_gate daily logic.
6. Do NOT deploy to the Pi as part of this task. The user will deploy after
   review.

## CONSTRAINTS

- Do not bypass any existing safety gate. Tier promotion is an additive
  upper bound on the Priority 5 result; the weather_gate, battery_stale,
  and emergency_soc gates still take precedence.
- Do not change Priority 5's decile table.
- Each tier change is a power command that risks the known firmware quirk
  (see STATE.md "User Power toggle flips OFF" entry). Keep tier changes
  rare by design: one-shot at crossing + 30-min cooldown. Do not introduce
  any code path that re-fires the promotion logic on every tick when
  already in the promoted tier.
- SOLID, 3-4 level nesting max. 300-line cap applies to new files only.
- No hardcoded credentials. Open-Meteo is keyless.
- All new logs use the compile-time + yaml two-layer gate writing to
  `logs/debug.log`.

## GOALS

- WeatherService fetches Open-Meteo hourly cloud cover and exposes
  `cloud_cover_remaining_daylight_pct` (mean from now through sunset),
  returning None when past sunset or data is missing.
- `/api/weather/status` includes that field; mocked unit tests cover the
  mean-over-remaining-daylight computation.
- Priorities 2 and 3 in `services/autocontrol_service.py` are deleted and
  replaced by a single tier-promotion block implementing the rules above.
- Promotion is one-shot at upward SOC crossing of 90% or 99%, gated by
  `cloud_cover_remaining_daylight_pct < 10` AND `>3h before sunset` AND
  cooldown inactive.
- Demotion is SOC-driven only: 100→90 when SOC < 99, 90→Priority 5 when
  SOC < 90. Each demotion arms a 30-minute cooldown for that tier.
- Stale forecast / past-sunset / unreachable WeatherService blocks
  promotion but never forces demotion.
- New state (`weather_promotion_tier`, cooldown timestamps, `last_seen_soc`)
  persists via `utils/state_manager.py`.
- `/api/autocontrol/status` includes a `tier_promotion` block with current
  tier, both cooldown remainders, and last_seen_soc.
- Dashboard surfaces the active tier-promotion status and cooldown.
- All new tests pass; existing tests still pass.
- No deployment to the Pi in this task.
