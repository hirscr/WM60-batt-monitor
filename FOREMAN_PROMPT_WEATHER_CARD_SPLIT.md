# Foreman Prompt — Split Weather Gate card into two side-by-side cards

**Status**: Drafted 2026-05-18 in a FIXER session, signed off by user via
visual companion brainstorming. Awaiting dispatch.

---

## CONTEXT

The current Weather Gate card (`<div class="card" id="weatherCard">` in
`static/index.html`) packs both the day's forecast/decision AND the
editable parameters into a single full-width card. The parameter input
fields are unnecessarily wide and the whole card is tall, pushing other
content below the fold on a laptop. User wants the parameter inputs to be
narrow (≈5 characters), and the card split into two side-by-side cards on
wide viewports — forecast/decision on the left, parameters on the right.

This is a pure UI rearrangement. **No backend changes. No new data. No
API changes. No new state.** Only HTML/CSS/JS-DOM updates.

Read CLAUDE.md, STATE.md, and CONTINUE.md before touching code.

## REFERENCE — what exists today

- `static/index.html` around lines 151–209 — the `weatherCard` markup with
  two `weather-section` blocks (Today's forecast + Parameters).
- `static/css/dashboard.css` around lines 572–668 — the
  `#weatherCard .weather-*` rules including `.weather-config-grid` which
  currently uses `repeat(2, minmax(160px, 1fr))` (the source of the wide
  inputs).
- `static/js/dashboard.js` — functions `updateWeatherStatus()`,
  `saveWeatherConfig()`, `forceEvaluateWeatherGate()`, and the element IDs
  they read/write (`weatherSunrise`, `weatherSunset`, `weatherCloudCover`,
  `weatherMaxForDay`, `weatherExpected`, `weatherSoc`, `weatherDeficit`,
  `weatherEvaluatedAt`, `weatherDecisionBanner`, `weatherFreshness`,
  `weatherEvaluateBtn`, `weatherCfgEnabled`, `weatherCfgBatteryKwh`,
  `weatherCfgSummerKwh`, `weatherCfgWinterKwh`, `weatherCfgSafetyFactor`,
  `weatherCfgPreSunriseMin`, `weatherCfgRecoverySoc`,
  `weatherCfgRecoveryHours`, `weatherSaveStatus`).

## TARGET DESIGN

Replace the single `weatherCard` with TWO cards inside a wrapper that
controls the side-by-side layout.

### LEFT card — id `weatherForecastCard`, title "Weather Gate"

Contains everything that is read-only / decision-related plus the "Force
re-evaluate" button in the header:

- Header row: `<h2>Weather Gate</h2>` and the `Force re-evaluate` button
  (id `weatherEvaluateBtn`).
- The current Section A forecast grid (sunrise, sunset, cloud cover, max
  for day, expected energy, current SOC, deficit, last evaluated). Keep
  all existing element IDs unchanged.
- Decision banner (id `weatherDecisionBanner`) with its existing
  `weather-decision-*` color classes intact.
- Freshness line (id `weatherFreshness`).

Drop the `<h3>Today's forecast</h3>` sub-heading — with the card title
now solely on this content, the sub-heading is redundant.

### RIGHT card — id `weatherParamsCard`, title "Parameters"

Contains the editable form:

- Header: `<h2>Parameters</h2>`.
- All 8 inputs in their existing order:
    1. Master enable (checkbox, id `weatherCfgEnabled`)
    2. Battery total (kWh) — `weatherCfgBatteryKwh`
    3. Summer max (kWh) — `weatherCfgSummerKwh`
    4. Winter max (kWh) — `weatherCfgWinterKwh`
    5. Safety factor (1.0–2.0) — `weatherCfgSafetyFactor`
    6. Pre-sunrise window (min) — `weatherCfgPreSunriseMin`
    7. Recovery SOC (%) — `weatherCfgRecoverySoc`
    8. Min hours before sunset for recovery — `weatherCfgRecoveryHours`
- Save row: button (existing `saveWeatherConfig()` onclick) and status
  span (id `weatherSaveStatus`).

Drop the `<h3>Parameters</h3>` sub-heading inside the section — same
reason, redundant with the card title.

### Input width

Numeric inputs must be sized so that at most ~5 characters fit
comfortably. Use a fixed width via CSS (suggest `width: 5.5em` or
`max-width: 5.5em`) on `#weatherParamsCard input[type="number"]`. The
checkbox stays its natural size.

Use a "label-on-left, input-on-right" row layout inside the params card —
each row is a flexbox with the label expanding to fill space and the
input pinned to the right at its fixed narrow width. This keeps the card
itself narrow and the inputs visually consistent.

### Wrapper + breakpoint

Create a wrapper element (suggest `<div class="weather-pair">`) that
contains both new cards in source order: forecast card first, params card
second.

CSS behavior:
- Viewport ≥ 900px wide: `weather-pair` is `display: grid` with
  `grid-template-columns: minmax(0, 1fr) auto` OR equivalent — the
  forecast card fills available space and the parameter card sizes to
  its content. Gap matches the existing 15px between cards.
- Viewport < 900px: `weather-pair` collapses to a single column; both
  cards stack vertically and span full width.

The wrapper replaces the existing `weatherCard` block in source order.
Surrounding cards (the empty row above and Network Devices below) must
not move.

## CSS HOUSEKEEPING

- Remove all `#weatherCard .weather-config-grid`,
  `#weatherCard .weather-save-row`, and any other rules scoped to the old
  card id that no longer have a target.
- Migrate the still-needed rules (decision banner colors, freshness line,
  forecast grid) to be scoped to the new card IDs.
- Keep the `@media (max-width: 600px)` mobile rule's effect (stacked
  inputs in one column) — but now it applies to the params card alone.

## JS

`static/js/dashboard.js` should require **no functional changes** because
all element IDs are preserved. The only thing to double-check: if any
code path queries `#weatherCard` or `.weather-section` to scope a lookup,
update it to the new wrapper or use the IDs directly.

Verify by grepping for `weatherCard`, `weather-section`, `weather-grid`,
`weather-config-grid`, and `weather-save-row` in the JS and CSS. Each hit
must either be migrated to the new structure or deleted.

## VERIFICATION

Before declaring done:

1. `pytest tests/` — all existing tests still pass.
2. Run Flask locally on Mac (`python3 app.py`). Open the dashboard.
3. Resize the browser window across the 900px boundary:
   - Above 900px: forecast card left, parameters card right.
   - Below 900px: cards stack, forecast on top.
4. Confirm in the rendered DOM that all original element IDs exist exactly
   once (no duplicates, no rename drift). Use the browser dev tools
   console: `['weatherSunrise','weatherCfgBatteryKwh',...].every(id =>
   document.getElementById(id) !== null)`.
5. Confirm GET `/api/weather/status` populates the forecast values
   correctly (sunrise, sunset, cloud cover, etc.).
6. Confirm POST `/api/weather/config` still works:
   change Battery total to a slightly different number, click Save,
   verify the save status text updates and the value reloads after page
   refresh.
7. Confirm Force re-evaluate button still triggers POST
   `/api/weather/evaluate_now` and refreshes the card.
8. Confirm the decision banner still shows the right color (green for
   sufficient, amber for insufficient, blue for recovered, neutral
   otherwise). Don't need to trigger every state — visually inspect the
   current one and grep for the class names to confirm they're still
   applied conditionally.
9. Confirm input width: every numeric input renders ~5 chars wide.

## CONSTRAINTS

- Do NOT change any backend code, API, or weather/autocontrol service logic.
- Do NOT touch any other card on the dashboard.
- Do NOT rename any element ID consumed by JS.
- Do NOT change the data shown or its order; only the visual arrangement.
- Match the existing card visual style (border, padding, shadow). The new
  cards should look like every other card on the page.
- Per CLAUDE.md UI Change Policy, this change has explicit user approval
  via brainstorming session 2026-05-18.
- SOLID / file-size rules: no new files needed for this change; edits to
  static/index.html, static/css/dashboard.css are within scope.
- No new JS file. If a tiny JS adjustment is needed, edit the existing
  dashboard.js.

## GOALS

- `#weatherCard` no longer exists in the DOM. In its place:
  `#weatherForecastCard` and `#weatherParamsCard`, both with the standard
  `.card` class, wrapped by `.weather-pair`.
- Viewports ≥ 900px display the two cards side-by-side; viewports < 900px
  stack them with the forecast card on top.
- Every numeric input inside the parameters card renders at ~5-character
  width.
- Every pre-existing element ID consumed by dashboard.js is preserved
  exactly once in the new DOM.
- GET `/api/weather/status`, POST `/api/weather/config`, and POST
  `/api/weather/evaluate_now` all continue to populate / accept input
  correctly with no JS changes (or with only minimal scope-related JS
  changes that preserve behavior).
- Decision banner colors continue to work for all four states.
- All existing tests pass; no new tests required for this UI-only change.
- No deployment to the Pi as part of this task.
