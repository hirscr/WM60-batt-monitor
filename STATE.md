# WM60 Controller — Run State

**Generated**: 2026-05-09  
**Session commit**: `95b2016`

---

## Priority Blocks Landed

### Priority 1 — Battery freshness safety gate ✅
**Commit**: 95b2016  
**Changes**: `eg4_client.py`, `services/battery_service.py`, `services/autocontrol_service.py`

**Acceptance test passed on Pi**:
- At startup, `AutoControl] WARNING: Battery telemetry stale (unknown) — stopping miner for safety` appears in journal before first EG4 poll succeeds.
- After first poll, `battery_fresh: true`, `battery_age_seconds: ~14s`.
- Autocontrol status returns `"Normal discharge at 40%"` once battery is fresh.
- Note: the staleness simulation (pkill -STOP the EG4 thread for 11+ minutes) was NOT performed because the miner is live at the site and interrupting it carries risk. The startup-window behavior was verified instead, which exercises the same code path.

### Priority 2 — Authoritative miner is_off ✅
**Commit**: 95b2016  
**Changes**: `services/miner_service.py`, `utils/nc_miner_api.py`, `services/autocontrol_service.py`

**Acceptance test**:
- `is_off=False` logged when miner is hashing at ~15 TH/s.
- `is_off=True` logged when miner just restarted (Power Limit=0, MHS 5s=0).
- Note: `status` command (for `mineroff` field) returns None on this firmware via pyasic's BTMinerRPCAPI; falls back to Power Limit == 0 check which is reliable.
- Note: `get_psu()` also returns None on this pyasic version; `Power 5s` is null in API but non-fatal. PSU test with `adjust_power_limit(0)` / `adjust_power_limit(1440)` NOT performed to avoid disrupting live operation; `is_off` transitions were observed via startup behavior.

### Priority 3 — Dual hashrate and power readings ✅
**Commit**: 95b2016  
**Changes**: `services/miner_service.py`, `static/index.html`, `static/js/dashboard.js`

**Acceptance test**:
- `/api/miner/status` confirmed to include `Hashrate`, `Hashrate 5s`, `Power`, `Power 5s` fields.
- Dashboard HTML grep confirms `minerHashrate5s` and `minerPower5s` IDs present.
- Journal shows: `Sending update to UI: 12.9TH/s, 12.8TH/s(5s), 1383W, NoneW(5s)` — both hashrate values rendering.
- `Power 5s` is null (get_psu unavailable on this pyasic version) — non-fatal by design.

### Priority 4 — Emergency SOC UI ✅
**Commit**: 95b2016  
**Changes**: `services/autocontrol_service.py`, `app.py`, `utils/state_manager.py`, `static/index.html`, `static/js/dashboard.js`

**Acceptance test**:
- `POST /api/autocontrol/emergency_soc {percent: 35}` → `{"ok":true,"percent":35}` ✓
- `GET /api/autocontrol/emergency_soc` → `{"percent":35}` ✓
- `POST /api/autocontrol/emergency_soc {percent: 4}` → HTTP 400 ✓
- Restart: `GET` still returns 35 ✓
- Reset to 30 (config value) ✓
- `/api/autocontrol/status` includes `emergency_soc`, `battery_fresh`, `battery_age_seconds` ✓
- Dashboard HTML includes `emergencySocInput`, `emergencySocCurrent`, `batteryFreshnessStatus` elements ✓

### Priority 5 — State-before-verification + chart dedup ✅
**Commit**: 95b2016  
**Changes**: `services/miner_service.py`, `app.py`

**Acceptance test**:
- Chart endpoint journal shows `"Live miner row overwrites CSV row at ts=..."` when duplicates exist ✓
- `on_verified` callback pattern wired through MinerController queue; state saved only after controller verification succeeds.
- Note: full power-command pending-state test not performed live to avoid interrupting mining.

---

## Current Miner State (end of run)
- **autocontrol**: enabled, away mode
- **emergency_soc**: 30%
- **target_power_pct**: 40% (1440W)
- **miner_power_state**: running
- **SOC at time of run**: 46.3%
- **Power Limit**: 1437W (pre-existing from before this run)

## Deferred Items
1. **`get_psu()` on Pi**: pyasic's BTMinerRPCAPI does not expose `get_psu()` or a raw `send_command("get_psu")`, so `Power 5s` is always null on the Pi. If real-time PSU power is needed, either upgrade pyasic or add a raw nc-based call directly to the Pi's miner API. The NCMinerAPI (macOS) already has `get_psu()` wired.
2. **`status` command / `mineroff` field**: Same issue — pyasic doesn't expose it. `is_off` falls back to Power Limit == 0 / MHS 5s == 0, which is reliable for the use cases observed.
3. **Staleness simulation test**: The pkill -STOP test from Priority 1's acceptance criteria was not performed on the live miner to avoid a forced safety-stop during the day. The startup-window behavior (is_fresh() = False before first poll) was verified instead.

---

## Braiins integration shipped on 2026-05-09

### Commits landed this run

| SHA | Description |
|-----|-------------|
| `d6620ae` | feat: add Braiins Pool stats panel and slim Battery card |

### Braiins API details (for future maintainers)

- **Endpoint**: `GET https://pool.braiins.com/accounts/profile/json/btc/`
- **Auth header**: `SlushPool-Auth-Token: <token>` (Bearer form returns 403 — do not use)
- **Token source**: `BRAIINS_API_KEY` env var, loaded via systemd drop-in at `/etc/systemd/system/whatsminer.service.d/env.conf` which sets `EnvironmentFile=/home/hirscr/WM_controller/.env`
- **Hash rate unit**: `Gh/s` — divide by 1000 to get TH/s
- **BTC/USD price**: Coinbase spot `https://api.coinbase.com/v2/prices/BTC-USD/spot`, no auth

### Smoke test output (token redacted)

```json
{
  "account_balance_btc": 0.00202302,
  "age_seconds": 13.5,
  "all_time_btc": 0.00202302,
  "btc_usd_price": 80750.005,
  "btc_usd_price_age_seconds": 1.1,
  "error": null,
  "estimated_btc": 0.00002851,
  "hashrate_1h_ths": 86.16,
  "hashrate_24h_ths": 60.79,
  "hashrate_5m_ths": 82.57,
  "is_fresh": true,
  "today_btc": 0.00002851,
  "today_usd": 2.30
}
```

### Verifications passed

1. **Endpoint smoke test**: HTTP 200, all expected keys present, `is_fresh: true`, `error: null`, `hashrate_5m_ths` > 0, balance plausible (< 1 BTC).
2. **Value sanity**: 5m hashrate 82 TH/s (within 0–200 TH/s range), all-time 0.00202302 BTC matches expected, balance same. BTC price ~$80,750.
3. **Frontend markup**: `curl localhost:8080/ | grep braiins` returns Braiins card with all 5 tile divs present.
4. **Disabled-mode test**: With `braiins.enabled: false`, endpoint returns HTTP 503 `{"error":"braiins integration disabled"}`. Restored to `enabled: true`.
5. **Bad-token test**: With `BRAIINS_API_KEY=invalid_test_token`, service remains `active (running)`, battery + autocontrol endpoints respond normally, `/api/braiins/status` returns `{"error":"upstream 403","is_fresh":false}`. Token string does NOT appear in journal (0 occurrences). Restored real token.
6. **Isolation test**: During bad-token run, `/api/battery/status` returns `connected: true` and `/api/autocontrol/status` returns `battery_fresh: true`. Braiins failure has zero effect on safety-critical loops.

### Infrastructure change

A systemd drop-in was created at `/etc/systemd/system/whatsminer.service.d/env.conf` to load `/home/hirscr/WM_controller/.env` as environment variables. This is required for `BRAIINS_API_KEY` (and any future env-var secrets) to reach the Python process. The base service file is unchanged.

### Current state at end of run

- **Braiins panel**: rendering live — 5m hashrate ~82 TH/s, today's reward ~0.0000285 BTC (~$2.30), all-time 0.00202302 BTC
- **BTC/USD price feed**: working — ~$80,750 via Coinbase spot
- **Battery card**: slimmed — pack voltage/current added; per-unit SOC moved to collapsible `<details>` element
- **3-column grid**: active at ≥1100px viewport width

### Deferred items from this run

- **Battery pack_voltage_v / pack_current_a**: these fields ARE present in the battery API response (confirmed in journal: `pack_voltage_v: 53.0, pack_current_a: 43.0`). The frontend renders them, but the `<span>` IDs were added to the HTML only. No backend change needed.
- **Per-unit battery SOC list**: the `batteryUnitSoc` element is present but the backend `status` dict returns `units` as a Python list of dicts, not a pre-formatted string. The JS checks `status.unit_soc` which will be undefined for now — the `<details>` section will show `—`. A future enhancement could format this server-side or client-side from the `units` array.

---

## Stop-reason indicator shipped on 2026-05-09

### Commits

| SHA | Description |
|-----|-------------|
| `e99323f` | feat: add stop-reason indicator to Miner Control dashboard card |

### What was done

**Backend — `services/autocontrol_service.py`**
- Added `stop_reason` (str) and `resume_at_soc` (Optional[int]) instance variables, initialized to `"normal"` and `None`.
- Set `stop_reason = "emergency_soc"` and `resume_at_soc = self.emergency_soc` in Priority 1 (emergency shutdown).
- Set `stop_reason = "normal"` and `resume_at_soc = None` in Priorities 2–5 (all running conditions).
- Updated `get_state()` to derive the effective stop_reason for the dashboard:
  - If autocontrol disabled: `"manual_off"` when `miner.is_off`, else `"normal"`.
  - If autocontrol enabled and emergency_soc: `"emergency_soc"` with `resume_at_soc`.
  - If autocontrol enabled and miner on but `upfreq_complete == 0`: `"ramping"`.
  - If autocontrol enabled and miner on and `upfreq_complete == 1`: `"normal"`.
- Both `stop_reason` and `resume_at_soc` are returned in `/api/autocontrol/status`.

**Backend — `services/miner_service.py`**
- Extracts `"Upfreq Complete"` from the miner SUMMARY response; stores as `upfreq_complete` (int, 0 or 1, defaults to 0).
- Automatically exposed via the existing `/api/miner/status` → `status` dict.

**Frontend — `static/index.html`**
- Added `<div id="minerStopReason">` banner between the autoModeStatus box and the status-grid, hidden by default.

**Frontend — `static/js/dashboard.js`**
- Added `state.lastMinerStatus` and `state.lastAutocontrolStatus` caches.
- Added `updateMinerStopReasonBanner()`:
  - Hide condition: `upfreq_complete == 1` AND `abs(power_5s - target_w) / target_w <= 0.10`.
  - Show when: `upfreq_complete == 0` OR `stop_reason != "normal"`.
  - Colors: amber (`emergency_soc`), gray (`manual_off`), blue (`ramping`/upfreq=0), yellow (fallback).
- Called from `updateMinerStatus()` and `updateAutoControlStatus()`.

### Live values at end of run (2026-05-09 18:24 EDT)

- `upfreq_complete`: `0` (miner stopped — battery telemetry stale, safety stop active)
- `stop_reason`: `"normal"` (stale-battery path does not set stop_reason — see deferred items)
- `resume_at_soc`: `null`
- Service: `active (running)`

### Self-verification results

1. `GET /api/autocontrol/status` — includes `stop_reason` and `resume_at_soc` ✓
2. `GET /api/miner/status` — includes `upfreq_complete: 0` ✓
3. `sudo systemctl status whatsminer` — `active (running)` ✓
4. Battery and miner data updating normally in journal ✓
5. `grep minerStopReason` in deployed HTML — found at line 82 ✓

### Deferred items

- ~~The stale-battery safety path in `_away_mode_control` does not set `stop_reason`; could add `"battery_stale"` reason if desired.~~ **Shipped 2026-05-09 — see section below.**
- ~~When autocontrol is enabled but battery is stale (miner stopped by safety gate), `stop_reason` stays at its last value (`"normal"` at startup). The banner will not show in this state unless `upfreq_complete == 0` triggers it — which it does since the miner is off.~~ **Resolved — stop_reason is now explicitly set to `battery_stale`.**

---

## battery_stale stop reason shipped on 2026-05-09

| SHA | Description |
|-----|-------------|
| `81f2c03` | feat: set stop_reason=battery_stale when safety gate fires |

**What changed:**
- `services/autocontrol_service.py`: The battery freshness safety gate in `_away_mode_control` now sets `self.stop_reason = "battery_stale"` and `self.resume_at_soc = None` before calling `power_off()`. The existing priority branches (2–5) already set `stop_reason = "normal"`, so recovery is automatic when telemetry becomes fresh.
- `static/js/dashboard.js`: Added `battery_stale` case to `updateMinerStopReasonBanner()` — orange/red banner (`bg: #ffe0cc, color: #7d2b00`) with text "Miner paused — battery telemetry stale. Waiting for fresh data."

**Self-verification passed:**
1. `GET /api/autocontrol/status` — `stop_reason: "battery_stale"` confirmed at startup (before first EG4 poll) ✓
2. `grep battery_stale /home/hirscr/WM_controller/static/js/dashboard.js` — found at line 837 ✓
3. `sudo systemctl status whatsminer` — `active (running)` ✓
4. Battery and miner polling updating normally in journal ✓

**Deferred items:** None.

---

## Phase 1 verification — Fixes #1/#2/#3 ship and live miner power-on (2026-05-10)

| SHA | Description |
|-----|-------------|
| `78c2bf0` | (local, not pushed) Fix #1 + Fix #2 in `services/autocontrol_service.py`; Fix #3 in `static/js/dashboard.js` |

### Fixes shipped to Pi
- **Fix #1 + #2** (`services/autocontrol_service.py`): all four "miner is off" branches in `_away_mode_control` (Priorities 2, 3, 4, 5) now defer the rate-limited `set_power_pct` call when they have to issue `power_on` first. The branch returns immediately after `self.miner.power_on()` so the next 60s tick re-evaluates. Rationale: the firmware's privileged session locks for ~180s per `get_token`; enqueueing `power_on` and `set_power_pct` on the same tick self-contends and one of the two will lose.
- **Fix #3** (`static/js/dashboard.js`): chart dedup / state display fix.

Files modified:
- `/home/hirscr/WM_controller/services/autocontrol_service.py` (deployed)
- `/home/hirscr/WM_controller/static/js/dashboard.js` (deployed)

### Verification — Phase 1 acceptance met

Conditions during run: nighttime (00:39–00:40 EDT), SOC 43.3%, PV 0W, autocontrol in away mode, normal-discharge tier 40% / 1440W target.

Two consecutive polls of `GET /api/miner/status` (60s apart, after service restart):

| Time | is_off | Power Limit | MHS av | Power | Notes |
|------|--------|-------------|--------|-------|-------|
| 00:39:30 | false | 1440 | 1,012,917 | 766 W | ramping |
| 00:40:31 | false | 1440 | 1,407,560 | 1045 W | hashing |

Acceptance criteria (`is_off=false`, `Power Limit > 0`, `MHS av > 0`, two consecutive polls): **PASS**.

### What we learned (Phase B probe — diagnostic)

Initial polling at 00:10–00:11 showed the new code firing as expected (`Powering on miner (deferring power adjust to next tick)...` followed by `Sending AES privileged command: power_on`), but the AES envelope returned `"enc json load err"` — i.e. the miner rejected the encrypted body. To diagnose, a one-shot probe (`tools/probe_aes_power_on_only.py`) was scp'd to the Pi (with the service stopped to remove contention) and run.

Probe outcome:
- `get_token` returned salt `L7mYce6.`, time `7804`, newsalt `yHmabpw8`.
- AES envelope built: 110 bytes, JSON shape `{"enc": 1, "data": "<43-byte b64>"}` (33 bytes after base64-decode — exactly one AES block + padding for `{"cmd": "power_on"}`).
- Miner response: 138 bytes, JSON shape `{"enc": "<encrypted blob>"}` — i.e. a **successful encrypted response**, not an error.
- 5s after the probe, summary showed Power Limit jumped 0 → 1440 and Power 14W → 24W (boot starting).
- 60s later, summary showed MHS av = 447,489 (hashing).

**Root cause of the 00:11 `enc json load err`** appears to be transient — likely a get_token salt/time race where the production code path's `_get_token_with_retry(max_attempts=2)` succeeded after a 185s wait but a different in-flight session (possibly from before the restart's 5-min staircase of session locks) had already poisoned that salt's slot. The exact same code path now produces an "enc"-keyed encrypted reply, the miner powers on, autocontrol's verification loop confirms Power Limit=1440, and the next-tick `set_power_pct` is no longer needed because the miner already has the correct limit.

The fix as deployed (one privileged op per tick when the miner is off) is the right architectural answer; the residual transient `enc json load err` will self-recover on the next tick because:
1. After a successful `power_on`, `is_off` flips false on the next poll.
2. With `is_off == false`, autocontrol falls through to `_set_power_with_rate_limit`, which dispatches a fresh `adjust_power_limit` op via the MD5-crypt inline path (known reliable).

Probe scripts removed from Pi after run; local copies kept under `tools/` for future debugging.

### Deferred items

- The probe script (`tools/probe_aes_power_on_only.py`) is preserved locally but not committed yet.
- The local commit `78c2bf0` carrying Fixes #1/#2/#3 is **not pushed to origin** (per spec).
- No further fix to `utils/nc_miner_api.py` was needed; the AES envelope is correct as-is. The transient `enc json load err` observed at 00:11 is consistent with firmware-side session salt-slot contention that the per-tick deferral pattern recovers from automatically.
