# WM60 Controller — Session Continuation Prompt
**Last session ended:** 2026-05-12  
**Session role:** BUILDER (write code directly, dispatch Foreman agents for complex tasks)

---

## Project overview

`WM_controller` is a Python/Flask web app that monitors a WhatsMiner M60S Bitcoin ASIC and
an EG4 solar/battery system. Runs as a systemd service on a Raspberry Pi at a remote install
site, accessed via Tailscale.

- **Mac source tree:** `/Users/roberf/Dropbox/Programming/Websites/WM_controller`
- **Pi runtime:** `/home/hirscr/WM_controller` — user `hirscr`
- **Pi address (LAN):** `192.168.86.46`
- **Miner address (LAN):** `192.168.86.26`
- **Systemd service:** `whatsminer.service`
- **Dashboard:** `http://192.168.86.46:8080/`
- **Pi venv:** `/home/hirscr/WM_controller/miner-venv/bin/python3`

---

## What happened this session

### Task 1 — Battery CSV schema fix (COMPLETED, committed, deployed)

**Bug:** `miner_logs/eg4_battery_log.csv` had a 27-column legacy header written under an
obsolete schema, but recent data rows had only 10 columns. `csv.DictReader` maps positionally,
so `row["soc_percent"]` was returning `pv_power_w` (thousands of watts). The Battery SOC
trace on the Plotly chart was plotting ~9.5k instead of 0–100.

**Fix shipped:**
- `services/battery_service.py`:
  - Added `CSV_FIELDNAMES` module-level constant (10 canonical fields in order)
  - Added `_reconcile_csv_schema()` — on startup, reads existing header, archives the file
    to `eg4_battery_log_legacy_<UTC>.csv` on mismatch, no-ops on match, called from `start()`
    before the polling thread spawns
  - `_log_to_csv` now uses `fieldnames=CSV_FIELDNAMES, extrasaction="ignore"`
- `tests/test_battery_csv_schema.py` — new file, 5 unit tests (all pass)
- `requirements.txt` — added `pytest`
- `.gitignore` — changed `test_*.py` to `/test_*.py` (root-only) so `tests/` directory is
  no longer blocked from git tracking

**Commit:** `c18d18d` on `main`.

**Pi deployment result:**
- Legacy file archived as `miner_logs/eg4_battery_log_legacy_20260512T125628Z.csv` (2.5M,
  md5=4931d5f8a82936498a8d474a30a11be4 — verified byte-identical to pre-deploy snapshot)
- New `eg4_battery_log.csv` started with canonical header; first row: `soc_percent=54.6` ✓
- Second restart confirmed idempotency: `[BatteryService] CSV schema OK.` — no re-archive
- Dashboard SOC trace confirmed plotting in 0–100 range ✓

### Task 2 — EG4 silent-failure self-recovery (PLANNED, NOT IMPLEMENTED)

A Foreman agent was dispatched with a full spec for this fix. The agent read the codebase,
confirmed the root cause, and presented a detailed approved plan — but the session ended
before implementation began. **This task is ready to implement from the plan below.**

**Background:** On 2026-05-11, EG4's portal returned HTTP 200 with
`{"success": false, "data": null, "error_message": null}` (silent session expiry). The
existing guard `if not batt or not runtime` in `eg4_client._poll_once` only catches falsy
responses — a truthy dict sails right through. The client looped for 5+ hours writing `None`
to every field; the dashboard showed no battery data; the freshness gate eventually stopped
the miner. A service restart fixed it instantly.

**Approved implementation plan (implement exactly this, no re-planning needed):**

Files to modify:
- `eg4_client.py` — primary change
- `tests/test_eg4_client_empty_response.py` — new file (4 unit tests)

Changes to `eg4_client.py`:
1. Add module constant: `RELOGIN_COOLDOWN_SEC = 300` (with comment: half the 600s freshness
   gate, so at least one recovery attempt is possible before the staleness threshold fires).
2. Add `_last_relogin_attempt: float = 0.0` to `__init__` (monotonic timestamp; 0 = eligible
   immediately).
3. Add module-level classifier function `_is_empty_response(resp) -> bool`:
   - Returns `True` if `resp` is `None` or falsy
   - Returns `True` if `resp` is a dict with `resp.get("success") is False`
   - Returns `True` if `resp` is a dict with `resp.get("data")` is `None`
   - Returns `False` otherwise
   - Document the rule once in a comment above the function. This is the only place the
     rule is encoded — no duplicated checks elsewhere.
4. Rewrite the empty-response block in `_poll_once` (currently lines 162–173):
   - After fetching both endpoints, check `_is_empty_response(batt) or _is_empty_response(runtime)`
   - If either is empty:
     - Build a description of which endpoint(s) were empty
     - Check cooldown: `time.monotonic() - self._last_relogin_attempt < RELOGIN_COOLDOWN_SEC`
     - If in cooldown: set `self._last_error = "empty_response_cooldown"`, log
       `[EG4Client] Empty response from <endpoints> — in cooldown, skipping re-login
       (next eligible in Xs)`, return early (snapshot ts does not advance; freshness gate
       handles miner safety)
     - If eligible: set `self._last_error = "empty_response_relogin_pending"`, log
       `[EG4Client] Empty response from <endpoints> — attempting re-login`,
       set `self._last_relogin_attempt = time.monotonic()`, call
       `await self._api.login(ignore_ssl=True)`, re-fetch both endpoints
       - If retry succeeds (both non-empty): log
         `[EG4Client] Recovered — session re-established, data flowing`, fall through to
         normal merge path; `_last_error` cleared by outer try/except
       - If retry still empty: log
         `[EG4Client] Re-login attempted but still no data — next retry in Xs`,
         set `self._last_error = "empty_response_relogin_pending"`, return early
5. Reduce the noisy per-poll debug prints (lines 179–198: `Full runtime response keys`,
   `Runtime sample data`, `EPS L1N/L2N`, fallback prints) to one terse line per successful
   poll, e.g. `[EG4Client] poll ok SOC=X% PV=Yw load=Zw`.

New file `tests/test_eg4_client_empty_response.py` — 4 tests (no network, no EG4 lib):
1. `{"success": false, "data": null, "error_message": null}` → `_is_empty_response` returns True
2. Normal dict with real fields → returns False
3. `None` → returns True
4. `{"success": true, "data": {"k": "v"}}` → returns False

**Workflow for Task 2:**
1. Implement locally, run `pytest tests/test_eg4_client_empty_response.py -v`
2. Show diff + test output, then **stop and wait for user approval before deploying to Pi**
3. Pi behavioral verification (iptables block test, cooldown test) requires explicit approval

---

## Current state (end of session)

- **Git:** `main` branch, `c18d18d`. **Not pushed to `origin/main`.**
- **Miner:** Running — SOC ~54.6%, PV 7507W, hashing normally, Power Limit 1800W
- **Autocontrol:** enabled, away mode
- **Battery CSV:** fresh canonical file on Pi; accumulating data from 08:56 2026-05-12

---

## What is working

- **Emergency shutdown:** AES `power_off` verified working
- **Normal autocontrol:** decile ratcheting, power_on/power_off cycle, grace period
- **Rate limiting:** 185s minimum between `get_token` calls
- **Braiins pool integration:** operational; USD equivalents showing on all tiles
- **Dashboard UI:** emergency banner, SOC tiles, hashrate/power, All-Time stats, chart
- **Chart:** lines-only mode, time-based decimation, manual-only data fetch, toggle via restyle
- **Battery CSV:** canonical schema pinned; legacy file archived; chart SOC plotting correctly
- **Tests:** `tests/test_battery_csv_schema.py` — 5 tests passing

---

## Immediate next steps

1. **Implement Task 2 (EG4 self-recovery)** — the plan is fully approved, implement directly:
   - Modify `eg4_client.py` per the plan above
   - Add `tests/test_eg4_client_empty_response.py`
   - Run unit tests locally, show diff, await user approval before Pi deploy

2. **Push to origin** when user approves:
   ```
   git push origin main
   ```

3. **Clean up stale prompt files** in project root (both tasks are done or in-flight):
   - `BUILDER_PROMPT_EMERGENCY_SHUTDOWN.md` — already shipped in prior sessions
   - `FOREMAN_PROMPT_BUGFIXES.md` — already shipped in prior sessions

---

## Known issues / things to watch

1. **EG4 verbose logging still in production.** `eg4_client.py` lines 179–198 dump full
   runtime response keys and sample data on every poll. This is journal spam. Task 2's
   implementation should reduce this to one summary line per poll.

2. **`battery_stale` stop_reason during first poll.** On every service restart, before the
   first EG4 poll succeeds, autocontrol shows `stop_reason="battery_stale"`. This is
   correct behavior (the startup grace period returns `is_fresh=True` to avoid shutdown, but
   `stop_reason` is set by the safety gate check). Benign.

3. **Battery chart history starts fresh.** After the CSV schema fix, the chart only shows
   data from 2026-05-12 08:56 onward. The 2.5M legacy file is preserved as
   `eg4_battery_log_legacy_20260512T125628Z.csv` but is not loaded by the chart. Repairing/
   migrating historical data is a deferred task.

4. **"EPS Power" chart trace label is misleading.** It actually plots load power (from
   `pEpsL1N + pEpsL2N`). Deferred rename task.

5. **`origin/main` not pushed.** Local `main` is ahead of last push. Run
   `git push origin main` when ready.

6. **pytest installed in Pi venv via `python -m pip`.** The Pi venv's `pip` shebang is stale
   (points to a moved path) — always use `python -m pip` on the Pi, not `pip` directly.

---

## Architecture reminders

- **NCMinerAPI** (`utils/nc_miner_api.py`) — all miner communication via subprocess `nc`.
  MD5-crypt inline (`enc_pwd`) for `adjust_power_limit`; AES envelope for `power_off`,
  `power_on`, fast boot.
- **MinerController** (inner class in `services/miner_service.py`) — FIFO queue worker.
  `drain_queue()` called before emergency stop.
- **AutoControlService** (`services/autocontrol_service.py`) — 60s tick loop, away mode
  decile ratcheting, emergency latch, 120s post-command grace period.
- **`is_off`** — `Power Limit == 0 AND MHS 5s == 0`. Not reliable during recalibration.
- **Static files** served directly; deploy with `scp`, no service restart needed.
- **`eg4_client.py`** — asyncio event loop on its own thread. `_poll_once` is the single
  coroutine that fetches and merges battery data. `BatteryService` wraps it with session
  management and CSV logging.
