# WM60 Controller — Session Continuation Prompt
**Last session ended:** 2026-05-10 ~14:30 EDT  
**Session role:** BUILDER (write code directly, use agents for complex tasks)

---

## Project overview

`WM_controller` is a Python/Flask web app that monitors a WhatsMiner M60S Bitcoin ASIC and
an EG4 solar/battery system. It runs as a systemd service on a Raspberry Pi at a remote
install site, accessed via Tailscale. The dashboard is a single-page mobile UI served on
port 8080.

- **Mac source tree:** `/Users/roberf/Dropbox/Programming/Websites/WM_controller`
- **Pi runtime:** `/home/hirscr/WM_controller` — user `hirscr`
- **Pi address (LAN):** `192.168.86.46`
- **Miner address (LAN, DHCP):** `192.168.86.26` — always verify from Pi's `config.local.yaml`
- **Systemd service:** `whatsminer.service`
- **Dashboard:** `http://192.168.86.46:8080/`
- **Venv on Pi:** `/home/hirscr/WM_controller/miner-venv/bin/python3`

---

## What happened this session

This session completed a multi-session bug hunt to make the emergency shutdown path
(AES `power_off`) work reliably. All fixes are committed and pushed (`53e2ff1`).

### Root cause found and fixed: wrong password in .env

The WhatsMiner M60S requires AES-encrypted commands (`power_off`, `power_on`). The AES key
is derived from `sha256(host_passwd_md5)` where `host_passwd_md5` comes from MD5-crypt of
the miner password. The service was loading `WM_PASS=admin` (5 chars) from
`/home/hirscr/WM_controller/.env` via a systemd `EnvironmentFile` drop-in at
`/etc/systemd/system/whatsminer.service.d/env.conf`. This overrode the correct
`password: "No1WantsRulers"` (14 chars) from `config.local.yaml`. Wrong password → wrong
AES key → firmware returns "enc json load err" → every emergency `power_off` failed.

**Fix:** Updated `.env` on the Pi directly:
```
WM_PASS=No1WantsRulers
```
This is a runtime-only change — not in git. The `.env` file must not be committed
(it's in `.gitignore`). If the Pi is re-provisioned, this value must be set manually.

**Confirmed:** After fix, journal showed `pwd_len=14`, `host_passwd_md5=AVeOUZWY258WCjVkIpv6S1`
(matches standalone probe), encrypted response 138 bytes, `✓ Miner powered off (verified
after attempt 1)`.

### Fix 1: Rate-limit sleep used actual remaining time (nc_miner_api.py)

`_get_token_with_retry` was sleeping the full `SESSION_TIMEOUT_SEC=185` even when the local
rate limiter had only just fired (e.g., 60s into the 185s window → only 127s remaining).
Now detects the synthetic "local rate limit" response and sleeps `max(1, 185 - elapsed + 2)`
instead of the full 185s.

### Fix 2: Post-command grace period in autocontrol (autocontrol_service.py)

After `adjust_power_limit` is issued, the firmware does chip recalibration for ~60-90s
during which both `Power Limit` and `MHS 5s` read 0 — making `is_off` appear True even
though the miner is actively reconfiguring (fans running at 7200 RPM, ~400-500W draw).
Without a guard, autocontrol would see `Miner=OFF` and issue `power_on`, which resets the
power limit back to the firmware default (1440W) and restarts recalibration → oscillation
loop: `adjust_power_limit(2160W) → recalibrate → is_off=True → power_on → 1440W → adjust
→ recalibrate → ...`.

**Fix:** Added `_in_post_cmd_grace_period()` helper (120s window from `last_set_ts`) and
checked it in all four `if is_miner_off: power_on()` branches (PRIORITY 2–5). Also updated
`last_set_ts` after each `power_on` call so the next tick's `adjust_power_limit` isn't
blocked from acting.

### Fix 3: Diagnostic logging removed (nc_miner_api.py)

During diagnosis, `send_aes_privileged_command` was logging `pwd_len`, `host_passwd_md5`,
`host_sign`, and `newsalt` for every AES command. These were removed. The token log was
also cleaned up (removed `newsalt` from the confirmation line).

### Fix 4: Misleading power-set log (miner_service.py)

`MinerController._run_op("power_pct")` was printing `✓ Power limit set to {watts}W` after
every `adjust_power_limit` call regardless of whether the API returned SUCCESS or ERROR.
Now only prints the success line if `STATUS == "S"`; otherwise logs a failure note and
falls through to the verify step (which passes idempotently if the miner is already at the
target power level).

---

## Files changed this session

All changes are in commit `53e2ff1` (pushed to `origin/main`):

| File | Change |
|------|--------|
| `utils/nc_miner_api.py` | Removed `pwd_len`/`host_passwd_md5`/`host_sign` diagnostic logging; removed `newsalt` from token log; fixed rate-limit sleep to use remaining window time |
| `services/autocontrol_service.py` | Added `_post_cmd_grace_sec = 120` instance var; added `_in_post_cmd_grace_period()` helper; added grace period check + `last_set_ts` update to all four `is_miner_off → power_on` branches |
| `services/miner_service.py` | Fixed `✓ Power limit set` log to be conditional on `STATUS == "S"` |
| `app.py` | Emergency API routes and test endpoints (carried from prior session work) |
| `static/js/dashboard.js` | Emergency UI banner (carried from prior session work) |
| `.gitignore` | Minor additions |

**Pi-only runtime change (not in git):**
- `/home/hirscr/WM_controller/.env` — `WM_PASS=No1WantsRulers` (was `admin`)

---

## Current state (end of session)

- **Git:** `main` branch, up to date with `origin/main` at `53e2ff1`
- **Service:** `whatsminer` active (running) on Pi
- **Miner:** ON — `is_off: False`, `Power Limit: 2520W` (SOC ~70% at session end,
  autocontrol raised the tier), `MHS 5s: ramping`, `Power: ~2450W`
- **Autocontrol:** enabled, Away mode, no emergency active
- **Battery:** SOC ~68-70%, PV variable (partly cloudy), charging

---

## What is working

- **Emergency shutdown:** AES `power_off` verified working — stops miner on first attempt,
  `✓ Miner powered off (verified after attempt 1)`. Emergency latch trips at SOC < 40%,
  holds until SOC ≥ 90% and miner confirmed off.
- **Normal autocontrol:** decile ratcheting (SOC% → power%), power_on/power_off cycle,
  grace period preventing recalibration oscillation.
- **Rate limiting:** 185s minimum between `get_token` nc calls; local rate limit uses
  actual remaining time, not fixed 185s sleep.
- **Threading:** `_priv_lock` serializes all privileged ops; 15s timeout falls through
  without lock if a concurrent op is in a long sleep (emergency path still works).
- **Braiins pool integration:** operational (not touched this session).
- **Dashboard UI:** emergency banner, SOC tiles, hashrate/power, All-Time stats — all working.

---

## Known issues / things to watch

1. **`.env` is the authoritative password source on Pi.** If the service ever stops with
   "enc json load err" in the journal, the first thing to check is whether `WM_PASS` in
   `/home/hirscr/WM_controller/.env` matches the miner's web UI password. The systemd
   drop-in at `/etc/systemd/system/whatsminer.service.d/env.conf` injects it via
   `EnvironmentFile`.

2. **`adjust_power_limit` causes recalibration.** Every time the power tier changes (SOC
   crosses a 10% boundary), the firmware briefly shows `Power Limit=0, MHS=0` for ~60-90s.
   The grace period handles this, but if you see the miner appearing "off" after a recent
   power command, wait 2 minutes before concluding anything is broken.

3. **Rate limiter: 185s between `get_token` calls.** After any privileged command sequence,
   there is a ~185s window where subsequent `power_on`/`power_off` requests will block
   waiting for the rate limiter. This is expected and correct — it prevents the firmware
   session lock from extending indefinitely.

4. **`EMERGENCY STOP UNVERIFIED` in journal.** During this session's testing, the emergency
   latch tick ran while the miner was already mid-startup (after being manually restarted
   from a probe script). This is a one-time testing artifact, not a production bug. The
   emergency stop itself works correctly.

5. **Miner default Power Limit is 1440W.** When AES `power_on` is sent, the firmware
   restores to its stored value (1440W). Autocontrol will issue `adjust_power_limit` on
   the next tick to raise it to the SOC-appropriate target. During the ~60s between
   power_on and the next tick, the miner runs at 1440W — correct behavior.

6. **`MinerController ✗ Verification failed for resume`** may appear if `power_on` is sent
   and the 10×1s verify loop times out before the miner finishes starting up. The miner
   does start — the verify window is just shorter than the firmware's startup time. The
   next autocontrol tick will see `Miner=ON` and proceed normally.

---

## Architecture reminders

- **NCMinerAPI** (`utils/nc_miner_api.py`) — all miner communication via subprocess `nc`.
  Two auth paths: MD5-crypt inline (`enc_pwd` field) for `adjust_power_limit`; AES envelope
  (pyasic's `create_privileged_cmd`) for `power_off` and `power_on`.
- **MinerController** (inner class in `services/miner_service.py`) — FIFO queue worker for
  privileged ops (`stop`, `resume`, `power_pct`). `drain_queue()` is called before emergency
  stop to flush pending ops.
- **AutoControlService** (`services/autocontrol_service.py`) — 60s tick loop, Away mode
  decile ratcheting, emergency latch, post-command grace period.
- **`is_off`** — authoritative power state: `Power Limit == 0 AND MHS 5s == 0`. Not reliable
  during recalibration; the grace period compensates.
- **Emergency path** bypasses the MinerController queue entirely — calls
  `miner.emergency_power_off()` → `nc_api.send_aes_privileged_command("power_off")` directly.

---

## Next steps

No outstanding bugs or tasks. The system is production-stable. Possible future work:

- **Tune grace period** if a different tier change takes longer than 120s to recalibrate
  (unlikely; 120s has margin).
- **Tune emergency verify window** — current 10×1s may be tight for slow firmware response;
  could increase to 20×1s if UNVERIFIED messages appear in production.
- **Push notification** when emergency latch trips (low priority, nice-to-have).
- **SOC display accuracy** — currently uses unit average; could weight by capacity.
