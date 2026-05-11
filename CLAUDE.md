# Universal Rules

All global rules — communication style, coding standards, design patterns, logging, testing, verification, git workflow, SSH, credentials, file placement, UI change policy — live in `~/.claude/CLAUDE.md` and auto-load in every session. Read that file for the complete set.

Project-specific tech stack, build commands, architecture, and current state follow below this block.

# WhatsMiner Controller (WM60)

Solar-powered crypto mining controller. Monitors EG4 battery SOC and adjusts a WhatsMiner ASIC's power output to match available solar. Runs on a Raspberry Pi at the install site, accessed via Tailscale.

## Tech Stack
- **Language**: Python 3
- **Framework**: Flask (single `app.py` entrypoint)
- **Pattern**: Service/model separation — `services/` (business logic), `models/` (data types), `utils/` (helpers), `api/` (route blueprints), `config/` (settings loader), `static/` (web UI)
- **Platform**: Linux (Raspberry Pi production), macOS (dev)
- **Key deps**: pyasic (miner RPC), Flask, requests, PyYAML

## Build & Run
- Venv: `.venv/` at project root — activate before running
- Install: `pip install -r requirements.txt`
- Run: `python3 app.py` → http://localhost:8080/
- Tests: `pytest tests/` (plus root-level `test_*.py` ad-hoc scripts)
- Config: `config.yaml` (committed shape) + `config.local.yaml` (local overrides) + `.wm_env` (secrets)
- Production: deployed to Raspberry Pi, accessed via Tailscale

## Current State
See `STATE.md` for last-run details and `CONTINUE.md` for resume prompt.
- Production-ready as of 2026-05-09 (commit 95b2016).
- Five priority blocks landed: battery freshness safety gate, authoritative miner is_off, dual hashrate/power readings, emergency SOC UI, state-before-verification + chart dedup.
- Braiins integration shipped 2026-05-09.

## Architecture
- `app.py` — Flask app + route registration + background polling threads
- `services/` — `miner_service.py`, `battery_service.py`, `autocontrol_service.py`, `network_scanner.py`, `data_loader.py`
- `models/device.py` — device types/status
- `utils/state_manager.py` — persistent state in `wm_state.json`
- `eg4_client.py` — EG4 portal HTTP client (session mgmt + auto-refresh)
- `static/` — `index.html` + `dashboard.js` + `dashboard.css` (single unified mobile dashboard)
- Logs: `miner_logs/wm_status_log.csv`, `miner_logs/eg4_battery_log.csv`, `battery_logs/`

## Critical Constraints
- **Battery freshness gate**: if EG4 telemetry is stale (>10 min) or unknown, autocontrol must STOP the miner. Never send power commands based on stale SOC.
- **Emergency SOC**: configurable floor (default 30%); below this, miner is force-off. UI lets user adjust at runtime; persisted across restarts.
- **Latched floor**: once auto-power drops, it cannot rise until SOC hits 100% (prevents oscillation).
- **Live miner**: production miner is at a remote site. Test changes carefully — disruptive operations (forced shutoffs, power cycling) lose mining revenue.
- Credentials live in `.wm_env` / `config.local.yaml` — never commit.

## Domain Terms
- **SOC** — State of Charge (battery %)
- **EG4** — battery system; telemetry pulled from monitor.eg4electronics.com portal
- **WhatsMiner / WM** — the ASIC miner; controlled via pyasic RPC over LAN
- **Bitaxe** — secondary miner type discovered by network scan
- **Braiins** — mining pool integration (shipped 2026-05-09)
- **Decile ratcheting** — auto-control maps SOC to power in 10% steps, rounded up
- **Sunset logic** — after configured sunset time, if SOC > 60%, turn miner on to consume excess
- **Away mode** — autocontrol mode where user is off-site; conservative thresholds
- **`is_off`** — authoritative miner power state (Power Limit==0 OR MHS 5s==0); pyasic's `mineroff`/`get_psu()` unavailable on this firmware

## File Placement Rules
- New service logic → `services/<name>_service.py`
- New API routes → `api/` blueprint, registered in `app.py`
- New data models → `models/`
- Pure helpers → `utils/`
- Never grow `app.py` beyond route registration + thread orchestration — push logic into services.
- Tests → `tests/` (formal); root-level `test_*.py` scripts are ad-hoc and OK to leave.

## Project Rules
- Treat the live miner as production. Avoid invasive testing during daylight hours; prefer code-path verification over live disruption.
- pyasic limitations on this firmware: `get_psu()` returns None, `status` command returns None. Don't add features that depend on these — fall back to Power Limit / MHS 5s checks.
- Never ship code that bypasses the battery freshness gate.

## Key Documents
- `README.md` — feature overview and setup walkthrough
- `STATE.md` — last-run state (auto-written at context milestones)
- `CONTINUE.md` — resume prompt for next session
- `MIGRATION_GUIDE.md` — migration notes from the old monolithic WM_Server.py
- `REFACTORING_PLAN.md` — historical refactor plan (2025-11-08)
- `FOREMAN_PROMPT_BUGFIXES.md` — prompt drafts from prior bug-fix sessions
