#!/usr/bin/env bash
# Launch the probe orchestrator as a nohup-detached background process.
# Single-instance: the orchestrator itself refuses to start if probe_state.json
# shows a live PID — this script just adds a friendly check up front.

set -u

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV_PY="${PROJECT_DIR}/miner-venv/bin/python3"
ORCH="${PROJECT_DIR}/tools/probe_orchestrator.py"
LOG_DIR="${PROJECT_DIR}/logs"
NOHUP_LOG="${LOG_DIR}/probe_orchestrator.nohup.out"
PID_FILE="${PROJECT_DIR}/tools/probe.pid"

mkdir -p "${LOG_DIR}"

if [ ! -x "${VENV_PY}" ]; then
  echo "[launch] FATAL: venv python not found at ${VENV_PY}" >&2
  exit 1
fi

if [ -f "${PID_FILE}" ]; then
  existing_pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [ -n "${existing_pid}" ] && kill -0 "${existing_pid}" 2>/dev/null; then
    echo "[launch] An orchestrator is already running (PID ${existing_pid})." >&2
    echo "[launch] Use POST /api/probe/stop or 'kill ${existing_pid}' to stop it first." >&2
    exit 3
  fi
fi

# Source the project .env so WM_PASS and any other secrets are available.
if [ -f "${PROJECT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "${PROJECT_DIR}/.env"
  set +a
fi

cd "${PROJECT_DIR}" || exit 1

echo "[launch] Starting orchestrator…"
nohup "${VENV_PY}" "${ORCH}" "$@" >> "${NOHUP_LOG}" 2>&1 &
launched_pid=$!
disown "${launched_pid}" 2>/dev/null || true

# Give it a moment to write the PID file.
sleep 1

if kill -0 "${launched_pid}" 2>/dev/null; then
  echo "[launch] Orchestrator PID ${launched_pid} running."
  echo "[launch] nohup log: ${NOHUP_LOG}"
else
  echo "[launch] FATAL: orchestrator failed to start. Tail of ${NOHUP_LOG}:" >&2
  tail -n 40 "${NOHUP_LOG}" >&2 || true
  exit 1
fi
