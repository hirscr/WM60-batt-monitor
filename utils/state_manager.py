import json, os, tempfile, time

DEFAULT_STATE = {
    "autocontrol": False,           # whether autocontrol is enabled
    "miner_power_state": "stopped", # "stopped" or "running"
    "target_power_pct": 0,          # e.g., 40 for 40 percent
    "last_updated": 0               # unix time seconds
}

class StateManager:
    def __init__(self, path="wm_state.json"):
        self.path = path
        # create file if missing
        if not os.path.exists(self.path):
            self._atomic_write(DEFAULT_STATE)

    def load(self):
        try:
            with open(self.path, "r") as f:
                data = json.load(f)
            # fill any missing fields for forward compatibility
            merged = DEFAULT_STATE.copy()
            merged.update(data or {})
            return merged
        except Exception:
            return DEFAULT_STATE.copy()

    def save(self, **kwargs):
        state = self.load()
        state.update(kwargs)
        state["last_updated"] = int(time.time())
        self._atomic_write(state)
        return state

    def _atomic_write(self, state):
        d = os.path.dirname(os.path.abspath(self.path)) or "."
        fd, tmp = tempfile.mkstemp(prefix=".wmstate.", dir=d)
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(state, f, separators=(",", ":"), sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)  # atomic on POSIX
        finally:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
