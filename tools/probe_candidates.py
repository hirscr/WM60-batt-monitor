"""Candidate enumeration for the power-tuning probe orchestrator.

Each candidate is one payload shape for the AES privileged-API envelope.
The orchestrator iterates this list in order. The list is intentionally a
plain Python module — git diff shows exactly what will be tried.

Excluded — too risky / proven catastrophic:
  - set_target_freq with negative percent string
      Proven catastrophic Reset in the prior sweep (PL->0, MHS->0, 9 min
      recovery). Per builder spec, never include this in any run.
  - adjust_power_limit
      Known to trigger full chip retune. This is the command we're trying
      to *replace*, so testing it here is meaningless.
  - power_off / power_on
      Out of scope — those are start/stop, not in-flight power changes.

Each entry:
  label:        unique human-readable id (also used in log/state)
  cmd:          dict shape sent inside the AES envelope. Use the literal
                string "{percent}" wherever the orchestrator should
                substitute the target percent. Use "{percent_int}" for an
                int substitution.
  value_type:   "str" or "int" — documents what value_type the percent is
                rendered as. Used for log clarity only.
"""

# Five original candidates from the prior sweep, retested with the
# Power-based classifier. The first sweep mis-classified three of these
# as No-op because it watched Power Limit instead of actual Power.
CANDIDATES = [
    # V2.0.5 spec section 3.22 — Normal mode, percent as string (per spec).
    {
        "label": "set_power_pct_v2_str",
        "cmd": {"cmd": "set_power_pct_v2", "percent": "{percent}"},
        "value_type": "str",
    },
    # V2.0.5 spec section 3.21 — Fast mode, percent as string (per spec).
    {
        "label": "set_power_pct_str",
        "cmd": {"cmd": "set_power_pct", "percent": "{percent}"},
        "value_type": "str",
    },
    # Same two commands but with percent rendered as int (in case prior
    # No-op was a type-mismatch).
    {
        "label": "set_power_pct_v2_int",
        "cmd": {"cmd": "set_power_pct_v2", "percent": "{percent_int}"},
        "value_type": "int",
    },
    {
        "label": "set_power_pct_int",
        "cmd": {"cmd": "set_power_pct", "percent": "{percent_int}"},
        "value_type": "int",
    },
    # Production code in utils/nc_miner_api.py uses "parameter" (not
    # "percent") as the field name for set_power_pct. Try both shapes
    # explicitly — Whatsminer firmware has used both spellings across
    # versions.
    {
        "label": "set_power_pct_parameter_str",
        "cmd": {"cmd": "set_power_pct", "parameter": "{percent}"},
        "value_type": "str",
    },
    {
        "label": "set_power_pct_v2_parameter_str",
        "cmd": {"cmd": "set_power_pct_v2", "parameter": "{percent}"},
        "value_type": "str",
    },
    # set_power_limit via AES envelope with computed watts as string.
    # The MD5-crypt path uses adjust_power_limit (Reset). Try the AES
    # envelope route in case it triggers a smoother retune path.
    {
        "label": "set_power_limit_watts_str",
        "cmd": {"cmd": "set_power_limit", "power_limit": "{watts}"},
        "value_type": "str",
    },
]


def render(template_cmd: dict, percent: int, base_watts: int) -> dict:
    """Substitute {percent}, {percent_int}, {watts} placeholders.

    Returns a fresh dict each call (no shared mutation).
    """
    watts = int(round(base_watts * (percent / 100.0)))
    out = {}
    for k, v in template_cmd.items():
        if not isinstance(v, str):
            out[k] = v
            continue
        if v == "{percent}":
            out[k] = str(percent)
        elif v == "{percent_int}":
            out[k] = int(percent)
        elif v == "{watts}":
            out[k] = str(watts)
        else:
            out[k] = v
    return out
