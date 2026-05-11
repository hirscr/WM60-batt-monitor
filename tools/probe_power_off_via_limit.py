#!/usr/bin/env python3
"""
Phase 0 probe: confirm adjust_power_limit(0) actually halts hashing.

Steps:
  1. adjust_power_limit(0) — print full request/response
  2. wait 60s
  3. summary — print Power Limit, MHS av, MHS 5s, Power
  4. adjust_power_limit(1440) — restore — print req/resp
  5. wait 60s
  6. summary — confirm restored
"""
import os
import sys
import json
import time
import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from utils.nc_miner_api import NCMinerAPI


def load_creds():
    cfg_path = os.path.join(PROJECT_ROOT, "config.local.yaml")
    env_path = os.path.join(PROJECT_ROOT, ".wm_env")
    pwd = None
    with open(cfg_path) as f:
        cfg = yaml.safe_load(f) or {}
    miner = cfg.get("miner") or {}
    host = miner.get("host") or miner.get("ip") or "192.168.86.26"
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k in ("MINER_PWD", "MINER_PASSWORD", "WM_PASSWORD", "WHATSMINER_PASSWORD"):
                    pwd = v
    if not pwd:
        pwd = (cfg.get("miner") or {}).get("password")
    return host, pwd


def print_summary(api, label):
    summ = api.summary()
    item = ((summ or {}).get("SUMMARY") or [{}])[0]
    print(f"[probe] {label}: Power Limit={item.get('Power Limit')}  "
          f"MHS av={item.get('MHS av')}  MHS 5s={item.get('MHS 5s')}  "
          f"Power={item.get('Power')}")
    return item


def main():
    host, pwd = load_creds()
    print(f"[probe] miner host: {host}")
    print(f"[probe] password loaded: {bool(pwd)}")
    if not pwd:
        print("[probe] FATAL: no password")
        sys.exit(2)

    api = NCMinerAPI(host, password=pwd)

    print("\n[probe] === Baseline ===")
    print_summary(api, "baseline")

    print("\n[probe] === Step 1: adjust_power_limit(0) ===")
    resp_off = api.send_privileged_command("adjust_power_limit", power_limit="0")
    print(f"[probe] resp_off: {json.dumps(resp_off, default=str)}")

    print("\n[probe] === Step 2: wait 60s ===")
    time.sleep(60)

    print("\n[probe] === Step 3: summary after power=0 ===")
    item_after_off = print_summary(api, "after power_limit=0")

    pl_off = item_after_off.get("Power Limit")
    mhs_5s_off = item_after_off.get("MHS 5s")
    # Use MHS 5s (not MHS av) — MHS av lags real state and can stay nonzero after shutdown
    halted_ok = (str(pl_off) == "0") and (float(mhs_5s_off or 0) == 0.0)
    print(f"[probe] HALT VERIFIED: {halted_ok}  (Power Limit={pl_off}, MHS 5s={mhs_5s_off})")

    print("\n[probe] === Step 4: adjust_power_limit(1440) ===")
    resp_on = api.send_privileged_command("adjust_power_limit", power_limit="1440")
    print(f"[probe] resp_on: {json.dumps(resp_on, default=str)}")

    print("\n[probe] === Step 5: wait 60s ===")
    time.sleep(60)

    print("\n[probe] === Step 6: summary after restore ===")
    item_after_on = print_summary(api, "after restore=1440")
    pl_on = item_after_on.get("Power Limit")
    print(f"[probe] RESTORED: Power Limit={pl_on}  (expected 1440)")

    print("\n[probe] === RESULT ===")
    print(f"[probe] halted_ok={halted_ok}  restored_pl={pl_on}")
    sys.exit(0 if halted_ok else 1)


if __name__ == "__main__":
    main()
