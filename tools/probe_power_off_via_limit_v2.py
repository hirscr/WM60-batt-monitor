#!/usr/bin/env python3
"""
Phase 0 probe v2: power miner ON (AES), then confirm adjust_power_limit(0) halts hashing.

Steps:
  1. Check baseline state
  2. If miner is off, use AES power_on to start it and wait for MHS 5s > 0
  3. Record running state
  4. adjust_power_limit(0) — print full request/response
  5. wait 60s
  6. summary — check Power Limit, MHS av, MHS 5s, Power
  7. RESULT: halted_ok = Power Limit==0 AND MHS 5s==0
  8. Restore to 1440W and verify
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

    print("\n[probe] === Step 1: Baseline ===")
    item_base = print_summary(api, "baseline")
    mhs_5s_base = float(item_base.get("MHS 5s") or 0)
    pl_base = item_base.get("Power Limit")

    # If miner is off, power it on so we can test shutdown from running state
    if mhs_5s_base == 0.0:
        print("\n[probe] Miner is off — sending AES power_on to start it...")
        resp_on = api.power_on()
        print(f"[probe] power_on response: {json.dumps(resp_on, default=str)}")

        print("[probe] Waiting up to 120s for MHS 5s > 0...")
        started = False
        for i in range(120):
            time.sleep(1)
            item_check = print_summary(api, f"  wait {i+1}s")
            mhs_check = float(item_check.get("MHS 5s") or 0)
            if mhs_check > 0:
                print(f"[probe] Miner is hashing: MHS 5s={mhs_check}")
                started = True
                break

        if not started:
            print("[probe] FATAL: miner did not start hashing within 120s")
            print("[probe] AES power_on may be failing on this firmware. Cannot test shutdown from running state.")
            sys.exit(3)
    else:
        print(f"[probe] Miner is already hashing: MHS 5s={mhs_5s_base}")

    print("\n[probe] === Step 2: Current running state ===")
    item_running = print_summary(api, "running")

    print("\n[probe] === Step 3: adjust_power_limit(0) ===")
    resp_off = api.send_privileged_command("adjust_power_limit", power_limit="0")
    print(f"[probe] resp_off: {json.dumps(resp_off, default=str)}")

    print("\n[probe] === Step 4: wait 60s ===")
    time.sleep(60)

    print("\n[probe] === Step 5: summary after power_limit=0 ===")
    item_after_off = print_summary(api, "after power_limit=0")

    pl_off = item_after_off.get("Power Limit")
    mhs_5s_off = float(item_after_off.get("MHS 5s") or 0)
    mhs_av_off = item_after_off.get("MHS av")
    # Use MHS 5s (not MHS av) — MHS av lags real state
    halted_ok = (str(pl_off) == "0") and (mhs_5s_off == 0.0)
    print(f"[probe] HALT VERIFIED: {halted_ok}  "
          f"(Power Limit={pl_off}, MHS 5s={mhs_5s_off}, MHS av={mhs_av_off})")

    if not halted_ok:
        print(f"[probe] RESULT: adjust_power_limit(0) did NOT halt hashing.")
        print(f"[probe] Power Limit={pl_off} (expected 0), MHS 5s={mhs_5s_off} (expected 0)")
        print(f"[probe] ACTION REQUIRED: Cannot use set_power_limit(0) for shutdown on this firmware.")

    print("\n[probe] === Step 6: Restore adjust_power_limit(1440) ===")
    resp_restore = api.send_privileged_command("adjust_power_limit", power_limit="1440")
    print(f"[probe] resp_restore: {json.dumps(resp_restore, default=str)}")

    print("\n[probe] === Step 7: wait 60s ===")
    time.sleep(60)

    print("\n[probe] === Step 8: summary after restore ===")
    item_after_on = print_summary(api, "after restore=1440")
    pl_on = item_after_on.get("Power Limit")
    print(f"[probe] RESTORED: Power Limit={pl_on}  (expected 1440)")

    print("\n[probe] === FINAL RESULT ===")
    print(f"[probe] halted_ok={halted_ok}  restored_pl={pl_on}")
    sys.exit(0 if halted_ok else 1)


if __name__ == "__main__":
    main()
