#!/usr/bin/env python3
"""
Phase 0 probe v3: wait for miner to be hashing, then test adjust_power_limit(0).
The miner was already powered on by the previous probe. This picks up from there.
"""
import os, sys, json, time, yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
from utils.nc_miner_api import NCMinerAPI

def load_creds():
    with open(os.path.join(PROJECT_ROOT, "config.local.yaml")) as f:
        cfg = yaml.safe_load(f) or {}
    m = cfg.get("miner") or {}
    host = m.get("host") or "192.168.86.26"
    pwd = m.get("password")
    env = os.path.join(PROJECT_ROOT, ".wm_env")
    if os.path.exists(env):
        with open(env) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line: continue
                k, v = line.split("=", 1)
                k = k.strip(); v = v.strip().strip('"').strip("'")
                if k in ("MINER_PWD", "MINER_PASSWORD", "WM_PASSWORD", "WHATSMINER_PASSWORD"):
                    pwd = v
    return host, pwd

def snap(api, label):
    s = api.summary()
    item = ((s or {}).get("SUMMARY") or [{}])[0]
    print(f"[probe] {label}: PL={item.get('Power Limit')} MHS5s={item.get('MHS 5s')} "
          f"MHSav={item.get('MHS av')} Power={item.get('Power')} Upfreq={item.get('Upfreq Complete')}")
    return item

def main():
    host, pwd = load_creds()
    api = NCMinerAPI(host, password=pwd)

    print("[probe] === Phase 0 v3: Wait for hashrate, then test shutdown ===")
    print("[probe] Waiting up to 10 minutes for Upfreq Complete=1 and MHS 5s > 0...")

    hashing = False
    for i in range(600):
        time.sleep(1)
        item = snap(api, f"  t+{i+1}s")
        mhs = float(item.get("MHS 5s") or 0)
        upfreq = item.get("Upfreq Complete")
        if mhs > 0:
            print(f"[probe] Miner is hashing! MHS 5s={mhs}")
            hashing = True
            break
        if i % 30 == 29:
            power = item.get("Power")
            print(f"[probe] Still ramping at {i+1}s — Power={power}W, MHS 5s={mhs}")

    if not hashing:
        print("[probe] FATAL: miner did not start hashing within 10 minutes")
        # Try to restore power limit anyway
        api.send_privileged_command("adjust_power_limit", power_limit="1440")
        sys.exit(3)

    print("\n[probe] === Running state confirmed ===")
    item_run = snap(api, "running")

    print("\n[probe] === Sending adjust_power_limit(0) ===")
    resp = api.send_privileged_command("adjust_power_limit", power_limit="0")
    print(f"[probe] Response: {json.dumps(resp, default=str)}")

    print("\n[probe] === Waiting 60s ===")
    time.sleep(60)

    print("\n[probe] === Summary after power_limit=0 ===")
    item_off = snap(api, "after_limit_0")
    pl = item_off.get("Power Limit")
    mhs5 = float(item_off.get("MHS 5s") or 0)
    mhsav = item_off.get("MHS av")
    halted = (str(pl) == "0") and (mhs5 == 0.0)
    print(f"[probe] HALT_OK={halted}  PL={pl}  MHS5s={mhs5}  MHSav={mhsav}")

    print("\n[probe] === Restoring adjust_power_limit(1440) ===")
    resp2 = api.send_privileged_command("adjust_power_limit", power_limit="1440")
    print(f"[probe] Restore response: {json.dumps(resp2, default=str)}")

    print("\n[probe] === RESULT ===")
    print(f"[probe] halted_ok={halted}")
    if not halted:
        print(f"[probe] FINDING: adjust_power_limit(0) did NOT halt hashing on this firmware")
        print(f"[probe] PL={pl} (expected 0), MHS 5s={mhs5} (expected 0)")
    sys.exit(0 if halted else 1)

if __name__ == "__main__":
    main()
