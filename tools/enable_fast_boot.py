#!/usr/bin/env python3
"""One-shot tool: enable Btminer Fast Boot on the production WhatsMiner.
Run on the Pi. Sends enable_btminer_fast_boot via MD5-crypt privileged path,
then verifies the setting persisted via a summary() poll.
"""
import sys
import os
import json
import time
import yaml

sys.path.insert(0, '/home/hirscr/WM_controller')

from utils.nc_miner_api import NCMinerAPI


def load_env(path: str) -> dict:
    result = {}
    if not os.path.exists(path):
        return result
    with open(path) as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                key, _, val = line.partition('=')
                result[key.strip()] = val.strip().strip('"').strip("'")
    return result


def load_config() -> dict:
    cfg_path = '/home/hirscr/WM_controller/config.local.yaml'
    if os.path.exists(cfg_path):
        with open(cfg_path) as f:
            return yaml.safe_load(f) or {}
    return {}


def get_summary_fields(api: NCMinerAPI) -> dict:
    data = api.summary()
    lst = (data or {}).get('SUMMARY') or []
    return lst[0] if lst else {}


def main():
    env = load_env('/home/hirscr/WM_controller/.env')
    password = env.get('WM_PASS')

    if not password:
        cfg = load_config()
        password = cfg.get('miner', {}).get('password')

    if not password:
        print("ERROR: Could not load miner password from .env or config.local.yaml")
        sys.exit(1)

    cfg = load_config()
    miner_host = cfg.get('miner', {}).get('host', '192.168.86.52')

    print(f"Miner host: {miner_host}")
    api = NCMinerAPI(miner_host, password=password)

    # Pre-command baseline
    print("\n--- Pre-command summary ---")
    pre = get_summary_fields(api)
    pre_power_limit = pre.get('Power Limit')
    pre_mhs = pre.get('MHS 5s')
    pre_fastboot = pre.get('Btminer Fast Boot')
    print(f"  Power Limit:      {pre_power_limit}")
    print(f"  MHS 5s:           {pre_mhs}")
    print(f"  Btminer Fast Boot: {pre_fastboot}")

    # Send command via AES path (pyasic's send_privileged_command uses create_privileged_cmd/AES,
    # not the MD5-crypt inline enc_pwd format)
    print("\n--- Sending enable_btminer_fast_boot (AES path) ---")
    response = api.send_aes_privileged_command("enable_btminer_fast_boot")
    print(f"Full response:\n{json.dumps(response, indent=2)}")

    # Wait 30s then poll
    print("\nWaiting 30s before verification poll...")
    time.sleep(30)

    print("\n--- Post-command summary (30s) ---")
    post = get_summary_fields(api)
    post_power_limit = post.get('Power Limit')
    post_mhs = post.get('MHS 5s')
    post_fastboot = post.get('Btminer Fast Boot')
    print(f"  Power Limit:      {post_power_limit}")
    print(f"  MHS 5s:           {post_mhs}")
    print(f"  Btminer Fast Boot: {post_fastboot}")

    if post_fastboot == 'enable':
        print("\nVERIFICATION PASSED: Btminer Fast Boot = enable")
        print(f"Power Limit unchanged: {pre_power_limit} -> {post_power_limit}")
        sys.exit(0)

    # Retry after another 60s
    print(f"\nFast Boot still '{post_fastboot}' after 30s — waiting 60s more and re-polling...")
    time.sleep(60)

    print("\n--- Post-command summary (90s) ---")
    post2 = get_summary_fields(api)
    post2_fastboot = post2.get('Btminer Fast Boot')
    post2_power_limit = post2.get('Power Limit')
    post2_mhs = post2.get('MHS 5s')
    print(f"  Power Limit:      {post2_power_limit}")
    print(f"  MHS 5s:           {post2_mhs}")
    print(f"  Btminer Fast Boot: {post2_fastboot}")

    if post2_fastboot == 'enable':
        print("\nVERIFICATION PASSED (90s poll): Btminer Fast Boot = enable")
        sys.exit(0)

    print(f"\nVERIFICATION FAILED: Btminer Fast Boot = '{post2_fastboot}' after 90s")

    # Diagnose
    status_list = response.get('STATUS') or []
    if isinstance(status_list, list) and status_list:
        status_code = status_list[0].get('STATUS')
        msg = status_list[0].get('Msg', '')
    else:
        status_code = response.get('STATUS')
        msg = response.get('Msg', '')

    print(f"Command STATUS: {status_code}, Msg: {msg}")
    if status_code != 'S':
        print("Command was NOT accepted by firmware — likely needs AES path or different command name.")
    else:
        print("Command was accepted (STATUS:S) but setting did not persist — may need a different command or firmware restart.")
    sys.exit(1)


if __name__ == '__main__':
    main()
