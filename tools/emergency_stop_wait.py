#!/usr/bin/env python3
"""Emergency stop: waits for session lock to clear, then sets power_limit=0."""
import sys
import os
import json
import time
sys.path.insert(0, '/home/hirscr/WM_controller')

from utils.nc_miner_api import NCMinerAPI

def load_password():
    cfg_path = '/home/hirscr/WM_controller/config.local.yaml'
    if os.path.exists(cfg_path):
        import yaml
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        return cfg.get('miner', {}).get('password')
    return None

if __name__ == '__main__':
    password = load_password()
    if not password:
        print("ERROR: Could not load miner password")
        sys.exit(1)

    api = NCMinerAPI('192.168.86.26', password=password)

    # Try up to 3 times, sleeping 185s between attempts to let session lock expire
    for attempt in range(3):
        print(f"\nAttempt {attempt+1}/3: sending adjust_power_limit=0...")
        result = api.send_privileged_command('adjust_power_limit', power_limit='0')
        print(f"Result: {json.dumps(result)}")

        # Check if it worked
        time.sleep(3)
        summary = api.summary()
        lst = (summary or {}).get('SUMMARY') or []
        s = lst[0] if lst else {}
        pl = s.get('Power Limit')
        mhs = s.get('MHS av')
        print(f"Power Limit: {pl}, MHS av: {mhs}")

        if pl == 0 or pl == '0':
            print("SUCCESS: Miner stopped.")
            sys.exit(0)

        # Check for over max connect in result
        status_list = result.get('STATUS', [])
        msg = ''
        if status_list and isinstance(status_list, list):
            msg = status_list[0].get('Msg', '')
        elif isinstance(result.get('Msg'), str):
            msg = result.get('Msg', '')

        if 'over max connect' in msg.lower() or 'timed out' in msg.lower():
            if attempt < 2:
                print(f"Session locked. Waiting 185s for lock to expire...")
                time.sleep(185)
        else:
            print(f"Unexpected error, retrying in 10s...")
            time.sleep(10)

    print("FAILED: Could not stop miner after 3 attempts.")
    sys.exit(1)
