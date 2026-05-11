#!/usr/bin/env python3
"""Probe AES power_off without respbefore parameter. Waits for session lock to clear first."""
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

    # Try AES power_off without respbefore, with session-wait retry
    for attempt in range(3):
        print(f"\nAttempt {attempt+1}/3: AES power_off (no respbefore)...")
        result = api.send_aes_privileged_command("power_off")
        print(f"Result: {json.dumps(result)}")

        time.sleep(5)
        import subprocess
        nc = subprocess.run(
            ['nc', '-w', '5', '192.168.86.26', '4028'],
            input=b'{"command":"summary"}',
            capture_output=True, timeout=8
        )
        if nc.returncode == 0:
            try:
                s = json.loads(nc.stdout.decode())
                pl = s.get('SUMMARY', [{}])[0].get('Power Limit')
                mhs = s.get('SUMMARY', [{}])[0].get('MHS av')
                pwr = s.get('SUMMARY', [{}])[0].get('Power')
                print(f"Power Limit: {pl}, MHS av: {mhs}, Power: {pwr}")
                if pl == 0 or mhs == 0:
                    print("SUCCESS: Miner appears off.")
                    sys.exit(0)
            except Exception as e:
                print(f"Summary parse error: {e}")

        # Check if we got "over max connect" and need to wait
        status_list = result.get('STATUS', [])
        msg = ''
        if status_list and isinstance(status_list, list):
            msg = status_list[0].get('Msg', '')

        if 'over max connect' in str(msg).lower() or 'timed out' in str(msg).lower():
            if attempt < 2:
                print(f"Session locked. Waiting 185s...")
                time.sleep(185)
        else:
            print(f"Command returned (not a lock error). Waiting 10s and retrying...")
            time.sleep(10)

    print("FAILED after 3 attempts.")
    sys.exit(1)
