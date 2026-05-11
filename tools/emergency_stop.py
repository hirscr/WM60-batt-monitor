#!/usr/bin/env python3
"""Emergency stop: sets power_limit=0 via MD5-crypt path (reliable). Run on Pi."""
import sys
import os
import json
import subprocess
sys.path.insert(0, '/home/hirscr/WM_controller')

from utils.nc_miner_api import NCMinerAPI

def load_password():
    env_path = '/home/hirscr/WM_controller/.wm_env'
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith('MINER_PASSWORD='):
                    return line.split('=', 1)[1].strip().strip('"').strip("'")
    # Try config.local.yaml
    cfg_path = '/home/hirscr/WM_controller/config.local.yaml'
    if os.path.exists(cfg_path):
        import yaml
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        return cfg.get('miner', {}).get('password') or cfg.get('password')
    return None

if __name__ == '__main__':
    password = load_password()
    if not password:
        print("ERROR: Could not load miner password")
        sys.exit(1)

    api = NCMinerAPI('192.168.86.52', password=password)
    print("Sending adjust_power_limit=0 (MD5-crypt path)...")
    result = api.send_privileged_command('adjust_power_limit', power_limit='0')
    print(f"Result: {json.dumps(result, indent=2)}")

    import time
    time.sleep(3)
    summary = api.summary()
    lst = (summary or {}).get('SUMMARY') or []
    s = lst[0] if lst else {}
    print(f"Post-command Power Limit: {s.get('Power Limit')}")
    print(f"Post-command MHS av: {s.get('MHS av')}")
