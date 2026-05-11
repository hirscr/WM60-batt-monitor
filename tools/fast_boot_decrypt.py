#!/usr/bin/env python3
"""Diagnostic: send enable_btminer_fast_boot via AES and decrypt the response."""
import sys
import os
import json
import time
import yaml
import hashlib
import binascii
import base64
import subprocess

sys.path.insert(0, '/home/hirscr/WM_controller')

from passlib.hash import md5_crypt
from pyasic.rpc.btminer import _crypt, create_privileged_cmd, parse_btminer_priviledge_data


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


def nc_send(ip: str, port: int, payload, timeout: int = 5):
    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    result = subprocess.run(
        ['nc', '-w', str(timeout), ip, str(port)],
        input=payload,
        capture_output=True,
        timeout=timeout + 2
    )
    if result.returncode == 0:
        return result.stdout.decode('utf-8', errors='replace').strip()
    raise RuntimeError(f"nc failed: {result.stderr.decode()}")


def main():
    env = load_env('/home/hirscr/WM_controller/.env')
    password = env.get('WM_PASS')
    if not password:
        cfg = load_config()
        password = cfg.get('miner', {}).get('password')
    if not password:
        print("ERROR: could not load password")
        sys.exit(1)

    cfg = load_config()
    ip = cfg.get('miner', {}).get('host', '192.168.86.52')
    port = 4028

    print(f"Miner: {ip}:{port}")

    # Step 1: get_token
    raw = nc_send(ip, port, json.dumps({"command": "get_token"}))
    token_resp = json.loads(raw)
    print(f"Token response: {json.dumps(token_resp, indent=2)}")

    msg = token_resp.get("Msg", {})
    salt = msg["salt"]
    time_str = msg["time"]
    newsalt = msg["newsalt"]

    # Step 2: derive credentials
    pwd_crypt = _crypt(password, "$1$" + salt + "$")
    host_passwd_md5 = pwd_crypt.split("$")[3]
    tmp_crypt = _crypt(host_passwd_md5 + time_str, "$1$" + newsalt + "$")
    host_sign = tmp_crypt.split("$")[3]

    token_data = {"host_sign": host_sign, "host_passwd_md5": host_passwd_md5}
    print(f"host_passwd_md5: {host_passwd_md5}")

    # Step 3: build AES command
    cmd_dict = {"cmd": "enable_btminer_fast_boot"}
    enc_payload = create_privileged_cmd(token_data, cmd_dict)
    print(f"AES payload ({len(enc_payload)} bytes): {enc_payload[:60]}...")

    time.sleep(0.5)

    # Step 4: send
    raw_resp = nc_send(ip, port, enc_payload)
    print(f"Raw response ({len(raw_resp)} bytes): {raw_resp[:100]}")
    data = json.loads(raw_resp)

    if "enc" not in data:
        print(f"Non-encrypted response: {data}")
        sys.exit(1)

    # Step 5: decrypt
    decrypted = parse_btminer_priviledge_data(token_data, data)
    print(f"Decrypted response:\n{json.dumps(decrypted, indent=2)}")


if __name__ == '__main__':
    main()
