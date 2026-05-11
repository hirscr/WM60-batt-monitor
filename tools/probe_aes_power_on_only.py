#!/usr/bin/env python3
"""
One-shot probe for AES power_on path. Captures envelope hex and response hex.
Does NOT perform a second get_token (so we don't burn the privileged session).
"""
import os
import sys
import json
import time
import socket
import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from utils.nc_miner_api import NCMinerAPI


def load_creds():
    cfg_path = os.path.join(PROJECT_ROOT, "config.local.yaml")
    env_path = os.path.join(PROJECT_ROOT, ".wm_env")
    host = None
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


def hex_dump(label, data, max_bytes=400):
    if data is None:
        print(f"  {label}: <None>")
        return
    n = len(data)
    head = data[:max_bytes]
    print(f"  {label}: {n} bytes")
    print(f"    hex   : {head.hex()}")
    preview = "".join(chr(b) if 32 <= b < 127 else "." for b in head)
    print(f"    ascii : {preview}")


def main():
    host, pwd = load_creds()
    print(f"[probe] miner host: {host}")
    print(f"[probe] password loaded: {bool(pwd)}")
    api = NCMinerAPI(host, password=pwd)

    from pyasic.rpc.btminer import _crypt, create_privileged_cmd

    print("[probe] Requesting get_token...")
    token_response = api.get_token()
    print(f"[probe] token_response: {token_response}")
    if not token_response:
        print("[probe] ✗ get_token returned None")
        return

    msg = token_response.get("Msg", {})
    if isinstance(msg, str):
        print(f"[probe] ✗ token error: {msg}")
        return
    salt = msg.get("salt")
    time_str = msg.get("time")
    newsalt = msg.get("newsalt")
    print(f"[probe] salt={salt!r} time={time_str!r} newsalt={newsalt!r}")
    if not (salt and time_str and newsalt):
        print("[probe] ✗ missing fields")
        return

    pwd_crypt = _crypt(api.pwd, "$1$" + salt + "$")
    host_passwd_md5 = pwd_crypt.split("$")[3]
    tmp_crypt = _crypt(host_passwd_md5 + time_str, "$1$" + newsalt + "$")
    host_sign = tmp_crypt.split("$")[3]
    token_data = {"host_sign": host_sign, "host_passwd_md5": host_passwd_md5}
    cmd_dict = {"cmd": "power_on"}

    enc_payload = create_privileged_cmd(token_data, cmd_dict)
    print(f"[probe] enc_payload type: {type(enc_payload).__name__}")
    if isinstance(enc_payload, str):
        enc_payload_bytes = enc_payload.encode("utf-8")
    else:
        enc_payload_bytes = enc_payload

    hex_dump("ENVELOPE SENT", enc_payload_bytes, max_bytes=600)

    try:
        env_text = enc_payload_bytes.decode("utf-8", errors="replace")
        env_obj = json.loads(env_text)
        print(f"[probe] envelope keys: {list(env_obj.keys())}")
        for k, v in env_obj.items():
            preview = (v[:80] + "...") if isinstance(v, str) and len(v) > 80 else v
            print(f"    {k}: {preview!r}")
    except Exception as e:
        print(f"[probe] envelope is not parseable JSON: {e}")

    time.sleep(0.5)

    print("[probe] Sending envelope via raw socket...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(8)
    try:
        s.connect((api.ip, api.port))
        s.sendall(enc_payload_bytes)
        try:
            s.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        chunks = []
        while True:
            try:
                buf = s.recv(4096)
            except socket.timeout:
                break
            if not buf:
                break
            chunks.append(buf)
        resp = b"".join(chunks)
    finally:
        try:
            s.close()
        except Exception:
            pass

    hex_dump("RESPONSE RECEIVED", resp, max_bytes=600)
    try:
        resp_text = resp.decode("utf-8", errors="replace").strip()
        try:
            resp_obj = json.loads(resp_text)
            print(f"[probe] response JSON: {resp_obj}")
        except json.JSONDecodeError:
            print(f"[probe] response text: {resp_text!r}")
    except Exception as e:
        print(f"[probe] response decode error: {e}")

    # Confirm miner state after
    print("\n[probe] Polling summary 5s after to see effect...")
    time.sleep(5)
    summ = api.summary()
    s_item = ((summ or {}).get("SUMMARY") or [{}])[0]
    print(f"[probe] After: Power Limit={s_item.get('Power Limit')}, MHS av={s_item.get('MHS av')}, Power={s_item.get('Power')}")


if __name__ == "__main__":
    main()
