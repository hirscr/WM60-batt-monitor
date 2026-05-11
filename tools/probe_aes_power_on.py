#!/usr/bin/env python3
"""
Diagnostic probe for the AES power_on path.

Run on the Raspberry Pi (production) to capture exactly what's sent to the
miner and what's returned for both:
  1. send_aes_privileged_command("power_on")  -- the failing path
  2. send_privileged_command("adjust_power_limit", power_limit="1440") -- known good

Prints hex of bytes sent and bytes received. Waits 185s between calls so the
firmware's privileged session lock has time to expire.

Reads MINER_HOST from config.local.yaml; reads MINER_PWD from .wm_env.
Does NOT log credentials.
"""
import os
import sys
import json
import time
import socket
import yaml

# Allow `from utils.nc_miner_api import NCMinerAPI` when invoked from project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from utils.nc_miner_api import NCMinerAPI


def load_creds():
    """Read miner host from config.local.yaml; password from .wm_env."""
    cfg_path = os.path.join(PROJECT_ROOT, "config.local.yaml")
    env_path = os.path.join(PROJECT_ROOT, ".wm_env")

    host = None
    pwd = None

    with open(cfg_path) as f:
        cfg = yaml.safe_load(f) or {}
    miner = cfg.get("miner") or {}
    host = miner.get("host") or miner.get("ip")

    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k in ("MINER_PWD", "MINER_PASSWORD", "WM_PASSWORD", "WHATSMINER_PASSWORD"):
                    pwd = v

    if not host:
        host = miner.get("host_ip") or "192.168.86.26"

    if not pwd:
        # Fall back to top-level password key in config.local.yaml
        pwd = (cfg.get("miner") or {}).get("password")

    if not host or not pwd:
        raise RuntimeError(f"Missing host ({bool(host)}) or password ({bool(pwd)})")
    return host, pwd


def hex_dump(label: str, data: bytes, max_bytes: int = 256):
    if data is None:
        print(f"  {label}: <None>")
        return
    n = len(data)
    head = data[:max_bytes]
    print(f"  {label}: {n} bytes")
    print(f"    hex   : {head.hex()}")
    # Print printable preview (replace non-printable with .)
    preview = "".join(chr(b) if 32 <= b < 127 else "." for b in head)
    print(f"    ascii : {preview}")


def probe_aes_power_on(api: NCMinerAPI):
    """
    Replicate the AES power_on path but capture raw bytes sent and received.

    We re-implement the path inline so we can intercept both directions.
    """
    print("\n" + "=" * 70)
    print("PROBE 1: AES power_on (the failing path)")
    print("=" * 70)

    from pyasic.rpc.btminer import _crypt, create_privileged_cmd

    # Step 1: get_token
    print("[probe] Requesting get_token...")
    token_response = api.get_token()
    print(f"[probe] token_response: {token_response}")
    if not token_response:
        print("[probe] ✗ get_token returned None — abort probe")
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
        print("[probe] ✗ missing salt/time/newsalt — abort")
        return

    # Step 2: derive
    pwd_crypt = _crypt(api.pwd, "$1$" + salt + "$")
    host_passwd_md5 = pwd_crypt.split("$")[3]
    tmp_crypt = _crypt(host_passwd_md5 + time_str, "$1$" + newsalt + "$")
    host_sign = tmp_crypt.split("$")[3]
    print(f"[probe] host_passwd_md5 length: {len(host_passwd_md5)}")
    print(f"[probe] host_sign length: {len(host_sign)}")

    token_data = {
        "host_sign": host_sign,
        "host_passwd_md5": host_passwd_md5,
    }
    cmd_dict = {"cmd": "power_on"}

    # Build envelope
    enc_payload = create_privileged_cmd(token_data, cmd_dict)
    print(f"[probe] enc_payload type: {type(enc_payload).__name__}")
    if isinstance(enc_payload, str):
        enc_payload_bytes = enc_payload.encode("utf-8")
    else:
        enc_payload_bytes = enc_payload

    hex_dump("ENVELOPE SENT", enc_payload_bytes, max_bytes=400)

    # Try to decode as JSON for inspection (envelope is usually JSON {"enc": "...", ...})
    try:
        env_text = enc_payload_bytes.decode("utf-8", errors="replace")
        env_obj = json.loads(env_text)
        print(f"[probe] envelope keys: {list(env_obj.keys())}")
        for k, v in env_obj.items():
            preview = (v[:60] + "...") if isinstance(v, str) and len(v) > 60 else v
            print(f"    {k}: {preview!r}")
    except Exception as e:
        print(f"[probe] envelope is not parseable JSON: {e}")

    # Brief pause so the get_token TCP connection is fully closed
    time.sleep(0.5)

    # Step 4: send via raw socket so we can capture all bytes back
    print("[probe] Opening raw socket to send envelope...")
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

    hex_dump("RESPONSE RECEIVED", resp, max_bytes=400)
    try:
        resp_text = resp.decode("utf-8", errors="replace").strip()
        try:
            resp_obj = json.loads(resp_text)
            print(f"[probe] response JSON keys: {list(resp_obj.keys())}")
            print(f"[probe] response JSON: {resp_obj}")
        except json.JSONDecodeError as je:
            print(f"[probe] response is not JSON: {je}")
            print(f"[probe] response text: {resp_text!r}")
    except Exception as e:
        print(f"[probe] response decode error: {e}")


def probe_md5_adjust_power_limit(api: NCMinerAPI):
    print("\n" + "=" * 70)
    print("PROBE 2: MD5-crypt inline adjust_power_limit=1440 (known-good path)")
    print("=" * 70)
    result = api.send_privileged_command("adjust_power_limit", power_limit="1440")
    print(f"[probe] result: {result}")


def main():
    host, pwd = load_creds()
    print(f"[probe] miner host: {host}")
    print(f"[probe] password loaded: {bool(pwd)} (length={len(pwd) if pwd else 0})")

    api = NCMinerAPI(host, password=pwd)

    probe_aes_power_on(api)

    print("\n[probe] Sleeping 185s for firmware privileged session lock to expire...")
    for remaining in range(185, 0, -15):
        print(f"[probe]   ...{remaining}s remaining")
        time.sleep(15)

    probe_md5_adjust_power_limit(api)
    print("\n[probe] Done.")


if __name__ == "__main__":
    main()
