"""One-shot probe: hit EG4 portal /api/weather/forecast and dump the
response shape, so we can validate that the endpoint exists, accepts our
session, and returns ePvPredict in the shape the JS code implies.

Run on the Pi with the service stopped. Reads creds from the env.
"""
import asyncio
import os
import sys
import json
import inspect

from eg4_inverter_api import EG4InverterAPI


def _safe_print_attrs(obj, indent="  "):
    """Print useful attributes/methods of an object without dumping internals."""
    public = [a for a in dir(obj) if not a.startswith("_")]
    for name in public:
        try:
            val = getattr(obj, name)
        except Exception as e:
            print(f"{indent}{name}: <getattr error: {e}>")
            continue
        if callable(val):
            try:
                sig = str(inspect.signature(val))
            except (TypeError, ValueError):
                sig = "(?)"
            print(f"{indent}{name}{sig}  (callable)")
        else:
            r = repr(val)
            if len(r) > 120:
                r = r[:120] + "..."
            print(f"{indent}{name} = {r}")


async def main():
    user = os.getenv("EG4_USER")
    pwd = os.getenv("EG4_PASS")
    base = os.getenv("EG4_BASE_URL", "https://monitor.eg4electronics.com")
    if not user or not pwd:
        print("ERROR: EG4_USER / EG4_PASS not set", file=sys.stderr)
        sys.exit(1)

    print(f"[probe] base_url={base}")
    print(f"[probe] user={user[:3]}...")

    api = EG4InverterAPI(username=user, password=pwd, base_url=base)
    print("[probe] logging in...")
    await api.login(ignore_ssl=True)
    print("[probe] login ok")

    print("\n[probe] EG4InverterAPI public surface:")
    _safe_print_attrs(api)

    print("\n[probe] looking up inverters...")
    invs = api.get_inverters()
    print(f"[probe] got {len(invs) if invs else 0} inverter(s)")
    if invs:
        inv = invs[0]
        print(f"[probe] inverter[0] type: {type(inv).__name__}")
        print("[probe] inverter[0] public surface:")
        _safe_print_attrs(inv)
        # Try common serial attribute names
        for key in ("serialNum", "serial_num", "sn", "serial", "serialNumber"):
            try:
                v = getattr(inv, key, None)
            except Exception:
                v = None
            if v is not None:
                print(f"[probe] inverter[0].{key} = {v!r}")
        # Also try dict-like access (in case it's a dataclass with .dict() or model_dump())
        for method in ("dict", "model_dump", "to_dict"):
            if hasattr(inv, method):
                try:
                    print(f"[probe] inverter[0].{method}() = {getattr(inv, method)()!r}"[:400])
                except Exception as e:
                    print(f"[probe] inverter[0].{method}() error: {e}")
        api.set_selected_inverter(inverterIndex=0)
        print("[probe] selected inverter 0")

    # Look at the underlying session/transport
    print("\n[probe] looking for the aiohttp session on the API object...")
    for attr in ("_session", "session", "_client", "client", "_http", "http"):
        if hasattr(api, attr):
            print(f"[probe]   api.{attr} = {type(getattr(api, attr)).__name__}")

    # Try to hit the forecast endpoint via the library's session if accessible.
    print("\n[probe] attempting POST /api/weather/forecast ...")
    # Find a usable session
    session = None
    for attr in ("_session", "session", "_client", "client"):
        candidate = getattr(api, attr, None)
        if candidate is not None and hasattr(candidate, "post"):
            session = candidate
            print(f"[probe] using api.{attr} as the session")
            break

    serial_value = None
    if invs:
        for key in ("serialNum", "serial_num", "sn", "serial", "serialNumber"):
            v = getattr(invs[0], key, None)
            if v:
                serial_value = v
                break

    if not serial_value:
        print("[probe] ERROR: could not find a serial number on the inverter object")
    elif session is None:
        print("[probe] ERROR: could not find a usable aiohttp session on the API object")
    else:
        url = f"{base}/WManage/api/weather/forecast"
        print(f"[probe] POST {url} with serialNum={serial_value}")
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Origin": base,
            "Referer": f"{base}/WManage/web/monitor/inverter",
        }
        try:
            async with session.post(
                url,
                data={"serialNum": serial_value},
                headers=headers,
                ssl=False,
            ) as resp:
                status = resp.status
                body_text = await resp.text()
            print(f"[probe] status: {status}")
            print(f"[probe] body length: {len(body_text)}")
            print("[probe] body (first 4000 chars):")
            print(body_text[:4000])
            # Try to parse JSON and pretty-print the keys we care about
            try:
                parsed = json.loads(body_text)
                print("\n[probe] parsed JSON top-level keys:")
                if isinstance(parsed, dict):
                    for k in parsed.keys():
                        v = parsed[k]
                        if isinstance(v, (dict, list)):
                            print(f"  {k}: <{type(v).__name__}>")
                        else:
                            print(f"  {k} = {v!r}")
                if isinstance(parsed, dict) and "ePvPredict" in parsed:
                    print("\n[probe] ePvPredict block:")
                    print(json.dumps(parsed["ePvPredict"], indent=2))
            except Exception as e:
                print(f"[probe] JSON parse failed: {e}")
        except Exception as e:
            print(f"[probe] POST raised: {type(e).__name__}: {e}")

    await api.close()
    print("\n[probe] done")


if __name__ == "__main__":
    asyncio.run(main())
