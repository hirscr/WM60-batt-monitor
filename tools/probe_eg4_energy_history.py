"""Probe EG4 portal for historical daily energy endpoints.

Tries candidate URLs that inverter portals commonly expose for daily/cumulative
energy statistics. Run on Pi with the service stopped, creds from env.

Usage:
    source .wm_env && python3 tools/probe_eg4_energy_history.py
"""
import asyncio
import json
import os
import sys
from datetime import date, timedelta

from eg4_inverter_api import EG4InverterAPI


# Yesterday and today as strings
TODAY = date.today().isoformat()
YESTERDAY = (date.today() - timedelta(days=1)).isoformat()
YEAR = date.today().strftime("%Y")
MONTH = date.today().strftime("%Y-%m")


async def main():
    user = os.getenv("EG4_USER")
    pwd = os.getenv("EG4_PASS")
    base_url = os.getenv("EG4_BASE_URL", "https://monitor.eg4electronics.com")

    if not user or not pwd:
        print("ERROR: EG4_USER and EG4_PASS must be set in environment")
        sys.exit(1)

    api = EG4InverterAPI(username=user, password=pwd, base_url=base_url)
    try:
        await api.login(ignore_ssl=True)
        invs = api.get_inverters()
        if invs:
            api.set_selected_inverter(inverterIndex=0)
            sn = invs[0].get("sn", "") if isinstance(invs[0], dict) else getattr(invs[0], "sn", "")
            print(f"Logged in. Inverter SN: {sn}")
        else:
            sn = ""
            print("Logged in. No inverters found.")

        # Grab the aiohttp session from the API object
        session = getattr(api, "_session", None)
        if session is None:
            get_sess = getattr(api, "_get_session", None)
            if get_sess:
                session = await get_sess()

        if session is None:
            print("ERROR: could not obtain aiohttp session")
            return

        # Candidate endpoints — common patterns for EG4/Luxpower/SolarEdge-style portals
        candidates = [
            # Daily energy summary by date range
            f"/WManage/api/inverter/energy?sn={sn}&date={YESTERDAY}",
            f"/WManage/api/inverter/energy?sn={sn}&startDate={YESTERDAY}&endDate={TODAY}",
            f"/WManage/api/monitor/energy?sn={sn}&date={YESTERDAY}",
            f"/WManage/api/plant/energy?date={YESTERDAY}",
            f"/WManage/api/analysis/energy?sn={sn}&date={YESTERDAY}",
            f"/WManage/api/analysis/pv?sn={sn}&date={YESTERDAY}",
            # Monthly/daily history
            f"/WManage/api/inverter/history?sn={sn}&date={YESTERDAY}",
            f"/WManage/api/inverter/energy/day?sn={sn}&date={YESTERDAY}",
            f"/WManage/api/inverter/energy/month?sn={sn}&month={MONTH}",
            f"/WManage/api/inverter/energyDay?sn={sn}&date={YESTERDAY}",
            f"/WManage/api/inverter/energyMonth?sn={sn}&month={MONTH}",
            # Alternate casing / path styles
            f"/WManage/web/monitor/energyStatistic?sn={sn}&date={YESTERDAY}",
            f"/WManage/web/monitor/energyStatistic?sn={sn}&startDate={YESTERDAY}&endDate={TODAY}&chartType=1",
            f"/WManage/api/plant/energyStatistic?date={YESTERDAY}",
            f"/WManage/api/monitor/chart?sn={sn}&date={YESTERDAY}&type=energy",
            f"/WManage/api/inverter/energyStatistic?sn={sn}&date={YESTERDAY}",
            # Yearly breakdown
            f"/WManage/api/inverter/energy/year?sn={sn}&year={YEAR}",
            f"/WManage/api/inverter/energyYear?sn={sn}&year={YEAR}",
        ]

        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{base_url}/WManage/web/monitor/inverter",
            "Accept": "application/json, text/plain, */*",
        }

        for path in candidates:
            url = f"{base_url}{path}"
            try:
                async with session.get(url, headers=headers, ssl=False, timeout=10) as resp:
                    status = resp.status
                    ct = resp.headers.get("Content-Type", "")
                    body = await resp.text(errors="replace")
                    body_short = body[:300].replace("\n", " ")
                    print(f"\n{'='*60}")
                    print(f"GET {path}")
                    print(f"  Status: {status}  Content-Type: {ct}")
                    print(f"  Body: {body_short}")
                    if status == 200 and "application/json" in ct:
                        try:
                            parsed = json.loads(body)
                            print(f"  Keys: {list(parsed.keys()) if isinstance(parsed, dict) else type(parsed).__name__}")
                        except Exception:
                            pass
            except Exception as exc:
                print(f"\nGET {path}  ERROR: {exc}")

    finally:
        await api.close()


asyncio.run(main())
