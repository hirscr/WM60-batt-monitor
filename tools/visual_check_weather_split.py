"""One-shot Playwright visual check of the weather-card split.

Captures two screenshots of the dashboard — wide (1400px, side-by-side
expected) and narrow (800px, stacked expected) — and verifies the
expected DOM IDs are present exactly once.

The Flask server must already be running on http://localhost:8080.
"""
from __future__ import annotations

from pathlib import Path
from playwright.sync_api import sync_playwright


OUT_DIR = Path("/tmp/wm_visual_check")
OUT_DIR.mkdir(exist_ok=True)


EXPECTED_IDS = [
    "weatherForecastCard",
    "weatherParamsCard",
    "weatherSunrise",
    "weatherSunset",
    "weatherCloudCover",
    "weatherMaxForDay",
    "weatherExpected",
    "weatherSoc",
    "weatherDeficit",
    "weatherEvaluatedAt",
    "weatherDecisionBanner",
    "weatherFreshness",
    "weatherEvaluateBtn",
    "weatherCfgEnabled",
    "weatherCfgBatteryKwh",
    "weatherCfgSummerKwh",
    "weatherCfgWinterKwh",
    "weatherCfgSafetyFactor",
    "weatherCfgPreSunriseMin",
    "weatherCfgRecoverySoc",
    "weatherCfgRecoveryHours",
    "weatherSaveStatus",
    "tierPromoTier",
    "tierPromoTierBaseline",
    "tierPromoCooldown90",
    "tierPromoCooldown100",
    "tierPromoDetail",
]


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        # --- WIDE viewport: expect side-by-side ---
        page_wide = browser.new_page(viewport={"width": 1400, "height": 1100})
        page_wide.goto("http://localhost:8080/", wait_until="networkidle")
        page_wide.wait_for_timeout(1500)  # let weather card render

        wide_shot = OUT_DIR / "wide_1400.png"
        page_wide.screenshot(path=str(wide_shot), full_page=True)
        print(f"Wide screenshot: {wide_shot}")

        # ID presence check
        missing = []
        duplicates = []
        for el_id in EXPECTED_IDS:
            count = page_wide.evaluate(
                "id => document.querySelectorAll('#' + id).length", el_id
            )
            if count == 0:
                missing.append(el_id)
            elif count > 1:
                duplicates.append((el_id, count))
        if missing:
            print(f"MISSING IDs: {missing}")
        if duplicates:
            print(f"DUPLICATE IDs: {duplicates}")
        if not missing and not duplicates:
            print(f"All {len(EXPECTED_IDS)} expected IDs present exactly once.")

        # Layout sanity: forecast card should be LEFT of params card on wide.
        forecast_box = page_wide.locator("#weatherForecastCard").bounding_box()
        params_box = page_wide.locator("#weatherParamsCard").bounding_box()
        print(f"Wide forecast box: {forecast_box}")
        print(f"Wide params box:   {params_box}")
        if forecast_box and params_box:
            same_row = abs(forecast_box["y"] - params_box["y"]) < 30
            params_right_of_forecast = params_box["x"] > forecast_box["x"] + forecast_box["width"] - 10
            print(f"Wide same row?            {same_row}")
            print(f"Wide params right of fc?  {params_right_of_forecast}")

        # Input width check — every numeric input in params card should be
        # narrow (<= 90px is generous; CSS sets 5.5em ≈ 77px).
        widths = page_wide.evaluate(
            """() => Array.from(
                document.querySelectorAll('#weatherParamsCard input[type=number]')
            ).map(el => ({id: el.id, w: el.getBoundingClientRect().width}))"""
        )
        print(f"Wide numeric input widths: {widths}")
        bad = [w for w in widths if w["w"] > 100]
        if bad:
            print(f"NUMERIC INPUTS TOO WIDE: {bad}")
        else:
            print("All numeric inputs render <= 100px wide.")

        page_wide.close()

        # --- NARROW viewport: expect stacked ---
        page_narrow = browser.new_page(viewport={"width": 800, "height": 1400})
        page_narrow.goto("http://localhost:8080/", wait_until="networkidle")
        page_narrow.wait_for_timeout(1500)
        narrow_shot = OUT_DIR / "narrow_800.png"
        page_narrow.screenshot(path=str(narrow_shot), full_page=True)
        print(f"Narrow screenshot: {narrow_shot}")

        forecast_box_n = page_narrow.locator("#weatherForecastCard").bounding_box()
        params_box_n = page_narrow.locator("#weatherParamsCard").bounding_box()
        print(f"Narrow forecast box: {forecast_box_n}")
        print(f"Narrow params box:   {params_box_n}")
        if forecast_box_n and params_box_n:
            stacked = params_box_n["y"] > forecast_box_n["y"] + forecast_box_n["height"] - 10
            print(f"Narrow stacked (params below forecast)? {stacked}")

        page_narrow.close()
        browser.close()


if __name__ == "__main__":
    main()
