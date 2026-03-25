from __future__ import annotations

import csv
import os
from pathlib import Path

import requests

# MOENV open data: Air Quality Index (AQI) realtime dataset
API_URL = "https://data.moenv.gov.tw/api/v2/aqx_p_432"
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = BASE_DIR / "data" / "hourly_aqi.csv"


def fetch_hourly_aqi(api_key: str | None = None, timeout: int = 20) -> list[dict]:
    """Fetch hourly AQI data for all stations in Taiwan.

    Returns an empty list if the API request fails.
    """
    params = {
        "format": "json",
        "limit": 1000,
    }

    # API key is optional for public data, but can be provided for stability.
    if api_key:
        params["api_key"] = api_key

    try:
        response = requests.get(API_URL, params=params, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        print(f"[WARN] API request failed, skip this run: {exc}")
        return []
    except ValueError as exc:
        print(f"[WARN] Invalid JSON response, skip this run: {exc}")
        return []

    records = payload.get("records", [])
    if not isinstance(records, list):
        print("[WARN] Unexpected API payload structure, skip this run.")
        return []

    return records


def save_to_csv(records: list[dict], output_path: Path = OUTPUT_PATH) -> None:
    """Save records to CSV and create folder automatically if needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not records:
        print("[INFO] No data fetched, skip writing CSV.")
        return

    # Use keys from API response to keep all available columns.
    fieldnames = sorted({key for row in records for key in row.keys()})

    with output_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"[OK] Saved {len(records)} rows to {output_path}")


def main() -> None:
    api_key = os.getenv("API_KEY")
    records = fetch_hourly_aqi(api_key=api_key)
    save_to_csv(records)


if __name__ == "__main__":
    main()
