from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

import requests
import certifi
from tenacity import retry, stop_after_attempt, wait_exponential

# MOENV open data endpoints
HOURLY_API_URL = "https://data.moenv.gov.tw/api/v2/aqx_p_432"
DAILY_API_URL = "https://data.moenv.gov.tw/api/v2/aqx_p_434"
BASE_DIR = Path(__file__).resolve().parent.parent
HOURLY_OUTPUT_PATH = BASE_DIR / "data" / "hourly_aqi.csv"
DAILY_OUTPUT_PATH = BASE_DIR / "data" / "daily_aqi.csv"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def _fetch_records(
    api_url: str,
    api_key: str | None = None,
    timeout: int = 20,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    """Fetch records from MOENV API with simple error handling."""
    params = {
        "format": "json",
        "limit": limit,
    }

    # API key is optional for public data, but can be provided for stability.
    if api_key:
        params["api_key"] = api_key

    try:
        response = requests.get(api_url, params=params, timeout=timeout, verify=certifi.where())
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        print(f"[WARN] API request failed for {api_url}, skip this run: {exc}")
        return []
    except ValueError as exc:
        print(f"[WARN] Invalid JSON response for {api_url}, skip this run: {exc}")
        return []

    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        records = payload.get("records", [])
    else:
        print("[WARN] Unexpected API payload type, skip this run.")
        return []

    if not isinstance(records, list):
        print("[WARN] Unexpected API payload structure, skip this run.")
        return []

    rows = [row for row in records if isinstance(row, dict)]

    # In-memory dedup to avoid duplicate rows returned by API.
    unique_rows: list[dict[str, Any]] = []
    seen: set[tuple[tuple[str, str], ...]] = set()
    for row in rows:
        row_key = tuple(sorted((str(k), str(v)) for k, v in row.items()))
        if row_key in seen:
            continue
        seen.add(row_key)
        unique_rows.append(row)

    return unique_rows


def fetch_hourly_aqi(api_key: str | None = None, timeout: int = 20) -> list[dict[str, Any]]:
    """Fetch hourly AQI data for all stations in Taiwan."""
    return _fetch_records(HOURLY_API_URL, api_key=api_key, timeout=timeout, limit=1000)


def fetch_daily_aqi(api_key: str | None = None, timeout: int = 20) -> list[dict[str, Any]]:
    """Fetch daily AQI data for all stations in Taiwan."""
    return _fetch_records(DAILY_API_URL, api_key=api_key, timeout=timeout, limit=20000)


def save_to_csv(records: list[dict[str, Any]], output_path: Path) -> None:
    """Merge with existing CSV, deduplicate, and save to path."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not records:
        print(f"[INFO] No data fetched for {output_path.name}, skip writing CSV.")
        return

    merged_records: list[dict[str, Any]] = []
    if output_path.exists():
        with output_path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            merged_records.extend(dict(row) for row in reader)

    merged_records.extend(records)

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[tuple[str, str], ...]] = set()
    for row in merged_records:
        row_key = tuple(sorted((str(k), str(v)) for k, v in row.items()))
        if row_key in seen:
            continue
        seen.add(row_key)
        deduped.append(row)

    # Keep all columns from historical + new data.
    fieldnames = sorted({key for row in deduped for key in row.keys()})

    with output_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduped)

    print(f"[OK] Saved {len(deduped)} unique rows to {output_path}")


def main() -> None:
    api_key = os.getenv("API_KEY")
    hourly_records = fetch_hourly_aqi(api_key=api_key)
    daily_records = fetch_daily_aqi(api_key=api_key)
    save_to_csv(hourly_records, HOURLY_OUTPUT_PATH)
    save_to_csv(daily_records, DAILY_OUTPUT_PATH)


if __name__ == "__main__":
    main()
