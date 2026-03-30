from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

CWA_URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001"
TAIPEI_TZ = "Asia/Taipei"


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _extract_element_value(weather_element: Any, target_keys: list[str]) -> Any:
    """Extract a weather element value from dict/list payload variants."""
    target_set = {key.lower() for key in target_keys}

    if isinstance(weather_element, dict):
        for key, value in weather_element.items():
            if key.lower() in target_set:
                return value

        # Some CWA payloads use nested structures under weather element keys.
        for key, value in weather_element.items():
            if key.lower() in target_set and isinstance(value, dict):
                return value.get("ElementValue")

        # Some CWA payloads use ElementName/ElementValue rows in list form.
        if "ElementName" in weather_element and "ElementValue" in weather_element:
            name = str(weather_element.get("ElementName", "")).lower()
            if name in target_set:
                return weather_element.get("ElementValue")

    if isinstance(weather_element, list):
        for item in weather_element:
            value = _extract_element_value(item, target_keys)
            if value is not None:
                return value

    return None


def fetch_cwa_station_payload(cwa_api_key: str, timeout: int = 30) -> list[dict[str, Any]]:
    params = {
        "Authorization": cwa_api_key,
        "format": "JSON",
    }
    response = requests.get(CWA_URL, params=params, timeout=timeout)
    response.raise_for_status()

    payload = response.json()
    return payload.get("records", {}).get("Station", [])


def build_hourly_weather_dataframe(stations: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for station in stations:
        station_id = station.get("StationId")
        if not station_id:
            continue

        obs_datetime = station.get("ObsTime", {}).get("DateTime")
        weather_element = station.get("WeatherElement", {})

        wind_speed_raw = _extract_element_value(
            weather_element,
            ["WindSpeed", "WINDSPEED", "WDSD"],
        )
        wind_dir_raw = _extract_element_value(
            weather_element,
            ["WindDirection", "WINDDIRECTION", "WDIR"],
        )

        wind_speed = _to_float(wind_speed_raw)
        wind_direction = _to_float(wind_dir_raw)

        # Filter out invalid records: missing direction/speed or negative speed.
        if wind_speed is None or wind_direction is None:
            continue
        if wind_speed < 0:
            continue
        if not (0 <= wind_direction <= 360):
            continue

        obs_ts = pd.to_datetime(obs_datetime, errors="coerce")
        if pd.isna(obs_ts):
            continue

        # Normalize all weather timestamps to Taiwan local time (naive timestamp).
        if getattr(obs_ts, "tzinfo", None) is None:
            obs_ts = obs_ts.tz_localize(TAIPEI_TZ)
        else:
            obs_ts = obs_ts.tz_convert(TAIPEI_TZ)

        obs_time = obs_ts.tz_localize(None).floor("h")

        # U = -speed * sin(theta), V = -speed * cos(theta), theta in degrees.
        theta = math.radians(wind_direction)
        wind_u = -wind_speed * math.sin(theta)
        wind_v = -wind_speed * math.cos(theta)

        rows.append(
            {
                "station_id": str(station_id),
                "obs_time": obs_time.floor("h"),
                "wind_u": wind_u,
                "wind_v": wind_v,
            }
        )

    return pd.DataFrame(rows, columns=["station_id", "obs_time", "wind_u", "wind_v"])


def append_hourly_weather_to_postgres(df: pd.DataFrame, database_url: str, table_name: str = "hourly_weather") -> None:
    if df.empty:
        return

    engine = create_engine(database_url)
    try:
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
    finally:
        engine.dispose()


def normalize_existing_hourly_weather_timezone(database_url: str, table_name: str = "hourly_weather") -> None:
    """One-time migration: convert timestamptz obs_time into Taiwan local timestamp."""
    engine = create_engine(database_url)
    try:
        with engine.begin() as conn:
            table_check = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=:table_name
                    """
                ),
                {"table_name": table_name},
            ).scalar()

            if not table_check:
                return

            dtype = conn.execute(
                text(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:table_name AND column_name='obs_time'
                    """
                ),
                {"table_name": table_name},
            ).scalar()

            if dtype == "timestamp with time zone":
                conn.execute(
                    text(
                        f"""
                        ALTER TABLE {table_name}
                        ALTER COLUMN obs_time TYPE timestamp without time zone
                        USING (obs_time AT TIME ZONE '{TAIPEI_TZ}')
                        """
                    )
                )
                print("Normalized existing hourly_weather.obs_time to Asia/Taipei local time.")
            elif dtype == "timestamp without time zone":
                print("hourly_weather.obs_time already uses local timestamp (no timezone).")
    finally:
        engine.dispose()


def main() -> None:
    base_dir = Path(__file__).resolve().parent.parent
    load_dotenv(base_dir / ".env")

    cwa_api_key = os.getenv("CWA_API_KEY", "")
    database_url = os.getenv("DATABASE_URL", "")

    if not cwa_api_key:
        raise ValueError("CWA_API_KEY environment variable is not set")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    normalize_existing_hourly_weather_timezone(database_url=database_url)

    stations = fetch_cwa_station_payload(cwa_api_key=cwa_api_key)
    weather_df = build_hourly_weather_dataframe(stations)
    append_hourly_weather_to_postgres(weather_df, database_url=database_url)

    print(f"Fetched and saved {len(weather_df)} hourly weather records to hourly_weather table.")


if __name__ == "__main__":
    main()
