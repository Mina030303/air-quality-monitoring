from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any
import datetime

import certifi
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import create_engine, text
from tenacity import retry, stop_after_attempt, wait_exponential

# ==========================================
# 設定與常數區
# ==========================================
# MOENV open data endpoints
HOURLY_API_URL = "https://data.moenv.gov.tw/api/v2/aqx_p_432"
DAILY_API_URL = "https://data.moenv.gov.tw/api/v2/aqx_p_434"

BASE_DIR = Path(__file__).resolve().parent.parent
HOURLY_OUTPUT_PATH = BASE_DIR / "data" / "hourly_aqi.csv"
DAILY_OUTPUT_PATH = BASE_DIR / "data" / "daily_aqi.csv"

# ==========================================
# 網路請求區
# ==========================================
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
    return _fetch_records(HOURLY_API_URL, api_key=api_key, timeout=timeout, limit=1000)

def fetch_daily_aqi(api_key: str | None = None, timeout: int = 20) -> list[dict[str, Any]]:
    return _fetch_records(DAILY_API_URL, api_key=api_key, timeout=timeout, limit=20000)

def save_to_csv(records: list[dict[str, Any]], output_path: Path) -> None:
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

    fieldnames = sorted({key for row in deduped for key in row.keys()})
    with output_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduped)

    print(f"[OK] Saved {len(deduped)} unique rows to {output_path}")

# ==========================================
# 資料庫寫入區 (包含修復邏輯與 PM2.5)
# ==========================================
def upsert_hourly_to_db(records: list[dict[str, Any]], db_url: str) -> None:
    if not records:
        print("[INFO] No hourly AQI data to upsert to DB.")
        return
        
    engine = create_engine(db_url)
    valid_cols = {"siteid", "sitename", "county", "aqi", "pollutant", "status", "so2", "co", "o3", "o3_8hr", "pm10", "pm2.5", "no2", "nox", "no", "wind_speed", "wind_direc", "publishtime", "publish_time"}
    
    publish_time_aliases = [
        "publish_time", "PublishTime", "publishTime", "publishtime", "datacreationdate", "DataCreationDate", "發布時間", "PublishingDate", "RecordTime"
    ]
    
    for row in records:
        for alias in publish_time_aliases:
            if alias in row:
                row["publish_time"] = row[alias]
                break

    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Taipei")
    except ImportError:
        import pytz
        tz = pytz.timezone("Asia/Taipei")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(publish_time) FROM hourly_aqi"))
        max_db_time = result.scalar()
        if max_db_time:
            try:
                max_db_time = pd.to_datetime(max_db_time)
                if max_db_time.tzinfo is None:
                    max_db_time = max_db_time.replace(tzinfo=tz)
                else:
                    max_db_time = max_db_time.astimezone(tz)
            except Exception:
                max_db_time = None

    filtered = []
    for row in records:
        pt_raw = row.get("publish_time")
        try:
            t = pd.to_datetime(pt_raw)
            if t.tzinfo is None:
                t = t.replace(tzinfo=tz)
            else:
                t = t.astimezone(tz)
        except Exception:
            continue
            
        if t is pd.NaT:
            continue
            
        if not max_db_time or t > max_db_time:
            filtered.append(row)
            
    if not filtered:
        print("[INFO] No new hourly AQI data to upsert to DB.")
        return

    with engine.begin() as conn:
        for row in filtered:
            row = {k: v for k, v in row.items() if k in valid_cols}
            
            # 1. 基礎防呆：沒有站名、縣市、時間的廢棄資料直接跳過
            if not row.get("publish_time") or not row.get("county") or not row.get("sitename"):
                continue
                
            # 2. 安全處理 AQI 空值 (維持時間連續性)
            aqi_val = row.get("aqi")
            if aqi_val == "" or aqi_val is None:
                aqi_val = None
            else:
                try:
                    aqi_val = float(aqi_val)
                except ValueError:
                    aqi_val = None
                    
            # 3. 安全處理 pm2.5 空值 (維持時間連續性)
            pm25_val = row.get("pm2.5")
            if pm25_val == "" or pm25_val is None:
                pm25_val = None
            else:
                try:
                    pm25_val = float(pm25_val)
                except ValueError:
                    pm25_val = None

            # 4. 寫入資料庫：加入 "pm2.5" 欄位，並修正 ON CONFLICT 為正確的 (site_name, publish_time)
            sql = text("""
                INSERT INTO hourly_aqi (site_name, county, publish_time, aqi, "pm2.5")
                VALUES (:site_name, :county, :publish_time, :aqi, :pm25)
                ON CONFLICT (site_name, publish_time)
                DO UPDATE SET 
                    aqi = EXCLUDED.aqi,
                    "pm2.5" = EXCLUDED."pm2.5"
            """)
            
            try:
                conn.execute(sql, {
                    "site_name": row.get("sitename"),
                    "county": row["county"],
                    "publish_time": row["publish_time"],
                    "aqi": aqi_val,
                    "pm25": pm25_val
                })
            except sqlalchemy.exc.ProgrammingError as e:
                print(f"[ERROR] SQL 執行失敗: {e}")
                continue

    print(f"[OK] Upserted {len(filtered)} new hourly AQI rows to DB (last 6 hours)")

# ==========================================
# 主程式
# ==========================================
def main() -> None:
    api_key = os.getenv("API_KEY")
    db_url = os.getenv("DATABASE_URL")
    
    hourly_records = fetch_hourly_aqi(api_key=api_key)
    daily_records = fetch_daily_aqi(api_key=api_key)
    
    save_to_csv(hourly_records, HOURLY_OUTPUT_PATH)
    save_to_csv(daily_records, DAILY_OUTPUT_PATH)
    
    if db_url:
        upsert_hourly_to_db(hourly_records, db_url)

if __name__ == "__main__":
    main()
