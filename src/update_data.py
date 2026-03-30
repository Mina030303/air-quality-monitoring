from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

from fetch_data import _fetch_paginated_csv_data
from clean_data import clean_hourly_data, clean_daily_data
from save_data import save_csv


BASE_DIR = Path(__file__).resolve().parent.parent


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _trim_hourly_window(df: pd.DataFrame, days: int = 30) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["datacreationdate"] = pd.to_datetime(df["datacreationdate"], errors="coerce")
    cutoff = datetime.now() - timedelta(days=days)

    df = df[df["datacreationdate"] >= cutoff]
    df = df.sort_values("datacreationdate").drop_duplicates().reset_index(drop=True)
    return df


def _trim_daily_window(df: pd.DataFrame, days: int = 365 * 2) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df["monitordate"] = pd.to_datetime(df["monitordate"], errors="coerce")
    cutoff = datetime.now() - timedelta(days=days)

    df = df[df["monitordate"] >= cutoff]
    df = df.sort_values("monitordate").drop_duplicates().reset_index(drop=True)
    return df


def update_hourly_data(api_key: str, limit: int = 1000, max_pages: int | None = 1) -> pd.DataFrame:
    processed_path = BASE_DIR / "data/processed/hourly_clean.csv"
    old_df = _safe_read_csv(processed_path)

    if old_df.empty:
        # 沒舊資料就抓最近30天
        end_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=30)
    else:
        old_df["datacreationdate"] = pd.to_datetime(old_df["datacreationdate"], errors="coerce")
        last_dt = old_df["datacreationdate"].max()

        # 往前退1小時，避免邊界漏資料
        start_dt = last_dt - timedelta(hours=1)
        end_dt = datetime.now().replace(minute=0, second=0, microsecond=0)

    filters = (
        f"datacreationdate,GR,{start_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        f"|datacreationdate,LE,{end_dt.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    new_df = _fetch_paginated_csv_data(
        dataset_code="aqx_p_488",
        api_key=api_key,
        filters=filters,
        sort="datacreationdate desc",
        limit=limit,
        max_pages=max_pages,
    )

    new_df = clean_hourly_data(new_df)

    if old_df.empty:
        combined = new_df
    else:
        old_df = clean_hourly_data(old_df)
        combined = pd.concat([old_df, new_df], ignore_index=True)

    combined = combined.drop_duplicates().reset_index(drop=True)
    combined = _trim_hourly_window(combined, days=30)

    save_csv(combined, processed_path)
    return combined


def update_daily_data(api_key: str, limit: int = 1000, max_pages: int | None = 1) -> pd.DataFrame:
    processed_path = BASE_DIR / "data/processed/daily_clean.csv"
    old_df = _safe_read_csv(processed_path)

    if old_df.empty:
        end_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(days=365 * 2)
    else:
        old_df["monitordate"] = pd.to_datetime(old_df["monitordate"], errors="coerce")
        last_dt = old_df["monitordate"].max()

        # 往前退1天，避免邊界漏資料
        start_dt = last_dt - timedelta(days=1)
        end_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    filters = (
        f"monitordate,GR,{start_dt.strftime('%Y-%m-%d')}"
        f"|monitordate,LE,{end_dt.strftime('%Y-%m-%d')}"
    )

    new_df = _fetch_paginated_csv_data(
        dataset_code="aqx_p_434",
        api_key=api_key,
        filters=filters,
        sort="monitordate desc",
        limit=limit,
        max_pages=max_pages,
    )

    new_df = clean_daily_data(new_df)

    if old_df.empty:
        combined = new_df
    else:
        old_df = clean_daily_data(old_df)
        combined = pd.concat([old_df, new_df], ignore_index=True)

    combined = combined.drop_duplicates().reset_index(drop=True)
    combined = _trim_daily_window(combined, days=365 * 2)

    save_csv(combined, processed_path)
    return combined


def update_all_data(api_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    hourly_df = update_hourly_data(api_key)
    daily_df = update_daily_data(api_key)
    return hourly_df, daily_df