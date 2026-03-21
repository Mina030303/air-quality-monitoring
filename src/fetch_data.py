from __future__ import annotations

import io
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

BASE_URL = "https://data.moenv.gov.tw/api/v2"
PAGE_SIZE = 1000


def _read_csv_response(text: str) -> pd.DataFrame:
    if not text or not text.strip():
        return pd.DataFrame()

    stripped = text.lstrip("\ufeff").strip()

    df = pd.read_csv(
        io.StringIO(stripped),
        engine="python",
        sep=",",
        quotechar='"',
        on_bad_lines="warn",
    )
    df.columns = df.columns.str.strip()
    return df


def _fetch_paginated_csv_data(
    dataset_code: str,
    api_key: str,
    filters: Optional[str] = None,
    sort: Optional[str] = None,
    limit: int = PAGE_SIZE,
    max_pages: Optional[int] = None,
) -> pd.DataFrame:

    all_pages = []
    offset = 0
    page_count = 0

    with requests.Session() as session:
        while True:
            params = {
                "format": "csv",
                "offset": offset,
                "limit": limit,
                "api_key": api_key,
            }

            if filters:
                params["filters"] = filters
            if sort:
                params["sort"] = sort

            response = session.get(
                f"{BASE_URL}/{dataset_code}",
                params=params,
                timeout=30,
            )
            response.raise_for_status()

            df = _read_csv_response(response.text)

            if df.empty:
                break

            all_pages.append(df)
            page_count += 1

            if len(df) < limit:
                break

            if max_pages is not None and page_count >= max_pages:
                break

            offset += limit

    if not all_pages:
        return pd.DataFrame()

    return pd.concat(all_pages, ignore_index=True).drop_duplicates()


# ---------- HOURLY ----------

def fetch_recent_30d_hourly_data(
    api_key: str,
    limit: int = PAGE_SIZE,
    max_pages: int | None = None,
) -> pd.DataFrame:

    end_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=30)

    filters = (
        f"datacreationdate,GR,{start_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        f"|datacreationdate,LE,{end_dt.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    return _fetch_paginated_csv_data(
        "aqx_p_488",
        api_key,
        filters,
        "datacreationdate desc",
        limit,
        max_pages,
    )


# ---------- DAILY ----------

def fetch_recent_2y_daily_data(
    api_key: str,
    limit: int = PAGE_SIZE,
    max_pages: int | None = None,
) -> pd.DataFrame:

    end_dt = datetime.now().replace(hour=0, minute=0, second=0)
    start_dt = end_dt - timedelta(days=365 * 2)

    filters = (
        f"monitordate,GR,{start_dt.strftime('%Y-%m-%d')}"
        f"|monitordate,LE,{end_dt.strftime('%Y-%m-%d')}"
    )

    return _fetch_paginated_csv_data(
        "aqx_p_434",
        api_key,
        filters,
        "monitordate desc",
        limit,
        max_pages,
    )