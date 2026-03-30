from __future__ import annotations

import io
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

# base URL for MOENV open data API
BASE_URL = "https://data.moenv.gov.tw/api/v2"
PAGE_SIZE = 1000

# convert raw CSV text returned by the API into a df
def _read_csv_response(text: str) -> pd.DataFrame:
    if not text or not text.strip():
        return pd.DataFrame()

    # remove BOM and surrounding whitespace
    stripped = text.lstrip("\ufeff").strip()

    # parse CSV string into df
    df = pd.read_csv(
        io.StringIO(stripped),
        engine="python",
        sep=",",
        quotechar='"',
        on_bad_lines="warn",
    )

    # normalize column names
    df.columns = df.columns.str.strip()
    return df


# generic function to fetch paginated CSV data from the API
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

    # reuse HTTP connection for multiple requests
    with requests.Session() as session:
        while True:
            params = {
                "format": "csv",
                "offset": offset,
                "limit": limit,
                "api_key": api_key,
            }

            # add optional filters and sorting
            if filters:
                params["filters"] = filters
            if sort:
                params["sort"] = sort

            # send request
            response = session.get(
                f"{BASE_URL}/{dataset_code}",
                params=params,
                timeout=30,
                verify=False,  # Disable SSL verification for Windows environments
            )
            response.raise_for_status()

            df = _read_csv_response(response.text)

            if df.empty:
                break

            all_pages.append(df)
            page_count += 1

            # stop if last page
            if len(df) < limit:
                break

            # stop if page limit reached
            if max_pages is not None and page_count >= max_pages:
                break

            offset += limit

    if not all_pages:
        return pd.DataFrame()

    # combine all pages and remove duplicates
    return pd.concat(all_pages, ignore_index=True).drop_duplicates()


# ---------- HOURLY ----------

# fetch hourly data for the most recent 30 days
def fetch_recent_30d_hourly_data(
    api_key: str,
    limit: int = PAGE_SIZE,
    max_pages: int | None = None,
) -> pd.DataFrame:

    # define time range (last 30 days)
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
        "datacreationdate desc",  # newest 
        limit,
        max_pages,
    )


# ---------- DAILY ----------

# fetch daily data for the most recent 2 years
def fetch_recent_2y_daily_data(
    api_key: str,
    limit: int = PAGE_SIZE,
    max_pages: int | None = None,
) -> pd.DataFrame:

    # define time range (last 2 years)
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
        "monitordate desc",   # newest 
        limit,
        max_pages,
    )