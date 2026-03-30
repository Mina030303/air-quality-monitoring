from __future__ import annotations

import io
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests
import certifi
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from urllib3.exceptions import InsecureRequestWarning, ProtocolError

# base URL for MOENV open data API
BASE_URL = "https://data.moenv.gov.tw/api/v2"
PAGE_SIZE = 1000
REQUEST_DELAY_SECONDS = 1
HOURLY_HISTORY_DATASET_ID = "aqx_p_488"
DAILY_HISTORY_DATASET_ID = "aqx_p_434"

logger = logging.getLogger(__name__)
_SSL_FALLBACK_USED = False


def _request_with_tls_fallback(
    session: requests.Session,
    url: str,
    params: dict,
    timeout: int,
) -> requests.Response:
    global _SSL_FALLBACK_USED
    try:
        return session.get(url, params=params, timeout=timeout, verify=certifi.where())
    except requests.exceptions.SSLError:
        if not _SSL_FALLBACK_USED:
            logger.warning(
                "TLS certificate verification failed for MOENV API in this environment. "
                "Falling back to verify=False for compatibility."
            )
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
            _SSL_FALLBACK_USED = True
        return session.get(url, params=params, timeout=timeout, verify=False)

# convert raw CSV text returned by the API into a df
def _read_csv_response(text: str) -> pd.DataFrame:
    if not text or not text.strip():
        return pd.DataFrame()

    # MOENV may return plain-text errors with HTTP 200 (non-CSV payload).
    error_markers = ("查無資料", "資料庫錯誤", "api_key 不存在")
    if any(marker in text for marker in error_markers):
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
    request_delay_seconds: int = REQUEST_DELAY_SECONDS,
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
            response = _request_with_tls_fallback(
                session=session,
                url=f"{BASE_URL}/{dataset_code}",
                params=params,
                timeout=30,
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

            # Rate limit to reduce chance of MOENV throttling on long backfills.
            time.sleep(request_delay_seconds)
            offset += limit

    if not all_pages:
        return pd.DataFrame()

    # combine all pages and remove duplicates
    return pd.concat(all_pages, ignore_index=True).drop_duplicates()


# ---------- HOURLY ----------

# fetch hourly data for the most recent 30 days
@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, ProtocolError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=45),
    reraise=True,
)
def fetch_hourly_history_range(
    api_key: str,
    start_dt: datetime,
    end_dt: datetime,
    limit: int = PAGE_SIZE,
    max_pages: int | None = None,
    request_delay_seconds: int = REQUEST_DELAY_SECONDS,
) -> pd.DataFrame:
    """Fetch AQX_P_488 hourly history for a custom datetime range."""
    filters = (
        f"datacreationdate,GR,{start_dt.strftime('%Y-%m-%d %H:%M:%S')}"
        f"|datacreationdate,LE,{end_dt.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    return _fetch_paginated_csv_data(
        HOURLY_HISTORY_DATASET_ID,
        api_key,
        filters,
        "datacreationdate desc",
        limit,
        max_pages,
        request_delay_seconds,
    )


@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, ProtocolError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=45),
    reraise=True,
)
def fetch_recent_30d_hourly_data(
    api_key: str,
    limit: int = PAGE_SIZE,
    max_pages: int | None = None,
    request_delay_seconds: int = REQUEST_DELAY_SECONDS,
) -> pd.DataFrame:

    # define time range (last 30 days)
    end_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=30)

    return fetch_hourly_history_range(
        api_key=api_key,
        start_dt=start_dt,
        end_dt=end_dt,
        limit=limit,
        max_pages=max_pages,
        request_delay_seconds=request_delay_seconds,
    )


# ---------- DAILY ----------

# fetch daily data for the most recent 2 years
@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, ProtocolError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=45),
    reraise=True,
)
def fetch_daily_history_range(
    api_key: str,
    start_dt: datetime,
    end_dt: datetime,
    limit: int = PAGE_SIZE,
    max_pages: int | None = None,
    request_delay_seconds: int = REQUEST_DELAY_SECONDS,
) -> pd.DataFrame:
    """Fetch AQX_P_434 daily history for a custom date range."""
    filters = (
        f"monitordate,GR,{start_dt.strftime('%Y-%m-%d')}"
        f"|monitordate,LE,{end_dt.strftime('%Y-%m-%d')}"
    )

    return _fetch_paginated_csv_data(
        DAILY_HISTORY_DATASET_ID,
        api_key,
        filters,
        "monitordate desc",
        limit,
        max_pages,
        request_delay_seconds,
    )


@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, ProtocolError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=45),
    reraise=True,
)
def fetch_recent_2y_daily_data(
    api_key: str,
    limit: int = PAGE_SIZE,
    max_pages: int | None = None,
    request_delay_seconds: int = REQUEST_DELAY_SECONDS,
) -> pd.DataFrame:

    # define time range (last 2 years)
    end_dt = datetime.now().replace(hour=0, minute=0, second=0)
    start_dt = end_dt - timedelta(days=365 * 2)

    return fetch_daily_history_range(
        api_key=api_key,
        start_dt=start_dt,
        end_dt=end_dt,
        limit=limit,
        max_pages=max_pages,
        request_delay_seconds=request_delay_seconds,
    )
