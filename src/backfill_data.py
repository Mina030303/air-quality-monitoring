from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from pydantic import ValidationError

from database import (
    close_connection_pool,
    get_db_connection,
    init_daily_db,
    init_db,
    upsert_aqi,
    upsert_daily_aqi,
)
from fetch_data import fetch_daily_history_range, fetch_hourly_history_range
from models import AQIRecord, DailyAQIRecord

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _iter_windows(total_days: int, window_days: int) -> list[tuple[datetime, datetime, int]]:
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=total_days)
    windows: list[tuple[datetime, datetime, int]] = []

    covered_days = 0
    cursor = start
    while cursor < now:
        next_cursor = min(cursor + timedelta(days=window_days), now)
        span_days = max(1, (next_cursor - cursor).days)
        covered_days = min(total_days, covered_days + span_days)
        windows.append((cursor, next_cursor, covered_days))
        cursor = next_cursor

    return windows


def _flush_hourly_batch(batch: list[tuple], synced_total: int) -> int:
    if not batch:
        return synced_total

    deduped: dict[tuple, tuple] = {}
    for row in batch:
        # hourly unique constraint: (site_name, publish_time)
        key = (row[0], row[4])
        deduped[key] = row

    deduped_rows = list(deduped.values())
    if len(deduped_rows) < len(batch):
        logger.info(
            "[Backfill][Hourly] Batch dedup: %s -> %s rows",
            len(batch),
            len(deduped_rows),
        )

    upserted = upsert_aqi(deduped_rows)
    batch.clear()
    return synced_total + upserted


def _flush_daily_batch(batch: list[tuple], synced_total: int) -> int:
    if not batch:
        return synced_total

    deduped: dict[tuple, tuple] = {}
    for row in batch:
        # daily unique constraint: (site_name, monitor_date)
        key = (row[0], row[4])
        deduped[key] = row

    deduped_rows = list(deduped.values())
    if len(deduped_rows) < len(batch):
        logger.info(
            "[Backfill][Daily] Batch dedup: %s -> %s rows",
            len(batch),
            len(deduped_rows),
        )

    upserted = upsert_daily_aqi(deduped_rows)
    batch.clear()
    return synced_total + upserted


def _hourly_window_count(start_dt: datetime, end_dt: datetime) -> int:
    query = """
    SELECT COUNT(1)
    FROM hourly_aqi
    WHERE publish_time >= %s AND publish_time < %s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (start_dt, end_dt))
            result = cursor.fetchone()
    return int(result[0] if result else 0)


def _daily_window_count(start_dt: datetime, end_dt: datetime) -> int:
    query = """
    SELECT COUNT(1)
    FROM daily_aqi
    WHERE monitor_date >= %s::date AND monitor_date < %s::date
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (start_dt, end_dt))
            result = cursor.fetchone()
    return int(result[0] if result else 0)


def _sync_hourly_backfill(
    api_key: str,
    days: int,
    chunk_size: int = 2000,
    window_days: int = 7,
    max_pages: int = 2000,
    request_delay_seconds: int = 1,
    skip_existing: bool = False,
    skip_threshold: int = 1000,
) -> int:
    logger.info(
        "[Backfill][Hourly] Start AQX_P_488 backfill for last %s days (window_days=%s, chunk_size=%s, max_pages=%s)",
        days,
        window_days,
        chunk_size,
        max_pages,
    )
    init_db()

    synced_total = 0
    invalid_total = 0
    windows = _iter_windows(days, window_days)

    for start_dt, end_dt, covered_days in windows:
        if skip_existing:
            existing_count = _hourly_window_count(start_dt, end_dt)
            if existing_count >= skip_threshold:
                logger.info(
                    "[Backfill] Progress: Day %s/%s - Synced %s records (skip existing window, existing=%s)",
                    covered_days,
                    days,
                    synced_total,
                    existing_count,
                )
                continue

        hourly_df = fetch_hourly_history_range(
            api_key=api_key,
            start_dt=start_dt,
            end_dt=end_dt,
            limit=1000,
            max_pages=max_pages,
            request_delay_seconds=request_delay_seconds,
        )

        if hourly_df.empty:
            logger.info(
                "[Backfill] Progress: Day %s/%s - Synced %s records",
                covered_days,
                days,
                synced_total,
            )
            continue

        batch: list[tuple] = []
        for row in hourly_df.to_dict(orient="records"):
            try:
                record = AQIRecord.from_api_json(row)
                batch.append(record.to_db_tuple())
                if len(batch) >= chunk_size:
                    synced_total = _flush_hourly_batch(batch, synced_total)
            except ValidationError:
                invalid_total += 1
            except Exception:
                invalid_total += 1

        synced_total = _flush_hourly_batch(batch, synced_total)

        logger.info(
            "[Backfill] Progress: Day %s/%s - Synced %s records",
            covered_days,
            days,
            synced_total,
        )

    logger.info(
        "[Backfill][Hourly] Completed AQX_P_488 sync. synced=%s invalid=%s",
        synced_total,
        invalid_total,
    )
    return synced_total


def _sync_daily_backfill(
    api_key: str,
    days: int,
    chunk_size: int = 2000,
    window_days: int = 60,
    max_pages: int = 2000,
    request_delay_seconds: int = 1,
    skip_existing: bool = False,
    skip_threshold: int = 300,
) -> int:
    logger.info(
        "[Backfill][Daily] Start AQX_P_434 backfill for last %s days (window_days=%s, chunk_size=%s, max_pages=%s)",
        days,
        window_days,
        chunk_size,
        max_pages,
    )
    init_daily_db()

    synced_total = 0
    invalid_total = 0
    windows = _iter_windows(days, window_days)

    for start_dt, end_dt, covered_days in windows:
        if skip_existing:
            existing_count = _daily_window_count(start_dt, end_dt)
            if existing_count >= skip_threshold:
                logger.info(
                    "[Backfill] Progress: Day %s/%s - Synced %s records (skip existing window, existing=%s)",
                    covered_days,
                    days,
                    synced_total,
                    existing_count,
                )
                continue

        daily_df = fetch_daily_history_range(
            api_key=api_key,
            start_dt=start_dt,
            end_dt=end_dt,
            limit=1000,
            max_pages=max_pages,
            request_delay_seconds=request_delay_seconds,
        )

        if daily_df.empty:
            logger.info(
                "[Backfill] Progress: Day %s/%s - Synced %s records",
                covered_days,
                days,
                synced_total,
            )
            continue

        batch: list[tuple] = []
        for row in daily_df.to_dict(orient="records"):
            try:
                record = DailyAQIRecord.from_api_json(row)
                batch.append(record.to_db_tuple())
                if len(batch) >= chunk_size:
                    synced_total = _flush_daily_batch(batch, synced_total)
            except ValidationError:
                invalid_total += 1
            except Exception:
                invalid_total += 1

        synced_total = _flush_daily_batch(batch, synced_total)

        logger.info(
            "[Backfill] Progress: Day %s/%s - Synced %s records",
            covered_days,
            days,
            synced_total,
        )

    logger.info(
        "[Backfill][Daily] Completed AQX_P_434 sync. synced=%s invalid=%s",
        synced_total,
        invalid_total,
    )
    return synced_total


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Historical AQI backfill to Neon PostgreSQL")
    parser.add_argument("--hourly-days", type=int, default=180, help="Hourly backfill lookback days")
    parser.add_argument("--daily-days", type=int, default=365, help="Daily backfill lookback days")
    parser.add_argument("--hourly-window-days", type=int, default=7, help="Hourly API window size in days")
    parser.add_argument("--daily-window-days", type=int, default=60, help="Daily API window size in days")
    parser.add_argument("--chunk-size", type=int, default=2000, help="DB upsert chunk size")
    parser.add_argument("--max-pages", type=int, default=2000, help="Maximum pages fetched per API window")
    parser.add_argument("--request-delay-seconds", type=int, default=1, help="Delay seconds between API requests")
    parser.add_argument("--skip-existing", action="store_true", help="Enable resume logic that skips already-synced windows")
    parser.add_argument("--hourly-skip-threshold", type=int, default=1000, help="Skip hourly window if existing rows >= threshold")
    parser.add_argument("--daily-skip-threshold", type=int, default=300, help="Skip daily window if existing rows >= threshold")
    return parser.parse_args()


def main() -> None:
    logger.info("Starting historical backfill to Neon PostgreSQL...")
    args = _parse_args()
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("API_KEY missing in .env")

    logger.info(
        "[Backfill] Runtime config: hourly_days=%s daily_days=%s hourly_window_days=%s daily_window_days=%s chunk_size=%s max_pages=%s delay=%ss skip_existing=%s",
        args.hourly_days,
        args.daily_days,
        args.hourly_window_days,
        args.daily_window_days,
        args.chunk_size,
        args.max_pages,
        args.request_delay_seconds,
        args.skip_existing,
    )

    try:
        hourly_synced = _sync_hourly_backfill(
            api_key=api_key,
            days=args.hourly_days,
            chunk_size=args.chunk_size,
            window_days=args.hourly_window_days,
            max_pages=args.max_pages,
            request_delay_seconds=args.request_delay_seconds,
            skip_existing=args.skip_existing,
            skip_threshold=args.hourly_skip_threshold,
        )
        daily_synced = _sync_daily_backfill(
            api_key=api_key,
            days=args.daily_days,
            chunk_size=args.chunk_size,
            window_days=args.daily_window_days,
            max_pages=args.max_pages,
            request_delay_seconds=args.request_delay_seconds,
            skip_existing=args.skip_existing,
            skip_threshold=args.daily_skip_threshold,
        )

        logger.info(
            "Backfill completed. hourly_synced=%s, daily_synced=%s, total_synced=%s",
            hourly_synced,
            daily_synced,
            hourly_synced + daily_synced,
        )
    finally:
        close_connection_pool()


if __name__ == "__main__":
    main()
