"""
Database module for Neon PostgreSQL connection and AQI data management.
Handles connection pooling, schema initialization, and batch upsert operations.
"""

import os
import logging
from contextlib import contextmanager
from typing import List, Tuple, Optional

import psycopg2
from psycopg2 import pool, sql, extras
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

# Load environment variables from .env
load_dotenv()

logger = logging.getLogger(__name__)

# Connection pool for efficient connection reuse
_connection_pool: Optional[pool.SimpleConnectionPool] = None


def get_connection_pool() -> pool.SimpleConnectionPool:
    """
    Get or create a connection pool to Neon PostgreSQL.
    
    Returns:
        SimpleConnectionPool: Reusable connection pool
        
    Raises:
        ValueError: If DATABASE_URL environment variable is not set
        psycopg2.Error: If connection to database fails
    """
    global _connection_pool
    
    if _connection_pool is not None:
        return _connection_pool
    
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    # Ensure sslmode=require for Neon security
    if "sslmode" not in database_url:
        delimiter = "&" if "?" in database_url else "?"
        database_url = f"{database_url}{delimiter}sslmode=require"
    
    try:
        _connection_pool = pool.SimpleConnectionPool(
            1,  # Minimum connections
            10,  # Maximum connections
            database_url,
        )
        logger.info("Connection pool created successfully")
        return _connection_pool
    except psycopg2.Error as e:
        logger.error(f"Failed to create connection pool: {e}")
        raise


@contextmanager
def get_db_connection():
    """
    Context manager for acquiring and releasing database connections from the pool.
    
    Yields:
        psycopg2.connection: Database connection object
        
    Example:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM hourly_aqi")
    """
    pool_obj = get_connection_pool()
    conn = pool_obj.getconn()
    try:
        yield conn
    finally:
        pool_obj.putconn(conn)


def init_db() -> None:
    """
    Initialize database schema by creating hourly_aqi table if it doesn't exist.
    
    Creates:
        - hourly_aqi table with proper columns and constraints
        - UNIQUE constraint on (site_name, publish_time)
        - Default timestamp for created_at
        
    Raises:
        psycopg2.Error: If table creation fails
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS hourly_aqi (
        id SERIAL PRIMARY KEY,
        site_name VARCHAR(255) NOT NULL,
        county VARCHAR(255),
        aqi INTEGER,
        status VARCHAR(50),
        publish_time TIMESTAMP NOT NULL,
        longitude DECIMAL(10, 6),
        latitude DECIMAL(10, 6),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(site_name, publish_time)
    );
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                conn.commit()
            logger.info("Database schema initialized successfully")
    except psycopg2.Error as e:
        logger.error(f"Failed to initialize database schema: {e}")
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def upsert_aqi(data_list: List[Tuple]) -> int:
    """
    Batch insert or update AQI records using INSERT ... ON CONFLICT.
    
    Uses psycopg2.extras.execute_values for high-performance batch insertion.
    If a record with the same (site_name, publish_time) exists, it updates
    the aqi and status columns instead of failing.
    
    Args:
        data_list: List of tuples in format:
                   (site_name, county, aqi, status, publish_time, longitude, latitude)
                   
    Returns:
        int: Number of rows inserted/updated
        
    Raises:
        psycopg2.Error: If upsert operation fails
        ValueError: If data_list is empty
        
    Example:
        data = [
            ("台北監測站", "台北市", 45, "良好", "2026-03-30 12:00:00", 121.5, 25.0),
            ("高雄監測站", "高雄市", 78, "普通", "2026-03-30 12:00:00", 120.3, 22.6),
        ]
        num_inserted = upsert_aqi(data)
        print(f"Upserted {num_inserted} records")
    """
    if not data_list:
        logger.warning("Empty data list provided to upsert_aqi")
        return 0
    
    upsert_sql = """
    INSERT INTO hourly_aqi 
    (site_name, county, aqi, status, publish_time, longitude, latitude)
    VALUES %s
    ON CONFLICT (site_name, publish_time) 
    DO UPDATE SET
        aqi = EXCLUDED.aqi,
        status = EXCLUDED.status,
        county = EXCLUDED.county,
        longitude = EXCLUDED.longitude,
        latitude = EXCLUDED.latitude
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Use execute_values for high-performance batch insertion
                extras.execute_values(
                    cursor,
                    upsert_sql,
                    data_list,
                    template=None,
                    fetch=False
                )
                conn.commit()
                row_count = cursor.rowcount
            logger.info(f"Successfully upserted {row_count} records")
            return row_count
    except psycopg2.Error as e:
        logger.error(f"Failed to upsert AQI data: {e}")
        raise


def fetch_aqi_data(
    limit: Optional[int] = None,
    site_name: Optional[str] = None,
    days: Optional[int] = None
) -> List[dict]:
    """
    Fetch AQI records from the database.
    
    Args:
        limit: Maximum number of records to fetch (None for no limit)
        site_name: Filter by specific station name (None for all stations)
        days: Fetch records from the last N days (None for all records)
        
    Returns:
        List of dictionaries with record data
        
    Raises:
        psycopg2.Error: If fetch operation fails
    """
    query = """
    SELECT id, site_name, county, aqi, status, publish_time, 
           longitude, latitude, created_at
    FROM hourly_aqi
    WHERE 1=1
    """
    params = []
    
    if site_name:
        query += " AND site_name = %s"
        params.append(site_name)
    
    if days:
        query += " AND publish_time >= NOW() - INTERVAL '%s days'"
        params.append(days)
    
    query += " ORDER BY publish_time DESC"
    
    if limit:
        query += " LIMIT %s"
        params.append(limit)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
            logger.info(f"Fetched {len(results)} AQI records")
            return [dict(row) for row in results]
    except psycopg2.Error as e:
        logger.error(f"Failed to fetch AQI data: {e}")
        raise


def get_latest_aqi() -> List[dict]:
    """
    Fetch the latest AQI record for each monitoring station.
    
    Returns:
        List of latest AQI records per station
        
    Raises:
        psycopg2.Error: If fetch operation fails
    """
    query = """
    SELECT DISTINCT ON (site_name) 
           id, site_name, county, aqi, status, publish_time,
           longitude, latitude, created_at
    FROM hourly_aqi
    ORDER BY site_name, publish_time DESC
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            return [dict(row) for row in results]
    except psycopg2.Error as e:
        logger.error(f"Failed to fetch latest AQI data: {e}")
        raise


def close_connection_pool() -> None:
    """
    Close all connections in the pool.
    Call this when shutting down the application.
    """
    global _connection_pool
    
    if _connection_pool is not None:
        try:
            _connection_pool.closeall()
            _connection_pool = None
            logger.info("Connection pool closed successfully")
        except psycopg2.Error as e:
            logger.error(f"Error closing connection pool: {e}")


# ============================================================================
# DAILY AQI FUNCTIONS
# ============================================================================

def init_daily_db() -> None:
    """
    Initialize database schema by creating daily_aqi table if it doesn't exist.
    
    Creates:
        - daily_aqi table with proper columns and constraints
        - UNIQUE constraint on (site_name, monitor_date)
        - Default timestamp for created_at
        
    Raises:
        psycopg2.Error: If table creation fails
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS daily_aqi (
        id SERIAL PRIMARY KEY,
        site_name VARCHAR(255) NOT NULL,
        county VARCHAR(255),
        aqi INTEGER,
        status VARCHAR(50),
        monitor_date DATE NOT NULL,
        longitude DECIMAL(10, 6),
        latitude DECIMAL(10, 6),
        so2 DECIMAL(10, 2),
        co DECIMAL(10, 2),
        o3 DECIMAL(10, 2),
        pm10 DECIMAL(10, 2),
        pm2_5 DECIMAL(10, 2),
        no2 DECIMAL(10, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(site_name, monitor_date)
    );
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                conn.commit()
            logger.info("Daily database schema initialized successfully")
    except psycopg2.Error as e:
        logger.error(f"Failed to initialize daily database schema: {e}")
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def upsert_daily_aqi(data_list: List[Tuple]) -> int:
    """
    Batch insert or update daily AQI records using INSERT ... ON CONFLICT.
    
    Uses psycopg2.extras.execute_values for high-performance batch insertion.
    If a record with the same (site_name, monitor_date) exists, it updates
    the aqi, status, and pollutant columns instead of failing.
    
    Args:
        data_list: List of tuples in format:
                   (site_name, county, aqi, status, monitor_date, longitude, latitude, 
                    so2, co, o3, pm10, pm2_5, no2)
                   
    Returns:
        int: Number of rows inserted/updated
        
    Raises:
        psycopg2.Error: If upsert operation fails
        ValueError: If data_list is empty
    """
    if not data_list:
        logger.warning("Empty data list provided to upsert_daily_aqi")
        return 0
    
    upsert_sql = """
    INSERT INTO daily_aqi 
    (site_name, county, aqi, status, monitor_date, longitude, latitude, 
     so2, co, o3, pm10, pm2_5, no2)
    VALUES %s
    ON CONFLICT (site_name, monitor_date) 
    DO UPDATE SET
        aqi = EXCLUDED.aqi,
        status = EXCLUDED.status,
        county = EXCLUDED.county,
        longitude = EXCLUDED.longitude,
        latitude = EXCLUDED.latitude,
        so2 = EXCLUDED.so2,
        co = EXCLUDED.co,
        o3 = EXCLUDED.o3,
        pm10 = EXCLUDED.pm10,
        pm2_5 = EXCLUDED.pm2_5,
        no2 = EXCLUDED.no2
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Use execute_values for high-performance batch insertion
                extras.execute_values(
                    cursor,
                    upsert_sql,
                    data_list,
                    template=None,
                    fetch=False
                )
                conn.commit()
                row_count = cursor.rowcount
            logger.info(f"Successfully upserted {row_count} daily records")
            return row_count
    except psycopg2.Error as e:
        logger.error(f"Failed to upsert daily AQI data: {e}")
        raise


def fetch_daily_aqi_data(
    limit: Optional[int] = None,
    site_name: Optional[str] = None,
    days: Optional[int] = None
) -> List[dict]:
    """
    Fetch daily AQI records from the database.
    
    Args:
        limit: Maximum number of records to fetch (None for no limit)
        site_name: Filter by specific station name (None for all stations)
        days: Fetch records from the last N days (None for all records)
        
    Returns:
        List of dictionaries with record data
        
    Raises:
        psycopg2.Error: If fetch operation fails
    """
    query = """
    SELECT id, site_name, county, aqi, status, monitor_date,
           longitude, latitude, so2, co, o3, pm10, pm2_5, no2, created_at
    FROM daily_aqi
    WHERE 1=1
    """
    params = []
    
    if site_name:
        query += " AND site_name = %s"
        params.append(site_name)
    
    if days:
        query += " AND monitor_date >= CURRENT_DATE - INTERVAL '%s days'"
        params.append(days)
    
    query += " ORDER BY monitor_date DESC"
    
    if limit:
        query += " LIMIT %s"
        params.append(limit)
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
            logger.info(f"Fetched {len(results)} daily AQI records")
            return [dict(row) for row in results]
    except psycopg2.Error as e:
        logger.error(f"Failed to fetch daily AQI data: {e}")
        raise


def get_latest_daily_aqi() -> List[dict]:
    """
    Fetch the latest daily AQI record for each monitoring station.
    
    Returns:
        List of latest daily AQI records per station
        
    Raises:
        psycopg2.Error: If fetch operation fails
    """
    query = """
    SELECT DISTINCT ON (site_name) 
           id, site_name, county, aqi, status, monitor_date,
           longitude, latitude, so2, co, o3, pm10, pm2_5, no2, created_at
    FROM daily_aqi
    ORDER BY site_name, monitor_date DESC
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            return [dict(row) for row in results]
    except psycopg2.Error as e:
        logger.error(f"Failed to fetch latest daily AQI data: {e}")
        raise



if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Initialize database schema
        print("Initializing database schema...")
        init_db()
        print("✓ Database initialized")
        
        # Example data for testing
        sample_data = [
            ("台北監測站", "台北市", 45, "良好", "2026-03-30 12:00:00", 121.5, 25.0),
            ("新北監測站", "新北市", 52, "普通", "2026-03-30 12:00:00", 121.35, 25.05),
            ("高雄監測站", "高雄市", 78, "普通", "2026-03-30 12:00:00", 120.3, 22.6),
        ]
        
        # Upsert sample data
        print("Upserting sample data...")
        num_inserted = upsert_aqi(sample_data)
        print(f"✓ Upserted {num_inserted} records")
        
        # Fetch latest data
        print("Fetching latest data...")
        latest = get_latest_aqi()
        print(f"✓ Found {len(latest)} stations")
        for record in latest:
            print(f"  - {record['site_name']}: AQI={record['aqi']} ({record['status']})")
        
    except Exception as e:
        print(f"✗ Error: {e}")
    finally:
        close_connection_pool()
