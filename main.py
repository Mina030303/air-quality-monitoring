from pathlib import Path
import os
import logging
from datetime import datetime

from dotenv import load_dotenv
from pydantic import ValidationError

from src.crawler import fetch_hourly_aqi, fetch_daily_aqi, save_to_csv
from src.models import AQIRecord, AQIRecordList, DailyAQIRecord, DailyAQIRecordList
from src.database import (
    init_db, upsert_aqi, close_connection_pool,
    init_daily_db, upsert_daily_aqi
)
from src.alerts import send_discord_alert
from src.update_data import update_all_data
from src.save_data import save_csv
from src.analyze_data import (
    daily_avg_aqi,
    avg_aqi_by_county,
    high_pollution_hours,
    high_pollution_hour_ratio,
    high_pollution_hour_ratio_by_county,
    time_structure_analysis,
    current_status_interpretation,
    detect_pollution_spikes,
    spike_summary_by_county,
    spike_summary_by_site,
    spike_time_pattern,
    calculate_county_risk_score
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def fetch_and_validate_data(api_key: str) -> tuple[list[AQIRecord], list[dict]]:
    """
    Fetch data from MOENV API and validate using AQIRecord model.
    
    Args:
        api_key: MOENV API key
        
    Returns:
        Tuple of (valid_records, invalid_records)
        - valid_records: List of AQIRecord objects
        - invalid_records: List of dicts with raw data and error info
    """
    logger.info("Fetching AQI data from MOENV API...")
    
    try:
        raw_data = fetch_hourly_aqi(api_key)
        logger.info(f"Fetched {len(raw_data)} records from API")
    except Exception as e:
        error_msg = f"🚨 **[AQI Pipeline Error]** Failed to fetch hourly API data: {str(e)}"
        logger.error(error_msg)
        send_discord_alert(error_msg)
        raise
    
    valid_records = []
    invalid_records = []
    
    for idx, data in enumerate(raw_data):
        try:
            # Try to parse from API JSON format
            record = AQIRecord.from_api_json(data)
            valid_records.append(record)
        except ValidationError as e:
            # Log validation error and skip this record
            error_info = {
                "raw_data": data,
                "error": str(e),
                "error_count": e.error_count(),
            }
            invalid_records.append(error_info)
            logger.warning(f"Validation error on record {idx}: {e.error_count()} issue(s)")
            # Only show detailed errors for first few records
            if idx < 3:
                logger.warning(f"  Sample data: {data}")
                logger.warning(f"  Full error: {e}")
        except Exception as e:
            # Unexpected error
            error_info = {
                "raw_data": data,
                "error": f"Unexpected error: {str(e)}",
            }
            invalid_records.append(error_info)
            logger.error(f"Unexpected error processing record {idx}: {e}")
    
    logger.info(f"Validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid")
    
    return valid_records, invalid_records


def save_hot_data(records: list[AQIRecord]) -> None:
    """
    Save valid records to CSV (hot data for Streamlit).
    
    Overwrites the CSV with the latest batch for immediate Streamlit display.
    
    Args:
        records: List of validated AQIRecord objects
    """
    if not records:
        logger.warning("No records to save to hot data CSV")
        return
    
    csv_path = BASE_DIR / "data" / "hourly_aqi.csv"
    
    try:
        # Convert AQIRecord objects to dictionaries for save_to_csv
        record_dicts = [record.model_dump(mode='python') for record in records]
        
        # Append to existing CSV (merge strategy)
        save_to_csv(record_dicts, csv_path)
        logger.info(f"Saved {len(records)} records to hot data: {csv_path}")
    except Exception as e:
        logger.error(f"Failed to save hot data: {e}")
        raise


def sync_cold_data(records: list[AQIRecord]) -> None:
    """
    Sync valid records to Neon PostgreSQL database (cold data).
    
    Uses upsert strategy to avoid duplicates and ensure data consistency.
    
    Args:
        records: List of validated AQIRecord objects
    """
    if not records:
        logger.warning("No records to sync to database")
        return
    
    try:
        # Initialize database schema if needed
        logger.info("Initializing database schema...")
        init_db()
        
        # Convert records to database tuples
        db_tuples = [record.to_db_tuple() for record in records]
        
        # Upsert to database
        logger.info(f"Upserting {len(db_tuples)} records to Neon PostgreSQL...")
        num_inserted = upsert_aqi(db_tuples)
        logger.info(f"Successfully synced {num_inserted} records to database")
        
    except Exception as e:
        error_msg = f"🚨 **[AQI Pipeline Error]** Failed to sync hourly data to database: {str(e)}"
        logger.error(error_msg)
        send_discord_alert(error_msg)
        raise
    finally:
        close_connection_pool()


def fetch_and_validate_daily_data(api_key: str) -> tuple[list[DailyAQIRecord], list[dict]]:
    """
    Fetch daily AQI data from MOENV API and validate using DailyAQIRecord model.
    
    Args:
        api_key: MOENV API key
        
    Returns:
        Tuple of (valid_records, invalid_records)
        - valid_records: List of DailyAQIRecord objects
        - invalid_records: List of dicts with raw data and error info
    """
    logger.info("Fetching daily AQI data from MOENV API...")
    
    try:
        raw_data = fetch_daily_aqi(api_key)
        logger.info(f"Fetched {len(raw_data)} daily records from API")
    except Exception as e:
        logger.error(f"Failed to fetch daily data from API: {e}")
        raise
    
    valid_records = []
    invalid_records = []
    
    for idx, data in enumerate(raw_data):
        try:
            # Try to parse from API JSON format
            record = DailyAQIRecord.from_api_json(data)
            valid_records.append(record)
        except ValidationError as e:
            # Log validation error and skip this record
            error_info = {
                "raw_data": data,
                "error": str(e),
                "error_count": e.error_count(),
            }
            invalid_records.append(error_info)
            logger.warning(f"Daily validation error on record {idx}: {e.error_count()} issue(s)")
            # Show detailed errors for first few records at WARNING level
            if idx < 3:
                logger.warning(f"  Sample API data: {data}")
                logger.warning(f"  Full validation error: {e}")
        except Exception as e:
            # Unexpected error
            error_info = {
                "raw_data": data,
                "error": f"Unexpected error: {str(e)}",
            }
            invalid_records.append(error_info)
            logger.error(f"Unexpected error processing daily record {idx}: {e}")
    
    logger.info(f"Daily validation complete: {len(valid_records)} valid, {len(invalid_records)} invalid")
    
    return valid_records, invalid_records


def save_daily_hot_data(records: list[DailyAQIRecord]) -> None:
    """
    Save valid daily records to CSV (hot data for Streamlit).
    
    Args:
        records: List of validated DailyAQIRecord objects
    """
    if not records:
        logger.warning("No daily records to save to hot data CSV")
        return
    
    csv_path = BASE_DIR / "data" / "daily_aqi.csv"
    
    try:
        # Convert DailyAQIRecord objects to dictionaries for save_to_csv
        record_dicts = [record.model_dump(mode='python') for record in records]
        
        # Append to existing CSV (merge strategy)
        save_to_csv(record_dicts, csv_path)
        logger.info(f"Saved {len(records)} daily records to hot data: {csv_path}")
    except Exception as e:
        logger.error(f"Failed to save daily hot data: {e}")
        raise


def sync_daily_cold_data(records: list[DailyAQIRecord]) -> None:
    """
    Sync valid daily records to Neon PostgreSQL database (cold data).
    
    Uses upsert strategy to avoid duplicates and ensure data consistency.
    
    Args:
        records: List of validated DailyAQIRecord objects
    """
    if not records:
        logger.warning("No daily records to sync to database")
        return
    
    try:
        # Initialize database schema if needed
        logger.info("Initializing daily database schema...")
        init_daily_db()
        
        # Convert records to database tuples
        db_tuples = [record.to_db_tuple() for record in records]
        
        # Upsert to database
        logger.info(f"Upserting {len(db_tuples)} daily records to Neon PostgreSQL...")
        num_inserted = upsert_daily_aqi(db_tuples)
        logger.info(f"Successfully synced {num_inserted} daily records to database")
        
    except Exception as e:
        logger.error(f"Failed to sync daily cold data to database: {e}")
        raise
    finally:
        close_connection_pool()


def run_analysis_pipeline(hourly_clean) -> None:
    """
    Run the analysis pipeline on cleaned data.
    
    Args:
        hourly_clean: Cleaned hourly AQI dataframe
    """
    logger.info("Starting analysis pipeline...")
    
    try:
        trend_df = daily_avg_aqi(hourly_clean)
        county_df = avg_aqi_by_county(hourly_clean)
        risk_df = high_pollution_hours(hourly_clean)
        hour_ratio_df = high_pollution_hour_ratio(hourly_clean)
        hour_ratio_county_df = high_pollution_hour_ratio_by_county(hourly_clean)
        time_daily_df, weekday_vs_weekend_df, monthly_avg_df = time_structure_analysis(hourly_clean)
        status_text = current_status_interpretation(time_daily_df)

        # Save analysis results
        save_csv(trend_df, BASE_DIR / "output/tables/daily_trend.csv")
        save_csv(county_df, BASE_DIR / "output/tables/county_avg.csv")
        save_csv(risk_df, BASE_DIR / "output/tables/high_pollution_hours.csv")
        save_csv(hour_ratio_df, BASE_DIR / "output/tables/high_pollution_hour_ratio.csv")
        save_csv(hour_ratio_county_df, BASE_DIR / "output/tables/high_pollution_hour_ratio_by_county.csv")
        save_csv(time_daily_df, BASE_DIR / "output/tables/daily_time_structure.csv")
        save_csv(weekday_vs_weekend_df, BASE_DIR / "output/tables/weekday_vs_weekend.csv")
        save_csv(monthly_avg_df, BASE_DIR / "output/tables/monthly_avg.csv")

        # County risk analysis
        logger.info("Calculating county risk scores...")
        county_risk_df = calculate_county_risk_score(hourly_clean)
        save_csv(county_risk_df, BASE_DIR / "output/tables/county_risk_score.csv")

        # Spike detection
        logger.info("Running spike detection...")
        spikes_df = detect_pollution_spikes(
            hourly_clean, 
            pollutant_col="aqi",
            method="rolling_threshold",
            rolling_window=24,
            threshold_ratio=1.5,
            zscore_threshold=2.5,
            min_value=50.0
        )
        
        spike_county_df = spike_summary_by_county(spikes_df)
        spike_site_df = spike_summary_by_site(spikes_df)
        spike_hour_df = spike_time_pattern(spikes_df)
        
        save_csv(spikes_df, BASE_DIR / "output/tables/pollution_spikes.csv")
        save_csv(spike_county_df, BASE_DIR / "output/tables/spike_by_county.csv")
        save_csv(spike_site_df, BASE_DIR / "output/tables/spike_by_site.csv")
        save_csv(spike_hour_df, BASE_DIR / "output/tables/spike_by_hour.csv")
        
        logger.info("Analysis pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Error in analysis pipeline: {e}")
        raise


def main():
    """
    Main entry point implementing Hot/Cold data strategy.
    
    Flow:
    1. Fetch raw data from MOENV API
    2. Validate each record using AQIRecord model
    3. Save valid records to CSV (hot data)
    4. Sync valid records to PostgreSQL (cold data)
    5. Run analysis pipeline on combined historical + realtime data
    """
    logger.info("=" * 70)
    logger.info("Taiwan Air Quality Monitoring Platform - Data Pipeline Started")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 70)
    
    api_key = os.getenv("API_KEY")
    if not api_key:
        logger.error("API_KEY environment variable is not set")
        raise ValueError("API_KEY missing")
    
    try:
        # Step 1: Fetch and validate hourly data
        logger.info("\n[STEP 1a] Fetching and validating hourly AQI data...")
        hourly_valid, hourly_invalid = fetch_and_validate_data(api_key)
        
        if not hourly_valid:
            logger.warning("No valid hourly records obtained.")
        else:
            logger.info(f"✓ Obtained {len(hourly_valid)} valid hourly records")
            if hourly_invalid:
                logger.info(f"⚠ Skipped {len(hourly_invalid)} invalid hourly records")
        
        # Step 1b: Fetch and validate daily data
        logger.info("\n[STEP 1b] Fetching and validating daily AQI data...")
        daily_valid, daily_invalid = fetch_and_validate_daily_data(api_key)
        
        if not daily_valid:
            logger.warning("No valid daily records obtained.")
        else:
            logger.info(f"✓ Obtained {len(daily_valid)} valid daily records")
            if daily_invalid:
                logger.info(f"⚠ Skipped {len(daily_invalid)} invalid daily records")
        
        # Ensure we have at least some valid data
        if not hourly_valid and not daily_valid:
            logger.warning("No valid records obtained. Aborting pipeline.")
            return
        
        # Step 2a: Save hourly hot data (CSV for Streamlit)
        logger.info("\n[STEP 2a] Saving hourly hot data (CSV for Streamlit)...")
        if hourly_valid:
            try:
                save_hot_data(hourly_valid)
                logger.info("✓ Hourly hot data saved successfully")
            except Exception as e:
                logger.error(f"Failed to save hourly hot data: {e}")
                # Continue to other steps even if hot data fails
        else:
            logger.info("⊘ Skipping hourly hot data save (no valid records)")
        
        # Step 2b: Save daily hot data (CSV for Streamlit)
        logger.info("\n[STEP 2b] Saving daily hot data (CSV for Streamlit)...")
        if daily_valid:
            try:
                save_daily_hot_data(daily_valid)
                logger.info("✓ Daily hot data saved successfully")
            except Exception as e:
                logger.error(f"Failed to save daily hot data: {e}")
                # Continue to other steps even if hot data fails
        else:
            logger.info("⊘ Skipping daily hot data save (no valid records)")
        
        # Step 3a: Sync hourly cold data (PostgreSQL)
        logger.info("\n[STEP 3a] Syncing hourly cold data (PostgreSQL database)...")
        if hourly_valid:
            try:
                sync_cold_data(hourly_valid)
                logger.info("✓ Hourly cold data synced successfully")
            except Exception as e:
                logger.error(f"Failed to sync hourly cold data: {e}")
                # Continue with other steps even if database sync fails
        else:
            logger.info("⊘ Skipping hourly cold data sync (no valid records)")
        
        # Step 3b: Sync daily cold data (PostgreSQL)
        logger.info("\n[STEP 3b] Syncing daily cold data (PostgreSQL database)...")
        if daily_valid:
            try:
                sync_daily_cold_data(daily_valid)
                logger.info("✓ Daily cold data synced successfully")
            except Exception as e:
                logger.error(f"Failed to sync daily cold data: {e}")
                # Continue with analysis pipeline even if database sync fails
        else:
            logger.info("⊘ Skipping daily cold data sync (no valid records)")
        
        # Step 4: Load combined data and run analysis
        logger.info("\n[STEP 4] Loading combined data and running analysis pipeline...")
        try:
            hourly_clean, daily_clean = update_all_data(api_key)
            logger.info(f"Loaded hourly data: {hourly_clean.shape}")
            logger.info(f"Loaded daily data: {daily_clean.shape}")
            
            run_analysis_pipeline(hourly_clean)
            logger.info("✓ Analysis pipeline completed")
        except Exception as e:
            logger.error(f"Failed to run analysis pipeline: {e}")
            raise
        
        logger.info("\n" + "=" * 70)
        logger.info("Pipeline completed successfully")
        logger.info("=" * 70)
        
    except Exception as e:
        error_msg = f"🚨🚨 **[CRITICAL]** Taiwan AQI Pipeline completely failed: {str(e)}"
        logger.critical(error_msg)
        send_discord_alert(error_msg)
        logger.info("=" * 70)
        raise


if __name__ == "__main__":
    main()