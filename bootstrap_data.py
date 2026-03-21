from pathlib import Path
import os

from dotenv import load_dotenv

from src.fetch_data import (
    fetch_recent_30d_hourly_data,
    fetch_recent_2y_daily_data,
)
from src.clean_data import clean_hourly_data, clean_daily_data
from src.save_data import save_csv
from src.analyze_data import (
    daily_avg_aqi,
    avg_aqi_by_county,
    high_pollution_hours,
)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def main():
    print("bootstrap started")

    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("API_KEY missing")

    # 先保守抓，確認穩定後再加大
    hourly_df = fetch_recent_30d_hourly_data(api_key, limit=1000, max_pages=90)
    daily_df = fetch_recent_2y_daily_data(api_key, limit=1000, max_pages=140)

    print("Raw hourly:", hourly_df.shape)
    print("Raw daily:", daily_df.shape)

    save_csv(hourly_df, BASE_DIR / "data/raw/hourly_raw.csv")
    save_csv(daily_df, BASE_DIR / "data/raw/daily_raw.csv")

    hourly_clean = clean_hourly_data(hourly_df)
    daily_clean = clean_daily_data(daily_df)

    save_csv(hourly_clean, BASE_DIR / "data/processed/hourly_clean.csv")
    save_csv(daily_clean, BASE_DIR / "data/processed/daily_clean.csv")

    trend_df = daily_avg_aqi(hourly_clean)
    county_df = avg_aqi_by_county(hourly_clean)
    risk_df = high_pollution_hours(hourly_clean)

    save_csv(trend_df, BASE_DIR / "output/tables/daily_trend.csv")
    save_csv(county_df, BASE_DIR / "output/tables/county_avg.csv")
    save_csv(risk_df, BASE_DIR / "output/tables/high_pollution_hours.csv")

    print("bootstrap done")


if __name__ == "__main__":
    main()