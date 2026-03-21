from pathlib import Path
import os

from dotenv import load_dotenv
from src.fetch_data import (
    fetch_recent_30d_hourly_data,
    fetch_recent_2y_daily_data,
)
from src.save_data import save_csv
from src.clean_data import (
    clean_hourly_data,
    clean_daily_data,
)
from src.analyze_data import (
    daily_avg_aqi,
    avg_aqi_by_county,
    high_pollution_hours,
)
from src.visualize import (
    plot_trend,
    plot_county,
    plot_hours,
)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def main():
    print("main started")

    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("API_KEY missing")

    # ---------- FETCH ----------
    hourly_df = fetch_recent_30d_hourly_data(api_key, limit=1000, max_pages=5)
    daily_df = fetch_recent_2y_daily_data(api_key, max_pages=1)

    print("Raw hourly:", hourly_df.shape)
    print("Raw daily:", daily_df.shape)

    # ---------- SAVE RAW ----------
    save_csv(hourly_df, BASE_DIR / "data/raw/hourly_raw.csv")
    save_csv(daily_df, BASE_DIR / "data/raw/daily_raw.csv")

    # ---------- CLEAN ----------
    hourly_clean = clean_hourly_data(hourly_df)
    daily_clean = clean_daily_data(daily_df)

    print("Clean hourly:", hourly_clean.shape)
    print("Clean daily:", daily_clean.shape)

    # ---------- SAVE PROCESSED ----------
    save_csv(hourly_clean, BASE_DIR / "data/processed/hourly_clean.csv")
    save_csv(daily_clean, BASE_DIR / "data/processed/daily_clean.csv")

    # ---------- ANALYZE ----------

    trend_df = daily_avg_aqi(hourly_clean)
    county_df = avg_aqi_by_county(hourly_clean)
    risk_df = high_pollution_hours(hourly_clean)

    print("\nTrend:")
    print(trend_df.head())

    print("\nCounty AQI:")
    print(county_df.head())

    print("\nHigh pollution hours:")
    print(risk_df.head())

    # ---------- SAVE OUTPUT ----------

    save_csv(trend_df, BASE_DIR / "output/daily_trend.csv")
    save_csv(county_df, BASE_DIR / "output/county_avg.csv")
    save_csv(risk_df, BASE_DIR / "output/high_pollution_hours.csv")

    # ---------- VISUALIZE ----------

    plot_trend(trend_df)
    plot_county(county_df)
    plot_hours(risk_df)


    print("All done")


if __name__ == "__main__":
    main()