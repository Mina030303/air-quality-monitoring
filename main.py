from pathlib import Path
import os

from dotenv import load_dotenv

from src.update_data import update_all_data
from src.save_data import save_csv
from src.analyze_data import (
    daily_avg_aqi,
    avg_aqi_by_county,
    high_pollution_hours,
    time_structure_analysis,
)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def main():
    print("main started")

    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("API_KEY missing")

    hourly_clean, daily_clean = update_all_data(api_key)

    print("Updated hourly:", hourly_clean.shape)
    print("Updated daily:", daily_clean.shape)

    trend_df = daily_avg_aqi(hourly_clean)
    county_df = avg_aqi_by_county(hourly_clean)
    risk_df = high_pollution_hours(hourly_clean)
    time_daily_df, weekday_vs_weekend_df, monthly_avg_df = time_structure_analysis(hourly_clean)

    save_csv(trend_df, BASE_DIR / "output/tables/daily_trend.csv")
    save_csv(county_df, BASE_DIR / "output/tables/county_avg.csv")
    save_csv(risk_df, BASE_DIR / "output/tables/high_pollution_hours.csv")
    save_csv(time_daily_df, BASE_DIR / "output/tables/daily_time_structure.csv")
    save_csv(weekday_vs_weekend_df, BASE_DIR / "output/tables/weekday_vs_weekend.csv")
    save_csv(monthly_avg_df, BASE_DIR / "output/tables/monthly_avg.csv")

    print("update done")


if __name__ == "__main__":
    main()