from pathlib import Path
import os

from dotenv import load_dotenv

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
    hour_ratio_df = high_pollution_hour_ratio(hourly_clean)
    hour_ratio_county_df = high_pollution_hour_ratio_by_county(hourly_clean)
    time_daily_df, weekday_vs_weekend_df, monthly_avg_df = time_structure_analysis(hourly_clean)
    status_text = current_status_interpretation(time_daily_df)

    save_csv(trend_df, BASE_DIR / "output/tables/daily_trend.csv")
    save_csv(county_df, BASE_DIR / "output/tables/county_avg.csv")
    save_csv(risk_df, BASE_DIR / "output/tables/high_pollution_hours.csv")
    save_csv(hour_ratio_df, BASE_DIR / "output/tables/high_pollution_hour_ratio.csv")
    save_csv(hour_ratio_county_df, BASE_DIR / "output/tables/high_pollution_hour_ratio_by_county.csv")
    save_csv(time_daily_df, BASE_DIR / "output/tables/daily_time_structure.csv")
    save_csv(weekday_vs_weekend_df, BASE_DIR / "output/tables/weekday_vs_weekend.csv")
    save_csv(monthly_avg_df, BASE_DIR / "output/tables/monthly_avg.csv")

    # ----- 縣市風險分析 (County Risk) -----
    print("Calculating county risk scores...")
    county_risk_df = calculate_county_risk_score(hourly_clean)
    save_csv(county_risk_df, BASE_DIR / "output/tables/county_risk_score.csv")


    # ----- 異常污染飆高 (Spike Detection) -----
    print("Running spike detection...")
    # Defaulting to AQI for main pipeline overview
    spikes_df = detect_pollution_spikes(
        hourly_clean, 
        pollutant_col="aqi",
        method="rolling_threshold",
        rolling_window=24,
        threshold_ratio=1.5,
        zscore_threshold=2.5,
        min_value=50.0  # Optional minimum constraint for meaningful spikes
    )
    
    spike_county_df = spike_summary_by_county(spikes_df)
    spike_site_df = spike_summary_by_site(spikes_df)
    spike_hour_df = spike_time_pattern(spikes_df)
    
    save_csv(spikes_df, BASE_DIR / "output/tables/pollution_spikes.csv")
    save_csv(spike_county_df, BASE_DIR / "output/tables/spike_by_county.csv")
    save_csv(spike_site_df, BASE_DIR / "output/tables/spike_by_site.csv")
    save_csv(spike_hour_df, BASE_DIR / "output/tables/spike_by_hour.csv")

    print("update done")


if __name__ == "__main__":
    main()