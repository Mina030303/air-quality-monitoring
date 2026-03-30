from __future__ import annotations

import logging
import sys
from datetime import timedelta
from pathlib import Path

import joblib
import pandas as pd

from database import get_db_connection

BASE_DIR = Path(__file__).resolve().parents[1]
MODEL_PATHS = [
    BASE_DIR / "models" / "aqi_model.joblib",
    BASE_DIR / "models" / "aqi_model.pkl",
]
FORECAST_PATH = BASE_DIR / "data" / "forecast.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_latest_48h_data() -> pd.DataFrame:
    query = """
    SELECT county, publish_time, aqi
    FROM hourly_aqi
    WHERE publish_time >= NOW() - INTERVAL '48 hours'
      AND county IS NOT NULL
      AND county <> ''
      AND aqi IS NOT NULL
    ORDER BY publish_time ASC
    """

    with get_db_connection() as conn:
        df = pd.read_sql_query(query, conn)

    if df.empty:
        raise ValueError("No hourly data found in the last 48 hours.")

    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    df = df.dropna(subset=["county", "publish_time", "aqi"])  # safety against malformed rows

    if df.empty:
        raise ValueError("No valid rows remain after cleaning last-48h data.")

    logger.info("Loaded %s recent rows for inference", len(df))
    return df


def build_county_hourly_series(raw_df: pd.DataFrame) -> dict[str, pd.Series]:
    county_hourly = (
        raw_df.groupby(["county", "publish_time"], as_index=False)["aqi"]
        .mean()
        .sort_values(["county", "publish_time"])
        .reset_index(drop=True)
    )

    county_series: dict[str, pd.Series] = {}
    for county, grp in county_hourly.groupby("county"):
        series = grp.set_index("publish_time")["aqi"].astype(float)
        if not series.empty:
            county_series[str(county)] = series

    if not county_series:
        raise ValueError("No county time series available for inference.")

    return county_series


def load_model():
    for model_path in MODEL_PATHS:
        if model_path.exists():
            payload = joblib.load(model_path)
            if isinstance(payload, dict) and "model" in payload:
                logger.info("Loaded model from %s", model_path)
                return payload["model"]
            raise ValueError(f"Unexpected model payload format in {model_path}")

    paths = ", ".join(str(p) for p in MODEL_PATHS)
    raise FileNotFoundError(f"No model file found. Checked: {paths}")


def forecast_next_24_hours(model, county_series: dict[str, pd.Series]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for county, series in county_series.items():
        latest_time = series.index.max()
        last_aqi = float(series.iloc[-1])

        for step in range(1, 25):
            forecast_time = latest_time + timedelta(hours=step)
            lag_1 = last_aqi

            lag_24_time = forecast_time - timedelta(hours=24)
            if lag_24_time in series.index:
                lag_24 = float(series.loc[lag_24_time])
            else:
                lag_24 = float(series.iloc[-1])

            inference_row = pd.DataFrame(
                [
                    {
                        "county": county,
                        "hour": forecast_time.hour,
                        "day_of_week": forecast_time.dayofweek,
                        "is_weekend": int(forecast_time.dayofweek >= 5),
                        "aqi_lag_1": lag_1,
                        "aqi_lag_24": lag_24,
                    }
                ]
            )

            predicted_aqi = float(model.predict(inference_row)[0])
            predicted_aqi = max(0.0, min(500.0, predicted_aqi))

            rows.append(
                {
                    "county": county,
                    "forecast_time": forecast_time,
                    "predicted_aqi": round(predicted_aqi, 2),
                }
            )

            series.loc[forecast_time] = predicted_aqi
            last_aqi = predicted_aqi

    return pd.DataFrame(rows).sort_values(["county", "forecast_time"]).reset_index(drop=True)


def save_forecast(forecast_df: pd.DataFrame) -> None:
    if forecast_df.empty:
        raise ValueError("Forecast output is empty; refusing to overwrite forecast.csv")

    FORECAST_PATH.parent.mkdir(parents=True, exist_ok=True)
    to_save = forecast_df.copy()
    to_save["forecast_time"] = pd.to_datetime(to_save["forecast_time"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    to_save.to_csv(FORECAST_PATH, index=False, encoding="utf-8-sig")
    logger.info("Saved %s forecast rows to %s", len(to_save), FORECAST_PATH)


def main() -> int:
    try:
        raw_df = load_latest_48h_data()
        county_series = build_county_hourly_series(raw_df)
        model = load_model()
        forecast_df = forecast_next_24_hours(model, county_series)
        save_forecast(forecast_df)
        logger.info("Hourly inference completed successfully")
        return 0
    except Exception as exc:
        logger.exception("Hourly inference failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())