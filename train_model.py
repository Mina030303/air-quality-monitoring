from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

BASE_DIR = Path(__file__).resolve().parent
MODEL_DIR = BASE_DIR / "models"
MODEL_PATH = MODEL_DIR / "aqi_model.pkl"
FORECAST_PATH = BASE_DIR / "data" / "forecast.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class TrainArtifacts:
    model: Pipeline
    county_latest_context: dict[str, dict[str, float | pd.Timestamp]]


def get_database_url() -> str:
    load_dotenv(BASE_DIR / ".env")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set in .env")
    return database_url


def load_last_30_days(database_url: str) -> pd.DataFrame:
    query = """
    SELECT county, publish_time, aqi
    FROM hourly_aqi
    WHERE publish_time >= NOW() - INTERVAL '30 days'
      AND county IS NOT NULL
      AND county <> ''
      AND aqi IS NOT NULL
    ORDER BY publish_time ASC
    """

    engine = create_engine(database_url)
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
    finally:
        engine.dispose()

    if df.empty:
        raise ValueError("No training data found in the last 30 days.")

    logger.info("Loaded %s rows from hourly_aqi", len(df))
    return df


def build_features(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    df = df.dropna(subset=["county", "publish_time", "aqi"])

    # Build county-level hourly series for modeling and forecasting.
    county_hourly = (
        df.groupby(["county", "publish_time"], as_index=False)["aqi"]
        .mean()
        .sort_values(["county", "publish_time"])
        .reset_index(drop=True)
    )

    county_hourly["hour"] = county_hourly["publish_time"].dt.hour
    county_hourly["day_of_week"] = county_hourly["publish_time"].dt.dayofweek
    county_hourly["is_weekend"] = (county_hourly["day_of_week"] >= 5).astype(int)

    county_hourly["aqi_lag_1"] = county_hourly.groupby("county")["aqi"].shift(1)
    county_hourly["aqi_lag_24"] = county_hourly.groupby("county")["aqi"].shift(24)

    strict_df = county_hourly.dropna(subset=["aqi_lag_1", "aqi_lag_24"]).reset_index(drop=True)
    if not strict_df.empty:
        logger.info("Using strict lag features (lag_1 + lag_24) with %s rows", len(strict_df))
        return strict_df

    # Fallback path for sparse backfilled data:
    # keep required feature columns by imputing missing lags from recent/local statistics.
    logger.warning(
        "No rows remain after strict lag filtering. Falling back to lag imputation strategy."
    )

    fallback_df = county_hourly.copy()
    fallback_df["aqi_lag_1"] = fallback_df.groupby("county")["aqi_lag_1"].transform(
        lambda s: s.ffill().bfill()
    )
    fallback_df["aqi_lag_1"] = fallback_df["aqi_lag_1"].fillna(fallback_df["aqi"])

    fallback_df["aqi_lag_24"] = fallback_df.groupby("county")["aqi_lag_24"].transform(
        lambda s: s.ffill().bfill()
    )
    fallback_df["aqi_lag_24"] = fallback_df["aqi_lag_24"].fillna(fallback_df["aqi_lag_1"])

    fallback_df = fallback_df.dropna(subset=["aqi_lag_1", "aqi_lag_24", "aqi"]).reset_index(drop=True)

    if fallback_df.empty:
        raise ValueError(
            "No rows available for training after fallback lag imputation. Check source data quality."
        )

    logger.info("Using fallback lag features with %s rows", len(fallback_df))
    return fallback_df


def train_model(feature_df: pd.DataFrame) -> Pipeline:
    feature_cols = ["county", "hour", "day_of_week", "is_weekend", "aqi_lag_1", "aqi_lag_24"]
    target_col = "aqi"

    x = feature_df[feature_cols]
    y = feature_df[target_col]

    preprocessor = ColumnTransformer(
        transformers=[
            ("county_ohe", OneHotEncoder(handle_unknown="ignore"), ["county"]),
        ],
        remainder="passthrough",
    )

    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=12,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    pipeline.fit(x, y)
    logger.info("Model training complete on %s samples", len(feature_df))
    return pipeline


def extract_latest_context(feature_df: pd.DataFrame) -> dict[str, dict[str, float | pd.Timestamp]]:
    context: dict[str, dict[str, float | pd.Timestamp]] = {}

    for county, county_df in feature_df.groupby("county"):
        county_df = county_df.sort_values("publish_time")
        latest = county_df.iloc[-1]

        context[county] = {
            "latest_time": latest["publish_time"],
            "latest_aqi": float(latest["aqi"]),
        }

    return context


def save_model(artifacts: TrainArtifacts) -> None:
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {
            "model": artifacts.model,
            "county_latest_context": artifacts.county_latest_context,
            "feature_order": ["county", "hour", "day_of_week", "is_weekend", "aqi_lag_1", "aqi_lag_24"],
        },
        MODEL_PATH,
    )
    logger.info("Saved model to %s", MODEL_PATH)


def forecast_next_24_hours(model: Pipeline, feature_df: pd.DataFrame) -> pd.DataFrame:
    county_series = {
        county: grp.sort_values("publish_time").set_index("publish_time")["aqi"].astype(float)
        for county, grp in feature_df.groupby("county")
    }

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

    forecast_df = pd.DataFrame(rows).sort_values(["county", "forecast_time"]).reset_index(drop=True)
    return forecast_df


def save_forecast(forecast_df: pd.DataFrame) -> None:
    FORECAST_PATH.parent.mkdir(parents=True, exist_ok=True)
    forecast_to_save = forecast_df.copy()
    forecast_to_save["forecast_time"] = pd.to_datetime(forecast_to_save["forecast_time"]).dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    forecast_to_save.to_csv(FORECAST_PATH, index=False, encoding="utf-8-sig")
    logger.info("Saved forecast to %s", FORECAST_PATH)


def main() -> None:
    logger.info("Starting AQI model training pipeline")
    database_url = get_database_url()

    raw_df = load_last_30_days(database_url)
    feature_df = build_features(raw_df)

    model = train_model(feature_df)
    artifacts = TrainArtifacts(
        model=model,
        county_latest_context=extract_latest_context(feature_df),
    )
    save_model(artifacts)

    forecast_df = forecast_next_24_hours(model, feature_df)
    save_forecast(forecast_df)

    logger.info("Pipeline complete. Forecast rows: %s", len(forecast_df))


if __name__ == "__main__":
    main()
