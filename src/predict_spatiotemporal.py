from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parents[1]
MODEL_PATH = BASE_DIR / "models" / "aqi_model.joblib"


@dataclass
class StationPoint:
    siteid: str
    latitude: float
    longitude: float


@dataclass
class AQITableConfig:
    site_col: str
    pm25_col: str
    time_col: str
    lat_col: str
    lon_col: str
    county_col: str | None


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in kilometers."""
    earth_radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return earth_radius_km * c


def get_engine_from_env():
    load_dotenv(BASE_DIR / ".env")
    database_url = os.getenv("DATABASE_URL", "")
    if not database_url:
        raise ValueError("DATABASE_URL is not set in .env")
    return create_engine(database_url)


def load_model(model_path: Path = MODEL_PATH):
    payload = joblib.load(model_path)
    if isinstance(payload, dict) and "model" in payload:
        return payload["model"], payload
    return payload, {}


def detect_hourly_aqi_config(engine) -> AQITableConfig:
    query = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'hourly_aqi'
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    columns = {str(c).lower() for c in df["column_name"].tolist()}

    def _pick(candidates: list[str], label: str) -> str:
        for col in candidates:
            if col in columns:
                return col
        raise ValueError(f"hourly_aqi is missing required {label} column. Tried: {candidates}")

    site_col = _pick(["siteid", "epa_site_id", "site_id", "sitename", "site_name"], "site id")
    pm25_col = _pick(["pm25", "pm2_5", "pm25_avg", "pm25avg", "aqi"], "pm2.5/target")
    time_col = _pick(["publish_time", "datacreationdate", "obs_time", "record_time"], "time")
    lat_col = _pick(["latitude", "lat"], "latitude")
    lon_col = _pick(["longitude", "lon", "lng"], "longitude")
    county_col = "county" if "county" in columns else None

    print(
        "[Schema Detection] hourly_aqi columns -> "
        f"site={site_col}, target={pm25_col}, time={time_col}, lat={lat_col}, lon={lon_col}, county={county_col}"
    )

    return AQITableConfig(
        site_col=site_col,
        pm25_col=pm25_col,
        time_col=time_col,
        lat_col=lat_col,
        lon_col=lon_col,
        county_col=county_col,
    )


def load_active_site_ids(engine, cfg: AQITableConfig) -> list[str]:
    query = (
        f"SELECT DISTINCT CAST({cfg.site_col} AS TEXT) AS siteid "
        f"FROM hourly_aqi WHERE {cfg.site_col} IS NOT NULL ORDER BY siteid"
    )
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df["siteid"].astype(str).tolist()


def load_station_points(engine, cfg: AQITableConfig) -> dict[str, StationPoint]:
    query = f"""
    SELECT DISTINCT ON (CAST({cfg.site_col} AS TEXT))
        CAST({cfg.site_col} AS TEXT) AS siteid,
        {cfg.lat_col} AS latitude,
        {cfg.lon_col} AS longitude,
        {cfg.time_col} AS obs_time
    FROM hourly_aqi
    WHERE {cfg.site_col} IS NOT NULL
      AND {cfg.lat_col} IS NOT NULL
      AND {cfg.lon_col} IS NOT NULL
    ORDER BY CAST({cfg.site_col} AS TEXT), {cfg.time_col} DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)

    points: dict[str, StationPoint] = {}
    for row in df.itertuples(index=False):
        points[str(row.siteid)] = StationPoint(
            siteid=str(row.siteid),
            latitude=float(row.latitude),
            longitude=float(row.longitude),
        )
    return points


def build_neighbor_mapping(points: dict[str, StationPoint], k: int = 3) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    all_points = list(points.values())

    for src in all_points:
        distances: list[tuple[float, str]] = []
        for dst in all_points:
            if src.siteid == dst.siteid:
                continue
            dist = haversine_distance(src.latitude, src.longitude, dst.latitude, dst.longitude)
            distances.append((dist, dst.siteid))

        distances.sort(key=lambda x: x[0])
        mapping[src.siteid] = [siteid for _, siteid in distances[:k]]

    return mapping


def load_recent_joined_data(engine, cfg: AQITableConfig, hours: int = 6) -> pd.DataFrame:
    county_select = f"MAX(a.{cfg.county_col}) AS county," if cfg.county_col else "NULL::text AS county,"
    query = text(
                f"""
        SELECT
                        CAST(a.{cfg.site_col} AS TEXT) AS siteid,
                        DATE_TRUNC('hour', a.{cfg.time_col} AT TIME ZONE 'UTC') AS publish_time,
                        MAX(DATE_TRUNC('hour', w.obs_time AT TIME ZONE 'UTC')) AS obs_time,
            {county_select}
                        AVG(a.{cfg.pm25_col}) AS pm25,
                        COALESCE(AVG(w.wind_u), 0.0) AS wind_u,
                        COALESCE(AVG(w.wind_v), 0.0) AS wind_v
                FROM hourly_aqi a
        JOIN station_mapping sm
                    ON CAST(a.{cfg.site_col} AS TEXT) = CAST(sm.epa_site_id AS TEXT)
                    OR BTRIM(CAST(a.{cfg.site_col} AS TEXT)) = BTRIM(CAST(sm.epa_name AS TEXT))
                LEFT JOIN hourly_weather w
                    ON CAST(sm.cwa_station_id AS TEXT) = CAST(w.station_id AS TEXT)
                 AND DATE_TRUNC('hour', a.{cfg.time_col} AT TIME ZONE 'UTC') = DATE_TRUNC('hour', w.obs_time AT TIME ZONE 'UTC')
                WHERE a.{cfg.pm25_col} IS NOT NULL
                    AND a.{cfg.time_col} >= NOW() - (:hours || ' hours')::interval
                GROUP BY CAST(a.{cfg.site_col} AS TEXT), DATE_TRUNC('hour', a.{cfg.time_col} AT TIME ZONE 'UTC')
                ORDER BY publish_time ASC
                """
    )
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"hours": hours})

    if df.empty:
        return df

    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
    df["obs_time"] = pd.to_datetime(df["obs_time"], errors="coerce").fillna(df["publish_time"])
    df["pm25"] = pd.to_numeric(df["pm25"], errors="coerce")
    df["wind_u"] = pd.to_numeric(df["wind_u"], errors="coerce")
    df["wind_v"] = pd.to_numeric(df["wind_v"], errors="coerce")

    print("[Join Preview] first 3 rows (publish_time vs obs_time):")
    print(df[["siteid", "publish_time", "obs_time"]].head(3))

    df = df.dropna(subset=["siteid", "publish_time", "pm25", "wind_u", "wind_v"]).sort_values(["siteid", "publish_time"])
    return df


def load_recent_joined_data_latest_weather(engine, cfg: AQITableConfig, hours: int = 168) -> pd.DataFrame:
    """Fallback join: use latest available weather snapshot per CWA station."""
    county_select = f"MAX(aq.{cfg.county_col}) AS county," if cfg.county_col else "NULL::text AS county,"
    query = text(
        f"""
        WITH latest_weather AS (
            SELECT station_id, obs_time, wind_u, wind_v
            FROM (
                SELECT
                    station_id,
                    obs_time,
                    wind_u,
                    wind_v,
                    ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY obs_time DESC) AS rn
                FROM hourly_weather
            ) w
            WHERE rn = 1
        )
        SELECT
            CAST(aq.{cfg.site_col} AS TEXT) AS siteid,
            DATE_TRUNC('hour', aq.{cfg.time_col}) AS obs_time,
            {county_select}
            AVG(aq.{cfg.pm25_col}) AS pm25,
            AVG(lw.wind_u) AS wind_u,
            AVG(lw.wind_v) AS wind_v
        FROM hourly_aqi aq
        JOIN station_mapping sm
          ON CAST(aq.{cfg.site_col} AS TEXT) = CAST(sm.epa_site_id AS TEXT)
          OR BTRIM(CAST(aq.{cfg.site_col} AS TEXT)) = BTRIM(CAST(sm.epa_name AS TEXT))
        JOIN latest_weather lw
          ON CAST(sm.cwa_station_id AS TEXT) = CAST(lw.station_id AS TEXT)
        WHERE aq.{cfg.pm25_col} IS NOT NULL
          AND aq.{cfg.time_col} >= NOW() - (:hours || ' hours')::interval
        GROUP BY CAST(aq.{cfg.site_col} AS TEXT), DATE_TRUNC('hour', aq.{cfg.time_col})
        ORDER BY obs_time ASC
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"hours": hours})

    if df.empty:
        return df

    df["obs_time"] = pd.to_datetime(df["obs_time"], errors="coerce")
    df["pm25"] = pd.to_numeric(df["pm25"], errors="coerce")
    df["wind_u"] = pd.to_numeric(df["wind_u"], errors="coerce")
    df["wind_v"] = pd.to_numeric(df["wind_v"], errors="coerce")
    df = df.dropna(subset=["siteid", "obs_time", "pm25", "wind_u", "wind_v"]).sort_values(["siteid", "obs_time"])
    return df


def print_join_diagnostics(engine, cfg: AQITableConfig) -> None:
    """Print quick diagnostics to explain why join rows may be empty."""
    queries = {
        "hourly_aqi_count": f"SELECT COUNT(*) AS c FROM hourly_aqi WHERE {cfg.pm25_col} IS NOT NULL",
        "station_mapping_count": "SELECT COUNT(*) AS c FROM station_mapping",
        "hourly_weather_count": "SELECT COUNT(*) AS c FROM hourly_weather",
        "hourly_aqi_latest": f"SELECT MAX({cfg.time_col}) AS t FROM hourly_aqi",
        "hourly_weather_latest": "SELECT MAX(obs_time) AS t FROM hourly_weather",
        "station_mapping_distinct_epa": "SELECT COUNT(DISTINCT epa_site_id) AS c FROM station_mapping",
        "station_mapping_distinct_cwa": "SELECT COUNT(DISTINCT cwa_station_id) AS c FROM station_mapping",
        "aqi_name_to_map_match": (
            f"SELECT COUNT(*) AS c FROM hourly_aqi aq "
            f"JOIN station_mapping sm ON BTRIM(CAST(aq.{cfg.site_col} AS TEXT))=BTRIM(CAST(sm.epa_name AS TEXT))"
        ),
        "aqi_id_to_map_match": (
            f"SELECT COUNT(*) AS c FROM hourly_aqi aq "
            f"JOIN station_mapping sm ON CAST(aq.{cfg.site_col} AS TEXT)=CAST(sm.epa_site_id AS TEXT)"
        ),
        "overlap_168h_local_hour": (
            f"SELECT COUNT(*) AS c FROM hourly_aqi aq "
            f"JOIN station_mapping sm ON BTRIM(CAST(aq.{cfg.site_col} AS TEXT))=BTRIM(CAST(sm.epa_name AS TEXT)) "
            f"JOIN hourly_weather hw ON CAST(sm.cwa_station_id AS TEXT)=CAST(hw.station_id AS TEXT) "
            f"AND DATE_TRUNC('hour', aq.{cfg.time_col})=DATE_TRUNC('hour', hw.obs_time) "
            f"WHERE aq.{cfg.time_col} >= NOW() - INTERVAL '168 hours'"
        ),
    }

    print("[Diagnostics] Join coverage summary:")
    with engine.connect() as conn:
        for name, q in queries.items():
            try:
                row_df = pd.read_sql_query(q, conn)
                if row_df.empty:
                    print(f"  - {name}: <empty>")
                    continue
                val = row_df.iloc[0, 0]
                print(f"  - {name}: {val}")
            except Exception as exc:
                print(f"  - {name}: ERROR ({exc})")


def _build_feature_row(
    joined_df: pd.DataFrame,
    target_siteid: str,
    neighbors: list[str],
) -> tuple[pd.Timestamp, pd.DataFrame, pd.DataFrame] | None:
    target_df = joined_df[joined_df["siteid"] == target_siteid].sort_values("obs_time").copy()
    if len(target_df) < 3:
        return None

    target_df["pm25_lag_1"] = target_df["pm25"].shift(1)
    target_df["pm25_lag_2"] = target_df["pm25"].shift(2)

    pm25_wide = joined_df.pivot_table(index="obs_time", columns="siteid", values="pm25", aggfunc="mean").sort_index()
    pm25_lag_1 = pm25_wide.shift(1)

    latest = target_df.iloc[-1].copy()
    latest_time = pd.Timestamp(latest["obs_time"])

    if pd.isna(latest["pm25_lag_1"]) or pd.isna(latest["pm25_lag_2"]):
        return None

    feature_dict: dict[str, float] = {
        "wind_u": float(latest["wind_u"]),
        "wind_v": float(latest["wind_v"]),
        "pm25_lag_1": float(latest["pm25_lag_1"]),
        "pm25_lag_2": float(latest["pm25_lag_2"]),
    }

    for idx in range(3):
        neighbor_key = f"neighbor_{idx + 1}_pm25_lag_1"
        if idx >= len(neighbors):
            return None
        neighbor_id = neighbors[idx]
        if neighbor_id not in pm25_lag_1.columns:
            return None

        neighbor_val = pm25_lag_1.loc[latest_time, neighbor_id] if latest_time in pm25_lag_1.index else None
        if pd.isna(neighbor_val):
            return None
        feature_dict[neighbor_key] = float(neighbor_val)

    features = pd.DataFrame([feature_dict])
    return latest_time, features, target_df


def _align_features_to_model_schema(
    feature_values: pd.DataFrame,
    feature_order: list[str] | None,
    target_df: pd.DataFrame,
    latest_time: pd.Timestamp,
) -> tuple[pd.DataFrame, list[str]]:
    if not feature_order:
        return feature_values, []

    aligned = feature_values.copy()
    predict_time = latest_time + pd.Timedelta(hours=1)

    if "county" in feature_order and "county" not in aligned.columns:
        county_val = ""
        if "county" in target_df.columns:
            county_series = target_df["county"].dropna()
            if not county_series.empty:
                county_val = str(county_series.iloc[-1])
        aligned["county"] = county_val

    if "hour" in feature_order and "hour" not in aligned.columns:
        aligned["hour"] = int(predict_time.hour)

    if "day_of_week" in feature_order and "day_of_week" not in aligned.columns:
        aligned["day_of_week"] = int(predict_time.dayofweek)

    if "is_weekend" in feature_order and "is_weekend" not in aligned.columns:
        aligned["is_weekend"] = int(predict_time.dayofweek >= 5)

    if "aqi_lag_1" in feature_order and "aqi_lag_1" not in aligned.columns:
        aligned["aqi_lag_1"] = float(target_df["pm25"].iloc[-1])

    if "aqi_lag_24" in feature_order and "aqi_lag_24" not in aligned.columns:
        if len(target_df) >= 24:
            aligned["aqi_lag_24"] = float(target_df["pm25"].iloc[-24])
        else:
            aligned["aqi_lag_24"] = float(target_df["pm25"].iloc[-1])

    missing = [col for col in feature_order if col not in aligned.columns]
    if not missing:
        aligned = aligned[feature_order]
    return aligned, missing


def run_batch_inference(engine) -> pd.DataFrame:
    cfg = detect_hourly_aqi_config(engine)
    site_ids = load_active_site_ids(engine, cfg)
    station_points = load_station_points(engine, cfg)
    neighbor_map = build_neighbor_mapping(station_points, k=3)
    lookback_hours = [6, 24, 72, 168]
    joined_df = pd.DataFrame()
    used_window: str | None = None
    for hrs in lookback_hours:
        candidate = load_recent_joined_data(engine, cfg, hours=hrs)
        print(f"[Batch Inference] lookback={hrs}h -> joined rows={len(candidate)}")
        if not candidate.empty:
            joined_df = candidate
            used_window = f"{hrs}h"
            break

    if joined_df.empty:
        print("[Batch Inference] Strict hour-aligned join returned 0 rows. Trying latest-weather fallback...")
        joined_df = load_recent_joined_data_latest_weather(engine, cfg, hours=168)
        print(f"[Batch Inference] latest-weather fallback rows={len(joined_df)}")
        if not joined_df.empty:
            used_window = "latest-weather-fallback"

    if joined_df.empty:
        print_join_diagnostics(engine, cfg)
        raise ValueError(
            "No joined spatiotemporal data found within 168 hours. "
            "Please confirm hourly_weather and station_mapping are populated and timestamps overlap with hourly_aqi."
        )

    print(f"[Batch Inference] Using lookback window: {used_window}h")

    model, artifact_meta = load_model(MODEL_PATH)
    feature_order = artifact_meta.get("feature_order") if isinstance(artifact_meta, dict) else None

    rows: list[dict[str, object]] = []
    skipped_no_neighbors = 0
    skipped_no_features = 0
    skipped_missing_model_cols = 0

    for siteid in site_ids:
        if siteid not in neighbor_map:
            skipped_no_neighbors += 1
            continue
        built = _build_feature_row(joined_df, siteid, neighbor_map[siteid])
        if built is None:
            skipped_no_features += 1
            continue

        latest_time, feature_values, target_df = built
        feature_values, missing = _align_features_to_model_schema(
            feature_values,
            feature_order,
            target_df,
            latest_time,
        )
        if missing:
            skipped_missing_model_cols += 1
            continue

        pred = float(model.predict(feature_values)[0])
        pred = max(0.0, min(500.0, pred))

        rows.append(
            {
                "siteid": siteid,
                "predict_time": latest_time + pd.Timedelta(hours=1),
                "predicted_pm25": round(pred, 2),
            }
        )

    print(
        "[Batch Inference] station summary -> "
        f"total={len(site_ids)}, predicted={len(rows)}, "
        f"skip_no_neighbors={skipped_no_neighbors}, "
        f"skip_no_features={skipped_no_features}, "
        f"skip_missing_model_cols={skipped_missing_model_cols}"
    )

    return pd.DataFrame(rows, columns=["siteid", "predict_time", "predicted_pm25"])


def upsert_forecast(engine, forecast_df: pd.DataFrame) -> None:
    if forecast_df.empty:
        return

    create_sql = text(
        """
        CREATE TABLE IF NOT EXISTS forecast (
            siteid TEXT NOT NULL,
            predict_time TIMESTAMP NOT NULL,
            predicted_pm25 DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (siteid, predict_time)
        )
        """
    )

    upsert_sql = text(
        """
        INSERT INTO forecast (siteid, predict_time, predicted_pm25)
        VALUES (:siteid, :predict_time, :predicted_pm25)
        ON CONFLICT (siteid, predict_time)
        DO UPDATE SET
            predicted_pm25 = EXCLUDED.predicted_pm25,
            updated_at = CURRENT_TIMESTAMP
        """
    )

    records = [
        {
            "siteid": str(row.siteid),
            "predict_time": pd.Timestamp(row.predict_time).to_pydatetime(),
            "predicted_pm25": float(row.predicted_pm25),
        }
        for row in forecast_df.itertuples(index=False)
    ]

    with engine.begin() as conn:
        conn.execute(create_sql)
        conn.execute(upsert_sql, records)


def main() -> int:
    engine = get_engine_from_env()
    try:
        forecast_df = run_batch_inference(engine)
        upsert_forecast(engine, forecast_df)
        print(f"Batch inference complete for {len(forecast_df)} stations.")
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
