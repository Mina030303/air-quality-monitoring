"""
全台站點批次推論 + 寫入 forecast 資料表（siteid 對齊數字）
"""
import os
import pandas as pd
import joblib
from sqlalchemy import create_engine
from datetime import timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_PATH = os.path.join(BASE_DIR, "models", "aqi_model.joblib")
DATABASE_URL = os.getenv("DATABASE_URL", "")

def load_station_table(engine):
    stations = pd.read_sql("SELECT siteid, sitename, county FROM air_quality_stations", engine)
    stations["siteid"] = stations["siteid"].astype(int)
    return stations

def load_latest_hourly(engine):
    df = pd.read_sql("SELECT * FROM hourly_aqi WHERE publish_time >= NOW() - INTERVAL '48 hours'", engine)
    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")
    df = df.dropna(subset=["site_name", "publish_time", "aqi"])
    return df

def build_series(df):
    # 以 siteid 為 key
    series = {}
    for siteid, grp in df.groupby("siteid"):
        s = grp.sort_values("publish_time").set_index("publish_time")["aqi"].astype(float)
        if not s.empty:
            series[int(siteid)] = s
    return series

def forecast_next_24h(model, series_dict, siteid_to_county):
    rows = []
    for siteid, s in series_dict.items():
        latest_time = s.index.max()
        last_aqi = float(s.iloc[-1])
        county = siteid_to_county.get(siteid, "")
        for step in range(1, 25):
            forecast_time = latest_time + timedelta(hours=step)
            lag_1 = last_aqi
            lag_24_time = forecast_time - timedelta(hours=24)
            lag_24 = float(s.loc[lag_24_time]) if lag_24_time in s.index else float(s.iloc[-1])
            features = pd.DataFrame([
                {
                    "hour": forecast_time.hour,
                    "day_of_week": forecast_time.dayofweek,
                    "is_weekend": int(forecast_time.dayofweek >= 5),
                    "aqi_lag_1": lag_1,
                    "aqi_lag_24": lag_24,
                    "county": county,
                }
            ])
            pred = float(model.predict(features)[0])
            pred = max(0.0, min(500.0, pred))
            rows.append({
                "siteid": int(siteid),
                "forecast_time": forecast_time,
                "predicted_aqi": round(pred, 2),
            })
            s.loc[forecast_time] = pred
            last_aqi = pred
    return pd.DataFrame(rows)

def main():
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL not set")
    engine = create_engine(DATABASE_URL)
    stations = load_station_table(engine)
    print(f"stations: {len(stations)} rows, columns: {stations.columns.tolist()}")
    hourly = load_latest_hourly(engine)
    print(f"hourly: {len(hourly)} rows, columns: {hourly.columns.tolist()}")
    # 對齊 siteid，使用 sitename
    hourly = hourly.merge(stations[["siteid", "sitename"]], left_on="site_name", right_on="sitename", how="left")
    print(f"after merge: {len(hourly)} rows")
    hourly = hourly.dropna(subset=["siteid"])
    print(f"after dropna siteid: {len(hourly)} rows")
    hourly["siteid"] = hourly["siteid"].astype(int)
    series_dict = build_series(hourly)
    print(f"series_dict keys: {list(series_dict.keys())[:5]} ... total {len(series_dict)}")
    payload = joblib.load(MODEL_PATH)
    model = payload["model"] if isinstance(payload, dict) and "model" in payload else payload
    siteid_to_county = dict(zip(stations["siteid"], stations["county"]))
    forecast_df = forecast_next_24h(model, series_dict, siteid_to_county)
    print(f"forecast_df shape: {forecast_df.shape}, columns: {forecast_df.columns.tolist()}")
    if forecast_df.empty or "forecast_time" not in forecast_df.columns:
        raise RuntimeError(f"No forecast results or missing forecast_time column! Data shape: {forecast_df.shape}, columns: {forecast_df.columns.tolist()}")
    # 寫入 forecast 資料表
    forecast_df["forecast_time"] = pd.to_datetime(forecast_df["forecast_time"])
    forecast_df["siteid"] = forecast_df["siteid"].astype(int)
    forecast_df.to_sql("forecast", engine, if_exists="replace", index=False)
    print(f"寫入 {len(forecast_df)} 筆預測到 forecast 資料表 (siteid 為數字)")

if __name__ == "__main__":
    main()
