from __future__ import annotations
import logging
import os
import joblib
import numpy as np
import pandas as pd
from datetime import timedelta
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_PATH = BASE_DIR / "models" / "aqi_site_model.joblib"
FORECAST_PATH = BASE_DIR / "data" / "forecast.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_inference_context(db_url: str):
    # 預測時只需最近 48 小時來當引子
    query = """
    SELECT county, site_name AS site, publish_time, aqi
    FROM hourly_aqi
    WHERE publish_time >= NOW() - INTERVAL '48 hours'
    ORDER BY publish_time ASC
    """
    engine = create_engine(db_url)
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)

def main():
    load_dotenv(BASE_DIR / ".env")
    db_url = os.getenv("DATABASE_URL")
    
    if not MODEL_PATH.exists():
        logger.error("找不到模型，請先跑一次 train.py")
        return

    # 1. 載入模型與上下文
    model_bundle = joblib.load(MODEL_PATH)
    model = model_bundle["model"]
    df = load_inference_context(db_url)
    df["publish_time"] = pd.to_datetime(df["publish_time"])
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")

    # 2. 建立歷史檢索快取
    history = df.set_index(["site", "publish_time"])["aqi"].to_dict()
    sites = df["site"].unique()
    county_map = df.drop_duplicates("site").set_index("site")["county"].to_dict()
    latest_time = df["publish_time"].max()

    # 3. 遞歸預測未來 24 小時
    forecast_rows = []
    for step in range(1, 25):
        t = latest_time + timedelta(hours=step)
        step_data = []
        for s in sites:
            # 取得 lag 特徵 (如果歷史沒資料，則用該站最後一筆 Aqi 補位)
            fallback = df[df["site"] == s]["aqi"].iloc[-1]
            lag_1 = history.get((s, t - timedelta(hours=1)), fallback)
            lag_24 = history.get((s, t - timedelta(hours=24)), lag_1)
            
            step_data.append({
                "county": county_map[s], "site": s, "hour": t.hour,
                "day_of_week": t.dayofweek, "is_weekend": int(t.dayofweek >= 5),
                "aqi_lag_1": lag_1, "aqi_lag_24": lag_24
            })
        
        step_df = pd.DataFrame(step_data)
        step_df["county_mean_aqi_lag_1"] = step_df.groupby("county")["aqi_lag_1"].transform("mean")
        
        # 推論
        preds = np.clip(model.predict(step_df[model_bundle["feature_order"]]), 0, 500)
        
        for i, s in enumerate(sites):
            history[(s, t)] = preds[i]
            forecast_rows.append({"county": county_map[s], "sitename": s, "forecast_time": t, "predicted_aqi": round(preds[i], 2)})

    # 4. 存檔 (供 LINE Bot 讀取)
    pd.DataFrame(forecast_rows).to_csv(FORECAST_PATH, index=False, encoding="utf-8-sig")
    logger.info(f"✅ 預測更新完成。起點: {latest_time}")

if __name__ == "__main__":
    main()
