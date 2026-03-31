from __future__ import annotations
import logging
import os
from pathlib import Path
import joblib
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from dotenv import load_dotenv

# 設定路徑
BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_DIR = BASE_DIR / "models"
MODEL_PATH = MODEL_DIR / "aqi_site_model.joblib"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ==========================================
# MIS 控制台：手動切換訓練模式
# ==========================================
# 第一次跑請設為 True (3 年)；之後定期更新設為 False (30 天)
INITIAL_TRAIN = True 

def get_db_url():
    load_dotenv(BASE_DIR / ".env")
    return os.getenv("DATABASE_URL")

def load_training_data(db_url: str, days: int) -> pd.DataFrame:
    # 這裡依照你的需求，動態決定抓取天數 (3年 = 1095天)
    interval = f"{days} days"
    query = f"""
    SELECT county, site_name AS site, publish_time, aqi
    FROM hourly_aqi
    WHERE publish_time >= NOW() - INTERVAL '{interval}'
      AND county IS NOT NULL AND site_name IS NOT NULL AND aqi IS NOT NULL
    ORDER BY publish_time ASC
    """
    engine = create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df

def build_features(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df["publish_time"] = pd.to_datetime(df["publish_time"])
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    df = df.dropna(subset=["county", "site", "publish_time", "aqi"])

    # 1. 測站聚合
    site_hourly = df.groupby(["county", "site", "publish_time"], as_index=False)["aqi"].mean()
    site_hourly = site_hourly.sort_values(["site", "publish_time"]).reset_index(drop=True)

    # 2. 縣市平均特徵
    county_mean = site_hourly.groupby(["county", "publish_time"])["aqi"].mean().rename("county_mean_aqi").reset_index()
    site_hourly = site_hourly.merge(county_mean, on=["county", "publish_time"], how="left")

    # 3. 時間與滯後特徵 (Lag)
    site_hourly["hour"] = site_hourly["publish_time"].dt.hour
    site_hourly["day_of_week"] = site_hourly["publish_time"].dt.dayofweek
    site_hourly["is_weekend"] = (site_hourly["day_of_week"] >= 5).astype(int)
    
    # 計算 Lag (訓練時採用嚴格模式)
    site_hourly["aqi_lag_1"] = site_hourly.groupby("site")["aqi"].shift(1)
    site_hourly["aqi_lag_24"] = site_hourly.groupby("site")["aqi"].shift(24)
    site_hourly["county_mean_aqi_lag_1"] = site_hourly.groupby("site")["county_mean_aqi"].shift(1)
    
    return site_hourly.dropna(subset=["aqi_lag_1", "aqi_lag_24", "county_mean_aqi_lag_1"]).reset_index(drop=True)

def main():
    db_url = get_db_url()
    days_to_load = 900 if INITIAL_TRAIN else 30
    logger.info(f"開始訓練 Pipeline (模式: {'初次大批量' if INITIAL_TRAIN else '定期更新'}, 天數: {days_to_load})")
    
    # 執行流程
    raw_df = load_training_data(db_url, days_to_load)
    feature_df = build_features(raw_df)

    # 1. 印出前 5 筆資料，確認欄位名稱 (site_name, aqi, pm2.5...)
    print("--- 資料前 5 筆 ---")
    print(raw_df.head())

    # 2. 印出資料表的詳細資訊 (這最重要！)
    # 可以看到總筆數 (RangeIndex) 和每個欄位的空值狀況
    print("\n--- 資料表結構資訊 ---")
    print(raw_df.info())

    # 3. 印出統計資訊 (確認 AQI 有沒有奇怪的負數或離群值)
    print("\n--- 數值統計概況 ---")
    print(raw_df.describe())

    # 4. 確認時間範圍 (驗證是否真的有 7 年)
    print("\n--- 資料時間區間 ---")
    print(f"最早時間: {raw_df['publish_time'].min()}")
    print(f"最晚時間: {raw_df['publish_time'].max()}")
    
    # 準備模型
    feature_cols = ["county", "site", "hour", "day_of_week", "is_weekend", "aqi_lag_1", "aqi_lag_24", "county_mean_aqi_lag_1"]
    x, y = feature_df[feature_cols], feature_df["aqi"]
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

    pipeline = Pipeline([
        ("preprocessor", ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore"), ["county", "site"])], remainder="passthrough")),
        ("model", RandomForestRegressor(n_estimators=300, max_depth=15, random_state=42, n_jobs=-1))
    ])
    
    pipeline.fit(x_train, y_train)
    
    # 儲存模型包
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": pipeline, "feature_order": feature_cols}, MODEL_PATH)
    logger.info(f"模型訓練完成並存檔至 {MODEL_PATH}")

if __name__ == "__main__":
    main()
