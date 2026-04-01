from __future__ import annotations
import logging
import os
import gc  # 記憶體垃圾回收工具
import numpy as np
from pathlib import Path
import joblib
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from dotenv import load_dotenv

# --- 路徑設定 ---
BASE_DIR = Path(__file__).resolve().parent.parent
MODEL_DIR = BASE_DIR / "models"
MODEL_PATH = MODEL_DIR / "aqi_site_model.joblib"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ==========================================
# MIS 控制台：模式切換
# ==========================================
# 第一次跑大數據請設為 True；之後定期更新設為 False
INITIAL_TRAIN = False 

def get_db_url():
    load_dotenv(BASE_DIR / ".env")
    return os.getenv("DATABASE_URL")

def load_training_data(db_url: str, days: int) -> pd.DataFrame:
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

    # 3. 時間特徵
    site_hourly["hour"] = site_hourly["publish_time"].dt.hour
    site_hourly["day_of_week"] = site_hourly["publish_time"].dt.dayofweek
    site_hourly["is_weekend"] = (site_hourly["day_of_week"] >= 5).astype(int)
    
    # 4. 滯後特徵 (Lag)
    site_hourly["aqi_lag_1"] = site_hourly.groupby("site")["aqi"].shift(1)
    site_hourly["aqi_lag_24"] = site_hourly.groupby("site")["aqi"].shift(24)
    site_hourly["county_mean_aqi_lag_1"] = site_hourly.groupby("site")["county_mean_aqi"].shift(1)
    
    return site_hourly.dropna(subset=["aqi_lag_1", "aqi_lag_24", "county_mean_aqi_lag_1"]).reset_index(drop=True)

def main():
    db_url = get_db_url()
    # 2.7 年約為 980 天，若記憶體仍吃緊，可降至 600
    days_to_load = 980 if INITIAL_TRAIN else 30
    
    logger.info(f"--- 開始訓練 Pipeline (模式: {'初次大批量' if INITIAL_TRAIN else '定期更新'}) ---")
    
    # 1. 載入資料
    raw_df = load_training_data(db_url, days_to_load)
    logger.info(f"成功載入資料: {len(raw_df)} 筆")

    # 2. 特徵工程
    feature_df = build_features(raw_df)
    
    # 【記憶體優化 A】釋放原始資料，因為特徵已經做好了
    del raw_df
    gc.collect()

    # 3. 準備模型輸入
    feature_cols = [
        "county", "site", "hour", "day_of_week", "is_weekend", 
        "aqi_lag_1", "aqi_lag_24", "county_mean_aqi_lag_1"
    ]
    x = feature_df[feature_cols]
    y = feature_df["aqi"]
    
    # 切分訓練與測試集
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)

    # 【記憶體優化 B】釋放大型 DataFrame，只留下訓練用的陣列
    del feature_df
    gc.collect()

    # 4. 定義模型管線 (輕量化參數)
    pipeline = Pipeline([
        ("preprocessor", ColumnTransformer([
            ("cat", OneHotEncoder(handle_unknown="ignore"), ["county", "site"])
        ], remainder="passthrough")),
        ("model", RandomForestRegressor(
            n_estimators=100,  # 100 顆樹是效能與記憶體的最佳平衡點
            max_depth=12,      # 深度 12 足以抓取 AQI 規律且不會讓模型爆炸
            min_samples_leaf=5,
            random_state=42,
            n_jobs=-1          # 使用全核心運算
        ))
    ])
    
    # 5. 訓練模型
    logger.info("模型正在訓練中，風扇轉動為正常現象...")
    pipeline.fit(x_train, y_train)
    
    # 6. 驗證正確率
    y_pred = pipeline.predict(x_test)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print("-" * 30)
    print(f"模型訓練完成！")
    print(f"正確率指標 (Evaluation):")
    print(f"   - R-squared (R2): {r2:.4f} (愈接近 1 愈好)")
    print(f"   - Mean Absolute Error (MAE): {mae:.2f} (平均誤差點數)")
    print("-" * 30)

    # 7. 儲存模型
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": pipeline, "feature_order": feature_cols}, MODEL_PATH)
    logger.info(f"模型已存檔至: {MODEL_PATH}")

if __name__ == "__main__":
    main()
