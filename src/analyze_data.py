from __future__ import annotations
import pandas as pd


# ---------- 1. 每日平均 AQI 趨勢 ----------

def daily_avg_aqi(hourly_df: pd.DataFrame) -> pd.DataFrame:
    df = hourly_df.copy()

    df["date"] = df["datacreationdate"].dt.date

    result = (
        df.groupby("date")["aqi"]
        .mean()
        .reset_index()
        .rename(columns={"aqi": "avg_aqi"})
    )

    return result


# ---------- 2. 各縣市平均 AQI ----------

def avg_aqi_by_county(hourly_df: pd.DataFrame) -> pd.DataFrame:
    df = hourly_df.copy()

    result = (
        df.groupby("county")["aqi"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )

    return result


# ---------- 3. 高污染時段（AQI > 100） ----------

def high_pollution_hours(hourly_df: pd.DataFrame) -> pd.DataFrame:
    df = hourly_df.copy()

    df = df[df["aqi"] > 100]

    df["hour"] = df["datacreationdate"].dt.hour

    result = (
        df.groupby("hour")
        .size()
        .reset_index(name="high_pollution_count")
        .sort_values("high_pollution_count", ascending=False)
    )

    return result