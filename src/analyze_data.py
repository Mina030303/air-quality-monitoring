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


# ---------- 4. 時間結構分析（日、週末、月份） ----------

def time_structure_analysis(
    hourly_clean: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = hourly_clean.copy()

    df["datacreationdate"] = pd.to_datetime(df["datacreationdate"], errors="coerce")
    df = df.dropna(subset=["datacreationdate", "aqi"])

    daily_df = (
        df.assign(date=df["datacreationdate"].dt.normalize())
        .groupby("date", as_index=False)["aqi"]
        .mean()
        .rename(columns={"aqi": "avg_aqi"})
        .sort_values("date")
    )

    daily_df["rolling_7d_avg"] = daily_df["avg_aqi"].rolling(window=7, min_periods=1).mean()
    daily_df["weekday"] = daily_df["date"].dt.weekday
    daily_df["is_weekend"] = daily_df["weekday"].isin([5, 6]).astype(int)

    weekday_vs_weekend_df = (
        daily_df.assign(day_type=daily_df["is_weekend"].map({0: "weekday", 1: "weekend"}))
        .groupby("day_type", as_index=False)["avg_aqi"]
        .mean()
    )

    monthly_avg_df = (
        daily_df.assign(month=daily_df["date"].dt.month)
        .groupby("month", as_index=False)["avg_aqi"]
        .mean()
        .sort_values("month")
    )

    daily_df = daily_df[["date", "avg_aqi", "rolling_7d_avg"]]

    return daily_df, weekday_vs_weekend_df, monthly_avg_df