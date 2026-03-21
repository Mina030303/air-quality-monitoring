from __future__ import annotations
import pandas as pd


# ---------- HOURLY ----------

def clean_hourly_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 欄位統一
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(".", "", regex=False)
        .str.replace(" ", "_", regex=False)
    )

    # 時間
    df["datacreationdate"] = pd.to_datetime(
        df["datacreationdate"], errors="coerce"
    )

    # 數值欄位
    numeric_cols = [
        "siteid", "aqi", "so2", "so2_avg", "co", "co_8hr",
        "o3", "o3_8hr", "pm10", "pm10_avg",
        "pm2.5", "pm2.5_avg", "no2", "nox", "no",
        "windspeed", "winddirec", "longitude", "latitude"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 去重
    df = df.drop_duplicates().reset_index(drop=True)

    return df


# ---------- DAILY ----------

def clean_daily_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(".", "", regex=False)
        .str.replace(" ", "_", regex=False)
    )

    df["monitordate"] = pd.to_datetime(
        df["monitordate"], errors="coerce"
    )

    numeric_cols = [
        "siteid", "aqi", "o38subindex", "o3subindex",
        "pm25subindex", "pm10subindex",
        "cosubindex", "so2subindex", "no2subindex"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates().reset_index(drop=True)

    return df