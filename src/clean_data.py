from __future__ import annotations
import pandas as pd


# ---------- HOURLY ----------

# clean raw hourly data
def clean_hourly_data(df: pd.DataFrame) -> pd.DataFrame:
    
    df = df.copy()

    # normalize column names
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(".", "", regex=False)
        .str.replace(" ", "_", regex=False)
    )

    #  parse datetime 
    df["datacreationdate"] = pd.to_datetime(
        df["datacreationdate"], errors="coerce"
    )

   # convert numeric columns, coercing errors to NaN
    numeric_cols = [
        "siteid", "aqi", "so2", "so2_avg", "co", "co_8hr",
        "o3", "o3_8hr", "pm10", "pm10_avg",
        "pm25", "pm25_avg", "no2", "nox", "no",
        "windspeed", "winddirec", "longitude", "latitude"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # remove duplicates
    df = df.drop_duplicates().reset_index(drop=True)

    return df


# ---------- DAILY ----------

# clean raw daily data
def clean_daily_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # normalize column names
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(".", "", regex=False)
        .str.replace(" ", "_", regex=False)
    )

    # parse datetime
    df["monitordate"] = pd.to_datetime(
        df["monitordate"], errors="coerce"
    )

    # convert numeric columns
    numeric_cols = [
        "siteid", "aqi", "o38subindex", "o3subindex",
        "pm25subindex", "pm10subindex",
        "cosubindex", "so2subindex", "no2subindex"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # remove duplicates
    df = df.drop_duplicates().reset_index(drop=True)

    return df