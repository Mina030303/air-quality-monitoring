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


# ---------- 2.5 縣市穩定性 vs 波動度 ----------

def analyze_county_stability(hourly_df: pd.DataFrame) -> pd.DataFrame:
    df = hourly_df.copy()

    # Calculate metrics per county
    grouped = df.groupby("county")["aqi"]
    
    metrics = grouped.agg(
        mean_aqi="mean",
        std_aqi="std",
        total_count="count",
    ).reset_index()

    # Calculate high pollution count safely (aqi > 100)
    high_pol_counts = df[df["aqi"] > 100].groupby("county").size().reset_index(name="high_pollution_count")
    
    # Merge and fillna for counties with 0 high pollution events
    result = metrics.merge(high_pol_counts, on="county", how="left")
    result["high_pollution_count"] = result["high_pollution_count"].fillna(0)
    
    # Calculate ratio
    result["high_pollution_ratio"] = result["high_pollution_count"] / result["total_count"]
    
    # Calculate ranks (dense descending)
    result["mean_rank"] = result["mean_aqi"].rank(ascending=False, method="min").astype(int)
    result["volatility_rank"] = result["std_aqi"].rank(ascending=False, method="min").astype(int)
    result["high_pollution_rank"] = result["high_pollution_count"].rank(ascending=False, method="min").astype(int)
    result["high_pollution_ratio_rank"] = result["high_pollution_ratio"].rank(ascending=False, method="min").astype(int)

    return result.sort_values("mean_rank")

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


def high_pollution_hour_ratio(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the ratio of high pollution (AQI > 100) records for each hour of the day.
    Outputs: hour, total_count, high_pollution_count, high_pollution_ratio
    """
    df = hourly_df.copy()
    
    # Ensure datacreationdate is datetime and aqi is numeric
    df["datacreationdate"] = pd.to_datetime(df["datacreationdate"], errors="coerce")
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    
    # Drop rows with missing datetime or aqi
    df = df.dropna(subset=["datacreationdate", "aqi"])
    
    # Extract hour
    df["hour"] = df["datacreationdate"].dt.hour
    
    # Calculate total records per hour
    total_counts = df.groupby("hour").size().reset_index(name="total_count")
    
    # Calculate high pollution records per hour
    high_pol = df[df["aqi"] > 100]
    high_counts = high_pol.groupby("hour").size().reset_index(name="high_pollution_count")
    
    # Merge and fill missing high pollution counts with 0, then cast both to int
    result = total_counts.merge(high_counts, on="hour", how="left")
    result["high_pollution_count"] = result["high_pollution_count"].fillna(0).astype(int)
    result["total_count"] = result["total_count"].astype(int)
    
    # Calculate ratio safely to prevent division by zero
    result["high_pollution_ratio"] = 0.0
    mask = result["total_count"] > 0
    result.loc[mask, "high_pollution_ratio"] = result.loc[mask, "high_pollution_count"] / result.loc[mask, "total_count"]
    
    # Sort primarily by hour
    result = result.sort_values("hour").reset_index(drop=True)
    
    return result[["hour", "total_count", "high_pollution_count", "high_pollution_ratio"]]


def high_pollution_hour_ratio_by_county(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the ratio of high pollution (AQI > 100) records for each hour, grouped by county.
    Outputs: county, hour, total_count, high_pollution_count, high_pollution_ratio
    """
    df = hourly_df.copy()
    
    # Ensure datacreationdate is datetime and aqi is numeric
    df["datacreationdate"] = pd.to_datetime(df["datacreationdate"], errors="coerce")
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    
    # Drop rows with missing datetime, aqi, or county
    df = df.dropna(subset=["datacreationdate", "aqi", "county"])
    
    # Extract hour
    df["hour"] = df["datacreationdate"].dt.hour
    
    # Calculate total records per county and hour
    total_counts = df.groupby(["county", "hour"]).size().reset_index(name="total_count")
    
    # Calculate high pollution records per county and hour
    high_pol = df[df["aqi"] > 100]
    high_counts = high_pol.groupby(["county", "hour"]).size().reset_index(name="high_pollution_count")
    
    # Merge and fill missing high pollution counts with 0, then cast both to int
    result = total_counts.merge(high_counts, on=["county", "hour"], how="left")
    result["high_pollution_count"] = result["high_pollution_count"].fillna(0).astype(int)
    result["total_count"] = result["total_count"].astype(int)
    
    # Calculate ratio safely to prevent division by zero
    result["high_pollution_ratio"] = 0.0
    mask = result["total_count"] > 0
    result.loc[mask, "high_pollution_ratio"] = result.loc[mask, "high_pollution_count"] / result.loc[mask, "total_count"]
    
    # Sort primarily by county, then by hour
    result = result.sort_values(["county", "hour"]).reset_index(drop=True)
    
    return result[["county", "hour", "total_count", "high_pollution_count", "high_pollution_ratio"]]
    result = result.sort_values(["county", "hour"]).reset_index(drop=True)
    
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

# ---------- 5. 目前狀態判讀 ----------

def current_status_interpretation(daily_df: pd.DataFrame) -> str:
    df = daily_df.copy()

    if df.empty or len(df) < 2:
        return "status_insufficient_data"

    df = df.sort_values("date").reset_index(drop=True)

    latest = df.iloc[-1]
    previous = df.iloc[-2]

    latest_aqi = latest["avg_aqi"]
    latest_rolling = latest["rolling_7d_avg"]
    previous_rolling = previous["rolling_7d_avg"]

    if latest_aqi >= 100 and latest_rolling >= 80:
        return "status_sustained_pollution"

    if latest_aqi - latest_rolling >= 15:
        return "status_short_term_spike"

    if latest_rolling < previous_rolling - 3:
        return "status_improving_trend"

    if abs(latest_aqi - latest_rolling) < 10 and abs(latest_rolling - previous_rolling) < 3:
        return "status_normal_variation"

    if latest_rolling > previous_rolling:
        return "status_worsening_trend"

    return "status_normal_variation"


# ---------- 6. 異常污染飆高 (Spike) 偵測 ----------

def detect_pollution_spikes(
    df: pd.DataFrame,
    pollutant_col: str,
    site_col: str = "sitename",
    time_col: str = "datacreationdate",
    county_col: str = "county",
    method: str = "rolling_threshold",
    rolling_window: int = 24,
    threshold_ratio: float = 1.5,
    zscore_threshold: float = 2.5,
    min_value: float = 0.0
) -> pd.DataFrame:
    """
    Detect abnormal high-pollution events (spikes) in air quality time series.
    Uses strict look-ahead bias avoidance.
    """
    result_df = df.copy()
    
    # Validation & Cleaning
    result_df[time_col] = pd.to_datetime(result_df[time_col], errors="coerce")
    result_df[pollutant_col] = pd.to_numeric(result_df[pollutant_col], errors="coerce")
    result_df = result_df.dropna(subset=[pollutant_col, time_col])
    
    # Sort for robust time series calculations
    result_df = result_df.sort_values(by=[site_col, time_col]).reset_index(drop=True)
    
    # Initialize metric arrays
    baseline_list = []
    zscore_list = []
    spike_flag_list = []
    spike_strength_list = []
    
    # Process GroupBy avoiding SettingWithCopyWarning
    for site, group in result_df.groupby(site_col):
        values = group[pollutant_col]
        
        # Calculate past expanding mean
        expanding_mean_past = values.expanding().mean().shift(1)
        
        if method == "rolling_threshold":
            # Calculate past rolling mean
            rolling_mean_past = values.rolling(window=rolling_window, min_periods=1).mean().shift(1)
            
            # Fill NaNs with past expanding mean, clip to 0.1 to avoid DivisionByZero
            baseline = rolling_mean_past.fillna(expanding_mean_past).clip(lower=0.1)
            
            # Calculate ratios and logic
            ratio = values / baseline
            is_spike = (ratio > threshold_ratio) & (values >= min_value)
            
            strength = values - baseline
            z_score = pd.Series([pd.NA] * len(values), index=values.index)
            
        elif method == "zscore":
            expanding_std_past = values.expanding().std().shift(1)
            
            # Fallback for STD 0
            expanding_std_past = expanding_std_past.replace(0, 1.0).fillna(1.0)
            baseline = expanding_mean_past.clip(lower=0.1)
            
            z_score = (values - baseline) / expanding_std_past
            is_spike = (z_score > zscore_threshold) & (values >= min_value)
            
            strength = values - baseline
            
        baseline_list.extend(baseline.tolist())
        zscore_list.extend(z_score.tolist())
        spike_flag_list.extend(is_spike.tolist())
        spike_strength_list.extend(strength.tolist())
        
    result_df["baseline"] = baseline_list
    result_df["z_score"] = zscore_list
    result_df["spike_flag"] = spike_flag_list
    result_df["spike_strength"] = spike_strength_list

    spikes = result_df[result_df["spike_flag"]].copy()
    cols_to_keep = [time_col, site_col, county_col, pollutant_col, "baseline", "z_score", "spike_strength"]
    return spikes[cols_to_keep].sort_values(by=time_col, ascending=False).reset_index(drop=True)

# ---------- 7. 異常污染飆高 Summaries ----------

def spike_summary_by_county(spike_df: pd.DataFrame) -> pd.DataFrame:
    if spike_df.empty:
        return pd.DataFrame(columns=["county", "spike_count", "avg_spike_strength", "max_spike_strength"])
        
    summary = spike_df.groupby("county").agg(
        spike_count=("sitename", "count"),
        avg_spike_strength=("spike_strength", "mean"),
        max_spike_strength=("spike_strength", "max")
    ).reset_index()
    return summary.sort_values(by="spike_count", ascending=False).reset_index(drop=True)

def spike_summary_by_site(spike_df: pd.DataFrame) -> pd.DataFrame:
    if spike_df.empty:
        return pd.DataFrame(columns=["sitename", "county", "spike_count", "avg_spike_strength", "max_spike_strength"])
        
    summary = spike_df.groupby(["sitename", "county"]).agg(
        spike_count=("sitename", "count"),
        avg_spike_strength=("spike_strength", "mean"),
        max_spike_strength=("spike_strength", "max")
    ).reset_index()
    return summary.sort_values(by="spike_count", ascending=False).reset_index(drop=True)

def spike_time_pattern(spike_df: pd.DataFrame, time_col: str = "datacreationdate") -> pd.DataFrame:
    if spike_df.empty:
        return pd.DataFrame(columns=["hour", "spike_count"])
        
    df = spike_df.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df["hour"] = df[time_col].dt.hour
    
    summary = df.groupby("hour").agg(
        spike_count=("sitename", "count")
    ).reset_index()
    return summary.sort_values(by="hour").reset_index(drop=True)