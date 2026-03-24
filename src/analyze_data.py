from __future__ import annotations
import pandas as pd
import numpy as np


# ---------- 1. 每日平均 AQI 趨勢 ----------

def daily_avg_aqi(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """計算每日平均 AQI。"""
    df = hourly_df.copy()
    df["datacreationdate"] = pd.to_datetime(df["datacreationdate"], errors="coerce")
    df = df.dropna(subset=["datacreationdate", "aqi"])
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
    """計算各縣市平均 AQI 以進行排名。"""
    df = hourly_df.copy()
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    df = df.dropna(subset=["aqi", "county"])
    
    result = (
        df.groupby("county")["aqi"]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
    )
    return result


# ---------- 2.5 縣市穩定性 vs 波動度 ----------

def analyze_county_stability(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """分析各縣市 AQI 的平均值、波動度及高污染次數，並計算其排名。"""
    df = hourly_df.copy()
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    df = df.dropna(subset=["aqi", "county"])

    # 單次聚合處理平均、變異與高污染次數
    result = df.groupby("county").agg(
        mean_aqi=("aqi", "mean"),
        std_aqi=("aqi", "std"),
        total_count=("aqi", "count"),
        high_pollution_count=("aqi", lambda x: (x > 100).sum())
    ).reset_index()

    result["std_aqi"] = result["std_aqi"].fillna(0)
    # 加入最低樣本數限制：觀察數少於 5，視為代表性不足，比例設為 0
    result["high_pollution_ratio"] = np.where(result["total_count"] < 5, 0.0, (result["high_pollution_count"] / result["total_count"]).fillna(0))
    
    # 排名
    result["mean_rank"] = result["mean_aqi"].rank(ascending=False, method="min").astype(int)
    result["volatility_rank"] = result["std_aqi"].rank(ascending=False, method="min").astype(int)
    result["high_pollution_rank"] = result["high_pollution_count"].rank(ascending=False, method="min").astype(int)
    result["high_pollution_ratio_rank"] = result["high_pollution_ratio"].rank(ascending=False, method="min").astype(int)

    return result.sort_values("mean_rank")


# ---------- 2.6 縣市風險分數 (County Risk) ----------

def calculate_county_risk_score(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """計算各縣市的綜合污染風險分數 (合併平均濃度與相對波動特徵)。"""
    df = hourly_df.copy()
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    df = df.dropna(subset=["aqi", "county"])

    result = df.groupby("county").agg(
        mean_aqi=("aqi", "mean"),
        std_aqi=("aqi", "std"),
        total_count=("aqi", "count"),
        high_pollution_count=("aqi", lambda x: (x > 100).sum())
    ).reset_index()

    result["std_aqi"] = result["std_aqi"].fillna(0)
    # 加入最低樣本數限制：觀察數少於 5，視為代表性不足，比例設為 0
    result["high_pollution_ratio"] = np.where(result["total_count"] < 5, 0.0, (result["high_pollution_count"] / result["total_count"]).fillna(0))
    
    # 安全計算 CV (變異係數)：加入邏輯基數 30.0，防止低污染區 (如台東) 因分母極小導致 CV 噴發誤導
    safe_mean = np.maximum(result["mean_aqi"], 30.0)
    result["cv_aqi"] = result["std_aqi"] / safe_mean
    
    def normalize(series):
        s_min, s_max = series.min(), series.max()
        return (series - s_min) / (s_max - s_min) if s_max > s_min else series * 0.0

    result["mean_aqi_norm"] = normalize(result["mean_aqi"])
    result["cv_aqi_norm"] = normalize(result["cv_aqi"])

    # Base Score & Penalty
    base_score = result["mean_aqi_norm"] * 0.5 + result["cv_aqi_norm"] * 0.5
    raw_risk = base_score * (1.0 + result["high_pollution_ratio"])

    # 常態化成 0~100 分
    result["risk_score"] = normalize(raw_risk) * 100.0
    result["risk_rank"] = result["risk_score"].rank(ascending=False, method="min").astype(int)
    
    final_cols = ["county", "mean_aqi", "std_aqi", "high_pollution_ratio", "cv_aqi", "risk_score", "risk_rank"]
    return result[final_cols].sort_values("risk_score", ascending=False)


# ---------- 3. 高污染時段（AQI > 100） ----------

def high_pollution_hours(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """計算整體資料中，各小時發生高污染 (AQI > 100) 的總次數。"""
    df = hourly_df.copy()
    df["datacreationdate"] = pd.to_datetime(df["datacreationdate"], errors="coerce")
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    df = df.dropna(subset=["datacreationdate", "aqi"])

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
    """計算全區各小時高污染 (AQI > 100) 的發生比例。"""
    df = hourly_df.copy()
    df["datacreationdate"] = pd.to_datetime(df["datacreationdate"], errors="coerce")
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    df = df.dropna(subset=["datacreationdate", "aqi"])
    df["hour"] = df["datacreationdate"].dt.hour
    
    result = df.groupby("hour").agg(
        total_count=("aqi", "count"),
        high_pollution_count=("aqi", lambda x: (x > 100).sum())
    ).reset_index()
    
    # 加入最低樣本數限制避免單一極端值主導
    result["high_pollution_ratio"] = np.where(result["total_count"] < 5, 0.0, (result["high_pollution_count"] / result["total_count"]).fillna(0))
    return result.sort_values("hour").reset_index(drop=True)


def high_pollution_hour_ratio_by_county(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """計算各縣市每小時高污染 (AQI > 100) 的發生比例。"""
    df = hourly_df.copy()
    df["datacreationdate"] = pd.to_datetime(df["datacreationdate"], errors="coerce")
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    df = df.dropna(subset=["datacreationdate", "aqi", "county"])
    df["hour"] = df["datacreationdate"].dt.hour
    
    result = df.groupby(["county", "hour"]).agg(
        total_count=("aqi", "count"),
        high_pollution_count=("aqi", lambda x: (x > 100).sum())
    ).reset_index()
    # 加入最低樣本數限制避免單一極端值主導
    result["high_pollution_ratio"] = np.where(result["total_count"] < 5, 0.0, (result["high_pollution_count"] / result["total_count"]).fillna(0))
    return result.sort_values(["county", "hour"]).reset_index(drop=True)


# ---------- 4. 時間結構分析（日、週末、月份） ----------

def time_structure_analysis(
    hourly_clean: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """計算日平均與 7 天滾動平均、週末效應、及月平均趨勢。"""
    df = hourly_clean.copy()
    df["datacreationdate"] = pd.to_datetime(df["datacreationdate"], errors="coerce")
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
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
    """依據日平均 AQI 與滾動平均趨勢，判斷污染的短期狀態。"""
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

    # 動態判讀：從絕對達標改為要求 1) 長期均值以上增長 25%, 且 2) AQI 大於 50 避免極低區間雜訊, 且 3) 絕對值增長至少 10
    if (latest_aqi >= latest_rolling * 1.25) and (latest_aqi >= 50) and (latest_aqi - latest_rolling >= 10):
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
    偵測測站空品異常飆高的事件。向量化操作避免 Python for 迴圈，
    並嚴格利用 .shift(1) 防止看透未來偏差（Look-ahead bias）。
    """
    result_df = df.copy()
    result_df[time_col] = pd.to_datetime(result_df[time_col], errors="coerce")
    result_df[pollutant_col] = pd.to_numeric(result_df[pollutant_col], errors="coerce")
    result_df = result_df.dropna(subset=[pollutant_col, time_col])
    
    result_df = result_df.sort_values(by=[site_col, time_col]).reset_index(drop=True)
    # 填補因斷訊導致的空缺 (前向填補，限3小時以內)
    result_df[pollutant_col] = result_df.groupby(site_col)[pollutant_col].ffill(limit=3)
    
    # 計算「區域背景值」：找出同時段同縣市「其他」測站的平均值。避免受每日普遍通勤尖峰誤導
    county_sum = result_df.groupby([county_col, time_col])[pollutant_col].transform('sum')
    county_count = result_df.groupby([county_col, time_col])[pollutant_col].transform('count')
    # 若該縣市此時只有一個有效測站，背景值退化為自己本身，否則排除自己來算同區平均
    county_bg = np.where(county_count > 1, (county_sum - result_df[pollutant_col]) / (county_count - 1), result_df[pollutant_col])

    # 群組化並提取目標序列
    grouped = result_df.groupby(site_col)[pollutant_col]
    
    # 針對所有 group 進行向量化運算
    expanding_mean_past = grouped.expanding().mean().reset_index(level=0, drop=True).shift(1)
    
    if method == "rolling_threshold":
        rolling_mean_past = grouped.rolling(window=rolling_window, min_periods=1).mean().reset_index(level=0, drop=True).shift(1)
        baseline = rolling_mean_past.fillna(expanding_mean_past).clip(lower=0.1)
        
        ratio = result_df[pollutant_col] / baseline
        # 新增條件：必須也高於「同縣市同時段區域背景值」達 25%，確保這是「局部異質污染」而非大環境一起變差
        is_spike = (ratio > threshold_ratio) & (result_df[pollutant_col] >= min_value) & (result_df[pollutant_col] > county_bg * 1.25)
        strength = result_df[pollutant_col] - baseline
        z_score = pd.Series([pd.NA] * len(result_df), index=result_df.index)
        
    elif method == "zscore":
        expanding_std_past = grouped.expanding().std().reset_index(level=0, drop=True).shift(1)
        expanding_std_past = expanding_std_past.replace(0, 1.0).fillna(1.0)
        baseline = expanding_mean_past.clip(lower=0.1)
        
        z_score = (result_df[pollutant_col] - baseline) / expanding_std_past
        # 加入大於區域背景值的檢驗，確保是「局部異質」
        is_spike = (z_score > zscore_threshold) & (result_df[pollutant_col] >= min_value) & (result_df[pollutant_col] > county_bg * 1.25)
        strength = result_df[pollutant_col] - baseline
        
    result_df["baseline"] = baseline
    result_df["z_score"] = z_score
    result_df["spike_flag"] = is_spike
    result_df["spike_strength"] = strength

    spikes = result_df[result_df["spike_flag"]].copy()
    cols_to_keep = [time_col, site_col, county_col, pollutant_col, "baseline", "z_score", "spike_strength"]
    return spikes[cols_to_keep].sort_values(by=time_col, ascending=False).reset_index(drop=True)


# ---------- 7. 異常污染飆高 Summaries ----------

def spike_summary_by_county(spike_df: pd.DataFrame) -> pd.DataFrame:
    """彙整各縣市的 Spike 次數與強度。"""
    if spike_df.empty:
        return pd.DataFrame(columns=["county", "spike_count", "avg_spike_strength", "max_spike_strength"])
        
    summary = spike_df.groupby("county").agg(
        spike_count=("sitename", "count"),
        avg_spike_strength=("spike_strength", "mean"),
        max_spike_strength=("spike_strength", "max")
    ).reset_index()
    return summary.sort_values(by="spike_count", ascending=False).reset_index(drop=True)

def spike_summary_by_site(spike_df: pd.DataFrame) -> pd.DataFrame:
    """彙整各測站的 Spike 次數與強度。"""
    if spike_df.empty:
        return pd.DataFrame(columns=["sitename", "county", "spike_count", "avg_spike_strength", "max_spike_strength"])
        
    summary = spike_df.groupby(["sitename", "county"]).agg(
        spike_count=("sitename", "count"),
        avg_spike_strength=("spike_strength", "mean"),
        max_spike_strength=("spike_strength", "max")
    ).reset_index()
    return summary.sort_values(by="spike_count", ascending=False).reset_index(drop=True)

def spike_time_pattern(spike_df: pd.DataFrame, time_col: str = "datacreationdate") -> pd.DataFrame:
    """解析每日各小時容易發生 Spike 的時間規律。"""
    if spike_df.empty:
        return pd.DataFrame(columns=["hour", "spike_count"])
        
    df = spike_df.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col])
    df["hour"] = df[time_col].dt.hour
    
    summary = df.groupby("hour").agg(
        spike_count=("sitename", "count")
    ).reset_index()
    return summary.sort_values(by="hour").reset_index(drop=True)