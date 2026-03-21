import streamlit as st
import pandas as pd
from pathlib import Path

# ---------- BASE ----------
BASE_DIR = Path(__file__).resolve().parent

st.set_page_config(
    page_title="Air Quality Dashboard",
    layout="wide",
)

# ---------- STYLE ----------
st.markdown("""
<style>
.main {
    background-color: #f7fbff;
}
h1, h2, h3 {
    color: #1f4e79;
}
.block-container {
    padding-top: 2rem;
}
</style>
""", unsafe_allow_html=True)

# ---------- TITLE ----------
st.title("🌫 Taiwan Air Quality Dashboard")

# ---------- LOAD DATA ----------
trend = pd.read_csv(BASE_DIR / "output/tables/daily_trend.csv")
county = pd.read_csv(BASE_DIR / "output/tables/county_avg.csv")
hours = pd.read_csv(BASE_DIR / "output/tables/high_pollution_hours.csv")

# ---------- SIDEBAR ----------
st.sidebar.header("🔎 Filter（預留）")

selected_county = st.sidebar.selectbox(
    "選擇縣市",
    options=["全部"] + list(county["county"].unique())
)

# ---------- KPI ----------
st.subheader("📊 Overview")

col1, col2, col3 = st.columns(3)

col1.metric("平均 AQI", round(trend["avg_aqi"].mean(), 1))
col2.metric("最高 AQI", int(trend["avg_aqi"].max()))
col3.metric("高污染時段數", int(hours["high_pollution_count"].sum()))

# ---------- TREND ----------
st.subheader("📈 AQI Trend")

st.line_chart(trend.set_index("date"))

# ---------- COUNTY ----------
st.subheader("🏙 Pollution by County")

if selected_county != "全部":
    county_filtered = county[county["county"] == selected_county]
else:
    county_filtered = county

st.bar_chart(county_filtered.head(10).set_index("county"))

# ---------- HOURS ----------
st.subheader("⏰ High Pollution Hours")

st.bar_chart(hours.set_index("hour"))

# ---------- FUTURE ANALYSIS ----------
st.subheader("🧠 Advanced Analysis（預留）")

st.info("""
這個區塊未來可以加入：

- 污染 spike 偵測
- 測站分析
- 污染物分布（PM2.5 / O3）
- 時間序列模型
""")