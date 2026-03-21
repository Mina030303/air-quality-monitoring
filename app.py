import streamlit as st
import pandas as pd

st.set_page_config(page_title="Air Quality Dashboard", layout="wide")

st.title("Taiwan Air Quality Dashboard")

# ---------- LOAD DATA ----------
daily = pd.read_csv("data/processed/daily_clean.csv")
trend = pd.read_csv("output/tables/daily_trend.csv")
county = pd.read_csv("output/tables/county_avg.csv")
hours = pd.read_csv("output/tables/high_pollution_hours.csv")

# ---------- TREND ----------
st.subheader("Daily AQI Trend")
st.line_chart(trend.set_index("date"))

# ---------- COUNTY ----------
st.subheader("Top Polluted Counties")
st.bar_chart(county.head(10).set_index("county"))

# ---------- HOURS ----------
st.subheader("High Pollution Hours")
st.bar_chart(hours.set_index("hour"))