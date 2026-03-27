import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
from utils import apply_style, render_global_sidebar, t

BASE_DIR = Path(__file__).resolve().parent
HOURLY_AQI_PATH = BASE_DIR / "data" / "hourly_aqi.csv"


@st.cache_data(ttl=600)
def load_home_hourly_aqi() -> tuple[pd.DataFrame, str | None, bool]:
    """Load latest hourly AQI CSV with 10-minute cache for Streamlit Home."""
    if not HOURLY_AQI_PATH.exists():
        return pd.DataFrame(), None, False

    df = pd.read_csv(HOURLY_AQI_PATH)
    last_sync: str | None = None

    if "publishtime" in df.columns:
        publish_ts = pd.to_datetime(df["publishtime"], errors="coerce").dropna()
        if not publish_ts.empty:
            last_sync = publish_ts.max().strftime("%Y-%m-%d %H:%M:%S")

    if last_sync is None:
        file_mtime = pd.to_datetime(HOURLY_AQI_PATH.stat().st_mtime, unit="s")
        last_sync = file_mtime.strftime("%Y-%m-%d %H:%M:%S")

    return df, last_sync, True

apply_style()
render_global_sidebar("app.py")

if st.sidebar.button("手動更新數據", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

_, last_sync_time, has_hourly_data = load_home_hourly_aqi()

if has_hourly_data and last_sync_time:
    st.caption(f"📅 數據最後同步時間：{last_sync_time}")
else:
    st.info("目前尚未找到 data/hourly_aqi.csv，請先執行 crawler 或等待 GitHub Actions 同步。")

st.title(t("home_title"))
st.caption(t("home_desc"))

st.markdown('<div class="home-grid">', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        f'''
        <div class="custom-card">
            <div class="card-title">{t("trend")}</div>
            <div class="card-text">{t("trend_desc")}</div>
        </div>
        ''',
        unsafe_allow_html=True,
    )
    if st.button(t("home_button_trend"), key="home_btn_trend", use_container_width=True):
        st.switch_page("pages/trend.py")

with col2:
    st.markdown(
        f'''
        <div class="custom-card">
            <div class="card-title">{t("county_overview_title")}</div>
            <div class="card-text">{t("county_overview_desc")}</div>
        </div>
        ''',
        unsafe_allow_html=True,
    )
    if st.button(t("home_button_county"), key="home_btn_county", use_container_width=True):
        st.switch_page("pages/county_analysis.py")

with col3:
    st.markdown(
        f'''
        <div class="custom-card">
            <div class="card-title">{t("county_risk_title")}</div>
            <div class="card-text">{t("county_risk_desc")}</div>
        </div>
        ''',
        unsafe_allow_html=True,
    )
    if st.button(t("home_button_county_risk"), key="home_btn_county_risk", use_container_width=True):
        st.switch_page("pages/county_risk.py")

st.markdown('<br>', unsafe_allow_html=True)
col4, col5, col6 = st.columns(3)

with col4:
    st.markdown(
        f'''
        <div class="custom-card">
            <div class="card-title">{t("hours")}</div>
            <div class="card-text">{t("hours_desc")}</div>
        </div>
        ''',
        unsafe_allow_html=True,
    )
    if st.button(t("home_button_hours"), key="home_btn_hours", use_container_width=True):
        st.switch_page("pages/high_pollution_hours.py")

with col5:
    st.markdown(
        f'''
        <div class="custom-card">
            <div class="card-title">{t("spike_title")}</div>
            <div class="card-text">{t("spike_desc")}</div>
        </div>
        ''',
        unsafe_allow_html=True,
    )
    if st.button(t("home_button_spike"), key="home_btn_spike", use_container_width=True):
        st.switch_page("pages/spike_detection.py")

st.markdown("</div>", unsafe_allow_html=True)
