import streamlit as st
import pandas as pd
from pathlib import Path
from config import TEXT

BASE_DIR = Path(__file__).resolve().parent


def t(key: str) -> str:
    lang = st.session_state.get("lang", "zh")
    return TEXT[lang][key]


def set_language():
    if "lang" not in st.session_state:
        st.session_state.lang = "zh"

    lang_map = {
        "zh": "中文",
        "en": "EN",
    }

    selected = st.selectbox(
        "Language",
        options=list(lang_map.keys()),
        index=list(lang_map.keys()).index(st.session_state.lang),
        format_func=lambda x: lang_map[x],
        key="global_lang_select",
        label_visibility="collapsed",
    )

    if selected != st.session_state.lang:
        st.session_state.lang = selected
        st.rerun()

    return st.session_state.lang


def render_global_sidebar(current_page: str):
    set_language()

    pages = [
        ("app.py", t("home_menu")),
        ("pages/trend.py", t("trend")),
        ("pages/county_analysis.py", t("county")),
        ("pages/high_pollution_hours.py", t("hours")),
        ("pages/spike_detection.py", t("spike_title")),
    ]

    for page_path, label in pages:
        st.sidebar.page_link(
            page_path,
            label=label,
            disabled=(page_path == current_page),
        )


def render_back_home_button():
    if st.button(t("back_home_button")):
        st.switch_page("app.py")


def get_aqi_band(aqi: float) -> tuple[str, str, str]:
    if aqi <= 50:
        return t("aqi_good_label"), "#2e7d32", t("aqi_good_text")
    if aqi <= 100:
        return t("aqi_moderate_label"), "#e6b800", t("aqi_moderate_text")
    return t("aqi_polluted_label"), "#d64545", t("aqi_polluted_text")


def render_aqi_meaning_block():
    st.markdown(f"**{t('aqi_guide_title')}**")
    st.markdown(
        f"""
<div style="line-height: 1.7;">
  <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#2e7d32;margin-right:8px;"></span>{t('aqi_good_label')} (0-50): {t('aqi_good_text')}</div>
  <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#e6b800;margin-right:8px;"></span>{t('aqi_moderate_label')} (51-100): {t('aqi_moderate_text')}</div>
  <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#d64545;margin-right:8px;"></span>{t('aqi_polluted_label')} (101+): {t('aqi_polluted_text')}</div>
</div>
        """,
        unsafe_allow_html=True,
    )


def apply_style():
    st.set_page_config(
        page_title="Air Quality Dashboard",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.markdown(
        """
        <style>
        .stApp {
            background: linear-gradient(180deg, #f7fbff 0%, #eef7fc 100%);
        }

        h1, h2, h3 {
            color: #24557a;
        }

        header[data-testid="stHeader"] {
            background: transparent !important;
        }

        .block-container {
            padding-top: 0.35rem;
        }

        h1 {
            margin-top: -16px;
        }

        [data-testid="stDecoration"] {
            display: none !important;
        }

        /* 只隱藏右上角元件 */
        [data-testid="stAppDeployButton"] {
            display: none !important;
        }

        #MainMenu {
            display: none !important;
        }

        /* 保留左上 sidebar 開關 */
        [data-testid="collapsedControl"] {
            display: flex !important;
            visibility: visible !important;
            opacity: 1 !important;
            position: fixed !important;
            top: 0.5rem !important;
            left: 0.5rem !important;
            z-index: 100001 !important;
        }

        button[kind="header"] {
            display: inline-flex !important;
            visibility: visible !important;
            opacity: 1 !important;
        }

        /* 隱藏 sidebar 內建 pages 導航 */
        [data-testid="stSidebarNav"] {
            display: none !important;
        }

        section[data-testid="stSidebar"] .block-container {
            padding-top: 1rem;
        }

        /* 把語言選單固定到右上角，避免被 header 蓋住 */
        .st-key-global_lang_select {
            position: fixed !important;
            top: 4.2rem !important;
            right: 5rem !important;
            width: 110px !important;
            z-index: 99999 !important;
            margin: 0 !important;
        }

        /* 語言選單的內部樣式 */
        .st-key-global_lang_select div[data-baseweb="select"] {
            min-width: 100px !important;
            cursor: pointer !important;          /* 滑鼠變手指 */
            position: relative !important;
            z-index: 99999 !important;
        }

        .st-key-global_lang_select div[data-baseweb="select"] > div {
            min-height: 34px !important;
            height: 34px !important;
            border-radius: 10px !important;
            border: 1px solid #c9d7e3 !important;
            background: #ffffff !important;
            box-shadow: none !important;
            padding: 0 8px !important;           /* 左右收緊 */
            cursor: pointer !important;
        }

        .st-key-global_lang_select div[data-baseweb="select"] span {
            font-size: 14px !important;
            cursor: pointer !important;
        }

        .st-key-global_lang_select div[data-baseweb="select"]:hover > div {
            border-color: #9fb8cf !important;
            background: #f6f9fc !important;
        }

        /* 縣市 filter 樣式 (白底藍框，與語言選單一致) */
        .st-key-hours_county_select div[data-baseweb="select"] > div,
        .st-key-spike_pollutant_select div[data-baseweb="select"] > div,
        .st-key-spike_method_select div[data-baseweb="select"] > div,
        .st-key-spike_county_select div[data-baseweb="select"] > div {
            border-radius: 10px !important;
            border: 1px solid #c9d7e3 !important;
            background: #ffffff !important;
            box-shadow: none !important;
        }

        /* hover 或點擊時維持與語言選單相同效果 */
        .st-key-hours_county_select div[data-baseweb="select"]:hover > div,
        .st-key-hours_county_select div[data-baseweb="select"]:focus-within > div,
        .st-key-spike_pollutant_select div[data-baseweb="select"]:hover > div,
        .st-key-spike_pollutant_select div[data-baseweb="select"]:focus-within > div,
        .st-key-spike_method_select div[data-baseweb="select"]:hover > div,
        .st-key-spike_method_select div[data-baseweb="select"]:focus-within > div,
        .st-key-spike_county_select div[data-baseweb="select"]:hover > div,
        .st-key-spike_county_select div[data-baseweb="select"]:focus-within > div {
            border-color: #9fb8cf !important;
            background: #f6f9fc !important;
        }

        /* 首頁卡片 */
        .custom-card {
            background: rgba(255, 255, 255, 0.82);
            border: 1px solid rgba(210, 225, 235, 0.95);
            border-radius: 18px;
            padding: 22px 20px 18px 20px;
            min-height: 170px;
            box-shadow: 0 8px 22px rgba(80, 120, 160, 0.10);
            backdrop-filter: blur(4px);
            -webkit-backdrop-filter: blur(4px);
            margin-bottom: 10px;
        }

        .card-title {
            font-size: 1.15rem;
            font-weight: 700;
            color: #244a68;
            margin-bottom: 10px;
        }

        .card-text {
            font-size: 0.96rem;
            line-height: 1.65;
            color: #466176;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data
def load_data():
    time_structure_path = BASE_DIR / "output/tables/daily_time_structure.csv"
    fallback_path = BASE_DIR / "output/tables/daily_trend.csv"
    trend_path = time_structure_path if time_structure_path.exists() else fallback_path

    trend = pd.read_csv(trend_path)
    county = pd.read_csv(BASE_DIR / "output/tables/county_avg.csv")
    hours = pd.read_csv(BASE_DIR / "output/tables/high_pollution_hours.csv")
    return trend, county, hours