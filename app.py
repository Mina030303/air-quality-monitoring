import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
from utils import apply_style, render_global_sidebar, t

apply_style()
render_global_sidebar("app.py")

st.title(t("home_title"))
st.caption(t("home_desc"))

st.markdown('<div class="home-grid">', unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        f"""
        <div class="custom-card">
            <div class="card-title">{t("trend")}</div>
            <div class="card-text">{t("trend_desc")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button(t("home_button_trend"), key="home_btn_trend", use_container_width=True):
        st.switch_page("pages/trend.py")

with col2:
    st.markdown(
        f"""
        <div class="custom-card">
            <div class="card-title">{t("county")}</div>
            <div class="card-text">{t("county_desc")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button(t("home_button_county"), key="home_btn_county", use_container_width=True):
        st.switch_page("pages/county_analysis.py")

with col3:
    st.markdown(
        f"""
        <div class="custom-card">
            <div class="card-title">{t("hours")}</div>
            <div class="card-text">{t("hours_desc")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button(t("home_button_hours"), key="home_btn_hours", use_container_width=True):
        st.switch_page("pages/high_pollution_hours.py")

st.markdown('<br>', unsafe_allow_html=True)
col4, col5, col6 = st.columns(3)

with col4:
    st.markdown(
        f"""
        <div class="custom-card">
            <div class="card-title">{t("spike_title")}</div>
            <div class="card-text">{t("spike_desc")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button(t("home_button_spike"), key="home_btn_spike", use_container_width=True):
        st.switch_page("pages/spike_detection.py")

st.markdown("</div>", unsafe_allow_html=True)
