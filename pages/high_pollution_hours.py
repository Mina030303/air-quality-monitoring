import streamlit as st
from utils import apply_style, render_global_sidebar, render_back_home_button, load_data, t

apply_style()
render_global_sidebar("pages/high_pollution_hours.py")

trend, county, hours = load_data()

st.title(t("hours_chart_title"))
render_back_home_button()

chart_df = hours.copy()
count_col = chart_df.columns[1]
chart_df = chart_df.rename(columns={count_col: t("high_pollution_hours_label")})

st.bar_chart(chart_df.set_index("hour"))