import streamlit as st
import pandas as pd
from utils import apply_style, render_global_sidebar, render_back_home_button, load_data, t, BASE_DIR

apply_style()
render_global_sidebar("pages/high_pollution_hours.py")

trend, county, hours = load_data()

# Load new probability data
try:
    ratio_df = pd.read_csv(BASE_DIR / "output/tables/high_pollution_hour_ratio.csv")
except Exception as e:
    st.error(f"Failed to load new ratio data: {e}")
    ratio_df = pd.DataFrame()

st.title(t("hours_risk_analysis_title"))
st.markdown(t("hours_risk_analysis_desc"))

render_back_home_button()

st.divider()

st.subheader(t("hours_prob_by_hour"))
if not ratio_df.empty:
    st.bar_chart(ratio_df.set_index("hour")["high_pollution_ratio"])
    
    with st.expander(t("hours_view_overall_ratio")):
        st.dataframe(ratio_df, use_container_width=True)
else:
    st.info(t("hours_overall_ratio_na"))

st.divider()

st.subheader(t("hours_original_count_analysis"))
chart_df = hours.copy()
count_col = chart_df.columns[1]
chart_df = chart_df.rename(columns={count_col: t("high_pollution_hours_label")})

st.bar_chart(chart_df.set_index("hour"))
with st.expander(t("hours_view_raw_count")):
    st.dataframe(chart_df, use_container_width=True)
