import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils import apply_style, render_global_sidebar, t
from src.analyze_data import detect_pollution_spikes

# Initialize base style and translation dict routing
apply_style()
render_global_sidebar("spike_detection.py")

st.title(t("spike_title"))
st.caption(t("spike_desc"))

# Load data 
data_path = Path(__file__).resolve().parent.parent / "data" / "processed" / "hourly_clean.csv"
hourly_df = pd.read_csv(data_path)

if hourly_df.empty:
    st.error("No data found.")
    st.stop()

# Ensure datetime
hourly_df["datacreationdate"] = pd.to_datetime(hourly_df["datacreationdate"], errors="coerce")

# 2. UI Controls / Filters
col_p, col_m, col_c = st.columns(3)

with col_p:
    pollutant = st.selectbox(
        t("spike_pollutant_select"), 
        ["aqi", "pm25", "pm10", "o3"], 
        format_func=lambda x: "PM2.5" if x == "pm25" else x.upper(),
        key="spike_pollutant_select"
    )
    pollutant_display = "PM2.5" if pollutant == "pm25" else pollutant.upper()

with col_m:
    method = st.selectbox(
        t("spike_method_select"), 
        ["rolling_threshold", "zscore"], 
        format_func=lambda x: t(f"spike_method_{x}"),
        key="spike_method_select"
    )

with col_c:
    counties = ["All"] + sorted(hourly_df["county"].dropna().unique().tolist())
    selected_county = st.selectbox(t("spike_county_select"), counties, key="spike_county_select")

# Method Parameters Options
with st.expander(t("spike_param_settings"), expanded=False):
    param_col1, param_col2, param_col3 = st.columns(3)
    rolling_window = param_col1.slider(t("spike_rolling_window"), 6, 72, 24, 6)
    
    if method == "rolling_threshold":
        threshold_val = param_col2.slider(t("spike_ratio_threshold"), 1.1, 5.0, 1.5, 0.1)
    else:
        threshold_val = param_col2.slider(t("spike_zscore_threshold"), 1.0, 5.0, 2.5, 0.1)
        
    min_val = param_col3.slider(t("spike_min_value"), 0, 100, 0, 5)

# 3. Filter DataFrame
if selected_county != "All":
    working_df = hourly_df[hourly_df["county"] == selected_county].copy()
else:
    working_df = hourly_df.copy()

# 4. Spike Computation
with st.spinner(t("spike_calculating")):
    val_arg = threshold_val if method == "rolling_threshold" else 1.5
    z_arg = threshold_val if method == "zscore" else 2.5
    
    spikes_df = detect_pollution_spikes(
        working_df, 
        pollutant_col=pollutant,
        method=method,
        rolling_window=rolling_window,
        threshold_ratio=val_arg,
        zscore_threshold=z_arg,
        min_value=min_val
    )

# 5. Dashboard View
if spikes_df.empty:
    st.info(t("spike_no_data"))
    st.stop()

# 5.1 KPIs
total_spikes = len(spikes_df)
max_val = spikes_df[pollutant].max()
top_site = spikes_df["sitename"].mode()[0] if not spikes_df.empty else t("na")

k1, k2, k3 = st.columns(3)
k1.metric(t("spike_total_count"), total_spikes)
k2.metric(f'{t("spike_max_title")} ({pollutant_display})', round(max_val, 2))
k3.metric(t("spike_top_site"), top_site)

st.markdown("---")

# 5.2. Visualizations
st.subheader(t("spike_chart_title"))

# Plot Base Pollutant Line tracking (picking top site for clarity)
chart_site = selected_county if selected_county != "All" else top_site

# If All counties, just plot the top_site to avoid massive line clusters
plot_df = working_df[working_df["sitename"] == top_site] if selected_county == "All" else working_df

base_line = alt.Chart(plot_df).mark_line(opacity=0.4, color='#6B7280').encode(
    x=alt.X("datacreationdate:T", title=t("date_label")),
    y=alt.Y(f"{pollutant}:Q", title=pollutant_display),
    detail="sitename:N",
    tooltip=["sitename", "datacreationdate", pollutant]
)

# Overlay Spikes Layer

# Filter the spikes so they match the plot_df exactly. Otherwise red dots from other stations are drawn on white space.
plot_spikes_df = spikes_df[spikes_df["sitename"] == top_site] if selected_county == "All" else spikes_df

spike_points = alt.Chart(plot_spikes_df).mark_circle(size=80, color='#EF4444').encode(
    x=alt.X("datacreationdate:T", title=""),
    y=alt.Y(f"{pollutant}:Q", title=""),
    tooltip=[
        alt.Tooltip("sitename:N", title=t("spike_site")),
        alt.Tooltip("datacreationdate:T", title=t("date_label")),
        alt.Tooltip(f"{pollutant}:Q", title=pollutant_display),
        alt.Tooltip("spike_strength:Q", title=t("spike_strength"))
    ]
)

combined_chart = (base_line + spike_points).properties(
    height=400,
    background='transparent'
)

st.altair_chart(combined_chart, use_container_width=True)

# 5.3 Interpretation Text
st.markdown(t("spike_interpretation_text"))
