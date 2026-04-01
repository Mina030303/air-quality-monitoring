import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils import apply_style, render_global_sidebar, t, load_raw_data, cached_detect_pollution_spikes

# Initialize base style and translation dict routing
apply_style()
render_global_sidebar("pages/spike_detection.py")

with st.spinner(t('loading_raw_data')):
    hourly_df = load_raw_data()

if hourly_df.empty:
    st.error(t("data_not_found"))
    st.stop()

st.title(t("spike_title"))
st.caption(t("spike_desc"))

tab_event, tab_summary = st.tabs([t("spike_event_tab"), t("spike_summary_tab")])

with tab_event:
    event_heatmap_slot = st.container()
    event_series_slot = st.container()
    event_settings_slot = st.container()
    event_interp_slot = st.container()

    with event_settings_slot:
        st.markdown(
            f"<div style='font-size: 1.1rem; font-weight: 600; margin-top: 0.25rem;'>{t('spike_param_settings')}</div>",
            unsafe_allow_html=True,
        )
        with st.expander(t("spike_param_settings"), expanded=False):
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
                counties = ["__all__"] + sorted(hourly_df["county"].dropna().unique().tolist())
                selected_county = st.selectbox(
                    t("spike_county_select"),
                    counties,
                    key="spike_county_select",
                    format_func=lambda x: t("all_option") if x == "__all__" else x,
                )

            param_col1, param_col2, param_col3 = st.columns(3)
            rolling_window = param_col1.slider(t("spike_rolling_window"), 6, 72, 24, 6)
            if method == "rolling_threshold":
                threshold_val = param_col2.slider(t("spike_ratio_threshold"), 1.1, 5.0, 1.5, 0.1)
            else:
                threshold_val = param_col2.slider(t("spike_zscore_threshold"), 1.0, 5.0, 2.5, 0.1)
            min_val = param_col3.slider(t("spike_min_value"), 0, 100, 0, 5)

# Calculate Data
if selected_county != "__all__":
    working_df = hourly_df[hourly_df["county"] == selected_county].copy()       
else:
    working_df = hourly_df.copy()

working_df["datacreationdate"] = pd.to_datetime(working_df["datacreationdate"], errors="coerce")
working_df = working_df.dropna(subset=["datacreationdate"]).sort_values("datacreationdate")

with st.spinner(t("spike_calculating")):
    val_arg = threshold_val if method == "rolling_threshold" else 1.5
    z_arg = threshold_val if method == "zscore" else 2.5

    spikes_df = cached_detect_pollution_spikes(
        working_df,
        pollutant_col=pollutant,
        method=method,
        rolling_window=rolling_window,
        threshold_ratio=val_arg,
        zscore_threshold=z_arg,
        min_value=min_val
    )
    if not spikes_df.empty:
        spikes_df["datacreationdate"] = pd.to_datetime(spikes_df["datacreationdate"])
        spikes_df["hour"] = spikes_df["datacreationdate"].dt.hour
        spikes_df["weekday"] = spikes_df["datacreationdate"].dt.weekday

if spikes_df.empty:
    st.info(t("spike_no_data"))
    st.stop()

top_site = spikes_df["sitename"].mode()[0] if not spikes_df.empty else t("na")

with tab_event:
    with event_heatmap_slot:
        st.subheader(t("hours_spike_title"))
        st.caption(t("hours_spike_caption"))

        heatmap_df = spikes_df.groupby(["weekday", "hour"]).size().reset_index(name="spike_count")
        weekday_map = {
            0: t("wd_1"), 1: t("wd_2"), 2: t("wd_3"),
            3: t("wd_4"), 4: t("wd_5"), 5: t("wd_6"), 6: t("wd_7")
        }
        heatmap_df["weekday_name"] = heatmap_df["weekday"].map(weekday_map)
        heatmap_df["hour_label"] = heatmap_df["hour"].astype(str) + ":00"

        heatmap_chart = alt.Chart(heatmap_df).mark_rect(cornerRadius=3).encode(
            x=alt.X("hour_label:N", title=t("hours_x_axis_label"), sort=[f"{i}:00" for i in range(24)]),
            y=alt.Y("weekday_name:O", title=t("hours_y_axis_weekday_label"), sort=[t(f"wd_{i}") for i in range(1, 8)]),
            color=alt.Color(
                "spike_count:Q",
                scale=alt.Scale(scheme="blues"),
                legend=alt.Legend(title=t("hours_spike_count_legend"), titleAnchor="middle"),
            ),
            tooltip=[
                alt.Tooltip("weekday_name:N", title=t("hours_tooltip_weekday")),
                alt.Tooltip("hour_label:N", title=t("hours_tooltip_hour")),
                alt.Tooltip("spike_count:Q", title=t("hours_tooltip_spike_count"))
            ]
        ).properties(
            background='transparent',
            width="container"
        ).configure_axis(
            grid=False
        ).configure_view(
            strokeOpacity=0
        )
        st.altair_chart(heatmap_chart, use_container_width=True)

    with event_series_slot:
        st.subheader(t("spike_chart_title"))

        plot_df = working_df[working_df["sitename"] == top_site] if selected_county == "__all__" else working_df
        plot_df = plot_df.sort_values("datacreationdate")

        latest_update_time = plot_df["datacreationdate"].max()
        earliest_time = plot_df["datacreationdate"].min()

        base_line = alt.Chart(plot_df).mark_line(opacity=0.4, color='#6B7280').encode(
            x=alt.X(
                "datacreationdate:T",
                title=t("date_label"),
                scale=alt.Scale(domain=[earliest_time, latest_update_time]),
            ),
            y=alt.Y(f"{pollutant}:Q", title=pollutant_display),
            detail="sitename:N",
            tooltip=["sitename", "datacreationdate", pollutant]
        )

        plot_spikes_df = spikes_df[spikes_df["sitename"] == top_site] if selected_county == "__all__" else spikes_df
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
            background='transparent',
            width="container"
        )
        st.altair_chart(combined_chart, use_container_width=True)

    with event_interp_slot:
        st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)
        st.markdown(t("spike_interpretation_text"))

with tab_summary:
    st.markdown(f"### {t('spike_detection_summary')}")
    summary_cols = st.columns(2)

    with summary_cols[0]:
        county_spike = (
            spikes_df.groupby("county")
            .size()
            .reset_index(name="spike_count")
            .sort_values("spike_count", ascending=False)
        )
        county_spike_chart = (
            alt.Chart(county_spike)
            .mark_bar(color="#2F6DA8", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                x=alt.X("county:N", sort="-y", title=t("county"), axis=alt.Axis(labelAngle=-45)),
                y=alt.Y("spike_count:Q", title=t("spike_total_count")),
                tooltip=[
                    alt.Tooltip("county:N", title=t("county")),
                    alt.Tooltip("spike_count:Q", title=t("spike_total_count")),
                ],
            )
            .properties(height=280)
            .configure_axis(grid=True, gridColor="#d8e2ec")
            .configure_view(strokeWidth=0, fill="transparent")
            .configure(background="transparent")
        )
        st.altair_chart(county_spike_chart, use_container_width=True)

    with summary_cols[1]:
        hourly_spike = (
            spikes_df.groupby("hour")
            .size()
            .reset_index(name="spike_count")
            .sort_values("hour")
        )
        hourly_spike["hour_label"] = hourly_spike["hour"].astype(int).astype(str) + ":00"

        spike_hour_chart = (
            alt.Chart(hourly_spike)
            .mark_line(strokeWidth=2.8, color="#1F5D99", point=alt.OverlayMarkDef(filled=True, size=42))
            .encode(
                x=alt.X("hour_label:N", title=t("hours_x_axis_label"), sort=[f"{h}:00" for h in range(24)]),
                y=alt.Y("spike_count:Q", title=t("hours_spike_count_legend")),
                tooltip=[
                    alt.Tooltip("hour_label:N", title=t("hours_tooltip_hour")),
                    alt.Tooltip("spike_count:Q", title=t("hours_spike_count_legend")),
                ],
            )
            .properties(height=280)
            .configure_axis(grid=True, gridColor="#d8e2ec")
            .configure_view(strokeWidth=0, fill="transparent")
            .configure(background="transparent")
        )
        st.altair_chart(spike_hour_chart, use_container_width=True)

    st.markdown(t("county_risk_spike_interpretation_text"))

    with st.expander(t("spike_sample_table_title"), expanded=False):
        st.caption(t("spike_sample_note"))
        st.dataframe(spikes_df.head(20), hide_index=True, use_container_width=True)

