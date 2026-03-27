import streamlit as st
import altair as alt
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils import (
    apply_style,
    render_global_sidebar,
    load_raw_data,
    cached_calculate_county_risk_score,
    cached_detect_pollution_spikes,
    t,
)

apply_style()
render_global_sidebar("pages/county_risk.py")

st.markdown(
    """
    <style>
    @media (max-width: 768px) {
      div[data-testid="stMetric"] label {
        font-size: 0.8rem !important;
      }
      div[data-testid="stMetricValue"] {
        font-size: 1.1rem !important;
      }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title(t("county_risk_title"))

with st.spinner(t("analyzing_data")):
    hourly_df = load_raw_data()
    if hourly_df.empty:
        st.warning(t("county_risk_table_not_found"))
        st.stop()

    risk_df = cached_calculate_county_risk_score(hourly_df)
    spikes_df = cached_detect_pollution_spikes(
        hourly_df,
        pollutant_col="aqi",
        method="rolling_threshold",
        rolling_window=24,
        threshold_ratio=1.5,
        zscore_threshold=2.5,
        min_value=50.0,
    )

risk_df = risk_df.sort_values("risk_score", ascending=False).reset_index(drop=True)

top_row = risk_df.iloc[0]
top_county = top_row["county"]
top_risk_score = float(top_row["risk_score"])
spike_count = int(len(spikes_df))

st.caption(f"{t('county_risk_intro')}")

tab_risk, tab_spike, tab_trend = st.tabs([t("risk_ranking_tab"), t("spike_detection_tab"), t("trend_analysis_tab")])

with tab_risk:
    # ===== 頂部資訊容器 =====
    with st.container(border=True):
        col_info, col_risk_score, col_spike, col_spacer = st.columns([1.5, 1, 1, 2])
        
        with col_info:
            st.markdown(f"**{t('highest_risk')}**")
            st.markdown(f"<div style='font-size: 1.2rem; font-weight: bold; color: #1F5D99;'>{top_county}</div>", unsafe_allow_html=True)
        
        with col_risk_score:
            st.markdown(f"**{t('risk_score')}**")
            st.markdown(f"<div style='font-size: 1.2rem; font-weight: bold; color: #333333;'>{top_risk_score:.1f}</div>", unsafe_allow_html=True)
        
        with col_spike:
            st.markdown(f"**{t('total_anomalies')}**")
            st.markdown(f"<div style='font-size: 1.2rem; font-weight: bold; color: #333333;'>{spike_count}</div>", unsafe_allow_html=True)
    
    st.markdown(f"### {t('risk_score_chart_title')}")

    axis_cfg = alt.Axis(labelAngle=-45, labelPadding=6, titlePadding=8)
    risk_chart = (
        alt.Chart(risk_df)
        .mark_bar(color="#1F5D99", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X(
                "county:N",
                sort=alt.SortField("risk_score", order="descending"),
                title=t("county"),
                axis=axis_cfg,
            ),
            y=alt.Y("risk_score:Q", title=t("risk_score"), scale=alt.Scale(domain=[0, 100])),
            tooltip=[
                alt.Tooltip("county:N", title=t("county")),
                alt.Tooltip("risk_score:Q", title=t("risk_score"), format=".1f"),
                alt.Tooltip("risk_rank:Q", title=t("risk_rank")),
                alt.Tooltip("mean_aqi:Q", title=t("county_mean_aqi"), format=".1f"),
                alt.Tooltip("std_aqi:Q", title=t("county_std_aqi"), format=".1f"),
                alt.Tooltip("high_pollution_ratio:Q", title=t("county_high_pol_ratio"), format=".1%"),
            ],
        )
        .properties(width="container")
        .configure_axis(grid=True, gridColor="#d8e2ec")
        .configure_view(strokeWidth=0, fill="transparent")
        .configure(background="transparent")
    )
    st.altair_chart(risk_chart, use_container_width=True)

    st.markdown(f"#### {t('risk_score_table_title')}")
    table_cols = [
        "county",
        "risk_rank",
        "risk_score",
        "mean_aqi",
        "high_pollution_ratio",
    ]
    risk_table = risk_df[table_cols].copy()
    risk_table["risk_score"] = risk_table["risk_score"].round(1)
    risk_table["mean_aqi"] = risk_table["mean_aqi"].round(1)
    risk_table["high_pollution_ratio"] = risk_table["high_pollution_ratio"].round(4)

    st.dataframe(
        risk_table.rename(
            columns={
                "county": t("county"),
                "risk_rank": t("risk_rank"),
                "risk_score": t("risk_score"),
                "mean_aqi": t("county_mean_aqi"),
                "high_pollution_ratio": t("county_high_pol_ratio"),
            }
        ),
        hide_index=True,
        use_container_width=True,
    )

    with st.expander(t("view_details"), expanded=False):
        st.caption(t("risk_score_methodology"))
        st.dataframe(hourly_df.head(20), use_container_width=True, hide_index=True)

with tab_spike:
    st.markdown(f"### {t('spike_detection_summary')}")

    if spikes_df.empty:
        st.info(t("spike_no_data"))
    else:
        spikes_work = spikes_df.copy()
        spikes_work["datacreationdate"] = pd.to_datetime(spikes_work["datacreationdate"], errors="coerce")
        spikes_work = spikes_work.dropna(subset=["datacreationdate"])
        spikes_work["hour"] = spikes_work["datacreationdate"].dt.hour

        county_spike = (
            spikes_work.groupby("county")
            .size()
            .reset_index(name="spike_count")
            .sort_values("spike_count", ascending=False)
        )

        spike_chart = (
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
            .properties(width="container")
            .configure_axis(grid=True, gridColor="#d8e2ec")
            .configure_view(strokeWidth=0, fill="transparent")
            .configure(background="transparent")
        )
        st.altair_chart(spike_chart, use_container_width=True)

        hourly_spike = (
            spikes_work.groupby("hour")
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
            .properties(width="container")
            .configure_axis(grid=True, gridColor="#d8e2ec")
            .configure_view(strokeWidth=0, fill="transparent")
            .configure(background="transparent")
        )
        st.altair_chart(spike_hour_chart, use_container_width=True)

    with st.expander(t("view_details"), expanded=False):
        st.markdown(t("spike_interpretation_text"))
        if not spikes_df.empty:
            st.dataframe(spikes_df.head(20), hide_index=True, use_container_width=True)

with tab_trend:
    st.markdown(f"### {t('trend_analysis')}")

    trend_df = hourly_df.copy()
    trend_df["datacreationdate"] = pd.to_datetime(trend_df["datacreationdate"], errors="coerce")
    trend_df["aqi"] = pd.to_numeric(trend_df["aqi"], errors="coerce")
    trend_df = trend_df.dropna(subset=["datacreationdate", "aqi"])
    trend_df["date"] = trend_df["datacreationdate"].dt.date

    daily_df = (
        trend_df.groupby("date", as_index=False)["aqi"]
        .mean()
        .rename(columns={"aqi": "avg_aqi"})
        .sort_values("date")
    )
    daily_df["date"] = pd.to_datetime(daily_df["date"])
    daily_df["rolling_7d"] = daily_df["avg_aqi"].rolling(window=7, min_periods=1).mean()

    long_df = pd.concat(
        [
            daily_df[["date", "avg_aqi"]].rename(columns={"avg_aqi": "value"}).assign(series=t("daily_legend")),
            daily_df[["date", "rolling_7d"]].rename(columns={"rolling_7d": "value"}).assign(series=t("rolling_legend")),
        ],
        ignore_index=True,
    )

    trend_chart = (
        alt.Chart(long_df)
        .mark_line(strokeWidth=2.8)
        .encode(
            x=alt.X("date:T", title=t("date_label")),
            y=alt.Y("value:Q", title=t("aqi_value_label")),
            color=alt.Color(
                "series:N",
                scale=alt.Scale(domain=[t("daily_legend"), t("rolling_legend")], range=["#245E9B", "#7FAFDE"]),
                legend=alt.Legend(orient="top", title=None),
            ),
            tooltip=[
                alt.Tooltip("date:T", title=t("date_label"), format="%Y-%m-%d"),
                alt.Tooltip("series:N", title=t("legend_title")),
                alt.Tooltip("value:Q", title=t("aqi_value_label"), format=".2f"),
            ],
        )
        .properties(width="container")
        .configure_axis(grid=True, gridColor="#d8e2ec")
        .configure_view(strokeWidth=0, fill="transparent")
        .configure(background="transparent")
    )

    st.altair_chart(trend_chart, use_container_width=True)

    with st.expander(t("view_details"), expanded=False):
        st.markdown(t("trend_interpretation_body"), unsafe_allow_html=True)
        st.dataframe(daily_df.tail(20), hide_index=True, use_container_width=True)
