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

risk_df = risk_df.sort_values("risk_score", ascending=False).reset_index(drop=True)

# Build a decomposition table for transparent risk-score explanation.
explain_df = hourly_df.copy()
explain_df["aqi"] = pd.to_numeric(explain_df["aqi"], errors="coerce")
explain_df = explain_df.dropna(subset=["aqi", "county"])

explain_df = (
    explain_df.groupby("county").agg(
        mean_aqi=("aqi", "mean"),
        std_aqi=("aqi", "std"),
        total_count=("aqi", "count"),
        high_pollution_count=("aqi", lambda x: (x > 100).sum()),
    )
).reset_index()

explain_df["std_aqi"] = explain_df["std_aqi"].fillna(0)
explain_df["high_pollution_ratio"] = (
    (explain_df["high_pollution_count"] / explain_df["total_count"]).fillna(0)
)
explain_df.loc[explain_df["total_count"] < 5, "high_pollution_ratio"] = 0.0

safe_mean = explain_df["mean_aqi"].clip(lower=30.0)
explain_df["cv_aqi"] = explain_df["std_aqi"] / safe_mean

def _normalize(series: pd.Series) -> pd.Series:
    s_min, s_max = series.min(), series.max()
    return (series - s_min) / (s_max - s_min) if s_max > s_min else series * 0.0

explain_df["mean_aqi_norm"] = _normalize(explain_df["mean_aqi"])
explain_df["cv_aqi_norm"] = _normalize(explain_df["cv_aqi"])
explain_df["base_score"] = explain_df["mean_aqi_norm"] * 0.5 + explain_df["cv_aqi_norm"] * 0.5
explain_df["raw_risk"] = explain_df["base_score"] * (1.0 + explain_df["high_pollution_ratio"])
explain_df["risk_score"] = _normalize(explain_df["raw_risk"]) * 100.0

explain_df = explain_df.sort_values("risk_score", ascending=False).reset_index(drop=True)

has_site_col = "site_name" in hourly_df.columns
station_count = hourly_df["site_name"].dropna().nunique() if has_site_col else None
county_count = hourly_df["county"].dropna().nunique() if "county" in hourly_df.columns else None
time_min = pd.to_datetime(hourly_df["datacreationdate"], errors="coerce").min()
time_max = pd.to_datetime(hourly_df["datacreationdate"], errors="coerce").max()

top_row = risk_df.iloc[0]
top_county = top_row["county"]
top_risk_score = float(top_row["risk_score"])

st.caption(f"{t('county_risk_intro')}")

tab_risk = st.container()

with tab_risk:
    # ===== 頂部資訊容器 =====
    with st.container(border=True):
        col_info, col_risk_score, col_spacer = st.columns([1.5, 1, 3])
        
        with col_info:
            st.markdown(f"**{t('highest_risk')}**")
            st.markdown(f"<div style='font-size: 1.2rem; font-weight: bold; color: #1F5D99;'>{top_county}</div>", unsafe_allow_html=True)
        
        with col_risk_score:
            st.markdown(f"**{t('risk_score')}**")
            st.markdown(f"<div style='font-size: 1.2rem; font-weight: bold; color: #333333;'>{top_risk_score:.1f}</div>", unsafe_allow_html=True)
    
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

    with st.expander(t("risk_details_title"), expanded=False):
        st.markdown(t("risk_details_intro"))

        if pd.notna(time_min) and pd.notna(time_max):
            st.caption(
                t("risk_time_range_caption").format(
                    start=time_min.strftime("%Y-%m-%d %H:%M"),
                    end=time_max.strftime("%Y-%m-%d %H:%M"),
                )
            )

        st.markdown(f"#### {t('risk_formula_table_title')}")
        st.caption(t("risk_formula_note"))
        formula_table = explain_df[
            [
                "county",
                "mean_aqi",
                "std_aqi",
                "high_pollution_ratio",
                "mean_aqi_norm",
                "cv_aqi_norm",
                "base_score",
                "raw_risk",
                "risk_score",
            ]
        ].copy()

        for col in ["mean_aqi", "std_aqi", "mean_aqi_norm", "cv_aqi_norm", "base_score", "raw_risk", "risk_score"]:
            formula_table[col] = formula_table[col].round(4)
        formula_table["high_pollution_ratio"] = formula_table["high_pollution_ratio"].round(4)

        st.dataframe(
            formula_table.rename(
                columns={
                    "county": t("county"),
                    "mean_aqi": t("county_mean_aqi"),
                    "std_aqi": t("county_std_aqi"),
                    "high_pollution_ratio": t("county_high_pol_ratio"),
                    "mean_aqi_norm": t("risk_component_mean_norm"),
                    "cv_aqi_norm": t("risk_component_cv_norm"),
                    "base_score": t("risk_component_base_score"),
                    "raw_risk": t("risk_component_raw_risk"),
                    "risk_score": t("risk_score"),
                }
            ),
            hide_index=True,
            use_container_width=True,
        )

