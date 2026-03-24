import streamlit as st
import altair as alt
import pandas as pd
from utils import apply_style, render_global_sidebar, load_data, t

apply_style()
render_global_sidebar("pages/county_risk.py")

st.title(t("county_risk_title"))

try:
    risk_df = pd.read_csv("output/tables/county_risk_score.csv")
except Exception as e:
    st.warning("County Risk table is not found. Please run main.py data pipeline first.")
    st.stop()

st.markdown(
    f"""
    <div style="
        margin-top: 6px;
        margin-bottom: 18px;
        padding: 14px 16px;
        background: rgba(126, 166, 224, 0.10);
        border: 1px solid rgba(126, 166, 224, 0.20);
        border-radius: 12px;
        color: #37506b;
        line-height: 1.75;
        font-size: 0.98rem;
    ">
        {t("county_risk_intro")}
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(f"### {t('risk_score_chart_title')}")
st.caption(t("risk_score_methodology"))

base_axis = alt.Axis(labelAngle=-60, labelPadding=8, titlePadding=10)

risk_chart = (
    alt.Chart(risk_df)
    .mark_bar(color="#e76f51", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
    .encode(
        x=alt.X("county:N", sort=alt.SortField("risk_score", order="descending"), title=t("county"), axis=base_axis),
        y=alt.Y("risk_score:Q", title=t("risk_score"), scale=alt.Scale(domain=[0, 100])),
        tooltip=[
            alt.Tooltip("county:N", title=t("county")),
            alt.Tooltip("risk_score:Q", title=t("risk_score"), format=".1f"),
            alt.Tooltip("risk_rank:Q", title=t("risk_rank")),
            alt.Tooltip("mean_aqi:Q", title="Mean AQI", format=".1f"),
            alt.Tooltip("std_aqi:Q", title="Std AQI", format=".1f"),
            alt.Tooltip("high_pollution_ratio:Q", title="High Pollution Ratio", format=".1f"),
        ],
    )
    .properties(height=380)
    .configure_axis(grid=True, gridColor="#d8e2ec")
    .configure_view(strokeWidth=0, fill="transparent")
    .configure(background="transparent")
)

st.altair_chart(risk_chart, use_container_width=True)

st.markdown("---")

st.markdown(f"### {t('top_risky_counties_title')}")
top_5 = risk_df.head(5)[["county", "risk_rank", "risk_score"]].copy()
top_5["risk_score"] = top_5["risk_score"].round(1)

st.dataframe(
    top_5.rename(columns={
        "county": t("county"),
        "risk_rank": t("risk_rank"),
        "risk_score": t("risk_score")
    }),
    hide_index=True,
    use_container_width=True
)

st.markdown("---")

with st.expander(t("view_individual_metrics")):
    mean_sorted = risk_df.sort_values("mean_aqi", ascending=False)
    std_sorted = risk_df.sort_values("std_aqi", ascending=False)
    ratio_sorted = risk_df.sort_values("high_pollution_ratio", ascending=False)

    mean_sorted["aqi_band"] = pd.cut(
        mean_sorted["mean_aqi"],
        bins=[0, 50, 100, 120],
        labels=["Good", "Moderate", "Polluted"],
        include_lowest=True,
    )

    st.markdown(f"**{t('county_chart_mean_title')}**")
    st.caption(t("county_chart_mean_desc"))
    mean_chart = (
        alt.Chart(mean_sorted)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("county:N", sort=None, title=t("county"), axis=base_axis),
            y=alt.Y("mean_aqi:Q", title=t("county_mean_aqi"), scale=alt.Scale(domain=[0, 120])),
            color=alt.Color(
                "aqi_band:N",
                title=t("aqi_value_label"),
                scale=alt.Scale(
                    domain=["Good", "Moderate", "Polluted"],
                    range=["#6fbf73", "#e6d36f", "#f4a259"],
                ),
                legend=alt.Legend(orient="right", symbolType="square", titlePadding=10, labelPadding=6, offset=12),
            ),
            tooltip=[
                alt.Tooltip("county:N", title=t("county")),
                alt.Tooltip("mean_aqi:Q", title=t("county_mean_aqi"), format=".2f"),
            ],
        )
        .properties(height=280)
        .configure_axis(grid=True, gridColor="#d8e2ec")
        .configure_view(strokeWidth=0, fill="transparent")
        .configure(background="transparent")
    )
    st.altair_chart(mean_chart, use_container_width=True)

    st.markdown("---")

    st.markdown(f"**{t('county_chart_std_title')}**")
    st.caption(t("county_chart_std_desc"))
    std_chart = (
        alt.Chart(std_sorted)
        .mark_bar(color="#5a8dee", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("county:N", sort=None, title=t("county"), axis=base_axis),
            y=alt.Y("std_aqi:Q", title=t("county_std_aqi")),
            tooltip=[
                alt.Tooltip("county:N", title=t("county")),
                alt.Tooltip("std_aqi:Q", title=t("county_std_aqi"), format=".2f"),
            ],
        )
        .properties(height=280)
        .configure_axis(grid=True, gridColor="#d8e2ec")
        .configure_view(strokeWidth=0, fill="transparent")
        .configure(background="transparent")
    )
    st.altair_chart(std_chart, use_container_width=True)

    st.markdown("---")

    st.markdown(f"**{t('county_chart_ratio_title')}**")
    st.caption(t("county_chart_ratio_desc"))
    ratio_chart = (
        alt.Chart(ratio_sorted)
        .mark_bar(color="#ff9f43", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("county:N", sort=None, title=t("county"), axis=base_axis),
            y=alt.Y("high_pollution_ratio:Q", title=t("county_high_pol_ratio"), axis=alt.Axis(format="%")),
            tooltip=[
                alt.Tooltip("county:N", title=t("county")),
                alt.Tooltip("high_pollution_ratio:Q", title=t("county_high_pol_ratio"), format=".2%"),
            ],
        )
        .properties(height=280)
        .configure_axis(grid=True, gridColor="#d8e2ec")
        .configure_view(strokeWidth=0, fill="transparent")
        .configure(background="transparent")
    )
    st.altair_chart(ratio_chart, use_container_width=True)
