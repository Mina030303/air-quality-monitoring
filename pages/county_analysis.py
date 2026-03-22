import streamlit as st
import altair as alt
import pandas as pd
from utils import apply_style, render_global_sidebar, render_back_home_button, load_data, t
from src.analyze_data import analyze_county_stability

apply_style()
render_global_sidebar("pages/county_analysis.py")

trend, county, hours = load_data()
hourly_df = pd.read_csv("data/processed/hourly_clean.csv", parse_dates=["datacreationdate"])
stability_df = analyze_county_stability(hourly_df).copy()

stability_df["high_pollution_ratio_pct"] = stability_df["high_pollution_ratio"] * 100

mean_sorted = stability_df.sort_values("mean_aqi", ascending=False)
std_sorted = stability_df.sort_values("std_aqi", ascending=False)
ratio_sorted = stability_df.sort_values("high_pollution_ratio", ascending=False)

top_mean = mean_sorted.iloc[0]
top_std = std_sorted.iloc[0]
top_ratio = ratio_sorted.iloc[0]

st.title(t("county_stability_title"))

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
        {t("county_page_intro")}
    </div>
    """,
    unsafe_allow_html=True,
)

c1, c2, c3 = st.columns(3)

with c1:
    st.markdown(
        f"""
        <div class="custom-card" style="min-height: 132px;">
            <div class="card-title">{t("county_top_mean_title")}</div>
            <div style="font-size: 1.25rem; font-weight: 700; color: #244a68; margin-bottom: 6px;">
                {top_mean["county"]}
            </div>
            <div class="card-text">
                {t("county_mean_aqi")}：{top_mean["mean_aqi"]:.2f}<br>
                {t("county_top_mean_desc")}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with c2:
    st.markdown(
        f"""
        <div class="custom-card" style="min-height: 132px;">
            <div class="card-title">{t("county_top_std_title")}</div>
            <div style="font-size: 1.25rem; font-weight: 700; color: #244a68; margin-bottom: 6px;">
                {top_std["county"]}
            </div>
            <div class="card-text">
                {t("county_std_aqi")}：{top_std["std_aqi"]:.2f}<br>
                {t("county_top_std_desc")}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with c3:
    st.markdown(
        f"""
        <div class="custom-card" style="min-height: 132px;">
            <div class="card-title">{t("county_top_ratio_title")}</div>
            <div style="font-size: 1.25rem; font-weight: 700; color: #244a68; margin-bottom: 6px;">
                {top_ratio["county"]}
            </div>
            <div class="card-text">
                {t("county_high_pol_ratio")}：{top_ratio["high_pollution_ratio"]:.2%}<br>
                {t("county_top_ratio_desc")}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

base_axis = alt.Axis(
    labelAngle=-60,
    labelPadding=8,
    titlePadding=10,
)

with col1:
    st.markdown(f"**{t('county_chart_mean_title')}**")
    st.caption(t("county_chart_mean_desc"))

    mean_chart = (
        alt.Chart(mean_sorted)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("county:N", sort=None, title=t("county"), axis=base_axis),
            y=alt.Y("mean_aqi:Q", title=t("county_mean_aqi")),
            color=alt.Color(
                "mean_aqi:Q",
                title=t("aqi_value_label"),
                scale=alt.Scale(
                    domain=[0, 50, 100, 101, 300],
                    range=["#2e7d32", "#2e7d32", "#e6b800", "#d64545", "#d64545"],
                ),
            ),
            tooltip=[
                alt.Tooltip("county:N", title=t("county")),
                alt.Tooltip("mean_aqi:Q", title=t("county_mean_aqi"), format=".2f"),
                alt.Tooltip("mean_rank:Q", title=t("county_mean_rank")),
            ],
        )
        .properties(height=320)
    )
    st.altair_chart(mean_chart, use_container_width=True)

with col2:
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
                alt.Tooltip("volatility_rank:Q", title=t("county_std_rank")),
            ],
        )
        .properties(height=320)
    )
    st.altair_chart(std_chart, use_container_width=True)

with col3:
    st.markdown(f"**{t('county_chart_ratio_title')}**")
    st.caption(t("county_chart_ratio_desc"))

    ratio_chart = (
        alt.Chart(ratio_sorted)
        .mark_bar(color="#ff9f43", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("county:N", sort=None, title=t("county"), axis=base_axis),
            y=alt.Y(
                "high_pollution_ratio:Q",
                title=t("county_high_pol_ratio"),
                axis=alt.Axis(format="%"),
            ),
            tooltip=[
                alt.Tooltip("county:N", title=t("county")),
                alt.Tooltip("high_pollution_ratio:Q", title=t("county_high_pol_ratio"), format=".2%"),
                alt.Tooltip("high_pollution_ratio_rank:Q", title=t("county_ratio_rank")),
            ],
        )
        .properties(height=320)
    )
    st.altair_chart(ratio_chart, use_container_width=True)

st.markdown("---")

st.markdown(f"**{t('county_table_title')}**")
st.caption(t("county_table_desc"))

display_df = stability_df[
    [
        "county",
        "mean_aqi",
        "std_aqi",
        "high_pollution_ratio",
        "mean_rank",
        "volatility_rank",
        "high_pollution_ratio_rank",
    ]
].copy()

display_df = display_df.rename(
    columns={
        "county": t("county"),
        "mean_aqi": t("county_mean_aqi"),
        "std_aqi": t("county_std_aqi"),
        "high_pollution_ratio": t("county_high_pol_ratio"),
        "mean_rank": t("county_mean_rank"),
        "volatility_rank": t("county_std_rank"),
        "high_pollution_ratio_rank": t("county_ratio_rank"),
    }
)

st.dataframe(
    display_df.style.format(
        {
            t("county_mean_aqi"): "{:.2f}",
            t("county_std_aqi"): "{:.2f}",
            t("county_high_pol_ratio"): "{:.2%}",
        }
    ),
    use_container_width=True,
)

st.markdown("---")

st.markdown(
    f"""
    <div style="margin-top: 12px;">
        <div style="
            font-size: 1.05rem;
            font-weight: 700;
            color: #25324a;
            margin-bottom: 8px;
        ">
            {t("county_how_to_read_title")}
        </div>
        <div style="
            font-size: 0.98rem;
            color: #3a4860;
            line-height: 1.8;
            font-weight: 400;
        ">
            {t("county_how_to_read_body")}
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)