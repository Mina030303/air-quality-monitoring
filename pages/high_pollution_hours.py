import streamlit as st
import pandas as pd
import altair as alt
from utils import (
    apply_style,
    render_global_sidebar,
    render_back_home_button,
    load_data,
    t,
    BASE_DIR,
)

apply_style()
render_global_sidebar("pages/high_pollution_hours.py")

trend, county, hours = load_data()

try:
    ratio_df = pd.read_csv(BASE_DIR / "output/tables/high_pollution_hour_ratio.csv")
except Exception as e:
    st.error(f"Failed to load overall ratio data: {e}")
    ratio_df = pd.DataFrame()

try:
    ratio_county_df = pd.read_csv(BASE_DIR / "output/tables/high_pollution_hour_ratio_by_county.csv")
except Exception as e:
    st.error(f"Failed to load county ratio data: {e}")
    ratio_county_df = pd.DataFrame()

st.title(t("hours_risk_analysis_title"))
st.caption(t("hours_risk_analysis_desc"))

st.divider()

# ---------- KPI ----------
st.subheader(t("hours_overview_title"))

k1, k2, k3 = st.columns(3)

if not ratio_df.empty:
    ratio_df = ratio_df.copy().sort_values("hour")
    top_row = ratio_df.sort_values("high_pollution_ratio", ascending=False).iloc[0]
    highest_hour = int(top_row["hour"])
    highest_ratio = float(top_row["high_pollution_ratio"])
    avg_ratio = float(ratio_df["high_pollution_ratio"].mean())
else:
    highest_hour = None
    highest_ratio = 0.0
    avg_ratio = 0.0

k1.metric(
    t("hours_highest_risk_hour"),
    "-" if highest_hour is None else f"{highest_hour}:00"
)
k2.metric(t("hours_highest_risk_ratio"), f"{highest_ratio:.2%}")
k3.metric(t("hours_avg_hourly_risk"), f"{avg_ratio:.2%}")

st.divider()

# ---------- OVERALL RISK ----------
st.subheader(t("hours_overall_risk_title"))
st.caption(t("hours_overall_risk_caption"))

if not ratio_df.empty:
    overall_chart_df = ratio_df.copy()
    overall_chart_df["hour_label"] = overall_chart_df["hour"].astype(int).astype(str) + ":00"
    
    # 建立無背景且附帶翻譯軸標籤與提示的 Altair Bar Chart
    overall_chart = alt.Chart(overall_chart_df).mark_bar().encode(
        x=alt.X('hour_label:N', title=t("hours_x_axis_label"), sort=None),
        y=alt.Y('high_pollution_ratio:Q', title=t("hours_y_axis_ratio_label"), axis=alt.Axis(format="%")),
        tooltip=[
            alt.Tooltip("hour_label:N", title=t("hours_tooltip_hour")),
            alt.Tooltip("high_pollution_ratio:Q", title=t("hours_tooltip_ratio"), format=".2%"),
            alt.Tooltip("high_pollution_count:Q", title=t("hours_tooltip_count")),
        ]
    ).properties(
        background='transparent'
    )
    
    st.altair_chart(
        overall_chart, 
        use_container_width=True
    )
else:
    st.info(t("hours_overall_na"))

st.divider()

# ---------- COUNTY COMPARISON ----------
st.subheader(t("hours_county_risk_title"))
st.caption(t("hours_county_risk_caption"))

if not ratio_county_df.empty and "county" in ratio_county_df.columns:
    ratio_county_df = ratio_county_df.copy()
    ratio_county_df = ratio_county_df.sort_values(["county", "hour"])

    county_options = sorted(ratio_county_df["county"].dropna().unique().tolist())

    filter_col, _ = st.columns([1.0, 5.0])
    with filter_col:
        selected_county = st.selectbox(
            "",
            county_options,
            key="hours_county_select",
            label_visibility="collapsed"
        )

    county_filtered = ratio_county_df[ratio_county_df["county"] == selected_county].copy()

    if not county_filtered.empty:
        county_filtered["hour_label"] = county_filtered["hour"].astype(int).astype(str) + ":00"

        # 建立無背景且附帶翻譯與較大感應範圍的 Altair Line Chart
        line = alt.Chart(county_filtered).mark_line(point=True).encode(
            x=alt.X('hour_label:N', title=t("hours_x_axis_label"), sort=None),
            y=alt.Y('high_pollution_ratio:Q', title=t("hours_y_axis_ratio_label"), axis=alt.Axis(format="%")),
            tooltip=[
                alt.Tooltip("county:N", title=t("hours_tooltip_county")),
                alt.Tooltip("hour_label:N", title=t("hours_tooltip_hour")),
                alt.Tooltip("high_pollution_ratio:Q", title=t("hours_tooltip_ratio"), format=".2%"),
                alt.Tooltip("high_pollution_count:Q", title=t("hours_tooltip_count")),
            ]
        )

        # 增加透明且感應範圍較大的圓點供 tooltip 觸發
        selectors = alt.Chart(county_filtered).mark_point(size=300, opacity=0).encode(
            x=alt.X('hour_label:N', sort=None),
            y=alt.Y('high_pollution_ratio:Q'),
            tooltip=[
                alt.Tooltip("county:N", title=t("hours_tooltip_county")),
                alt.Tooltip("hour_label:N", title=t("hours_tooltip_hour")),
                alt.Tooltip("high_pollution_ratio:Q", title=t("hours_tooltip_ratio"), format=".2%"),
                alt.Tooltip("high_pollution_count:Q", title=t("hours_tooltip_count")),
            ]
        )

        county_chart = (line + selectors).properties(background='transparent')

        st.altair_chart(
            county_chart, 
            use_container_width=True
        )

        top_county_row = county_filtered.sort_values("high_pollution_ratio", ascending=False).iloc[0]
        st.caption(
            f"{selected_county}：{t('hours_highest_risk_hour')} "
            f"{int(top_county_row['hour'])}:00，{t('hours_highest_risk_ratio')} "
            f"{float(top_county_row['high_pollution_ratio']):.2%}"
        )
    else:
        st.info(t("hours_county_no_data"))
else:
    st.info(t("hours_county_na"))

st.divider()

# ---------- ORIGINAL COUNT ANALYSIS ----------
st.subheader(t("hours_orig_count_title"))
st.caption(t("hours_orig_count_caption"))

chart_df = hours.copy()
count_col = chart_df.columns[1]
chart_df = chart_df.rename(columns={count_col: "high_pollution_count"})
chart_df["hour_label"] = chart_df["hour"].astype(int).astype(str) + ":00"

# 建立無背景、有標籤與提示的 Altair Bar Chart
orig_chart = alt.Chart(chart_df).mark_bar().encode(
    x=alt.X('hour_label:N', title=t("hours_x_axis_label"), sort=None),
    y=alt.Y('high_pollution_count:Q', title=t("hours_y_axis_count_label")),
    tooltip=[
        alt.Tooltip("hour_label:N", title=t("hours_tooltip_hour")),
        alt.Tooltip("high_pollution_count:Q", title=t("hours_tooltip_count")),
    ]
).properties(
    background='transparent'
)

st.altair_chart(
    orig_chart,
    use_container_width=True,
)

st.divider()

# ---------- INTERPRETATION ----------
st.subheader(t("hours_interpretation_title"))
st.markdown(t("hours_interpretation_desc"))