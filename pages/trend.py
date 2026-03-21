import streamlit as st
import pandas as pd
import altair as alt
from utils import apply_style, render_global_sidebar, render_aqi_meaning_block, load_data, t

apply_style()
render_global_sidebar("pages/trend.py")

trend, county, hours = load_data()

st.markdown(
    f"""
    <div style="margin-left: 0px; margin-top: 0px; margin-bottom: 8px;">
        <h1 style="
            margin: 0;
            font-size: 3.2rem;
            font-weight: 800;
            color: #25324a;
            text-align: left;
        ">
            {t("trend")}
        </h1>
    </div>
    """,
    unsafe_allow_html=True,
)

chart_df = trend.copy()
chart_df["date"] = pd.to_datetime(chart_df["date"], errors="coerce")
chart_df = chart_df.dropna(subset=["date", "avg_aqi"]).sort_values("date")

if "rolling_7d_avg" not in chart_df.columns:
    chart_df["rolling_7d_avg"] = (
        chart_df["avg_aqi"].rolling(window=7, min_periods=1).mean()
    )

if chart_df.empty:
    st.warning(t("no_trend_data"))
    st.stop()

min_date = chart_df["date"].min()
max_date = chart_df["date"].max()
x_max = max_date

y_max = 160.0

band_df = pd.DataFrame(
    [
        {"start": min_date, "end": x_max, "y0": 0, "y1": 50, "band": t("aqi_good_label")},
        {"start": min_date, "end": x_max, "y0": 50, "y1": 100, "band": t("aqi_moderate_label")},
        {"start": min_date, "end": x_max, "y0": 100, "y1": y_max, "band": t("aqi_polluted_label")},
    ]
)

band_chart = (
    alt.Chart(band_df)
    .mark_rect(opacity=0.35)
    .encode(
        x="start:T",
        x2="end:T",
        y="y0:Q",
        y2="y1:Q",
        color=alt.Color(
            "band:N",
            scale=alt.Scale(
                domain=[
                    t("aqi_good_label"),
                    t("aqi_moderate_label"),
                    t("aqi_polluted_label"),
                ],
                range=["#d6eadf", "#f2e7b8", "#eed9df"],
            ),
            legend=None,
        ),
    )
)

daily_color = "#2f5fa5"
rolling_color = "#8fb3e8"

x_encoding = alt.X(
    "date:T",
    title=t("date_label"),
    scale=alt.Scale(domain=[min_date, x_max]),
    axis=alt.Axis(
        format="%m/%d",
        values=chart_df["date"].tolist(),
        labelAngle=-35,
        labelPadding=8,
    ),
)

daily_line = (
    alt.Chart(chart_df)
    .mark_line(strokeWidth=3.2, color=daily_color)
    .encode(
        x=x_encoding,
        y=alt.Y(
            "avg_aqi:Q",
            title=t("aqi_value_label"),
            scale=alt.Scale(domain=[0, y_max]),
        ),
        tooltip=[
            alt.Tooltip("date:T", format="%Y-%m-%d", title=t("date_label")),
            alt.Tooltip("avg_aqi:Q", format=".2f", title=t("daily_legend")),
        ],
    )
)

rolling_line = (
    alt.Chart(chart_df)
    .mark_line(strokeWidth=3.2, color=rolling_color)
    .encode(
        x=x_encoding,
        y=alt.Y("rolling_7d_avg:Q", scale=alt.Scale(domain=[0, y_max])),
        tooltip=[
            alt.Tooltip("date:T", format="%Y-%m-%d", title=t("date_label")),
            alt.Tooltip("rolling_7d_avg:Q", format=".2f", title=t("rolling_legend")),
        ],
    )
)

main_chart = alt.layer(
    band_chart,
    daily_line,
    rolling_line,
).properties(
    height=420,
    width=860,
)

legend_df = pd.DataFrame(
    [
        {"x1": 0.00, "x2": 0.14, "y": 0.88, "label": t("daily_legend"), "color": daily_color},
        {"x1": 0.00, "x2": 0.14, "y": 0.80, "label": t("rolling_legend"), "color": rolling_color},
    ]
)

legend_lines = alt.Chart(legend_df).mark_rule(strokeWidth=2.6).encode(
    x=alt.X("x1:Q", axis=None, scale=alt.Scale(domain=[0, 1])),
    x2="x2:Q",
    y=alt.Y("y:Q", axis=None, scale=alt.Scale(domain=[0, 1])),
    color=alt.Color("color:N", scale=None, legend=None),
)

legend_text = alt.Chart(legend_df).mark_text(
    align="left",
    dx=6,
    fontSize=13,
    fontWeight="bold",
).encode(
    x=alt.X("x2:Q", axis=None, scale=alt.Scale(domain=[0, 1])),
    y=alt.Y("y:Q", axis=None, scale=alt.Scale(domain=[0, 1])),
    text="label:N",
    color=alt.Color("color:N", scale=None, legend=None),
)

legend_chart = alt.layer(
    legend_lines,
    legend_text,
).properties(height=420, width=135)

combined_chart = (
    alt.hconcat(main_chart, legend_chart, spacing=14)
    .resolve_scale(color="independent")
    .configure_axis(gridColor="#cfd9e2")
    .configure_view(
        strokeWidth=0,
        fill="transparent",
    )
    .configure(background="transparent")
)

st.altair_chart(combined_chart, use_container_width=True)

render_aqi_meaning_block()

st.markdown("---")

st.markdown(
    f"""
**{t("trend_interpretation_title")}**

{t("trend_interpretation_body")}
"""
)