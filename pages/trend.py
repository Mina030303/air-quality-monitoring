import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils import apply_style, render_global_sidebar, render_aqi_meaning_block, load_data, t
from src.analyze_data import current_status_interpretation

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

status_key = current_status_interpretation(
    chart_df[["date", "avg_aqi", "rolling_7d_avg"]]
)

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

daily_color = "#245E9B"
rolling_color = "#7FAFDE"

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

daily_color = "#245E9B"
rolling_color = "#7FAFDE"

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

# 先把兩條線資料整理成 long format，方便做 hover
hover_df = pd.concat(
    [
        chart_df[["date", "avg_aqi"]].rename(columns={"avg_aqi": "value"}).assign(series=t("daily_legend")),
        chart_df[["date", "rolling_7d_avg"]].rename(columns={"rolling_7d_avg": "value"}).assign(series=t("rolling_legend")),
    ],
    ignore_index=True,
)

hover_df["value"] = hover_df["value"].round(2)

# 只在靠近資料點時觸發，不要點背景就跳 tooltip
nearest = alt.selection_point(
    name="nearest",
    fields=["date"],
    nearest=True,
    on="pointermove",
    clear="pointerout",
    empty=False,
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
    )
)

rolling_line = (
    alt.Chart(chart_df)
    .mark_line(strokeWidth=3.2, color=rolling_color)
    .encode(
        x=x_encoding,
        y=alt.Y(
            "rolling_7d_avg:Q",
            scale=alt.Scale(domain=[0, y_max]),
        ),
    )
)

# 放大感應範圍：用透明點接 hover
hover_points = (
    alt.Chart(hover_df)
    .mark_point(size=220, opacity=0)
    .encode(
        x=alt.X("date:T"),
        y=alt.Y("value:Q"),
        color=alt.Color(
            "series:N",
            scale=alt.Scale(
                domain=[t("daily_legend"), t("rolling_legend")],
                range=[daily_color, rolling_color],
            ),
            legend=None,
        ),
    )
    .add_params(nearest)
)

# 被 hover 到時顯示的小點
visible_points = (
    alt.Chart(hover_df)
    .mark_point(size=30, filled=True, color="#1E4F86")
    .encode(
        x="date:T",
        y="value:Q",
        opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
    )
)

# tooltip 只跟著 selected point，不跟背景
tooltip_points = (
    alt.Chart(hover_df)
    .transform_filter(nearest)
    .transform_pivot(
        "series",
        value="value",
        groupby=["date"]
    )
    .mark_point(opacity=0)
    .encode(
        x="date:T",
        y=alt.Y(f"{t('daily_legend')}:Q"),
        tooltip=[
            alt.Tooltip("date:T", format="%Y-%m-%d", title=t("date_label")),
            alt.Tooltip(f"{t('daily_legend')}:Q", format=".2f", title=t("daily_legend")),
            alt.Tooltip(f"{t('rolling_legend')}:Q", format=".2f", title=t("rolling_legend")),
        ],
    )
)

main_chart = alt.layer(
    band_chart,
    daily_line,
    rolling_line,
    hover_points,
    visible_points,
    tooltip_points,
).properties(
    height=420,
    width="container",
)

legend_df = pd.DataFrame(
    [
        {"x1": 0.08, "x2": 0.30, "x_text": 0.34, "y": 0.72, "label": t("daily_legend"), "color": daily_color},
        {"x1": 0.08, "x2": 0.30, "x_text": 0.34, "y": 0.42, "label": t("rolling_legend"), "color": rolling_color},
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
    dx=0,
    fontSize=13,
    fontWeight="bold",
).encode(
    x=alt.X("x_text:Q", axis=None, scale=alt.Scale(domain=[0, 1])),
    y=alt.Y("y:Q", axis=None, scale=alt.Scale(domain=[0, 1])),
    text="label:N",
    color=alt.Color("color:N", scale=None, legend=None),
)

legend_chart = alt.layer(
    legend_lines,
    legend_text,
).properties(height=110, width="container")

main_chart = (
    main_chart
    .configure_axis(gridColor="#cfd9e2")
    .configure_view(
        strokeWidth=0,
        fill="transparent",
    )
    .configure(
        background="transparent",
    )
)

legend_chart = (
    legend_chart
    .configure_view(
        strokeWidth=0,
        fill="transparent",
    )
    .configure(
        background="transparent",
    )
)

chart_col, legend_col = st.columns([6, 1], vertical_alignment="top")

with chart_col:
    st.altair_chart(main_chart, use_container_width=True)

with legend_col:
    st.altair_chart(legend_chart, use_container_width=True)

render_aqi_meaning_block()

st.markdown("---")

st.markdown(
    f"""
        <div style="
            font-size: 1.05rem;
            font-weight: 700;
            color: #25324a;
            margin-bottom: 6px;
        ">
        </div>
        <div style="
            font-size: 1.05rem;
            font-weight: 700;
            color: #25324a;
            margin-bottom: 15px;
        ">
            {t("current_status_title")}
        </div>
        <div style="
            font-size: 0.98rem;
            color: #3a4860;
            line-height: 1.7;
            font-weight: 400;
        ">
            {t(status_key) if status_key else t("current_status_placeholder")}
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("---")

st.markdown(
    f"""
**{t("trend_interpretation_title")}**

{t("trend_interpretation_body")}
""",
    unsafe_allow_html=True
)