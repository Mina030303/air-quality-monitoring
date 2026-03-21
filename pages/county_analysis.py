import streamlit as st
import altair as alt
from utils import apply_style, render_global_sidebar, render_back_home_button, render_aqi_meaning_block, load_data, t

apply_style()
render_global_sidebar("pages/county_analysis.py")

trend, county, hours = load_data()

st.title(t("county_chart_title"))
render_back_home_button()

chart_df = county.copy()
aqi_col = chart_df.columns[1]
chart_df = chart_df.rename(columns={aqi_col: "avg_aqi"})

county_chart = (
	alt.Chart(chart_df)
	.mark_bar()
	.encode(
		x=alt.X("county:N", sort="-y", title=t("county")),
		y=alt.Y("avg_aqi:Q", title=t("county_avg_aqi")),
		color=alt.Color(
			"avg_aqi:Q",
			title=t("aqi_value_label"),
			scale=alt.Scale(
				domain=[0, 50, 100, 101, 300],
				range=["#2e7d32", "#2e7d32", "#e6b800", "#d64545", "#d64545"],
			),
		),
		tooltip=[
			alt.Tooltip("county:N", title=t("county")),
			alt.Tooltip("avg_aqi:Q", title=t("county_avg_aqi"), format=".2f"),
		],
	)
	.properties(height=380)
)

st.altair_chart(county_chart, use_container_width=True)
render_aqi_meaning_block()