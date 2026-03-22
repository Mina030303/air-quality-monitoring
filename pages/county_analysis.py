import streamlit as st
import altair as alt
import pandas as pd
from utils import apply_style, render_global_sidebar, load_data, t
from src.analyze_data import analyze_county_stability

apply_style()
render_global_sidebar("pages/county_analysis.py")

trend, county, hours = load_data()
hourly_df = pd.read_csv("data/processed/hourly_clean.csv", parse_dates=["datacreationdate"])
stability_df = analyze_county_stability(hourly_df).copy()

mean_sorted = stability_df.sort_values("mean_aqi", ascending=False)
std_sorted = stability_df.sort_values("std_aqi", ascending=False)
ratio_sorted = stability_df.sort_values("high_pollution_ratio", ascending=False)

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

st.caption(t("county_how_to_read_body"))

# 以平均中心為主縮放，但保證所有點都不被切掉
mean_center = stability_df["mean_aqi"].mean()
std_center = stability_df["std_aqi"].mean()

x_half_range = max(
    12,
    (stability_df["mean_aqi"] - mean_center).abs().max() + 2
)
y_half_range = max(
    6,
    (stability_df["std_aqi"] - std_center).abs().max() + 1.5
)

x_min = max(0, mean_center - x_half_range)
x_max = mean_center + x_half_range
y_min = max(0, std_center - y_half_range)
y_max = std_center + y_half_range

# 懸浮感應縮小，以圓上為主
hover = alt.selection_point(
    fields=["county"],
    on="mouseover",
    clear="mouseout",
    empty=False,
)

risk_points = (
    alt.Chart(stability_df)
    .mark_circle(opacity=0.88, stroke="white", strokeWidth=1)
    .encode(
        x=alt.X(
            "mean_aqi:Q",
            title=t("county_mean_aqi"),
            scale=alt.Scale(domain=[x_min, x_max]),
        ),
        y=alt.Y(
            "std_aqi:Q",
            title=t("county_std_aqi"),
            scale=alt.Scale(domain=[y_min, y_max]),
        ),
        size=alt.Size(
            "high_pollution_ratio:Q",
            title=t("county_high_pol_ratio"),
            scale=alt.Scale(range=[90, 850]),
        ),
        color=alt.Color(
            "high_pollution_ratio:Q",
            title=t("county_high_pol_ratio"),
            scale=alt.Scale(scheme="oranges"),
			legend=alt.Legend(
				titlePadding=12,   # 👉 標題往上推
				labelPadding=6,
				offset=10,
			),
        ),
        tooltip=[
            alt.Tooltip("county:N", title=t("county")),
            alt.Tooltip("mean_aqi:Q", title=t("county_mean_aqi"), format=".2f"),
            alt.Tooltip("std_aqi:Q", title=t("county_std_aqi"), format=".2f"),
            alt.Tooltip("high_pollution_ratio:Q", title=t("county_high_pol_ratio"), format=".2%"),
        ],
        opacity=alt.condition(hover, alt.value(1), alt.value(0.88)),
    )
    .add_params(hover)
)

# 圖上顯示英文縮寫，不顯示中文縣市名
label_map = {
    "Keelung City": "KEL",
    "New Taipei City": "NTPC",
    "Taipei City": "TPE",
    "Taoyuan City": "TYN",
    "Hsinchu County": "HSQ",
    "Hsinchu City": "HSZ",
    "Miaoli County": "MIA",
    "Taichung City": "TXG",
    "Changhua County": "CHA",
    "Nantou County": "NAN",
    "Yunlin County": "YUN",
    "Chiayi County": "CYQ",
    "Chiayi City": "CYI",
    "Tainan City": "TNN",
    "Kaohsiung City": "KHH",
    "Pingtung County": "PIF",
    "Yilan County": "ILA",
    "Hualien County": "HUA",
    "Taitung County": "TTT",
    "Penghu County": "PEN",
    "Kinmen County": "KIN",
    "Lienchiang County": "LIE",
}

label_df = stability_df.copy()
label_df["label_abbrev"] = label_df["county"].map(label_map).fillna(label_df["county"])

label_left = label_df.iloc[::2].copy()
label_right = label_df.iloc[1::2].copy()

risk_labels_left = (
    alt.Chart(label_left)
    .mark_text(dx=-20, dy=-8, fontSize=11, color="#244a68", align="right")
    .encode(
        x=alt.X("mean_aqi:Q", scale=alt.Scale(domain=[x_min, x_max])),
        y=alt.Y("std_aqi:Q", scale=alt.Scale(domain=[y_min, y_max])),
        text="label_abbrev:N",
    )
)

risk_labels_right = (
    alt.Chart(label_right)
    .mark_text(dx=20, dy=8, fontSize=11, color="#244a68", align="left")
    .encode(
        x=alt.X("mean_aqi:Q", scale=alt.Scale(domain=[x_min, x_max])),
        y=alt.Y("std_aqi:Q", scale=alt.Scale(domain=[y_min, y_max])),
        text="label_abbrev:N",
    )
)

risk_chart = (
    alt.layer(risk_points, risk_labels_left, risk_labels_right)
    .properties(height=420, padding={"right": 70, "left": 10, "top": 10, "bottom": 10})
    .interactive()
    .configure_axis(grid=True, gridColor="#d8e2ec")
    .configure_view(strokeWidth=0, fill="transparent")
    .configure(background="transparent")
)

st.altair_chart(risk_chart, use_container_width=True)

county_ref_df = pd.DataFrame(
    [
        {"county_zh": "基隆市", "county_en": "Keelung City", "county_abbrev": "KEL"},
        {"county_zh": "新北市", "county_en": "New Taipei City", "county_abbrev": "NTPC"},
        {"county_zh": "臺北市", "county_en": "Taipei City", "county_abbrev": "TPE"},
        {"county_zh": "桃園市", "county_en": "Taoyuan City", "county_abbrev": "TYN"},
        {"county_zh": "新竹縣", "county_en": "Hsinchu County", "county_abbrev": "HSQ"},
        {"county_zh": "新竹市", "county_en": "Hsinchu City", "county_abbrev": "HSZ"},
        {"county_zh": "苗栗縣", "county_en": "Miaoli County", "county_abbrev": "MIA"},
        {"county_zh": "臺中市", "county_en": "Taichung City", "county_abbrev": "TXG"},
        {"county_zh": "彰化縣", "county_en": "Changhua County", "county_abbrev": "CHA"},
        {"county_zh": "南投縣", "county_en": "Nantou County", "county_abbrev": "NAN"},
        {"county_zh": "雲林縣", "county_en": "Yunlin County", "county_abbrev": "YUN"},
        {"county_zh": "嘉義縣", "county_en": "Chiayi County", "county_abbrev": "CYQ"},
        {"county_zh": "嘉義市", "county_en": "Chiayi City", "county_abbrev": "CYI"},
        {"county_zh": "臺南市", "county_en": "Tainan City", "county_abbrev": "TNN"},
        {"county_zh": "高雄市", "county_en": "Kaohsiung City", "county_abbrev": "KHH"},
        {"county_zh": "屏東縣", "county_en": "Pingtung County", "county_abbrev": "PIF"},
        {"county_zh": "宜蘭縣", "county_en": "Yilan County", "county_abbrev": "ILA"},
        {"county_zh": "花蓮縣", "county_en": "Hualien County", "county_abbrev": "HUA"},
        {"county_zh": "臺東縣", "county_en": "Taitung County", "county_abbrev": "TTT"},
        {"county_zh": "澎湖縣", "county_en": "Penghu County", "county_abbrev": "PEN"},
        {"county_zh": "金門縣", "county_en": "Kinmen County", "county_abbrev": "KIN"},
        {"county_zh": "連江縣", "county_en": "Lienchiang County", "county_abbrev": "LIE"},
    ]
).rename(
    columns={
        "county_zh": t("county_zh"),
        "county_en": t("county_en"),
        "county_abbrev": t("county_abbrev"),
    }
)

with st.expander(t("county_name_reference_title")):
    st.dataframe(county_ref_df, use_container_width=True, hide_index=True)
    

# ===== 分類邏輯（用三個指標）=====
mean_avg = stability_df["mean_aqi"].mean()
std_avg = stability_df["std_aqi"].mean()
ratio_avg = stability_df["high_pollution_ratio"].mean()

risk_df = stability_df[
    (
        (stability_df["mean_aqi"] > mean_avg) &
        (stability_df["std_aqi"] > std_avg)
    ) |
    (
        (stability_df["mean_aqi"] > mean_avg) &
        (stability_df["high_pollution_ratio"] > ratio_avg)
    ) |
    (
        (stability_df["std_aqi"] > std_avg) &
        (stability_df["high_pollution_ratio"] > ratio_avg)
    )
]

good_df = stability_df[
    (stability_df["mean_aqi"] < mean_avg) &
    (stability_df["std_aqi"] < std_avg) &
    (stability_df["high_pollution_ratio"] < ratio_avg)
]

risk_list = "、".join(risk_df["county"].head(5).tolist())
good_list = "、".join(good_df["county"].head(5).tolist())

risk_text = risk_list if risk_list else t("no_data")
good_text = good_list if good_list else t("no_data")

st.markdown(
    f'<div style="font-size: 1.05rem; font-weight: 600; margin-top: 12px;">{t("county_overall_insight_title")}</div>',
    unsafe_allow_html=True,
)

st.markdown(
    f'<div style="font-size: 0.98rem; line-height: 1.8; color:#3a4860;">{t("county_overall_insight_body")}</div>',
    unsafe_allow_html=True,
)

st.markdown(
    f'<div style="font-size: 0.98rem; margin-top:10px; color:#3a4860;"><b>{t("county_high_risk_label")}</b>：{risk_text}</div>',
    unsafe_allow_html=True,
)

st.markdown(
    f'<div style="font-size: 0.98rem; margin-top:4px; color:#3a4860;"><b>{t("county_good_label")}</b>：{good_text}</div>',
    unsafe_allow_html=True,
)

st.markdown("---")

base_axis = alt.Axis(
    labelAngle=-60,
    labelPadding=8,
    titlePadding=10,
)

st.markdown(f"**{t('county_chart_mean_title')}**")
st.caption(t("county_chart_mean_desc"))

mean_sorted = mean_sorted.copy()
mean_sorted["aqi_band"] = pd.cut(
    mean_sorted["mean_aqi"],
    bins=[0, 50, 100, 120],
    labels=["Good", "Moderate", "Polluted"],
    include_lowest=True,
)

mean_chart = (
    alt.Chart(mean_sorted)
    .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
    .encode(
        x=alt.X("county:N", sort=None, title=t("county"), axis=base_axis),
        y=alt.Y(
            "mean_aqi:Q",
            title=t("county_mean_aqi"),
            scale=alt.Scale(domain=[0, 120]),
        ),
        color=alt.Color(
            "aqi_band:N",
            title=t("aqi_value_label"),
            scale=alt.Scale(
                domain=["Good", "Moderate", "Polluted"],
                range=["#6fbf73", "#e6d36f", "#f4a259"],
            ),
            legend=alt.Legend(
                orient="right",
                symbolType="square",
                titlePadding=10,
                labelPadding=6,
                offset=12,
            ),
        ),
        tooltip=[
            alt.Tooltip("county:N", title=t("county")),
            alt.Tooltip("mean_aqi:Q", title=t("county_mean_aqi"), format=".2f"),
            alt.Tooltip("mean_rank:Q", title=t("county_mean_rank")),
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
            alt.Tooltip("volatility_rank:Q", title=t("county_std_rank")),
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
    .properties(height=280)
    .configure_axis(grid=True, gridColor="#d8e2ec")
    .configure_view(strokeWidth=0, fill="transparent")
    .configure(background="transparent")
)
st.altair_chart(ratio_chart, use_container_width=True)