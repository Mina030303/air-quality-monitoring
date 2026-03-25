import streamlit as st
import altair as alt
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils import apply_style, render_global_sidebar, load_data, load_raw_data, cached_analyze_county_stability, t

apply_style()
render_global_sidebar("pages/county_analysis.py")

trend, county, hours = load_data()

with st.spinner(t('loading_analysis')):
    hourly_df = load_raw_data()
    if hourly_df.empty:
        st.warning(t("data_not_found"))
        st.stop()
    stability_df = cached_analyze_county_stability(hourly_df)

st.title(t("county_overview_title"))

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

# ===== 分類邏輯（絕對門檻 + 相對條件）=====
AQI_GOOD_THRESHOLD = 30
AQI_RISK_THRESHOLD = 50

mean_avg = stability_df["mean_aqi"].mean()
std_avg = stability_df["std_aqi"].mean()

risk_df = stability_df[
    (stability_df["mean_aqi"] > AQI_RISK_THRESHOLD)
    & (
        (stability_df["mean_aqi"] > mean_avg)
        | (stability_df["std_aqi"] > std_avg)
    )
].copy()

good_df = stability_df[
    (stability_df["mean_aqi"] < AQI_GOOD_THRESHOLD)
    |
    (
        (stability_df["mean_aqi"] >= AQI_GOOD_THRESHOLD)
        & (stability_df["mean_aqi"] < mean_avg)
        & (stability_df["high_pollution_ratio"] == 0)
    )
].copy()

st.caption(t("county_how_to_read_body"))

# ===== 縣市圖表與詳細指標（Tabs 組織）=====
tab_overview, tab_metrics, tab_data = st.tabs([
    t("county_tab_overview"),
    t("county_tab_metrics"),
    t("county_tab_data"),
])

with tab_overview:
    # ===== 頂部資訊容器 =====
    with st.container(border=True):
        high_aqi_county = stability_df.loc[stability_df["mean_aqi"].idxmax()]
        low_aqi_county = stability_df.loc[stability_df["mean_aqi"].idxmin()]
        high_volatility_county = stability_df.loc[stability_df["std_aqi"].idxmax()]
        
        col_high, col_low, col_volatile, col_spacer = st.columns([1.2, 1.2, 1.2, 2])
        
        with col_high:
            st.markdown(f"**{t('highest_risk_county')}**")
            st.markdown(f"<div style='font-size: 1rem; font-weight: bold; color: #d64545;'>{high_aqi_county['county'][:5]}</div>", unsafe_allow_html=True)
            st.caption(f"{high_aqi_county['mean_aqi']:.1f} AQI")
        
        with col_low:
            st.markdown(f"**{t('best_county')}**")
            st.markdown(f"<div style='font-size: 1rem; font-weight: bold; color: #2e7d32;'>{low_aqi_county['county'][:5]}</div>", unsafe_allow_html=True)
            st.caption(f"{low_aqi_county['mean_aqi']:.1f} AQI")
        
        with col_volatile:
            st.markdown(f"**{t('highest_volatility')}**")
            st.markdown(f"<div style='font-size: 1rem; font-weight: bold; color: #F4A259;'>{high_volatility_county['county'][:5]}</div>", unsafe_allow_html=True)
            st.caption(f"{high_volatility_county['std_aqi']:.2f}")
    
    st.markdown(f"### {t('county_overview_chart')}")
    
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
                scale=alt.Scale(range=[90, 920]),
                legend=None,
            ),
            color=alt.Color(
                "high_pollution_ratio:Q",
                title=t("county_high_pol_ratio"),
                scale=alt.Scale(scheme="blues"),
                legend=alt.Legend(
                    titlePadding=12,
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
        .properties(width="container", padding={"right": 24, "left": 4, "top": 10, "bottom": 10})
        .interactive()
        .configure_axis(grid=True, gridColor="#d8e2ec")
        .configure_view(strokeWidth=0, fill="transparent")
        .configure(background="transparent")
    )

    st.altair_chart(risk_chart, use_container_width=True, key="county_risk_bubble_chart")

with tab_metrics:
    st.markdown(f"### {t('indicator_ranking')}")
    
    mean_sorted = stability_df.sort_values("mean_aqi", ascending=False)
    std_sorted = stability_df.sort_values("std_aqi", ascending=False)
    ratio_sorted = stability_df.sort_values("high_pollution_ratio", ascending=False)

    metric_c1, metric_c2 = st.columns(2)

    with metric_c1:
        st.markdown(f"**{t('mean_aqi_ranking')}**")
        mean_chart = (
            alt.Chart(mean_sorted)
            .mark_bar(color="#2B6CB0", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                y=alt.Y("county:N", sort=None, title=""),
                x=alt.X("mean_aqi:Q", title=t("county_mean_aqi")),
                tooltip=[
                    alt.Tooltip("county:N", title=t("county")),
                    alt.Tooltip("mean_aqi:Q", title=t("county_mean_aqi"), format=".2f"),
                ],
            )
            .properties(width="container")
            .configure_axis(grid=True, gridColor="#d8e2ec")
            .configure_view(strokeWidth=0, fill="transparent")
            .configure(background="transparent")
        )
        st.altair_chart(mean_chart, use_container_width=True)

    with metric_c2:
        st.markdown(f"**{t('volatility_ranking')}**")
        std_chart = (
            alt.Chart(std_sorted)
            .mark_bar(color="#5A9BD5", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                y=alt.Y("county:N", sort=None, title=""),
                x=alt.X("std_aqi:Q", title=t("county_std_aqi")),
                tooltip=[
                    alt.Tooltip("county:N", title=t("county")),
                    alt.Tooltip("std_aqi:Q", title=t("county_std_aqi"), format=".2f"),
                ],
            )
            .properties(width="container")
            .configure_axis(grid=True, gridColor="#d8e2ec")
            .configure_view(strokeWidth=0, fill="transparent")
            .configure(background="transparent")
        )
        st.altair_chart(std_chart, use_container_width=True)

    st.markdown(f"**{t('high_pollution_ratio_ranking')}**")
    ratio_chart = (
        alt.Chart(ratio_sorted)
        .mark_bar(color="#5A9BD5", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            y=alt.Y("county:N", sort=None, title=""),
            x=alt.X("high_pollution_ratio:Q", title=t("county_high_pol_ratio"), axis=alt.Axis(format="%")),
            tooltip=[
                alt.Tooltip("county:N", title=t("county")),
                alt.Tooltip("high_pollution_ratio:Q", title=t("county_high_pol_ratio"), format=".2%"),
            ],
        )
        .properties(width="container")
        .configure_axis(grid=True, gridColor="#d8e2ec")
        .configure_view(strokeWidth=0, fill="transparent")
        .configure(background="transparent")
    )
    st.altair_chart(ratio_chart, use_container_width=True)

with tab_data:
    st.markdown(f"### {t('complete_data_table')}")
    
    display_df = stability_df[
        ["county", "mean_aqi", "std_aqi", "high_pollution_ratio", "total_count"]
    ].copy()
    display_df["mean_aqi"] = display_df["mean_aqi"].round(2)
    display_df["std_aqi"] = display_df["std_aqi"].round(2)
    display_df["high_pollution_ratio"] = (display_df["high_pollution_ratio"] * 100).round(2)
    
    st.dataframe(
        display_df.rename(
            columns={
                "county": t("county"),
                "mean_aqi": t("county_mean_aqi"),
                "std_aqi": t("county_std_aqi"),
                "high_pollution_ratio": f"{t('county_high_pol_ratio')}(%)",
                "total_count": t("county_total_count"),
            }
        ),
        use_container_width=True,
        hide_index=True,
    )


# ===== 綜合分析結論 =====
st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown("---")
st.markdown("<br>", unsafe_allow_html=True)

st.markdown(
    f'<div style="font-size: 1.05rem; font-weight: 600; margin-bottom: 16px;">{t("county_overall_insight_title")}</div>',
    unsafe_allow_html=True,
)

st.markdown(
    f'<div style="font-size: 0.98rem; line-height: 1.8; color:#3a4860; margin-bottom: 24px;">{t("county_overall_insight_body")}</div>',
    unsafe_allow_html=True,
)

col_risk, col_good = st.columns(2)

with col_risk:
    st.markdown(f"**{t('county_high_risk_label')}**")
    if risk_df.empty:
        st.info(t("stable_air_quality"))
    else:
        risk_text = "、".join(risk_df["county"].head(5).tolist())
        st.info(risk_text)

with col_good:
    st.markdown(f"**{t('county_good_label')}**")
    good_df_sorted = good_df.sort_values(["mean_aqi", "std_aqi"], ascending=[True, True])
    if good_df_sorted.empty:
        st.info(t("no_data"))
    else:
        good_text = "、".join(good_df_sorted["county"].head(5).tolist())
        st.info(good_text)

# ===== 詳細資訊 Expander =====
st.markdown("---")

with st.expander(t("details_info"), expanded=False):
    st.markdown(f"#### {t('county_name_comparison')}")
    
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
            {"county_zh": "連江縣", "county_en": "Lianchiang County", "county_abbrev": "LIE"},
        ]
    ).rename(
        columns={
            "county_zh": t("county_zh"),
            "county_en": t("county_en"),
            "county_abbrev": t("county_abbrev"),
        }
    )
    
    st.dataframe(county_ref_df, use_container_width=True, hide_index=True)
    
    st.markdown(f"#### {t('classification_criteria')}")
    st.markdown(f"""
    **{t('county_high_risk_criteria_label')}**：
    - {t('county_high_risk_criteria_line1').format(risk_threshold=AQI_RISK_THRESHOLD)}
    - {t('county_high_risk_criteria_line2').format(mean_avg=mean_avg, std_avg=std_avg)}

    **{t('county_good_criteria_label')}**：
    - {t('county_good_criteria_line1').format(good_threshold=AQI_GOOD_THRESHOLD)}
    - {t('county_good_criteria_line2').format(good_threshold=AQI_GOOD_THRESHOLD, mean_avg=mean_avg)}
    """)
    
    st.markdown(f"#### {t('raw_data_preview')}")
    preview_df = stability_df[
        ["county", "mean_aqi", "std_aqi", "high_pollution_ratio", "total_count"]
    ].head(20).copy()
    preview_df = preview_df.rename(
        columns={
            "county": t("county"),
            "mean_aqi": t("county_mean_aqi"),
            "std_aqi": t("county_std_aqi"),
            "high_pollution_ratio": t("county_high_pol_ratio"),
            "total_count": t("county_total_count"),
        }
    )
    
    st.dataframe(preview_df, use_container_width=True, hide_index=True)

