from pathlib import Path
import sys
import altair as alt
import pandas as pd
import streamlit as st

# 插入路徑以載入自定義 utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils import apply_style, render_global_sidebar, t

BASE_DIR = Path(__file__).resolve().parent.parent
FORECAST_PATH = BASE_DIR / "data" / "forecast.csv"

# --- 資料讀取 ---
def load_data():
    if not FORECAST_PATH.exists(): return pd.DataFrame()
    df = pd.read_csv(FORECAST_PATH)
    df["forecast_time"] = pd.to_datetime(df["forecast_time"])
    current_hour = pd.Timestamp.now().floor("h")
    df = df[df["forecast_time"] >= current_hour].copy()
    return df

def main():
    apply_style()
    render_global_sidebar("pages/forecast.py")

    # --- 自定義淺藍主題 CSS ---
    st.markdown(f"""
        <style>
        .stApp {{ background-color: #f0f7ff; }}
        /* Metric Card 樣式 */
        [data-testid="stMetricValue"] {{ color: #1e3a8a; font-weight: bold; }}
        [data-testid="stMetricLabel"] {{ color: #60a5fa; }}
        /* 按鈕與下拉選單顏色調整 */
        .stSelectbox div[data-baseweb="select"] {{ border-color: #bfdbfe; }}
        .stSelectbox div[data-baseweb="select"] > div,
        .stMultiSelect div[data-baseweb="select"] > div {{
            background-color: #ffffff !important;
        }}
        .stButton>button {{ background-color: #60a5fa; color: white; border-radius: 8px; border: none; }}
        .stButton>button:hover {{ background-color: #3b82f6; border: none; }}
        /* 圖表複選 filter：改成藍色系，不用橘色預設 */
        .st-key-forecast_chart_county_multiselect [data-baseweb="tag"] {{
            background-color: #4f83cc !important;
            border-color: #4f83cc !important;
        }}
        .st-key-forecast_chart_county_multiselect [data-baseweb="tag"] span {{
            color: #ffffff !important;
        }}
        .st-key-forecast_chart_county_multiselect [data-baseweb="tag"] svg,
        .st-key-forecast_chart_county_multiselect [data-baseweb="tag"] path {{
            fill: #ffffff !important;
            stroke: #ffffff !important;
        }}
        .st-key-forecast_chart_county_multiselect div[role="option"][aria-selected="true"] {{
            background-color: #4f83cc !important;
            color: #ffffff !important;
        }}
        .st-key-forecast_chart_county_multiselect div[data-baseweb="select"] > div:focus,
        .st-key-forecast_chart_county_multiselect div[data-baseweb="select"] > div:focus-within {{
            border-color: #4f83cc !important;
            box-shadow: 0 0 0 1px #4f83cc !important;
        }}
        /* 標題顏色沿用藍色主題 */
            h1 {{ color: #1e40af; }}
        /* Altair 圖表透明背景 */
        [data-testid="stVegaLiteChart"] .vega-embed,
        [data-testid="stVegaLiteChart"] canvas,
        [data-testid="stVegaLiteChart"] svg {{
            background: transparent !important;
        }}
        </style>
    """, unsafe_allow_html=True)

    st.title(t("forecast_title"))
    
    df = load_data()
    if df.empty:
        st.error(t("forecast_error_expired_data"))
        st.stop()

    # --- 1. 頂部導覽篩選區 (按鈕化布局) ---
    counties = sorted(df["county"].unique().tolist())
    options = [t("forecast_all")] + counties
    current_selection = st.session_state.get("forecast_main_select", t("forecast_all"))
    if current_selection not in options:
        current_selection = t("forecast_all")

    summary_title_name = current_selection if current_selection in counties else t("forecast_all")
    st.write(f"### {summary_title_name} {t('forecast_summary_title')}")

    st.caption(t("forecast_select_hint"))

    # 使用 selectbox 作為主要切換器，預設為「全體」
    selection = st.selectbox(
        t("forecast_select_label"),
        options=options,
        index=options.index(current_selection),
        key="forecast_main_select",
        label_visibility="collapsed",
    )

    # 根據選擇過濾資料
    if selection == t("forecast_all"):
        display_df = df.copy()
        display_name = t("forecast_all")
    else:
        display_df = df[df["county"] == selection].copy()
        display_name = selection

    # --- 2. 核心數據摘要區 (隨選擇連動) ---
    avg_val = display_df["predicted_aqi"].mean()
    peak_row = display_df.loc[display_df["predicted_aqi"].idxmax()]
    best_row = display_df.loc[display_df["predicted_aqi"].idxmin()]

    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    m_col1, m_col2, m_col3 = st.columns(3)

    with m_col1:
        st.metric(t("forecast_metric_avg"), f"{avg_val:.1f}")
    with m_col2:
        st.metric(t("forecast_metric_peak"), f"{peak_row['predicted_aqi']:.1f}",
                  delta=f"{peak_row['forecast_time'].strftime('%H:00')}", delta_color="inverse")
    with m_col3:
        st.metric(t("forecast_metric_best"), best_row["forecast_time"].strftime("%H:00"),
                  delta=f"{t('forecast_aqi_prefix')} {best_row['predicted_aqi']:.1f}", delta_color="normal")

    st.divider()

    selected_for_title = st.session_state.get("forecast_chart_county_multiselect", [t("forecast_all")])
    if t("forecast_all") in selected_for_title:
        chart_title_name = t("forecast_all")
    else:
        selected_title_counties = [c for c in selected_for_title if c in counties]
        chart_title_name = ", ".join(selected_title_counties) if selected_title_counties else t("forecast_all")

    st.subheader(f"{chart_title_name} {t('forecast_chart_title')}")

    chart_options = [t("forecast_all")] + counties
    st.caption(t("forecast_chart_filter_label"))
    chart_selected = st.multiselect(
        t("forecast_chart_filter_label"),
        options=chart_options,
        default=[t("forecast_all")],
        key="forecast_chart_county_multiselect",
        label_visibility="collapsed",
    )
    st.caption(t("forecast_zoom_hint"))

    if not chart_selected:
        st.warning(t("forecast_warn_select_county"))
        st.stop()

    # --- 3. 視覺化圖表區 (去除了打叉模式，回歸簡潔) ---
    if t("forecast_all") in chart_selected:
        chart_df = df.copy()
        chart_name = t("forecast_all")
    else:
        selected_chart_counties = [c for c in chart_selected if c in counties]
        chart_df = df[df["county"].isin(selected_chart_counties)].copy()
        chart_name = ", ".join(selected_chart_counties)

    if chart_df.empty:
        st.warning(t("forecast_warn_no_chart_data"))
        st.stop()

    # 設置背景顏色區塊 (標準 AQI 分級)
    y_max = 160
    
    bands = alt.Chart(pd.DataFrame([
        {"y": 0, "y2": 50, "color": "#e2f9e2"},   # 優
        {"y": 50, "y2": 100, "color": "#fff9e2"}, # 普通
        {"y": 100, "y2": y_max, "color": "#f9e2e2"} # 不良
    ])).mark_rect(opacity=0.4).encode(
        y=alt.Y("y:Q"),
        y2="y2:Q",
        color=alt.Color("color:N", scale=None),
        tooltip=alt.value(None),
    )

    # 主趨勢線：平滑曲線 + 粗度增加
    base_encoding = {
        "x": alt.X("forecast_time:T", title=t("forecast_axis_time"), axis=alt.Axis(format="%H:00")),
        "y": alt.Y("predicted_aqi:Q", title=t("forecast_axis_aqi"), scale=alt.Scale(domain=[0, 160])),
    }

    line = alt.Chart(chart_df).mark_line(
        interpolate='monotone', 
        strokeWidth=3.2,
        color='#7ea8d8'
    ).encode(**base_encoding)

    # 擴大偵測範圍：透明點層專門負責 tooltip 命中。
    hover_points = alt.Chart(chart_df).mark_circle(size=240, opacity=0).encode(
        x=base_encoding["x"],
        y=base_encoding["y"],
        tooltip=[
            alt.Tooltip("county:N", title=t("forecast_tooltip_county")),
            alt.Tooltip("predicted_aqi:Q", title=t("forecast_tooltip_aqi"), format=".1f"),
            alt.Tooltip("forecast_time:T", title=t("forecast_tooltip_time"), format="%m/%d %H:%M"),
        ],
    )

    # 多縣市圖使用柔和多色；單縣市使用單一柔和藍色。
    if chart_df["county"].nunique() > 1:
        line = line.encode(
            color=alt.Color(
                "county:N",
                legend=None,
                scale=alt.Scale(
                    range=[
                        "#7ea8d8", "#8dc5b8", "#d8b48a", "#c7a7d8", "#f0a6a6",
                        "#a8bfd8", "#9ec6a2", "#d9c18f", "#b9addc", "#e7b5c5",
                    ]
                ),
            )
        )
        hover_points = hover_points.encode(color=alt.Color("county:N", legend=None))

    # 合併圖表 (去除了 rules 和 points，保持乾淨)
    final_chart = (
        alt.layer(bands, line, hover_points)
        .properties(height=400)
        .interactive()
        .configure_view(strokeOpacity=0, fill="transparent")
        .configure(background="transparent")
    )

    st.altair_chart(final_chart, use_container_width=True)

    # --- 4. 底部頁尾 ---
    st.info(t("forecast_disclaimer"))
    st.caption(t("forecast_model_logic"))

if __name__ == "__main__":
    main()
