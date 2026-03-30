from pathlib import Path
import sys
import altair as alt
import pandas as pd
import streamlit as st

# 插入路徑以載入自定義 utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from utils import apply_style, render_global_sidebar

BASE_DIR = Path(__file__).resolve().parent.parent
FORECAST_PATH = BASE_DIR / "data" / "forecast.csv"

# --- 介面文字設定 ---
def _lang(): return st.session_state.get("lang", "zh")

def _i18n():
    lang = _lang()
    return {
        "title": "AQI 未來 24 小時預測系統" if lang=="zh" else "AQI 24H Forecast System",
        "select_label": "查看對象" if lang=="zh" else "View Selection",
        "all": "全台灣總覽" if lang=="zh" else "National Overview",
        "chart_filter_label": "圖表縣市（可複選）" if lang=="zh" else "Chart Counties (multi-select)",
        "summary_title": "數據摘要" if lang=="zh" else "Summary",
        "metric_avg": "平均預測值" if lang=="zh" else "Avg Forecast",
        "metric_peak": "預測最高峰" if lang=="zh" else "Peak Forecast",
        "metric_best": "最清新時段" if lang=="zh" else "Best Air Quality",
        "chart_title": "AQI 趨勢預測圖" if lang=="zh" else "AQI Trend Forecast",
        "axis_time": "預測時間" if lang=="zh" else "Forecast Time",
        "axis_aqi": "預測 AQI" if lang=="zh" else "Predicted AQI",
        "tooltip_county": "縣市" if lang=="zh" else "County",
        "tooltip_time": "時間" if lang=="zh" else "Time",
        "tooltip_aqi": "預測值" if lang=="zh" else "Predicted AQI",
        "aqi_prefix": "AQI" if lang=="zh" else "AQI",
        "error_no_data": "找不到預測資料。" if lang=="zh" else "Forecast data not found.",
        "warn_select_county": "請至少選擇一個縣市。" if lang=="zh" else "Please select at least one county for the chart.",
        "warn_no_chart_data": "圖表無資料。" if lang=="zh" else "No data available for selected chart counties.",
        "disclaimer": "此數據由 XGBoost 模型計算，僅供參考。" if lang=="zh" else "⚠️ AI generated; for reference only."
    }

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
    t = _i18n()

    # --- 自定義淺藍主題 CSS ---
    st.markdown(f"""
        <style>
        .stApp {{ background-color: #f0f7ff; }}
        /* Metric Card 樣式 */
        [data-testid="stMetricValue"] {{ color: #1e3a8a; font-weight: bold; }}
        [data-testid="stMetricLabel"] {{ color: #60a5fa; }}
        /* 按鈕與下拉選單顏色調整 */
        .stSelectbox div[data-baseweb="select"] {{ border-color: #bfdbfe; }}
        .stButton>button {{ background-color: #60a5fa; color: white; border-radius: 8px; border: none; }}
        .stButton>button:hover {{ background-color: #3b82f6; border: none; }}
        /* 圖表複選 filter：改成藍色系，不用橘色預設 */
        .st-key-forecast_chart_county_multiselect [data-baseweb="tag"] {{
            background-color: #1e40af !important;
            border-color: #1e40af !important;
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
            background-color: #1e40af !important;
            color: #ffffff !important;
        }}
        .st-key-forecast_chart_county_multiselect div[data-baseweb="select"] > div:focus,
        .st-key-forecast_chart_county_multiselect div[data-baseweb="select"] > div:focus-within {{
            border-color: #1e40af !important;
            box-shadow: 0 0 0 1px #1e40af !important;
        }}
        /* 標題加強 */
            h1 {{ color: #1e40af; font-size: 2.2rem !important; }}
        /* Altair 圖表透明背景 */
        [data-testid="stVegaLiteChart"] .vega-embed,
        [data-testid="stVegaLiteChart"] canvas,
        [data-testid="stVegaLiteChart"] svg {{
            background: transparent !important;
        }}
        </style>
    """, unsafe_allow_html=True)

    st.title(t["title"])
    
    df = load_data()
    if df.empty:
        st.error("Forecast data has expired")
        st.stop()

    # --- 1. 頂部導覽篩選區 (按鈕化布局) ---
    counties = sorted(df["county"].unique().tolist())
    
    # 使用 selectbox 作為主要切換器，預設為「全體」
    selection = st.selectbox(
        t["select_label"],
        options=[t["all"]] + counties,
        index=0,
        label_visibility="collapsed",
    )

    # 根據選擇過濾資料
    if selection == t["all"]:
        display_df = df.copy()
        display_name = t["all"]
    else:
        display_df = df[df["county"] == selection].copy()
        display_name = selection

    # --- 2. 核心數據摘要區 (隨選擇連動) ---
    avg_val = display_df["predicted_aqi"].mean()
    peak_row = display_df.loc[display_df["predicted_aqi"].idxmax()]
    best_row = display_df.loc[display_df["predicted_aqi"].idxmin()]

    st.write(f"### {display_name} {t['summary_title']}")
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
    m_col1, m_col2, m_col3 = st.columns(3)
    
    with m_col1:
        st.metric(t["metric_avg"], f"{avg_val:.1f}")
    with m_col2:
        st.metric(t["metric_peak"], f"{peak_row['predicted_aqi']:.1f}", 
                  delta=f"{peak_row['forecast_time'].strftime('%H:00')}", delta_color="inverse")
    with m_col3:
        st.metric(t["metric_best"], best_row["forecast_time"].strftime("%H:00"), 
                  delta=f"{t['aqi_prefix']} {best_row['predicted_aqi']:.1f}", delta_color="normal")

    st.divider()

    chart_options = [t["all"]] + counties
    chart_selected = st.multiselect(
        t["chart_filter_label"],
        options=chart_options,
        default=[t["all"]],
        key="forecast_chart_county_multiselect",
    )

    if not chart_selected:
        st.warning(t["warn_select_county"])
        st.stop()

    # --- 3. 視覺化圖表區 (去除了打叉模式，回歸簡潔) ---
    if t["all"] in chart_selected:
        chart_df = df.copy()
        chart_name = t["all"]
    else:
        selected_chart_counties = [c for c in chart_selected if c in counties]
        chart_df = df[df["county"].isin(selected_chart_counties)].copy()
        chart_name = ", ".join(selected_chart_counties)

    if chart_df.empty:
        st.warning(t["warn_no_chart_data"])
        st.stop()

    st.subheader(f"{chart_name} {t['chart_title']}")

    # 設置背景顏色區塊 (標準 AQI 分級)
    y_max = 160
    
    bands = alt.Chart(pd.DataFrame([
        {"y": 0, "y2": 50, "color": "#e2f9e2"},   # 優
        {"y": 50, "y2": 100, "color": "#fff9e2"}, # 普通
        {"y": 100, "y2": y_max, "color": "#f9e2e2"} # 不良
    ])).mark_rect(opacity=0.4).encode(
        y=alt.Y("y:Q"), y2="y2:Q", color=alt.Color("color:N", scale=None)
    )

    # 主趨勢線：平滑曲線 + 粗度增加
    line = alt.Chart(chart_df).mark_line(
        interpolate='monotone', 
        strokeWidth=3.2,
        color='#7ea8d8'
    ).encode(
        x=alt.X("forecast_time:T", title=t["axis_time"], axis=alt.Axis(format="%H:00")),
        y=alt.Y("predicted_aqi:Q", title=t["axis_aqi"], scale=alt.Scale(domain=[0, 160])),
        tooltip=[
            alt.Tooltip("county:N", title=t["tooltip_county"]),
            alt.Tooltip("forecast_time:T", title=t["tooltip_time"], format="%m/%d %H:%M"),
            alt.Tooltip("predicted_aqi:Q", title=t["tooltip_aqi"], format=".1f")
        ]
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

    # 合併圖表 (去除了 rules 和 points，保持乾淨)
    final_chart = (
        alt.layer(bands, line)
        .properties(height=400)
        .configure_view(strokeOpacity=0, fill="transparent")
        .configure(background="transparent")
    )

    st.altair_chart(final_chart, use_container_width=True)

    # --- 4. 底部頁尾 ---
    st.info(t["disclaimer"])

if __name__ == "__main__":
    main()
