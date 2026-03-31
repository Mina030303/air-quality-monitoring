from __future__ import annotations

from linebot.v3.webhooks import FollowEvent

import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from flask import Flask, abort, request
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    ApiClient,
    Configuration,
    MessagingApi,
    PushMessageRequest,
    ReplyMessageRequest,
    TextMessage,
)
from linebot.v3.messaging.models import QuickReply, QuickReplyItem, MessageAction
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from sqlalchemy import create_engine, text

# Logger 實例
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")

app = Flask(__name__)
handler = WebhookHandler(LINE_CHANNEL_SECRET)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
engine = create_engine(os.getenv("DATABASE_URL", ""))

ROUTINE_HOURS = {7, 12, 17}
_last_routine_marker: str | None = None

# ==========================================
# 輔助函數區
# ==========================================

def get_aqi_status(aqi: float) -> str:
    if aqi is None:
        return ""
    try:
        aqi = float(aqi)
    except Exception:
        return ""
    if aqi <= 50:
        return "🟢 良好"
    elif aqi <= 100:
        return "🟡 普通"
    elif aqi <= 150:
        return "🟠 汙染"
    elif aqi <= 200:
        return "🔴 不健康"
    elif aqi <= 300:
        return "🟣 非常不健康"
    else:
        return "🟤 危害"

def push_line_message(user_id: str, message: str) -> bool:
    try:
        with ApiClient(configuration) as api_client:
            messaging_api = MessagingApi(api_client)
            msg = TextMessage(text=message)
            messaging_api.push_message(
                PushMessageRequest(
                    to=user_id,
                    messages=[msg],
                )
            )
        return True
    except Exception as exc:
        logger.error("LINE push failed: %s", exc)
        return False

def reply_line_message(reply_token: str, message: str, quick_reply=None) -> bool:
    try:
        with ApiClient(configuration) as api_client:
            messaging_api = MessagingApi(api_client)
            msg = TextMessage(text=message)
            if quick_reply:
                msg.quick_reply = quick_reply
            messaging_api.reply_message(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[msg],
                )
            )
        return True
    except Exception as exc:
        logger.error("LINE reply failed: %s", exc)
        return False

def ensure_tables() -> None:
    pass

def normalize_county_name(name: str) -> str | None:
    if not name:
        return None
    name = name.strip().replace("台", "臺")
    county_map = {
        "臺北": "臺北市", "台北": "臺北市", "北市": "臺北市",
        "新北": "新北市", "新北市": "新北市",
        "桃園": "桃園市", "桃園市": "桃園市",
        "臺中": "臺中市", "台中": "臺中市", "中市": "臺中市",
        "臺南": "臺南市", "台南": "臺南市", "南市": "臺南市",
        "高雄": "高雄市", "高雄市": "高雄市",
        "基隆": "基隆市", "基隆市": "基隆市",
        "新竹": "新竹縣", "新竹縣": "新竹縣", "新竹市": "新竹市",
        "嘉義": "嘉義縣", "嘉義縣": "嘉義縣", "嘉義市": "嘉義市",
        "苗栗": "苗栗縣", "苗栗縣": "苗栗縣",
        "彰化": "彰化縣", "彰化縣": "彰化縣",
        "南投": "南投縣", "南投縣": "南投縣",
        "雲林": "雲林縣", "雲林縣": "雲林縣",
        "屏東": "屏東縣", "屏東縣": "屏東縣",
        "宜蘭": "宜蘭縣", "宜蘭縣": "宜蘭縣",
        "花蓮": "花蓮縣", "花蓮縣": "花蓮縣",
        "臺東": "臺東縣", "台東": "臺東縣", "臺東縣": "臺東縣", "台東縣": "臺東縣",
        "澎湖": "澎湖縣", "澎湖縣": "澎湖縣",
        "金門": "金門縣", "金門縣": "金門縣",
        "連江": "連江縣", "連江縣": "連江縣",
    }
    if name in county_map:
        return county_map[name]
    for k, v in county_map.items():
        if name == v:
            return v
    for k, v in county_map.items():
        if name in k or name in v:
            return v
    return None

def remove_user_subscription(line_user_id: str) -> None:
    query = text("DELETE FROM user_subscriptions WHERE line_user_id = :line_user_id")
    with engine.begin() as conn:
        conn.execute(query, {"line_user_id": line_user_id})

def get_user_subscription(line_user_id: str) -> str | None:
    query = text("SELECT target_county FROM user_subscriptions WHERE line_user_id = :line_user_id")
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"line_user_id": line_user_id})
    if df.empty:
        return None
    return str(df.iloc[0]["target_county"])

def upsert_user_subscription(line_user_id: str, county: str) -> None:
    upsert_sql = text("""
        INSERT INTO user_subscriptions (line_user_id, target_county, updated_at)
        VALUES (:line_user_id, :county, CURRENT_TIMESTAMP)
        ON CONFLICT (line_user_id)
        DO UPDATE SET
            target_county = EXCLUDED.target_county,
            updated_at = CURRENT_TIMESTAMP
    """)
    with engine.begin() as conn:
        conn.execute(upsert_sql, {"line_user_id": line_user_id, "county": county})

def get_top3_stations_latest_forecast() -> pd.DataFrame:
    query = text("""
        SELECT f.forecast_time, CAST(f.siteid AS TEXT) AS siteid, s.sitename, s.county, f.predicted_aqi
        FROM public.forecast f
        JOIN public.air_quality_stations s ON CAST(f.siteid AS TEXT) = CAST(s.siteid AS TEXT)
        WHERE f.forecast_time = (
            SELECT MIN(f2.forecast_time) FROM public.forecast f2 WHERE f2.forecast_time > NOW()
        )
        ORDER BY f.predicted_aqi DESC
        LIMIT 3
    """)
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)

def send_routine_updates(now: datetime) -> None:
    pass

def scheduler_loop() -> None:
    pass

# ==========================================
# Webhook 進入點
# ==========================================

@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        logger.warning("Invalid LINE signature")
        abort(400)
    except Exception as exc:
        logger.error("Webhook handling error: %s", exc)
        abort(500)
    return "OK"

# ==========================================
# 核心對話邏輯 (唯一入口)
# ==========================================

@handler.add(MessageEvent, message=TextMessageContent)
def handle_text_message(event: MessageEvent) -> None:
    text_msg = event.message.text.strip()
    text_msg_lower = text_msg.lower()
    line_user_id = event.source.user_id
    reply_token = event.reply_token

    if not line_user_id:
        return

    # 1. 全台概況
    if text_msg in {"全台概況", "全台", "概況"}:
        time_query = text("SELECT MIN(forecast_time) AS ft FROM public.forecast WHERE forecast_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Taipei')")
        with engine.connect() as conn:
            time_df = pd.read_sql_query(time_query, conn)
            ft = time_df.loc[0, "ft"]
            if pd.isna(ft):
                reply_line_message(reply_token, "目前暫無預測數據，請稍後再試。")
                return
            query = text("""
                SELECT s.county, s.sitename, f.predicted_aqi
                FROM public.forecast f
                JOIN public.air_quality_stations s ON CAST(f.siteid AS TEXT) = CAST(s.siteid AS TEXT)
                WHERE f.forecast_time = :ft
            """)
            df = pd.read_sql_query(query, conn, params={"ft": ft})
            
        if df.empty or len(df) < 3:
            reply_line_message(reply_token, "目前暫無預測數據，請稍後再試。")
            return
            
        worst = df.nlargest(3, "predicted_aqi")
        best = df.nsmallest(3, "predicted_aqi")
        msg = ["【全台 AQI 測站概況】\n\n空氣最糟 Top 3："]
        for _, row in worst.iterrows():
            status = get_aqi_status(row["predicted_aqi"])
            msg.append(f"[{row['county']}] {row['sitename']}：{row['predicted_aqi']:.0f} {status}")
            
        msg.append("\n空氣最佳 Top 3：")
        for _, row in best.iterrows():
            status = get_aqi_status(row["predicted_aqi"])
            msg.append(f"[{row['county']}] {row['sitename']}：{row['predicted_aqi']:.0f} {status}")
            
        reply_line_message(reply_token, "\n".join(msg))
        return

    # 2. 目前選取狀態
    elif text_msg in {"目前選取", "選取", "我的選取", "目前選取狀態"}:
        county = get_user_subscription(line_user_id)
        if county:
            reply_line_message(reply_token, f"您目前選取的縣市為：{county}")
        else:
            reply_line_message(reply_token, "尚未選取任何縣市。請直接輸入縣市名稱進行選取。")
        return

    # 3. 幫助指令與歡迎訊息
    elif text_msg in {"幫助", "指令", "功能表", "選單", "功能", "help", "commands", "menu", "options", "開始", "start", "hi", "你好", "您好", "hello", "哈囉", "嗨", "歡迎"}:
        help_msg = (
            "【功能說明】\n"
            "官方網頁(電腦版)：https://mina030303-air-quality-monitoring-app-f3ksh1.streamlit.app/\n"
            "１．全台概況：顯示空氣最好/最差縣市\n"
            "２．未來趨勢：查詢未來 6 小時預報\n"
            "３．目前選取：查詢目前選取的縣市\n"
            "４．選取縣市：直接輸入縣市名稱（如：台北、桃園），會顯示所有測站清單\n"
            "５．查詢：查詢目前選取縣市測站的最新預測\n"
            "６．解除選取：輸入「取消」\n"
            "７．功能表：輸入「功能表」或「選單」"
        )
        reply_line_message(reply_token, help_msg)
        return
    
    # 4. AQI分級查詢
    elif text_msg in {"aqi分級", "aqi 分級", "aqi等級", "aqi標準", "aqi級距", "aqi說明", "空氣品質分級", "空氣品質標準"}:
        aqi_guide = (
            "【AQI 分級標準】\n"
            "🟢 0~50：良好 (Good)\n"
            "🟡 51~100：普通 (Moderate)\n"
            "🟠 101~150：汙染 (Polluted)"
        )
        reply_line_message(reply_token, aqi_guide)
        return

    # 5. 歧義判斷
    ambiguous_map = {
        "嘉義": ["嘉義市", "嘉義縣"],
        "新竹": ["新竹市", "新竹縣"],
    }
    if text_msg in ambiguous_map:
        items = [QuickReplyItem(action=MessageAction(label=opt, text=opt)) for opt in ambiguous_map[text_msg]]
        reply_line_message(reply_token, f"請問您是指哪一個 {text_msg}？", quick_reply=QuickReply(items=items))
        return

    # 6. 解除選取
    if text_msg in {"取消", "取消選取", "unsubscribe"}:
        remove_user_subscription(line_user_id)
        reply_line_message(reply_token, "已成功取消選取！如需再次接收預測，請輸入縣市名稱重新選取。")
        return

    # 7. 特定測站趨勢查詢 (解析「趨勢:測站名」)
    if text_msg.startswith("趨勢:"):
        sitename = text_msg[3:].strip()
        county = get_user_subscription(line_user_id)
        if not county:
            reply_line_message(reply_token, "尚未選取，請先傳送縣市名稱（例如：台北、桃園）。")
            return
            
        # 確保抓取資料方法一致：加上 AVG 與 GROUP BY 處理同名測站，並 LIMIT 6 顯示六小時
        query = text("""
            SELECT f.forecast_time, AVG(f.predicted_aqi) AS predicted_aqi
            FROM public.forecast f
            JOIN public.air_quality_stations s ON CAST(f.siteid AS TEXT) = CAST(s.siteid AS TEXT)
            WHERE s.sitename = :sitename AND REPLACE(CAST(s.county AS TEXT), '台', '臺') = :county
              AND f.forecast_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Taipei')
            GROUP BY f.forecast_time
            ORDER BY f.forecast_time ASC
            LIMIT 6
        """)
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={"sitename": sitename, "county": county})
            
        if df.empty:
            reply_line_message(reply_token, f"查無 {sitename} 未來預測數據。")
            return
            
        msg = [f"【{sitename}】未來趨勢："]
        for _, row in df.iterrows():
            t = pd.to_datetime(row["forecast_time"]).strftime("%H:%M")
            aqi = row["predicted_aqi"]
            status = get_aqi_status(aqi)
            status_icon = status.split()[0] if status else ""
            msg.append(f"{t} - {aqi:.0f} {status_icon}")
            
        reply_line_message(reply_token, "\n".join(msg))
        return

    # 8. 查詢與未來趨勢 (觸發測站 QuickReply 按鈕)
    if text_msg in {"查詢", "查詢預測", "未來趨勢", "趨勢", "未來"} or text_msg_lower == "current prediction":
        county = get_user_subscription(line_user_id)
        if not county:
            reply_line_message(reply_token, "尚未選取，請先傳送縣市名稱（例如：台北、桃園）。")
            return
            
        # 查詢該縣市所有測站 (確保有預測資料才顯示)
        query = text("""
            SELECT DISTINCT TRIM(s.sitename) AS sitename 
            FROM public.air_quality_stations s
            JOIN public.forecast f ON CAST(s.siteid AS TEXT) = CAST(f.siteid AS TEXT)
            WHERE REPLACE(CAST(s.county AS TEXT), '台', '臺') = :county 
              AND f.forecast_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Taipei')
            ORDER BY sitename
        """)
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={"county": county})
            
        if df.empty:
            reply_line_message(reply_token, f"查無 {county} 測站清單。")
            return
            
        # 這裡正確設定了按鈕：使用者看到的是 sitename，點下去送出的是 "趨勢:sitename"
        items = [QuickReplyItem(action=MessageAction(label=sn, text=f"趨勢:{sn}")) for sn in df["sitename"]]
        quick_reply = QuickReply(items=items[:13])  # LINE 最多 13 個按鈕
        reply_line_message(reply_token, "請選擇要查詢未來 6 小時趨勢的測站：", quick_reply=quick_reply)
        return

    # 9. 選取縣市邏輯 (處理一般縣市輸入)
    normalized_county = normalize_county_name(text_msg)
    if normalized_county:
        upsert_user_subscription(line_user_id, normalized_county)

        query = text("""
            SELECT DISTINCT TRIM(s.sitename) AS sitename 
            FROM public.air_quality_stations s
            JOIN public.forecast f ON CAST(s.siteid AS TEXT) = CAST(f.siteid AS TEXT)
            WHERE REPLACE(CAST(s.county AS TEXT), '台', '臺') = :county 
              AND f.forecast_time >= (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Taipei')
            ORDER BY sitename
        """)
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={"county": normalized_county})
            
        if not df.empty:
            sitelist = "、".join(df["sitename"].tolist())
            reply_line_message(
                reply_token,
                f"已成功選取：{normalized_county}\n本區域包含以下測站：\n{sitelist}\n\n請輸入「查詢」，選擇特定測站查看未來預報！"
            )
        else:
            reply_line_message(
                reply_token,
                f"已成功選取 {normalized_county}！（目前查無有效測站資料）"
            )
        return

    # 防呆提示
    reply_line_message(reply_token, "請輸入縣市名稱完成選取（例如：台北、桃園），或輸入「查詢」。")

def main() -> None:
    ensure_tables()
    # 啟動 Flask 伺服器
    app.run(host="0.0.0.0", port=5000)

if __name__ == "__main__":
    main()
