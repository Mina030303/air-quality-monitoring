from __future__ import annotations

from linebot.v3.webhooks import FollowEvent

import logging
import os
import re
import threading
import time
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
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

if not LINE_CHANNEL_ACCESS_TOKEN:
    raise ValueError("LINE_CHANNEL_ACCESS_TOKEN is not set in .env")
if not LINE_CHANNEL_SECRET:
    raise ValueError("LINE_CHANNEL_SECRET is not set in .env")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in .env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
handler = WebhookHandler(LINE_CHANNEL_SECRET)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
engine = create_engine(DATABASE_URL)

OFFICIAL_COUNTIES = {
    "臺北市",
    "新北市",
    "桃園市",
    "臺中市",
    "臺南市",
    "高雄市",
    "基隆市",
    "新竹市",
    "嘉義市",
    "新竹縣",
    "苗栗縣",
    "彰化縣",
    "南投縣",
    "雲林縣",
    "嘉義縣",
    "屏東縣",
    "宜蘭縣",
    "花蓮縣",
    "臺東縣",
    "澎湖縣",
    "金門縣",
    "連江縣",
}

COUNTY_ALIAS_MAP = {
    "臺北": "臺北市",
    "新北": "新北市",
    "桃園": "桃園市",
    "臺中": "臺中市",
    "臺南": "臺南市",
    "高雄": "高雄市",
    "基隆": "基隆市",
    "新竹市": "新竹市",
    "嘉義市": "嘉義市",
    # "新竹": "新竹縣",  # 取消自動校正
    "苗栗": "苗栗縣",
    "彰化": "彰化縣",
    "南投": "南投縣",
    "雲林": "雲林縣",
    # "嘉義": "嘉義縣",  # 取消自動校正
    "屏東": "屏東縣",
    "宜蘭": "宜蘭縣",
    "花蓮": "花蓮縣",
    "臺東": "臺東縣",
    "澎湖": "澎湖縣",
    "金門": "金門縣",
    "連江": "連江縣",
}

COUNTY_REGEX_MAP: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^(?:台|臺)?北(?:市)?$"), "臺北市"),
    (re.compile(r"^新北(?:市)?$"), "新北市"),
    (re.compile(r"^桃園(?:市)?$"), "桃園市"),
    (re.compile(r"^(?:台|臺)?中(?:市)?$"), "臺中市"),
    (re.compile(r"^(?:台|臺)?南(?:市)?$"), "臺南市"),
    (re.compile(r"^高雄(?:市)?$"), "高雄市"),
]

ROUTINE_HOURS = {7, 12, 17}

# In-process marker to avoid duplicate routine send within same hour.
_last_routine_marker: str | None = None



# 台灣環境部 AQI 分級
def get_aqi_status(aqi: float) -> str:
    if aqi <= 50:
        return "🟢 良好"
    elif aqi <= 100:
        return "🟡 普通"
    elif aqi <= 150:
        return "🔴 汙染"
    else:
        return "⚫ 危險"


def normalize_county_name(user_input: str) -> str | None:
    text_in = re.sub(r"\s+", "", user_input.strip())
    if not text_in:
        return None

    for pattern, official in COUNTY_REGEX_MAP:
        if pattern.match(text_in):
            return official

    unified = text_in.replace("台", "臺")
    if unified in OFFICIAL_COUNTIES:
        return unified

    # 僅允許 base 自動校正，排除「嘉義」「新竹」
    base = unified[:-1] if unified.endswith(("市", "縣")) else unified
    if base in {"嘉義", "新竹"}:
        return None
    if base in COUNTY_ALIAS_MAP:
        return COUNTY_ALIAS_MAP[base]
    if unified in COUNTY_ALIAS_MAP:
        return COUNTY_ALIAS_MAP[unified]

    return None


def resolve_county_for_storage(normalized_county: str) -> str | None:
    """Resolve to exact county string used by air_quality_stations table."""
    query = text(
        """
        SELECT county
        FROM public.air_quality_stations
        WHERE REPLACE(CAST(county AS TEXT), '台', '臺') = :normalized_county
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"normalized_county": normalized_county})

    if df.empty:
        return None
    return str(df.iloc[0]["county"])


def send_to_discord(message: str) -> bool:
    if not DISCORD_WEBHOOK_URL:
        logger.debug("DISCORD_WEBHOOK_URL not set, skip Discord send")
        return False

    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10)
        if resp.status_code in (200, 204):
            return True
        logger.warning("Discord send failed: status=%s body=%s", resp.status_code, resp.text)
        return False
    except requests.RequestException as exc:
        logger.warning("Discord send request error: %s", exc)
        return False


def send_to_line(message: str) -> None:
    """Placeholder for future LINE Messaging API broadcast integration."""
    logger.info("[LINE placeholder] %s", message)


def push_line_message(user_id: str, message: str) -> bool:
    try:
        with ApiClient(configuration) as api_client:
            messaging_api = MessagingApi(api_client)
            messaging_api.push_message(
                PushMessageRequest(
                    to=user_id,
                    messages=[TextMessage(text=message)],
                )
            )
        return True
    except Exception as exc:
        logger.error("LINE push failed for user %s: %s", user_id, exc)
        return False


from linebot.v3.messaging.models import QuickReply, QuickReplyItem, MessageAction

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
    create_sql = text(
        """
        CREATE TABLE IF NOT EXISTS user_subscriptions (
            line_user_id TEXT PRIMARY KEY,
            target_county TEXT NOT NULL,
            last_alert_at TIMESTAMP NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(create_sql)

        # Backward-compatible column migration for older schema.
        conn.execute(
            text(
                """
                ALTER TABLE user_subscriptions
                RENAME COLUMN county TO target_county
                """
            )
        ) if _column_exists(conn, "county") and not _column_exists(conn, "target_county") else None

        conn.execute(
            text(
                """
                ALTER TABLE user_subscriptions
                RENAME COLUMN last_alert_sent TO last_alert_at
                """
            )
        ) if _column_exists(conn, "last_alert_sent") and not _column_exists(conn, "last_alert_at") else None


def _column_exists(conn, column_name: str) -> bool:
    q = text(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'user_subscriptions'
          AND column_name = :column_name
        """
    )
    return bool(conn.execute(q, {"column_name": column_name}).scalar())


def upsert_user_subscription(line_user_id: str, county: str) -> None:
    upsert_sql = text(
        """
        INSERT INTO user_subscriptions (line_user_id, target_county, updated_at)
        VALUES (:line_user_id, :county, CURRENT_TIMESTAMP)
        ON CONFLICT (line_user_id)
        DO UPDATE SET
            target_county = EXCLUDED.target_county,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    with engine.begin() as conn:
        conn.execute(upsert_sql, {"line_user_id": line_user_id, "county": county})


def remove_user_subscription(line_user_id: str) -> None:
    """移除使用者訂閱紀錄"""
    query = text(
        """
        DELETE FROM user_subscriptions
        WHERE line_user_id = :line_user_id
        """
    )
    with engine.begin() as conn:
        conn.execute(query, {"line_user_id": line_user_id})


def get_user_subscription(line_user_id: str) -> str | None:
    query = text(
        """
        SELECT target_county
        FROM user_subscriptions
        WHERE line_user_id = :line_user_id
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"line_user_id": line_user_id})
    if df.empty:
        return None
    return str(df.iloc[0]["target_county"])


def get_latest_county_avg_pm25(county: str) -> tuple[pd.Timestamp, float] | None:
    query = text(
        """
        SELECT MIN(f.forecast_time) AS forecast_time, AVG(f.predicted_aqi) AS avg_aqi
        FROM public.forecast f
        JOIN public.air_quality_stations s ON CAST(f.siteid AS TEXT) = CAST(s.siteid AS TEXT)
        WHERE REPLACE(CAST(s.county AS TEXT), '台', '臺') = :county
          AND f.forecast_time > NOW()
        GROUP BY f.forecast_time
        ORDER BY forecast_time ASC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"county": county})

    if df.empty:
        return None

    ts = pd.to_datetime(df.iloc[0]["forecast_time"], errors="coerce")
    val = pd.to_numeric(df.iloc[0]["avg_aqi"], errors="coerce")
    if pd.isna(ts) or pd.isna(val):
        return None
    return ts, float(val)


def get_top3_stations_latest_forecast() -> pd.DataFrame:
    query = text(
        """
        SELECT f.forecast_time, CAST(f.siteid AS TEXT) AS siteid, s.sitename, s.county, f.predicted_aqi
        FROM public.forecast f
        JOIN public.air_quality_stations s ON CAST(f.siteid AS TEXT) = CAST(s.siteid AS TEXT)
        WHERE f.forecast_time = (
            SELECT MIN(f2.forecast_time) FROM public.forecast f2 WHERE f2.forecast_time > NOW()
        )
        ORDER BY f.predicted_aqi DESC
        LIMIT 3
        """
    )
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn)


def get_county_extreme_rows(threshold: float = 54.5) -> pd.DataFrame:
    query = text(
        """
        SELECT s.county, f.forecast_time, AVG(f.predicted_aqi) AS avg_aqi
        FROM public.forecast f
        JOIN public.air_quality_stations s ON CAST(f.siteid AS TEXT) = CAST(s.siteid AS TEXT)
        WHERE f.forecast_time = (
            SELECT MIN(f2.forecast_time) FROM public.forecast f2 WHERE f2.forecast_time > NOW()
        )
        GROUP BY s.county, f.forecast_time
        HAVING AVG(f.predicted_aqi) >= :threshold
        ORDER BY avg_aqi DESC
        """
    )
    with engine.connect() as conn:
        return pd.read_sql_query(query, conn, params={"threshold": threshold})


def get_subscribers_by_county_with_cooldown(county: str) -> pd.DataFrame:
    query = text(
        """
        SELECT line_user_id, target_county, last_alert_at
                FROM public.user_subscriptions
        WHERE target_county = :county
                    AND (
                                last_alert_at IS NULL
                                OR last_alert_at < (CURRENT_TIMESTAMP - INTERVAL '2 hours')
                            )
        """
    )
    with engine.connect() as conn:
                return pd.read_sql_query(query, conn, params={"county": county})


def update_last_alert_at(line_user_id: str, sent_at: datetime) -> None:
    query = text(
        """
        UPDATE user_subscriptions
        SET last_alert_at = :sent_at,
            updated_at = CURRENT_TIMESTAMP
        WHERE line_user_id = :line_user_id
        """
    )
    with engine.begin() as conn:
        conn.execute(query, {"sent_at": sent_at, "line_user_id": line_user_id})


def send_routine_updates(now: datetime) -> None:
    global _last_routine_marker

    if now.hour not in ROUTINE_HOURS:
        return

    marker = now.strftime("%Y-%m-%d-%H")
    if _last_routine_marker == marker:
        return

    top3 = get_top3_stations_latest_forecast()
    if top3.empty:
        logger.info("Routine update skipped: no forecast data")
        return

    latest_time = pd.to_datetime(top3.iloc[0]["forecast_time"], errors="coerce")
    latest_label = latest_time.strftime("%Y-%m-%d %H:%M") if not pd.isna(latest_time) else "N/A"

    lines = [f"【例行更新 {latest_label}】全台 AQI (空氣品質指標) 預測 Top 3"]
    for idx, row in enumerate(top3.itertuples(index=False), start=1):
        status = get_aqi_status(float(row.predicted_aqi))
        lines.append(f"{idx}. {row.sitename}（{row.county}）: {float(row.predicted_aqi):.1f} | {status}")

    message = "\n".join(lines)

    # Push to all subscribers.
    query = text("SELECT DISTINCT line_user_id FROM user_subscriptions")
    with engine.connect() as conn:
        users_df = pd.read_sql_query(query, conn)

    for row in users_df.itertuples(index=False):
        push_line_message(str(row.line_user_id), message)

    send_to_discord(message)
    send_to_line(message)
    _last_routine_marker = marker
    logger.info("Routine updates sent to %s users", len(users_df))




def scheduler_loop() -> None:
    logger.info("Background scheduler started")
    while True:
        now = datetime.now()
        try:
            send_routine_updates(now)
            # check_extreme_spikes(now)  # 已移除主動警報功能
        except Exception as exc:
            logger.error("Background task error: %s", exc)
        time.sleep(60)


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



@handler.add(MessageEvent, message=TextMessageContent)
def handle_text_message(event: MessageEvent) -> None:
          
    text_msg = event.message.text.strip()
    text_msg_lower = text_msg.lower()
    line_user_id = event.source.user_id
    reply_token = event.reply_token

    # 文字選單指令
    if text_msg in {"全台概況", "全台", "概況"}:
            query = text(
                """
                SELECT s.county, AVG(f.predicted_aqi) AS avg_aqi
                FROM public.forecast f
                JOIN public.air_quality_stations s ON CAST(f.siteid AS TEXT) = CAST(s.siteid AS TEXT)
                GROUP BY s.county
                HAVING COUNT(*) > 0
                ORDER BY avg_aqi ASC
                """
            )
            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn)
            if df.empty or len(df) < 3:
                reply_line_message(reply_token, "目前暫無預測數據，請稍後再試。")
                return
            best = df.nsmallest(3, "avg_aqi")
            worst = df.nlargest(3, "avg_aqi")
            msg = ["【全台概況】"]
            msg.append("空氣最好：")
            for _, row in best.iterrows():
                msg.append(f"{row['county']}：{row['avg_aqi']:.1f}")
            msg.append("空氣最差：")
            for _, row in worst.iterrows():
                msg.append(f"{row['county']}：{row['avg_aqi']:.1f}")
            reply_line_message(reply_token, "\n".join(msg))
            return

    elif text_msg in {"未來趨勢", "趨勢", "未來"}:
            county = get_user_subscription(line_user_id)
            if not county:
                reply_line_message(reply_token, "尚未訂閱，請先傳送縣市名稱（例如：台北、桃園）。")
                return
            query = text(
                """
                SELECT forecast_time, AVG(predicted_aqi) AS avg_aqi
                FROM public.forecast f
                JOIN public.air_quality_stations s ON CAST(f.siteid AS TEXT) = CAST(s.siteid AS TEXT)
                WHERE s.county = :county
                GROUP BY forecast_time
                ORDER BY forecast_time DESC
                LIMIT 6
                """
            )
            with engine.connect() as conn:
                df = pd.read_sql_query(query, conn, params={"county": county})
            if df.empty:
                reply_line_message(reply_token, f"目前無法取得 {county} 的未來趨勢預測。")
                return
            msg = [f"【{county} 未來 6 小時 AQI (空氣品質指標) 預測】"]
            for _, row in df.sort_values("forecast_time").iterrows():
                t = pd.to_datetime(row["forecast_time"]).strftime("%m/%d %H:%M")
                status = get_aqi_status(row["avg_aqi"])
                msg.append(f"{t}：{row['avg_aqi']:.1f} {status}")
            reply_line_message(reply_token, "\n".join(msg))
            return

    elif text_msg in {"目前訂閱", "訂閱", "我的訂閱", "目前訂閱狀態"}:
            county = get_user_subscription(line_user_id)
            if county:
                reply_line_message(reply_token, f"您目前訂閱的縣市為：{county}")
            else:
                reply_line_message(reply_token, "尚未訂閱任何縣市。請直接輸入縣市名稱進行訂閱。")
            return

    elif text_msg in {"幫助", "指令", "功能表", "選單", "功能", "help", "commands", "menu", "options"}:
        help_msg = (
            "【功能說明】\n"
            "官方網頁(電腦版)：https://mina030303-air-quality-monitoring-app-f3ksh1.streamlit.app/\n"
            "１．全台概況：顯示空氣最好/最差縣市\n"
            "２．未來趨勢：查詢訂閱縣市未來 6 小時預測\n"
            "３．目前訂閱：查詢目前訂閱的縣市\n"
            "４．訂閱縣市：直接輸入縣市名稱（如：台北、桃園）\n"
            "５．查詢：查詢目前訂閱縣市的最新預測\n"
            "６．解除訂閱：輸入「取消」\n"
            "７．功能表：輸入「功能表」或「選單」\n"
        )
        reply_line_message(reply_token, help_msg)
        return

    # 歡迎訊息
    if text_msg in {"開始", "start", "hi", "你好", "您好", "hello", "哈囉", "嗨", "歡迎"}:
        welcome_msg = (
            "您好！歡迎使用「全台空氣品質預測小助手」！\n"
            "本機器人能提供 PM2.5 預測，幫助您規劃行程。\n"
            "官方網頁(電腦版)：https://mina030303-air-quality-monitoring-app-f3ksh1.streamlit.app/\n"
            "１．全台概況：顯示空氣最好/最差縣市\n"
            "２．未來趨勢：查詢訂閱縣市未來 6 小時預測\n"
            "３．目前訂閱：查詢目前訂閱的縣市\n"
            "４．訂閱縣市：直接輸入縣市名稱（如：台北、桃園）\n"
            "５．查詢：查詢目前訂閱縣市的最新預測\n"
            "６．解除訂閱：輸入「取消」\n"
            "７．功能表：輸入「功能表」或「選單」\n"
        )
        reply_line_message(reply_token, welcome_msg)
        return

    # PM2.5分級查詢
    if text_msg in {"pm2.5分級", "pm2.5 分級", "pm25分級", "pm25 分級", "pm2.5等級", "pm2.5標準", "pm2.5級距", "pm2.5說明"}:
        pm25_guide = (
            "【PM2.5 分級標準】\n"
            "🟢 0~15.4：良好 (Good)\n"
            "🟡 15.5~35.4：普通 (Moderate)\n"
            "🟠 35.5~54.4：對敏感族群不健康 (Unhealthy for Sensitive Groups)\n"
            "🔴 54.5~150.4：對所有族群不健康 (Unhealthy)\n"
            "🟣 150.5~250.4：非常不健康 (Very Unhealthy)\n"
            "🟤 250.5 以上：有害 (Hazardous)\n"
            "（依據台灣環保署標準）"
        )
        reply_line_message(reply_token, pm25_guide)
        return

    # 功能表（已合併至上方說明指令）


    if not line_user_id:
        return

    # 歧義判斷：「嘉義」或「新竹」
    ambiguous_map = {
        "嘉義": ["嘉義市", "嘉義縣"],
        "新竹": ["新竹市", "新竹縣"],
    }
    if text_msg in ambiguous_map:
        quick_reply = QuickReply(
            items=[
                QuickReplyItem(action=MessageAction(label=opt, text=opt))
                for opt in ambiguous_map[text_msg]
            ]
        )
        reply_line_message(
            reply_token,
            f"請問您是指哪一個 {text_msg}？",
            quick_reply=quick_reply,
        )
        return

    # 解除訂閱
    if text_msg in {"取消", "取消訂閱", "unsubscribe"}:
        remove_user_subscription(line_user_id)
        reply_line_message(
            reply_token,
            "已成功取消訂閱！如需再次接收預測，請輸入縣市名稱重新訂閱。",
        )
        return


    # 訂閱邏輯
    normalized_county = normalize_county_name(text_msg)
    if normalized_county:
        upsert_user_subscription(line_user_id, normalized_county)
        reply_line_message(
            reply_token,
            f"已成功訂閱 {normalized_county}！",
        )
        return

    # 查詢預測
    if text_msg in {"查詢", "查詢預測"} or text_msg_lower == "current prediction":
        county = get_user_subscription(line_user_id)
        if not county:
            reply_line_message(reply_token, "尚未訂閱，請先傳送縣市名稱（例如：台北、桃園）。")
            return

        try:
            result = get_latest_county_avg_pm25(county)
            if result is None:
                reply_line_message(reply_token, f"目前無法取得 {county} 的最新預測資料。")
                return

            forecast_time, avg_aqi = result
            status = get_aqi_status(avg_aqi)
            reply_line_message(
                reply_token,
                (
                    f"{county} 最新預測\n"
                    f"時間：{forecast_time.strftime('%Y-%m-%d %H:%M')}\n"
                    f"AQI (空氣品質指標)：{avg_aqi:.1f}\n"
                    f"分級：{status}"
                ),
            )
        except Exception as exc:
            logger.error("Forecast query failed: %s", exc)
            reply_line_message(reply_token, "查詢失敗，請稍後再試。")
        return

    reply_line_message(reply_token, "請輸入縣市名稱完成訂閱（例如：台北、桃園），或輸入「查詢」。")


def main() -> None:
    ensure_tables()

    scheduler = threading.Thread(target=scheduler_loop, daemon=True)
    scheduler.start()

    app.run(host="0.0.0.0", port=5000)


if __name__ == "__main__":
    main()
