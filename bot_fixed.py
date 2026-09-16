import os
import asyncio
import logging
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

import aiohttp


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
LEGACY_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TOP_N = int(os.getenv("TOP_N", "100"))
PERIOD = int(os.getenv("RSI_PERIOD", "14"))
ALERT_MODE = os.getenv("ALERT_MODE", "changes").lower()

USERS_FILE = Path(os.getenv("USERS_FILE", "users.json"))

CG = "https://api.coingecko.com/api/v3/coins/markets"

BASES = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api.binance.com",
]

TFS = {
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1D": "1d",
}

TF_ORDER = {
    "15m": 0,
    "1h": 1,
    "4h": 2,
    "1D": 3,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ---------------------------------------------------------
# RSI
# ---------------------------------------------------------

def rsi(values, period=14):
    if len(values) < period + 1:
        return []

    gains = []
    losses = []

    for i in range(1, period + 1):
        change = values[i] - values[i - 1]

        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    result = [None] * period

    if avg_loss == 0:
        first_rsi = 100 if avg_gain > 0 else 50
    else:
        rs = avg_gain / avg_loss
        first_rsi = 100 - (100 / (1 + rs))

    result.append(first_rsi)

    for i in range(period + 1, len(values)):
        change = values[i] - values[i - 1]

        gain = max(change, 0)
        loss = max(-change, 0)

        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

        if avg_loss == 0:
            current_rsi = 100 if avg_gain > 0 else 50
        else:
            rs = avg_gain / avg_loss
            current_rsi = 100 - (100 / (1 + rs))

        result.append(current_rsi)

    return result


def zone(value):
    if value < 30:
        return "oversold"

    if value > 70:
        return "overbought"

    return "neutral"


# ---------------------------------------------------------
# HTTP
# ---------------------------------------------------------

async def get(session, url, params=None):

    for attempt in range(3):

        try:

            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
                headers={
                    "User-Agent": "crypto-rsi-telegram-bot/3.0"
                },
            ) as response:

                if response.status == 429:

                    retry_after = float(
                        response.headers.get(
                            "Retry-After",
                            "3"
                        )
                    )

                    await asyncio.sleep(
                        min(retry_after, 15)
                    )

                    continue

                if response.status >= 400:

                    text = await response.text()

                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status,
                        message=text[:300],
                        headers=response.headers,
                    )

                return await response.json()

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ):

            if attempt == 2:
                raise

            await asyncio.sleep(
                1.5 * (attempt + 1)
            )

    return None


async def post_json(session, url, payload):

    async with session.post(
        url,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=20),
    ) as response:

        text = await response.text()

        if response.status >= 400:

            raise RuntimeError(
                f"Telegram HTTP {response.status}: "
                f"{text[:500]}"
            )

        data = json.loads(text)

        if not data.get("ok"):

            raise RuntimeError(
                f"Telegram API error: {text[:500]}"
            )

        return data.get("result")


async def telegram(session, method, payload=None):

    return await post_json(
        session,
        f"https://api.telegram.org/bot{TOKEN}/{method}",
        payload or {},
    )


# ---------------------------------------------------------
# Binance
# ---------------------------------------------------------

async def binance(session, path, params=None):

    last_error = None

    for base in BASES:

        try:

            return await get(
                session,
                base + path,
                params,
            )

        except aiohttp.ClientResponseError as error:

            last_error = error

            if error.status not in (
                400,
                403,
                418,
                429,
                451,
            ):
                raise

            logging.warning(
                "%s returned HTTP %s; trying next endpoint",
                base,
                error.status,
            )

    raise last_error


# ---------------------------------------------------------
# Top 100
# ---------------------------------------------------------

async def top_coins(session):

    data = await get(
        session,
        CG,
        {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": TOP_N,
            "page": 1,
            "sparkline": "false",
        },
    )

    return [
        {
            "name": item.get(
                "name",
                item.get("symbol", ""),
            ),
            "symbol": item.get(
                "symbol",
                "",
            ).upper(),
        }
        for item in data
        if item.get("symbol")
    ]


async def tickers(session):

    data = await binance(
        session,
        "/api/v3/ticker/24hr",
    )

    return {
        item["symbol"]: item
        for item in data
        if item.get("symbol")
    }


# ---------------------------------------------------------
# One timeframe
# ---------------------------------------------------------

async def one_tf(session, symbol, tf):

    rows = await binance(
        session,
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": tf,
            "limit": 200,
        },
    )

    if len(rows) < PERIOD + 5:
        return None

    # -----------------------------------------------------
    # مهم:
    # آخرین کندل Binance ممکن است هنوز باز باشد.
    # بنابراین همیشه حذف می‌شود.
    # -----------------------------------------------------

    rows = rows[:-1]

    closes = [
        float(row[4])
        for row in rows
    ]

    values = rsi(
        closes,
        PERIOD,
    )

    if (
        not values
        or values[-1] is None
        or values[-2] is None
    ):
        return None

    current_rsi = values[-1]
    previous_rsi = values[-2]

    current_zone = zone(current_rsi)
    previous_zone = zone(previous_rsi)

    # فقط ورود جدید به ناحیه هشدار
    if current_zone not in (
        "oversold",
        "overbought",
    ):
        return None

    if (
        ALERT_MODE != "always"
        and current_zone == previous_zone
    ):
        return None

    # -----------------------------------------------------
    # Volume
    # -----------------------------------------------------

    current_volume = float(
        rows[-1][5]
    )

    previous_3_volume = [
        float(rows[-2][5]),
        float(rows[-3][5]),
        float(rows[-4][5]),
    ]

    average_previous_3 = (
        sum(previous_3_volume) / 3
    )

    if current_volume > (
        average_previous_3 * 1.2
    ):

        volume_state = "زیاد 🔥"

    elif current_volume < (
        average_previous_3 * 0.8
    ):

        volume_state = "کم 📉"

    else:

        volume_state = "معمولی ➖"

    return {
        "tf": tf,
        "rsi": current_rsi,
        "prev": previous_rsi,

        "volume": current_volume,

        "prev3_vol": previous_3_volume,

        "vol_state": volume_state,

        # زمان بسته شدن کندل
        "close_ms": int(rows[-1][6]),
    }


# ---------------------------------------------------------
# Scan coin
# ---------------------------------------------------------

async def scan_coin(
    session,
    coin,
    ticker,
    semaphore,
):

    symbol = coin["symbol"] + "USDT"

    if symbol not in ticker:
        return []

    async def run_tf(tf_name, tf_value):

        async with semaphore:

            try:

                result = await one_tf(
                    session,
                    symbol,
                    tf_value,
                )

                if result:

                    result.update(
                        {
                            "name": coin["name"],
                            "symbol": symbol,
                            "tf": tf_name,
                        }
                    )

                return result

            except aiohttp.ClientResponseError as error:

                if error.status not in (
                    400,
                    404,
                ):

                    logging.warning(
                        "%s %s HTTP %s",
                        symbol,
                        tf_name,
                        error.status,
                    )

                return None

            except Exception as error:

                logging.warning(
                    "%s %s: %s",
                    symbol,
                    tf_name,
                    error,
                )

                return None

    results = await asyncio.gather(
        *(
            run_tf(tf_name, tf_value)
            for tf_name, tf_value
            in TFS.items()
        )
    )

    return [
        item
        for item in results
        if item
    ]


# ---------------------------------------------------------
# Volume formatting
# ---------------------------------------------------------

def fmtv(value):

    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"

    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"

    if value >= 1_000:
        return f"{value / 1_000:.2f}K"

    return f"{value:.2f}"


# ---------------------------------------------------------
# User state
# ---------------------------------------------------------

def load_state():

    if not USERS_FILE.exists():

        return {
            "users": [],
            "offset": 0,
        }

    try:

        data = json.loads(
            USERS_FILE.read_text(
                encoding="utf-8"
            )
        )

        return {
            "users": [
                str(x)
                for x in data.get(
                    "users",
                    [],
                )
            ],
            "offset": int(
                data.get(
                    "offset",
                    0,
                )
            ),
        }

    except Exception:

        logging.warning(
            "Could not read %s; "
            "starting with empty state.",
            USERS_FILE,
        )

        return {
            "users": [],
            "offset": 0,
        }


def save_state(state):

    USERS_FILE.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------
# Telegram commands
# ---------------------------------------------------------

async def process_commands(
    session,
    state,
):

    offset = state.get(
        "offset",
        0,
    )

    updates = await telegram(
        session,
        "getUpdates",
        {
            "offset": offset,
            "timeout": 0,
            "allowed_updates": [
                "message"
            ],
        },
    )

    changed = False

    max_update_id = offset

    for update in updates or []:

        update_id = int(
            update["update_id"]
        )

        max_update_id = max(
            max_update_id,
            update_id + 1,
        )

        message = (
            update.get("message")
            or {}
        )

        chat = (
            message.get("chat")
            or {}
        )

        chat_id = str(
            chat.get("id", "")
        )

        if not chat_id:
            continue

        raw_text = (
            message.get("text")
            or ""
        ).strip()

        if not raw_text:
            continue

        command = (
            raw_text.lower()
            .split()[0]
        )

        # -----------------------------
        # /start
        # -----------------------------

        if command.startswith("/start"):

            if chat_id not in state["users"]:

                state["users"].append(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text":
                            "✅ ربات فعال شد.\n"
                            "از این به بعد هشدارهای RSI "
                            "را دریافت می‌کنید.",
                    },
                )

            else:

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text":
                            "ℹ️ شما از قبل فعال هستید.",
                    },
                )

        # -----------------------------
        # /stop
        # -----------------------------

        elif command.startswith("/stop"):

            if chat_id in state["users"]:

                state["users"].remove(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text":
                            "⛔ هشدارها متوقف شد.\n"
                            "برای فعال‌سازی دوباره "
                            "/start را بزنید.",
                    },
                )

        # -----------------------------
        # /status
        # -----------------------------

        elif command.startswith("/status"):

            status = (
                "فعال ✅"
                if chat_id in state["users"]
                else "غیرفعال ⛔"
            )

            await telegram(
                session,
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text":
                        f"📡 وضعیت اشتراک هشدار: "
                        f"{status}",
                },
            )

    if updates:

        state["offset"] = max_update_id

        changed = True

    # پشتیبانی از Chat ID قدیمی
    if (
        LEGACY_CHAT_ID
        and LEGACY_CHAT_ID not in state["users"]
    ):

        state["users"].append(
            LEGACY_CHAT_ID
        )

        changed = True

        logging.info(
            "Added TELEGRAM_CHAT_ID "
            "to subscriber list."
        )

    if changed:
        save_state(state)

    return state


# ---------------------------------------------------------
# Messages
# ---------------------------------------------------------

def messages(alerts):

    if not alerts:
        return []

    grouped = {}

    for alert in alerts:

        grouped.setdefault(
            alert["tf"],
            [],
        ).append(alert)

    result = []

    for tf in sorted(
        grouped,
        key=lambda x: TF_ORDER.get(
            x,
            99,
        ),
    ):

        message = (
            "🚨 RSI Scanner Alert\n\n"
        )

        for alert in sorted(
            grouped[tf],
            key=lambda x: x["symbol"],
        ):

            if alert["rsi"] < 30:
                emoji = "🔴"
            else:
                emoji = "🟢"

            base = alert[
                "symbol"
            ].replace(
                "USDT",
                "",
            )

            message += (
                f"{emoji} "
                f"{alert['name']} "
                f"({alert['symbol']})\n"
            )

            message += (
                f"⏱ TF: {alert['tf']}\n"
            )

            message += (
                f"📊 RSI(14): "
                f"{alert['rsi']:.2f}\n"
            )

            message += (
                f"📊 وضعیت حجم: "
                f"{alert['vol_state']}\n"
            )

            message += (
                f"📦 حجم فعلی: "
                f"{fmtv(alert['volume'])} "
                f"{base}\n"
            )

            message += (
                f"1️⃣: "
                f"{fmtv(alert['prev3_vol'][0])} "
                f"{base}\n"
            )

            message += (
                f"2️⃣: "
                f"{fmtv(alert['prev3_vol'][1])} "
                f"{base}\n"
            )

            message += (
                f"3️⃣: "
                f"{fmtv(alert['prev3_vol'][2])} "
                f"{base}\n"
            )

            message += (
                "📈 "
                f"https://www.tradingview.com/"
                f"symbols/{alert['symbol']}/\n\n"
            )

        # Telegram limit
        while len(message) > 3900:

            cut = (
                message.rfind(
                    "\n\n",
                    0,
                    3900,
                )
                or 3900
            )

            result.append(
                message[:cut]
            )

            message = (
                "🚨 RSI Scanner Alert\n\n"
                + message[
                    cut:
                ].lstrip()
            )

        if message.strip() != (
            "🚨 RSI Scanner Alert"
        ):

            result.append(message)

    return result


# ---------------------------------------------------------
# Send message to all users
# ---------------------------------------------------------

async def send_to_all(
    session,
    state,
    text,
):

    users = list(
        dict.fromkeys(
            state.get(
                "users",
                [],
            )
        )
    )

    for chat_id in users:

        try:

            await telegram(
                session,
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                },
            )

        except Exception as error:

            logging.warning(
                "Could not send to %s: %s",
                chat_id,
                error,
            )


# ---------------------------------------------------------
# Manual run timing
# ---------------------------------------------------------

async def wait_for_next_15m_boundary():

    # زمان‌بندی GitHub نیاز به انتظار ندارد.
    # فقط اجرای دستی تا بسته‌شدن کندل بعدی صبر می‌کند.

    if os.getenv(
        "GITHUB_EVENT_NAME",
        "",
    ) != "workflow_dispatch":

        return

    now = datetime.now(
        timezone.utc
    )

    next_minute = (
        (now.minute // 15) + 1
    ) * 15

    if next_minute >= 60:

        target = (
            now.replace(
                minute=0,
                second=0,
                microsecond=0,
            )
            + timedelta(hours=1)
        )

    else:

        target = now.replace(
            minute=next_minute,
            second=0,
            microsecond=0,
        )

    wait_seconds = max(
        0,
        (
            target - now
        ).total_seconds(),
    )

    logging.info(
        "Manual run started."
    )

    logging.info(
        "Waiting %.1f seconds "
        "until next 15m candle close.",
        wait_seconds,
    )

    if wait_seconds > 0:

        await asyncio.sleep(
            wait_seconds
        )

    # حاشیه کوچک برای نهایی‌شدن Binance
    await asyncio.sleep(2)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

async def main():

    if not TOKEN:

        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN "
            "GitHub Secret."
        )

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=30
        )
    ) as session:

        # اول کاربران و دستورات را بررسی می‌کنیم
        state = load_state()

        state = await process_commands(
            session,
            state,
        )

        # اجرای دستی:
        # تا بسته‌شدن کندل بعدی صبر کن
        await wait_for_next_15m_boundary()

        # Top 100
        coins = await top_coins(
            session
        )

        logging.info(
            "Top coins: %s",
            len(coins),
        )

        # Binance symbols
        ticker = await tickers(
            session
        )

        logging.info(
            "Binance tickers: %s",
            len(ticker),
        )

        semaphore = asyncio.Semaphore(
            12
        )

        groups = await asyncio.gather(
            *(
                scan_coin(
                    session,
                    coin,
                    ticker,
                    semaphore,
                )
                for coin in coins
            )
        )

        alerts = [
            alert
            for group in groups
            for alert in group
        ]

        logging.info(
            "Alerts: %s | Subscribers: %s",
            len(alerts),
            len(state["users"]),
        )

        if not state["users"]:

            logging.info(
                "No subscribers yet. "
                "Send /start to the bot."
            )

        # هر TF پیام جداگانه
        for message in messages(
            alerts
        ):

            await send_to_all(
                session,
                state,
                message,
            )

        if not alerts:

            logging.info(
                "No new RSI zone-entry alerts."
            )


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except Exception:

        logging.exception(
            "FATAL ERROR"
        )

        raise
