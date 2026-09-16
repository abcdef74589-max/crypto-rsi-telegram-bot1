ort os
import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta

import aiohttp

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

TOP_N = int(os.getenv("TOP_N", "100"))
PERIOD = int(os.getenv("RSI_PERIOD", "14"))
ALERT_MODE = os.getenv("ALERT_MODE", "changes").lower()

USERS_FILE = "users.json"

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# =========================
# Users
# =========================

def load_users():
    if not os.path.exists(USERS_FILE):
        return {}

    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return {
                str(x): {"active": True}
                for x in data
            }

        return data

    except Exception:
        logging.exception("Could not load users.json")
        return {}


def save_users(users):
    tmp = USERS_FILE + ".tmp"

    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)

    os.replace(tmp, USERS_FILE)


# =========================
# RSI
# =========================

def rsi(values, period=14):
    if len(values) < period + 1:
        return []

    gains = [
        max(values[i] - values[i - 1], 0)
        for i in range(1, period + 1)
    ]

    losses = [
        max(values[i - 1] - values[i], 0)
        for i in range(1, period + 1)
    ]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    result = [None] * period

    if avg_loss == 0:
        result.append(100 if avg_gain > 0 else 50)
    else:
        result.append(
            100 - 100 / (1 + avg_gain / avg_loss)
        )

    for i in range(period + 1, len(values)):
        change = values[i] - values[i - 1]

        gain = max(change, 0)
        loss = max(-change, 0)

        avg_gain = (
            avg_gain * (period - 1) + gain
        ) / period

        avg_loss = (
            avg_loss * (period - 1) + loss
        ) / period

        if avg_loss == 0:
            result.append(100 if avg_gain > 0 else 50)
        else:
            result.append(
                100 - 100 / (
                    1 + avg_gain / avg_loss
                )
            )

    return result


def zone(value):
    if value < 30:
        return "oversold"

    if value > 70:
        return "overbought"

    return "neutral"


# =========================
# HTTP
# =========================

async def get(session, url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
                headers={
                    "User-Agent": "crypto-rsi-telegram-bot/1.0"
                },
            ) as response:

                if response.status == 429:
                    await asyncio.sleep(
                        min(
                            float(
                                response.headers.get(
                                    "Retry-After", "3"
                                )
                            ),
                            15,
                        )
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


async def binance(session, path, params=None):
    last_error = None

    for base in BASES:
        try:
            return await get(
                session,
                base + path,
                params
            )

        except aiohttp.ClientResponseError as e:
            last_error = e

            if e.status not in (
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
                e.status,
            )

    raise last_error


# =========================
# Market data
# =========================

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
            "name": x.get(
                "name",
                x.get("symbol", "")
            ),
            "symbol": x.get(
                "symbol", ""
            ).upper(),
        }
        for x in data
        if x.get("symbol")
    ]


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

    if len(rows) < PERIOD + 3:
        return None

    # حذف کندل در حال تشکیل
    rows = rows[:-1]

    closes = [
        float(x[4])
        for x in rows
    ]

    values = rsi(
        closes,
        PERIOD
    )

    if (
        values[-1] is None
        or values[-2] is None
    ):
        return None

    current_zone = zone(values[-1])
    previous_zone = zone(values[-2])

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

    current_volume = float(
        rows[-1][5]
    )

    previous_volumes = [
        float(rows[-i][5])
        for i in (2, 3, 4)
    ]

    average_volume = (
        sum(previous_volumes) / 3
    )

    if current_volume > average_volume * 1.2:
        volume_status = "زیاد 🔥"

    elif current_volume < average_volume * 0.8:
        volume_status = "کم 📉"

    else:
        volume_status = "معمولی ➖"

    return {
        "tf": tf,
        "rsi": values[-1],
        "volume": current_volume,
        "prev3_vol": previous_volumes,
        "vol_state": volume_status,
    }


async def scan_coin(session, coin, semaphore):
    symbol = coin["symbol"] + "USDT"

    async def run(tf):
        async with semaphore:
            try:
                result = await one_tf(
                    session,
                    symbol,
                    tf
                )

                if result:
                    result.update(
                        name=coin["name"],
                        symbol=symbol,
                    )

                return result

            except aiohttp.ClientResponseError as e:

                if e.status not in (
                    400,
                    404,
                ):
                    logging.warning(
                        "%s %s HTTP %s",
                        symbol,
                        tf,
                        e.status,
                    )

                return None

            except Exception as e:
                logging.warning(
                    "%s %s: %s",
                    symbol,
                    tf,
                    e,
                )
                return None

    results = await asyncio.gather(
        *(run(tf) for tf in TFS.values())
    )

    return [
        x for x in results
        if x
    ]


# =========================
# Message
# =========================

def fmt_volume(value):
    if value >= 1e9:
        return f"{value / 1e9:.2f}B"

    if value >= 1e6:
        return f"{value / 1e6:.2f}M"

    if value >= 1e3:
        return f"{value / 1e3:.2f}K"

    return f"{value:.2f}"


def messages(alerts):
    if not alerts:
        return []

    output = []

    for alert in sorted(
        alerts,
        key=lambda x: (
            x["symbol"],
            x["tf"]
        ),
    ):
        emoji = (
            "🔴"
            if alert["rsi"] < 30
            else "🟢"
        )

        base = alert["symbol"].replace(
            "USDT",
            ""
        )

        text = "🚨 RSI Scanner Alert\n\n"

        text += (
            f"{emoji} "
            f"{alert['name']} "
            f"({alert['symbol']})\n"
        )

        text += (
            f"⏱ TF: {alert['tf']}\n"
        )

        text += (
            f"📊 RSI(14): "
            f"{alert['rsi']:.2f}\n"
        )

        text += (
            f"📊 وضعیت حجم: "
            f"{alert['vol_state']}\n"
        )

        v = alert["prev3_vol"]

        text += (
            f"📦 حجم: "
            f"فعلی {fmt_volume(alert['volume'])} {base}"
            f" | ۱ قبل {fmt_volume(v[0])}"
            f" | ۲ قبل {fmt_volume(v[1])}"
            f" | ۳ قبل {fmt_volume(v[2])}\n"
        )

        text += (
            f"📈 "
            f"https://www.tradingview.com/"
            f"symbols/{alert['symbol']}/\n\n"
        )

        output.append(text)

    # تقسیم پیام‌های طولانی
    final_messages = []
    current = ""

    for text in output:

        if len(current) + len(text) > 3900:
            if current:
                final_messages.append(current)

            current = text

        else:
            current += text

    if current:
        final_messages.append(current)

    return final_messages


# =========================
# Telegram
# =========================

async def telegram_request(
    session,
    method,
    data=None,
):
    url = (
        f"https://api.telegram.org/"
        f"bot{TOKEN}/{method}"
    )

    async with session.post(
        url,
        json=data or {},
        timeout=aiohttp.ClientTimeout(
            total=20
        ),
    ) as response:

        result = await response.json()

        if response.status >= 400:
            raise RuntimeError(
                f"Telegram HTTP "
                f"{response.status}: "
                f"{result}"
            )

        return result


async def send_message(
    session,
    chat_id,
    text,
):
    await telegram_request(
        session,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        },
    )


# =========================
# Telegram commands
# =========================

async def handle_updates(
    session,
    users,
    offset,
):
    result = await telegram_request(
        session,
        "getUpdates",
        {
            "offset": offset,
            "timeout": 0,
        },
    )

    updates = result.get(
        "result",
        []
    )

    for update in updates:

        offset = update["update_id"] + 1

        message = update.get(
            "message"
        )

        if not message:
            continue

        chat = message.get(
            "chat",
            {}
        )

        chat_id = str(
            chat.get("id")
        )

        text = (
            message.get("text", "")
            .strip()
            .lower()
        )

        if not chat_id:
            continue

        if text.startswith("/start"):

            users[chat_id] = {
                "active": True
            }

            save_users(users)

            await send_message(
                session,
                chat_id,
                "✅ ربات برای شما فعال شد.\n\n"
                "از این به بعد هشدارهای RSI "
                "را دریافت می‌کنید.\n\n"
                "دستورات:\n"
                "/status - وضعیت\n"
                "/stop - توقف هشدارها\n"
                "/start - فعال‌سازی دوباره",
            )

        elif text.startswith("/stop"):

            if chat_id in users:
                users[chat_id]["active"] = False
                save_users(users)

            await send_message(
                session,
                chat_id,
                "⛔ دریافت هشدارها برای شما متوقف شد."
            )

        elif text.startswith("/status"):

            active = (
                chat_id in users
                and users[chat_id].get(
                    "active",
                    True
                )
            )

            status = (
                "فعال 🟢"
                if active
                else "غیرفعال 🔴"
            )

            await send_message(
                session,
                chat_id,
                f"📡 وضعیت ربات: {status}"
            )

    return offset


# =========================
# Main
# =========================

async def main():

    if not TOKEN:
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN "
            "GitHub Secret."
        )

    users = load_users()

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=30
        )
    ) as session:

        # دریافت دستورات کاربران
        offset = 0

        try:
            offset = await handle_updates(
                session,
                users,
                offset,
            )
        except Exception:
            logging.exception(
                "Telegram update error"
            )

        coins = await top_coins(
            session
        )

        logging.info(
            "Top coins: %s",
            len(coins)
        )

        semaphore = asyncio.Semaphore(12)

        groups = await asyncio.gather(
            *(
                scan_coin(
                    session,
                    coin,
                    semaphore
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
            "Alerts: %s",
            len(alerts)
        )

        if not alerts:
            logging.info(
                "No new RSI zone-entry alerts."
            )
            return

        active_users = [
            user_id
            for user_id, info
            in users.items()
            if info.get(
                "active",
                True
            )
        ]

        if not active_users:
            logging.info(
                "No active users."
            )
            return

        for message in messages(alerts):

            for user_id in active_users:

                try:
                    await send_message(
                        session,
                        user_id,
                        message,
                    )

                except Exception:
                    logging.exception(
                        "Failed to send to %s",
                        user_id,
                    )


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception:
        logging.exception('FATAL ERROR')
        raise
        
