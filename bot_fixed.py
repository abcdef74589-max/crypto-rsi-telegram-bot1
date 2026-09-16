import os
import asyncio
import logging
import json
from pathlib import Path
from datetime import datetime, timezone
import aiohttp

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TOP_N = int(os.getenv("TOP_N", "100"))
PERIOD = int(os.getenv("RSI_PERIOD", "14"))
USERS_FILE = Path("users.json")

BINANCE = "https://api.binance.com"
COINGECKO = "https://api.coingecko.com/api/v3/coins/markets"

TF_ORDER = {
    "15m": 0,
    "1h": 1,
    "4h": 2,
    "1D": 3,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def load_state():
    if not USERS_FILE.exists():
        return {
            "users": [],
            "offset": 0,
            "signals": {}
        }

    try:
        data = json.loads(
            USERS_FILE.read_text(encoding="utf-8")
        )
    except Exception:
        data = {}

    data.setdefault("users", [])
    data.setdefault("offset", 0)
    data.setdefault("signals", {})

    return data


def save_state(state):
    USERS_FILE.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2
        ) + "\n",
        encoding="utf-8"
    )


def calculate_rsi(closes, period=14):
    if len(closes) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, period + 1):
        change = closes[i] - closes[i - 1]

        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        rsi = 100
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    for i in range(period + 1, len(closes)):
        change = closes[i] - closes[i - 1]

        gain = max(change, 0)
        loss = max(-change, 0)

        avg_gain = (
            (avg_gain * (period - 1)) + gain
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) + loss
        ) / period

        if avg_loss == 0:
            rsi = 100
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

    return rsi


def get_active_timeframes(now=None):
    """
    فقط تایم‌فریم‌هایی که در 10 دقیقه پایانی
    کندل فعلی هستند.

    زمان Binance / UTC
    """

    if now is None:
        now = datetime.now(timezone.utc)

    minute = now.minute
    hour = now.hour

    result = []

    # 15m
    # 15:05 تا 15:15
    # 15:20 تا 15:30
    # 15:35 تا 15:45
    # 15:50 تا 16:00
    if minute % 15 >= 5:
        result.append("15m")

    # 1h
    if minute >= 50:
        result.append("1h")

    # 4h
    # 03:50-04:00
    # 07:50-08:00
    # 11:50-12:00
    # 15:50-16:00
    # 19:50-20:00
    # 23:50-00:00
    if hour % 4 == 3 and minute >= 50:
        result.append("4h")

    # 1D
    if hour == 23 and minute >= 50:
        result.append("1D")

    return result


async def http_get(session, url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
                headers={
                    "User-Agent": "RSI-Telegram-Scanner"
                }
            ) as response:

                text = await response.text()

                if response.status == 429:
                    await asyncio.sleep(3)
                    continue

                if response.status >= 400:
                    raise RuntimeError(
                        f"HTTP {response.status}: {text[:300]}"
                    )

                return json.loads(text)

        except Exception:
            if attempt == 2:
                raise

            await asyncio.sleep(2)

    return None


async def telegram_call(
    session,
    method,
    payload=None
):
    url = (
        f"https://api.telegram.org/"
        f"bot{TOKEN}/{method}"
    )

    async with session.post(
        url,
        json=payload or {},
        timeout=aiohttp.ClientTimeout(total=20)
    ) as response:

        text = await response.text()

        if response.status >= 400:
            raise RuntimeError(
                f"Telegram HTTP {response.status}: {text}"
            )

        data = json.loads(text)

        if not data.get("ok"):
            raise RuntimeError(
                f"Telegram API error: {text}"
            )

        return data.get("result")


async def get_top_coins(session):
    data = await http_get(
        session,
        COINGECKO,
        {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": TOP_N,
            "page": 1,
            "sparkline": "false"
        }
    )

    coins = []

    for coin in data or []:
        symbol = str(
            coin.get("symbol", "")
        ).upper()

        name = coin.get(
            "name",
            symbol
        )

        if symbol:
            coins.append(
                {
                    "name": name,
                    "symbol": symbol
                }
            )

    return coins


async def get_klines(
    session,
    symbol,
    interval
):
    return await http_get(
        session,
        f"{BINANCE}/api/v3/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": 200
        }
    )


def format_volume(value):
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"

    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"

    if value >= 1_000:
        return f"{value / 1_000:.2f}K"

    return f"{value:.2f}"


async def scan_symbol(
    session,
    coin,
    tf_name
):
    symbol = coin["symbol"] + "USDT"

    interval = {
        "15m": "15m",
        "1h": "1h",
        "4h": "4h",
        "1D": "1d"
    }[tf_name]

    try:
        rows = await get_klines(
            session,
            symbol,
            interval
        )

    except Exception as e:
        logging.warning(
            "%s %s -> %s",
            symbol,
            tf_name,
            e
        )
        return None

    if not rows or len(rows) < PERIOD + 5:
        return None

    current = rows[-1]

    now_ms = int(
        datetime.now(
            timezone.utc
        ).timestamp() * 1000
    )

    candle_open_ms = int(
        current[0]
    )

    candle_close_ms = int(
        current[6]
    )

    remaining_ms = (
        candle_close_ms - now_ms
    )

    # فقط 10 دقیقه پایانی
    if (
        remaining_ms < 0
        or remaining_ms > 600000
    ):
        return None

    closes = [
        float(row[4])
        for row in rows
    ]

    current_rsi = calculate_rsi(
        closes,
        PERIOD
    )

    if current_rsi is None:
        return None

    # سیگنال
    if current_rsi > 70:
        zone_name = "overbought"
    elif current_rsi < 30:
        zone_name = "oversold"
    else:
        return None

    # حجم کندل فعلی
    current_volume = float(
        current[5]
    )

    # سه کندل قبلی
    previous_volumes = [
        float(rows[-2][5]),
        float(rows[-3][5]),
        float(rows[-4][5])
    ]

    average_volume = (
        sum(previous_volumes) / 3
    )

    if current_volume > (
        average_volume * 1.2
    ):
        volume_status = "زیاد 🔥"

    elif current_volume < (
        average_volume * 0.8
    ):
        volume_status = "کم 📉"

    else:
        volume_status = "معمولی ➖"

    return {
        "name": coin["name"],
        "symbol": symbol,
        "tf": tf_name,
        "rsi": current_rsi,
        "zone": zone_name,
        "candle_open_ms": candle_open_ms,
        "remaining_ms": remaining_ms,
        "current_volume": current_volume,
        "previous_volumes": previous_volumes,
        "volume_status": volume_status
    }


def mark_signal_status(
    alerts,
    state
):
    signals = state.setdefault(
        "signals",
        {}
    )

    now_ms = int(
        datetime.now(
            timezone.utc
        ).timestamp() * 1000
    )

    cutoff = (
        now_ms -
        (2 * 24 * 60 * 60 * 1000)
    )

    # پاک کردن state قدیمی
    cleaned = {}

    for key, value in signals.items():

        if not isinstance(value, dict):
            continue

        candle = int(
            value.get(
                "candle_open_ms",
                0
            )
        )

        if candle >= cutoff:
            cleaned[key] = value

    state["signals"] = cleaned
    signals = state["signals"]

    for alert in alerts:

        key = (
            f"{alert['symbol']}|"
            f"{alert['tf']}|"
            f"{alert['zone']}"
        )

        old = signals.get(key)

        same_candle = (
            old is not None
            and int(
                old.get(
                    "candle_open_ms",
                    -1
                )
            )
            == alert["candle_open_ms"]
        )

        if same_candle:
            alert["new_signal"] = False
        else:
            alert["new_signal"] = True

            signals[key] = {
                "candle_open_ms":
                    alert["candle_open_ms"],

                "last_seen_ms":
                    now_ms
            }

    return alerts


def build_messages(alerts):
    if not alerts:
        return []

    grouped = {}

    for alert in alerts:
        grouped.setdefault(
            alert["tf"],
            []
        ).append(alert)

    messages = []

    for tf in sorted(
        grouped.keys(),
        key=lambda x: TF_ORDER[x]
    ):

        text = (
            "🚨 RSI Scanner Alert\n\n"
        )

        for alert in sorted(
            grouped[tf],
            key=lambda x: x["symbol"]
        ):

            # اولین بار = سبز
            # تکراری = سفید
            signal_circle = (
                "🟢"
                if alert["new_signal"]
                else "⚪"
            )

            # RSI زیر 30 = قرمز
            # RSI بالای 70 = سبز
            rsi_icon = (
                "🔴"
                if alert["rsi"] < 30
                else "🟢"
            )

            base = alert["symbol"].replace(
                "USDT",
                ""
            )

            remaining = max(
                0,
                alert["remaining_ms"] / 60000
            )

            text += (
                f"{signal_circle} "
                f"{alert['name']} "
                f"({alert['symbol']})\n"
            )

            text += (
                f"⏱ TF: {alert['tf']}\n"
            )

            text += (
                f"📊 RSI(14): "
                f"{alert['rsi']:.2f} "
                f"{rsi_icon}\n"
            )

            text += (
                f"⏳ مانده تا بسته‌شدن: "
                f"{remaining:.1f} دقیقه\n"
            )

            text += (
                f"📊 وضعیت حجم: "
                f"{alert['volume_status']}\n"
            )

            text += (
                f"📦 حجم فعلی: "
                f"{format_volume(alert['current_volume'])} "
                f"{base}\n"
            )

            text += (
                f"1️⃣: "
                f"{format_volume(alert['previous_volumes'][0])} "
                f"{base}\n"
            )

            text += (
                f"2️⃣: "
                f"{format_volume(alert['previous_volumes'][1])} "
                f"{base}\n"
            )

            text += (
                f"3️⃣: "
                f"{format_volume(alert['previous_volumes'][2])} "
                f"{base}\n"
            )

            text += (
                f"📈 "
                f"https://www.tradingview.com/"
                f"symbols/{alert['symbol']}/\n\n"
            )

        messages.append(
            text.rstrip()
        )

    return messages


async def send_message(
    session,
    chat_id,
    text
):
    try:
        await telegram_call(
            session,
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True
            }
        )

        logging.info(
            "Telegram message sent to %s",
            chat_id
        )

    except Exception as e:
        logging.error(
            "Telegram send failed for %s: %s",
            chat_id,
            e
        )


async def handle_updates(
    session,
    state
):
    try:
        updates = await telegram_call(
            session,
            "getUpdates",
            {
                "offset": state.get(
                    "offset",
                    0
                ),
                "timeout": 0,
                "allowed_updates": [
                    "message"
                ]
            }
        )

    except Exception as e:
        logging.error(
            "getUpdates failed: %s",
            e
        )
        return False

    changed = False

    for update in updates or []:

        update_id = int(
            update["update_id"]
        )

        state["offset"] = (
            update_id + 1
        )

        message = update.get(
            "message"
        )

        if not message:
            continue

        chat = message.get(
            "chat"
        ) or {}

        chat_id = str(
            chat.get("id", "")
        )

        text = (
            message.get("text")
            or ""
        ).strip().lower()

        if not chat_id:
            continue

        if text.startswith("/start"):

            if chat_id not in state["users"]:
                state["users"].append(
                    chat_id
                )
                changed = True

            # تأیید فعال شدن
            await send_message(
                session,
                chat_id,
                "✅ ربات فعال شد.\n"
                "سیگنال‌های RSI برای شما "
                "ارسال می‌شوند."
            )

        elif text.startswith("/stop"):

            if chat_id in state["users"]:

                state["users"].remove(
                    chat_id
                )

                changed = True

            await send_message(
                session,
                chat_id,
                "⛔ دریافت سیگنال‌ها متوقف شد."
            )

        elif text.startswith("/status"):

            active = (
                chat_id in state["users"]
            )

            await send_message(
                session,
                chat_id,
                (
                    "📡 وضعیت: فعال ✅"
                    if active
                    else
                    "📡 وضعیت: غیرفعال ⛔"
                )
            )

    if changed:
        save_state(state)

    return changed


async def scan_all(
    session,
    state,
    timeframes
):
    if not timeframes:
        logging.info(
            "No active timeframe."
        )
        return

    coins = await get_top_coins(
        session
    )

    logging.info(
        "Top coins: %s",
        len(coins)
    )

    semaphore = asyncio.Semaphore(15)

    async def worker(
        coin,
        tf
    ):
        async with semaphore:
            return await scan_symbol(
                session,
                coin,
                tf
            )

    tasks = []

    for coin in coins:
        for tf in timeframes:
            tasks.append(
                worker(
                    coin,
                    tf
                )
            )

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )

    alerts = []

    for result in results:

        if isinstance(
            result,
            Exception
        ):
            logging.warning(
                "Scan error: %s",
                result
            )
            continue

        if result:
            alerts.append(result)

    alerts.sort(
        key=lambda x: (
            TF_ORDER[x["tf"]],
            x["symbol"]
        )
    )

    alerts = mark_signal_status(
        alerts,
        state
    )

    save_state(state)

    logging.info(
        "Alerts found: %s",
        len(alerts)
    )

    if not alerts:
        return

    messages = build_messages(
        alerts
    )

    # هر تایم‌فریم پیام جدا
    for message in messages:

        for chat_id in list(
            state["users"]
        ):

            await send_message(
                session,
                chat_id,
                message
            )

            # جلوگیری از فشار به Telegram
            await asyncio.sleep(0.05)


async def main():

    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing."
        )

    state = load_state()

    async with aiohttp.ClientSession() as session:

        # اول دستورات Telegram را بخوان
        await handle_updates(
            session,
            state
        )

        # بعد state را دوباره بخوان
        state = load_state()

        if not state["users"]:
            logging.info(
                "No active Telegram users."
            )

            return

        now = datetime.now(
            timezone.utc
        )

        timeframes = get_active_timeframes(
            now
        )

        logging.info(
            "UTC: %s",
            now.isoformat()
        )

        logging.info(
            "Active TF: %s",
            ", ".join(timeframes)
            if timeframes
            else "NONE"
        )

        await scan_all(
            session,
            state,
            timeframes
        )


if __name__ == "__main__":
    asyncio.run(main())
