import os
import json
import math
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp


# =========================================================
# CONFIG
# =========================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TOP_N = 100
RSI_PERIOD = 14

TIMEFRAMES = {
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}

STATE_FILE = "users.json"

BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
BINANCE_TICKERS = "https://api.binance.com/api/v3/ticker/24hr"

COINGECKO_TOP = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd"
    "&order=market_cap_desc"
    "&per_page=100"
    "&page=1"
    "&sparkline=false"
)

POLYMARKET_MARKETS = "https://gamma-api.polymarket.com/markets"


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("rsi-scanner")


# =========================================================
# STATE
# =========================================================

def load_state():
    if not os.path.exists(STATE_FILE):
        return {
            "users": [],
            "offset": 0,
            "signals": {},
        }

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("Invalid state")

        data.setdefault("users", [])
        data.setdefault("offset", 0)
        data.setdefault("signals", {})

        return data

    except Exception as e:
        log.warning("State load failed: %s", e)
        return {
            "users": [],
            "offset": 0,
            "signals": {},
        }


def save_state(state):
    tmp = STATE_FILE + ".tmp"

    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(
            state,
            f,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(tmp, STATE_FILE)


# =========================================================
# TELEGRAM
# =========================================================

async def telegram_request(session, method, payload=None):
    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/{method}"
    )

    try:
        async with session.post(
            url,
            json=payload or {},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:

            text = await response.text()

            if response.status != 200:
                log.error(
                    "Telegram %s: %s",
                    method,
                    text,
                )
                return None

            try:
                return json.loads(text)
            except Exception:
                return None

    except Exception as e:
        log.error(
            "Telegram request error %s: %s",
            method,
            e,
        )
        return None


async def send_message(
    session,
    chat_id,
    text,
    disable_preview=True,
):
    return await telegram_request(
        session,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_preview,
        },
    )


async def delete_message(
    session,
    chat_id,
    message_id,
):
    return await telegram_request(
        session,
        "deleteMessage",
        {
            "chat_id": chat_id,
            "message_id": message_id,
        },
    )


# =========================================================
# TELEGRAM COMMANDS
# =========================================================

async def process_commands(session, state):
    offset = int(state.get("offset", 0))

    result = await telegram_request(
        session,
        "getUpdates",
        {
            "offset": offset,
            "timeout": 0,
            "allowed_updates": ["message"],
        },
    )

    if not result or not result.get("ok"):
        return

    updates = result.get("result", [])

    for update in updates:

        update_id = update.get("update_id")

        if update_id is not None:
            state["offset"] = update_id + 1

        message = update.get("message") or {}
        chat = message.get("chat") or {}

        chat_id = str(chat.get("id", ""))

        text = (message.get("text") or "").strip().lower()

        if not chat_id:
            continue

        if text.startswith("/start"):

            if chat_id not in state["users"]:
                state["users"].append(chat_id)

            await send_message(
                session,
                chat_id,
                "🟢 <b>ربات RSI فعال شد</b>\n\n"
                "اسکن ۱۰۰ ارز برتر هر ۵ دقیقه انجام می‌شود.",
            )

            log.info(
                "User activated: %s",
                chat_id,
            )

        elif text.startswith("/stop"):

            if chat_id in state["users"]:
                state["users"].remove(chat_id)

            await send_message(
                session,
                chat_id,
                "🔴 <b>ربات متوقف شد.</b>",
            )

        elif text.startswith("/status"):

            status = (
                "🟢 فعال"
                if chat_id in state["users"]
                else "🔴 غیرفعال"
            )

            await send_message(
                session,
                chat_id,
                f"وضعیت ربات: <b>{status}</b>",
            )

        elif text.startswith("/test"):

            await send_message(
                session,
                chat_id,
                "✅ پیام تست با موفقیت ارسال شد.",
            )

    save_state(state)


# =========================================================
# TOP 100
# =========================================================

async def get_top_coins(session):

    try:
        async with session.get(
            COINGECKO_TOP,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:

            if response.status != 200:
                log.error(
                    "CoinGecko status: %s",
                    response.status,
                )
                return []

            data = await response.json()

    except Exception as e:
        log.error(
            "CoinGecko error: %s",
            e,
        )
        return []

    coins = []

    for coin in data:

        symbol = (
            coin.get("symbol") or ""
        ).upper()

        if not symbol:
            continue

        # فقط نمادهایی که احتمالاً در Binance USDT دارند
        coins.append(symbol + "USDT")

    return coins[:TOP_N]


# =========================================================
# BINANCE SYMBOLS
# =========================================================

async def get_binance_symbols(session):

    try:
        async with session.get(
            BINANCE_TICKERS,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:

            if response.status != 200:
                return set()

            data = await response.json()

    except Exception as e:
        log.error(
            "Binance ticker error: %s",
            e,
        )
        return set()

    symbols = set()

    for item in data:

        symbol = item.get("symbol", "")

        if symbol.endswith("USDT"):
            symbols.add(symbol)

    return symbols


# =========================================================
# KLINES
# =========================================================

async def get_klines(
    session,
    symbol,
    interval,
    limit=100,
):

    try:
        async with session.get(
            BINANCE_KLINES,
            params={
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
            },
            timeout=aiohttp.ClientTimeout(total=20),
        ) as response:

            if response.status != 200:
                return []

            return await response.json()

    except Exception as e:
        log.debug(
            "Kline error %s %s: %s",
            symbol,
            interval,
            e,
        )
        return []


# =========================================================
# RSI
# =========================================================

def calculate_rsi(closes, period=14):

    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):

        change = closes[i] - closes[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(gains)):
        avg_gain = (
            (avg_gain * (period - 1))
            + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1))
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


# =========================================================
# EMA
# =========================================================

def ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    result = sum(values[:period]) / period

    for value in values[period:]:
        result = (
            (value - result) * multiplier
        ) + result

    return result


# =========================================================
# TECHNICAL / AI-LIKE MODEL
# =========================================================

def technical_score(closes, rsi):

    if len(closes) < 25 or rsi is None:
        return 50.0

    ema9 = ema(closes, 9)
    ema21 = ema(closes, 21)

    if ema9 is None or ema21 is None:
        return 50.0

    score = 50.0

    # EMA trend
    if ema9 > ema21:
        score += 18
    else:
        score -= 18

    # Current price momentum
    recent = closes[-1]
    previous = closes[-2]

    if recent > previous:
        score += 8
    else:
        score -= 8

    # RSI contribution
    if rsi >= 70:
        score += 12
    elif rsi <= 30:
        score -= 12
    elif rsi >= 55:
        score += 7
    elif rsi <= 45:
        score -= 7

    # Short momentum
    if len(closes) >= 6:
        momentum = (
            (closes[-1] - closes[-6])
            / closes[-6]
        ) * 100

        if momentum > 0:
            score += min(momentum * 3, 10)
        elif momentum < 0:
            score += max(momentum * 3, -10)

    return max(0.0, min(100.0, score))


# =========================================================
# VOLUME
# =========================================================

def volume_analysis(klines):

    if len(klines) < 5:
        return None

    # آخر کندل = کندل فعلی
    current_volume = float(
        klines[-1][5]
    )

    # سه کندل بسته شده قبلی
    previous_volumes = [
        float(klines[-2][5]),
        float(klines[-3][5]),
        float(klines[-4][5]),
    ]

    average_volume = (
        sum(previous_volumes)
        / len(previous_volumes)
    )

    if average_volume <= 0:
        return None

    ratio = (
        current_volume
        / average_volume
    )

    if ratio > 1.2:
        status = "🔥 زیاد"
    elif ratio < 0.8:
        status = "📉 کم"
    else:
        status = "➖ معمولی"

    return {
        "ratio": ratio,
        "status": status,
    }


# =========================================================
# CANDLE CLOSE COUNTDOWN
# =========================================================

def timeframe_seconds(interval):

    mapping = {
        "15m": 15 * 60,
        "1h": 60 * 60,
        "4h": 4 * 60 * 60,
        "1d": 24 * 60 * 60,
    }

    return mapping[interval]


def candle_close_countdown(interval):

    now = datetime.now(timezone.utc)

    seconds = int(
        now.timestamp()
    )

    duration = timeframe_seconds(interval)

    next_close = (
        (seconds // duration) + 1
    ) * duration

    remaining = max(
        0,
        next_close - seconds,
    )

    hours = remaining // 3600

    minutes = (
        remaining % 3600
    ) // 60

    secs = remaining % 60

    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    return f"{minutes:02d}:{secs:02d}"


# =========================================================
# POLYMARKET
# =========================================================

def extract_probability(market):

    try:
        prices = market.get(
            "outcomePrices"
        )

        if isinstance(prices, str):
            prices = json.loads(prices)

        if not isinstance(prices, list):
            return None

        if len(prices) < 2:
            return None

        values = [
            float(x)
            for x in prices
        ]

        # معمولاً outcome اول Yes/Up و دوم No/Down
        return values[0] * 100

    except Exception:
        return None


def market_matches(
    market,
    symbol,
    timeframe,
):

    text = " ".join([
        str(market.get("question", "")),
        str(market.get("title", "")),
        str(market.get("description", "")),
    ]).lower()

    coin = symbol.replace(
        "USDT",
        ""
    ).lower()

    if coin not in text:
        return False

    if timeframe == "15m":
        return (
            "15m" in text
            or "15 min" in text
            or "5m" in text
            or "5 min" in text
        )

    if timeframe == "1h":
        return (
            "1h" in text
            or "1 hour" in text
            or "hour" in text
        )

    if timeframe == "4h":
        return (
            "4h" in text
            or "4 hour" in text
        )

    if timeframe == "1d":
        return (
            "1d" in text
            or "1 day" in text
            or "daily" in text
            or "day" in text
        )

    return False


async def get_polymarket_probability(
    session,
    symbol,
    timeframe,
):

    try:
        async with session.get(
            POLYMARKET_MARKETS,
            params={
                "closed": "false",
                "limit": 500,
            },
            timeout=aiohttp.ClientTimeout(total=20),
        ) as response:

            if response.status != 200:
                return None

            markets = await response.json()

    except Exception:
        return None

    if not isinstance(markets, list):
        return None

    candidates = []

    for market in markets:

        if market_matches(
            market,
            symbol,
            timeframe,
        ):
            probability = (
                extract_probability(
                    market
                )
            )

            if probability is not None:
                candidates.append(
                    probability
                )

    if not candidates:
        return None

    return candidates[0]


# =========================================================
# COMBINED PREDICTION
# =========================================================

def combined_prediction(
    ai_score,
    polymarket_up,
):

    if polymarket_up is None:

        final_score = ai_score

    else:

        final_score = (
            ai_score * 0.70
            + polymarket_up * 0.30
        )

    if final_score >= 65:
        direction = "🟢 صعودی"

    elif final_score <= 35:
        direction = "🔴 نزولی"

    else:
        # برای حالت خنثی، نزدیک‌ترین جهت را نشان می‌دهیم
        if final_score >= 50:
            direction = "🟢 صعودی"
        else:
            direction = "🔴 نزولی"

    return direction, final_score


# =========================================================
# SIGNAL STATE
# =========================================================

def signal_key(symbol, timeframe):

    return f"{symbol}:{timeframe}"


def get_zone(rsi):

    if rsi > 70:
        return "high"

    if rsi < 30:
        return "low"

    return "neutral"


def should_send_signal(
    state,
    symbol,
    timeframe,
    rsi,
):

    key = signal_key(
        symbol,
        timeframe,
    )

    signals = state.setdefault(
        "signals",
        {},
    )

    previous_zone = signals.get(
        key,
        "neutral",
    )

    current_zone = get_zone(rsi)

    # هنوز در همان محدوده هستیم
    if current_zone == previous_zone:

        return False

    # خروج از محدوده
    if current_zone == "neutral":

        signals[key] = "neutral"

        return False

    # ورود جدید به محدوده
    signals[key] = current_zone

    return True


# =========================================================
# FORMAT
# =========================================================

def format_percent(value):

    return f"{value:.0f}%"


def build_volume_text(volume):

    if not volume:
        return "📊 حجم: —"

    ratio = volume["ratio"]
    status = volume["status"]

    return (
        f"📊 حجم: {status} "
        f"{ratio:.1f}× میانگین ۳ کندل قبل"
    )


def build_polymarket_text(
    probability,
):

    if probability is None:
        return "🎯 Polymarket: —"

    if probability >= 50:
        return (
            f"🎯 Polymarket: "
            f"{probability:.0f}% صعودی"
        )

    bearish = 100 - probability

    return (
        f"🎯 Polymarket: "
        f"{bearish:.0f}% نزولی"
    )


def build_timeframe_section(
    symbol,
    timeframe,
    rsi,
    ai_score,
    polymarket_probability,
    volume,
):

    if rsi > 70:

        rsi_line = (
            f"🟢 RSI {rsi:.2f} | اشباع خرید"
        )

    else:

        rsi_line = (
            f"🔴 RSI {rsi:.2f} | اشباع فروش"
        )

    direction, final_score = (
        combined_prediction(
            ai_score,
            polymarket_probability,
        )
    )

    ai_direction = (
        "صعودی"
        if ai_score >= 50
        else "نزولی"
    )

    return (
        f"━━━ {timeframe.upper()} ━━━\n"
        f"{rsi_line}\n"
        f"🔮 بعدی: {direction} "
        f"{final_score:.0f}%\n"
        f"🤖 AI: {ai_score:.0f}% "
        f"{ai_direction}\n"
        f"{build_polymarket_text(polymarket_probability)}\n"
        f"{build_volume_text(volume)}\n"
        f"⏳ بسته‌شدن: "
        f"{candle_close_countdown(timeframe)}\n"
        f"📈 <a href=\"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}\">TV</a>"
    )


# =========================================================
# SCAN ONE COIN
# =========================================================

async def scan_coin(
    session,
    symbol,
    state,
):

    sections = []

    for timeframe, interval in TIMEFRAMES.items():

        klines = await get_klines(
            session,
            symbol,
            interval,
            limit=100,
        )

        if len(klines) < RSI_PERIOD + 5:
            continue

        closes = [
            float(k[4])
            for k in klines
        ]

        rsi = calculate_rsi(
            closes,
            RSI_PERIOD,
        )

        if rsi is None:
            continue

        # فقط بالای 70 یا پایین 30
        if not (
            rsi > 70
            or rsi < 30
        ):
            # خروج از محدوده را ثبت کن
            should_send_signal(
                state,
                symbol,
                timeframe,
                rsi,
            )
            continue

        new_signal = should_send_signal(
            state,
            symbol,
            timeframe,
            rsi,
        )

        if not new_signal:
            continue

        ai_score = technical_score(
            closes,
            rsi,
        )

        volume = volume_analysis(
            klines
        )

        polymarket_probability = (
            await get_polymarket_probability(
                session,
                symbol,
                timeframe,
            )
        )

        section = build_timeframe_section(
            symbol,
            timeframe,
            rsi,
            ai_score,
            polymarket_probability,
            volume,
        )

        sections.append(section)

    return sections


# =========================================================
# SCAN ALL
# =========================================================

async def run_scan(
    session,
    state,
):

    top_coins = await get_top_coins(
        session
    )

    if not top_coins:
        log.error(
            "Could not get top coins."
        )
        return

    binance_symbols = (
        await get_binance_symbols(
            session
        )
    )

    coins = [
        symbol
        for symbol in top_coins
        if symbol in binance_symbols
    ]

    log.info(
        "Top coins: %s",
        len(coins),
    )

    for chat_id in list(
        state.get("users", [])
    ):

        all_sections = []

        for symbol in coins:

            try:

                sections = await scan_coin(
                    session,
                    symbol,
                    state,
                )

                if sections:

                    all_sections.append(
                        (
                            symbol,
                            sections,
                        )
                    )

            except Exception as e:

                log.debug(
                    "Scan error %s: %s",
                    symbol,
                    e,
                )

        if not all_sections:
            log.info(
                "No new signals."
            )
            continue

        # هر ارز با بخش خودش
        for symbol, sections in all_sections:

            text = (
                f"💠 <b>{symbol}</b>\n\n"
                + "\n\n".join(sections)
            )

            await send_message(
                session,
                chat_id,
                text,
            )

    save_state(state)


# =========================================================
# 5 MINUTE SCAN
# =========================================================

def should_scan_now():

    now = datetime.now(
        timezone.utc
    )

    # فقط رأس هر 5 دقیقه
    return now.minute % 5 == 0


# =========================================================
# MAIN
# =========================================================

async def main():

    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing."
        )

    state = load_state()

    timeout = aiohttp.ClientTimeout(
        total=40
    )

    connector = aiohttp.TCPConnector(
        limit=20
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
    ) as session:

        # همیشه اول دستورات تلگرام
        await process_commands(
            session,
            state,
        )

        # اگر کاربری فعال نیست، اسکن نکن
        if not state.get("users"):
            log.info(
                "No active users."
            )
            return

        # اسکن فقط هر 5 دقیقه
        if not should_scan_now():
            log.info(
                "Not scan time."
            )
            return

        log.info(
            "Starting RSI scan..."
        )

        await run_scan(
            session,
            state,
        )

        save_state(state)


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        pass

    except Exception as e:
        log.exception(
            "Fatal error: %s",
            e,
        )
        raise
