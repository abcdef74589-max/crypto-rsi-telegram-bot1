import os
import asyncio
import aiohttp
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from statistics import mean
from html import escape


# ============================================================
# CONFIG
# ============================================================

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
LEGACY_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TOP_N = int(os.getenv("TOP_N", "100"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))

# Run continuously for almost 5 minutes.
MONITOR_SECONDS = 285

# Scan once every 60 seconds.
# 100 coins × 4 timeframes = 400 kline requests per scan.
# 60 seconds keeps the Binance request rate much safer.
SCAN_INTERVAL_SECONDS = 60

USERS_FILE = Path(os.getenv("USERS_FILE", "users.json"))

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

# IMPORTANT:
# This endpoint is Binance public market-data service.
# It avoids the HTTP 451 problem from api.binance.com.
BINANCE_BASE_URL = os.getenv(
    "BINANCE_BASE_URL",
    "https://data-api.binance.vision"
).rstrip("/")

TELEGRAM_API = "https://api.telegram.org/bot"

TIMEFRAMES = {
    "15M": "15m",
    "1H": "1h",
    "4H": "4h",
    "1D": "1d",
}

TF_ORDER = {
    "15M": 0,
    "1H": 1,
    "4H": 2,
    "1D": 3,
}

# Maximum simultaneous Binance requests.
BINANCE_CONCURRENCY = 10


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("RSI-Scanner")


# ============================================================
# HTTP
# ============================================================

async def http_get(session, url, params=None, retries=3):
    last_error = None

    for attempt in range(retries):
        try:
            timeout = aiohttp.ClientTimeout(total=25)

            async with session.get(
                url,
                params=params,
                timeout=timeout,
                headers={
                    "User-Agent": "crypto-rsi-telegram-bot/5.0"
                }
            ) as response:

                text = await response.text()

                if response.status == 429:
                    retry_after = response.headers.get(
                        "Retry-After",
                        "3"
                    )

                    try:
                        delay = min(float(retry_after), 15)
                    except Exception:
                        delay = 3

                    logger.warning(
                        "HTTP 429 from %s - waiting %.1fs",
                        url,
                        delay
                    )

                    await asyncio.sleep(delay)
                    continue

                if response.status >= 400:
                    logger.warning(
                        "HTTP %s from %s: %s",
                        response.status,
                        url,
                        text[:400]
                    )

                    raise RuntimeError(
                        f"HTTP {response.status}: {text[:400]}"
                    )

                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    raise RuntimeError(
                        f"Invalid JSON from {url}: {text[:300]}"
                    )

        except Exception as exc:
            last_error = exc

            if attempt < retries - 1:
                await asyncio.sleep(1.5 * (attempt + 1))
            else:
                raise last_error

    raise last_error


async def telegram_request(session, method, payload=None):
    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing."
        )

    url = f"{TELEGRAM_API}{TOKEN}/{method}"

    timeout = aiohttp.ClientTimeout(total=25)

    async with session.post(
        url,
        json=payload or {},
        timeout=timeout
    ) as response:

        text = await response.text()

        if response.status >= 400:
            raise RuntimeError(
                f"Telegram HTTP {response.status}: {text[:500]}"
            )

        try:
            data = json.loads(text)
        except Exception:
            raise RuntimeError(
                f"Telegram invalid JSON: {text[:500]}"
            )

        if not data.get("ok"):
            raise RuntimeError(
                f"Telegram API error: {text[:500]}"
            )

        return data.get("result")


async def send_telegram(session, chat_id, text):
    try:
        await telegram_request(
            session,
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": True,
            }
        )

        logger.info(
            "Telegram message sent to %s",
            chat_id
        )

        return True

    except Exception as exc:
        logger.error(
            "Telegram send failed to %s: %s",
            chat_id,
            exc
        )

        return False


# ============================================================
# STATE
# ============================================================

def default_state():
    return {
        "users": [],
        "offset": 0,
        "signals": {}
    }


def load_state():
    if not USERS_FILE.exists():
        logger.info(
            "%s does not exist. Creating new state.",
            USERS_FILE
        )
        return default_state()

    try:
        data = json.loads(
            USERS_FILE.read_text(
                encoding="utf-8"
            )
        )

        if not isinstance(data, dict):
            return default_state()

        data.setdefault("users", [])
        data.setdefault("offset", 0)
        data.setdefault("signals", {})

        data["users"] = [
            str(x)
            for x in data["users"]
        ]

        return data

    except Exception as exc:
        logger.warning(
            "Could not load state: %s",
            exc
        )

        return default_state()


def save_state(state):
    USERS_FILE.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2
        ) + "\n",
        encoding="utf-8"
    )


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def process_commands(session, state):
    offset = int(state.get("offset", 0))

    try:
        updates = await telegram_request(
            session,
            "getUpdates",
            {
                "offset": offset,
                "timeout": 0,
                "allowed_updates": ["message"]
            }
        )
    except Exception as exc:
        logger.warning(
            "getUpdates failed: %s",
            exc
        )
        return state

    max_update_id = offset
    changed = False

    for update in updates or []:

        update_id = int(
            update.get("update_id", 0)
        )

        max_update_id = max(
            max_update_id,
            update_id + 1
        )

        message = update.get("message") or {}
        chat = message.get("chat") or {}

        chat_id = str(
            chat.get("id", "")
        )

        if not chat_id:
            continue

        text = (
            message.get("text") or ""
        ).strip().lower()

        if not text:
            continue

        command = text.split()[0]

        # -----------------------------
        # /start
        # -----------------------------

        if command.startswith("/start"):

            if chat_id not in state["users"]:

                state["users"].append(
                    chat_id
                )

                changed = True

                await send_telegram(
                    session,
                    chat_id,
                    "✅ ربات فعال شد.\n\n"
                    "از این به بعد هشدارهای RSI را دریافت می‌کنید."
                )

                logger.info(
                    "User activated: %s",
                    chat_id
                )

            else:

                await send_telegram(
                    session,
                    chat_id,
                    "ℹ️ شما از قبل فعال هستید."
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

                await send_telegram(
                    session,
                    chat_id,
                    "⛔ هشدارها متوقف شد.\n\n"
                    "برای فعال‌سازی دوباره /start را بزنید."
                )

                logger.info(
                    "User deactivated: %s",
                    chat_id
                )

        # -----------------------------
        # /status
        # -----------------------------

        elif command.startswith("/status"):

            status = (
                "فعال ✅"
                if chat_id in state["users"]
                else
                "غیرفعال ⛔"
            )

            await send_telegram(
                session,
                chat_id,
                f"📡 وضعیت اشتراک هشدار: {status}"
            )

    if updates:
        state["offset"] = max_update_id
        changed = True

    # Optional legacy chat ID.
    if (
        LEGACY_CHAT_ID
        and LEGACY_CHAT_ID not in state["users"]
    ):
        state["users"].append(
            LEGACY_CHAT_ID
        )

        changed = True

        logger.info(
            "Added TELEGRAM_CHAT_ID to subscribers."
        )

    if changed:
        save_state(state)

    return state


# ============================================================
# COINGECKO TOP 100
# ============================================================

async def get_top_coins(session):
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_N,
        "page": 1,
        "sparkline": "false"
    }

    data = await http_get(
        session,
        COINGECKO_URL,
        params
    )

    coins = []

    for item in data:

        symbol = (
            item.get("symbol") or ""
        ).upper()

        name = (
            item.get("name")
            or symbol
        )

        if not symbol:
            continue

        coins.append({
            "name": name,
            "symbol": symbol
        })

    return coins[:TOP_N]


# ============================================================
# BINANCE
# ============================================================

async def get_binance_exchange_info(session):
    url = (
        f"{BINANCE_BASE_URL}"
        "/api/v3/exchangeInfo"
    )

    return await http_get(
        session,
        url
    )


async def get_binance_tickers(session):
    url = (
        f"{BINANCE_BASE_URL}"
        "/api/v3/ticker/24hr"
    )

    return await http_get(
        session,
        url
    )


def build_valid_symbols(exchange_info):
    valid = set()

    for item in (
        exchange_info.get("symbols", [])
        if isinstance(exchange_info, dict)
        else []
    ):

        symbol = item.get("symbol")

        status = item.get("status")
        quote = item.get("quoteAsset")

        if (
            symbol
            and status == "TRADING"
            and quote == "USDT"
        ):
            valid.add(symbol)

    return valid


def build_ticker_map(ticker_data):
    result = {}

    for item in ticker_data or []:

        symbol = item.get("symbol")

        if symbol:
            result[symbol] = item

    return result


async def get_klines(
    session,
    symbol,
    interval,
    limit=100
):
    url = (
        f"{BINANCE_BASE_URL}"
        "/api/v3/klines"
    )

    return await http_get(
        session,
        url,
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
    )


# ============================================================
# RSI
# ============================================================

def calculate_rsi(values, period=14):
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, period + 1):

        change = (
            values[i]
            - values[i - 1]
        )

        gains.append(
            max(change, 0)
        )

        losses.append(
            max(-change, 0)
        )

    average_gain = sum(gains) / period
    average_loss = sum(losses) / period

    if average_loss == 0:

        if average_gain == 0:
            return 50.0

        return 100.0

    rs = (
        average_gain
        / average_loss
    )

    rsi_value = (
        100
        - (100 / (1 + rs))
    )

    for i in range(
        period + 1,
        len(values)
    ):

        change = (
            values[i]
            - values[i - 1]
        )

        gain = max(
            change,
            0
        )

        loss = max(
            -change,
            0
        )

        average_gain = (
            (
                average_gain
                * (period - 1)
            )
            + gain
        ) / period

        average_loss = (
            (
                average_loss
                * (period - 1)
            )
            + loss
        ) / period

        if average_loss == 0:

            if average_gain == 0:
                rsi_value = 50.0
            else:
                rsi_value = 100.0

        else:

            rs = (
                average_gain
                / average_loss
            )

            rsi_value = (
                100
                - (100 / (1 + rs))
            )

    return rsi_value


# ============================================================
# TECHNICAL MODEL
# ============================================================

def ema(values, period):
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    result = sum(
        values[:period]
    ) / period

    for value in values[period:]:
        result = (
            (value - result)
            * multiplier
            + result
        )

    return result


def technical_probability(closes, rsi_value):
    """
    Local technical heuristic.
    This is NOT an external AI model.
    """

    if len(closes) < 30:
        return 50.0, "خنثی"

    fast = ema(
        closes[-30:],
        9
    )

    slow = ema(
        closes[-30:],
        21
    )

    if fast is None or slow is None:
        return 50.0, "خنثی"

    score = 50.0

    # Trend
    if fast > slow:
        score += 15
    else:
        score -= 15

    # RSI
    if rsi_value >= 75:
        score -= 12
    elif rsi_value >= 70:
        score -= 6
    elif rsi_value <= 25:
        score += 12
    elif rsi_value <= 30:
        score += 6

    # Short momentum
    if len(closes) >= 6:

        momentum = (
            closes[-1]
            / closes[-6]
            - 1
        )

        if momentum > 0.002:
            score += 8

        elif momentum < -0.002:
            score -= 8

    score = max(
        1,
        min(99, score)
    )

    if score >= 50:
        return score, "صعودی"

    return 100 - score, "نزولی"


# ============================================================
# VOLUME
# ============================================================

def volume_info(rows):
    if len(rows) < 5:
        return "➖ معمولی", 1.0

    current_volume = float(
        rows[-1][5]
    )

    previous = [
        float(rows[-2][5]),
        float(rows[-3][5]),
        float(rows[-4][5])
    ]

    average_volume = mean(
        previous
    )

    if average_volume <= 0:
        return "➖ معمولی", 1.0

    ratio = (
        current_volume
        / average_volume
    )

    if ratio > 1.2:
        label = "🔥 زیاد"

    elif ratio < 0.8:
        label = "📉 کم"

    else:
        label = "➖ معمولی"

    return label, ratio


# ============================================================
# CANDLE TIME
# ============================================================

def candle_close_time_ms(
    open_ms,
    interval
):
    minutes = {
        "15m": 15,
        "1h": 60,
        "4h": 240,
        "1d": 1440
    }[interval]

    return (
        open_ms
        + minutes * 60 * 1000
    )


def remaining_text(close_ms):
    now_ms = int(
        datetime.now(
            timezone.utc
        ).timestamp() * 1000
    )

    remaining = max(
        0,
        close_ms - now_ms
    )

    total_seconds = (
        remaining // 1000
    )

    hours = total_seconds // 3600

    minutes = (
        total_seconds % 3600
    ) // 60

    seconds = (
        total_seconds % 60
    )

    if hours > 0:
        return (
            f"{hours:02d}:"
            f"{minutes:02d}:"
            f"{seconds:02d}"
        )

    return (
        f"{minutes:02d}:"
        f"{seconds:02d}"
    )


# ============================================================
# POLYMARKET
# ============================================================

async def polymarket_sentiment(
    session,
    coin_name,
    symbol
):
    """
    Public Polymarket search.
    If no relevant market is found, returns None.
    """

    queries = [
        f"{coin_name} crypto",
        symbol.replace("USDT", "")
    ]

    for query in queries:

        try:

            url = (
                "https://gamma-api.polymarket.com"
                "/public-search"
            )

            data = await http_get(
                session,
                url,
                {
                    "q": query,
                    "limit": 10
                },
                retries=1
            )

            if not isinstance(data, dict):
                continue

            results = (
                data.get("events")
                or data.get("markets")
                or []
            )

            for market in results:

                text = json.dumps(
                    market,
                    ensure_ascii=False
                ).lower()

                if (
                    "bitcoin" in text
                    or "btc" in text
                    or symbol.replace(
                        "USDT", ""
                    ).lower() in text
                    or coin_name.lower() in text
                ):

                    # We only use an explicit probability
                    # if the API exposes one.
                    probability = None

                    for key in (
                        "probability",
                        "outcomePrices",
                        "price"
                    ):

                        value = market.get(
                            key
                        )

                        if value is not None:
                           
