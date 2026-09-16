import asyncio
import aiohttp
import json
import logging
import math
import os
from datetime import datetime, timezone, timedelta
from html import escape
from pathlib import Path
from statistics import mean

# =========================================================
# CONFIG
# =========================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TOP_N = 100
RSI_PERIOD = 14

BINANCE_API = "https://api.binance.com"
COINGECKO_API = "https://api.coingecko.com/api/v3"
POLYMARKET_API = "https://gamma-api.polymarket.com"

STATE_FILE = Path("users.json")

TIMEFRAMES = {
    "15M": {
        "interval": "15m",
        "minutes": 15,
    },
    "1H": {
        "interval": "1h",
        "minutes": 60,
    },
    "4H": {
        "interval": "4h",
        "minutes": 240,
    },
    "1D": {
        "interval": "1d",
        "minutes": 1440,
    },
}

MAX_CONCURRENT_BINANCE = 8

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("RSI-SCANNER")


# =========================================================
# STATE
# =========================================================

def load_state():
    if not STATE_FILE.exists():
        return {
            "users": [],
            "offset": 0,
            "signals": {},
        }

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        if not isinstance(state, dict):
            raise ValueError("Invalid state")

        state.setdefault("users", [])
        state.setdefault("offset", 0)
        state.setdefault("signals", {})

        return state

    except Exception as e:
        log.error("Failed to load state: %s", e)

        return {
            "users": [],
            "offset": 0,
            "signals": {},
        }


def save_state(state):
    temp_file = STATE_FILE.with_suffix(".tmp")

    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(
            state,
            f,
            ensure_ascii=False,
            indent=2,
        )

    temp_file.replace(STATE_FILE)


# =========================================================
# HTTP
# =========================================================

async def http_get_json(session, url, params=None, timeout=25):
    try:
        timeout_obj = aiohttp.ClientTimeout(total=timeout)

        async with session.get(
            url,
            params=params,
            timeout=timeout_obj,
        ) as response:

            if response.status != 200:
                text = await response.text()

                log.warning(
                    "GET %s -> HTTP %s | %s",
                    url,
                    response.status,
                    text[:300],
                )

                return None

            return await response.json(content_type=None)

    except Exception as e:
        log.warning("GET failed %s | %s", url, e)
        return None


async def http_post_json(session, url, payload, timeout=25):
    try:
        timeout_obj = aiohttp.ClientTimeout(total=timeout)

        async with session.post(
            url,
            json=payload,
            timeout=timeout_obj,
        ) as response:

            if response.status != 200:
                text = await response.text()

                log.warning(
                    "POST %s -> HTTP %s | %s",
                    url,
                    response.status,
                    text[:300],
                )

                return None

            return await response.json(content_type=None)

    except Exception as e:
        log.warning("POST failed %s | %s", url, e)
        return None


# =========================================================
# TELEGRAM
# =========================================================

async def telegram_request(session, method, payload=None):
    if not TELEGRAM_BOT_TOKEN:
        log.error("TELEGRAM_BOT_TOKEN is missing.")
        return None

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/{method}"
    )

    if payload is None:
        payload = {}

    return await http_post_json(
        session,
        url,
        payload,
        timeout=30,
    )


async def send_message(session, chat_id, text):
    payload = {
        "chat_id": str(chat_id),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    result = await telegram_request(
        session,
        "sendMessage",
        payload,
    )

    if result and result.get("ok"):
        return True

    log.warning(
        "Telegram send failed for %s",
        chat_id,
    )

    return False


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

    if not updates:
        return

    for update in updates:

        update_id = update.get("update_id")

        if update_id is not None:
            state["offset"] = update_id + 1

        message = update.get("message") or {}

        chat = message.get("chat") or {}
        chat_id = chat.get("id")

        if chat_id is None:
            continue

        text = (message.get("text") or "").strip()

        if not text:
            continue

        command = text.split()[0].lower()

        if command.startswith("/start"):

            if str(chat_id) not in [
                str(x) for x in state["users"]
            ]:
                state["users"].append(str(chat_id))

            await send_message(
                session,
                chat_id,
                "✅ <b>ربات RSI فعال شد</b>\n\n"
                "اسکنر هر بار که Workflow اجرا شود، "
                "Top 100 را بررسی می‌کند.\n\n"
                "⏱ تایم‌فریم‌ها: 15M / 1H / 4H / 1D\n"
                "📊 RSI(14)\n"
                "🔔 فقط سیگنال‌های جدید ارسال می‌شوند.",
            )

            log.info(
                "User activated: %s",
                chat_id,
            )

        elif command.startswith("/stop"):

            state["users"] = [
                str(x)
                for x in state["users"]
                if str(x) != str(chat_id)
            ]

            await send_message(
                session,
                chat_id,
                "⛔ <b>ربات متوقف شد.</b>",
            )

            log.info(
                "User deactivated: %s",
                chat_id,
            )


# =========================================================
# COINGECKO TOP 100
# =========================================================

async def get_top_coins(session):
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_N,
        "page": 1,
        "sparkline": "false",
    }

    data = await http_get_json(
        session,
        f"{COINGECKO_API}/coins/markets",
        params,
        timeout=30,
    )

    if not data or not isinstance(data, list):
        log.error("CoinGecko Top 100 failed.")
        return []

    coins = []

    for coin in data:

        symbol = str(
            coin.get("symbol", "")
        ).upper()

        if not symbol:
            continue

        # Binance USDT symbols
        symbol = symbol.replace("-", "")

        coins.append(symbol + "USDT")

    # Remove duplicates while preserving order
    coins = list(dict.fromkeys(coins))

    log.info("Top coins: %s", len(coins))

    return coins


# =========================================================
# BINANCE SYMBOLS
# =========================================================

async def get_binance_symbols(session):
    data = await http_get_json(
        session,
        f"{BINANCE_API}/api/v3/exchangeInfo",
        timeout=30,
    )

    if not data:
        return set()

    symbols = set()

    for item in data.get("symbols", []):

        symbol = item.get("symbol")

        if not symbol:
            continue

        if (
            item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("isSpotTradingAllowed", True)
        ):
            symbols.add(symbol)

    return symbols


# =========================================================
# BINANCE TICKERS
# =========================================================

async def get_binance_tickers(session):
    data = await http_get_json(
        session,
        f"{BINANCE_API}/api/v3/ticker/24hr",
        timeout=30,
    )

    if not data or not isinstance(data, list):
        return {}

    tickers = {}

    for item in data:

        symbol = item.get("symbol")

        if symbol:
            tickers[symbol] = item

    log.info(
        "Binance tickers: %s",
        len(tickers),
    )

    return tickers


# =========================================================
# KLINES
# =========================================================

async def get_klines(
    session,
    symbol,
    interval,
    limit=100,
    semaphore=None,
):

    async def request():

        return await http_get_json(
            session,
            f"{BINANCE_API}/api/v3/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
            },
            timeout=25,
        )

    if semaphore:
        async with semaphore:
            return await request()

    return await request()


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

        if change >= 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1)) + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) + losses[i]
        ) / period

        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

    return round(rsi, 2)


# =========================================================
# TECHNICAL AI-LIKE SCORE
# =========================================================

def technical_prediction(klines):

    if len(klines) < 30:
        return None, None

    closes = [
        float(k[4])
        for k in klines
    ]

    rsi = calculate_rsi(
        closes,
        RSI_PERIOD,
    )

    if rsi is None:
        return None, None

    ema9 = calculate_ema(closes, 9)
    ema21 = calculate_ema(closes, 21)

    if ema9 is None or ema21 is None:
        return None, None

    current = closes[-1]
    previous = closes[-2]

    momentum = 0

    if current > previous:
        momentum = 1
    elif current < previous:
        momentum = -1

    score = 50.0

    # EMA trend
    if ema9 > ema21:
        score += 15
    else:
        score -= 15

    # Current candle direction
    if momentum > 0:
        score += 10
    elif momentum < 0:
        score -= 10

    # RSI contribution
    if rsi >= 70:
        score += 8
    elif rsi <= 30:
        score -= 8
    elif rsi > 55:
        score += 5
    elif rsi < 45:
        score -= 5

    # Short momentum
    if len(closes) >= 6:
        old = closes[-6]

        if old != 0:
            change_pct = (
                (current - old) / old
            ) * 100

            if change_pct > 1:
                score += 7

            elif change_pct < -1:
                score -= 7

    score = max(
        1,
        min(99, score),
    )

    if score >= 50:
        direction = "صعودی"
    else:
        direction = "نزولی"

    return round(score, 0), direction


def calculate_ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(values[:period]) / period

    for price in values[period:]:
        ema = (
            (price - ema) * multiplier
        ) + ema

    return ema


# =========================================================
# VOLUME
# =========================================================

def volume_ratio(klines):

    if len(klines) < 5:
        return None

    current_volume = float(
        klines[-1][5]
    )

    previous_volumes = [
        float(k[5])
        for k in klines[-4:-1]
    ]

    avg_volume = mean(
        previous_volumes
    )

    if avg_volume <= 0:
        return None

    return current_volume / avg_volume


def volume_text(ratio):

    if ratio is None:
        return "📊 حجم: —"

    if ratio > 1.2:
        icon = "🔥"
        label = "زیاد"

    elif ratio < 0.8:
        icon = "📉"
        label = "کم"

    else:
        icon = "➖"
        label = "معمولی"

    return (
        f"📊 حجم: {icon} "
        f"{ratio:.2f}× میانگین ۳ کندل قبل "
        f"({label})"
    )


# =========================================================
# CANDLE TIME
# =========================================================

def candle_remaining(klines, timeframe_minutes):

    if not klines:
        return "—"

    open_time_ms = int(
        klines[-1][0]
    )

    open_time = datetime.fromtimestamp(
        open_time_ms / 1000,
        tz=timezone.utc,
    )

    close_time = (
        open_time
        + timedelta(minutes=timeframe_minutes)
    )

    now = datetime.now(timezone.utc)

    remaining = (
        close_time - now
    ).total_seconds()

    remaining = max(
        0,
        int(remaining),
    )

    hours = remaining // 3600
    minutes = (remaining % 3600) // 60
    seconds = remaining % 60

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


# =========================================================
# POLYMARKET
# =========================================================

def normalize_text(text):
    return (
        str(text or "")
        .lower()
        .replace("-", " ")
        .replace("_", " ")
    )


def timeframe_matches(text, timeframe):

    text = normalize_text(text)

    if timeframe == "15M":
        return (
            "15m" in text
            or "15 min" in text
            or "15 minute" in text
            or "15 minutes" in text
            or "5m" in text
        )

    if timeframe == "1H":
        return (
            "1h" in text
            or "1 hour" in text
            or "1hour" in text
            or "hour" in text
        )

    if timeframe == "4H":
        return (
            "4h" in text
            or "4 hour" in text
            or "4hour" in text
        )

    if timeframe == "1D":
        return (
            "1d" in text
            or "1 day" in text
            or "daily" in text
            or "24 hour" in text
        )

    return False


def symbol_matches(text, symbol):

    text = normalize_text(text)

    base = symbol.upper().replace(
        "USDT",
        "",
    ).lower()

    aliases = {
        "btc": ["bitcoin", "btc"],
        "eth": ["ethereum", "eth"],
        "bnb": ["bnb", "binance coin"],
        "sol": ["solana", "sol"],
        "xrp": ["xrp", "ripple"],
        "ada": ["cardano", "ada"],
        "doge": ["dogecoin", "doge"],
        "avax": ["avalanche", "avax"],
        "dot": ["polkadot", "dot"],
        "link": ["chainlink", "link"],
    }

    terms = aliases.get(
        base,
        [base],
    )

    return any(
        term in text
        for term in terms
    )


async def get_polymarket_sentiment(
    session,
    symbol,
    timeframe,
):

    base = symbol.replace(
        "USDT",
        "",
    )

    # Search by coin symbol.
    queries = [
        base,
    ]

    aliases = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "BNB": "bnb",
        "SOL": "solana",
        "XRP": "xrp",
        "DOGE": "dogecoin",
        "ADA": "cardano",
        "AVAX": "avalanche",
        "DOT": "polkadot",
        "LINK": "chainlink",
    }

    if base in aliases:
        queries.insert(
            0,
            aliases[base],
        )

    for query in queries:

        data = await http_get_json(
            session,
            f"{POLYMARKET_API}/public-search",
            {
                "q": query,
                "limit": 20,
            },
            timeout=15,
        )

        if not data:
            continue

        markets = []

        if isinstance(data, list):
            markets = data

        elif isinstance(data, dict):
            markets.extend(
                data.get("markets", [])
            )

            markets.extend(
                data.get("events", [])
            )

        for market in markets:

            question = market.get(
                "question",
                "",
            )

            slug = market.get(
                "slug",
                "",
            )

            text = (
                f"{question} {slug}"
            )

            if not symbol_matches(
                text,
                symbol,
            ):
                continue

            if not timeframe_matches(
                text,
                timeframe,
            ):
                continue

            outcomes = market.get(
                "outcomes"
            )

            prices = market.get(
                "outcomePrices"
            )

            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(
                        outcomes
                    )
                except Exception:
                    outcomes = None

            if isinstance(prices, str):
                try:
                    prices = json.loads(
                        prices
                    )
                except Exception:
                    prices = None

            if not outcomes or not prices:
                continue

            parsed = []

            for outcome, price in zip(
                outcomes,
                prices,
            ):
                try:
                    parsed.append(
                        (
                            str(outcome).lower(),
                            float(price),
                        )
                    )
                except Exception:
                    pass

            if not parsed:
                continue

            bullish = None

            for outcome, price in parsed:

                if any(
                    x in outcome
                    for x in [
                        "yes",
                        "up",
                        "higher",
                        "above",
                        "bullish",
                    ]
                ):
                    bullish = price * 100
                    break

            if bullish is None:
                for outcome, price in parsed:

                    if any(
                        x in outcome
                        for x in [
                            "no",
                            "down",
                            "lower",
                            "below",
                            "bearish",
                        ]
                    ):
                        bullish = (
                            100
                            - price * 100
                        )
                        break

            if bullish is None:
                continue

            bullish = max(
                0,
                min(100, bullish),
            )

            return round(
                bullish,
                0,
            )

    return None


# =========================================================
# COMBINED PREDICTION
# =========================================================

def combined_prediction(
    technical_score,
    technical_direction,
    polymarket_score,
):

    if technical_score is None:
        return None, None

    if polymarket_score is None:

        score = technical_score

    else:

        # 70% technical
        # 30% Polymarket

        score = (
            technical_score * 0.70
            + polymarket_score * 0.30
        )

    score = max(
        1,
        min(99, score),
    )

    if score >= 65:
        direction = "🟢 صعودی"

    elif score <= 35:
        direction = "🔴 نزولی"

    else:

        if score >= 50:
            direction = "🟢 صعودی"
        else:
            direction = "🔴 نزولی"

    return round(score, 0), direction


# =========================================================
# SIGNAL STATE
# =========================================================

def signal_zone(rsi):

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

    key = f"{symbol}:{timeframe}"

    zone = signal_zone(rsi)

    previous = state["signals"].get(
        key,
        "neutral",
    )

    # Neutral resets the state.
    if zone == "neutral":

        if previous != "neutral":
            state["signals"][key] = "neutral"

        return False

    # Same zone = do not repeat.
    if zone == previous:
        return False

    # New entry into overbought/oversold.
    state["signals"][key] = zone

    return True


# =========================================================
# RSI SIGNAL
# =========================================================

def rsi_description(rsi):

    if rsi > 70:
        return (
            f"🟢 RSI {rsi:.2f} | اشباع خرید"
        )

    if rsi < 30:
        return (
            f"🔴 RSI {rsi:.2f} | اشباع فروش"
        )

    return (
        f"RSI {rsi:.2f}"
    )


# =========================================================
# NEXT CANDLE
# =========================================================

def next_candle_prediction(
    technical_score,
    technical_direction,
):

    if technical_score is None:
        return "—"

    if technical_score >= 65:
        return (
            f"🟢 صعودی "
            f"{technical_score:.0f}%"
        )

    if technical_score <= 35:
        return (
            f"🔴 نزولی "
            f"{100 - technical_score:.0f}%"
        )

    if technical_direction == "صعودی":
        return (
            f"🟢 صعودی "
            f"{technical_score:.0f}%"
        )

    return (
        f"🔴 نزولی "
        f"{100 - technical_score:.0f}%"
    )


# =========================================================
# TRADINGVIEW
# =========================================================

def tradingview_url(symbol):
    return (
        "https://www.tradingview.com/"
        f"symbols/{symbol}/"
        "?exchange=BINANCE"
    )


# =========================================================
# BUILD SIGNAL BLOCK
# =========================================================

async def build_signal(
    session,
    symbol,
    timeframe,
    klines,
):

    closes = [
        float(k[4])
        for k in klines
    ]

    rsi = calculate_rsi(
        closes,
        RSI_PERIOD,
    )

    if rsi is None:
        return None

    if not (
        rsi > 70
        or rsi < 30
    ):
        return None

    technical_score, technical_direction = (
        technical_prediction(
            klines
        )
    )

    polymarket = (
        await get_polymarket_sentiment(
            session,
            symbol,
            timeframe,
        )
    )

    combined_score, combined_direction = (
        combined_prediction(
            technical_score,
            technical_direction,
            polymarket,
        )
    )

    remaining = candle_remaining(
        klines,
        TIMEFRAMES[timeframe]["minutes"],
    )

    volume = volume_ratio(
        klines
    )

    if polymarket is None:
        polymarket_text = (
            "🎯 Polymarket: —"
        )
    else:
        pm_direction = (
            "صعودی"
            if polymarket >= 50
            else "نزولی"
        )

        polymarket_text = (
            f"🎯 Polymarket: "
            f"{polymarket:.0f}% "
            f"{pm_direction}"
        )

    if combined_score is None:
        next_text = "—"
        ai_text = "—"
    else:
        next_text = (
            f"🔮 بعدی: "
            f"{combined_direction} "
            f"{combined_score:.0f}%"
        )

        if technical_direction == "صعودی":
            ai_text = (
                f"🤖 AI: "
                f"{technical_score:.0f}% صعودی"
            )
        else:
            ai_text = (
                f"🤖 AI: "
                f"{100 - technical_score:.0f}% نزولی"
            )

    tv_url = tradingview_url(
        symbol
    )

    block = (
        f"━━━ {timeframe} ━━━\n"
        f"{rsi_description(rsi)}\n"
        f"{next_text}\n"
        f"{ai_text}\n"
        f"{polymarket_text}\n"
        f"{volume_text(volume)}\n"
        f"⏳ بسته‌شدن: {remaining}\n"
        f'📈 <a href="{tv_url}">TV</a>'
    )

    return block


# =========================================================
# SCAN ONE COIN
# =========================================================

async def scan_coin(
    session,
    symbol,
    state,
    semaphore,
):

    results = {}

    for timeframe, config in TIMEFRAMES.items():

        klines = await get_klines(
            session,
            symbol,
            config["interval"],
            limit=100,
            semaphore=semaphore,
        )

        if not klines:
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

        # IMPORTANT:
        # Reset signal state when RSI returns
        # to the neutral zone.
        if 30 <= rsi <= 70:

            should_send_signal(
                state,
                symbol,
                timeframe,
                rsi,
            )

            continue

        # Only send when entering a NEW zone.
        if not should_send_signal(
            state,
            symbol,
            timeframe,
            rsi,
        ):
            continue

        block = await build_signal(
            session,
            symbol,
            timeframe,
            klines,
        )

        if block:
            results[timeframe] = block

    return symbol, results


# =========================================================
# RUN FULL SCAN
# =========================================================

async def run_scan(session, state):

    log.info("Starting RSI scan...")

    top_coins = await get_top_coins(
        session
    )

    if not top_coins:
        log.error("No Top 100 coins.")
        return

    binance_symbols = await get_binance_symbols(
        session
    )

    binance_tickers = await get_binance_tickers(
        session
    )

    # Keep only coins that actually exist
    # on Binance Spot.
    symbols = [
        symbol
        for symbol in top_coins
        if symbol in binance_symbols
        and symbol in binance_tickers
    ]

    log.info(
        "Valid Binance Top 100 symbols: %s",
        len(symbols),
    )

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT_BINANCE
    )

    tasks = [
        scan_coin(
            session,
            symbol,
            state,
            semaphore,
        )
        for symbol in symbols
    ]

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    alerts = []

    for result in results:

        if isinstance(
            result,
            Exception,
        ):
            log.warning(
                "Coin scan error: %s",
                result,
            )
            continue

        symbol, blocks = result

        if not blocks:
            continue

        # Preserve timeframe order.
        ordered_blocks = []

        for timeframe in TIMEFRAMES:

            if timeframe in blocks:
                ordered_blocks.append(
                    blocks[timeframe]
                )

        message = (
            f"💠 <b>{escape(symbol)}</b>\n\n"
            + "\n\n".join(
                ordered_blocks
            )
        )

        alerts.append(message)

    log.info(
        "Alerts: %s",
        len(alerts),
    )

    # Send each coin as ONE Telegram message.
    users = state.get(
        "users",
        [],
    )

    if not users:
        log.info("No active users.")
        return

    for message in alerts:

        for chat_id in users:

            await send_message(
                session,
                chat_id,
                message,
            )

            # Small delay to avoid hammering Telegram.
            await asyncio.sleep(0.15)


# =========================================================
# MAIN
# =========================================================

async def main():

    if not TELEGRAM_BOT_TOKEN:
        log.error(
            "TELEGRAM_BOT_TOKEN is not configured."
        )
        return

    state = load_state()

    timeout = aiohttp.ClientTimeout(
        total=60
    )

    connector = aiohttp.TCPConnector(
        limit=30
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={
            "User-Agent":
                "RSI-Telegram-Scanner/1.0"
        },
    ) as session:

        # -------------------------------------------------
        # IMPORTANT:
        # Commands are processed BEFORE the scan.
        # -------------------------------------------------

        await process_commands(
            session,
            state,
        )

        # Save immediately so /start is not lost.
        save_state(state)

        if not state.get("users"):
            log.info(
                "No active users."
            )
            return

        # -------------------------------------------------
        # IMPORTANT:
        # NO minute check here.
        #
        # Every GitHub Actions invocation scans.
        # -------------------------------------------------

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
        log.info("Stopped.")

    except Exception as e:
        log.exception(
            "Fatal error: %s",
            e,
        )
        raise
