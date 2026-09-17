import os
import json
import asyncio
import logging
import math
from pathlib import Path
from datetime import datetime, timezone, timedelta

import aiohttp


# ============================================================
# CONFIG
# ============================================================

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
LEGACY_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TOP_N = int(os.getenv("TOP_N", "100"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
MARKET = os.getenv("MARKET", "spot").lower()
ALERT_MODE = os.getenv("ALERT_MODE", "changes").lower()

USERS_FILE = Path(os.getenv("USERS_FILE", "users.json"))

# Optional PRISM API key
PRISM_API_KEY = os.getenv("PRISM_API_KEY", "").strip()

# How long one GitHub Actions invocation should run
RUN_SECONDS = int(os.getenv("RUN_SECONDS", "285"))

# Scan interval
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))

# We only want alerts when the CURRENT OPEN candle
# has approximately this much time left.
ALERT_MINUTES_BEFORE_CLOSE = 10

BINANCE_BASE_URLS = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api.binance.com",
]

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
)

POLYMARKET_MARKETS_URL = (
    "https://gamma-api.polymarket.com/markets"
)

PRISM_BASE_URL = (
    "https://www.prismforecasting.com/v1/forecast"
)


TIMEFRAMES = {
    "15m": {
        "binance": "15m",
        "tradingview": "15",
        "minutes": 15,
    },
    "1h": {
        "binance": "1h",
        "tradingview": "60",
        "minutes": 60,
    },
    "4h": {
        "binance": "4h",
        "tradingview": "240",
        "minutes": 240,
    },
    "1D": {
        "binance": "1d",
        "tradingview": "D",
        "minutes": 1440,
    },
}

TIMEFRAME_ORDER = ["15m", "1h", "4h", "1D"]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("rsi-scanner")


# ============================================================
# NUMBER FORMATTING
# ============================================================

BOLD_DIGITS = str.maketrans(
    "0123456789",
    "𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵"
)


def bold_numbers(value):
    return str(value).translate(BOLD_DIGITS)


def format_number(value, decimals=2):
    try:
        return bold_numbers(f"{float(value):.{decimals}f}")
    except Exception:
        return bold_numbers(str(value))


# ============================================================
# USERS / STATE
# ============================================================

DEFAULT_USERS = {
    "users": [],
    "offset": 0,
    "states": {},
}


def load_state():
    if not USERS_FILE.exists():
        return DEFAULT_USERS.copy()

    try:
        with USERS_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return DEFAULT_USERS.copy()

        data.setdefault("users", [])
        data.setdefault("offset", 0)
        data.setdefault("states", {})

        return data

    except Exception as e:
        logger.error("Cannot load users.json: %s", e)
        return DEFAULT_USERS.copy()


def save_state(state):
    tmp = USERS_FILE.with_suffix(".tmp")

    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(
                state,
                f,
                ensure_ascii=False,
                indent=2,
            )

        tmp.replace(USERS_FILE)

    except Exception as e:
        logger.error("Cannot save users.json: %s", e)


# ============================================================
# TELEGRAM
# ============================================================

async def telegram_request(
    session,
    method,
    params=None,
):
    if not TOKEN:
        return None

    url = f"https://api.telegram.org/bot{TOKEN}/{method}"

    try:
        async with session.post(
            url,
            data=params or {},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as response:

            if response.status != 200:
                text = await response.text()
                logger.error(
                    "Telegram %s HTTP %s: %s",
                    method,
                    response.status,
                    text[:500],
                )
                return None

            return await response.json()

    except Exception as e:
        logger.error(
            "Telegram request failed: %s",
            e,
        )
        return None


async def send_message(
    session,
    chat_id,
    text,
):
    if not chat_id:
        return False

    result = await telegram_request(
        session,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
    )

    return bool(
        result and result.get("ok")
    )


async def send_long_message(
    session,
    chat_id,
    text,
):
    MAX_LENGTH = 4000

    if len(text) <= MAX_LENGTH:
        await send_message(
            session,
            chat_id,
            text,
        )
        return

    parts = []
    current = ""

    for block in text.split("\n\n"):
        candidate = (
            current + "\n\n" + block
            if current
            else block
        )

        if len(candidate) > MAX_LENGTH:
            if current:
                parts.append(current)

            current = block

        else:
            current = candidate

    if current:
        parts.append(current)

    for part in parts:
        await send_message(
            session,
            chat_id,
            part,
        )


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def process_updates(
    session,
    state,
):
    offset = int(
        state.get("offset", 0)
    )

    result = await telegram_request(
        session,
        "getUpdates",
        {
            "offset": offset,
            "timeout": 1,
            "allowed_updates": json.dumps(
                ["message"]
            ),
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

        chat_id = chat.get("id")
        text = (
            message.get("text") or ""
        ).strip()

        if not chat_id or not text:
            continue

        if text.startswith("/start"):
            if chat_id not in state["users"]:
                state["users"].append(
                    chat_id
                )

            await send_message(
                session,
                chat_id,
                (
                    "🤖 RSI Telegram Scanner\n\n"
                    "فعال شد.\n\n"
                    "RSI زیر 30 یا بالای 70 "
                    "در تایم‌فریم‌های 15m، 1h، 4h و 1D "
                    "بررسی می‌شود."
                ),
            )

        elif text.startswith("/stop"):
            if chat_id in state["users"]:
                state["users"].remove(chat_id)

            await send_message(
                session,
                chat_id,
                "⛔ ربات برای شما متوقف شد.",
            )

        elif text.startswith("/status"):
            active = (
                chat_id in state["users"]
            )

            await send_message(
                session,
                chat_id,
                (
                    "🟢 فعال"
                    if active
                    else "🔴 غیرفعال"
                ),
            )

    save_state(state)


# ============================================================
# BINANCE
# ============================================================

async def binance_get(
    session,
    path,
    params=None,
):
    for base in BINANCE_BASE_URLS:
        url = f"{base}{path}"

        try:
            async with session.get(
                url,
                params=params or {},
                timeout=aiohttp.ClientTimeout(
                    total=20
                ),
            ) as response:

                if response.status != 200:
                    continue

                return await response.json()

        except Exception:
            continue

    return None


async def get_binance_exchange_info(
    session,
):
    return await binance_get(
        session,
        "/api/v3/exchangeInfo",
    )


async def get_binance_tickers(
    session,
):
    return await binance_get(
        session,
        "/api/v3/ticker/24hr",
    )


async def get_klines(
    session,
    symbol,
    interval,
    limit=100,
):
    return await binance_get(
        session,
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        },
    )


# ============================================================
# COINGECKO TOP COINS
# ============================================================

async def get_top_coins(
    session,
):
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_N,
        "page": 1,
        "sparkline": "false",
    }

    try:
        async with session.get(
            COINGECKO_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(
                total=30
            ),
        ) as response:

            if response.status != 200:
                logger.error(
                    "CoinGecko HTTP %s",
                    response.status,
                )
                return []

            data = await response.json()

            result = []

            for coin in data:
                symbol = (
                    coin.get("symbol") or ""
                ).upper()

                if symbol:
                    result.append(symbol)

            return result

    except Exception as e:
        logger.error(
            "CoinGecko failed: %s",
            e,
        )
        return []


# ============================================================
# VALID BINANCE SYMBOLS
# ============================================================

async def get_valid_usdt_symbols(
    session,
):
    info = await get_binance_exchange_info(
        session
    )

    if not info:
        return set()

    symbols = set()

    for item in info.get(
        "symbols",
        [],
    ):
        if (
            item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get(
                "isSpotTradingAllowed",
                True,
            )
        ):
            symbols.add(
                item.get(
                    "baseAsset",
                    "",
                ).upper()
            )

    return symbols


# ============================================================
# RSI
# ============================================================

def calculate_rsi(
    closes,
    period=14,
):
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):
        change = (
            closes[i] - closes[i - 1]
        )

        if change > 0:
            gains.append(change)
            losses.append(0.0)

        else:
            gains.append(0.0)
            losses.append(abs(change))

    avg_gain = sum(
        gains[:period]
    ) / period

    avg_loss = sum(
        losses[:period]
    ) / period

    for i in range(
        period,
        len(gains),
    ):
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

    return 100.0 - (
        100.0 / (1.0 + rs)
    )


# ============================================================
# CANDLE TIME
# ============================================================

def candle_close_datetime(
    timeframe,
    now=None,
):
    if now is None:
        now = datetime.now(
            timezone.utc
        )

    minutes = TIMEFRAMES[
        timeframe
    ]["minutes"]

    timestamp = int(
        now.timestamp()
    )

    seconds_per_candle = (
        minutes * 60
    )

    candle_open_timestamp = (
        timestamp
        - (
            timestamp
            % seconds_per_candle
        )
    )

    close_timestamp = (
        candle_open_timestamp
        + seconds_per_candle
    )

    return datetime.fromtimestamp(
        close_timestamp,
        timezone.utc,
    )


def minutes_until_close(
    timeframe,
    now=None,
):
    close_dt = candle_close_datetime(
        timeframe,
        now,
    )

    if now is None:
        now = datetime.now(
            timezone.utc
        )

    return (
        close_dt - now
    ).total_seconds() / 60.0


def iran_time_string(
    dt,
):
    iran_tz = timezone(
        timedelta(hours=3, minutes=30)
    )

    local = dt.astimezone(
        iran_tz
    )

    return local.strftime(
        "%H:%M"
    )


# ============================================================
# VOLUME
# ============================================================

def calculate_volume_ratio(
    klines,
):
    if len(klines) < 5:
        return None

    current_volume = float(
        klines[-1][5]
    )

    previous_volumes = [
        float(x[5])
        for x in klines[-4:-1]
    ]

    if not previous_volumes:
        return None

    average = sum(
        previous_volumes
    ) / len(previous_volumes)

    if average <= 0:
        return None

    return (
        current_volume / average
    )


# ============================================================
# RSI SIGNAL
# ============================================================

def get_rsi_zone(
    rsi,
):
    if rsi is None:
        return None

    if rsi > 70:
        return "high"

    if rsi < 30:
        return "low"

    return None


def get_rsi_icon(
    zone,
):
    if zone == "high":
        return "🟢"

    if zone == "low":
        return "🔴"

    return "⚪"


def get_direction(
    zone,
):
    if zone == "high":
        return "↑"

    if zone == "low":
        return "↓"

    return "→"


# ============================================================
# TRADINGVIEW
# ============================================================

def tradingview_url(
    symbol,
    timeframe,
):
    tv_tf = TIMEFRAMES[
        timeframe
    ]["tradingview"]

    return (
        "https://www.tradingview.com/chart/"
        "?symbol=BINANCE%3A"
        f"{symbol}USDT"
        f"&interval={tv_tf}"
    )


# ============================================================
# POLYMARKET
# ============================================================

def parse_json_string(
    value,
):
    if isinstance(value, list):
        return value

    if not isinstance(value, str):
        return None

    try:
        return json.loads(value)
    except Exception:
        return None


def extract_polymarket_probability(
    market,
    symbol,
):
    question = (
        market.get("question")
        or ""
    ).lower()

    symbol_lower = symbol.lower()

    if symbol_lower not in question:
        return None

    # We prefer short-term Up/Down markets
    crypto_words = [
        "up or down",
        "up/down",
        "up or down 15m",
        "up or down 1h",
        "up or down 4h",
    ]

    if not any(
        word in question
        for word in crypto_words
    ):
        return None

    outcomes = parse_json_string(
        market.get("outcomes")
    )

    prices = parse_json_string(
        market.get("outcomePrices")
    )

    if not outcomes or not prices:
        return None

    try:
        pairs = []

        for outcome, price in zip(
            outcomes,
            prices,
        ):
            pairs.append(
                (
                    str(outcome).lower(),
                    float(price),
                )
            )

        for outcome, price in pairs:
            if outcome in (
                "up",
                "yes",
            ):
                return price * 100.0

    except Exception:
        return None

    return None


async def get_polymarket_probability(
    session,
    symbol,
    timeframe,
):
    """
    Public Polymarket market lookup.

    Only uses a real market probability.
    It does NOT insert 50% when no market exists.
    """

    # Polymarket currently has especially
    # visible short-term crypto markets.
    if timeframe not in (
        "15m",
        "1h",
        "4h",
    ):
        return None

    try:
        params = {
            "closed": "false",
            "limit": 100,
        }

        async with session.get(
            POLYMARKET_MARKETS_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(
                total=20
            ),
        ) as response:

            if response.status != 200:
                return None

            markets = await response.json()

        candidates = []

        for market in markets:
            question = (
                market.get("question")
                or ""
            ).lower()

            if (
                symbol.lower()
                not in question
            ):
                continue

            if "up or down" not in question:
                continue

            if timeframe == "15m":
                if "15m" not in question:
                    continue

            if timeframe == "1h":
                if (
                    "1h" not in question
                    and "1 hour"
                    not in question
                ):
                    continue

            if timeframe == "4h":
                if (
                    "4h" not in question
                    and "4 hour"
                    not in question
                ):
                    continue

            candidates.append(market)

        if not candidates:
            return None

        # Prefer the market ending soonest.
        candidates.sort(
            key=lambda x: x.get(
                "endDate",
                "",
            )
        )

        for market in candidates:
            probability = (
                extract_polymarket_probability(
                    market,
                    symbol,
                )
            )

            if probability is not None:
                if 0 <= probability <= 100:
                    return probability

    except Exception as e:
        logger.debug(
            "Polymarket unavailable: %s",
            e,
        )

    return None


# ============================================================
# PRISM FORECASTING
# ============================================================

PRISM_TIMEFRAMES = {
    "15m": "M15",
    "1h": "H1",
    "4h": "H4",
    "1D": "D1",
}


async def get_prism_probability(
    session,
    symbol,
    timeframe,
):
    """
    PRISM official API.

    Requires PRISM_API_KEY.
    Returns the probability of UP for the
    first forecast candle when available.
    """

    if not PRISM_API_KEY:
        return None

    prism_symbol = (
        f"{symbol}USD"
    )

    params = {
        "symbol": prism_symbol,
        "timeframe": PRISM_TIMEFRAMES[
            timeframe
        ],
    }

    headers = {
        "Authorization":
            f"Bearer {PRISM_API_KEY}",
        "Accept":
            "application/json",
    }

    try:
        async with session.get(
            PRISM_BASE_URL,
            params=params,
            headers=headers,
            timeout=aiohttp.ClientTimeout(
                total=20
            ),
        ) as response:

            if response.status != 200:
                return None

            data = await response.json()

        candles = data.get(
            "candles",
            [],
        )

        # Find the first FUTURE candle
        for candle in candles:
            if (
                candle.get("kind")
                == "future"
            ):
                probability = candle.get(
                    "dir_prob"
                )

                if probability is None:
                    continue

                probability = (
                    float(probability)
                    * 100.0
                )

                if 0 <= probability <= 100:
                    return probability

    except Exception as e:
        logger.debug(
            "PRISM unavailable: %s",
            e,
        )

    return None


# ============================================================
# EXTERNAL SOURCES
# ============================================================

async def get_external_predictions(
    session,
    symbol,
    timeframe,
):
    """
    Returns ONLY real numerical values that can
    actually be retrieved.

    Sources:
      1. Next Candle Predictor
      2. CandleCast
      3. NextCandle
      4. LiveCharts.AI
      5. PRISM Forecasting
      6. TradingView
      7. Polymarket
      8. Polymarket AI (when a real numeric API
         is configured)

    Important:
    We do not fabricate values for sources that
    do not expose a usable public API.
    """

    values = []

    # --------------------------------------------------------
    # PRISM
    # --------------------------------------------------------

    prism = await get_prism_probability(
        session,
        symbol,
        timeframe,
    )

    if prism is not None:
        values.append(prism)

    # --------------------------------------------------------
    # POLYMARKET
    # --------------------------------------------------------

    polymarket = (
        await get_polymarket_probability(
            session,
            symbol,
            timeframe,
        )
    )

    if polymarket is not None:
        values.append(
            polymarket
        )

    # --------------------------------------------------------
    # The following sources currently do not have
    # a verified public endpoint that can safely
    # be called from this free GitHub Actions bot.
    #
    # Therefore:
    #
    # Next Candle Predictor -> no fake value
    # CandleCast            -> no fake value
    # NextCandle             -> no fake value
    # LiveCharts.AI          -> no fake value
    # TradingView            -> no fake value
    #
    # They can be added later if a real API/output
    # endpoint is available.
    # --------------------------------------------------------

    return values


# ============================================================
# LOCAL TECHNICAL SCORE
# ============================================================

def technical_score(
    klines,
    rsi,
):
    """
    Local technical estimate.

    Used ONLY as fallback if no external
    statistical source is available.
    """

    if not klines or rsi is None:
        return None

    score = 50.0

    # RSI contribution
    if rsi >= 70:
        score += min(
            20,
            (rsi - 70) * 0.8,
        )

    elif rsi <= 30:
        score -= min(
            20,
            (30 - rsi) * 0.8,
        )

    # Recent candle direction
    try:
        close_now = float(
            klines[-1][4]
        )
        close_prev = float(
            klines[-2][4]
        )

        if close_now > close_prev:
            score += 5

        elif close_now < close_prev:
            score -= 5

    except Exception:
        pass

    return max(
        0,
        min(
            100,
            score,
        ),
    )


def momentum_score(
    klines,
):
    if len(klines) < 6:
        return None

    try:
        closes = [
            float(x[4])
            for x in klines[-6:]
        ]

        first = closes[0]
        last = closes[-1]

        if first <= 0:
            return None

        change_pct = (
            (last - first)
            / first
        ) * 100

        score = (
            50
            + change_pct * 8
        )

        return max(
            0,
            min(
                100,
                score,
            ),
        )

    except Exception:
        return None


# ============================================================
# NEXT CANDLE SCORE
# ============================================================

async def calculate_next_candle(
    session,
    symbol,
    timeframe,
    klines,
    rsi,
):
    external_scores = (
        await get_external_predictions(
            session,
            symbol,
            timeframe,
        )
    )

    # --------------------------------------------------------
    # REAL EXTERNAL DATA AVAILABLE
    # --------------------------------------------------------

    if external_scores:
        final_score = (
            sum(external_scores)
            / len(external_scores)
        )

        return {
            "score": final_score,
            "sources_count": len(
                external_scores
            ),
            "is_external": True,
        }

    # --------------------------------------------------------
    # NO EXTERNAL SOURCE AVAILABLE
    # --------------------------------------------------------
    # Keep the bot functional.
    # This is NOT mixed into an external average.
    # --------------------------------------------------------

    technical = technical_score(
        klines,
        rsi,
    )

    momentum = momentum_score(
        klines,
    )

    local_values = [
        x
        for x in (
            technical,
            momentum,
        )
        if x is not None
    ]

    if not local_values:
        return {
            "score": 50.0,
            "sources_count": 0,
            "is_external": False,
        }

    local_score = (
        sum(local_values)
        / len(local_values)
    )

    return {
        "score": local_score,
        "sources_count": 0,
        "is_external": False,
    }


# ============================================================
# ALERT STATE
# ============================================================

def state_key(
    symbol,
    timeframe,
):
    return f"{symbol}:{timeframe}"


def should_alert(
    state,
    symbol,
    timeframe,
    zone,
):
    key = state_key(
        symbol,
        timeframe,
    )

    states = state.setdefault(
        "states",
        {},
    )

    previous = states.get(key)

    # First occurrence
    if previous is None:
        states[key] = zone
        return True

    # Same zone -> no duplicate
    if previous == zone:
        return False

    # RSI returned to neutral and later
    # enters again -> alert again
    states[key] = zone

    return True


# ============================================================
# SIGNAL FORMAT
# ============================================================

def format_signal(
    symbol,
    timeframe,
    rsi,
    zone,
    volume_ratio,
    close_dt,
    prediction,
):
    rsi_icon = get_rsi_icon(
        zone
    )

    direction = get_direction(
        zone
    )

    rsi_text = format_number(
        rsi,
        2,
    )

    score_text = format_number(
        prediction,
        1,
    )

    if volume_ratio is None:
        volume_text = "—"
    else:
        volume_text = format_number(
            volume_ratio,
            2,
        )

    close_text = bold_numbers(
        iran_time_string(
            close_dt
        )
    )

    tv_url = tradingview_url(
        symbol,
        timeframe,
    )

    return (
        f"💠 {symbol}\n\n"
        f"{rsi_icon} RSI                 "
        f"{rsi_text}\n"
        f"🔮 {direction}                "
        f"{score_text} %\n"
        f"volume                 "
        f"{volume_text} ×\n"
        f"close                  "
        f"{close_text}\n"
        f"📈 <a href=\"{tv_url}\">TV</a>"
    )


# ============================================================
# SCAN SYMBOL
# ============================================================

async def scan_symbol(
    session,
    symbol,
    timeframe,
):
    config = TIMEFRAMES[
        timeframe
    ]

    klines = await get_klines(
        session,
        f"{symbol}USDT",
        config["binance"],
        limit=100,
    )

    if not klines or len(klines) < (
        RSI_PERIOD + 10
    ):
        return None

    closes = [
        float(x[4])
        for x in klines
    ]

    rsi = calculate_rsi(
        closes,
        RSI_PERIOD,
    )

    if rsi is None:
        return None

    zone = get_rsi_zone(
        rsi
    )

    if zone is None:
        return None

    # Only alert when current OPEN candle
    # has around 10 minutes remaining.
    remaining = minutes_until_close(
        timeframe
    )

    if not (
        9.0 <= remaining <= 11.0
    ):
        return None

    volume_ratio = (
        calculate_volume_ratio(
            klines
        )
    )

    prediction = (
        await calculate_next_candle(
            session,
            symbol,
            timeframe,
            klines,
            rsi,
        )
    )

    close_dt = candle_close_datetime(
        timeframe
    )

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "rsi": rsi,
        "zone": zone,
        "volume_ratio": volume_ratio,
        "close_dt": close_dt,
        "prediction": prediction[
            "score"
        ],
        "sources_count": prediction[
            "sources_count"
        ],
        "is_external": prediction[
            "is_external"
        ],
    }


# ============================================================
# SCAN ALL
# ============================================================

async def scan_all(
    session,
    state,
):
    top_coins = await get_top_coins(
        session
    )

    valid_symbols = (
        await get_valid_usdt_symbols(
            session
        )
    )

    coins = [
        coin
        for coin in top_coins
        if coin in valid_symbols
    ]

    logger.info(
        "Top coins: %s",
        len(coins),
    )

    signals = []

    # --------------------------------------------------------
    # IMPORTANT:
    # 15m first
    # 1h second
    # 4h third
    # 1D last
    # --------------------------------------------------------

    for timeframe in TIMEFRAME_ORDER:

        for symbol in coins:

            try:
                signal = await scan_symbol(
                    session,
                    symbol,
                    timeframe,
                )

                if signal is None:
                    continue

                if not should_alert(
                    state,
                    symbol,
                    timeframe,
                    signal["zone"],
                ):
                    continue

                signals.append(
                    signal
                )

            except Exception as e:
                logger.debug(
                    "Scan failed %s %s: %s",
                    symbol,
                    timeframe,
                    e,
                )

    return signals


# ============================================================
# MESSAGE BUILDING
# ============================================================

def timeframe_header(
    timeframe,
):
    return (
        f"━━━━━━━━ {timeframe} ━━━━━━━━"
    )


def build_message(
    signals,
):
    if not signals:
        return None

    # Explicit ordering
    ordered = []

    for timeframe in TIMEFRAME_ORDER:
        ordered.extend(
            [
                s
                for s in signals
                if s["timeframe"]
                == timeframe
            ]
        )

    blocks = []

    current_tf = None

    for signal in ordered:

        tf = signal["timeframe"]

        if tf != current_tf:
            blocks.append(
                timeframe_header(tf)
            )
            current_tf = tf

        blocks.append(
            format_signal(
                signal["symbol"],
                signal["timeframe"],
                signal["rsi"],
                signal["zone"],
                signal["volume_ratio"],
                signal["close_dt"],
                signal["prediction"],
            )
        )

    return "\n\n--------------------\n\n".join(
        blocks
    )


# ============================================================
# MAIN
# ============================================================

async def main():
    if not TOKEN:
        logger.error(
            "TELEGRAM_BOT_TOKEN is missing."
        )
        return

    state = load_state()

    # Legacy single-user compatibility
    if LEGACY_CHAT_ID:
        try:
            legacy_id = int(
                LEGACY_CHAT_ID
            )

            if (
                legacy_id
                not in state["users"]
            ):
                state["users"].append(
                    legacy_id
                )

        except Exception:
            pass

    timeout = aiohttp.ClientTimeout(
        total=40
    )

    connector = aiohttp.TCPConnector(
        limit=30,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={
            "User-Agent":
                "RSI-Telegram-Scanner/1.0"
        },
    ) as session:

        # Process Telegram commands
        await process_updates(
            session,
            state,
        )

        if not state["users"]:
            logger.info(
                "No active Telegram users."
            )
            save_state(state)
            return

        start = asyncio.get_event_loop().time()

        sent_any = False

        while (
            asyncio.get_event_loop().time()
            - start
            < RUN_SECONDS
        ):
            try:
                # Commands can arrive during
                # the current workflow too.
                await process_updates(
                    session,
                    state,
                )

                if not state["users"]:
                    break

                signals = await scan_all(
                    session,
                    state,
                )

                logger.info(
                    "Alerts: %s",
                    len(signals),
                )

                if signals:
                    message = build_message(
                        signals
                    )

                    if message:
                        for chat_id in list(
                            state["users"]
                        ):
                            await send_long_message(
                                session,
                                chat_id,
                                message,
                            )

                        sent_any = True

                save_state(state)

            except Exception as e:
                logger.exception(
                    "Scanner error: %s",
                    e,
                )

            await asyncio.sleep(
                SCAN_INTERVAL
            )

        save_state(state)

        logger.info(
            "Scanner finished. sent=%s",
            sent_any,
        )


if __name__ == "__main__":
    try:
        asyncio.run(
            main()
        )
    except KeyboardInterrupt:
        pass
