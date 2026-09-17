import os
import json
import asyncio
import logging
import hashlib
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

PRISM_API_KEY = os.getenv("PRISM_API_KEY", "").strip()

RUN_SECONDS = int(os.getenv("RUN_SECONDS", "285"))
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))

# همان زمان‌بندی قبلی
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

DEGEN_BASE_URL = (
    "https://www.degensignal.com/markets"
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
        return bold_numbers(
            f"{float(value):.{decimals}f}"
        )
    except Exception:
        return bold_numbers(str(value))


# ============================================================
# USERS / STATE
# ============================================================

DEFAULT_USERS = {
    "users": [],
    "offset": 0,
    "states": {},
    "sent_messages": {},
}


def load_state():
    if not USERS_FILE.exists():
        return DEFAULT_USERS.copy()

    try:
        with USERS_FILE.open(
            "r",
            encoding="utf-8"
        ) as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return DEFAULT_USERS.copy()

        data.setdefault("users", [])
        data.setdefault("offset", 0)
        data.setdefault("states", {})
        data.setdefault("sent_messages", {})

        return data

    except Exception as e:
        logger.error(
            "Cannot load users.json: %s",
            e
        )
        return DEFAULT_USERS.copy()


def save_state(state):
    tmp = USERS_FILE.with_suffix(".tmp")

    try:
        with tmp.open(
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                state,
                f,
                ensure_ascii=False,
                indent=2,
            )

        tmp.replace(USERS_FILE)

    except Exception as e:
        logger.error(
            "Cannot save users.json: %s",
            e
        )


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

    url = (
        f"https://api.telegram.org/bot"
        f"{TOKEN}/{method}"
    )

    try:
        async with session.post(
            url,
            data=params or {},
            timeout=aiohttp.ClientTimeout(
                total=20
            ),
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
            e
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
        return await send_message(
            session,
            chat_id,
            text,
        )

    parts = []
    current = ""

    for block in text.split("\n\n"):

        candidate = (
            current
            + "\n\n"
            + block
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

    success = True

    for part in parts:

        ok = await send_message(
            session,
            chat_id,
            part,
        )

        if not ok:
            success = False

    return success


# ============================================================
# DUPLICATE MESSAGE PROTECTION
# ============================================================

def message_hash(
    chat_id,
    text,
):
    raw = (
        f"{chat_id}|{text}"
    ).encode(
        "utf-8"
    )

    return hashlib.sha256(
        raw
    ).hexdigest()


def already_sent(
    state,
    chat_id,
    text,
):
    sent = state.setdefault(
        "sent_messages",
        {}
    )

    key = message_hash(
        chat_id,
        text,
    )

    now = int(
        datetime.now(
            timezone.utc
        ).timestamp()
    )

    # Keep only the recent 15 minutes
    cutoff = now - 900

    old_keys = []

    for k, timestamp in sent.items():

        try:
            if int(timestamp) < cutoff:
                old_keys.append(k)

        except Exception:
            old_keys.append(k)

    for k in old_keys:
        sent.pop(k, None)

    if key in sent:
        return True

    sent[key] = now

    return False


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

    updates = result.get(
        "result",
        []
    )

    for update in updates:

        update_id = update.get(
            "update_id"
        )

        if update_id is not None:
            state["offset"] = (
                update_id + 1
            )

        message = (
            update.get("message")
            or {}
        )

        chat = (
            message.get("chat")
            or {}
        )

        chat_id = chat.get("id")

        text = (
            message.get("text")
            or ""
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
                state["users"].remove(
                    chat_id
                )

            await send_message(
                session,
                chat_id,
                "⛔ ربات برای شما متوقف شد.",
            )

        elif text.startswith("/status"):

            active = (
                chat_id
                in state["users"]
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
# COINGECKO
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
                    coin.get("symbol")
                    or ""
                ).upper()

                if symbol:
                    result.append(
                        symbol
                    )

            return result

    except Exception as e:

        logger.error(
            "CoinGecko failed: %s",
            e
        )

        return []


# ============================================================
# BINANCE FILTER
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
        []
    ):

        if (
            item.get("status")
            == "TRADING"
            and item.get("quoteAsset")
            == "USDT"
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

    for i in range(
        1,
        len(closes)
    ):

        change = (
            closes[i]
            - closes[i - 1]
        )

        if change > 0:
            gains.append(change)
            losses.append(0.0)

        else:
            gains.append(0.0)
            losses.append(
                abs(change)
            )

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    for i in range(
        period,
        len(gains)
    ):

        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + losses[i]
        ) / period

    if avg_loss == 0:
        return 100.0

    rs = (
        avg_gain
        / avg_loss
    )

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


def iran_time_string(dt):
    iran_tz = timezone(
        timedelta(
            hours=3,
            minutes=30
        )
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

    average = (
        sum(previous_volumes)
        / len(previous_volumes)
    )

    if average <= 0:
        return None

    return (
        current_volume
        / average
    )


# ============================================================
# RSI SIGNAL
# ============================================================

def get_rsi_zone(rsi):
    if rsi is None:
        return None

    if rsi > 70:
        return "high"

    if rsi < 30:
        return "low"

    return None


def get_rsi_icon(zone):
    if zone == "high":
        return "🟢"

    if zone == "low":
        return "🔴"

    return "⚪"


def get_direction(zone):
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

def parse_json_string(value):
    if isinstance(value, list):
        return value

    if not isinstance(
        value,
        str
    ):
        return None

    try:
        return json.loads(value)

    except Exception:
        return None


async def get_polymarket_probability(
    session,
    symbol,
    timeframe,
):
    if timeframe not in (
        "15m",
        "1h",
        "4h",
    ):
        return None

    try:

        params = {
            "active": "true",
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

        symbol_lower = (
            symbol.lower()
        )

        candidates = []

        for market in markets:

            question = (
                market.get("question")
                or ""
            ).lower()

            if symbol_lower not in question:
                continue

            if "up or down" not in question:
                continue

            if timeframe == "15m":
                if "15m" not in question:
                    continue

            elif timeframe == "1h":
                if (
                    "1h" not in question
                    and "1 hour"
                    not in question
                ):
                    continue

            elif timeframe == "4h":
                if (
                    "4h" not in question
                    and "4 hour"
                    not in question
                ):
                    continue

            candidates.append(
                market
            )

        if not candidates:
            return None

        candidates.sort(
            key=lambda x:
                x.get(
                    "endDate",
                    ""
                )
        )

        for market in candidates:

            outcomes = (
                parse_json_string(
                    market.get(
                        "outcomes"
                    )
                )
            )

            prices = (
                parse_json_string(
                    market.get(
                        "outcomePrices"
                    )
                )
            )

            if not outcomes or not prices:
                continue

            for outcome, price in zip(
                outcomes,
                prices,
            ):

                if (
                    str(outcome).lower()
                    in (
                        "yes",
                        "up",
                    )
                ):

                    probability = (
                        float(price)
                        * 100
                    )

                    if 0 <= probability <= 100:
                        return probability

    except Exception as e:

        logger.debug(
            "Polymarket unavailable: %s",
            e
        )

    return None


# ============================================================
# PRISM
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
    if not PRISM_API_KEY:
        return None

    params = {
        "symbol": f"{symbol}USD",
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
            []
        )

        for candle in candles:

            if (
                candle.get("kind")
                != "future"
            ):
                continue

            probability = candle.get(
                "dir_prob"
            )

            if probability is None:
                continue

            probability = (
                float(probability)
                * 100
            )

            if 0 <= probability <= 100:
                return probability

    except Exception as e:

        logger.debug(
            "PRISM unavailable: %s",
            e
        )

    return None


# ============================================================
# DEGEN SIGNAL
# ============================================================

async def get_degen_probability(
    session,
    symbol,
    timeframe,
):
    """
    Degen Signal only publishes
    5m / 15m predictions for BTC,
    ETH, SOL and XRP.

    No fake value is returned for
    other coins/timeframes.
    """

    if timeframe != "15m":
        return None

    if symbol not in (
        "BTC",
        "ETH",
        "SOL",
        "XRP",
    ):
        return None

    url = (
        f"{DEGEN_BASE_URL}/"
        f"{symbol.lower()}"
    )

    try:

        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(
                total=20
            ),
            headers={
                "User-Agent":
                    "Mozilla/5.0 "
                    "(compatible; RSI-Scanner/1.0)"
            },
        ) as response:

            if response.status != 200:
                return None

            text = await response.text()

        import re

        # Search for:
        # Call UP/DOWN
        # Confidence XX%
        match = re.search(
            r"Call\s*"
            r"(UP|DOWN)"
            r".{0,500}?"
            r"Confidence\s*"
            r"(\d{1,3})%",
            text,
            re.IGNORECASE |
            re.DOTALL,
        )

        if not match:
            match = re.search(
                r"(UP|DOWN)"
                r".{0,200}?"
                r"Confidence\s*"
                r"(\d{1,3})%",
                text,
                re.IGNORECASE |
                re.DOTALL,
            )

        if not match:
            return None

        direction = (
            match.group(1)
            .upper()
        )

        confidence = float(
            match.group(2)
        )

        if direction == "UP":
            return confidence

        return (
            100.0
            - confidence
        )

    except Exception as e:

        logger.debug(
            "Degen Signal unavailable "
            "for %s: %s",
            symbol,
            e,
        )

    return None


# ============================================================
# MULTI-SOURCE PREDICTION
# ============================================================

async def get_prediction_values(
    session,
    symbol,
    timeframe,
):
    """
    فقط داده واقعی منابعی که در لحظه
    قابل دریافت هستند.

    منابع متصل:
      - PRISM
      - Polymarket
      - Degen Signal

    منابعی که API عمومی قابل اتکا ندارند
    عدد ساختگی دریافت نمی‌کنند.
    """

    tasks = [
        get_prism_probability(
            session,
            symbol,
            timeframe,
        ),

        get_polymarket_probability(
            session,
            symbol,
            timeframe,
        ),

        get_degen_probability(
            session,
            symbol,
            timeframe,
        ),
    ]

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    values = []

    for value in results:

        if isinstance(
            value,
            Exception
        ):
            continue

        if value is None:
            continue

        try:
            value = float(value)

            if 0 <= value <= 100:
                values.append(
                    value
                )

        except Exception:
            continue

    return values


# ============================================================
# LOCAL FALLBACK
# ============================================================

def technical_score(
    klines,
    rsi,
):
    if not klines or rsi is None:
        return None

    score = 50.0

    if rsi >= 70:

        score += min(
            20,
            (rsi - 70) * 0.8
        )

    elif rsi <= 30:

        score -= min(
            20,
            (30 - rsi) * 0.8
        )

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
            score
        )
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
                score
            )
        )

    except Exception:
        return None


async def calculate_next_candle(
    session,
    symbol,
    timeframe,
    klines,
    rsi,
):
    # ========================================================
    # NEW:
    # Average real prediction sources
    # ========================================================

    external_values = (
        await get_prediction_values(
            session,
            symbol,
            timeframe,
        )
    )

    if external_values:

        average = (
            sum(external_values)
            / len(external_values)
        )

        logger.info(
            "Prediction %s %s: "
            "sources=%s average=%.2f",
            symbol,
            timeframe,
            len(external_values),
            average,
        )

        return average

    # ========================================================
    # If no external source is available,
    # keep the old local calculation.
    # ========================================================

    technical = technical_score(
        klines,
        rsi,
    )

    momentum = momentum_score(
        klines,
    )

    values = [
        x
        for x in (
            technical,
            momentum,
        )
        if x is not None
    ]

    if not values:
        return 50.0

    return (
        sum(values)
        / len(values)
    )


# ============================================================
# ALERT STATE
# ============================================================

def state_key(
    symbol,
    timeframe,
):
    return (
        f"{symbol}:{timeframe}"
    )


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
        {}
    )

    previous = states.get(
        key
    )

    if previous is None:

        states[key] = zone

        return True

    if previous == zone:
        return False

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

    # ========================================================
    # همان شرط قبلی ۱۰ دقیقه
    # ========================================================

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
        "prediction": prediction,
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

    # همان فیلتر قبلی
    coins = [
        coin
        for coin in top_coins
        if coin in valid_symbols
    ]

    logger.info(
        "Top coins: %s",
        len(coins)
    )

    signals = []

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
# MESSAGE
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

        tf = signal[
            "timeframe"
        ]

        if tf != current_tf:

            blocks.append(
                timeframe_header(
                    tf
                )
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

    return (
        "🚨 RSI Scanner Alert\n\n"
        + "\n\n--------------------\n\n".join(
            blocks
        )
    )


# ============================================================
# SEND WITH DUPLICATE PROTECTION
# ============================================================

async def send_alert_once(
    session,
    state,
    chat_id,
    text,
):
    # ========================================================
    # NEW:
    # Don't send exact same message twice
    # to the same user within 15 minutes.
    # ========================================================

    if already_sent(
        state,
        chat_id,
        text,
    ):
        logger.warning(
            "Duplicate blocked: chat=%s",
            chat_id,
        )

        return False

    # Save BEFORE sending.
    # This reduces duplicate risk if the same
    # scanner cycle gets repeated.
    save_state(state)

    return await send_long_message(
        session,
        chat_id,
        text,
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

        start = (
            asyncio.get_event_loop()
            .time()
        )

        sent_any = False

        while (
            asyncio.get_event_loop().time()
            - start
            < RUN_SECONDS
        ):

            try:

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

                            ok = (
                                await send_alert_once(
                                    session,
                                    state,
                                    chat_id,
                                    message,
                                )
                            )

                            if ok:
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


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:
        pass
