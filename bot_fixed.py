import os
import asyncio
import logging
import json
from pathlib import Path
from datetime import datetime, timezone

import aiohttp


# =========================================================
# CONFIG
# =========================================================

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

LEGACY_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
).strip()

TOP_N = int(
    os.getenv("TOP_N", "100")
)

PERIOD = int(
    os.getenv("RSI_PERIOD", "14")
)

USERS_FILE = Path(
    os.getenv("USERS_FILE", "users.json")
)

# اسکن اصلی در این دقیقه‌ها
SCAN_MINUTES = {
    5,
    20,
    35,
    50
}

# پیام بدون سیگنال بعد از چند ثانیه حذف شود
NO_SIGNAL_DELETE_AFTER = 5

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
)

BINANCE_BASES = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api.binance.com",
]

POLYMARKET_URL = (
    "https://gamma-api.polymarket.com/markets"
)

TFS = {
    "15m": "15m",
    "1H": "1h",
    "4H": "4h",
    "1D": "1d",
}

TF_ORDER = {
    "15m": 0,
    "1H": 1,
    "4H": 2,
    "1D": 3,
}


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )
)


# =========================================================
# RSI
# =========================================================

def calculate_rsi(values, period=14):

    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):

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

    avg_gain = (
        sum(gains[:period])
        / period
    )

    avg_loss = (
        sum(losses[:period])
        / period
    )

    if avg_loss == 0:

        if avg_gain > 0:
            return 100.0

        return 50.0

    for i in range(
        period,
        len(gains)
    ):

        avg_gain = (
            (
                avg_gain
                * (period - 1)
                + gains[i]
            )
            / period
        )

        avg_loss = (
            (
                avg_loss
                * (period - 1)
                + losses[i]
            )
            / period
        )

    if avg_loss == 0:

        if avg_gain > 0:
            return 100.0

        return 50.0

    rs = (
        avg_gain
        / avg_loss
    )

    return (
        100
        - (100 / (1 + rs))
    )


def get_zone(rsi_value):

    if rsi_value < 30:
        return "oversold"

    if rsi_value > 70:
        return "overbought"

    return "neutral"


# =========================================================
# EMA
# =========================================================

def calculate_ema(
    values,
    period
):

    if len(values) < period:
        return None

    multiplier = (
        2 / (period + 1)
    )

    ema = (
        sum(values[:period])
        / period
    )

    for value in values[period:]:

        ema = (
            (
                value - ema
            )
            * multiplier
        ) + ema

    return ema


# =========================================================
# TECHNICAL MODEL
# =========================================================

def technical_model(
    candles,
    rsi_value
):

    if (
        len(candles) < 30
        or rsi_value is None
    ):
        return 50

    closes = [
        c["close"]
        for c in candles
    ]

    score = 50

    current = candles[-1]
    previous = candles[-2]

    # EMA 9 / 21
    ema9 = calculate_ema(
        closes,
        9
    )

    ema21 = calculate_ema(
        closes,
        21
    )

    if (
        ema9 is not None
        and ema21 is not None
    ):

        if ema9 > ema21:
            score += 15

        else:
            score -= 15

    # Momentum
    if (
        current["close"]
        > previous["close"]
    ):
        score += 10

    elif (
        current["close"]
        < previous["close"]
    ):
        score -= 10

    # RSI
    if rsi_value >= 70:

        score -= 20

    elif rsi_value <= 30:

        score += 20

    elif rsi_value >= 55:

        score += 8

    elif rsi_value <= 45:

        score -= 8

    # Short momentum
    if len(closes) >= 6:

        old_price = closes[-6]

        if old_price > 0:

            change = (
                (
                    closes[-1]
                    - old_price
                )
                / old_price
            ) * 100

            if change > 1:
                score += 5

            elif change < -1:
                score -= 5

    return max(
        0,
        min(100, round(score))
    )


# =========================================================
# HTTP
# =========================================================

async def http_get(
    session,
    url,
    params=None,
    retries=3
):

    last_error = None

    for attempt in range(retries):

        try:

            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(
                    total=25
                ),
                headers={
                    "User-Agent":
                        "RSI-Telegram-Scanner/3.0"
                }
            ) as response:

                if response.status == 429:

                    wait = float(
                        response.headers.get(
                            "Retry-After",
                            "3"
                        )
                    )

                    await asyncio.sleep(
                        min(wait, 15)
                    )

                    continue

                if response.status >= 400:

                    text = await response.text()

                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status,
                        message=text[:300],
                        headers=response.headers
                    )

                return await response.json()

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError
        ) as error:

            last_error = error

            if attempt < retries - 1:

                await asyncio.sleep(
                    1.5 * (attempt + 1)
                )

    if last_error:
        raise last_error

    return None


async def binance_get(
    session,
    path,
    params=None
):

    last_error = None

    for base in BINANCE_BASES:

        try:

            return await http_get(
                session,
                base + path,
                params
            )

        except aiohttp.ClientResponseError as error:

            last_error = error

            if error.status not in (
                400,
                403,
                418,
                429,
                451
            ):
                raise

            logging.warning(
                "%s HTTP %s - trying next endpoint",
                base,
                error.status
            )

    if last_error:
        raise last_error

    return None


# =========================================================
# COINGECKO TOP 100
# =========================================================

async def get_top_coins(
    session
):

    data = await http_get(
        session,
        COINGECKO_URL,
        {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": TOP_N,
            "page": 1,
            "sparkline": "false"
        }
    )

    if not data:
        return []

    result = []

    for coin in data:

        symbol = str(
            coin.get(
                "symbol",
                ""
            )
        ).upper().strip()

        if not symbol:
            continue

        result.append({
            "name": coin.get(
                "name",
                symbol
            ),
            "symbol": symbol
        })

    return result


# =========================================================
# BINANCE TICKERS
# =========================================================

async def get_tickers(
    session
):

    data = await binance_get(
        session,
        "/api/v3/ticker/24hr"
    )

    if not data:
        return {}

    return {
        x["symbol"]: x
        for x in data
        if x.get("symbol")
    }


# =========================================================
# KLINES
# =========================================================

async def get_candles(
    session,
    symbol,
    interval
):

    rows = await binance_get(
        session,
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": 200
        }
    )

    if not rows:
        return []

    candles = []

    for row in rows:

        try:

            candles.append({
                "open_time": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "close_time": int(row[6])
            })

        except Exception:
            continue

    return candles


# =========================================================
# VOLUME
# =========================================================

def format_volume(value):

    if value >= 1_000_000_000:

        return (
            f"{value / 1_000_000_000:.2f}B"
        )

    if value >= 1_000_000:

        return (
            f"{value / 1_000_000:.2f}M"
        )

    if value >= 1_000:

        return (
            f"{value / 1_000:.0f}K"
        )

    return f"{value:.0f}"


def volume_analysis(
    candles
):

    if len(candles) < 21:
        return None

    current = candles[-1]["volume"]

    previous = [
        candles[-i]["volume"]
        for i in range(2, 5)
    ]

    # میانگین 20 کندل قبلی
    previous20 = [
        candles[-i]["volume"]
        for i in range(2, 22)
    ]

    if not previous20:
        return None

    average = (
        sum(previous20)
        / len(previous20)
    )

    if average <= 0:
        return None

    ratio = (
        current / average
    )

    if ratio > 1.2:

        status = "🔥 زیاد"

    elif ratio < 0.8:

        status = "📉 کم"

    else:

        status = "➖ معمولی"

    values = [
        current,
        previous[0],
        previous[1],
        previous[2]
    ]

    maximum = max(values)

    if maximum <= 0:
        maximum = 1

    bars = []

    for value in values:

        count = max(
            1,
            round(
                (
                    value
                    / maximum
                ) * 6
            )
        )

        bars.append(
            "🟦" * count
        )

    return {
        "current":
            format_volume(current),

        "prev1":
            format_volume(previous[0]),

        "prev2":
            format_volume(previous[1]),

        "prev3":
            format_volume(previous[2]),

        "bar_current":
            bars[0],

        "bar1":
            bars[1],

        "bar2":
            bars[2],

        "bar3":
            bars[3],

        "status":
            status,

        "average":
            format_volume(average)
    }


# =========================================================
# POLYMARKET
# =========================================================

COIN_NAMES = {

    "BTC": [
        "bitcoin",
        "btc"
    ],

    "ETH": [
        "ethereum",
        "eth"
    ],

    "SOL": [
        "solana",
        "sol"
    ],

    "XRP": [
        "xrp",
        "ripple"
    ],

    "DOGE": [
        "dogecoin",
        "doge"
    ],

    "BNB": [
        "bnb",
        "binance coin"
    ],

    "ADA": [
        "cardano",
        "ada"
    ],

    "AVAX": [
        "avalanche",
        "avax"
    ],

    "LINK": [
        "chainlink",
        "link"
    ],

    "DOT": [
        "polkadot",
        "dot"
    ]
}


def parse_polymarket(
    market
):

    try:

        outcomes = market.get(
            "outcomes"
        )

        prices = market.get(
            "outcomePrices"
        )

        if isinstance(
            outcomes,
            str
        ):

            outcomes = json.loads(
                outcomes
            )

        if isinstance(
            prices,
            str
        ):

            prices = json.loads(
                prices
            )

        if not isinstance(
            outcomes,
            list
        ):
            return None

        if not isinstance(
            prices,
            list
        ):
            return None

        for outcome, price in zip(
            outcomes,
            prices
        ):

            outcome_text = str(
                outcome
            ).lower()

            if "up" not in outcome_text:
                continue

            probability = float(
                price
            )

            if probability <= 1:
                probability *= 100

            return max(
                0,
                min(
                    100,
                    probability
                )
            )

    except Exception:
        return None

    return None


def polymarket_matches(
    market,
    coin,
    timeframe
):

    question = str(
        market.get(
            "question",
            ""
        )
    ).lower()

    if not question:
        return False

    names = COIN_NAMES.get(
        coin.upper(),
        [coin.lower()]
    )

    if not any(
        name in question
        for name in names
    ):
        return False

    if "up or down" not in question:
        return False

    if timeframe == "15m":

        return (
            "5m" in question
            or "15m" in question
            or "15 min" in question
        )

    if timeframe == "1H":

        return (
            "1h" in question
            or "1 hour" in question
            or "hour" in question
        )

    if timeframe == "4H":

        return (
            "4h" in question
            or "4 hour" in question
        )

    if timeframe == "1D":

        return (
            "daily" in question
            or "1d" in question
            or "day" in question
        )

    return False


async def get_polymarket_probability(
    session,
    coin,
    timeframe
):

    # فقط برای ارزهایی که احتمالاً
    # بازار مستقیم دارند
    if coin.upper() not in COIN_NAMES:
        return None

    try:

        data = await http_get(
            session,
            POLYMARKET_URL,
            {
                "closed": "false",
                "limit": 500
            }
        )

    except Exception as error:

        logging.warning(
            "Polymarket error %s: %s",
            coin,
            error
        )

        return None

    if not data:
        return None

    if isinstance(
        data,
        dict
    ):

        markets = data.get(
            "data",
            []
        )

    else:

        markets = data

    candidates = []

    for market in markets:

        if not polymarket_matches(
            market,
            coin,
            timeframe
        ):
            continue

        probability = parse_polymarket(
            market
        )

        if probability is None:
            continue

        candidates.append({
            "market": market,
            "probability":
                probability
        })

    if not candidates:
        return None

    # بازار دارای endDate را ترجیح می‌دهیم
    candidates.sort(
        key=lambda x: str(
            x["market"].get(
                "endDate",
                ""
            )
        )
    )

    return candidates[0][
        "probability"
    ]


# =========================================================
# FINAL MODEL
# =========================================================

def final_prediction(
    technical_score,
    polymarket_probability
):

    if polymarket_probability is None:

        score = technical_score

    else:

        # 70% مدل تکنیکال
        # 30% Polymarket

        score = round(
            (
                technical_score
                * 0.70
            )
            +
            (
                polymarket_probability
                * 0.30
            )
        )

    score = max(
        0,
        min(100, score)
    )

    if score >= 65:

        direction = "🟢 صعودی"

    elif score <= 35:

        direction = "🔴 نزولی"

    else:

        direction = "🟡 خنثی"

    return direction, score


# =========================================================
# STATE
# =========================================================

def load_state():

    if not USERS_FILE.exists():

        return {
            "users": [],
            "offset": 0,
            "signals": {},
            "last_scan_slot": None
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
                    []
                )
            ],

            "offset": int(
                data.get(
                    "offset",
                    0
                )
            ),

            "signals": data.get(
                "signals",
                {}
            ),

            "last_scan_slot":
                data.get(
                    "last_scan_slot"
                )
        }

    except Exception as error:

        logging.warning(
            "State read error: %s",
            error
        )

        return {
            "users": [],
            "offset": 0,
            "signals": {},
            "last_scan_slot": None
        }


def save_state(
    state
):

    USERS_FILE.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2
        )
        + "\n",
        encoding="utf-8"
    )


# =========================================================
# TELEGRAM
# =========================================================

async def telegram(
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
        timeout=aiohttp.ClientTimeout(
            total=20
        )
    ) as response:

        text = await response.text()

        if response.status >= 400:

            raise RuntimeError(
                f"Telegram HTTP "
                f"{response.status}: "
                f"{text[:500]}"
            )

        data = json.loads(text)

        if not data.get("ok"):

            raise RuntimeError(
                f"Telegram API error: "
                f"{text[:500]}"
            )

        return data.get(
            "result"
        )


# =========================================================
# COMMANDS
# =========================================================

async def process_commands(
    session,
    state
):

    try:

        updates = await telegram(
            session,
            "getUpdates",
            {
                "offset":
                    state.get(
                        "offset",
                        0
                    ),

                "timeout":
                    0,

                "allowed_updates":
                    ["message"]
            }
        )

    except Exception as error:

        logging.warning(
            "Telegram getUpdates error: %s",
            error
        )

        return state

    changed = False

    for update in updates or []:

        update_id = int(
            update.get(
                "update_id",
                0
            )
        )

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

        chat_id = str(
            chat.get(
                "id",
                ""
            )
        )

        if not chat_id:
            continue

        raw_text = str(
            message.get(
                "text",
                ""
            )
        ).strip()

        command = (
            raw_text
            .lower()
            .split()[0]
            if raw_text
            else ""
        )

        # -------------------------
        # START
        # -------------------------

        if command.startswith(
            "/start"
        ):

            if chat_id not in state[
                "users"
            ]:

                state[
                    "users"
                ].append(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id":
                            chat_id,

                        "text":
                            "✅ <b>RSI Scanner فعال شد.</b>\n\n"
                            "📊 Top 100 کریپتو\n"
                            "⏱ 15m | 1H | 4H | 1D\n"
                            "🎯 RSI(14)\n"
                            "🧠 مدل تکنیکال\n"
                            "🎯 Polymarket\n"
                            "📦 تحلیل حجم\n\n"
                            "ربات فقط تحلیل و هشدار ارسال می‌کند "
                            "و هیچ معامله‌ای انجام نمی‌دهد.",

                        "parse_mode":
                            "HTML"
                    }
                )

            else:

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id":
                            chat_id,

                        "text":
                            "ℹ️ شما از قبل فعال هستید."
                    }
                )

        # -------------------------
        # STOP
        # -------------------------

        elif command.startswith(
            "/stop"
        ):

            if chat_id in state[
                "users"
            ]:

                state[
                    "users"
                ].remove(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id":
                            chat_id,

                        "text":
                            "⛔ هشدارها متوقف شد.\n"
                            "برای فعال‌سازی دوباره /start را بزنید."
                    }
                )

        # -------------------------
        # STATUS
        # -------------------------

        elif command.startswith(
            "/status"
        ):

            status = (
                "فعال ✅"
                if chat_id in state[
                    "users"
                ]
                else
                "غیرفعال ⛔"
            )

            await telegram(
                session,
                "sendMessage",
                {
                    "chat_id":
                        chat_id,

                    "text":
                        f"📡 وضعیت ربات: {status}"
                }
            )

        # -------------------------
        # TEST
        # -------------------------

        elif command.startswith(
            "/test"
        ):

            await telegram(
                session,
                "sendMessage",
                {
                    "chat_id":
                        chat_id,

                    "text":
                        "🧪 <b>تست موفق بود.</b>\n\n"
                        "ربات می‌تواند به تلگرام پیام ارسال کند.",
                    "parse_mode":
                        "HTML"
                }
            )

    # Legacy chat ID
    if (
        LEGACY_CHAT_ID
        and LEGACY_CHAT_ID
        not in state["users"]
    ):

        state[
            "users"
        ].append(
            LEGACY_CHAT_ID
        )

        changed = True

    if changed:

        save_state(
            state
        )

    return state


# =========================================================
# SIGNAL STATE
# =========================================================

def is_new_signal(
    state,
    symbol,
    timeframe,
    candle_open,
    zone
):

    key = (
        f"{symbol}|{timeframe}"
    )

    signals = state.setdefault(
        "signals",
        {}
    )

    old = signals.get(
        key
    )

    # اگر RSI به حالت عادی برگشته
    if zone == "neutral":

        signals.pop(
            key,
            None
        )

        return False

    # اولین سیگنال
    if old is None:

        signals[key] = {
            "candle":
                candle_open,

            "zone":
                zone
        }

        return True

    # کندل جدید
    if old.get(
        "candle"
    ) != candle_open:

        signals[key] = {
            "candle":
                candle_open,

            "zone":
                zone
        }

        return True

    # همان کندل و همان محدوده
    if old.get(
        "zone"
    ) == zone:

        return False

    # تغییر اشباع خرید به فروش یا برعکس
    signals[key] = {
        "candle":
            candle_open,

        "zone":
            zone
    }

    return True


# =========================================================
# SCAN ONE TIMEFRAME
# =========================================================

async def scan_timeframe(
    session,
    coin,
    timeframe,
    interval,
    state
):

    symbol = (
        coin["symbol"]
        + "USDT"
    )

    try:

        candles = await get_candles(
            session,
            symbol,
            interval
        )

    except Exception as error:

        logging.warning(
            "%s %s candle error: %s",
            symbol,
            timeframe,
            error
        )

        return None

    if len(candles) < PERIOD + 5:
        return None

    # مهم:
    # کندل آخر = کندل فعلی و باز
    closes = [
        c["close"]
        for c in candles
    ]

    rsi_value = calculate_rsi(
        closes,
        PERIOD
    )

    if rsi_value is None:
        return None

    zone = get_zone(
        rsi_value
    )

    candle_open = candles[
        -1
    ]["open_time"]

    # سیگنال جدید؟
    if not is_new_signal(
        state,
        symbol,
        timeframe,
        candle_open,
        zone
    ):

        return None

    # فقط اشباع خرید/فروش
    if zone not in (
        "overbought",
        "oversold"
    ):

        return None

    # مدل تکنیکال
    technical_score = (
        technical_model(
            candles,
            rsi_value
        )
    )

    # Polymarket
    polymarket = (
        await get_polymarket_probability(
            session,
            coin["symbol"],
            timeframe
        )
    )

    # مدل نهایی
    direction, final_score = (
        final_prediction(
            technical_score,
            polymarket
        )
    )

    volume = volume_analysis(
        candles
    )

    return {
        "symbol":
            symbol,

        "name":
            coin["name"],

        "tf":
            timeframe,

        "rsi":
            rsi_value,

        "zone":
            zone,

        "direction":
            direction,

        "score":
            final_score,

        "technical":
            technical_score,

        "polymarket":
            polymarket,

        "volume":
            volume
    }


# =========================================================
# SCAN ONE COIN
# =========================================================

async def scan_coin(
    session,
    coin,
    ticker,
    semaphore,
    state
):

    symbol = (
        coin["symbol"]
        + "USDT"
    )

    if symbol not in ticker:
        return []

    results = []

    async def run(
        timeframe,
        interval
    ):

        async with semaphore:

            try:

                result = (
                    await scan_timeframe(
                        session,
                        coin,
                        timeframe,
                        interval,
                        state
                    )
                )

                return result

            except Exception as error:

                logging.warning(
                    "%s %s error: %s",
                    symbol,
                    timeframe,
                    error
                )

                return None

    tasks = [
        run(
            timeframe,
            interval
        )
        for timeframe, interval
        in TFS.items()
    ]

    values = await asyncio.gather(
        *tasks
    )

    for value in values:

        if value:
            results.append(
                value
            )

    return results


# =========================================================
# MESSAGE
# =========================================================

def build_alert(
    alert
):

    rsi_value = alert[
        "rsi"
    ]

    if rsi_value > 70:

        rsi_line = (
            f"🔴 RSI: "
            f"{rsi_value:.2f} | اشباع خرید"
        )

    else:

        rsi_line = (
            f"🟢 RSI: "
            f"{rsi_value:.2f} | اشباع فروش"
        )

    volume = alert[
        "volume"
    ]

    lines = []

    lines.append(
        f"💠 <b>{alert['symbol']}</b>"
    )

    lines.append("")

    lines.append(
        rsi_line
    )

    lines.append("")

    lines.append(
        "🔮 <b>تمایل کندل بعدی:</b>"
    )

    lines.append(
        f"{alert['direction']} — "
        f"{alert['score']}%"
    )

    lines.append("")

    lines.append(
        f"🧠 مدل تکنیکال: "
        f"{alert['technical']}%"
    )

    if alert[
        "polymarket"
    ] is not None:

        pm = alert[
            "polymarket"
        ]

        pm_direction = (
            "صعودی"
            if pm >= 50
            else "نزولی"
        )

        lines.append(
            f"🎯 Polymarket: "
            f"{pm:.0f}% "
            f"{pm_direction}"
        )

    else:

        lines.append(
            "🎯 Polymarket: —"
        )

    if volume:

        lines.append(
            f"📊 حجم: "
            f"{volume['status']}"
        )

        lines.append("")

        lines.append(
            f"📦 {volume['bar_current']} "
            f"{volume['current']}"
        )

        lines.append(
            f"1️⃣ {volume['bar1']} "
            f"{volume['prev1']}"
        )

        lines.append(
            f"2️⃣ {volume['bar2']} "
            f"{volume['prev2']}"
        )

        lines.append(
            f"3️⃣ {volume['bar3']} "
            f"{volume['prev3']}"
        )

        lines.append("")

        lines.append(
            f"{volume['status']} | "
            f"📊 میانگین: "
            f"{volume['average']}"
        )

    lines.append("")

    # لینک TradingView کوتاه
    tv = (
        f"https://www.tradingview.com/"
        f"symbol/BINANCE-{alert['symbol']}/"
    )

    lines.append(
        f'<a href="{tv}">📈 TV</a>'
    )

    return "\n".join(
        lines
    )


def build_message(
    alerts
):

    grouped = {}

    for alert in alerts:

        grouped.setdefault(
            alert["tf"],
            []
        ).append(
            alert
        )

    parts = []

    for timeframe in sorted(
        grouped,
        key=lambda x:
            TF_ORDER.get(x, 99)
    ):

        parts.append(
            f"━━━━━━━━ {timeframe} ━━━━━━━━"
        )

        for alert in sorted(
            grouped[timeframe],
            key=lambda x:
                x["symbol"]
        ):

            parts.append(
                build_alert(
                    alert
                )
            )

    return "\n\n".join(
        parts
    )


# =========================================================
# SEND ALERTS
# =========================================================

async def send_alerts(
    session,
    state,
    alerts
):

    users = list(
        dict.fromkeys(
            state.get(
                "users",
                []
            )
        )
    )

    if not users:

        logging.warning(
            "No Telegram subscribers."
        )

        return

    if alerts:

        text = build_message(
            alerts
        )

        for chat_id in users:

            try:

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id":
                            chat_id,

                        "text":
                            text,

                        "parse_mode":
                            "HTML",

                        "disable_web_page_preview":
                            True
                    }
                )

            except Exception as error:

                logging.warning(
                    "Send error %s: %s",
                    chat_id,
                    error
                )

    else:

        # پیام بدون سیگنال
        # و حذف خودکار بعد از 5 ثانیه

        async def no_signal(
            chat_id
        ):

            try:

                result = await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id":
                            chat_id,

                        "text":
                            "⚪ <b>سیگنالی یافت نشد.</b>",

                        "parse_mode":
                            "HTML"
                    }
                )

                if not result:
                    return

                message_id = result.get(
                    "message_id"
                )

                if not message_id:
                    return

                await asyncio.sleep(
                    NO_SIGNAL_DELETE_AFTER
                )

                try:

                    await telegram(
                        session,
                        "deleteMessage",
                        {
                            "chat_id":
                                chat_id,

                            "message_id":
                                message_id
                        }
                    )

                except Exception as error:

                    logging.warning(
                        "Delete message error: %s",
                        error
                    )

            except Exception as error:

                logging.warning(
                    "No-signal message error: %s",
                    error
                )

        await asyncio.gather(
            *[
                no_signal(chat_id)
                for chat_id in users
            ]
        )


# =========================================================
# SCAN SLOT
# =========================================================

def get_scan_slot():

    now = datetime.now(
        timezone.utc
    )

    minute = now.minute

    # GitHub ممکن است چند دقیقه دیر اجرا شود.
    # بنابراین برای هر اسلات 5 دقیقه بازه داریم.

    if 5 <= minute < 10:

        scan_minute = 5

    elif 20 <= minute < 25:

        scan_minute = 20

    elif 35 <= minute < 40:

        scan_minute = 35

    elif 50 <= minute:

        scan_minute = 50

    else:

        return None

    return (
        f"{now.strftime('%Y-%m-%d')}-"
        f"{now.hour:02d}-"
        f"{scan_minute:02d}"
    )


def should_scan(
    state
):

    slot = get_scan_slot()

    if slot is None:

        return False, None

    if (
        state.get(
            "last_scan_slot"
        )
        == slot
    ):

        return False, slot

    return True, slot


# =========================================================
# MARKET SCAN
# =========================================================

async def run_scan(
    session,
    state
):

    coins = await get_top_coins(
        session
    )

    logging.info(
        "Top coins: %s",
        len(coins)
    )

    if not coins:
        return []

    tickers = await get_tickers(
        session
    )

    logging.info(
        "Binance tickers: %s",
        len(tickers)
    )

    semaphore = asyncio.Semaphore(
        12
    )

    tasks = [
        scan_coin(
            session,
            coin,
            tickers,
            semaphore,
            state
        )
        for coin in coins
    ]

    groups = await asyncio.gather(
        *tasks
    )

    alerts = []

    for group in groups:
        alerts.extend(
            group
        )

    logging.info(
        "New alerts: %s",
        len(alerts)
    )

    return alerts


# =========================================================
# MAIN
# =========================================================

async def main():

    if not TOKEN:

        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN"
        )

    state = load_state()

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=40
        )
    ) as session:

        # اول دستورات Telegram
        state = await process_commands(
            session,
            state
        )

        # بررسی زمان اسکن
        do_scan, slot = should_scan(
            state
        )

        if not do_scan:

            logging.info(
                "No scan slot."
            )

            save_state(
                state
            )

            return

        logging.info(
            "Starting scan slot: %s",
            slot
        )

        # همینجا slot ثبت می‌شود
        # تا اجرای دوباره همان slot
        # دوباره اسکن نکند.

        state[
            "last_scan_slot"
        ] = slot

        save_state(
            state
        )

        alerts = await run_scan(
            session,
            state
        )

        # ذخیره سیگنال‌ها
        save_state(
            state
        )

        # دقیقاً یک نتیجه برای کاربر
        await send_alerts(
            session,
            state,
            alerts
        )

        save_state(
            state
        )

        logging.info(
            "Scan completed successfully."
        )


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except Exception:

        logging.exception(
            "FATAL ERROR"
        )

        raise
