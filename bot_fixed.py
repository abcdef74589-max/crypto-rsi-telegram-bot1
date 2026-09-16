import asyncio
import json
import logging
import os
import time
from statistics import mean

import aiohttp


# =========================================================
# CONFIG
# =========================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

TOP_N = int(os.getenv("TOP_N", "100"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))

BINANCE_BASE = "https://api.binance.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
POLYMARKET_BASE = "https://gamma-api.polymarket.com"

SCAN_MINUTES = {5, 20, 35, 50}

TIMEFRAMES = {
    "15m": "15m",
    "1H": "1h",
    "4H": "4h",
    "1D": "1d",
}

TIMEFRAME_ORDER = [
    "15m",
    "1H",
    "4H",
    "1D",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("RSI-SCANNER")


# =========================================================
# STATE
# =========================================================

def load_state():
    default = {
        "users": [],
        "offset": 0,
        "signals": {},
        "last_scan_slot": None,
    }

    try:
        with open("users.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return default

        data.setdefault("users", [])
        data.setdefault("offset", 0)
        data.setdefault("signals", {})
        data.setdefault("last_scan_slot", None)

        return data

    except Exception:
        return default


def save_state(state):
    with open("users.json", "w", encoding="utf-8") as f:
        json.dump(
            state,
            f,
            ensure_ascii=False,
            indent=2,
        )


# =========================================================
# HTTP
# =========================================================

async def http_get(
    session,
    url,
    params=None,
    timeout=20,
):
    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(
                total=timeout
            ),
            headers={
                "User-Agent": "RSI-Telegram-Scanner/1.0"
            },
        ) as response:

            if response.status != 200:
                logger.warning(
                    "HTTP %s: %s",
                    response.status,
                    url,
                )
                return None

            return await response.json()

    except Exception as e:
        logger.warning(
            "HTTP error: %s",
            e,
        )
        return None


# =========================================================
# COINGECKO TOP 100
# =========================================================

async def get_top_coins(session):
    url = f"{COINGECKO_BASE}/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_N,
        "page": 1,
        "sparkline": "false",
    }

    data = await http_get(
        session,
        url,
        params=params,
        timeout=30,
    )

    if not data:
        return []

    coins = []

    for coin in data:
        symbol = str(
            coin.get("symbol", "")
        ).upper().strip()

        name = str(
            coin.get("name", "")
        ).strip()

        if symbol:
            coins.append({
                "symbol": symbol,
                "name": name,
            })

    logger.info(
        "Top coins: %s",
        len(coins),
    )

    return coins


# =========================================================
# BINANCE
# =========================================================

async def get_binance_tickers(session):
    url = f"{BINANCE_BASE}/api/v3/ticker/24hr"

    data = await http_get(
        session,
        url,
        timeout=30,
    )

    if not data:
        return {}

    result = {}

    for item in data:

        symbol = item.get(
            "symbol"
        )

        if not symbol:
            continue

        if not symbol.endswith(
            "USDT"
        ):
            continue

        result[symbol] = item

    logger.info(
        "Binance USDT tickers: %s",
        len(result),
    )

    return result


async def get_klines(
    session,
    symbol,
    interval,
    limit=100,
):
    url = f"{BINANCE_BASE}/api/v3/klines"

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    data = await http_get(
        session,
        url,
        params=params,
        timeout=20,
    )

    if not data:
        return []

    candles = []

    for k in data:
        try:
            candles.append({
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "close_time": int(k[6]),
            })
        except Exception:
            continue

    return candles


# =========================================================
# RSI
# =========================================================

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
            closes[i]
            - closes[i - 1]
        )

        if change > 0:
            gains.append(change)
            losses.append(0)

        else:
            gains.append(0)
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
        len(gains),
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

    return 100 - (
        100 / (1 + rs)
    )


# =========================================================
# EMA
# =========================================================

def calculate_ema(
    values,
    period,
):
    if len(values) < period:
        return None

    multiplier = 2 / (
        period + 1
    )

    ema = (
        sum(values[:period])
        / period
    )

    for value in values[period:]:

        ema = (
            (value - ema)
            * multiplier
        ) + ema

    return ema


# =========================================================
# TECHNICAL MODEL
# =========================================================

def technical_model(
    candles,
    rsi,
):
    if (
        len(candles) < 30
        or rsi is None
    ):
        return "🟡 خنثی", 50

    closes = [
        c["close"]
        for c in candles
    ]

    current = candles[-1]
    previous = candles[-2]

    score = 50

    # EMA 9 / EMA 21
    ema9 = calculate_ema(
        closes,
        9,
    )

    ema21 = calculate_ema(
        closes,
        21,
    )

    if (
        ema9 is not None
        and ema21 is not None
    ):

        if ema9 > ema21:
            score += 15
        else:
            score -= 15

    # آخرین حرکت قیمت
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
    if rsi >= 70:
        score -= 20

    elif rsi <= 30:
        score += 20

    elif rsi >= 55:
        score += 8

    elif rsi <= 45:
        score -= 8

    # Momentum
    if len(closes) >= 6:

        old_price = closes[-6]

        if old_price != 0:

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

    score = max(
        0,
        min(100, score),
    )

    if score >= 65:
        return "🟢 صعودی", score

    if score <= 35:
        return "🔴 نزولی", score

    return "🟡 خنثی", score


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


def get_volume_info(candles):

    if len(candles) < 21:
        return None

    current_volume = (
        candles[-1]["volume"]
    )

    previous_20 = [
        c["volume"]
        for c in candles[-21:-1]
    ]

    if not previous_20:
        return None

    average = mean(
        previous_20
    )

    if average <= 0:
        return None

    ratio = (
        current_volume
        / average
    )

    if ratio > 1.2:
        status = "🔥 زیاد"

    elif ratio < 0.8:
        status = "📉 کم"

    else:
        status = "➖ معمولی"

    values = [
        current_volume,
        candles[-2]["volume"],
        candles[-3]["volume"],
        candles[-4]["volume"],
    ]

    max_value = max(values)

    if max_value <= 0:
        max_value = 1

    bars = []

    for value in values:

        count = max(
            1,
            round(
                (
                    value
                    / max_value
                ) * 6
            ),
        )

        bars.append(
            "🟦" * count
        )

    return {
        "current": format_volume(
            current_volume
        ),
        "previous1": format_volume(
            candles[-2]["volume"]
        ),
        "previous2": format_volume(
            candles[-3]["volume"]
        ),
        "previous3": format_volume(
            candles[-4]["volume"]
        ),
        "bar_current": bars[0],
        "bar1": bars[1],
        "bar2": bars[2],
        "bar3": bars[3],
        "status": status,
        "average": format_volume(
            average
        ),
    }


# =========================================================
# POLYMARKET
# =========================================================

COIN_NAMES = {

    "BTC": [
        "bitcoin",
        "btc",
    ],

    "ETH": [
        "ethereum",
        "eth",
    ],

    "SOL": [
        "solana",
        "sol",
    ],

    "XRP": [
        "xrp",
        "ripple",
    ],

    "DOGE": [
        "dogecoin",
        "doge",
    ],

    "BNB": [
        "bnb",
        "binance coin",
    ],

    "ADA": [
        "cardano",
        "ada",
    ],

    "AVAX": [
        "avalanche",
        "avax",
    ],

    "LINK": [
        "chainlink",
        "link",
    ],

    "DOT": [
        "polkadot",
        "dot",
    ],
}


def parse_polymarket_probability(
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
            str,
        ):
            outcomes = json.loads(
                outcomes
            )

        if isinstance(
            prices,
            str,
        ):
            prices = json.loads(
                prices
            )

        if not isinstance(
            outcomes,
            list,
        ):
            return None

        if not isinstance(
            prices,
            list,
        ):
            return None

        for outcome, price in zip(
            outcomes,
            prices,
        ):

            text = str(
                outcome
            ).lower()

            if "up" in text:

                probability = float(
                    price
                )

                if probability <= 1:
                    probability *= 100

                return max(
                    0,
                    min(
                        100,
                        probability,
                    ),
                )

    except Exception:
        return None

    return None


def market_matches(
    market,
    coin_symbol,
    timeframe,
):
    question = str(
        market.get(
            "question",
            "",
        )
    ).lower()

    if not question:
        return False

    names = COIN_NAMES.get(
        coin_symbol.upper(),
        [coin_symbol.lower()],
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
    coin_symbol,
    timeframe,
):

    if (
        coin_symbol.upper()
        not in COIN_NAMES
    ):
        return None

    url = (
        f"{POLYMARKET_BASE}/markets"
    )

    params = {
        "closed": "false",
        "limit": 500,
    }

    data = await http_get(
        session,
        url,
        params=params,
        timeout=20,
    )

    if not data:
        return None

    if isinstance(
        data,
        dict,
    ):
        markets = data.get(
            "data",
            [],
        )
    else:
        markets = data

    candidates = []

    for market in markets:

        if not market_matches(
            market,
            coin_symbol,
            timeframe,
        ):
            continue

        probability = (
            parse_polymarket_probability(
                market
            )
        )

        if probability is None:
            continue

        candidates.append(
            (
                market,
                probability,
            )
        )

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: str(
            x[0].get(
                "endDate",
                "",
            )
        )
    )

    return candidates[0][1]


# =========================================================
# COMBINED MODEL
# =========================================================

def combined_prediction(
    technical_score,
    polymarket_probability,
):

    if polymarket_probability is None:

        final_score = round(
            technical_score
        )

    else:

        # Technical 70%
        # Polymarket 30%

        final_score = round(
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

    final_score = max(
        0,
        min(
            100,
            final_score,
        ),
    )

    if final_score >= 65:
        direction = "🟢 صعودی"

    elif final_score <= 35:
        direction = "🔴 نزولی"

    else:
        direction = "🟡 خنثی"

    return direction, final_score


# =========================================================
# TRADINGVIEW
# =========================================================

def tradingview_url(symbol):

    return (
        "https://www.tradingview.com/"
        f"symbol/BINANCE-{symbol}/"
    )


# =========================================================
# RSI ZONE
# =========================================================

def get_rsi_zone(rsi):

    if rsi > 70:
        return "overbought"

    if rsi < 30:
        return "oversold"

    return None


def rsi_status(rsi):

    if rsi > 70:
        return "🔴 اشباع خرید"

    if rsi < 30:
        return "🟢 اشباع فروش"

    return "⚪ عادی"


# =========================================================
# SIGNAL DEDUPLICATION
# =========================================================

def filter_new_signal(
    state,
    symbol,
    timeframe,
    candle_open,
    zone,
):

    key = (
        f"{symbol}|{timeframe}"
    )

    signals = state.setdefault(
        "signals",
        {},
    )

    previous = signals.get(
        key
    )

    # RSI خارج از محدوده
    if zone is None:

        if previous is not None:
            signals.pop(
                key,
                None,
            )

        return False

    # اولین سیگنال
    if previous is None:

        signals[key] = {
            "candle": candle_open,
            "zone": zone,
        }

        return True

    # کندل جدید
    if (
        previous.get("candle")
        != candle_open
    ):

        signals[key] = {
            "candle": candle_open,
            "zone": zone,
        }

        return True

    # همان کندل و همان محدوده
    if (
        previous.get("zone")
        == zone
    ):
        return False

    # تغییر از خرید به فروش یا برعکس
    signals[key] = {
        "candle": candle_open,
        "zone": zone,
    }

    return True


# =========================================================
# SCAN COIN
# =========================================================

async def scan_coin(
    session,
    coin,
    binance_tickers,
    state,
):

    base_symbol = (
        coin["symbol"]
        .upper()
    )

    symbol = (
        f"{base_symbol}USDT"
    )

    if symbol not in binance_tickers:
        return []

    alerts = []

    for timeframe, interval in TIMEFRAMES.items():

        candles = await get_klines(
            session,
            symbol,
            interval,
            limit=100,
        )

        if len(candles) < 30:
            continue

        closes = [
            c["close"]
            for c in candles
        ]

        # کندل باز فعلی
        rsi = calculate_rsi(
            closes,
            RSI_PERIOD,
        )

        if rsi is None:
            continue

        zone = get_rsi_zone(
            rsi
        )

        candle_open = candles[-1][
            "open_time"
        ]

        # اگر RSI نرمال است
        if zone is None:

            filter_new_signal(
                state,
                symbol,
                timeframe,
                candle_open,
                None,
            )

            continue

        is_new = filter_new_signal(
            state,
            symbol,
            timeframe,
            candle_open,
            zone,
        )

        if not is_new:
            continue

        # مدل تکنیکال
        (
            technical_direction,
            technical_score,
        ) = technical_model(
            candles,
            rsi,
        )

        # Polymarket
        polymarket_probability = (
            await get_polymarket_probability(
                session,
                base_symbol,
                timeframe,
            )
        )

        # مدل نهایی
        (
            final_direction,
            final_score,
        ) = combined_prediction(
            technical_score,
            polymarket_probability,
        )

        volume = get_volume_info(
            candles
        )

        alerts.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "rsi": rsi,
            "rsi_status": rsi_status(rsi),
            "direction": final_direction,
            "score": final_score,
            "technical_score": technical_score,
            "polymarket": polymarket_probability,
            "volume": volume,
        })

    return alerts


# =========================================================
# MESSAGE
# =========================================================

def build_alert_message(
    alert
):

    symbol = alert["symbol"]
    rsi = alert["rsi"]

    direction = alert[
        "direction"
    ]

    score = alert["score"]

    technical_score = alert[
        "technical_score"
    ]

    polymarket = alert[
        "polymarket"
    ]

    volume = alert[
        "volume"
    ]

    lines = []

    lines.append(
        f"💠 <b>{symbol}</b>"
    )

    lines.append("")

    # دقیقاً فرمت موردنظر
    lines.append(
        f"{alert['rsi_status'].split(' ')[0]} "
        f"RSI: {rsi:.2f} | "
        f"{'اشباع خرید' if rsi > 70 else 'اشباع فروش'}"
    )

    lines.append("")

    lines.append(
        "🔮 <b>تمایل کندل بعدی:</b>"
    )

    lines.append(
        f"{direction} — {score}%"
    )

    lines.append("")

    lines.append(
        f"🧠 مدل تکنیکال: "
        f"{technical_score}%"
    )

    if polymarket is not None:

        pm_direction = (
            "صعودی"
            if polymarket >= 50
            else "نزولی"
        )

        lines.append(
            f"🎯 Polymarket: "
            f"{polymarket:.0f}% "
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
            f"📦 "
            f"{volume['bar_current']} "
            f"{volume['current']}"
        )

        lines.append(
            f"1️⃣ "
            f"{volume['bar1']} "
            f"{volume['previous1']}"
        )

        lines.append(
            f"2️⃣ "
            f"{volume['bar2']} "
            f"{volume['previous2']}"
        )

        lines.append(
            f"3️⃣ "
            f"{volume['bar3']} "
            f"{volume['previous3']}"
        )

        lines.append("")

        lines.append(
            f"{volume['status']} | "
            f"📊 میانگین: "
            f"{volume['average']}"
        )

    lines.append("")

    # فقط TV دیده می‌شود
    tv_url = tradingview_url(
        symbol
    )

    lines.append(
        f'<a href="{tv_url}">📈 TV</a>'
    )

    return "\n".join(lines)


def build_full_message(
    alerts
):

    groups = {
        tf: []
        for tf in TIMEFRAME_ORDER
    }

    for alert in alerts:

        groups.setdefault(
            alert["timeframe"],
            [],
        ).append(alert)

    parts = []

    for timeframe in TIMEFRAME_ORDER:

        items = groups.get(
            timeframe,
            [],
        )

        if not items:
            continue

        parts.append(
            f"━━━━━━━━ {timeframe} ━━━━━━━━"
        )

        for alert in items:

            parts.append(
                build_alert_message(
                    alert
                )
            )

    return "\n\n".join(parts)


# =========================================================
# TELEGRAM
# =========================================================

async def telegram_request(
    session,
    method,
    params=None,
):

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/"
        f"{method}"
    )

    try:

        async with session.post(
            url,
            data=params or {},
            timeout=aiohttp.ClientTimeout(
                total=20
            ),
        ) as response:

            data = await response.json()

            if not data.get("ok"):

                logger.warning(
                    "Telegram error: %s",
                    data,
                )

            return data

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

    return await telegram_request(
        session,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
    )


async def send_message_to_all(
    session,
    state,
    text,
):

    users = state.get(
        "users",
        [],
    )

    for chat_id in list(users):

        await send_message(
            session,
            chat_id,
            text,
        )

        await asyncio.sleep(
            0.05
        )


# =========================================================
# TELEGRAM COMMANDS
# =========================================================

async def process_commands(
    session,
    state,
):

    result = await telegram_request(
        session,
        "getUpdates",
        {
            "offset": state.get(
                "offset",
                0,
            ),
            "timeout": 1,
            "allowed_updates": json.dumps(
                ["message"]
            ),
        },
    )

    if (
        not result
        or not result.get("ok")
    ):
        return

    updates = result.get(
        "result",
        [],
    )

    for update in updates:

        state["offset"] = (
            update["update_id"]
            + 1
        )

        message = update.get(
            "message"
        )

        if not message:
            continue

        text = str(
            message.get(
                "text",
                "",
            )
        ).strip()

        chat = message.get(
            "chat",
            {},
        )

        chat_id = chat.get(
            "id"
        )

        if not chat_id:
            continue

        if text.startswith(
            "/start"
        ):

            if (
                chat_id
                not in state["users"]
            ):
                state["users"].append(
                    chat_id
                )

            await send_message(
                session,
                chat_id,
                "✅ <b>RSI Scanner فعال شد.</b>\n\n"
                "📊 Top 100 کریپتو\n"
                "⏱ 15m | 1H | 4H | 1D\n"
                "🎯 RSI(14)\n"
                "🧠 مدل تکنیکال\n"
                "🎯 Polymarket\n"
                "📦 تحلیل حجم\n\n"
                "ربات فقط تحلیل و هشدار ارسال می‌کند و "
                "هیچ معامله‌ای انجام نمی‌دهد.",
            )

    save_state(state)


# =========================================================
# RELIABLE SCAN SLOT
# =========================================================

def get_current_scan_slot():

    now = time.gmtime()

    minute = now.tm_min

    eligible = [
        m
        for m in SCAN_MINUTES
        if m <= minute
    ]

    if not eligible:
        # آخرین اسلات ساعت قبل
        hour = now.tm_hour - 1
        day = now.tm_yday
        year = now.tm_year

        if hour < 0:
            hour = 23
            day -= 1

        return (
            f"{year}-"
            f"{day:03d}-"
            f"{hour:02d}-"
            f"50"
        )

    scan_minute = max(
        eligible
    )

    return (
        f"{now.tm_year}-"
        f"{now.tm_yday:03d}-"
        f"{now.tm_hour:02d}-"
        f"{scan_minute:02d}"
    )


def should_scan(state):

    now = time.gmtime()

    minute = now.tm_min

    # فقط نزدیک اسلات‌های موردنظر
    # هر اجرای 5 دقیقه یکبار این را بررسی می‌کند.
    if minute not in SCAN_MINUTES:

        # برای جبران تأخیر GitHub:
        # اگر از اسلات عبور کرده‌ایم،
        # همان اسلات آخر را بررسی می‌کنیم.
        pass

    slot = get_current_scan_slot()

    last_slot = state.get(
        "last_scan_slot"
    )

    if slot == last_slot:

        return (
            False,
            slot,
        )

    # فقط اگر دقیقه در محدوده منطقی
    # اجرای 5 دقیقه‌ای باشد.
    #
    # مثلاً اگر GitHub در 07 اجرا شد
    # اسلات 05 را انجام می‌دهد.
    #
    # اما اگر ساعت 08 اجرا شود،
    # دیگر اسلات 05 مربوط به گذشته
    # و باید اجرای بعدی 10 باشد.
    #
    # چون Workflow هر 5 دقیقه است،
    # این حالت معمولاً رخ نمی‌دهد.

    return (
        True,
        slot,
    )


# =========================================================
# REAL SCAN
# =========================================================

async def run_market_scan(
    session,
    state,
):

    logger.info(
        "Starting market scan..."
    )

    top_coins = (
        await get_top_coins(
            session
        )
    )

    binance_tickers = (
        await get_binance_tickers(
            session
        )
    )

    if not top_coins:

        logger.error(
            "Top coins unavailable."
        )

        return []

    alerts = []

    semaphore = asyncio.Semaphore(
        10
    )

    async def scan_limited(
        coin
    ):

        async with semaphore:

            try:

                return await scan_coin(
                    session,
                    coin,
                    binance_tickers,
                    state,
                )

            except Exception as e:

                logger.exception(
                    "Error scanning %s: %s",
                    coin.get(
                        "symbol"
                    ),
                    e,
                )

                return []

    results = await asyncio.gather(
        *[
            scan_limited(coin)
            for coin in top_coins
        ]
    )

    for result in results:
        alerts.extend(result)

    logger.info(
        "New alerts: %s",
        len(alerts),
    )

    return alerts


# =========================================================
# MAIN
# =========================================================

async def main():

    if not TELEGRAM_BOT_TOKEN:

        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is not set"
        )

    state = load_state()

    connector = aiohttp.TCPConnector(
        limit=30
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        # اول دستورات تلگرام
        await process_commands(
            session,
            state,
        )

        # بررسی اینکه نوبت اسکن رسیده یا نه
        do_scan, slot = (
            should_scan(
                state
            )
        )

        if not do_scan:

            logger.info(
                "No new scan slot."
            )

            save_state(state)

            return

        logger.info(
            "Scan slot: %s",
            slot,
        )

        # ثبت نوبت قبل از اسکن
        # تا اجرای همزمان دوباره اسکن نکند.
        state[
            "last_scan_slot"
        ] = slot

        save_state(state)

        # اسکن
        alerts = (
            await run_market_scan(
                session,
                state,
            )
        )

        # ذخیره سیگنال‌ها
        save_state(state)

        # =====================================
        # ALWAYS SEND ONE MESSAGE
        # =====================================

        if alerts:

            alerts.sort(
                key=lambda x: (
                    TIMEFRAME_ORDER.index(
                        x["timeframe"]
                    )
                    if x["timeframe"]
                    in TIMEFRAME_ORDER
                    else 99
                )
            )

            message = (
                build_full_message(
                    alerts
                )
            )

        else:

            message = (
                "✅ <b>اسکن انجام شد.</b>"
            )

        await send_message_to_all(
            session,
            state,
            message,
        )

        save_state(state)

        logger.info(
            "Message sent."
        )


# =========================================================
# ENTRY
# =========================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except Exception as e:

        logger.exception(
            "FATAL ERROR"
        )

        print(
            f"FATAL ERROR: "
            f"{type(e).__name__}: {e}"
        )

        raise
