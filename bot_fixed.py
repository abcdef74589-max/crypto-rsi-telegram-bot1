import asyncio
import aiohttp
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from html import escape
from pathlib import Path
from statistics import mean

# =========================================================
# CONFIG
# =========================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

STATE_FILE = Path("users.json")

TOP_N = 100
RSI_PERIOD = 14

# مدت پایش هر اجرای Workflow
MONITOR_SECONDS = 285

# فاصله بین اسکن‌ها
SCAN_INTERVAL_SECONDS = 30

BINANCE_API = "https://api.binance.com"
COINGECKO_API = "https://api.coingecko.com/api/v3"
POLYMARKET_API = "https://gamma-api.polymarket.com"

MAX_BINANCE_CONCURRENT = 12

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
        with open(
            STATE_FILE,
            "r",
            encoding="utf-8",
        ) as f:
            state = json.load(f)

        if not isinstance(state, dict):
            raise ValueError("Invalid state")

        state.setdefault("users", [])
        state.setdefault("offset", 0)
        state.setdefault("signals", {})

        return state

    except Exception as e:

        log.error(
            "State load error: %s",
            e,
        )

        return {
            "users": [],
            "offset": 0,
            "signals": {},
        }


def save_state(state):

    temp = STATE_FILE.with_suffix(".tmp")

    with open(
        temp,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            state,
            f,
            ensure_ascii=False,
            indent=2,
        )

    temp.replace(STATE_FILE)


# =========================================================
# HTTP
# =========================================================

async def get_json(
    session,
    url,
    params=None,
    timeout=20,
):

    try:

        async with session.get(
            url,
            params=params,
            timeout=timeout,
        ) as response:

            if response.status != 200:

                text = await response.text()

                log.warning(
                    "HTTP %s: %s",
                    response.status,
                    text[:200],
                )

                return None

            return await response.json(
                content_type=None
            )

    except Exception as e:

        log.warning(
            "GET error: %s",
            e,
        )

        return None


async def post_json(
    session,
    url,
    payload,
    timeout=25,
):

    try:

        async with session.post(
            url,
            json=payload,
            timeout=timeout,
        ) as response:

            if response.status != 200:

                text = await response.text()

                log.warning(
                    "POST HTTP %s: %s",
                    response.status,
                    text[:200],
                )

                return None

            return await response.json(
                content_type=None
            )

    except Exception as e:

        log.warning(
            "POST error: %s",
            e,
        )

        return None


# =========================================================
# TELEGRAM
# =========================================================

async def telegram(
    session,
    method,
    payload=None,
):

    if not TELEGRAM_BOT_TOKEN:

        log.error(
            "TELEGRAM_BOT_TOKEN missing"
        )

        return None

    url = (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/{method}"
    )

    return await post_json(
        session,
        url,
        payload or {},
    )


async def send_message(
    session,
    chat_id,
    text,
):

    result = await telegram(
        session,
        "sendMessage",
        {
            "chat_id": str(chat_id),
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
    )

    return bool(
        result
        and result.get("ok")
    )


# =========================================================
# TELEGRAM COMMANDS
# =========================================================

async def process_commands(
    session,
    state,
):

    offset = int(
        state.get("offset", 0)
    )

    result = await telegram(
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

    if not result or not result.get("ok"):
        return

    updates = result.get(
        "result",
        [],
    )

    for update in updates:

        update_id = update.get(
            "update_id"
        )

        if update_id is not None:
            state["offset"] = (
                update_id + 1
            )

        message = update.get(
            "message"
        ) or {}

        chat = message.get(
            "chat"
        ) or {}

        chat_id = chat.get("id")

        if chat_id is None:
            continue

        text = (
            message.get("text")
            or ""
        ).strip()

        command = (
            text.split()[0].lower()
            if text
            else ""
        )

        if command.startswith("/start"):

            users = [
                str(x)
                for x in state["users"]
            ]

            if str(chat_id) not in users:

                state["users"].append(
                    str(chat_id)
                )

            await send_message(
                session,
                chat_id,
                "✅ <b>ربات فعال شد</b>\n\n"
                "پایش RSI آغاز شد.\n"
                "⏱ پایش مداوم\n"
                "📊 15M / 1H / 4H / 1D\n"
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
                "User stopped: %s",
                chat_id,
            )


# =========================================================
# TOP 100
# =========================================================

async def get_top_coins(session):

    data = await get_json(
        session,
        f"{COINGECKO_API}/coins/markets",
        {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": TOP_N,
            "page": 1,
            "sparkline": "false",
        },
        timeout=30,
    )

    if not data:
        return []

    result = []

    for coin in data:

        symbol = str(
            coin.get(
                "symbol",
                "",
            )
        ).upper()

        if not symbol:
            continue

        result.append(
            symbol + "USDT"
        )

    result = list(
        dict.fromkeys(result)
    )

    log.info(
        "Top coins: %d",
        len(result),
    )

    return result


# =========================================================
# BINANCE SYMBOLS
# =========================================================

async def get_binance_symbols(
    session
):

    data = await get_json(
        session,
        f"{BINANCE_API}/api/v3/exchangeInfo",
        timeout=30,
    )

    if not data:
        return set()

    result = set()

    for item in data.get(
        "symbols",
        [],
    ):

        if (
            item.get("status")
            == "TRADING"
            and item.get("quoteAsset")
            == "USDT"
        ):

            result.add(
                item.get(
                    "symbol"
                )
            )

    return result


# =========================================================
# KLINES
# =========================================================

async def get_klines(
    session,
    symbol,
    interval,
    semaphore,
):

    async with semaphore:

        return await get_json(
            session,
            f"{BINANCE_API}/api/v3/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "limit": 100,
            },
            timeout=20,
        )


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

    for i in range(
        1,
        len(closes),
    ):

        change = (
            closes[i]
            - closes[i - 1]
        )

        if change >= 0:

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

    return round(
        100 - (
            100 / (1 + rs)
        ),
        2,
    )


# =========================================================
# EMA
# =========================================================

def ema(
    values,
    period,
):

    if len(values) < period:
        return None

    value = sum(
        values[:period]
    ) / period

    multiplier = (
        2 / (period + 1)
    )

    for price in values[period:]:

        value = (
            (
                price - value
            ) * multiplier
        ) + value

    return value


# =========================================================
# TECHNICAL MODEL
# =========================================================

def technical_model(
    klines
):

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

    e9 = ema(
        closes,
        9,
    )

    e21 = ema(
        closes,
        21,
    )

    if (
        rsi is None
        or e9 is None
        or e21 is None
    ):
        return None, None

    score = 50

    if e9 > e21:
        score += 15
    else:
        score -= 15

    if closes[-1] > closes[-2]:
        score += 10
    else:
        score -= 10

    if rsi > 70:
        score += 8
    elif rsi < 30:
        score -= 8
    elif rsi > 55:
        score += 5
    elif rsi < 45:
        score -= 5

    if len(closes) >= 6:

        change = (
            (
                closes[-1]
                - closes[-6]
            )
            / closes[-6]
        ) * 100

        if change > 1:
            score += 7

        elif change < -1:
            score -= 7

    score = max(
        1,
        min(
            99,
            score,
        ),
    )

    direction = (
        "صعودی"
        if score >= 50
        else "نزولی"
    )

    return round(score), direction


# =========================================================
# VOLUME
# =========================================================

def get_volume_ratio(
    klines
):

    if len(klines) < 5:
        return None

    current = float(
        klines[-1][5]
    )

    previous = [
        float(k[5])
        for k in klines[-4:-1]
    ]

    average = mean(
        previous
    )

    if average <= 0:
        return None

    return current / average


def volume_text(
    ratio
):

    if ratio is None:
        return "📊 حجم: —"

    if ratio > 1.2:
        return (
            f"📊 حجم: 🔥 {ratio:.2f}× "
            "میانگین ۳ کندل قبل (زیاد)"
        )

    if ratio < 0.8:
        return (
            f"📊 حجم: 📉 {ratio:.2f}× "
            "میانگین ۳ کندل قبل (کم)"
        )

    return (
        f"📊 حجم: ➖ {ratio:.2f}× "
        "میانگین ۳ کندل قبل (معمولی)"
    )


# =========================================================
# CANDLE REMAINING
# =========================================================

def candle_remaining(
    klines,
    minutes,
):

    if not klines:
        return "—"

    open_ms = int(
        klines[-1][0]
    )

    opened = datetime.fromtimestamp(
        open_ms / 1000,
        timezone.utc,
    )

    closes_at = (
        opened
        + timedelta(
            minutes=minutes
        )
    )

    remaining = int(
        (
            closes_at
            - datetime.now(timezone.utc)
        ).total_seconds()
    )

    remaining = max(
        0,
        remaining,
    )

    hours = remaining // 3600
    mins = (
        remaining % 3600
    ) // 60
    secs = remaining % 60

    if hours:

        return (
            f"{hours:02d}:"
            f"{mins:02d}:"
            f"{secs:02d}"
        )

    return (
        f"{mins:02d}:"
        f"{secs:02d}"
    )


# =========================================================
# POLYMARKET
# =========================================================

async def get_polymarket(
    session,
    symbol,
    timeframe,
):

    base = symbol.replace(
        "USDT",
        "",
    )

    names = {
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

    query = names.get(
        base,
        base,
    )

    data = await get_json(
        session,
        f"{POLYMARKET_API}/public-search",
        {
            "q": query,
            "limit": 20,
        },
        timeout=15,
    )

    if not data:
        return None

    markets = []

    if isinstance(
        data,
        list,
    ):
        markets = data

    elif isinstance(
        data,
        dict,
    ):

        markets.extend(
            data.get(
                "markets",
                [],
            )
        )

    for market in markets:

        question = str(
            market.get(
                "question",
                "",
            )
        ).lower()

        slug = str(
            market.get(
                "slug",
                "",
            )
        ).lower()

        text = (
            question
            + " "
            + slug
        )

        # Coin
        if (
            base.lower()
            not in text
            and query.lower()
            not in text
        ):
            continue

        # Timeframe
        if timeframe == "15M":

            tf_ok = (
                "15m" in text
                or "15 min" in text
                or "15 minute" in text
                or "5m" in text
            )

        elif timeframe == "1H":

            tf_ok = (
                "1h" in text
                or "1 hour" in text
                or "hour" in text
            )

        elif timeframe == "4H":

            tf_ok = (
                "4h" in text
                or "4 hour" in text
            )

        else:

            tf_ok = (
                "1d" in text
                or "1 day" in text
                or "daily" in text
                or "24 hour" in text
            )

        if not tf_ok:
            continue

        outcomes = market.get(
            "outcomes"
        )

        prices = market.get(
            "outcomePrices"
        )

        try:

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

        except Exception:
            continue

        if not outcomes or not prices:
            continue

        for outcome, price in zip(
            outcomes,
            prices,
        ):

            try:
                price = float(price)
            except Exception:
                continue

            outcome = str(
                outcome
            ).lower()

            if (
                outcome == "yes"
                or "up" in outcome
                or "higher" in outcome
                or "bullish" in outcome
            ):

                return round(
                    price * 100
                )

    return None


# =========================================================
# SIGNAL STATE
# =========================================================

def get_zone(rsi):

    if rsi > 70:
        return "high"

    if rsi < 30:
        return "low"

    return "neutral"


def new_signal(
    state,
    symbol,
    timeframe,
    rsi,
):

    key = (
        f"{symbol}:"
        f"{timeframe}"
    )

    zone = get_zone(rsi)

    old = state[
        "signals"
    ].get(
        key,
        "neutral",
    )

    # Neutral resets signal
    if zone == "neutral":

        if old != "neutral":

            state[
                "signals"
            ][key] = "neutral"

        return False

    # Already in same zone
    if zone == old:
        return False

    # New entry
    state[
        "signals"
    ][key] = zone

    return True


# =========================================================
# BUILD SIGNAL
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

    ai_score, ai_direction = (
        technical_model(
            klines
        )
    )

    pm = await get_polymarket(
        session,
        symbol,
        timeframe,
    )

    if ai_score is None:
        combined = None
    elif pm is None:
        combined = ai_score
    else:
        combined = (
            ai_score * 0.70
            + pm * 0.30
        )

    if combined is None:

        next_text = (
            "🔮 بعدی: —"
        )

    elif combined >= 50:

        next_text = (
            f"🔮 بعدی: 🟢 صعودی "
            f"{combined:.0f}%"
        )

    else:

        next_text = (
            f"🔮 بعدی: 🔴 نزولی "
            f"{100 - combined:.0f}%"
        )

    if ai_score is None:

        ai_text = "🤖 AI: —"

    elif ai_direction == "صعودی":

        ai_text = (
            f"🤖 AI: "
            f"{ai_score:.0f}% صعودی"
        )

    else:

        ai_text = (
            f"🤖 AI: "
            f"{100 - ai_score:.0f}% نزولی"
        )

    if pm is None:

        pm_text = (
            "🎯 Polymarket: —"
        )

    elif pm >= 50:

        pm_text = (
            f"🎯 Polymarket: "
            f"{pm:.0f}% صعودی"
        )

    else:

        pm_text = (
            f"🎯 Polymarket: "
            f"{100 - pm:.0f}% نزولی"
        )

    ratio = get_volume_ratio(
        klines
    )

    remaining = candle_remaining(
        klines,
        TIMEFRAMES[
            timeframe
        ]["minutes"],
    )

    if rsi > 70:

        rsi_text = (
            f"🟢 RSI {rsi:.2f} "
            "| اشباع خرید"
        )

    else:

        rsi_text = (
            f"🔴 RSI {rsi:.2f} "
            "| اشباع فروش"
        )

    tv_url = (
        "https://www.tradingview.com/"
        f"symbols/{symbol}/"
        "?exchange=BINANCE"
    )

    return (
        f"━━━ {timeframe} ━━━\n"
        f"{rsi_text}\n"
        f"{next_text}\n"
        f"{ai_text}\n"
        f"{pm_text}\n"
        f"{volume_text(ratio)}\n"
        f"⏳ بسته‌شدن: {remaining}\n"
        f'<a href="{tv_url}">📈 TV</a>'
    )


# =========================================================
# SCAN
# =========================================================

async def scan_once(
    session,
    state,
    symbols,
    semaphore,
    collected,
):

    tasks = []

    for symbol in symbols:

        tasks.append(
            scan_symbol(
                session,
                state,
                symbol,
                semaphore,
            )
        )

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    for result in results:

        if isinstance(
            result,
            Exception,
        ):
            continue

        if not result:
            continue

        symbol, blocks = result

        if not blocks:
            continue

        if symbol not in collected:
            collected[symbol] = {}

        for timeframe, block in blocks.items():

            collected[
                symbol
            ][timeframe] = block


async def scan_symbol(
    session,
    state,
    symbol,
    semaphore,
):

    blocks = {}

    for timeframe, config in TIMEFRAMES.items():

        klines = await get_klines(
            session,
            symbol,
            config["interval"],
            semaphore,
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

        # Always process neutral
        # so old signal can reset.
        if 30 <= rsi <= 70:

            new_signal(
                state,
                symbol,
                timeframe,
                rsi,
            )

            continue

        if not new_signal(
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
            blocks[
                timeframe
            ] = block

    return symbol, blocks


# =========================================================
# SEND COLLECTED
# =========================================================

async def send_collected(
    session,
    state,
    collected,
):

    users = state.get(
        "users",
        [],
    )

    if not users:
        return

    if not collected:
        log.info(
            "No new signals in this window."
        )
        return

    messages = []

    for symbol in sorted(
        collected.keys()
    ):

        blocks = []

        for timeframe in TIMEFRAMES:

            if (
                timeframe
                in collected[symbol]
            ):

                blocks.append(
                    collected[
                        symbol
                    ][timeframe]
                )

        if not blocks:
            continue

        message = (
            f"💠 <b>{escape(symbol)}</b>\n\n"
            + "\n\n".join(blocks)
        )

        messages.append(
            message
        )

    if not messages:
        return

    log.info(
        "Sending %d collected alerts.",
        len(messages),
    )

    for message in messages:

        for chat_id in users:

            await send_message(
                session,
                chat_id,
                message,
            )

            await asyncio.sleep(
                0.2
            )


# =========================================================
# MAIN MONITOR
# =========================================================

async def main():

    if not TELEGRAM_BOT_TOKEN:

        log.error(
            "TELEGRAM_BOT_TOKEN missing."
        )

        return

    state = load_state()

    timeout = aiohttp.ClientTimeout(
        total=60
    )

    connector = aiohttp.TCPConnector(
        limit=40
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={
            "User-Agent":
                "RSI-Telegram-Scanner/2.0"
        },
    ) as session:

        # Check commands immediately.
        await process_commands(
            session,
            state,
        )

        save_state(state)

        if not state.get("users"):

            log.info(
                "No active users."
            )

            return

        log.info(
            "Getting Top 100..."
        )

        top = await get_top_coins(
            session
        )

        if not top:
            return

        binance = await get_binance_symbols(
            session
        )

        symbols = [
            x
            for x in top
            if x in binance
        ]

        log.info(
            "Valid symbols: %d",
            len(symbols),
        )

        semaphore = asyncio.Semaphore(
            MAX_BINANCE_CONCURRENT
        )

        collected = {}

        started = datetime.now(
            timezone.utc
        )

        end_time = (
            started
            + timedelta(
                seconds=MONITOR_SECONDS
            )
        )

        scan_number = 0

        while (
            datetime.now(timezone.utc)
            < end_time
        ):

            scan_number += 1

            log.info(
                "Monitoring scan #%d",
                scan_number,
            )

            try:

                await scan_once(
                    session,
                    state,
                    symbols,
                    semaphore,
                    collected,
                )

                save_state(state)

            except Exception as e:

                log.exception(
                    "Scan error: %s",
                    e,
                )

            # Check Telegram commands
            await process_commands(
                session,
                state,
            )

            save_state(state)

            remaining = (
                end_time
                - datetime.now(
                    timezone.utc
                )
            ).total_seconds()

            if remaining <= 0:
                break

            await asyncio.sleep(
                min(
                    SCAN_INTERVAL_SECONDS,
                    remaining,
                )
            )

        log.info(
            "5-minute monitoring window finished."
        )

        await send_collected(
            session,
            state,
            collected,
        )

        save_state(state)


if __name__ == "__main__":

    try:

        asyncio.run(main())

    except Exception as e:

        log.exception(
            "Fatal error: %s",
            e,
        )

        raise
