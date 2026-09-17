import os
import asyncio
import aiohttp
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from statistics import mean


# ============================================================
# CONFIG
# ============================================================

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
LEGACY_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TOP_N = int(os.getenv("TOP_N", "100"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))

# تقریباً 5 دقیقه مانیتور می‌کند
MONITOR_SECONDS = 285

# هر 60 ثانیه یک اسکن
SCAN_INTERVAL_SECONDS = 60

USERS_FILE = Path(
    os.getenv("USERS_FILE", "users.json")
)

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
)

# Binance public market-data endpoint
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

# حداکثر درخواست همزمان Binance
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

async def http_get(
    session,
    url,
    params=None,
    retries=3
):
    last_error = None

    for attempt in range(retries):
        try:
            timeout = aiohttp.ClientTimeout(
                total=25
            )

            async with session.get(
                url,
                params=params,
                timeout=timeout,
                headers={
                    "User-Agent":
                    "crypto-rsi-telegram-bot/6.0"
                }
            ) as response:

                text = await response.text()

                if response.status == 429:
                    retry_after = (
                        response.headers.get(
                            "Retry-After",
                            "3"
                        )
                    )

                    try:
                        delay = min(
                            float(retry_after),
                            15
                        )
                    except Exception:
                        delay = 3

                    logger.warning(
                        "HTTP 429 from %s - "
                        "waiting %.1fs",
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
                        text[:500]
                    )

                    raise RuntimeError(
                        f"HTTP {response.status}: "
                        f"{text[:500]}"
                    )

                try:
                    return json.loads(text)

                except json.JSONDecodeError:
                    raise RuntimeError(
                        f"Invalid JSON from {url}: "
                        f"{text[:300]}"
                    )

        except Exception as exc:
            last_error = exc

            if attempt < retries - 1:
                await asyncio.sleep(
                    1.5 * (attempt + 1)
                )
            else:
                raise last_error

    raise last_error


async def telegram_request(
    session,
    method,
    payload=None
):
    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing."
        )

    url = (
        f"{TELEGRAM_API}"
        f"{TOKEN}/{method}"
    )

    timeout = aiohttp.ClientTimeout(
        total=25
    )

    async with session.post(
        url,
        json=payload or {},
        timeout=timeout
    ) as response:

        text = await response.text()

        if response.status >= 400:
            raise RuntimeError(
                f"Telegram HTTP "
                f"{response.status}: "
                f"{text[:500]}"
            )

        try:
            data = json.loads(text)

        except Exception:
            raise RuntimeError(
                f"Telegram invalid JSON: "
                f"{text[:500]}"
            )

        if not data.get("ok"):
            raise RuntimeError(
                f"Telegram API error: "
                f"{text[:500]}"
            )

        return data.get("result")


async def send_telegram(
    session,
    chat_id,
    text
):
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
            "%s does not exist.",
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

async def process_commands(
    session,
    state
):
    offset = int(
        state.get("offset", 0)
    )

    try:
        updates = await telegram_request(
            session,
            "getUpdates",
            {
                "offset": offset,
                "timeout": 0,
                "allowed_updates": [
                    "message"
                ]
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
            update.get(
                "update_id",
                0
            )
        )

        max_update_id = max(
            max_update_id,
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
            chat.get("id", "")
        )

        if not chat_id:
            continue

        text = (
            message.get("text")
            or ""
        ).strip().lower()

        if not text:
            continue

        command = text.split()[0]

        # /start
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
                    "از این به بعد هشدارهای "
                    "RSI را دریافت می‌کنید."
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

        # /stop
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
                    "برای فعال‌سازی دوباره "
                    "/start را بزنید."
                )

                logger.info(
                    "User deactivated: %s",
                    chat_id
                )

        # /status
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
                f"📡 وضعیت اشتراک هشدار: "
                f"{status}"
            )

    if updates:
        state["offset"] = max_update_id
        changed = True

    # پشتیبانی از Chat ID قدیمی
    if (
        LEGACY_CHAT_ID
        and LEGACY_CHAT_ID
        not in state["users"]
    ):
        state["users"].append(
            LEGACY_CHAT_ID
        )

        changed = True

        logger.info(
            "Added TELEGRAM_CHAT_ID "
            "to subscribers."
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
    seen = set()

    for item in data:

        symbol = (
            item.get("symbol")
            or ""
        ).upper()

        name = (
            item.get("name")
            or symbol
        )

        if not symbol:
            continue

        if symbol in seen:
            continue

        seen.add(symbol)

        coins.append({
            "name": name,
            "symbol": symbol
        })

    return coins[:TOP_N]


# ============================================================
# BINANCE
# ============================================================

async def get_binance_exchange_info(
    session
):
    url = (
        f"{BINANCE_BASE_URL}"
        "/api/v3/exchangeInfo"
    )

    return await http_get(
        session,
        url
    )


def build_valid_symbols(
    exchange_info
):
    valid = set()

    if not isinstance(
        exchange_info,
        dict
    ):
        return valid

    for item in (
        exchange_info.get(
            "symbols",
            []
        )
    ):

        symbol = item.get(
            "symbol"
        )

        status = item.get(
            "status"
        )

        quote = item.get(
            "quoteAsset"
        )

        if (
            symbol
            and status == "TRADING"
            and quote == "USDT"
        ):
            valid.add(symbol)

    return valid


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

def calculate_rsi(
    values,
    period=14
):
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(
        1,
        period + 1
    ):

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

    average_gain = (
        sum(gains) / period
    )

    average_loss = (
        sum(losses) / period
    )

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
# EMA / TECHNICAL MODEL
# ============================================================

def ema(
    values,
    period
):
    if len(values) < period:
        return None

    multiplier = (
        2 / (period + 1)
    )

    result = (
        sum(values[:period])
        / period
    )

    for value in values[period:]:

        result = (
            (value - result)
            * multiplier
            + result
        )

    return result


def technical_probability(
    closes,
    rsi_value
):
    """
    Local technical heuristic.
    این مدل هوش مصنوعی خارجی نیست.
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

    if (
        fast is None
        or slow is None
    ):
        return 50.0, "خنثی"

    score = 50.0

    # روند
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

    # مومنتوم کوتاه
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

    return (
        100 - score,
        "نزولی"
    )


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
        ).timestamp()
        * 1000
    )

    remaining = max(
        0,
        close_ms - now_ms
    )

    total_seconds = (
        remaining // 1000
    )

    hours = (
        total_seconds // 3600
    )

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

def extract_probability(
    market
):
    """
    تلاش برای استخراج احتمال از
    داده عمومی Polymarket.
    """

    if not isinstance(
        market,
        dict
    ):
        return None

    value = market.get(
        "probability"
    )

    if isinstance(
        value,
        (int, float)
    ):
        value = float(value)

        if 0 <= value <= 1:
            return value * 100

        if 0 <= value <= 100:
            return value

    value = market.get(
        "outcomePrices"
    )

    if isinstance(
        value,
        str
    ):

        try:
            parsed = json.loads(
                value
            )
        except Exception:
            parsed = None

        if isinstance(
            parsed,
            list
        ) and parsed:

            try:
                p = float(
                    parsed[0]
                )

                if 0 <= p <= 1:
                    return p * 100

                if 0 <= p <= 100:
                    return p

            except Exception:
                pass

    if isinstance(
        value,
        list
    ) and value:

        try:
            p = float(
                value[0]
            )

            if 0 <= p <= 1:
                return p * 100

            if 0 <= p <= 100:
                return p

        except Exception:
            pass

    value = market.get(
        "price"
    )

    if isinstance(
        value,
        (int, float)
    ):

        value = float(value)

        if 0 <= value <= 1:
            return value * 100

        if 0 <= value <= 100:
            return value

    return None


async def polymarket_sentiment(
    session,
    coin_name,
    symbol
):
    """
    داده عمومی Polymarket.
    در صورت نبود بازار مرتبط، None.
    """

    clean_symbol = (
        symbol
        .replace("USDT", "")
        .upper()
    )

    queries = [
        f"{coin_name} crypto",
        clean_symbol
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

            if not isinstance(
                data,
                dict
            ):
                continue

            results = []

            if isinstance(
                data.get("events"),
                list
            ):
                results.extend(
                    data["events"]
                )

            if isinstance(
                data.get("markets"),
                list
            ):
                results.extend(
                    data["markets"]
                )

            for market in results:

                raw_text = json.dumps(
                    market,
                    ensure_ascii=False
                ).lower()

                relevant = (
                    clean_symbol.lower()
                    in raw_text
                    or coin_name.lower()
                    in raw_text
                )

                if not relevant:
                    continue

                probability = (
                    extract_probability(
                        market
                    )
                )

                if probability is None:
                    continue

                probability = max(
                    1,
                    min(
                        99,
                        probability
                    )
                )

                if probability >= 50:
                    direction = "صعودی"
                else:
                    direction = "نزولی"

                return (
                    probability,
                    direction
                )

        except Exception as exc:

            logger.warning(
                "Polymarket error for %s: %s",
                symbol,
                exc
            )

    return None


# ============================================================
# SIGNAL STATE
# ============================================================

def signal_zone(rsi_value):

    if rsi_value > 70:
        return "high"

    if rsi_value < 30:
        return "low"

    return "neutral"


def signal_key(
    symbol,
    timeframe
):
    return (
        f"{symbol}:"
        f"{timeframe}"
    )


def is_new_signal(
    state,
    symbol,
    timeframe,
    zone
):
    key = signal_key(
        symbol,
        timeframe
    )

    previous = (
        state["signals"]
        .get(key)
    )

    # وقتی RSI به حالت خنثی برگشت،
    # وضعیت قبلی پاک می‌شود.
    if zone == "neutral":

        if previous is not None:
            state["signals"].pop(
                key,
                None
            )

        return False

    # ورود دوباره به همان منطقه
    # هشدار تکراری ایجاد نمی‌کند.
    if previous == zone:
        return False

    return True


def mark_signal(
    state,
    symbol,
    timeframe,
    zone
):
    key = signal_key(
        symbol,
        timeframe
    )

    state["signals"][key] = zone


# ============================================================
# BUILD SIGNAL
# ============================================================

async def build_signal(
    session,
    coin_name,
    symbol,
    timeframe,
    interval,
    rows
):
    if len(rows) < RSI_PERIOD + 5:
        return None

    # close کندل باز را هم شامل می‌کنیم.
    closes = [
        float(row[4])
        for row in rows
    ]

    rsi_value = calculate_rsi(
        closes,
        RSI_PERIOD
    )

    if rsi_value is None:
        return None

    zone = signal_zone(
        rsi_value
    )

    if zone == "neutral":
        return None

    if rsi_value > 70:

        rsi_line = (
            f"🟢 RSI "
            f"{rsi_value:.2f} | "
            f"اشباع خرید"
        )

    else:

        rsi_line = (
            f"🔴 RSI "
            f"{rsi_value:.2f} | "
            f"اشباع فروش"
        )

    # مدل تکنیکال
    technical_score, technical_direction = (
        technical_probability(
            closes,
            rsi_value
        )
    )

    # Polymarket
    poly = await polymarket_sentiment(
        session,
        coin_name,
        symbol
    )

    if poly is None:

        combined_score = (
            technical_score
        )

        combined_direction = (
            technical_direction
        )

        poly_line = (
            "🎯 Polymarket: —"
        )

    else:

        poly_score, poly_direction = poly

        if (
            technical_direction
            == poly_direction
        ):

            combined_score = (
                technical_score * 0.70
                + poly_score * 0.30
            )

            combined_direction = (
                technical_direction
            )

        else:

            # اگر دو منبع مخالف باشند،
            # امتیاز جهت تکنیکال با وزن 70%
            # و Polymarket با وزن 30% ترکیب می‌شود.
            if technical_direction == "صعودی":
                technical_signed = (
                    technical_score
                )
            else:
                technical_signed = (
                    100 - technical_score
                )

            if poly_direction == "صعودی":
                poly_signed = poly_score
            else:
                poly_signed = (
                    100 - poly_score
                )

            signed = (
                technical_signed * 0.70
                + poly_signed * 0.30
            )

            if signed >= 50:
                combined_direction = "صعودی"
                combined_score = signed
            else:
                combined_direction = "نزولی"
                combined_score = 100 - signed

        poly_line = (
            f"🎯 Polymarket: "
            f"{poly_score:.0f}% "
            f"{poly_direction}"
        )

    if combined_direction == "صعودی":

        next_line = (
            f"🔮 بعدی: 🟢 صعودی "
            f"{combined_score:.0f}%"
        )

    elif combined_direction == "نزولی":

        next_line = (
            f"🔮 بعدی: 🔴 نزولی "
            f"{combined_score:.0f}%"
        )

    else:

        next_line = (
            "🔮 بعدی: ⚪ خنثی"
        )

    ai_line = (
        f"🤖 AI: "
        f"{technical_score:.0f}% "
        f"{technical_direction}"
    )

    volume_label, ratio = (
        volume_info(rows)
    )

    volume_line = (
        f"📊 حجم: "
        f"{volume_label} "
        f"{ratio:.1f}× "
        f"میانگین ۳ کندل قبل"
    )

    open_ms = int(
        rows[-1][0]
    )

    close_ms = candle_close_time_ms(
        open_ms,
        interval
    )

    remaining = remaining_text(
        close_ms
    )

    close_line = (
        f"⏳ بسته‌شدن: "
        f"{remaining}"
    )

    tv_symbol = symbol

    tv_url = (
        "https://www.tradingview.com/"
        f"symbols/{tv_symbol}/"
        "?exchange=BINANCE"
    )

    tv_line = (
        f"📈 TV: {tv_url}"
    )

    return {
        "symbol": symbol,
        "coin_name": coin_name,
        "timeframe": timeframe,
        "zone": zone,
        "rsi": rsi_value,
        "text": "\n".join([
            rsi_line,
            next_line,
            ai_line,
            poly_line,
            volume_line,
            close_line,
            tv_line
        ])
    }


# ============================================================
# SCAN ONE SYMBOL
# ============================================================

async def scan_symbol(
    session,
    coin,
    binance_symbol,
    timeframe,
    interval,
    semaphore
):
    async with semaphore:

        try:

            rows = await get_klines(
                session,
                binance_symbol,
                interval,
                limit=100
            )

            if not isinstance(
                rows,
                list
            ):
                return None

            result = await build_signal(
                session,
                coin["name"],
                binance_symbol,
                timeframe,
                interval,
                rows
            )

            return result

        except Exception as exc:

            logger.debug(
                "Scan failed %s %s: %s",
                binance_symbol,
                timeframe,
                exc
            )

            return None


# ============================================================
# SCAN ONCE
# ============================================================

async def scan_once(
    session,
    coins,
    valid_symbols,
    state,
    window_seen
):
    semaphore = asyncio.Semaphore(
        BINANCE_CONCURRENCY
    )

    tasks = []

    for coin in coins:

        base_symbol = (
            coin["symbol"]
            .upper()
        )

        binance_symbol = (
            f"{base_symbol}USDT"
        )

        if (
            binance_symbol
            not in valid_symbols
        ):
            continue

        for timeframe, interval in (
            TIMEFRAMES.items()
        ):

            tasks.append(
                scan_symbol(
                    session,
                    coin,
                    binance_symbol,
                    timeframe,
                    interval,
                    semaphore
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
            continue

        if not result:
            continue

        symbol = result["symbol"]
        timeframe = result["timeframe"]
        zone = result["zone"]

        key = signal_key(
            symbol,
            timeframe
        )

        # خنثی شدن RSI وضعیت قبلی را آزاد می‌کند
        if zone == "neutral":

            state["signals"].pop(
                key,
                None
            )

            continue

        # جلوگیری از تکرار در همان پنجره
        if key in window_seen:
            continue

        # بررسی وضعیت ذخیره‌شده قبلی
        if not is_new_signal(
            state,
            symbol,
            timeframe,
            zone
        ):
            continue

        alerts.append(result)

        # فقط داخل همین پنجره علامت می‌زنیم.
        window_seen.add(key)

    return alerts


# ============================================================
# FORMAT BATCH MESSAGE
# ============================================================

def build_batch_message(
    alerts
):
    if not alerts:
        return None

    grouped = {}

    for alert in alerts:

        symbol = alert["symbol"]

        grouped.setdefault(
            symbol,
            []
        ).append(alert)

    blocks = []

    for symbol, items in grouped.items():

        items.sort(
            key=lambda x:
            TF_ORDER.get(
                x["timeframe"],
                99
            )
        )

        lines = [
            f"💠 {symbol}"
        ]

        for item in items:

            lines.append(
                f"\n━━━ "
                f"{item['timeframe']} "
                f"━━━"
            )

            lines.append(
                item["text"]
            )

        blocks.append(
            "\n".join(lines)
        )

    return "\n\n".join(blocks)


# ============================================================
# MAIN
# ============================================================

async def main():

    logger.info(
        "Starting RSI scanner..."
    )

    state = load_state()

    logger.info(
        "Current users: %d",
        len(state["users"])
    )

    connector = aiohttp.TCPConnector(
        limit=30,
        ttl_dns_cache=300
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        # -----------------------------
        # Telegram commands
        # -----------------------------

        state = await process_commands(
            session,
            state
        )

        logger.info(
            "Users after commands: %d",
            len(state["users"])
        )

        if not state["users"]:
            logger.warning(
                "No Telegram users are active."
            )

        # -----------------------------
        # Top 100
        # -----------------------------

        logger.info(
            "Getting Top 100..."
        )

        try:

            coins = await get_top_coins(
                session
            )

        except Exception as exc:

            logger.error(
                "Could not get Top 100: %s",
                exc
            )

            return

        logger.info(
            "Top coins: %d",
            len(coins)
        )

        # -----------------------------
        # Binance exchange info
        # -----------------------------

        try:

            exchange_info = (
                await get_binance_exchange_info(
                    session
                )
            )

        except Exception as exc:

            logger.error(
                "Could not get Binance "
                "exchange info: %s",
                exc
            )

            return

        valid_symbols = (
            build_valid_symbols(
                exchange_info
            )
        )

        logger.info(
            "Valid symbols: %d",
            len(valid_symbols)
        )

        if not valid_symbols:

            logger.error(
                "No valid Binance symbols. "
                "Check Binance endpoint."
            )

            return

        # -----------------------------
        # Monitoring window
        # -----------------------------

        start_time = asyncio.get_running_loop().time()

        scan_number = 0

        collected = []

        window_seen = set()

        while (
            asyncio.get_running_loop().time()
            - start_time
            < MONITOR_SECONDS
        ):

            scan_number += 1

            logger.info(
                "Monitoring scan #%d",
                scan_number
            )

            # دریافت /start و /stop
            state = await process_commands(
                session,
                state
            )

            alerts = await scan_once(
                session,
                coins,
                valid_symbols,
                state,
                window_seen
            )

            if alerts:

                logger.info(
                    "New alerts in scan #%d: %d",
                    scan_number,
                    len(alerts)
                )

                collected.extend(
                    alerts
                )

            elapsed = (
                asyncio.get_running_loop().time()
                - start_time
            )

            remaining = (
                MONITOR_SECONDS
                - elapsed
            )

            if remaining <= 0:
                break

            sleep_for = min(
                SCAN_INTERVAL_SECONDS,
                remaining
            )

            await asyncio.sleep(
                sleep_for
            )

        # -----------------------------
        # End of window
        # -----------------------------

        logger.info(
            "5-minute monitoring window "
            "finished."
        )

        if not collected:

            logger.info(
                "No new signals in this window."
            )

            save_state(state)
            return

        # مرتب‌سازی
        collected.sort(
            key=lambda x: (
                x["symbol"],
                TF_ORDER.get(
                    x["timeframe"],
                    99
                )
            )
        )

        message = build_batch_message(
            collected
        )

        if not message:
            save_state(state)
            return

        logger.info(
            "Sending %d collected alerts.",
            len(collected)
        )

        # -----------------------------
        # Send to all users
        # -----------------------------

        successful = False

        for chat_id in list(
            state["users"]
        ):

            ok = await send_telegram(
                session,
                chat_id,
                message
            )

            if ok:
                successful = True

        # فقط اگر پیام واقعاً ارسال شد،
        # وضعیت سیگنال‌ها ذخیره شود.
        if successful:

            for alert in collected:

                mark_signal(
                    state,
                    alert["symbol"],
                    alert["timeframe"],
                    alert["zone"]
                )

            save_state(state)

        else:

            logger.warning(
                "No Telegram message was "
                "successfully delivered. "
                "Signal state was not marked."
            )

            save_state(state)


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:

        logger.info(
            "Scanner stopped."
        )

    except Exception as exc:

        logger.exception(
            "Fatal error: %s",
            exc
        )

        raise
