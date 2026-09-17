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

MONITOR_SECONDS = 285
SCAN_INTERVAL_SECONDS = 60

USERS_FILE = Path(
    os.getenv("USERS_FILE", "users.json")
)

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
)

BINANCE_BASE_URL = os.getenv(
    "BINANCE_BASE_URL",
    "https://data-api.binance.vision"
).rstrip("/")

TELEGRAM_API = (
    "https://api.telegram.org/bot"
)

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

BINANCE_CONCURRENCY = 10


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(
    "RSI-Scanner"
)


# ============================================================
# HTTP
# ============================================================

async def http_get(
    session,
    url,
    params=None,
    retries=3,
):
    last_error = None

    for attempt in range(retries):

        try:

            timeout = aiohttp.ClientTimeout(
                total=25
            )

            headers = {
                "User-Agent":
                    "Mozilla/5.0 RSI-Telegram-Scanner"
            }

            async with session.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
            ) as response:

                text = await response.text()

                if response.status == 429:

                    wait_time = (
                        2 + attempt * 2
                    )

                    logger.warning(
                        "HTTP 429 - retry in %ss",
                        wait_time,
                    )

                    await asyncio.sleep(
                        wait_time
                    )

                    continue

                if response.status >= 400:

                    raise RuntimeError(
                        f"HTTP {response.status}: "
                        f"{text[:500]}"
                    )

                try:
                    return json.loads(text)

                except json.JSONDecodeError:

                    return text

        except Exception as exc:

            last_error = exc

            logger.warning(
                "HTTP error %s/%s: %s",
                attempt + 1,
                retries,
                exc,
            )

            await asyncio.sleep(
                1 + attempt
            )

    raise last_error


# ============================================================
# TELEGRAM
# ============================================================

async def telegram_request(
    session,
    method,
    payload=None,
):
    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is empty"
        )

    url = (
        TELEGRAM_API
        + TOKEN
        + "/"
        + method
    )

    async with session.post(
        url,
        json=payload or {},
        timeout=aiohttp.ClientTimeout(
            total=30
        ),
    ) as response:

        text = await response.text()

        if response.status >= 400:

            raise RuntimeError(
                f"Telegram HTTP "
                f"{response.status}: {text}"
            )

        try:

            data = json.loads(text)

        except json.JSONDecodeError:

            raise RuntimeError(
                f"Invalid Telegram response: "
                f"{text}"
            )

        if not data.get("ok"):

            raise RuntimeError(
                f"Telegram API error: {data}"
            )

        return data


async def send_telegram(
    session,
    chat_id,
    message,
):
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    return await telegram_request(
        session,
        "sendMessage",
        payload,
    )


# ============================================================
# STATE
# ============================================================

def default_state():

    return {
        "users": [],
        "offset": 0,
        "signals": {},
        "running": False,
        "startup_scan_done": False,
    }


def load_state():

    if not USERS_FILE.exists():

        return default_state()

    try:

        with USERS_FILE.open(
            "r",
            encoding="utf-8",
        ) as file:

            data = json.load(file)

        if not isinstance(
            data,
            dict,
        ):

            return default_state()

        state = default_state()

        state.update(data)

        if not isinstance(
            state.get("users"),
            list,
        ):

            state["users"] = []

        if not isinstance(
            state.get("signals"),
            dict,
        ):

            state["signals"] = {}

        if not isinstance(
            state.get("offset"),
            int,
        ):

            state["offset"] = 0

        if not isinstance(
            state.get("running"),
            bool,
        ):

            state["running"] = False

        if not isinstance(
            state.get("startup_scan_done"),
            bool,
        ):

            state[
                "startup_scan_done"
            ] = False

        return state

    except Exception as exc:

        logger.error(
            "Could not load users.json: %s",
            exc,
        )

        return default_state()


def save_state(state):

    USERS_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_file = USERS_FILE.with_suffix(
        ".tmp"
    )

    with temp_file.open(
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            state,
            file,
            ensure_ascii=False,
            indent=2,
        )

    temp_file.replace(
        USERS_FILE
    )


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def process_commands(
    session,
    state,
):
    """
    خروجی:
        started = True
    اگر /start دریافت شده باشد.

    این باعث می‌شود بعد از /start
    اسکن اولیه همان اجرا شروع شود.
    """

    started = False
    changed = False

    offset = int(
        state.get(
            "offset",
            0,
        )
    )

    try:

        data = await telegram_request(
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

    except Exception as exc:

        logger.error(
            "getUpdates failed: %s",
            exc,
        )

        return False

    updates = data.get(
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

            changed = True

        message = update.get(
            "message"
        )

        if not message:
            continue

        chat = message.get(
            "chat",
            {},
        )

        chat_id = chat.get(
            "id"
        )

        text = str(
            message.get(
                "text",
                "",
            )
        ).strip()

        if chat_id is None:
            continue

        # ====================================================
        # START
        # ====================================================

        if text.startswith(
            "/start"
        ):

            if chat_id not in state["users"]:

                state["users"].append(
                    chat_id
                )

            # فعال کردن ربات
            state["running"] = True

            # اسکن اولیه دوباره فعال شود
            state[
                "startup_scan_done"
            ] = False

            # وضعیت سیگنال‌های قبلی پاک می‌شود
            # تا در شروع جدید، وضعیت فعلی
            # دوباره گزارش شود.
            state["signals"] = {}

            changed = True
            started = True

            welcome_message = (
                "👋 <b>خوش اومدی!</b>\n\n"
                "🤖 ربات RSI Scanner فعال شد.\n\n"
                "📊 ارزهای Top 100 بازار بررسی می‌شوند.\n"
                "⏱ تایم‌فریم‌ها: "
                "<b>15m | 1h | 4h | 1D</b>\n\n"
                "🟢 RSI بالای 70\n"
                "🔴 RSI پایین 30\n\n"
                "🔎 در حال انجام اسکن اولیه..."
            )

            try:

                await send_telegram(
                    session,
                    chat_id,
                    welcome_message,
                )

            except Exception as exc:

                logger.error(
                    "Welcome message failed: %s",
                    exc,
                )

        # ====================================================
        # STOP
        # ====================================================

        elif text.startswith(
            "/stop"
        ):

            if chat_id in state["users"]:

                state["users"].remove(
                    chat_id
                )

            # اگر کاربری باقی نمانده
            # ربات کاملاً متوقف شود.
            if not state["users"]:

                state["running"] = False

            changed = True

            try:

                await send_telegram(
                    session,
                    chat_id,
                    (
                        "🛑 <b>ربات متوقف شد.</b>\n\n"
                        "دیگر سیگنال جدیدی دریافت "
                        "نخواهی کرد.\n\n"
                        "برای فعال‌سازی دوباره:\n"
                        "<code>/start</code>"
                    ),
                )

            except Exception as exc:

                logger.error(
                    "Stop message failed: %s",
                    exc,
                )

        # ====================================================
        # STATUS
        # ====================================================

        elif text.startswith(
            "/status"
        ):

            active = (
                chat_id in state["users"]
                and state.get(
                    "running",
                    False,
                )
            )

            if active:

                status_text = (
                    "🟢 فعال"
                )

            else:

                status_text = (
                    "🔴 متوقف"
                )

            try:

                await send_telegram(
                    session,
                    chat_id,
                    (
                        "📡 <b>وضعیت ربات</b>\n\n"
                        f"وضعیت: {status_text}"
                    ),
                )

            except Exception as exc:

                logger.error(
                    "Status message failed: %s",
                    exc,
                )

    # ========================================================
    # LEGACY CHAT ID
    # ========================================================

    if LEGACY_CHAT_ID:

        try:

            legacy_id = int(
                LEGACY_CHAT_ID
            )

            # فقط در صورتی اضافه شود
            # که ربات فعال باشد.
            if (
                state.get(
                    "running",
                    False,
                )
                and legacy_id
                not in state["users"]
            ):

                state["users"].append(
                    legacy_id
                )

                changed = True

        except ValueError:

            logger.warning(
                "Invalid TELEGRAM_CHAT_ID"
            )

    if changed:

        save_state(
            state
        )

    return started


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

    data = await http_get(
        session,
        COINGECKO_URL,
        params=params,
    )

    if not isinstance(
        data,
        list,
    ):

        raise RuntimeError(
            "CoinGecko returned invalid data"
        )

    coins = []
    seen = set()

    for coin in data:

        symbol = str(
            coin.get(
                "symbol",
                "",
            )
        ).upper().strip()

        name = str(
            coin.get(
                "name",
                symbol,
            )
        ).strip()

        if not symbol:
            continue

        if symbol in seen:
            continue

        seen.add(
            symbol
        )

        coins.append(
            {
                "symbol": symbol,
                "name": name,
            }
        )

    logger.info(
        "Top coins: %s",
        len(coins),
    )

    return coins


# ============================================================
# BINANCE
# ============================================================

async def get_binance_exchange_info(
    session,
):
    url = (
        BINANCE_BASE_URL
        + "/api/v3/exchangeInfo"
    )

    return await http_get(
        session,
        url,
    )


def build_valid_symbols(
    exchange_info,
    coins,
):
    symbols = set()

    for item in exchange_info.get(
        "symbols",
        [],
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

            symbols.add(
                symbol.upper()
            )

    result = []

    for coin in coins:

        symbol = (
            coin["symbol"]
            + "USDT"
        )

        if symbol in symbols:

            result.append(
                {
                    "symbol": symbol,
                    "name": coin["name"],
                }
            )

    logger.info(
        "Valid Binance USDT symbols: %s",
        len(result),
    )

    return result


async def get_klines(
    session,
    symbol,
    interval,
    limit=100,
):
    url = (
        BINANCE_BASE_URL
        + "/api/v3/klines"
    )

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    return await http_get(
        session,
        url,
        params=params,
    )


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
        period + 1,
    ):

        change = (
            closes[i]
            - closes[i - 1]
        )

        if change >= 0:

            gains.append(
                change
            )

            losses.append(
                0
            )

        else:

            gains.append(
                0
            )

            losses.append(
                abs(change)
            )

    avg_gain = (
        sum(gains)
        / period
    )

    avg_loss = (
        sum(losses)
        / period
    )

    for i in range(
        period + 1,
        len(closes),
    ):

        change = (
            closes[i]
            - closes[i - 1]
        )

        gain = max(
            change,
            0,
        )

        loss = max(
            -change,
            0,
        )

        avg_gain = (
            (
                avg_gain
                * (period - 1)
            )
            + gain
        ) / period

        avg_loss = (
            (
                avg_loss
                * (period - 1)
            )
            + loss
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


# ============================================================
# EMA
# ============================================================

def ema(
    values,
    period,
):
    if not values:
        return None

    if len(values) < period:
        return None

    multiplier = (
        2 / (period + 1)
    )

    result = mean(
        values[:period]
    )

    for value in values[period:]:

        result = (
            (
                value
                - result
            )
            * multiplier
        ) + result

    return result


# ============================================================
# TECHNICAL SCORE
# ============================================================

def technical_score(
    closes,
    rsi,
):
    if len(closes) < 25:
        return 50

    ema9 = ema(
        closes,
        9,
    )

    ema21 = ema(
        closes,
        21,
    )

    if (
        ema9 is None
        or ema21 is None
    ):

        return 50

    score = 50.0

    if ema9 > ema21:

        score += 15

    elif ema9 < ema21:

        score -= 15

    if rsi >= 70:

        score += 8

    elif rsi <= 30:

        score -= 8

    elif rsi >= 55:

        score += 5

    elif rsi <= 45:

        score -= 5

    if len(closes) >= 6:

        momentum = (
            closes[-1]
            / closes[-6]
            - 1
        )

        if momentum > 0.01:

            score += 10

        elif momentum < -0.01:

            score -= 10

    return max(
        0,
        min(
            100,
            round(score),
        ),
    )


# ============================================================
# VOLUME
# ============================================================

def volume_info(
    klines,
):
    if len(klines) < 5:

        return (
            "📊 حجم: ➖ معمولی"
        )

    current_volume = float(
        klines[-1][5]
    )

    previous_volumes = [
        float(row[5])
        for row in klines[-4:-1]
    ]

    if not previous_volumes:

        return (
            "📊 حجم: ➖ معمولی"
        )

    avg_volume = mean(
        previous_volumes
    )

    if avg_volume <= 0:

        return (
            "📊 حجم: ➖ معمولی"
        )

    ratio = (
        current_volume
        / avg_volume
    )

    if ratio > 1.2:

        icon = "🔥"

    elif ratio < 0.8:

        icon = "📉"

    else:

        icon = "➖"

    return (
        f"📊 حجم: {icon} "
        f"{ratio:.2f}× میانگین ۳ کندل قبل"
    )


# ============================================================
# CANDLE REMAINING
# ============================================================

def candle_remaining(
    close_timestamp_ms,
):
    now_ms = (
        datetime.now(
            timezone.utc
        ).timestamp()
        * 1000
    )

    remaining = max(
        0,
        (
            close_timestamp_ms
            - now_ms
        ) / 1000,
    )

    minutes = int(
        remaining // 60
    )

    seconds = int(
        remaining % 60
    )

    return (
        f"{minutes:02d}:{seconds:02d}"
    )


# ============================================================
# POLYMARKET
# ============================================================

async def polymarket_sentiment(
    session,
    symbol,
):
    try:

        clean_symbol = (
            symbol.replace(
                "USDT",
                "",
            )
        )

        url = (
            "https://gamma-api.polymarket.com/"
            "public-search"
        )

        params = {
            "q": clean_symbol,
        }

        data = await http_get(
            session,
            url,
            params=params,
        )

        probability = (
            extract_probability(
                data
            )
        )

        if probability is None:

            return None

        return probability

    except Exception as exc:

        logger.debug(
            "Polymarket error %s: %s",
            symbol,
            exc,
        )

        return None


def extract_probability(
    data,
):
    if data is None:
        return None

    candidates = []

    def walk(obj):

        if isinstance(
            obj,
            dict,
        ):

            for key, value in obj.items():

                key_lower = str(
                    key
                ).lower()

                if key_lower in {
                    "probability",
                    "yes_probability",
                    "outcome_probability",
                }:

                    try:

                        value_float = float(
                            value
                        )

                        if (
                            0
                            <= value_float
                            <= 1
                        ):

                            value_float *= 100

                        if (
                            0
                            <= value_float
                            <= 100
                        ):

                            candidates.append(
                                value_float
                            )

                    except Exception:

                        pass

                walk(value)

        elif isinstance(
            obj,
            list,
        ):

            for item in obj:

                walk(item)

    walk(data)

    if not candidates:
        return None

    return candidates[0]


# ============================================================
# SIGNAL ZONE
# ============================================================

def signal_zone(
    rsi,
):
    if rsi > 70:

        return "overbought"

    if rsi < 30:

        return "oversold"

    return "neutral"


def signal_key(
    symbol,
    timeframe,
):
    return (
        f"{symbol}:{timeframe}"
    )


def is_new_signal(
    state,
    symbol,
    timeframe,
    rsi,
):
    zone = signal_zone(
        rsi
    )

    if zone == "neutral":

        return False

    key = signal_key(
        symbol,
        timeframe,
    )

    previous_zone = state[
        "signals"
    ].get(
        key
    )

    if previous_zone == zone:

        return False

    return True


def mark_signal(
    state,
    symbol,
    timeframe,
    rsi,
):
    zone = signal_zone(
        rsi
    )

    key = signal_key(
        symbol,
        timeframe,
    )

    if zone == "neutral":

        state[
            "signals"
        ].pop(
            key,
            None,
        )

    else:

        state[
            "signals"
        ][key] = zone


def reset_if_neutral(
    state,
    symbol,
    timeframe,
    rsi,
):
    if signal_zone(rsi) != "neutral":
        return

    key = signal_key(
        symbol,
        timeframe,
    )

    state[
        "signals"
    ].pop(
        key,
        None,
    )


# ============================================================
# BUILD SIGNAL
# ============================================================

async def build_signal(
    session,
    symbol,
    coin_name,
    timeframe_key,
    timeframe,
    klines,
):
    if not klines:
        return None

    if len(klines) < RSI_PERIOD + 1:
        return None

    closes = [
        float(row[4])
        for row in klines
    ]

    rsi_value = calculate_rsi(
        closes,
        RSI_PERIOD,
    )

    if rsi_value is None:
        return None

    zone = signal_zone(
        rsi_value
    )

    if zone == "neutral":

        return {
            "symbol": symbol,
            "coin_name": coin_name,
            "timeframe": timeframe_key,
            "rsi": rsi_value,
            "zone": "neutral",
            "is_signal": False,
        }

    # ========================================================
    # RSI
    # ========================================================

    if zone == "overbought":

        rsi_line = (
            f"🟢 RSI "
            f"{rsi_value:.2f} | اشباع خرید"
        )

    else:

        rsi_line = (
            f"🔴 RSI "
            f"{rsi_value:.2f} | اشباع فروش"
        )

    # ========================================================
    # AI / TECHNICAL
    # ========================================================

    ai_score = technical_score(
        closes,
        rsi_value,
    )

    if ai_score >= 50:

        ai_direction = "صعودی"
        ai_percent = ai_score

    else:

        ai_direction = "نزولی"
        ai_percent = (
            100 - ai_score
        )

    ai_line = (
        f"🤖 AI: "
        f"{ai_percent}% "
        f"{ai_direction}"
    )

    # ========================================================
    # NEXT CANDLE
    # ========================================================

    if zone == "overbought":

        technical_next = max(
            55,
            min(
                90,
                int(
                    50
                    + (
                        rsi_value
                        - 70
                    ) * 2
                ),
            ),
        )

        next_direction = "صعودی"

    else:

        technical_next = max(
            10,
            min(
                45,
                int(
                    50
                    - (
                        30
                        - rsi_value
                    ) * 2
                ),
            ),
        )

        next_direction = "نزولی"

    next_line = (
        f"🔮 بعدی: "
        f"{'🟢' if next_direction == 'صعودی' else '🔴'} "
        f"{next_direction} "
        f"{technical_next}%"
    )

    # ========================================================
    # POLYMARKET
    # ========================================================

    poly_probability = (
        await polymarket_sentiment(
            session,
            symbol,
        )
    )

    if poly_probability is None:

        poly_line = (
            "🎯 Polymarket: —"
        )

    else:

        if poly_probability >= 50:

            poly_direction = "صعودی"

        else:

            poly_direction = "نزولی"

        poly_line = (
            f"🎯 Polymarket: "
            f"{poly_probability:.0f}% "
            f"{poly_direction}"
        )

    # ========================================================
    # VOLUME
    # ========================================================

    volume_line = volume_info(
        klines
    )

    # ========================================================
    # CANDLE CLOSE
    # ========================================================

    close_time_ms = int(
        klines[-1][6]
    )

    close_line = (
        "⏳ بسته‌شدن: "
        + candle_remaining(
            close_time_ms
        )
    )

    # ========================================================
    # TRADINGVIEW
    # ========================================================

    tv_url = (
        "https://www.tradingview.com/"
        f"symbols/{symbol}/"
        "?exchange=BINANCE"
    )

    tv_line = (
        f'📈 <a href="{tv_url}">TV</a>'
    )

    return {
        "symbol": symbol,
        "coin_name": coin_name,
        "timeframe": timeframe_key,
        "zone": zone,
        "rsi": rsi_value,
        "is_signal": True,
        "text": "\n".join(
            [
                rsi_line,
                next_line,
                ai_line,
                poly_line,
                volume_line,
                close_line,
                tv_line,
            ]
        ),
    }


# ============================================================
# SCAN SYMBOL
# ============================================================

async def scan_symbol(
    session,
    symbol,
    coin_name,
    timeframe_key,
    timeframe,
    semaphore,
):
    async with semaphore:

        klines = await get_klines(
            session,
            symbol,
            timeframe,
            100,
        )

    return await build_signal(
        session,
        symbol,
        coin_name,
        timeframe_key,
        timeframe,
        klines,
    )


# ============================================================
# SCAN ONCE
# ============================================================

async def scan_once(
    session,
    symbols,
    state,
):
    alerts = []

    semaphore = asyncio.Semaphore(
        BINANCE_CONCURRENCY
    )

    startup_scan = not state.get(
        "startup_scan_done",
        False,
    )

    if startup_scan:

        logger.info(
            "STARTUP SCAN ACTIVE"
        )

    async def scan_one(item):

        symbol = item["symbol"]
        coin_name = item["name"]

        local_alerts = []

        for (
            timeframe_key,
            timeframe,
        ) in TIMEFRAMES.items():

            try:

                result = await scan_symbol(
                    session,
                    symbol,
                    coin_name,
                    timeframe_key,
                    timeframe,
                    semaphore,
                )

                if not result:
                    continue

                rsi = result.get(
                    "rsi"
                )

                if rsi is None:
                    continue

                zone = signal_zone(
                    rsi
                )

                # =================================================
                # NEUTRAL
                # =================================================

                if zone == "neutral":

                    reset_if_neutral(
                        state,
                        symbol,
                        timeframe_key,
                        rsi,
                    )

                    continue

                # =================================================
                # FIRST SCAN
                # =================================================

                if startup_scan:

                    local_alerts.append(
                        result
                    )

                    continue

                # =================================================
                # NORMAL SCAN
                # =================================================

                if is_new_signal(
                    state,
                    symbol,
                    timeframe_key,
                    rsi,
                ):

                    local_alerts.append(
                        result
                    )

            except Exception as exc:

                logger.exception(
                    "Error scanning %s %s: %s",
                    symbol,
                    timeframe_key,
                    exc,
                )

        return local_alerts

    tasks = [
        scan_one(item)
        for item in symbols
    ]

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

        if result:

            alerts.extend(
                result
            )

    # ============================================================
    # FIRST SCAN COMPLETED
    # ============================================================

    if startup_scan:

        state[
            "startup_scan_done"
        ] = True

        logger.info(
            "Startup scan completed. Alerts: %s",
            len(alerts),
        )

    # ============================================================
    # SORT
    # ============================================================

    alerts.sort(
        key=lambda item: (
            TF_ORDER.get(
                item["timeframe"],
                99,
            ),
            item["symbol"],
        )
    )

    return alerts


# ============================================================
# BUILD MESSAGE
# ============================================================

def build_batch_message(
    alerts,
):
    grouped = {}

    for item in alerts:

        timeframe = item[
            "timeframe"
        ]

        grouped.setdefault(
            timeframe,
            [],
        ).append(item)

    lines = []

    for timeframe_key in [
        "15M",
        "1H",
        "4H",
        "1D",
    ]:

        items = grouped.get(
            timeframe_key,
            [],
        )

        if not items:
            continue

        timeframe_display = (
            TIMEFRAMES[
                timeframe_key
            ]
        )

        header = (
            f"━━━━━━ "
            f"{timeframe_display} "
            f"━━━━━━"
        )

        lines.append(
            f"<b>{header}</b>"
        )

        for item in items:

            symbol = item[
                "symbol"
            ]

            if symbol.endswith(
                "USDT"
            ):

                display_symbol = (
                    symbol[:-4]
                )

            else:

                display_symbol = symbol

            lines.append(
                f"\n<b>💠 "
                f"{display_symbol}</b>"
            )

            lines.append(
                item["text"]
            )

    return "\n".join(
        lines
    )


# ============================================================
# MAIN
# ============================================================

async def main():

    if not TOKEN:

        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is not set"
        )

    state = load_state()

    logger.info(
        "Starting RSI Telegram Scanner"
    )

    connector = aiohttp.TCPConnector(
        limit=50
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        # ====================================================
        # COMMANDS BEFORE SCAN
        # ====================================================

        started = await process_commands(
            session,
            state,
        )

        # ====================================================
        # اگر ربات متوقف است
        # ====================================================

        if not state.get(
            "running",
            False,
        ):

            logger.info(
                "Bot is stopped. Waiting for /start."
            )

            return

        # ====================================================
        # TOP 100
        # ====================================================

        coins = await get_top_coins(
            session
        )

        # ====================================================
        # BINANCE
        # ====================================================

        exchange_info = (
            await get_binance_exchange_info(
                session
            )
        )

        symbols = build_valid_symbols(
            exchange_info,
            coins,
        )

        if not symbols:

            raise RuntimeError(
                "No valid Binance symbols found"
            )

        logger.info(
            "Symbols to scan: %s",
            len(symbols),
        )

        # ====================================================
        # MONITORING
        # ====================================================

        start_time = (
            asyncio.get_running_loop()
            .time()
        )

        collected_alerts = []

        # اگر /start همین الان دریافت شده،
        # اسکن اولیه را بلافاصله انجام بده.
        first_scan_now = started

        while True:

            elapsed = (
                asyncio.get_running_loop()
                .time()
                - start_time
            )

            if elapsed >= MONITOR_SECONDS:

                break

            # =================================================
            # COMMANDS
            # =================================================

            new_started = (
                await process_commands(
                    session,
                    state,
                )
            )

            # =================================================
            # STOP
            # =================================================

            if not state.get(
                "running",
                False,
            ):

                logger.info(
                    "Bot stopped."
                )

                break

            if new_started:

                first_scan_now = True

            # =================================================
            # SCAN
            # =================================================

            try:

                alerts = await scan_once(
                    session,
                    symbols,
                    state,
                )

                if alerts:

                    logger.info(
                        "Alerts this scan: %s",
                        len(alerts),
                    )

                    collected_alerts.extend(
                        alerts
                    )

                else:

                    logger.info(
                        "No new RSI alerts"
                    )

                save_state(
                    state
                )

            except Exception as exc:

                logger.exception(
                    "Scan failed: %s",
                    exc,
                )

            first_scan_now = False

            # =================================================
            # WAIT
            # =================================================

            elapsed = (
                asyncio.get_running_loop()
                .time()
                - start_time
            )

            remaining = (
                MONITOR_SECONDS
                - elapsed
            )

            if remaining <= 0:

                break

            await asyncio.sleep(
                min(
                    SCAN_INTERVAL_SECONDS,
                    remaining,
                )
            )

        # ====================================================
        # UNIQUE ALERTS
        # ====================================================

        unique_alerts = {}

        for item in collected_alerts:

            key = (
                item["symbol"],
                item["timeframe"],
                item["zone"],
            )

            unique_alerts[key] = item

        final_alerts = list(
            unique_alerts.values()
        )

        final_alerts.sort(
            key=lambda item: (
                TF_ORDER.get(
                    item["timeframe"],
                    99,
                ),
                item["symbol"],
            )
        )

        logger.info(
            "Total alerts collected: %s",
            len(final_alerts),
        )

        # ====================================================
        # SEND
        # ====================================================

        if final_alerts:

            message = (
                build_batch_message(
                    final_alerts
                )
            )

            users = list(
                state.get(
                    "users",
                    [],
                )
            )

            send_success = False

            for chat_id in users:

                try:

                    await send_telegram(
                        session,
                        chat_id,
                        message,
                    )

                    send_success = True

                except Exception as exc:

                    logger.error(
                        "Could not send to %s: %s",
                        chat_id,
                        exc,
                    )

            # =================================================
            # MARK AFTER SUCCESS
            # =================================================

            if send_success:

                for item in final_alerts:

                    mark_signal(
                        state,
                        item["symbol"],
                        item["timeframe"],
                        item["rsi"],
                    )

                save_state(
                    state
                )

                logger.info(
                    "Batch alert sent successfully"
                )

        else:

            logger.info(
                "No alerts to send"
            )

        save_state(
            state
        )

    logger.info(
        "RSI Scanner finished successfully"
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        logger.info(
            "Stopped by user"
        )

    except Exception as exc:

        logger.exception(
            "FATAL ERROR: %s",
            exc,
        )

        raise
