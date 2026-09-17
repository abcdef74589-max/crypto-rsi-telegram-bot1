import os
import json
import asyncio
import logging
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

USERS_FILE = Path(os.getenv("USERS_FILE", "users.json"))

PRISM_API_KEY = os.getenv("PRISM_API_KEY", "").strip()

RUN_SECONDS = int(os.getenv("RUN_SECONDS", "285"))
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))

ALERT_MINUTES_BEFORE_CLOSE = 10


BINANCE_BASE_URLS = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api.binance.com",
]

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

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

TIMEFRAME_ORDER = [
    "15m",
    "1h",
    "4h",
    "1D",
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("rsi-scanner")


# ============================================================
# NUMBER FORMAT
# ============================================================

BOLD_DIGITS = str.maketrans(
    "0123456789",
    "𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵",
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
# STATE
# ============================================================

DEFAULT_STATE = {
    "users": [],
    "offset": 0,
    "states": {},
    "sent_alerts": {},
}


def load_state():

    if not USERS_FILE.exists():
        return json.loads(
            json.dumps(DEFAULT_STATE)
        )

    try:

        with USERS_FILE.open(
            "r",
            encoding="utf-8",
        ) as f:

            data = json.load(f)

        if not isinstance(data, dict):
            return json.loads(
                json.dumps(DEFAULT_STATE)
            )

        data.setdefault("users", [])
        data.setdefault("offset", 0)
        data.setdefault("states", {})
        data.setdefault("sent_alerts", {})

        return data

    except Exception as e:

        logger.error(
            "Cannot load users.json: %s",
            e,
        )

        return json.loads(
            json.dumps(DEFAULT_STATE)
        )


def save_state(state):

    tmp = USERS_FILE.with_suffix(".tmp")

    try:

        with tmp.open(
            "w",
            encoding="utf-8",
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
            e,
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
        f"https://api.telegram.org/"
        f"bot{TOKEN}/{method}"
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
        result
        and result.get("ok")
    )


async def send_long_message(
    session,
    chat_id,
    text,
):

    max_length = 4000

    if len(text) <= max_length:

        return await send_message(
            session,
            chat_id,
            text,
        )

    parts = []
    current = ""

    for block in text.split("\n\n"):

        candidate = (
            f"{current}\n\n{block}"
            if current
            else block
        )

        if len(candidate) > max_length:

            if current:
                parts.append(current)

            current = block

        else:

            current = candidate

    if current:
        parts.append(current)

    success = True

    for part in parts:

        if not await send_message(
            session,
            chat_id,
            part,
        ):

            success = False

    return success


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

    for update in result.get(
        "result",
        [],
    ):

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

        # ----------------------------------------------------
        # START
        # ----------------------------------------------------

        if text.startswith("/start"):

            if chat_id not in state["users"]:

                state["users"].append(
                    chat_id
                )

            await send_message(
                session,
                chat_id,
                "🤖 RSI Telegram Scanner\n\n"
                "فعال شد.\n\n"
                "RSI زیر 30 یا بالای 70 در "
                "تایم‌فریم‌های 15m، 1h، 4h و 1D "
                "بررسی می‌شود.",
            )

        # ----------------------------------------------------
        # STOP
        # ----------------------------------------------------

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

        # ----------------------------------------------------
        # STATUS
        # ----------------------------------------------------

        elif text.startswith("/status"):

            status = (
                "🟢 فعال"
                if chat_id in state["users"]
                else "🔴 غیرفعال"
            )

            await send_message(
                session,
                chat_id,
                status,
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

        try:

            async with session.get(
                f"{base}{path}",
                params=params or {},
                timeout=aiohttp.ClientTimeout(
                    total=20
                ),
            ) as response:

                if response.status == 200:

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
# TOP COINS
# ============================================================

async def get_top_coins(
    session,
):

    try:

        async with session.get(
            COINGECKO_URL,
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": TOP_N,
                "page": 1,
                "sparkline": "false",
            },
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
                    result.append(symbol)

            return result

    except Exception as e:

        logger.error(
            "CoinGecko failed: %s",
            e,
        )

        return []


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
        len(closes),
    ):

        change = (
            closes[i]
            - closes[i - 1]
        )

        gains.append(
            max(change, 0.0)
        )

        losses.append(
            max(-change, 0.0)
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

    rs = avg_gain / avg_loss

    return (
        100.0
        - (
            100.0
            / (1.0 + rs)
        )
    )


# ============================================================
# CANDLE TIME
# ============================================================

def candle_close_datetime(
    timeframe,
    now=None,
):

    now = (
        now
        or datetime.now(timezone.utc)
    )

    minutes = TIMEFRAMES[
        timeframe
    ]["minutes"]

    seconds = minutes * 60

    timestamp = int(
        now.timestamp()
    )

    candle_open = (
        timestamp
        - (
            timestamp
            % seconds
        )
    )

    return datetime.fromtimestamp(
        candle_open + seconds,
        timezone.utc,
    )


def minutes_until_close(
    timeframe,
    now=None,
):

    now = (
        now
        or datetime.now(timezone.utc)
    )

    close_dt = candle_close_datetime(
        timeframe,
        now,
    )

    return (
        close_dt - now
    ).total_seconds() / 60.0


def iran_time_string(dt):

    iran_tz = timezone(
        timedelta(
            hours=3,
            minutes=30,
        )
    )

    return dt.astimezone(
        iran_tz
    ).strftime("%H:%M")


# ============================================================
# VOLUME
# ============================================================

def calculate_volume_ratio(
    klines,
):

    if len(klines) < 5:
        return None

    current = float(
        klines[-1][5]
    )

    previous = [
        float(x[5])
        for x in klines[-4:-1]
    ]

    if not previous:
        return None

    average = (
        sum(previous)
        / len(previous)
    )

    if average <= 0:
        return None

    return current / average


# ============================================================
# RSI ZONE
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

    return "🔴"


def get_direction(zone):

    if zone == "high":
        return "↑"

    return "↓"


# ============================================================
# TRADINGVIEW
# ============================================================

def tradingview_url(
    symbol,
    timeframe,
):

    interval = TIMEFRAMES[
        timeframe
    ]["tradingview"]

    return (
        "https://www.tradingview.com/chart/"
        "?symbol=BINANCE%3A"
        f"{symbol}USDT"
        f"&interval={interval}"
    )


# ============================================================
# POLYMARKET
# ============================================================

def parse_json_string(value):

    if isinstance(value, list):
        return value

    if not isinstance(value, str):
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

        async with session.get(
            POLYMARKET_MARKETS_URL,
            params={
                "closed": "false",
                "limit": 100,
            },
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
                market.get(
                    "question"
                )
                or ""
            ).lower()

            if (
                symbol.lower()
                not in question
            ):
                continue

            if "up or down" not in question:
                continue

            if (
                timeframe == "15m"
                and "15m" not in question
            ):
                continue

            if (
                timeframe == "1h"
                and "1h" not in question
                and "1 hour"
                not in question
            ):
                continue

            if (
                timeframe == "4h"
                and "4h" not in question
                and "4 hour"
                not in question
            ):
                continue

            candidates.append(market)

        candidates.sort(
            key=lambda x:
            x.get(
                "endDate",
                "",
            )
        )

        for market in candidates:

            outcomes = parse_json_string(
                market.get(
                    "outcomes"
                )
            )

            prices = parse_json_string(
                market.get(
                    "outcomePrices"
                )
            )

            if not outcomes or not prices:
                continue

            for outcome, price in zip(
                outcomes,
                prices,
            ):

                if str(
                    outcome
                ).lower() in (
                    "up",
                    "yes",
                ):

                    probability = (
                        float(price)
                        * 100.0
                    )

                    if (
                        0
                        <= probability
                        <= 100
                    ):

                        return probability

    except Exception as e:

        logger.debug(
            "Polymarket unavailable: %s",
            e,
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

    try:

        async with session.get(
            PRISM_BASE_URL,
            params={
                "symbol": f"{symbol}USD",
                "timeframe":
                    PRISM_TIMEFRAMES[
                        timeframe
                    ],
            },
            headers={
                "Authorization":
                    f"Bearer {PRISM_API_KEY}",
                "Accept":
                    "application/json",
            },
            timeout=aiohttp.ClientTimeout(
                total=20
            ),
        ) as response:

            if response.status != 200:
                return None

            data = await response.json()

        for candle in data.get(
            "candles",
            [],
        ):

            if candle.get(
                "kind"
            ) != "future":
                continue

            probability = candle.get(
                "dir_prob"
            )

            if probability is None:
                continue

            probability = (
                float(probability)
                * 100.0
            )

            if (
                0
                <= probability
                <= 100
            ):

                return probability

    except Exception as e:

        logger.debug(
            "PRISM unavailable: %s",
            e,
        )

    return None


# ============================================================
# LOCAL FALLBACK PREDICTION
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
            (rsi - 70)
            * 0.8,
        )

    elif rsi <= 30:

        score -= min(
            20,
            (30 - rsi)
            * 0.8,
        )

    try:

        current = float(
            klines[-1][4]
        )

        previous = float(
            klines[-2][4]
        )

        if current > previous:
            score += 5

        elif current < previous:
            score -= 5

    except Exception:
        pass

    return max(
        0,
        min(100, score),
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
            min(100, score),
        )

    except Exception:
        return None


async def calculate_prediction(
    session,
    symbol,
    timeframe,
    klines,
    rsi,
):

    external_values = []

    prism = await get_prism_probability(
        session,
        symbol,
        timeframe,
    )

    if prism is not None:
        external_values.append(prism)

    polymarket = (
        await get_polymarket_probability(
            session,
            symbol,
            timeframe,
        )
    )

    if polymarket is not None:
        external_values.append(
            polymarket
        )

    if external_values:

        return (
            sum(external_values)
            / len(external_values)
        )

    local_values = []

    technical = technical_score(
        klines,
        rsi,
    )

    momentum = momentum_score(
        klines
    )

    if technical is not None:
        local_values.append(
            technical
        )

    if momentum is not None:
        local_values.append(
            momentum
        )

    if local_values:

        return (
            sum(local_values)
            / len(local_values)
        )

    return 50.0


# ============================================================
# DUPLICATE PROTECTION
# ============================================================

def alert_key(signal):

    close_timestamp = int(
        signal[
            "close_dt"
        ].timestamp()
    )

    return (
        f'{signal["symbol"]}:'
        f'{signal["timeframe"]}:'
        f'{signal["zone"]}:'
        f'{close_timestamp}'
    )


def claim_alert(
    state,
    signal,
):

    key = alert_key(signal)

    sent_alerts = state.setdefault(
        "sent_alerts",
        {},
    )

    if key in sent_alerts:

        return False

    sent_alerts[key] = {
        "created_at":
            datetime.now(
                timezone.utc
            ).isoformat()
    }

    # Keep users.json small.
    if len(sent_alerts) > 2000:

        sorted_items = sorted(
            sent_alerts.items(),
            key=lambda item:
                item[1].get(
                    "created_at",
                    "",
                ),
        )

        for old_key, _ in sorted_items[
            :500
        ]:

            sent_alerts.pop(
                old_key,
                None,
            )

    save_state(state)

    return True


# ============================================================
# SIGNAL FORMAT
# ============================================================

def format_signal(
    signal,
):

    symbol = signal["symbol"]
    timeframe = signal["timeframe"]
    rsi = signal["rsi"]
    zone = signal["zone"]
    volume_ratio = signal[
        "volume_ratio"
    ]
    close_dt = signal[
        "close_dt"
    ]
    prediction = signal[
        "prediction"
    ]

    rsi_icon = get_rsi_icon(
        zone
    )

    direction = get_direction(
        zone
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
        f"{rsi_icon} RSI"
        f"                 "
        f"{format_number(rsi, 2)}\n"
        f"🔮 {direction}"
        f"                "
        f"{format_number(prediction, 1)} %\n"
        f"volume"
        f"                 "
        f"{volume_text} ×\n"
        f"close"
        f"                  "
        f"{close_text}\n"
        f'📈 <a href="{tv_url}">TV</a>'
    )


def timeframe_header(
    timeframe,
):

    return (
        f"━━━━━━━━ "
        f"{timeframe} "
        f"━━━━━━━━"
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
                signal
                for signal in signals
                if signal[
                    "timeframe"
                ] == timeframe
            ]
        )

    blocks = []

    current_timeframe = None

    for signal in ordered:

        timeframe = signal[
            "timeframe"
        ]

        if (
            timeframe
            != current_timeframe
        ):

            blocks.append(
                timeframe_header(
                    timeframe
                )
            )

            current_timeframe = (
                timeframe
            )

        blocks.append(
            format_signal(
                signal
            )
        )

    return (
        "\n\n"
        "--------------------"
        "\n\n"
    ).join(blocks)


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

    if (
        not klines
        or len(klines)
        < RSI_PERIOD + 10
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

    # --------------------------------------------------------
    # ONLY CURRENT OPEN CANDLE WITH ~10 MINUTES LEFT
    # --------------------------------------------------------

    remaining = minutes_until_close(
        timeframe
    )

    if not (
        9.0
        <= remaining
        <= 11.0
    ):

        return None

    close_dt = candle_close_datetime(
        timeframe
    )

    prediction = (
        await calculate_prediction(
            session,
            symbol,
            timeframe,
            klines,
            rsi,
        )
    )

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "rsi": rsi,
        "zone": zone,
        "volume_ratio":
            calculate_volume_ratio(
                klines
            ),
        "close_dt": close_dt,
        "prediction": prediction,
    }


# ============================================================
# SCAN ALL
# ============================================================

async def scan_all(
    session,
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

    # Exact requested order:
    # 15m -> 1h -> 4h -> 1D

    for timeframe in TIMEFRAME_ORDER:

        for symbol in coins:

            try:

                signal = (
                    await scan_symbol(
                        session,
                        symbol,
                        timeframe,
                    )
                )

                if signal is not None:
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
# MAIN
# ============================================================

async def main():

    if not TOKEN:

        logger.error(
            "TELEGRAM_BOT_TOKEN is missing."
        )

        return

    state = load_state()

    # Optional legacy user.
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

    connector = aiohttp.TCPConnector(
        limit=30,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(
            total=40
        ),
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

        start_time = (
            asyncio.get_event_loop()
            .time()
        )

        sent_any = False

        while (
            asyncio.get_event_loop().time()
            - start_time
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
                    session
                )

                logger.info(
                    "Alerts found: %s",
                    len(signals),
                )

                # ------------------------------------------------
                # REMOVE DUPLICATES BEFORE SENDING
                # ------------------------------------------------

                new_signals = []

                for signal in signals:

                    if claim_alert(
                        state,
                        signal,
                    ):

                        new_signals.append(
                            signal
                        )

                logger.info(
                    "New alerts to send: %s",
                    len(new_signals),
                )

                if new_signals:

                    message = build_message(
                        new_signals
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


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        pass
