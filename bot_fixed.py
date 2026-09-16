import os
import json
import logging
import asyncio
from datetime import datetime, timezone

import aiohttp


# =========================================================
# CONFIG
# =========================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

TOP_N = int(os.getenv("TOP_N", "100"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))

STATE_FILE = "users.json"

BINANCE_BASE = "https://api.binance.com"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

TELEGRAM_BASE = (
    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
)

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

# Scan times: minute 05 / 20 / 35 / 50
SCAN_MINUTES = {5, 20, 35, 50}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

log = logging.getLogger("rsi-scanner")


# =========================================================
# HTTP
# =========================================================

async def http_get(session, url, params=None, timeout=20):
    async with session.get(
        url,
        params=params,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as response:

        text = await response.text()

        if response.status != 200:
            raise RuntimeError(
                f"HTTP {response.status}: {text[:500]}"
            )

        return json.loads(text)


async def http_post(session, url, payload, timeout=20):
    async with session.post(
        url,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as response:

        text = await response.text()

        if response.status != 200:
            raise RuntimeError(
                f"HTTP {response.status}: {text[:500]}"
            )

        return json.loads(text)


# =========================================================
# TELEGRAM
# =========================================================

async def telegram(session, method, payload):
    url = f"{TELEGRAM_BASE}/{method}"

    data = await http_post(
        session,
        url,
        payload,
    )

    if not data.get("ok"):
        raise RuntimeError(
            f"Telegram error: {data}"
        )

    return data


# =========================================================
# STATE
# =========================================================

def load_state():
    if not os.path.exists(STATE_FILE):
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
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("Invalid state format")

        data.setdefault("users", [])
        data.setdefault("offset", 0)
        data.setdefault("signals", {})

        return data

    except Exception as e:

        log.warning(
            "Could not load state: %s",
            e,
        )

        return {
            "users": [],
            "offset": 0,
            "signals": {},
        }


def save_state(state):
    temp_file = f"{STATE_FILE}.tmp"

    with open(
        temp_file,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            state,
            f,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(
        temp_file,
        STATE_FILE,
    )


# =========================================================
# TELEGRAM COMMANDS
# =========================================================

async def process_commands(session, state):

    offset = int(
        state.get("offset", 0)
    )

    log.info(
        "Checking Telegram updates..."
    )

    try:

        data = await telegram(
            session,
            "getUpdates",
            {
                "offset": offset,
                "timeout": 1,
            },
        )

    except Exception as e:

        log.error(
            "Telegram getUpdates failed: %s",
            e,
        )

        return

    updates = data.get(
        "result",
        [],
    )

    log.info(
        "Telegram updates received: %s",
        len(updates),
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
        )

        if not message:
            continue

        chat = message.get(
            "chat"
        )

        if not chat:
            continue

        chat_id = str(
            chat.get("id")
        )

        text = (
            message.get("text")
            or ""
        ).strip()

        log.info(
            "Telegram message from %s: %s",
            chat_id,
            text,
        )

        if text.startswith("/start"):

            users = state.setdefault(
                "users",
                [],
            )

            if chat_id not in users:

                users.append(
                    chat_id
                )

                log.info(
                    "New Telegram user added: %s",
                    chat_id,
                )

            try:

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text": (
                            "✅ ربات RSI فعال شد.\n\n"
                            "اسکن هر ۱۵ دقیقه انجام می‌شود.\n"
                            "⏱ 05 / 20 / 35 / 50"
                        ),
                    },
                )

                log.info(
                    "Start confirmation sent to %s",
                    chat_id,
                )

            except Exception as e:

                log.error(
                    "Failed to send /start confirmation "
                    "to %s: %s",
                    chat_id,
                    e,
                )

    save_state(state)

    log.info(
        "Registered Telegram users: %s",
        len(state.get("users", [])),
    )


# =========================================================
# BINANCE
# =========================================================

async def binance_get(
    session,
    path,
    params=None,
):
    return await http_get(
        session,
        f"{BINANCE_BASE}{path}",
        params=params,
    )


async def get_top_coins(session):

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_N,
        "page": 1,
        "sparkline": "false",
    }

    data = await http_get(
        session,
        f"{COINGECKO_BASE}/coins/markets",
        params=params,
    )

    coins = []

    for coin in data:

        symbol = str(
            coin.get("symbol", "")
        ).upper()

        if symbol:
            coins.append(symbol)

    return coins


async def get_binance_tickers(session):

    data = await binance_get(
        session,
        "/api/v3/ticker/24hr",
    )

    result = {}

    for item in data:

        symbol = item.get(
            "symbol"
        )

        if symbol:
            result[symbol] = item

    return result


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

        if change > 0:

            gains.append(change)
            losses.append(0)

        else:

            gains.append(0)
            losses.append(
                abs(change)
            )

    if len(gains) < period:
        return None

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

    return (
        100
        - (
            100
            / (1 + rs)
        )
    )


def get_zone(rsi):

    if rsi is None:
        return None

    if rsi > 70:
        return "high"

    if rsi < 30:
        return "low"

    return None


# =========================================================
# CANDLE PARSER
# =========================================================

def parse_klines(klines):

    result = []

    for k in klines:

        result.append(
            {
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            }
        )

    return result


# =========================================================
# VOLUME
# =========================================================

def format_number(value):

    if value is None:
        return "0"

    value = float(value)

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
            f"{value / 1_000:.2f}K"
        )

    return f"{value:.2f}"


def volume_cubes(
    volume,
    reference_max,
):

    if reference_max <= 0:
        return "🟦"

    ratio = (
        volume
        / reference_max
    )

    count = round(
        ratio * 8
    )

    if count < 1:
        count = 1

    if count > 8:
        count = 8

    return "🟦" * count


def get_volume_status(
    current_volume,
    previous_20,
):

    if not previous_20:
        return "➖ معمولی", 0

    average = (
        sum(previous_20)
        / len(previous_20)
    )

    if average <= 0:
        return "➖ معمولی", average

    if current_volume > (
        average * 1.2
    ):
        return "🔥 زیاد", average

    if current_volume < (
        average * 0.8
    ):
        return "📉 کم", average

    return "➖ معمولی", average


# =========================================================
# SCAN ONE COIN
# =========================================================

async def scan_coin(
    session,
    symbol,
    timeframe,
):

    interval = TIMEFRAMES[
        timeframe
    ]

    raw = await get_klines(
        session,
        symbol,
        interval,
        limit=100,
    )

    candles = parse_klines(
        raw
    )

    if len(candles) < 25:
        return None

    # Current OPEN candle
    current = candles[-1]

    closes = [
        c["close"]
        for c in candles
    ]

    rsi = calculate_rsi(
        closes,
        RSI_PERIOD,
    )

    zone = get_zone(rsi)

    if zone is None:
        return None

    current_volume = (
        current["volume"]
    )

    previous_3 = candles[-4:-1]

    previous_20 = candles[-21:-1]

    if len(previous_20) < 20:
        return None

    previous_volumes = [
        c["volume"]
        for c in previous_3
    ]

    status, average = (
        get_volume_status(
            current_volume,
            [
                c["volume"]
                for c in previous_20
            ],
        )
    )

    all_volumes = [
        current_volume,
        *previous_volumes,
    ]

    max_volume = max(
        all_volumes
    )

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "rsi": rsi,
        "zone": zone,
        "candle_open_ms": (
            current["open_time"]
        ),
        "current_volume": (
            current_volume
        ),
        "previous_volumes": (
            previous_volumes
        ),
        "average_volume": (
            average
        ),
        "volume_status": (
            status
        ),
        "max_volume": (
            max_volume
        ),
    }


# =========================================================
# SIGNAL DEDUPLICATION
# =========================================================

def filter_new_alerts(
    alerts,
    state,
):

    signals = state.setdefault(
        "signals",
        {},
    )

    new_alerts = []

    for alert in alerts:

        key = (
            f"{alert['symbol']}|"
            f"{alert['timeframe']}"
        )

        previous = signals.get(
            key
        )

        current_candle = (
            alert["candle_open_ms"]
        )

        current_zone = (
            alert["zone"]
        )

        if previous:

            previous_candle = (
                previous.get(
                    "candle_open_ms"
                )
            )

            previous_zone = (
                previous.get("zone")
            )

            if (
                previous_candle
                == current_candle
                and previous_zone
                == current_zone
            ):
                continue

        new_alerts.append(
            alert
        )

        signals[key] = {
            "candle_open_ms":
                current_candle,
            "zone":
                current_zone,
        }

    return new_alerts


# =========================================================
# MESSAGE
# =========================================================

def build_message(alerts):

    groups = {
        "15m": [],
        "1H": [],
        "4H": [],
        "1D": [],
    }

    for alert in alerts:

        groups[
            alert["timeframe"]
        ].append(alert)

    lines = [
        "🚨 سیگنال RSI",
        "",
    ]

    for timeframe in (
        TIMEFRAME_ORDER
    ):

        items = groups[
            timeframe
        ]

        if not items:
            continue

        lines.append(
            f"━━━━━━━━ {timeframe} ━━━━━━━━"
        )

        for alert in items:

            symbol = alert[
                "symbol"
            ]

            rsi = alert[
                "rsi"
            ]

            zone = alert[
                "zone"
            ]

            if zone == "high":

                signal_icon = "🔴"
                signal_text = (
                    "اشباع خرید"
                )

            else:

                signal_icon = "🟢"
                signal_text = (
                    "اشباع فروش"
                )

            current_volume = (
                alert[
                    "current_volume"
                ]
            )

            previous_volumes = (
                alert[
                    "previous_volumes"
                ]
            )

            average = (
                alert[
                    "average_volume"
                ]
            )

            max_volume = (
                alert[
                    "max_volume"
                ]
            )

            status = (
                alert[
                    "volume_status"
                ]
            )

            current_cubes = (
                volume_cubes(
                    current_volume,
                    max_volume,
                )
            )

            lines.append(
                f"💠 {symbol}"
            )

            lines.append(
                f"{signal_icon} RSI: "
                f"{rsi:.2f} | "
                f"{signal_text}"
            )

            lines.append(
                f"📦 {current_cubes} "
                f"{format_number(current_volume)}"
            )

            for index, volume in enumerate(
                previous_volumes,
                start=1,
            ):

                cubes = volume_cubes(
                    volume,
                    max_volume,
                )

                number_icon = {
                    1: "1️⃣",
                    2: "2️⃣",
                    3: "3️⃣",
                }[index]

                lines.append(
                    f"{number_icon} "
                    f"{cubes} "
                    f"{format_number(volume)}"
                )

            lines.append(
                f"{status} | "
                f"📊 میانگین: "
                f"{format_number(average)}"
            )

            tradingview_symbol = (
                f"BINANCE:{symbol}"
            )

            lines.append(
                "📈 TradingView: "
                f"https://www.tradingview.com/"
                f"symbol/{tradingview_symbol}/"
            )

            lines.append("")

    return "\n".join(lines)


# =========================================================
# SEND ALERTS
# =========================================================

async def send_message_to_all(
    session,
    state,
    text,
):

    users = state.get(
        "users",
        [],
    )

    if not users:

        log.warning(
            "No Telegram users registered."
        )

        return

    for chat_id in users:

        try:

            await telegram(
                session,
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": text,
                    "disable_web_page_preview":
                        True,
                },
            )

            log.info(
                "Telegram alert sent to %s",
                chat_id,
            )

        except Exception as e:

            log.error(
                "Telegram send failed "
                "for %s: %s",
                chat_id,
                e,
            )


# =========================================================
# TIME
# =========================================================

def is_scan_time():

    now = datetime.now(
        timezone.utc
    )

    return (
        now.minute
        in SCAN_MINUTES
    )


# =========================================================
# MAIN
# =========================================================

async def main():

    if not TELEGRAM_BOT_TOKEN:

        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing."
        )

    state = load_state()

    async with aiohttp.ClientSession() as session:

        # -------------------------------------------------
        # IMPORTANT:
        # Telegram commands are processed BEFORE
        # checking scan time.
        # -------------------------------------------------

        await process_commands(
            session,
            state,
        )

        # -------------------------------------------------
        # Scan time check
        # -------------------------------------------------

        if not is_scan_time():

            now = datetime.now(
                timezone.utc
            )

            log.info(
                "Not a scan time. "
                "Current UTC time: %s",
                now.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            )

            return

        log.info(
            "Starting RSI scan..."
        )

        # -------------------------------------------------
        # Top coins
        # -------------------------------------------------

        try:

            top_coins = (
                await get_top_coins(
                    session
                )
            )

            log.info(
                "Top coins: %s",
                len(top_coins),
            )

        except Exception as e:

            log.error(
                "Top coins failed: %s",
                e,
            )

            return

        # -------------------------------------------------
        # Binance tickers
        # -------------------------------------------------

        try:

            tickers = (
                await get_binance_tickers(
                    session
                )
            )

            log.info(
                "Binance tickers: %s",
                len(tickers),
            )

        except Exception as e:

            log.error(
                "Binance tickers failed: %s",
                e,
            )

            return

        # -------------------------------------------------
        # Build USDT symbols
        # -------------------------------------------------

        symbols = []

        for coin in top_coins:

            symbol = (
                f"{coin}USDT"
            )

            if symbol in tickers:

                symbols.append(
                    symbol
                )

        log.info(
            "USDT symbols to scan: %s",
            len(symbols),
        )

        # -------------------------------------------------
        # Scan
        # -------------------------------------------------

        alerts = []

        for symbol in symbols:

            for timeframe in (
                TIMEFRAME_ORDER
            ):

                try:

                    result = (
                        await scan_coin(
                            session,
                            symbol,
                            timeframe,
                        )
                    )

                    if result:

                        alerts.append(
                            result
                        )

                except Exception as e:

                    log.warning(
                        "Scan failed "
                        "%s %s: %s",
                        symbol,
                        timeframe,
                        e,
                    )

                await asyncio.sleep(
                    0.03
                )

        log.info(
            "Raw alerts: %s",
            len(alerts),
        )

        # -------------------------------------------------
        # Remove duplicate signals
        # -------------------------------------------------

        new_alerts = (
            filter_new_alerts(
                alerts,
                state,
            )
        )

        save_state(
            state
        )

        log.info(
            "New alerts: %s",
            len(new_alerts),
        )

        if not new_alerts:

            log.info(
                "No new signals."
            )

            return

        # -------------------------------------------------
        # Build and send message
        # -------------------------------------------------

        message = (
            build_message(
                new_alerts
            )
        )

        await send_message_to_all(
            session,
            state,
            message,
        )


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    asyncio.run(
        main()
        )
