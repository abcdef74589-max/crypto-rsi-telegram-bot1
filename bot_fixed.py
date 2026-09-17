import asyncio
import json
import os
from datetime import datetime, timezone, timedelta

import aiohttp


# =========================================================
# CONFIG
# =========================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
BINANCE_EXCHANGE_URL = "https://data-api.binance.vision/api/v3/exchangeInfo"
BINANCE_KLINES_URL = "https://data-api.binance.vision/api/v3/klines"

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

USERS_FILE = "users.json"

SCAN_INTERVAL = 60
MONITOR_SECONDS = 285

TOP_COINS = 100
MAX_CONCURRENT = 10
RSI_PERIOD = 14

IRAN_TZ = timezone(timedelta(hours=3, minutes=30))

TIMEFRAMES = {
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1D": "1d",
}


# =========================================================
# STATE
# =========================================================

DEFAULT_STATE = {
    "users": [],
    "offset": 0,
    "signals": {},
    "running": False,
}


def load_state():
    if not os.path.exists(USERS_FILE):
        return DEFAULT_STATE.copy()

    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)

        for key, value in DEFAULT_STATE.items():
            if key not in state:
                state[key] = value

        return state

    except Exception:
        return DEFAULT_STATE.copy()


def save_state(state):
    with open(USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(
            state,
            f,
            ensure_ascii=False,
            indent=2
        )


# =========================================================
# NUMBER FORMAT
# =========================================================

def bold_number(value):
    normal = "0123456789"
    bold = "𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵"

    table = str.maketrans(
        normal,
        bold
    )

    return str(value).translate(table)


# =========================================================
# TELEGRAM
# =========================================================

async def telegram_request(session, method, data=None):

    if not TELEGRAM_BOT_TOKEN:
        print("TELEGRAM_BOT_TOKEN is missing.")
        return None

    url = f"{TELEGRAM_API}/{method}"

    try:

        async with session.post(
            url,
            data=data,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:

            if response.status != 200:

                text = await response.text()

                print(
                    f"Telegram error {response.status}: {text}"
                )

                return None

            return await response.json()

    except Exception as e:

        print(
            f"Telegram request error: {e}"
        )

        return None


async def send_message(
    session,
    chat_id,
    text
):

    return await telegram_request(
        session,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
    )


# =========================================================
# TELEGRAM UPDATES
# =========================================================

async def get_updates(
    session,
    offset
):

    result = await telegram_request(
        session,
        "getUpdates",
        {
            "offset": offset,
            "timeout": 1,
            "allowed_updates": '["message"]',
        }
    )

    if not result:
        return []

    return result.get(
        "result",
        []
    )


async def process_commands(
    session,
    state
):

    updates = await get_updates(
        session,
        state["offset"]
    )

    for update in updates:

        state["offset"] = (
            update["update_id"] + 1
        )

        message = update.get(
            "message",
            {}
        )

        chat = message.get(
            "chat",
            {}
        )

        chat_id = str(
            chat.get("id", "")
        )

        text = message.get(
            "text",
            ""
        ).strip().lower()

        if not chat_id:
            continue

        # -------------------------------------------------
        # START
        # -------------------------------------------------

        if text == "/start":

            if chat_id not in state["users"]:

                state["users"].append(
                    chat_id
                )

            state["running"] = True

            await send_message(
                session,
                chat_id,
                "🤖 ربات اسکنر RSI فعال شد.\n\n"
                "📊 تایم‌فریم‌ها:\n"
                "15m | 1h | 4h | 1D\n\n"
                "🔔 فقط سیگنال‌های جدید ارسال می‌شوند."
            )

        # -------------------------------------------------
        # STOP
        # -------------------------------------------------

        elif text == "/stop":

            if chat_id in state["users"]:

                state["users"].remove(
                    chat_id
                )

            if not state["users"]:

                state["running"] = False

            await send_message(
                session,
                chat_id,
                "⛔ ربات متوقف شد."
            )

        # -------------------------------------------------
        # STATUS
        # -------------------------------------------------

        elif text == "/status":

            status = (
                "🟢 فعال"
                if chat_id in state["users"]
                else "🔴 غیرفعال"
            )

            await send_message(
                session,
                chat_id,
                f"وضعیت ربات: {status}"
            )

    save_state(state)


# =========================================================
# COINGECKO
# =========================================================

async def get_top_coins(
    session
):

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": TOP_COINS,
        "page": 1,
        "sparkline": "false",
    }

    try:

        async with session.get(
            COINGECKO_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:

            if response.status != 200:

                print(
                    f"CoinGecko error: {response.status}"
                )

                return []

            data = await response.json()

            return [
                coin["symbol"].upper()
                for coin in data
                if coin.get("symbol")
            ]

    except Exception as e:

        print(
            f"CoinGecko error: {e}"
        )

        return []


# =========================================================
# BINANCE SYMBOLS
# =========================================================

async def get_binance_symbols(
    session
):

    try:

        async with session.get(
            BINANCE_EXCHANGE_URL,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:

            if response.status != 200:

                print(
                    f"Binance exchangeInfo error: "
                    f"{response.status}"
                )

                return set()

            data = await response.json()

            valid_symbols = set()

            for item in data.get(
                "symbols",
                []
            ):

                if (
                    item.get("status") == "TRADING"
                    and item.get("quoteAsset") == "USDT"
                    and item.get(
                        "isSpotTradingAllowed",
                        False
                    )
                ):

                    symbol = item.get(
                        "symbol"
                    )

                    if symbol:
                        valid_symbols.add(
                            symbol
                        )

            return valid_symbols

    except Exception as e:

        print(
            f"Binance symbols error: {e}"
        )

        return set()


# =========================================================
# SCAN SYMBOLS
# =========================================================

async def get_scan_symbols(
    session
):

    top_task = asyncio.create_task(
        get_top_coins(session)
    )

    binance_task = asyncio.create_task(
        get_binance_symbols(session)
    )

    top_coins, binance_symbols = (
        await asyncio.gather(
            top_task,
            binance_task
        )
    )

    result = []

    for coin in top_coins:

        symbol = f"{coin}USDT"

        if symbol in binance_symbols:

            result.append(
                symbol
            )

    print(
        f"Scanning {len(result)} symbols."
    )

    return result


# =========================================================
# KLINES
# =========================================================

async def get_klines(
    session,
    symbol,
    interval,
    limit=100
):

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }

    try:

        async with session.get(
            BINANCE_KLINES_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:

            if response.status != 200:
                return []

            return await response.json()

    except Exception as e:

        print(
            f"Kline error {symbol} {interval}: {e}"
        )

        return []


# =========================================================
# RSI
# =========================================================

def calculate_rsi(
    closes,
    period=14
):

    if len(closes) < period + 1:
        return None

    closes = [
        float(x)
        for x in closes
    ]

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
            losses.append(-change)

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
                avg_gain * (period - 1)
            )
            + gains[i]
        ) / period

        avg_loss = (
            (
                avg_loss * (period - 1)
            )
            + losses[i]
        ) / period

    if avg_loss == 0:

        if avg_gain == 0:
            return 50.0

        return 100.0

    rs = (
        avg_gain
        / avg_loss
    )

    rsi = (
        100
        - (100 / (1 + rs))
    )

    return float(rsi)


# =========================================================
# VOLUME RATIO
# =========================================================

def calculate_volume_ratio(
    klines
):

    if len(klines) < 4:
        return 0.0

    volumes = [
        float(k[5])
        for k in klines
    ]

    current_volume = volumes[-1]

    previous_volumes = (
        volumes[-4:-1]
    )

    if not previous_volumes:
        return 0.0

    average_volume = (
        sum(previous_volumes)
        / len(previous_volumes)
    )

    if average_volume <= 0:
        return 0.0

    return (
        current_volume
        / average_volume
    )


# =========================================================
# CLOSE TIME
# =========================================================

def get_close_time(
    klines
):

    if not klines:
        return ""

    close_timestamp = int(
        klines[-1][6]
    )

    close_dt = datetime.fromtimestamp(
        close_timestamp / 1000,
        tz=timezone.utc
    ).astimezone(
        IRAN_TZ
    )

    return close_dt.strftime(
        "%H:%M"
    )


# =========================================================
# TECHNICAL SCORE
# =========================================================

def technical_score(
    klines,
    rsi
):

    if rsi is None:
        return 50.0

    score = 50.0

    if rsi > 70:

        score += min(
            (rsi - 70) * 1.5,
            25
        )

    elif rsi < 30:

        score -= min(
            (30 - rsi) * 1.5,
            25
        )

    return max(
        0,
        min(100, score)
    )


# =========================================================
# MOMENTUM SCORE
# =========================================================

def momentum_score(
    klines
):

    if len(klines) < 6:
        return 50.0

    closes = [
        float(k[4])
        for k in klines
    ]

    current = closes[-1]
    previous = closes[-6]

    if previous <= 0:
        return 50.0

    change_percent = (
        (
            current - previous
        )
        / previous
    ) * 100

    score = (
        50
        + (change_percent * 5)
    )

    return max(
        0,
        min(100, score)
    )


# =========================================================
# POLYMARKET
# =========================================================

async def get_polymarket_probability(
    session,
    symbol
):

    # در حال حاضر مقدار معتبر و مستقیم
    # برای تمام نمادهای Binance از Polymarket
    # در این ربات تعریف نشده است.

    return None


# =========================================================
# NEXT CANDLE PREDICTION
# =========================================================

async def calculate_next_candle(
    session,
    symbol,
    klines,
    rsi
):

    technical = technical_score(
        klines,
        rsi
    )

    momentum = momentum_score(
        klines
    )

    polymarket = (
        await get_polymarket_probability(
            session,
            symbol
        )
    )

    if polymarket is None:
        polymarket = 50.0

    final_score = (
        technical
        + polymarket
        + momentum
    ) / 3

    final_score = round(
        max(
            0,
            min(100, final_score)
        )
    )

    if final_score >= 50:

        direction = "🟢 ↑"

    else:

        direction = "🔴 ↓"

    return (
        int(final_score),
        direction
    )


# =========================================================
# SIGNAL STATE
# =========================================================

def get_signal_zone(
    rsi
):

    if rsi > 70:
        return "high"

    if rsi < 30:
        return "low"

    return "normal"


def should_send_signal(
    state,
    symbol,
    timeframe,
    rsi
):

    zone = get_signal_zone(
        rsi
    )

    key = (
        f"{symbol}_{timeframe}"
    )

    previous_zone = (
        state["signals"].get(
            key,
            "normal"
        )
    )

    if zone == "normal":

        state["signals"][key] = (
            "normal"
        )

        return False

    if zone != previous_zone:

        state["signals"][key] = zone

        return True

    return False


# =========================================================
# DISPLAY SYMBOL
# =========================================================

def display_symbol(
    symbol
):

    if symbol.endswith("USDT"):

        return symbol[:-4]

    return symbol


# =========================================================
# TRADINGVIEW
# =========================================================

def tradingview_url(
    symbol
):

    return (
        "https://www.tradingview.com/chart/"
        "?symbol=BINANCE%3A"
        f"{symbol}"
    )


# =========================================================
# FORMAT SIGNAL
# =========================================================

def format_signal(
    signal
):

    symbol = display_symbol(
        signal["symbol"]
    )

    rsi = signal["rsi"]

    score = signal["prediction"]

    direction = signal["direction"]

    volume = signal["volume_ratio"]

    close_time = signal["close_time"]

    if rsi >= 70:

        rsi_icon = "🟢"

    elif rsi <= 30:

        rsi_icon = "🔴"

    else:

        rsi_icon = "⚪"

    rsi_text = bold_number(
        f"{rsi:.2f}"
    )

    score_text = bold_number(
        str(score)
    )

    volume_text = bold_number(
        f"{volume:.2f}"
    )

    close_text = bold_number(
        close_time
    )

    tv_url = tradingview_url(
        signal["symbol"]
    )

    return (
        f"💠 {symbol}\n\n"
        f"{rsi_icon} RSI                       {rsi_text}\n"
        f"🔮 {direction}                      {score_text} %\n"
        f"volume                       {volume_text} ×\n"
        f"close                        {close_text}\n"
        f"📈 <a href=\"{tv_url}\">TV</a>"
    )


# =========================================================
# BATCH MESSAGE
# =========================================================

def build_batch_message(
    signals
):

    if not signals:
        return None

    timeframe_order = [
        "15m",
        "1h",
        "4h",
        "1D"
    ]

    parts = []

    for timeframe in timeframe_order:

        tf_signals = [
            signal
            for signal in signals
            if signal["timeframe"]
            == timeframe
        ]

        if not tf_signals:
            continue

        parts.append(
            f"━━━━━━━━ {timeframe} ━━━━━━━━"
        )

        for index, signal in enumerate(
            tf_signals
        ):

            parts.append(
                format_signal(
                    signal
                )
            )

            if index != len(
                tf_signals
            ) - 1:

                parts.append(
                    "--------------------"
                )

        parts.append("")

    return "\n".join(
        parts
    ).strip()


# =========================================================
# SPLIT MESSAGE
# =========================================================

def split_message(
    text,
    max_length=4000
):

    if len(text) <= max_length:
        return [text]

    parts = []

    current = ""

    for block in text.split(
        "\n\n"
    ):

        if (
            len(current)
            + len(block)
            + 2
            > max_length
        ):

            if current:
                parts.append(
                    current
                )

            current = block

        else:

            if current:
                current += "\n\n"

            current += block

    if current:
        parts.append(
            current
        )

    return parts


# =========================================================
# SCAN ONE SYMBOL
# =========================================================

async def scan_symbol(
    session,
    semaphore,
    symbol,
    state
):

    results = []

    async with semaphore:

        for timeframe, interval in (
            TIMEFRAMES.items()
        ):

            klines = await get_klines(
                session,
                symbol,
                interval
            )

            if not klines:
                continue

            closes = [
                float(k[4])
                for k in klines
            ]

            rsi = calculate_rsi(
                closes,
                RSI_PERIOD
            )

            if rsi is None:
                continue

            if not (
                rsi > 70
                or rsi < 30
            ):

                state["signals"][
                    f"{symbol}_{timeframe}"
                ] = "normal"

                continue

            send_signal = (
                should_send_signal(
                    state,
                    symbol,
                    timeframe,
                    rsi
                )
            )

            if not send_signal:
                continue

            prediction, direction = (
                await calculate_next_candle(
                    session,
                    symbol,
                    klines,
                    rsi
                )
            )

            volume_ratio = (
                calculate_volume_ratio(
                    klines
                )
            )

            close_time = (
                get_close_time(
                    klines
                )
            )

            results.append(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "rsi": rsi,
                    "prediction": prediction,
                    "direction": direction,
                    "volume_ratio": volume_ratio,
                    "close_time": close_time,
                }
            )

    return results


# =========================================================
# SCAN MARKET
# =========================================================

async def scan_market(
    session,
    state
):

    symbols = await get_scan_symbols(
        session
    )

    if not symbols:
        return []

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT
    )

    tasks = []

    for symbol in symbols:

        tasks.append(
            scan_symbol(
                session,
                semaphore,
                symbol,
                state
            )
        )

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )

    signals = []

    for result in results:

        if isinstance(
            result,
            Exception
        ):

            print(
                f"Symbol scan error: {result}"
            )

            continue

        signals.extend(
            result
        )

    order = {
        "15m": 0,
        "1h": 1,
        "4h": 2,
        "1D": 3,
    }

    signals.sort(
        key=lambda x: (
            order.get(
                x["timeframe"],
                99
            ),
            x["symbol"]
        )
    )

    return signals


# =========================================================
# SEND SIGNALS
# =========================================================

async def send_signals(
    session,
    state,
    signals
):

    if not signals:
        return

    message = build_batch_message(
        signals
    )

    if not message:
        return

    chunks = split_message(
        message
    )

    for chat_id in list(
        state["users"]
    ):

        for chunk in chunks:

            await send_message(
                session,
                chat_id,
                chunk
            )

            await asyncio.sleep(
                0.3
            )


# =========================================================
# MAIN MONITOR
# =========================================================

async def monitor():

    state = load_state()

    timeout = aiohttp.ClientTimeout(
        total=30
    )

    connector = aiohttp.TCPConnector(
        limit=50
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        await process_commands(
            session,
            state
        )

        if not state["users"]:

            print(
                "No active users."
            )

            state["running"] = False

            save_state(state)

            return

        state["running"] = True

        save_state(state)

        start_time = (
            asyncio.get_event_loop().time()
        )

        while (
            asyncio.get_event_loop().time()
            - start_time
            < MONITOR_SECONDS
        ):

            await process_commands(
                session,
                state
            )

            if not state["users"]:

                state["running"] = False

                save_state(state)

                return

            try:

                signals = await scan_market(
                    session,
                    state
                )

                if signals:

                    print(
                        f"Found {len(signals)} new signals."
                    )

                    await send_signals(
                        session,
                        state,
                        signals
                    )

                else:

                    print(
                        "No new signals."
                    )

                save_state(state)

            except Exception as e:

                print(
                    f"Scan error: {e}"
                )

            await asyncio.sleep(
                SCAN_INTERVAL
            )

    state["running"] = False

    save_state(state)


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            monitor()
        )

    except KeyboardInterrupt:

        print(
            "Bot stopped."
        )

    except Exception as e:

        print(
            f"Fatal error: {e}"
        )
