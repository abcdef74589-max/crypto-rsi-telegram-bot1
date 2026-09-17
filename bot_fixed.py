import asyncio
import aiohttp
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# =========================================================
# CONFIG
# =========================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
BINANCE_BASE = "https://data-api.binance.vision"

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

STATE_FILE = Path("users.json")

SCAN_INTERVAL = 60
MONITOR_SECONDS = 285

TOP_COINS = 100
MAX_CONCURRENT = 10

RSI_PERIOD = 14

TIMEFRAMES = {
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1D": "1d",
}

IRAN_TZ = timezone(timedelta(hours=3, minutes=30))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("rsi-scanner")


# =========================================================
# STATE
# =========================================================

def default_state():
    return {
        "users": [],
        "offset": 0,
        "signals": {},
        "running": False,
        "startup_scan_done": False,
    }


def load_state():
    if not STATE_FILE.exists():
        return default_state()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        state = default_state()

        if isinstance(data, dict):
            state.update(data)

        if not isinstance(state.get("users"), list):
            state["users"] = []

        if not isinstance(state.get("signals"), dict):
            state["signals"] = {}

        return state

    except Exception as e:
        logger.error("State load error: %s", e)
        return default_state()


def save_state(state):
    temp_file = STATE_FILE.with_suffix(".tmp")

    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(
                state,
                f,
                ensure_ascii=False,
                indent=2
            )

        temp_file.replace(STATE_FILE)

    except Exception as e:
        logger.error("State save error: %s", e)


# =========================================================
# TELEGRAM
# =========================================================

async def telegram_request(session, method, params=None):
    try:
        async with session.post(
            f"{TELEGRAM_API}/{method}",
            data=params or {},
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:

            text = await response.text()

            if response.status != 200:
                logger.error(
                    "Telegram %s error %s: %s",
                    method,
                    response.status,
                    text
                )
                return None

            try:
                return json.loads(text)
            except Exception:
                return None

    except Exception as e:
        logger.error("Telegram request error: %s", e)
        return None


async def send_message(session, chat_id, text):
    if not chat_id:
        return False

    result = await telegram_request(
        session,
        "sendMessage",
        {
            "chat_id": str(chat_id),
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
    )

    return bool(result and result.get("ok"))


# =========================================================
# TELEGRAM LONG MESSAGE SPLITTER
# =========================================================

def split_message(text, max_length=4000):
    if len(text) <= max_length:
        return [text]

    parts = []
    current = ""

    for block in text.split("\n\n"):
        candidate = block if not current else current + "\n\n" + block

        if len(candidate) <= max_length:
            current = candidate
        else:
            if current:
                parts.append(current)

            if len(block) <= max_length:
                current = block
            else:
                start = 0

                while start < len(block):
                    parts.append(block[start:start + max_length])
                    start += max_length

                current = ""

    if current:
        parts.append(current)

    return parts


async def send_long_message(session, chat_id, text):
    success = True

    for part in split_message(text):
        ok = await send_message(session, chat_id, part)

        if not ok:
            success = False

        await asyncio.sleep(0.3)

    return success


# =========================================================
# COMMANDS
# =========================================================

async def process_commands(session, state):
    try:
        result = await telegram_request(
            session,
            "getUpdates",
            {
                "offset": state.get("offset", 0),
                "timeout": 1,
                "allowed_updates": json.dumps(["message"]),
            }
        )

        if not result or not result.get("ok"):
            return False

        updates = result.get("result", [])

        started = False

        for update in updates:
            update_id = update.get("update_id")

            if update_id is not None:
                state["offset"] = update_id + 1

            message = update.get("message", {})
            chat = message.get("chat", {})

            chat_id = chat.get("id")

            if not chat_id:
                continue

            text = message.get("text", "")

            if not isinstance(text, str):
                continue

            command = text.strip().lower().split()[0] if text.strip() else ""

            # ---------------------------------------------
            # START
            # ---------------------------------------------

            if command == "/start":

                chat_id_str = str(chat_id)

                if chat_id_str not in state["users"]:
                    state["users"].append(chat_id_str)

                state["running"] = True

                # Fresh startup scan
                state["signals"] = {}
                state["startup_scan_done"] = False

                await send_message(
                    session,
                    chat_id,
                    "👋 <b>خوش آمدید</b>\n\n"
                    "🤖 ربات RSI فعال شد.\n"
                    "🔎 اسکن اولیه شروع شد."
                )

                started = True

            # ---------------------------------------------
            # STOP
            # ---------------------------------------------

            elif command == "/stop":

                chat_id_str = str(chat_id)

                if chat_id_str in state["users"]:
                    state["users"].remove(chat_id_str)

                if len(state["users"]) == 0:
                    state["running"] = False

                await send_message(
                    session,
                    chat_id,
                    "🛑 <b>ربات متوقف شد.</b>"
                )

            # ---------------------------------------------
            # STATUS
            # ---------------------------------------------

            elif command == "/status":

                if state.get("running"):
                    status = "🟢 فعال"
                else:
                    status = "🔴 متوقف"

                await send_message(
                    session,
                    chat_id,
                    f"وضعیت ربات: <b>{status}</b>"
                )

        return started

    except Exception as e:
        logger.error("Command processing error: %s", e)
        return False


# =========================================================
# COINGECKO
# =========================================================

async def get_top_coins(session):
    try:
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": TOP_COINS,
            "page": 1,
            "sparkline": "false",
        }

        async with session.get(
            COINGECKO_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:

            if response.status != 200:
                logger.error(
                    "CoinGecko error: %s",
                    response.status
                )
                return []

            data = await response.json()

            result = []

            for coin in data:
                symbol = str(
                    coin.get("symbol", "")
                ).upper()

                if symbol:
                    result.append(symbol)

            logger.info("Top coins: %s", len(result))

            return result

    except Exception as e:
        logger.error("CoinGecko error: %s", e)
        return []


# =========================================================
# BINANCE SYMBOLS
# =========================================================

async def get_binance_symbols(session):
    try:
        async with session.get(
            f"{BINANCE_BASE}/api/v3/exchangeInfo",
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:

            if response.status != 200:
                logger.error(
                    "Binance exchangeInfo error: %s",
                    response.status
                )
                return set()

            data = await response.json()

            symbols = set()

            for item in data.get("symbols", []):

                if (
                    item.get("status") == "TRADING"
                    and item.get("quoteAsset") == "USDT"
                    and item.get("isSpotTradingAllowed", True)
                ):
                    symbols.add(
                        item.get("symbol", "")
                    )

            logger.info(
                "Binance USDT symbols: %s",
                len(symbols)
            )

            return symbols

    except Exception as e:
        logger.error("Binance symbols error: %s", e)
        return set()


# =========================================================
# MAP TOP COINS TO BINANCE
# =========================================================

def get_scan_symbols(top_coins, binance_symbols):
    result = []

    seen = set()

    for coin in top_coins:
        symbol = f"{coin}USDT"

        if symbol in binance_symbols and symbol not in seen:
            result.append(symbol)
            seen.add(symbol)

    return result


# =========================================================
# BINANCE KLINES
# =========================================================

async def get_klines(session, symbol, interval, limit=30):
    try:
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }

        async with session.get(
            f"{BINANCE_BASE}/api/v3/klines",
            params=params,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:

            if response.status != 200:
                return []

            return await response.json()

    except Exception:
        return []


# =========================================================
# RSI
# =========================================================

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    for i in range(period, len(gains)):
        avg_gain = (
            (avg_gain * (period - 1)) + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) + losses[i]
        ) / period

        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

    return round(rsi, 2)


# =========================================================
# VOLUME
# =========================================================

def calculate_volume_ratio(klines):
    """
    Current candle volume compared with
    average volume of previous 3 candles.
    """

    if len(klines) < 4:
        return None

    current_volume = float(klines[-1][5])

    previous_volumes = [
        float(klines[-2][5]),
        float(klines[-3][5]),
        float(klines[-4][5]),
    ]

    average_volume = sum(previous_volumes) / 3

    if average_volume <= 0:
        return None

    return round(
        current_volume / average_volume,
        2
    )


# =========================================================
# CLOSE TIME
# =========================================================

def get_close_time(klines):
    if not klines:
        return None

    close_timestamp_ms = int(klines[-1][6])

    dt_utc = datetime.fromtimestamp(
        close_timestamp_ms / 1000,
        tz=timezone.utc
    )

    dt_iran = dt_utc.astimezone(IRAN_TZ)

    return dt_iran.strftime("%H:%M")


# =========================================================
# TECHNICAL SCORE
# =========================================================

def technical_score(klines, rsi):
    """
    Local technical heuristic.

    This is NOT an external AI model.
    """

    if len(klines) < 6 or rsi is None:
        return 50.0

    closes = [
        float(k[4])
        for k in klines
    ]

    current = closes[-1]

    previous = closes[-2]
    before = closes[-3]

    score = 50.0

    # Momentum
    if current > previous:
        score += 10
    elif current < previous:
        score -= 10

    # Short trend
    if previous > before:
        score += 5
    elif previous < before:
        score -= 5

    # RSI momentum
    if rsi > 70:
        score += 10
    elif rsi < 30:
        score -= 10

    # Price position
    recent = closes[-6:]

    high = max(recent)
    low = min(recent)

    if high != low:
        position = (
            (current - low) /
            (high - low)
        )

        score += (position - 0.5) * 20

    return max(
        0,
        min(
            100,
            round(score, 2)
        )
    )


# =========================================================
# THIRD CRITERION
# =========================================================

def momentum_score(klines):
    """
    Third component of the 3-part average.
    """

    if len(klines) < 6:
        return 50.0

    closes = [
        float(k[4])
        for k in klines
    ]

    c1 = closes[-1]
    c2 = closes[-2]
    c3 = closes[-3]
    c4 = closes[-4]
    c5 = closes[-5]

    score = 50.0

    changes = [
        c1 - c2,
        c2 - c3,
        c3 - c4,
        c4 - c5,
    ]

    positive = sum(
        1 for x in changes if x > 0
    )

    negative = sum(
        1 for x in changes if x < 0
    )

    score += positive * 7
    score -= negative * 7

    if c1 > c5:
        score += 8
    elif c1 < c5:
        score -= 8

    return max(
        0,
        min(
            100,
            round(score, 2)
        )
    )


# =========================================================
# POLYMARKET
# =========================================================

async def get_polymarket_probability(
    session,
    symbol
):
    """
    Attempts to obtain a public Polymarket probability.

    Returns:
        probability 0-100 or None
    """

    try:
        coin = symbol.replace("USDT", "").lower()

        url = (
            "https://gamma-api.polymarket.com/"
            "markets"
        )

        params = {
            "active": "true",
            "closed": "false",
            "limit": 100,
        }

        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as response:

            if response.status != 200:
                return None

            data = await response.json()

            if not isinstance(data, list):
                return None

            candidates = []

            for market in data:
                text = " ".join([
                    str(market.get("question", "")),
                    str(market.get("title", "")),
                    str(market.get("description", "")),
                ]).lower()

                if coin in text:
                    candidates.append(market)

            if not candidates:
                return None

            market = candidates[0]

            # Try common probability fields
            for key in [
                "probability",
                "outcomePrices",
                "bestBid",
            ]:

                value = market.get(key)

                if value is None:
                    continue

                if isinstance(value, (int, float)):
                    p = float(value)

                    if 0 <= p <= 1:
                        p *= 100

                    if 0 <= p <= 100:
                        return round(p, 2)

                if isinstance(value, str):
                    try:
                        parsed = json.loads(value)

                        if isinstance(parsed, list) and parsed:
                            p = float(parsed[0])

                            if 0 <= p <= 1:
                                p *= 100

                            if 0 <= p <= 100:
                                return round(p, 2)

                    except Exception:
                        pass

            return None

    except Exception:
        return None


# =========================================================
# NEXT CANDLE SCORE
# =========================================================

async def calculate_next_candle(
    session,
    symbol,
    klines,
    rsi
):
    """
    Average of exactly 3 criteria:

    1. Technical score
    2. Polymarket
    3. Momentum score

    Result = arithmetic average.
    """

    technical = technical_score(
        klines,
        rsi
    )

    momentum = momentum_score(
        klines
    )

    polymarket = await get_polymarket_probability(
        session,
        symbol
    )

    if polymarket is None:
        polymarket = 50.0

    final_score = (
        technical +
        polymarket +
        momentum
    ) / 3

    final_score = round(
        max(0, min(100, final_score)),
        0
    )

    # Direction
    if final_score >= 50:
        direction = "🟢 صعودی"
    else:
        direction = "🔴 نزولی"

    return int(final_score), direction


# =========================================================
# SIGNAL STATE
# =========================================================

def get_zone(rsi):
    if rsi is None:
        return "neutral"

    if rsi > 70:
        return "high"

    if rsi < 30:
        return "low"

    return "neutral"


def signal_key(symbol, timeframe):
    return f"{symbol}:{timeframe}"


def should_send_signal(
    state,
    symbol,
    timeframe,
    rsi,
    startup=False
):
    zone = get_zone(rsi)

    key = signal_key(
        symbol,
        timeframe
    )

    old_zone = state["signals"].get(
        key,
        "neutral"
    )

    # RSI returned to neutral:
    # reset state
    if zone == "neutral":
        if old_zone != "neutral":
            state["signals"][key] = "neutral"

        return False

    # Startup:
    # send if currently extreme
    if startup:
        if old_zone != zone:
            state["signals"][key] = zone
            return True

        return False

    # New entry into extreme zone
    if old_zone != zone:
        state["signals"][key] = zone
        return True

    # Still inside same zone
    return False


# =========================================================
# SCAN ONE SYMBOL / TIMEFRAME
# =========================================================

async def scan_symbol_timeframe(
    session,
    semaphore,
    symbol,
    timeframe,
    interval,
    state,
    startup
):
    async with semaphore:

        klines = await get_klines(
            session,
            symbol,
            interval,
            limit=40
        )

        if not klines:
            return None

        closes = [
            float(k[4])
            for k in klines
        ]

        rsi = calculate_rsi(
            closes,
            RSI_PERIOD
        )

        if rsi is None:
            return None

        zone = get_zone(rsi)

        # Neutral also resets state
        if zone == "neutral":

            key = signal_key(
                symbol,
                timeframe
            )

            if state["signals"].get(key) != "neutral":
                state["signals"][key] = "neutral"

            return None

        if not should_send_signal(
            state,
            symbol,
            timeframe,
            rsi,
            startup
        ):
            return None

        # Volume
        volume_ratio = calculate_volume_ratio(
            klines
        )

        # Close
        close_time = get_close_time(
            klines
        )

        # Three-factor average
        next_score, direction = (
            await calculate_next_candle(
                session,
                symbol,
                klines,
                rsi
            )
        )

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "rsi": rsi,
            "zone": zone,
            "volume": volume_ratio,
            "close": close_time,
            "next_score": next_score,
            "direction": direction,
        }


# =========================================================
# SCAN ALL
# =========================================================

async def scan_once(
    session,
    state,
    startup=False
):
    top_coins = await get_top_coins(
        session
    )

    if not top_coins:
        return []

    binance_symbols = await get_binance_symbols(
        session
    )

    if not binance_symbols:
        return []

    symbols = get_scan_symbols(
        top_coins,
        binance_symbols
    )

    logger.info(
        "Symbols to scan: %s",
        len(symbols)
    )

    semaphore = asyncio.Semaphore(
        MAX_CONCURRENT
    )

    tasks = []

    for symbol in symbols:

        for timeframe, interval in TIMEFRAMES.items():

            tasks.append(
                scan_symbol_timeframe(
                    session,
                    semaphore,
                    symbol,
                    timeframe,
                    interval,
                    state,
                    startup
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

        if result:
            alerts.append(result)

    logger.info(
        "Alerts: %s",
        len(alerts)
    )

    return alerts


# =========================================================
# MESSAGE FORMAT
# =========================================================

def display_symbol(symbol):
    return symbol.replace(
        "USDT",
        ""
    )


def format_signal(signal):
    symbol = display_symbol(
        signal["symbol"]
    )

    rsi = signal["rsi"]

    if signal["zone"] == "high":
        rsi_icon = "🟢"
    else:
        rsi_icon = "🔴"

    volume = signal["volume"]

    if volume is None:
        volume_text = "—"
    else:
        volume_text = f"{volume:.2f}×"

    close = signal["close"] or "—"

    return (
        f"💠 <b>{symbol}</b>\n"
        f"{rsi_icon} RSI {rsi:.2f}\n"
        f"🔮 {signal['direction']} "
        f"{signal['next_score']}%\n"
        f"volume {volume_text}\n"
        f"close {close}\n"
        f"📈 <a href=\""
        f"https://www.tradingview.com/symbols/"
        f"{symbol}/?exchange=BINANCE"
        f"\">TV</a>"
    )


# =========================================================
# BATCH MESSAGE
# =========================================================

def build_batch_message(alerts):
    if not alerts:
        return ""

    timeframe_order = [
        "15m",
        "1h",
        "4h",
        "1D",
    ]

    grouped = {}

    for alert in alerts:

        tf = alert["timeframe"]

        if tf not in grouped:
            grouped[tf] = []

        grouped[tf].append(alert)

    sections = []

    for timeframe in timeframe_order:

        if timeframe not in grouped:
            continue

        # Sort by symbol
        grouped[timeframe].sort(
            key=lambda x: x["symbol"]
        )

        # Header
        header = (
            f"━━━━━━ {timeframe} ━━━━━━"
        )

        signals_text = []

        for alert in grouped[timeframe]:
            signals_text.append(
                format_signal(alert)
            )

        # Line between signals
        body = "\n\n--------------------\n\n".join(
            signals_text
        )

        sections.append(
            f"{header}\n\n{body}"
        )

    return "\n\n\n".join(sections)


# =========================================================
# MAIN
# =========================================================

async def main():
    if not TELEGRAM_BOT_TOKEN:
        logger.error(
            "TELEGRAM_BOT_TOKEN is missing."
        )
        return

    state = load_state()

    connector = aiohttp.TCPConnector(
        limit=50,
        ttl_dns_cache=300
    )

    timeout = aiohttp.ClientTimeout(
        total=60
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout
    ) as session:

        # -------------------------------------------------
        # Commands
        # -------------------------------------------------

        started = await process_commands(
            session,
            state
        )

        save_state(state)

        # -------------------------------------------------
        # If stopped, finish this workflow
        # -------------------------------------------------

        if not state.get("running"):
            logger.info(
                "Scanner is stopped."
            )
            return

        # -------------------------------------------------
        # Monitoring
        # -------------------------------------------------

        collected_alerts = []

        start_time = time.monotonic()

        first_scan = not state.get(
            "startup_scan_done",
            False
        )

        while (
            time.monotonic() - start_time
            < MONITOR_SECONDS
        ):

            try:
                alerts = await scan_once(
                    session,
                    state,
                    startup=first_scan
                )

                if alerts:
                    collected_alerts.extend(
                        alerts
                    )

                # Startup scan happens only once
                if first_scan:
                    state[
                        "startup_scan_done"
                    ] = True

                    first_scan = False

                save_state(state)

            except Exception as e:
                logger.error(
                    "Scan error: %s",
                    e
                )

            # -------------------------------------------------
            # Wait until next scan
            # -------------------------------------------------

            elapsed = (
                time.monotonic() - start_time
            )

            remaining = (
                MONITOR_SECONDS - elapsed
            )

            if remaining <= 0:
                break

            await asyncio.sleep(
                min(
                    SCAN_INTERVAL,
                    remaining
                )
            )

        # -------------------------------------------------
        # Remove duplicate signals from same cycle
        # -------------------------------------------------

        unique = {}

        for alert in collected_alerts:

            key = (
                alert["symbol"],
                alert["timeframe"]
            )

            unique[key] = alert

        collected_alerts = list(
            unique.values()
        )

        # -------------------------------------------------
        # Send one grouped message
        # -------------------------------------------------

        if collected_alerts:

            message = build_batch_message(
                collected_alerts
            )

            users = list(
                state.get("users", [])
            )

            for chat_id in users:

                await send_long_message(
                    session,
                    chat_id,
                    message
                )

                await asyncio.sleep(0.5)

        else:
            logger.info(
                "No new signals."
            )

        save_state(state)


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info(
            "Scanner stopped manually."
        )

    except Exception as e:
        logger.exception(
            "Fatal error: %s",
            e
    )
