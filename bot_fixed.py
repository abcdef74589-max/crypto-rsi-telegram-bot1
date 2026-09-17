import os
import asyncio
import logging
import json
import hashlib
import re
import math
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

ALERT_MODE = os.getenv("ALERT_MODE", "changes").lower()
USERS_FILE = Path(os.getenv("USERS_FILE", "users.json"))

PRISM_API_KEY = os.getenv("PRISM_API_KEY", "").strip()

# Keep previous timing
RUN_SECONDS = 285
SCAN_INTERVAL = 60

# Alert when approximately 10 minutes remain
ALERT_MINUTES_BEFORE_CLOSE = 10

# Duplicate-message protection
SENT_MESSAGE_RETENTION_MINUTES = 15


# ============================================================
# URLS
# ============================================================

DEFILLAMA_TOKENS_URL = "https://defillama.com/tokens"

BINANCE_BASES = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api.binance.com",
]

TELEGRAM_URL = "https://api.telegram.org"

POLYMARKET_URL = "https://gamma-api.polymarket.com/markets"

PRISM_URL = "https://api.prismforecast.com"

DEGEN_SIGNAL_URL = "https://degensignal.com"


# ============================================================
# TIMEFRAMES
# ============================================================

TFS = {
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1D": "1d",
}

TF_ORDER = {
    "15m": 0,
    "1h": 1,
    "4h": 2,
    "1D": 3,
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ============================================================
# NUMBER FORMAT
# ============================================================

BOLD_DIGITS = str.maketrans(
    "0123456789",
    "𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵"
)


def bold_numbers(text):
    return str(text).translate(BOLD_DIGITS)


def fmt_number(value, decimals=2):
    try:
        return bold_numbers(f"{float(value):.{decimals}f}")
    except Exception:
        return bold_numbers(str(value))


def fmtv(value):
    try:
        value = float(value)

        if value >= 1e12:
            return f"{value / 1e12:.2f}T"

        if value >= 1e9:
            return f"{value / 1e9:.2f}B"

        if value >= 1e6:
            return f"{value / 1e6:.2f}M"

        if value >= 1e3:
            return f"{value / 1e3:.2f}K"

        return f"{value:.2f}"

    except Exception:
        return "0.00"


# ============================================================
# HTTP
# ============================================================

async def get_json(session, url, params=None, headers=None):
    last_error = None

    for attempt in range(3):
        try:
            request_headers = {
                "User-Agent": "crypto-rsi-telegram-bot/3.0",
                "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
            }

            if headers:
                request_headers.update(headers)

            async with session.get(
                url,
                params=params,
                headers=request_headers,
                timeout=aiohttp.ClientTimeout(total=25),
            ) as response:

                if response.status == 429:
                    retry_after = response.headers.get(
                        "Retry-After",
                        "3"
                    )

                    try:
                        retry_after = float(retry_after)
                    except Exception:
                        retry_after = 3

                    await asyncio.sleep(min(retry_after, 15))
                    continue

                if response.status >= 400:
                    text = await response.text()

                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status,
                        message=text[:500],
                        headers=response.headers,
                    )

                content_type = (
                    response.headers.get("Content-Type", "").lower()
                )

                if "json" in content_type:
                    return await response.json()

                text = await response.text()

                try:
                    return json.loads(text)
                except Exception:
                    return text

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ) as exc:

            last_error = exc

            if attempt == 2:
                raise

            await asyncio.sleep(1.5 * (attempt + 1))

    raise last_error


async def post_json(session, url, payload, headers=None):
    async with session.post(
        url,
        json=payload,
        headers=headers or {},
        timeout=aiohttp.ClientTimeout(total=25),
    ) as response:

        text = await response.text()

        if response.status >= 400:
            raise RuntimeError(
                f"HTTP {response.status}: {text[:500]}"
            )

        try:
            data = json.loads(text)
        except Exception:
            raise RuntimeError(
                f"Invalid JSON response: {text[:500]}"
            )

        return data


# ============================================================
# BINANCE
# ============================================================

async def binance(session, path, params=None):
    last_error = None

    for base in BINANCE_BASES:
        try:
            return await get_json(
                session,
                base + path,
                params=params,
            )

        except aiohttp.ClientResponseError as exc:
            last_error = exc

            if exc.status not in (
                400,
                403,
                418,
                429,
                451,
            ):
                raise

            logging.warning(
                "%s returned HTTP %s; trying next endpoint",
                base,
                exc.status,
            )

        except Exception as exc:
            last_error = exc

            logging.warning(
                "%s failed: %s",
                base,
                exc,
            )

    if last_error:
        raise last_error

    raise RuntimeError("No Binance endpoint available")


async def get_binance_exchange_info(session):
    return await binance(
        session,
        "/api/v3/exchangeInfo"
    )


async def get_binance_tickers(session):
    data = await binance(
        session,
        "/api/v3/ticker/24hr"
    )

    return {
        x["symbol"]: x
        for x in data
        if x.get("symbol")
    }


def build_binance_spot_symbols(exchange_info):
    result = {}

    for item in exchange_info.get("symbols", []):
        symbol = item.get("symbol", "")

        if not symbol:
            continue

        if item.get("status") != "TRADING":
            continue

        if item.get("quoteAsset") != "USDT":
            continue

        if not item.get("isSpotTradingAllowed", False):
            continue

        base_asset = item.get("baseAsset", "").upper()

        if not base_asset:
            continue

        result[base_asset] = symbol

    return result


# ============================================================
# DEFILLAMA
# ============================================================

def clean_symbol(symbol):
    if not symbol:
        return ""

    symbol = str(symbol).upper().strip()

    symbol = re.sub(
        r"[^A-Z0-9]",
        "",
        symbol
    )

    return symbol


def parse_number(text):
    if text is None:
        return None

    text = str(text)

    text = text.replace(",", "")
    text = text.replace("$", "")
    text = text.strip()

    multiplier = 1

    if text.endswith("T"):
        multiplier = 1e12
        text = text[:-1]

    elif text.endswith("B"):
        multiplier = 1e9
        text = text[:-1]

    elif text.endswith("M"):
        multiplier = 1e6
        text = text[:-1]

    elif text.endswith("K"):
        multiplier = 1e3
        text = text[:-1]

    try:
        return float(text) * multiplier
    except Exception:
        return None


def extract_defillama_rows_from_text(text):
    """
    Extract token rows from the rendered DeFiLlama Tokens page.

    The page currently exposes:
    rank / coin / price / changes / mcap / fdv / ...

    We intentionally use the symbol shown next to the coin.
    """

    if not text:
        return []

    result = []

    # Typical pattern:
    # BitcoinBitcoinBTC
    #
    # This parser is deliberately conservative.
    pattern = re.compile(
        r"(?:Image:\s*Logo of\s*)?"
        r"([A-Za-z0-9][A-Za-z0-9 .'\-&]{1,80}?)"
        r"([A-Z][A-Z0-9]{1,14})"
    )

    seen = set()

    for match in pattern.finditer(text):
        name = match.group(1).strip()
        symbol = clean_symbol(match.group(2))

        if not symbol:
            continue

        if symbol in seen:
            continue

        # Ignore obvious non-token table words
        if symbol in {
            "USD",
            "Mcap",
            "FDV",
            "REV",
            "TVL",
            "ETH",
            "BTC",
        }:
            # BTC/ETH are valid, so handle them separately below.
            if symbol not in ("BTC", "ETH"):
                continue

        seen.add(symbol)

        result.append({
            "name": name,
            "symbol": symbol,
        })

    return result


async def get_defillama_top_tokens(session):
    """
    Get the top token universe from DeFiLlama.

    We read the Tokens ranking page because the public free API
    does not expose a simple documented "top tokens by market cap"
    endpoint equivalent to the website table.
    """

    try:
        async with session.get(
            DEFILLAMA_TOKENS_URL,
            headers={
                "User-Agent": "crypto-rsi-telegram-bot/3.0",
                "Accept": "text/html",
            },
            timeout=aiohttp.ClientTimeout(total=30),
        ) as response:

            if response.status >= 400:
                raise RuntimeError(
                    f"DeFiLlama HTTP {response.status}"
                )

            html = await response.text()

    except Exception as exc:
        logging.error(
            "DeFiLlama request failed: %s",
            exc
        )
        return []

    # First try to locate common symbol/name information
    rows = extract_defillama_rows_from_text(html)

    # Keep first TOP_N unique symbols.
    result = []
    seen = set()

    for row in rows:
        symbol = row["symbol"]

        if symbol in seen:
            continue

        seen.add(symbol)
        result.append(row)

        if len(result) >= TOP_N:
            break

    logging.info(
        "DeFiLlama candidates: %s",
        len(result)
    )

    return result


# ============================================================
# FINAL COIN UNIVERSE
# ============================================================

async def top_coins(session):
    """
    Final universe:

    1. Get top tokens from DeFiLlama.
    2. Intersect with Binance USDT spot.
    3. Keep up to TOP_N.
    """

    defillama_tokens = await get_defillama_top_tokens(session)

    if not defillama_tokens:
        raise RuntimeError(
            "Could not obtain the DeFiLlama token ranking."
        )

    exchange_info = await get_binance_exchange_info(session)

    binance_symbols = build_binance_spot_symbols(
        exchange_info
    )

    final = []

    for token in defillama_tokens:
        base = token["symbol"].upper()

        if base not in binance_symbols:
            continue

        final.append({
            "name": token["name"],
            "symbol": base,
            "pair": binance_symbols[base],
        })

        if len(final) >= TOP_N:
            break

    logging.info(
        "Final DeFiLlama + Binance coins: %s",
        len(final)
    )

    return final


# ============================================================
# RSI
# ============================================================

def calculate_rsi(values, period=14):
    if len(values) < period + 1:
        return []

    gains = []
    losses = []

    for i in range(1, period + 1):
        change = values[i] - values[i - 1]

        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    output = [None] * period

    if avg_loss == 0:
        first_rsi = 100 if avg_gain > 0 else 50
    else:
        rs = avg_gain / avg_loss
        first_rsi = 100 - (100 / (1 + rs))

    output.append(first_rsi)

    for i in range(period + 1, len(values)):
        change = values[i] - values[i - 1]

        gain = max(change, 0)
        loss = max(-change, 0)

        avg_gain = (
            avg_gain * (period - 1) + gain
        ) / period

        avg_loss = (
            avg_loss * (period - 1) + loss
        ) / period

        if avg_loss == 0:
            current_rsi = 100 if avg_gain > 0 else 50
        else:
            rs = avg_gain / avg_loss
            current_rsi = 100 - (100 / (1 + rs))

        output.append(current_rsi)

    return output


def get_zone(value):
    if value < 30:
        return "oversold"

    if value > 70:
        return "overbought"

    return "neutral"


# ============================================================
# CANDLE TIME
# ============================================================

def now_ms():
    return int(
        datetime.now(timezone.utc).timestamp() * 1000
    )


def remaining_minutes(close_ms):
    return (
        close_ms - now_ms()
    ) / 60000.0


def iran_time_from_ms(ms):
    dt = datetime.fromtimestamp(
        ms / 1000,
        tz=timezone.utc
    )

    # Iran = UTC+3:30
    dt = dt + timedelta(hours=3, minutes=30)

    return dt.strftime("%H:%M")


# ============================================================
# VOLUME
# ============================================================

def volume_state(current_volume, previous_volumes):
    if not previous_volumes:
        return "معمولی ➖"

    average = sum(previous_volumes) / len(
        previous_volumes
    )

    if average <= 0:
        return "معمولی ➖"

    ratio = current_volume / average

    if ratio >= 1.20:
        return "زیاد 🔥"

    if ratio <= 0.80:
        return "کم 📉"

    return "معمولی ➖"


# ============================================================
# TRADINGVIEW
# ============================================================

def tradingview_url(symbol, tf):
    interval_map = {
        "15m": "15",
        "1h": "60",
        "4h": "240",
        "1D": "D",
    }

    interval = interval_map.get(tf, "15")

    return (
        "https://www.tradingview.com/chart/"
        f"?symbol=BINANCE%3A{symbol}"
        f"&interval={interval}"
    )


# ============================================================
# POLYMARKET
# ============================================================

async def polymarket_prediction(
    session,
    symbol,
    tf
):
    try:
        data = await get_json(
            session,
            POLYMARKET_URL,
            params={
                "closed": "false",
                "limit": 100,
            }
        )

        if not isinstance(data, list):
            return None

        base = symbol.replace(
            "USDT",
            ""
        ).upper()

        wanted = tf.lower()

        for market in data:
            question = str(
                market.get("question", "")
            )

            q = question.lower()

            if base.lower() not in q:
                continue

            if "up or down" not in q:
                continue

            if wanted not in q:
                continue

            outcomes = market.get("outcomes")
            prices = market.get("outcomePrices")

            if not outcomes or not prices:
                continue

            if isinstance(outcomes, str):
                try:
                    outcomes = json.loads(outcomes)
                except Exception:
                    outcomes = []

            if isinstance(prices, str):
                try:
                    prices = json.loads(prices)
                except Exception:
                    prices = []

            for outcome, price in zip(
                outcomes,
                prices
            ):
                if str(outcome).lower() in (
                    "up",
                    "yes",
                ):
                    try:
                        return float(price) * 100
                    except Exception:
                        pass

    except Exception as exc:
        logging.debug(
            "Polymarket unavailable for %s %s: %s",
            symbol,
            tf,
            exc
        )

    return None


# ============================================================
# PRISM
# ============================================================

async def prism_prediction(
    session,
    symbol,
    tf
):
    if not PRISM_API_KEY:
        return None

    try:
        headers = {
            "Authorization":
                f"Bearer {PRISM_API_KEY}"
        }

        params = {
            "symbol": symbol.replace(
                "USDT",
                ""
            ),
            "timeframe": tf,
        }

        data = await get_json(
            session,
            PRISM_URL,
            params=params,
            headers=headers,
        )

        if not isinstance(data, dict):
            return None

        # Accept several possible response fields.
        for key in (
            "up_probability",
            "upProbability",
            "probability_up",
            "probabilityUp",
            "confidence",
        ):
            value = data.get(key)

            if value is None:
                continue

            try:
                value = float(value)

                if 0 <= value <= 1:
                    value *= 100

                if 0 <= value <= 100:
                    return value

            except Exception:
                continue

    except Exception as exc:
        logging.debug(
            "PRISM unavailable for %s %s: %s",
            symbol,
            tf,
            exc
        )

    return None


# ============================================================
# DEGEN SIGNAL
# ============================================================

async def degen_prediction(
    session,
    symbol,
    tf
):
    """
    Public source.

    Because the site may change its HTML structure,
    this parser is intentionally defensive.
    """

    base = symbol.replace(
        "USDT",
        ""
    ).upper()

    if base not in (
        "BTC",
        "ETH",
        "SOL",
        "XRP",
    ):
        return None

    if tf not in (
        "15m",
        "1h",
    ):
        return None

    try:
        async with session.get(
            DEGEN_SIGNAL_URL,
            headers={
                "User-Agent":
                    "Mozilla/5.0 "
                    "crypto-rsi-telegram-bot/3.0"
            },
            timeout=aiohttp.ClientTimeout(total=20),
        ) as response:

            if response.status >= 400:
                return None

            html = await response.text()

        # Look for an UP probability/confidence near
        # the requested asset.
        escaped = re.escape(base)

        patterns = [
            rf"{escaped}.{{0,2000}}?UP.{{0,300}}?(\d{{1,3}}(?:\.\d+)?)\s*%",
            rf"{escaped}.{{0,2000}}?(\d{{1,3}}(?:\.\d+)?)\s*%[^<]{{0,100}}UP",
        ]

        for pattern in patterns:
            match = re.search(
                pattern,
                html,
                re.IGNORECASE | re.DOTALL
            )

            if match:
                value = float(
                    match.group(1)
                )

                if 0 <= value <= 100:
                    return value

    except Exception as exc:
        logging.debug(
            "Degen Signal unavailable for %s %s: %s",
            symbol,
            tf,
            exc
        )

    return None


# ============================================================
# LOCAL PREDICTION
# ============================================================

def local_prediction(
    closes,
    rsi_value
):
    """
    Local technical fallback.

    This is NOT presented as an external AI source.
    It is only used when external prediction sources
    do not return usable data.
    """

    if len(closes) < 10:
        return 50.0

    recent = closes[-5:]

    first = recent[0]
    last = recent[-1]

    if first <= 0:
        return 50.0

    momentum = (
        (last - first) / first
    ) * 100

    score = 50.0

    score += momentum * 4.0

    if rsi_value > 70:
        score += 8

    elif rsi_value < 30:
        score -= 8

    return max(
        0.0,
        min(100.0, score)
    )


# ============================================================
# MULTI-SOURCE PREDICTION
# ============================================================

async def get_external_predictions(
    session,
    symbol,
    tf
):
    results = []

    prism = await prism_prediction(
        session,
        symbol,
        tf
    )

    if prism is not None:
        results.append(prism)

    polymarket = await polymarket_prediction(
        session,
        symbol,
        tf
    )

    if polymarket is not None:
        results.append(polymarket)

    degen = await degen_prediction(
        session,
        symbol,
        tf
    )

    if degen is not None:
        results.append(degen)

    return results


async def calculate_prediction(
    session,
    symbol,
    tf,
    closes,
    rsi_value
):
    external = await get_external_predictions(
        session,
        symbol,
        tf
    )

    if external:
        return sum(external) / len(external)

    return local_prediction(
        closes,
        rsi_value
    )


# ============================================================
# USERS / STATE
# ============================================================

DEFAULT_STATE = {
    "users": [],
    "offset": 0,
    "states": {},
    "sent_messages": {},
}


def load_state():
    if not USERS_FILE.exists():
        return dict(DEFAULT_STATE)

    try:
        data = json.loads(
            USERS_FILE.read_text(
                encoding="utf-8"
            )
        )

        state = {
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
            "states": data.get(
                "states",
                {}
            ),
            "sent_messages": data.get(
                "sent_messages",
                {}
            ),
        }

        return state

    except Exception:
        logging.warning(
            "Could not read %s; starting fresh.",
            USERS_FILE
        )

        return dict(DEFAULT_STATE)


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
# TELEGRAM
# ============================================================

async def telegram(
    session,
    method,
    payload=None
):
    if not TOKEN:
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN"
        )

    data = await post_json(
        session,
        f"{TELEGRAM_URL}/bot{TOKEN}/{method}",
        payload or {},
    )

    if not data.get("ok"):
        raise RuntimeError(
            f"Telegram API error: {data}"
        )

    return data.get("result")


async def process_commands(
    session,
    state
):
    offset = state.get(
        "offset",
        0
    )

    try:
        updates = await telegram(
            session,
            "getUpdates",
            {
                "offset": offset,
                "timeout": 0,
                "allowed_updates": [
                    "message"
                ],
            }
        )

    except Exception as exc:
        logging.warning(
            "Telegram getUpdates failed: %s",
            exc
        )

        updates = []

    changed = False

    max_update_id = offset - 1

    for update in updates or []:
        try:
            update_id = int(
                update["update_id"]
            )

            max_update_id = max(
                max_update_id,
                update_id + 1
            )

        except Exception:
            continue

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

        command = (
            text.split()[0]
            if text
            else ""
        )

        if command.startswith("/start"):

            if chat_id not in state["users"]:
                state["users"].append(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text":
                            "✅ ربات فعال شد.\n"
                            "از این به بعد هشدارهای RSI را دریافت می‌کنید."
                    }
                )

            else:
                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text":
                            "ℹ️ شما از قبل فعال هستید."
                    }
                )

        elif command.startswith("/stop"):

            if chat_id in state["users"]:
                state["users"].remove(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text":
                            "⛔ هشدارها متوقف شد.\n"
                            "برای فعال‌سازی دوباره /start را بزنید."
                    }
                )

        elif command.startswith("/status"):

            status = (
                "فعال ✅"
                if chat_id in state["users"]
                else "غیرفعال ⛔"
            )

            await telegram(
                session,
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text":
                        f"📡 وضعیت اشتراک هشدار: {status}"
                }
            )

    if updates:
        state["offset"] = max_update_id
        changed = True

    if (
        LEGACY_CHAT_ID
        and LEGACY_CHAT_ID not in state["users"]
    ):
        state["users"].append(
            LEGACY_CHAT_ID
        )

        changed = True

    if changed:
        save_state(state)

    return state


# ============================================================
# RSI STATE
# ============================================================

def state_key(symbol, tf):
    return f"{symbol}:{tf}"


def should_alert(
    state,
    symbol,
    tf,
    current_zone
):
    key = state_key(
        symbol,
        tf
    )

    previous = state["states"].get(
        key
    )

    if current_zone == "neutral":
        state["states"].pop(
            key,
            None
        )

        return False

    if ALERT_MODE == "always":
        state["states"][key] = current_zone
        return True

    if previous == current_zone:
        return False

    state["states"][key] = current_zone

    return True


# ============================================================
# DUPLICATE MESSAGE PROTECTION
# ============================================================

def message_hash(
    chat_id,
    text
):
    raw = (
        f"{chat_id}|{text}"
    ).encode(
        "utf-8"
    )

    return hashlib.sha256(
        raw
    ).hexdigest()


def cleanup_sent_messages(state):
    sent = state.get(
        "sent_messages",
        {}
    )

    cutoff = (
        datetime.now(
            timezone.utc
        ).timestamp()
        - SENT_MESSAGE_RETENTION_MINUTES * 60
    )

    cleaned = {}

    for key, value in sent.items():
        try:
            timestamp = float(value)

            if timestamp >= cutoff:
                cleaned[key] = timestamp

        except Exception:
            continue

    state["sent_messages"] = cleaned


def already_sent(
    state,
    chat_id,
    text
):
    cleanup_sent_messages(
        state
    )

    key = message_hash(
        chat_id,
        text
    )

    return key in state[
        "sent_messages"
    ]


def mark_sent(
    state,
    chat_id,
    text
):
    cleanup_sent_messages(
        state
    )

    key = message_hash(
        chat_id,
        text
    )

    state[
        "sent_messages"
    ][key] = datetime.now(
        timezone.utc
    ).timestamp()


# ============================================================
# SCAN ONE TIMEFRAME
# ============================================================

async def scan_timeframe(
    session,
    coin,
    tf,
    state
):
    symbol = coin["pair"]

    try:
        rows = await binance(
            session,
            "/api/v3/klines",
            {
                "symbol": symbol,
                "interval": tf,
                "limit": 200,
            }
        )

        if len(rows) < RSI_PERIOD + 5:
            return None

        # IMPORTANT:
        # Keep the current/open candle.
        # We need its current RSI and its remaining time.
        current_candle = rows[-1]

        closes = [
            float(row[4])
            for row in rows
        ]

        values = calculate_rsi(
            closes,
            RSI_PERIOD
        )

        if len(values) < 2:
            return None

        current_rsi = values[-1]

        if current_rsi is None:
            return None

        current_zone = get_zone(
            current_rsi
        )

        if current_zone not in (
            "oversold",
            "overbought",
        ):
            return None

        close_ms = int(
            current_candle[6]
        )

        remaining = remaining_minutes(
            close_ms
        )

        # Keep previous timing condition.
        if not (
            9.0
            <= remaining
            <= 11.0
        ):
            return None

        # Current candle volume
        current_volume = float(
            current_candle[5]
        )

        # Previous 3 candle volumes
        previous_volumes = [
            float(rows[-2][5]),
            float(rows[-3][5]),
            float(rows[-4][5]),
        ]

        vol_state = volume_state(
            current_volume,
            previous_volumes
        )

        # Independent state per symbol/timeframe
        if not should_alert(
            state,
            symbol,
            tf,
            current_zone
        ):
            return None

        prediction = await calculate_prediction(
            session,
            symbol,
            tf,
            closes,
            current_rsi
        )

        return {
            "tf": tf,
            "name": coin["name"],
            "symbol": symbol,
            "base": coin["symbol"],
            "rsi": current_rsi,
            "zone": current_zone,
            "volume": current_volume,
            "prev_volumes": previous_volumes,
            "vol_state": vol_state,
            "close_ms": close_ms,
            "remaining": remaining,
            "prediction": prediction,
        }

    except aiohttp.ClientResponseError as exc:

        if exc.status not in (
            400,
            404,
        ):
            logging.warning(
                "%s %s HTTP %s",
                symbol,
                tf,
                exc.status
            )

        return None

    except Exception as exc:
        logging.warning(
            "%s %s scan failed: %s",
            symbol,
            tf,
            exc
        )

        return None


# ============================================================
# SCAN ALL COINS
# ============================================================

async def scan_all(
    session,
    coins,
    state
):
    semaphore = asyncio.Semaphore(
        12
    )

    alerts = []

    async def worker(
        coin,
        tf
    ):
        async with semaphore:
            return await scan_timeframe(
                session,
                coin,
                tf,
                state
            )

    # Explicit timeframe order
    for tf_name in (
        "15m",
        "1h",
        "4h",
        "1D",
    ):
        tf = TFS[tf_name]

        tasks = [
            worker(
                coin,
                tf
            )
            for coin in coins
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

        for result in results:
            if isinstance(
                result,
                Exception
            ):
                continue

            if result:
                alerts.append(
                    result
                )

    return alerts


# ============================================================
# MESSAGE FORMAT
# ============================================================

def direction_and_icon(
    zone
):
    if zone == "overbought":
        return "🟢 ↑"

    return "🔴 ↓"


def format_signal(
    alert
):
    zone_icon = (
        "🟢"
        if alert["zone"] == "overbought"
        else "🔴"
    )

    direction = (
        "↑"
        if alert["zone"] == "overbought"
        else "↓"
    )

    symbol = alert["base"]

    rsi_text = fmt_number(
        alert["rsi"],
        2
    )

    prediction = max(
        0,
        min(
            100,
            float(
                alert["prediction"]
            )
        )
    )

    prediction_text = fmt_number(
        round(prediction),
        0
    )

    volume_text = fmtv(
        alert["volume"]
    )

    prev1 = fmtv(
        alert["prev_volumes"][0]
    )

    prev2 = fmtv(
        alert["prev_volumes"][1]
    )

    prev3 = fmtv(
        alert["prev_volumes"][2]
    )

    close_text = bold_numbers(
        iran_time_from_ms(
            alert["close_ms"]
        )
    )

    tv = tradingview_url(
        alert["symbol"],
        alert["tf"]
    )

    return (
        f"💠 {symbol}\n\n"

        f"{zone_icon} RSI"
        f"                 {rsi_text}\n"

        f"🔮 {zone_icon} {direction}"
        f"                {prediction_text} %\n"

        f"volume"
        f"                 {bold_numbers(volume_text)} ×\n"

        f"1"
        f"                      {bold_numbers(prev1)} ×\n"

        f"2"
        f"                      {bold_numbers(prev2)} ×\n"

        f"3"
        f"                      {bold_numbers(prev3)} ×\n"

        f"close"
        f"                  {close_text}\n"

        f"📈 TV\n"
        f"{tv}\n"
    )


def build_messages(alerts):
    if not alerts:
        return []

    grouped = {}

    for alert in alerts:
        grouped.setdefault(
            alert["tf"],
            []
        ).append(alert)

    messages = []

    for tf in (
        "15m",
        "1h",
        "4h",
        "1D",
    ):
        items = grouped.get(
            tf,
            []
        )

        if not items:
            continue

        items.sort(
            key=lambda x:
                x["symbol"]
        )

        header = (
            "🚨 RSI Scanner Alert\n\n"
            f"━━━━━━━━ {tf} ━━━━━━━━\n\n"
        )

        body = ""

        for index, alert in enumerate(
            items
        ):
            body += format_signal(
                alert
            )

            if index != len(items) - 1:
                body += (
                    "\n"
                    "--------------------"
                    "\n\n"
                )

        full = header + body

        # Telegram safe limit
        if len(full) <= 3900:
            messages.append(
                full
            )
            continue

        # Split large timeframe messages
        current = header

        for alert in items:
            piece = format_signal(
                alert
            )

            if len(current) + len(piece) > 3900:
                messages.append(
                    current
                )

                current = (
                    "🚨 RSI Scanner Alert\n\n"
                    f"━━━━━━━━ {tf} ━━━━━━━━\n\n"
                )

            current += piece
            current += (
                "\n"
                "--------------------"
                "\n\n"
            )

        if current.strip():
            messages.append(
                current
            )

    return messages


# ============================================================
# SEND TO ALL USERS
# ============================================================

async def send_to_all(
    session,
    state,
    text
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
        return

    sent_any = False

    for chat_id in users:

        if already_sent(
            state,
            chat_id,
            text
        ):
            logging.info(
                "Duplicate message blocked for %s",
                chat_id
            )
            continue

        try:
            await telegram(
                session,
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": text,
                    "disable_web_page_preview":
                        True,
                }
            )

            mark_sent(
                state,
                chat_id,
                text
            )

            sent_any = True

        except Exception as exc:
            logging.warning(
                "Could not send to %s: %s",
                chat_id,
                exc
            )

    if sent_any:
        save_state(
            state
        )


# ============================================================
# MAIN
# ============================================================

async def main():
    if not TOKEN:
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN GitHub Secret."
        )

    state = load_state()

    cleanup_sent_messages(
        state
    )

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=30
        )
    ) as session:

        # Telegram commands
        state = await process_commands(
            session,
            state
        )

        # Get DeFiLlama + Binance universe
        coins = await top_coins(
            session
        )

        logging.info(
            "Final coins: %s",
            len(coins)
        )

        if not coins:
            raise RuntimeError(
                "No DeFiLlama + Binance coins found."
            )

        # Continuous scan during workflow run
        start_time = datetime.now(
            timezone.utc
        )

        while (
            datetime.now(
                timezone.utc
            ) - start_time
        ).total_seconds() < RUN_SECONDS:

            cycle_start = datetime.now(
                timezone.utc
            )

            try:
                alerts = await scan_all(
                    session,
                    coins,
                    state
                )

                logging.info(
                    "Alerts: %s | Subscribers: %s",
                    len(alerts),
                    len(
                        state.get(
                            "users",
                            []
                        )
                    )
                )

                # Save state after scanning
                save_state(
                    state
                )

                if alerts:
                    messages = build_messages(
                        alerts
                    )

                    for message in messages:
                        await send_to_all(
                            session,
                            state,
                            message
                        )

                else:
                    logging.info(
                        "No new RSI alerts."
                    )

                save_state(
                    state
                )

            except Exception as exc:
                logging.exception(
                    "Scan cycle failed: %s",
                    exc
                )

            elapsed = (
                datetime.now(
                    timezone.utc
                ) - cycle_start
            ).total_seconds()

            wait = max(
                1,
                SCAN_INTERVAL - elapsed
            )

            await asyncio.sleep(
                wait
            )


# ============================================================
# ENTRY POINT
# ============================================================

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
