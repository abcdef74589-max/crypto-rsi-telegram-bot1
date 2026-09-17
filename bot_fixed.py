import os
import re
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
PERIOD = int(os.getenv("RSI_PERIOD", "14"))

USERS_FILE = Path(os.getenv("USERS_FILE", "users.json"))

MARKET = os.getenv("MARKET", "spot").lower()

# Alert when the CURRENT candle has 9-11 minutes remaining.
ALERT_MINUTES_BEFORE_CLOSE = 10
ALERT_WINDOW_SECONDS = 120

# Binance public endpoints.
BASES = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api.binance.com",
]

# DeFiLlama token ranking page.
DEFILLAMA_TOKENS_URL = "https://defillama.com/tokens"

# Timeframes.
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

# Iran time = UTC+3:30.
IRAN_TZ = timezone(timedelta(hours=3, minutes=30))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ============================================================
# RSI
# ============================================================

def rsi(values, period=14):
    """
    Wilder RSI.
    Returns one RSI value for every input position after
    enough candles are available.
    """
    if len(values) < period + 1:
        return []

    gains = []
    losses = []

    for i in range(1, period + 1):
        change = values[i] - values[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(-change)

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    result = [None] * period

    if avg_loss == 0:
        first_rsi = 100.0 if avg_gain > 0 else 50.0
    else:
        rs = avg_gain / avg_loss
        first_rsi = 100.0 - (100.0 / (1.0 + rs))

    result.append(first_rsi)

    for i in range(period + 1, len(values)):
        change = values[i] - values[i - 1]

        gain = max(change, 0.0)
        loss = max(-change, 0.0)

        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

        if avg_loss == 0:
            value = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            value = 100.0 - (100.0 / (1.0 + rs))

        result.append(value)

    return result


def zone(value):
    if value < 30:
        return "oversold"

    if value > 70:
        return "overbought"

    return "neutral"


# ============================================================
# HTTP
# ============================================================

async def get_json(session, url, params=None, timeout=20):
    last_error = None

    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 "
                        "(compatible; CryptoRSITelegramBot/3.0)"
                    )
                },
            ) as response:

                if response.status == 429:
                    retry_after = response.headers.get(
                        "Retry-After",
                        "3",
                    )

                    try:
                        delay = min(float(retry_after), 15)
                    except Exception:
                        delay = 3

                    await asyncio.sleep(delay)
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

                return await response.json(content_type=None)

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            json.JSONDecodeError,
        ) as exc:

            last_error = exc

            if attempt == 2:
                raise

            await asyncio.sleep(1.5 * (attempt + 1))

    raise last_error


async def get_text(session, url, params=None, timeout=30):
    last_error = None

    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout),
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 "
                        "(compatible; CryptoRSITelegramBot/3.0)"
                    ),
                    "Accept": (
                        "text/html,application/xhtml+xml,"
                        "application/xml;q=0.9,*/*;q=0.8"
                    ),
                },
            ) as response:

                if response.status == 429:
                    await asyncio.sleep(3)
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

                return await response.text()

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ) as exc:

            last_error = exc

            if attempt == 2:
                raise

            await asyncio.sleep(1.5 * (attempt + 1))

    raise last_error


async def post_json(session, url, payload):
    async with session.post(
        url,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=20),
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

        if not data.get("ok"):
            raise RuntimeError(
                f"Telegram API error: {text[:500]}"
            )

        return data.get("result")


async def telegram(session, method, payload=None):
    if not TOKEN:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing."
        )

    return await post_json(
        session,
        f"https://api.telegram.org/bot{TOKEN}/{method}",
        payload or {},
    )


# ============================================================
# BINANCE
# ============================================================

async def binance(session, path, params=None):
    last_error = None

    for base in BASES:
        try:
            return await get_json(
                session,
                base + path,
                params,
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
                "%s returned HTTP %s; trying next Binance endpoint.",
                base,
                exc.status,
            )

    raise last_error


async def get_binance_exchange_info(session):
    return await binance(
        session,
        "/api/v3/exchangeInfo",
    )


async def get_binance_spot_usdt_symbols(session):
    """
    Returns Binance spot USDT symbols.
    """

    data = await get_binance_exchange_info(session)

    result = set()

    for item in data.get("symbols", []):
        symbol = item.get("symbol", "")
        status = item.get("status", "")
        quote = item.get("quoteAsset", "")
        market_type = item.get("isSpotTradingAllowed", False)

        if (
            status == "TRADING"
            and quote == "USDT"
            and market_type
            and symbol.endswith("USDT")
        ):
            result.add(symbol)

    return result


async def tickers(session):
    data = await binance(
        session,
        "/api/v3/ticker/24hr",
    )

    return {
        x["symbol"]: x
        for x in data
        if x.get("symbol")
    }


# ============================================================
# DEFILLAMA RANKING
# ============================================================

def clean_html_text(value):
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_symbol(symbol):
    if not symbol:
        return ""

    symbol = str(symbol).strip().upper()

    symbol = symbol.replace(
        "$",
        "",
    )

    symbol = re.sub(
        r"[^A-Z0-9._-]",
        "",
        symbol,
    )

    return symbol


def normalize_name(name, symbol):
    if name:
        name = str(name).strip()

        if name:
            return name

    return symbol


def token_object_is_valid(obj):
    if not isinstance(obj, dict):
        return False

    symbol = (
        obj.get("symbol")
        or obj.get("ticker")
        or obj.get("tokenSymbol")
    )

    if not symbol:
        return False

    symbol = normalize_symbol(symbol)

    if not symbol:
        return False

    # Require at least one ranking-related metric.
    metric_keys = (
        "mcap",
        "marketCap",
        "market_cap",
        "fdv",
        "rank",
        "price",
        "circulatingMarketCap",
    )

    return any(
        key in obj
        for key in metric_keys
    )


def extract_token_from_object(obj, fallback_rank=None):
    if not token_object_is_valid(obj):
        return None

    symbol = (
        obj.get("symbol")
        or obj.get("ticker")
        or obj.get("tokenSymbol")
    )

    symbol = normalize_symbol(symbol)

    name = (
        obj.get("name")
        or obj.get("tokenName")
        or obj.get("displayName")
        or symbol
    )

    rank = (
        obj.get("rank")
        or obj.get("position")
        or obj.get("index")
        or fallback_rank
    )

    try:
        rank = int(rank) if rank is not None else None
    except Exception:
        rank = fallback_rank

    mcap = (
        obj.get("mcap")
        or obj.get("marketCap")
        or obj.get("market_cap")
        or 0
    )

    try:
        mcap = float(mcap or 0)
    except Exception:
        mcap = 0.0

    return {
        "symbol": symbol,
        "name": normalize_name(name, symbol),
        "rank": rank,
        "mcap": mcap,
    }


def walk_json_for_tokens(value, output):
    """
    Recursively searches JSON structures for token objects.
    """

    if isinstance(value, dict):

        token = extract_token_from_object(value)

        if token:
            output.append(token)

        for child in value.values():
            walk_json_for_tokens(
                child,
                output,
            )

    elif isinstance(value, list):

        for child in value:
            walk_json_for_tokens(
                child,
                output,
            )


def extract_json_scripts(html):
    """
    Extract JSON-like <script> blocks.
    """

    scripts = []

    patterns = [
        r'<script[^>]+type=["\']application/json["\'][^>]*>(.*?)</script>',
        r'<script[^>]*>(.*?)</script>',
    ]

    for pattern in patterns:

        for match in re.finditer(
            pattern,
            html,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            content = match.group(1).strip()

            if content:
                scripts.append(content)

    return scripts


def parse_defillama_html(html):
    """
    Multi-method parser.

    DeFiLlama's /tokens page is a web page rather than a documented
    free ranking API, so the parser checks:
      1. JSON script data
      2. embedded application state
      3. raw HTML patterns
    """

    found = []

    # --------------------------------------------------------
    # Method 1: JSON scripts
    # --------------------------------------------------------

    for script in extract_json_scripts(html):

        candidates = [
            script,
        ]

        # Some pages wrap JSON in a JS assignment.
        for prefix in (
            "self.__next_f.push(",
            "window.__INITIAL_STATE__ =",
            "window.__NEXT_DATA__ =",
        ):
            if script.startswith(prefix):
                candidates.append(
                    script[len(prefix):].rstrip(");")
                )

        for candidate in candidates:

            candidate = candidate.strip()

            try:
                parsed = json.loads(candidate)

                walk_json_for_tokens(
                    parsed,
                    found,
                )

            except Exception:
                pass

    # --------------------------------------------------------
    # Method 2: regex around common token JSON fields
    # --------------------------------------------------------

    patterns = [
        re.compile(
            r'"symbol"\s*:\s*"([^"]{1,20})".{0,1000}?'
            r'"(?:mcap|marketCap|market_cap)"\s*:\s*'
            r'([0-9.eE+-]+)',
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r'"(?:ticker|tokenSymbol)"\s*:\s*"([^"]{1,20})".{0,1000}?'
            r'"(?:mcap|marketCap|market_cap)"\s*:\s*'
            r'([0-9.eE+-]+)',
            re.IGNORECASE | re.DOTALL,
        ),
    ]

    for pattern in patterns:

        for match in pattern.finditer(html):

            symbol = normalize_symbol(
                match.group(1)
            )

            try:
                mcap = float(
                    match.group(2)
                )
            except Exception:
                mcap = 0

            if symbol:
                found.append(
                    {
                        "symbol": symbol,
                        "name": symbol,
                        "rank": None,
                        "mcap": mcap,
                    }
                )

    # --------------------------------------------------------
    # Deduplicate
    # --------------------------------------------------------

    unique = {}

    for item in found:

        symbol = item["symbol"]

        if not symbol:
            continue

        old = unique.get(symbol)

        if old is None:
            unique[symbol] = item
            continue

        # Prefer item containing a rank.
        if (
            old.get("rank") is None
            and item.get("rank") is not None
        ):
            unique[symbol] = item

        elif (
            item.get("mcap", 0)
            > old.get("mcap", 0)
        ):
            unique[symbol] = item

    result = list(unique.values())

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    ranked = [
        x for x in result
        if x.get("rank") is not None
    ]

    if len(ranked) >= 10:

        ranked.sort(
            key=lambda x: (
                x.get("rank", 999999),
                -x.get("mcap", 0),
            )
        )

        return ranked

    result.sort(
        key=lambda x: (
            -x.get("mcap", 0),
            x["symbol"],
        )
    )

    return result


async def top_coins(session):
    """
    Get top tokens from DeFiLlama and then keep only tokens
    that have Binance USDT spot markets.

    Final result: up to TOP_N coins.
    """

    logging.info(
        "Downloading DeFiLlama token ranking..."
    )

    html = await get_text(
        session,
        DEFILLAMA_TOKENS_URL,
        timeout=40,
    )

    if not html:
        raise RuntimeError(
            "DeFiLlama returned an empty page."
        )

    tokens = parse_defillama_html(html)

    logging.info(
        "DeFiLlama parser found %s token candidates.",
        len(tokens),
    )

    if not tokens:
        raise RuntimeError(
            "Could not parse DeFiLlama token ranking."
        )

    # Binance markets.
    binance_symbols = (
        await get_binance_spot_usdt_symbols(
            session
        )
    )

    logging.info(
        "Binance USDT spot symbols: %s",
        len(binance_symbols),
    )

    # Some common symbols require mapping.
    symbol_aliases = {
        "WETH": "ETH",
        "WBTC": "BTC",
        "WBNB": "BNB",
        "WSTETH": "ETH",
        "STETH": "ETH",
        "WEETH": "ETH",
        "WSOL": "SOL",
    }

    result = []
    seen_pairs = set()

    for token in tokens:

        original_symbol = token["symbol"]

        symbol = symbol_aliases.get(
            original_symbol,
            original_symbol,
        )

        pair = symbol + "USDT"

        if pair not in binance_symbols:
            continue

        if pair in seen_pairs:
            continue

        seen_pairs.add(pair)

        result.append(
            {
                "name": token.get(
                    "name",
                    symbol,
                ),
                "symbol": symbol,
                "rank": token.get(
                    "rank"
                ),
            }
        )

        if len(result) >= TOP_N:
            break

    if not result:
        raise RuntimeError(
            "DeFiLlama ranking was obtained, "
            "but no Binance USDT spot pairs matched."
        )

    logging.info(
        "Final DeFiLlama ∩ Binance list: %s coins.",
        len(result),
    )

    return result


# ============================================================
# CANDLE / RSI
# ============================================================

def candle_remaining_seconds(close_ms):
    now_ms = (
        datetime.now(timezone.utc).timestamp()
        * 1000
    )

    return (
        float(close_ms) - now_ms
    ) / 1000.0


async def one_tf(session, symbol, tf):
    """
    Analyze the CURRENT OPEN candle.

    Alert only when approximately 10 minutes remain.
    """

    rows = await binance(
        session,
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": tf,
            "limit": 200,
        },
    )

    if len(rows) < PERIOD + 5:
        return None

    # Binance's last kline is the current open candle.
    current = rows[-1]

    close_ms = int(current[6])

    remaining = candle_remaining_seconds(
        close_ms
    )

    # Alert window: 9 to 11 minutes.
    target = ALERT_MINUTES_BEFORE_CLOSE * 60

    if abs(remaining - target) > (
        ALERT_WINDOW_SECONDS / 2
    ):
        return None

    closes = [
        float(row[4])
        for row in rows
    ]

    values = rsi(
        closes,
        PERIOD,
    )

    if len(values) < 2:
        return None

    current_rsi = values[-1]

    if current_rsi is None:
        return None

    current_zone = zone(
        current_rsi
    )

    if current_zone == "neutral":
        return None

    # Previous candle RSI.
    previous_rsi = (
        values[-2]
        if values[-2] is not None
        else 50.0
    )

    # --------------------------------------------------------
    # Volume
    # --------------------------------------------------------

    current_volume = float(
        current[5]
    )

    previous_volumes = [
        float(rows[-2][5]),
        float(rows[-3][5]),
        float(rows[-4][5]),
    ]

    avg_previous = (
        sum(previous_volumes)
        / len(previous_volumes)
    )

    if avg_previous <= 0:
        volume_ratio = 1.0
    else:
        volume_ratio = (
            current_volume
            / avg_previous
        )

    if current_volume > max(
        previous_volumes
    ):
        volume_state = "زیاد 🔥"

    elif current_volume < min(
        previous_volumes
    ):
        volume_state = "کم 📉"

    else:
        volume_state = "معمولی ➖"

    return {
        "tf": tf,
        "rsi": float(current_rsi),
        "prev_rsi": float(previous_rsi),
        "zone": current_zone,

        "volume": current_volume,
        "prev_volumes": previous_volumes,
        "volume_ratio": volume_ratio,
        "volume_state": volume_state,

        "close_ms": close_ms,
        "remaining": remaining,

        "open_time_ms": int(
            current[0]
        ),
    }


async def scan_coin(
    session,
    coin,
    ticker_data,
    semaphore,
):
    symbol = coin["symbol"] + "USDT"

    if symbol not in ticker_data:
        return []

    async def run_tf(tf_name):

        async with semaphore:

            try:
                result = await one_tf(
                    session,
                    symbol,
                    tf_name,
                )

                if result:
                    result.update(
                        {
                            "name": coin["name"],
                            "symbol": symbol,
                        }
                    )

                return result

            except aiohttp.ClientResponseError as exc:

                if exc.status not in (
                    400,
                    404,
                ):
                    logging.warning(
                        "%s %s HTTP %s",
                        symbol,
                        tf_name,
                        exc.status,
                    )

                return None

            except Exception as exc:

                logging.warning(
                    "%s %s: %s",
                    symbol,
                    tf_name,
                    exc,
                )

                return None

    results = await asyncio.gather(
        *(
            run_tf(tf)
            for tf in TFS.keys()
        )
    )

    return [
        x for x in results
        if x is not None
    ]


# ============================================================
# PERSISTENT STATE
# ============================================================

def default_state():
    return {
        "users": [],
        "offset": 0,
        "signals": {},
    }


def load_state():
    if not USERS_FILE.exists():
        return default_state()

    try:
        data = json.loads(
            USERS_FILE.read_text(
                encoding="utf-8"
            )
        )

        if not isinstance(data, dict):
            return default_state()

        users = [
            str(x)
            for x in data.get(
                "users",
                [],
            )
        ]

        signals = data.get(
            "signals",
            {},
        )

        if not isinstance(
            signals,
            dict,
        ):
            signals = {}

        return {
            "users": users,
            "offset": int(
                data.get(
                    "offset",
                    0,
                )
            ),
            "signals": signals,
        }

    except Exception as exc:

        logging.warning(
            "Could not read %s: %s",
            USERS_FILE,
            exc,
        )

        return default_state()


def save_state(state):
    USERS_FILE.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )


# ============================================================
# TELEGRAM COMMANDS
# ============================================================

async def process_commands(
    session,
    state,
):
    offset = int(
        state.get(
            "offset",
            0,
        )
    )

    updates = await telegram(
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

    changed = False

    max_update_id = offset - 1

    for update in updates or []:

        try:
            update_id = int(
                update["update_id"]
            )

            max_update_id = max(
                max_update_id,
                update_id + 1,
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

        # ----------------------------------------------------
        # /start
        # ----------------------------------------------------

        if command.startswith(
            "/start"
        ):

            if chat_id not in state[
                "users"
            ]:

                state["users"].append(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text": (
                            "✅ ربات فعال شد.\n"
                            "از این به بعد هشدارهای RSI "
                            "را دریافت می‌کنید."
                        ),
                    },
                )

            else:

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text": (
                            "ℹ️ شما از قبل فعال هستید."
                        ),
                    },
                )

        # ----------------------------------------------------
        # /stop
        # ----------------------------------------------------

        elif command.startswith(
            "/stop"
        ):

            if chat_id in state[
                "users"
            ]:

                state["users"].remove(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id": chat_id,
                        "text": (
                            "⛔ هشدارها متوقف شد.\n"
                            "برای فعال‌سازی دوباره "
                            "/start را بزنید."
                        ),
                    },
                )

        # ----------------------------------------------------
        # /status
        # ----------------------------------------------------

        elif command.startswith(
            "/status"
        ):

            active = (
                chat_id
                in state["users"]
            )

            status = (
                "فعال ✅"
                if active
                else "غیرفعال ⛔"
            )

            await telegram(
                session,
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": (
                        f"📡 وضعیت اشتراک هشدار: "
                        f"{status}"
                    ),
                },
            )

    if updates:
        state["offset"] = max_update_id
        changed = True

    # Backward compatibility.
    if (
        LEGACY_CHAT_ID
        and LEGACY_CHAT_ID
        not in state["users"]
    ):

        state["users"].append(
            LEGACY_CHAT_ID
        )

        changed = True

        logging.info(
            "Added TELEGRAM_CHAT_ID "
            "to subscribers."
        )

    if changed:
        save_state(state)

    return state


# ============================================================
# SIGNAL DEDUPE
# ============================================================

def signal_key(alert):
    return (
        f"{alert['symbol']}|"
        f"{alert['tf']}"
    )


def is_new_signal(
    state,
    alert,
):
    """
    Same RSI zone on the same timeframe is not repeatedly
    sent during the same candle/run cycle.

    When RSI returns to neutral, the signal becomes available
    again for the next overbought/oversold entry.
    """

    key = signal_key(alert)

    signals = state.setdefault(
        "signals",
        {},
    )

    current_zone = alert[
        "zone"
    ]

    previous = signals.get(
        key
    )

    # No previous state.
    if previous is None:

        signals[key] = {
            "zone": current_zone,
            "open_time_ms": alert[
                "open_time_ms"
            ],
        }

        return True

    previous_zone = previous.get(
        "zone",
        "neutral",
    )

    previous_open = int(
        previous.get(
            "open_time_ms",
            0,
        )
    )

    current_open = int(
        alert["open_time_ms"]
    )

    # New candle:
    # We still alert only when the zone has newly appeared.
    if current_open != previous_open:

        # If previous candle was neutral, this is a new entry.
        if previous_zone == "neutral":

            signals[key] = {
                "zone": current_zone,
                "open_time_ms": current_open,
            }

            return True

        # If zone changed from overbought to oversold
        # or vice versa, alert again.
        if previous_zone != current_zone:

            signals[key] = {
                "zone": current_zone,
                "open_time_ms": current_open,
            }

            return True

        # Same zone in next candle:
        # don't send duplicate.
        signals[key] = {
            "zone": current_zone,
            "open_time_ms": current_open,
        }

        return False

    # Same candle.
    if previous_zone == current_zone:
        return False

    # Zone changed inside current candle.
    signals[key] = {
        "zone": current_zone,
        "open_time_ms": current_open,
    }

    return True


def reset_neutral_states(
    state,
    observed_alerts,
):
    """
    If a symbol/timeframe is not currently in an extreme zone,
    reset its state to neutral.

    This allows a later RSI re-entry to generate a new alert.
    """

    observed = {
        (
            a["symbol"],
            a["tf"],
        )
        for a in observed_alerts
    }

    signals = state.setdefault(
        "signals",
        {},
    )

    for key, item in list(
        signals.items()
    ):

        try:
            symbol, tf = key.split(
                "|",
                1,
            )
        except ValueError:
            continue

        if (
            symbol,
            tf,
        ) not in observed:

            item["zone"] = "neutral"


# ============================================================
# NUMBER FORMATTING
# ============================================================

DIGIT_MAP = str.maketrans(
    "0123456789",
    "𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵",
)


def bold_digits(value):
    return str(value).translate(
        DIGIT_MAP
    )


def fmt_rsi(value):
    return bold_digits(
        f"{value:.2f}"
    )


def fmt_percent(value):
    return bold_digits(
        f"{value:.0f}"
    )


def fmt_price(value):
    if value >= 1000:
        text = f"{value:.2f}"
    elif value >= 1:
        text = f"{value:.4f}"
    elif value >= 0.01:
        text = f"{value:.6f}"
    else:
        text = f"{value:.8f}"

    return bold_digits(
        text
    )


def fmt_volume(value):
    if value >= 1_000_000_000:
        text = f"{value / 1e9:.2f}B"

    elif value >= 1_000_000:
        text = f"{value / 1e6:.2f}M"

    elif value >= 1_000:
        text = f"{value / 1e3:.2f}K"

    else:
        text = f"{value:.2f}"

    return bold_digits(
        text
    )


def iran_close_time(close_ms):
    dt = datetime.fromtimestamp(
        close_ms / 1000,
        tz=timezone.utc,
    ).astimezone(
        IRAN_TZ
    )

    return bold_digits(
        dt.strftime("%H:%M")
    )


# ============================================================
# ALERT MESSAGE
# ============================================================

def tradingview_url(
    symbol,
    tf,
):
    intervals = {
        "15m": "15",
        "1h": "60",
        "4h": "240",
        "1D": "D",
    }

    interval = intervals.get(
        tf,
        "15",
    )

    encoded = symbol.replace(
        ":",
        "%3A",
    )

    return (
        "https://www.tradingview.com/"
        f"chart/?symbol=BINANCE%3A{encoded}"
        f"&interval={interval}"
    )


def build_alert(
    alert,
    is_new,
):
    symbol = alert["symbol"]
    base = symbol[:-4] if symbol.endswith(
        "USDT"
    ) else symbol

    if alert["rsi"] > 70:
        direction = "↑"
        emoji = "🟢"

    else:
        direction = "↓"
        emoji = "🔴"

    # Green = newly appearing signal.
    # White = same signal already seen.
    marker = (
        "🟢"
        if is_new
        else "⚪"
    )

    rsi_value = fmt_rsi(
        alert["rsi"]
    )

    rsi_percent = fmt_percent(
        alert["rsi"]
    )

    volume_ratio = bold_digits(
        f"{alert['volume_ratio']:.2f}"
    )

    current_volume = fmt_volume(
        alert["volume"]
    )

    previous = [
        fmt_volume(x)
        for x in alert[
            "prev_volumes"
        ]
    ]

    close_time = iran_close_time(
        alert["close_ms"]
    )

    url = tradingview_url(
        symbol,
        alert["tf"],
    )

    text = (
        f"{marker} {base}\n\n"

        f"{emoji} RSI                 "
        f"{rsi_value}\n"

        f"🔮 {emoji} {direction}                "
        f"{rsi_percent} %\n"

        f"volume                 "
        f"{volume_ratio} ×\n"

        f"close                  "
        f"{close_time}\n"

        f"volume state            "
        f"{alert['volume_state']}\n"

        f"1                      "
        f"{previous[0]}\n"

        f"2                      "
        f"{previous[1]}\n"

        f"3                      "
        f"{previous[2]}\n"

        f"📈 TV\n"
        f"{url}"
    )

    return text


def split_message(
    text,
    max_length=3900,
):
    """
    Safe Telegram splitter.
    """

    if len(text) <= max_length:
        return [text]

    parts = []
    remaining = text

    while len(remaining) > max_length:

        cut = remaining.rfind(
            "\n\n",
            0,
            max_length,
        )

        if cut <= 0:
            cut = remaining.rfind(
                "\n",
                0,
                max_length,
            )

        if cut <= 0:
            cut = max_length

        parts.append(
            remaining[:cut].rstrip()
        )

        remaining = (
            remaining[cut:]
            .lstrip()
        )

    if remaining:
        parts.append(
            remaining
        )

    return parts


def build_messages(
    alerts_with_status,
):
    """
    Requested order:
    15m
    1h
    4h
    1D
    """

    if not alerts_with_status:
        return []

    grouped = {}

    for alert, is_new in (
        alerts_with_status
    ):
        grouped.setdefault(
            alert["tf"],
            [],
        ).append(
            (
                alert,
                is_new,
            )
        )

    messages = []

    for tf in sorted(
        grouped,
        key=lambda x: TF_ORDER.get(
            x,
            99,
        ),
    ):

        items = sorted(
            grouped[tf],
            key=lambda pair: pair[0][
                "symbol"
            ],
        )

        header = (
            "━━━━━━━━ "
            f"{tf}"
            " ━━━━━━━━"
        )

        current = (
            "🚨 RSI Scanner Alert\n\n"
            f"{header}\n\n"
        )

        blocks = []

        for alert, is_new in items:

            block = build_alert(
                alert,
                is_new,
            )

            blocks.append(
                block
            )

        for block in blocks:

            candidate = (
                current
                + block
                + "\n\n"
                + "--------------------"
                + "\n\n"
            )

            if (
                len(candidate)
                > 3900
            ):

                messages.append(
                    current.rstrip()
                )

                current = (
                    "🚨 RSI Scanner Alert\n\n"
                    f"{header}\n\n"
                    + block
                    + "\n\n"
                    + "--------------------"
                    + "\n\n"
                )

            else:
                current = candidate

        if current.strip():
            messages.append(
                current.rstrip()
            )

    return messages


# ============================================================
# SEND
# ============================================================

async def send_to_all(
    session,
    state,
    text,
):
    users = list(
        dict.fromkeys(
            state.get(
                "users",
                [],
            )
        )
    )

    failed = []

    for chat_id in users:

        try:

            await telegram(
                session,
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                },
            )

        except Exception as exc:

            logging.warning(
                "Could not send to %s: %s",
                chat_id,
                exc,
            )

            failed.append(
                chat_id
            )

    return failed


# ============================================================
# MANUAL WORKFLOW
# ============================================================

async def wait_for_manual_run():
    """
    Scheduled GitHub Actions:
        scan immediately.

    workflow_dispatch:
        also scan immediately.

    No 15-minute waiting is used.
    """

    return


# ============================================================
# MAIN
# ============================================================

async def main():

    if not TOKEN:
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN GitHub Secret."
        )

    await wait_for_manual_run()

    state = load_state()

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=40
        )
    ) as session:

        # ----------------------------------------------------
        # Telegram commands
        # ----------------------------------------------------

        state = await process_commands(
            session,
            state,
        )

        # ----------------------------------------------------
        # DeFiLlama ∩ Binance
        # ----------------------------------------------------

        coins = await top_coins(
            session
        )

        logging.info(
            "Final coins: %s",
            len(coins),
        )

        # ----------------------------------------------------
        # Binance ticker data
        # ----------------------------------------------------

        ticker_data = await tickers(
            session
        )

        logging.info(
            "Binance tickers: %s",
            len(ticker_data),
        )

        # ----------------------------------------------------
        # Scan
        # ----------------------------------------------------

        semaphore = asyncio.Semaphore(
            12
        )

        groups = await asyncio.gather(
            *(
                scan_coin(
                    session,
                    coin,
                    ticker_data,
                    semaphore,
                )
                for coin in coins
            )
        )

        alerts = [
            alert
            for group in groups
            for alert in group
        ]

        logging.info(
            "Extreme candles found: %s",
            len(alerts),
        )

        # ----------------------------------------------------
        # Dedupe
        # ----------------------------------------------------

        alerts_with_status = []

        for alert in alerts:

            new_signal = is_new_signal(
                state,
                alert,
            )

            # Only newly appearing signals
            # are sent.
            if new_signal:
                alerts_with_status.append(
                    (
                        alert,
                        True,
                    )
                )

        # Save state after signal processing.
        save_state(state)

        logging.info(
            "New alerts: %s | Subscribers: %s",
            len(
                alerts_with_status
            ),
            len(
                state.get(
                    "users",
                    [],
                )
            ),
        )

        # ----------------------------------------------------
        # No subscribers
        # ----------------------------------------------------

        if not state.get(
            "users"
        ):

            logging.info(
                "No subscribers yet. "
                "Send /start to the bot."
            )

        # ----------------------------------------------------
        # Send
        # ----------------------------------------------------

        messages = build_messages(
            alerts_with_status
        )

        for message in messages:

            await send_to_all(
                session,
                state,
                message,
            )

        # ----------------------------------------------------
        # Final save
        # ----------------------------------------------------

        save_state(state)

        if not alerts_with_status:

            logging.info(
                "No new RSI alerts."
            )


# ============================================================
# ENTRY
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
