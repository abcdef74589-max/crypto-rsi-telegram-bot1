import os
import asyncio
import logging
import json
import re
import math
from pathlib import Path
from datetime import datetime, timezone

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

# حدود 10 دقیقه مانده به بسته شدن کندل
ALERT_MINUTES_BEFORE_CLOSE = 10

# بازه قابل قبول برای تشخیص
WINDOW_MINUTES = 1.0

# مدت اجرای هر GitHub Actions run
RUN_SECONDS = 285

# فاصله اسکن داخل همان run
SCAN_INTERVAL = 60

# حداکثر تعداد درخواست همزمان Binance
BINANCE_CONCURRENCY = 12

# ------------------------------------------------------------
# Binance
# ------------------------------------------------------------

BINANCE_BASES = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api.binance.com",
]

# ------------------------------------------------------------
# DeFiLlama
# ------------------------------------------------------------

DEFILLAMA_TOKENS_URL = "https://defillama.com/tokens"

# ------------------------------------------------------------
# Timeframes
# ------------------------------------------------------------

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
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ============================================================
# NUMBER FORMATTING
# ============================================================

PERSIAN_BOLD_DIGITS = str.maketrans(
    "0123456789",
    "𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵",
)


def bold_digits(value):
    return str(value).translate(PERSIAN_BOLD_DIGITS)


def fmt_number(value, decimals=2):
    try:
        return bold_digits(f"{float(value):.{decimals}f}")
    except Exception:
        return bold_digits(str(value))


def fmt_volume(value):
    try:
        value = float(value)
    except Exception:
        return "0"

    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"

    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"

    if value >= 1_000:
        return f"{value / 1_000:.2f}K"

    return f"{value:.2f}"


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

        if change > 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(change))

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

        avg_gain = (
            (avg_gain * (period - 1)) + gain
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) + loss
        ) / period

        if avg_loss == 0:
            current_rsi = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            current_rsi = 100.0 - (
                100.0 / (1.0 + rs)
            )

        result.append(current_rsi)

    return result


def get_zone(rsi_value):
    if rsi_value is None:
        return "neutral"

    if rsi_value < 30:
        return "oversold"

    if rsi_value > 70:
        return "overbought"

    return "neutral"


# ============================================================
# HTTP
# ============================================================

async def get_json(session, url, params=None, headers=None):
    last_error = None

    for attempt in range(3):
        try:
            request_headers = {
                "User-Agent": "crypto-rsi-telegram-bot/4.0",
                "Accept": "application/json,text/plain,*/*",
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
                        "3",
                    )

                    try:
                        delay = float(retry_after)
                    except Exception:
                        delay = 3

                    await asyncio.sleep(
                        min(delay, 15)
                    )

                    continue

                if response.status >= 400:
                    text = await response.text(
                        errors="ignore"
                    )

                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status,
                        message=text[:500],
                        headers=response.headers,
                    )

                return await response.json(
                    content_type=None
                )

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ) as exc:

            last_error = exc

            if attempt >= 2:
                raise

            await asyncio.sleep(
                1.5 * (attempt + 1)
            )

    raise last_error


async def get_text(session, url, params=None, headers=None):
    last_error = None

    for attempt in range(3):
        try:
            request_headers = {
                "User-Agent": (
                    "Mozilla/5.0 "
                    "(X11; Linux x86_64) "
                    "AppleWebKit/537.36 "
                    "(KHTML, like Gecko) "
                    "Chrome/128.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
            }

            if headers:
                request_headers.update(headers)

            async with session.get(
                url,
                params=params,
                headers=request_headers,
                timeout=aiohttp.ClientTimeout(total=35),
                allow_redirects=True,
            ) as response:

                if response.status == 429:
                    await asyncio.sleep(
                        min(
                            float(
                                response.headers.get(
                                    "Retry-After",
                                    "3",
                                )
                            ),
                            15,
                        )
                    )
                    continue

                if response.status >= 400:
                    text = await response.text(
                        errors="ignore"
                    )

                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status,
                        message=text[:500],
                        headers=response.headers,
                    )

                return await response.text(
                    errors="ignore"
                )

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ) as exc:

            last_error = exc

            if attempt >= 2:
                raise

            await asyncio.sleep(
                1.5 * (attempt + 1)
            )

    raise last_error


async def post_json(session, url, payload):
    async with session.post(
        url,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=25),
    ) as response:

        text = await response.text(
            errors="ignore"
        )

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
                f"API error: {text[:500]}"
            )

        return data.get("result")


# ============================================================
# TELEGRAM
# ============================================================

async def telegram(session, method, payload=None):
    if not TOKEN:
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN."
        )

    url = (
        f"https://api.telegram.org/"
        f"bot{TOKEN}/{method}"
    )

    return await post_json(
        session,
        url,
        payload or {},
    )


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
                "%s returned HTTP %s; "
                "trying next Binance endpoint",
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

    raise RuntimeError(
        "No Binance endpoint available."
    )


async def get_binance_exchange_info(session):
    return await binance(
        session,
        "/api/v3/exchangeInfo",
    )


def build_binance_usdt_spot_symbols(exchange_info):
    result = {}

    for item in exchange_info.get(
        "symbols",
        [],
    ):
        try:
            symbol = item.get("symbol", "")
            status = item.get("status", "")
            quote_asset = item.get(
                "quoteAsset",
                "",
            )

            is_spot = item.get(
                "isSpotTradingAllowed",
                False,
            )

            if (
                status == "TRADING"
                and quote_asset == "USDT"
                and is_spot
            ):
                base_asset = item.get(
                    "baseAsset",
                    "",
                ).upper()

                if base_asset:
                    result[
                        base_asset
                    ] = symbol

        except Exception:
            continue

    return result


async def get_binance_tickers(session):
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
# DEFILLAMA
# ============================================================

def clean_html_text(value):
    value = re.sub(
        r"<script\b[^>]*>.*?</script>",
        " ",
        value,
        flags=re.I | re.S,
    )

    value = re.sub(
        r"<style\b[^>]*>.*?</style>",
        " ",
        value,
        flags=re.I | re.S,
    )

    value = re.sub(
        r"<[^>]+>",
        " ",
        value,
    )

    replacements = {
        "&nbsp;": " ",
        "&amp;": "&",
        "&quot;": '"',
        "&#x27;": "'",
        "&#39;": "'",
        "&lt;": "<",
        "&gt;": ">",
    }

    for old, new in replacements.items():
        value = value.replace(
            old,
            new,
        )

    value = re.sub(
        r"\s+",
        " ",
        value,
    )

    return value.strip()


def extract_defillama_rows_from_html(html):
    """
    DeFiLlama's Tokens page currently exposes a ranked table.

    The parser deliberately uses several patterns because the
    HTML structure can change without warning.
    """

    results = []
    seen = set()

    # --------------------------------------------------------
    # Method 1: table rows
    # --------------------------------------------------------

    rows = re.findall(
        r"<tr\b[^>]*>(.*?)</tr>",
        html,
        flags=re.I | re.S,
    )

    for row in rows:

        cells = re.findall(
            r"<t[dh]\b[^>]*>(.*?)</t[dh]>",
            row,
            flags=re.I | re.S,
        )

        if not cells:
            continue

        parts = [
            clean_html_text(x)
            for x in cells
        ]

        text = " ".join(parts)
        text = re.sub(
            r"\s+",
            " ",
            text,
        ).strip()

        # Example concept:
        # 1 Bitcoin BTC $...
        match = re.search(
            r"^\s*(\d{1,4})\s+(.+?)\s+\$",
            text,
            flags=re.I,
        )

        if not match:
            continue

        rank = int(match.group(1))
        coin_part = match.group(2).strip()

        if rank < 1 or rank > 5000:
            continue

        # ----------------------------------------------------
        # Remove UI noise
        # ----------------------------------------------------

        coin_part = re.sub(
            r"Add to watchlist",
            " ",
            coin_part,
            flags=re.I,
        )

        coin_part = re.sub(
            r"Image:\s*Logo of",
            " ",
            coin_part,
            flags=re.I,
        )

        coin_part = re.sub(
            r"\s+",
            " ",
            coin_part,
        ).strip()

        # ----------------------------------------------------
        # Symbol at end
        # ----------------------------------------------------

        symbol_match = re.search(
            r"(?:^|\s)"
            r"([A-Z][A-Z0-9._-]{1,19})"
            r"$",
            coin_part,
        )

        symbol = None

        if symbol_match:
            symbol = (
                symbol_match
                .group(1)
                .upper()
            )

        # ----------------------------------------------------
        # Handles concatenated text:
        #
        # BitcoinBTC
        # EthereumETH
        # SolanaSOL
        # ----------------------------------------------------

        if not symbol:

            compact = re.sub(
                r"[^A-Za-z0-9_]",
                "",
                coin_part,
            )

            candidates = re.findall(
                r"[A-Z][A-Z0-9]{1,19}",
                compact,
            )

            if candidates:
                symbol = candidates[-1].upper()

        if not symbol:
            continue

        if not re.fullmatch(
            r"[A-Z][A-Z0-9._-]{1,19}",
            symbol,
        ):
            continue

        if symbol in seen:
            continue

        seen.add(symbol)

        results.append(
            {
                "rank": rank,
                "symbol": symbol,
            }
        )

    # --------------------------------------------------------
    # Method 2: full visible text
    # --------------------------------------------------------

    if len(results) < 20:

        plain = clean_html_text(
            html
        )

        pattern = re.compile(
            r"\b"
            r"(\d{1,4})"
            r"\s+"
            r"(?:Add to watchlist\s*)?"
            r"(?:Image:\s*Logo of\s*)?"
            r"(.{1,100}?)"
            r"([A-Z][A-Z0-9._-]{1,19})"
            r"\s+\$",
            flags=re.I,
        )

        for match in pattern.finditer(
            plain
        ):

            rank = int(match.group(1))
            symbol = match.group(3).upper()

            if rank < 1 or rank > 5000:
                continue

            if not re.fullmatch(
                r"[A-Z][A-Z0-9._-]{1,19}",
                symbol,
            ):
                continue

            if symbol in seen:
                continue

            seen.add(symbol)

            results.append(
                {
                    "rank": rank,
                    "symbol": symbol,
                }
            )

    # --------------------------------------------------------
    # Sort
    # --------------------------------------------------------

    results.sort(
        key=lambda x: x["rank"]
    )

    return results


async def get_defillama_top_tokens(
    session,
    limit=100,
):
    """
    Get the top tokens from DeFiLlama.

    Important:
    This function does NOT silently replace DeFiLlama
    with CoinGecko. If DeFiLlama cannot be read, the
    scan fails instead of pretending that the ranking
    came from DeFiLlama.
    """

    headers = {
        "Referer": "https://defillama.com/",
    }

    try:
        html = await get_text(
            session,
            DEFILLAMA_TOKENS_URL,
            headers=headers,
        )

    except Exception as exc:
        logging.error(
            "DeFiLlama request failed: %s",
            exc,
        )
        return []

    if not html:
        logging.error(
            "DeFiLlama returned empty HTML."
        )
        return []

    logging.info(
        "DeFiLlama page received: %d bytes",
        len(html),
    )

    tokens = (
        extract_defillama_rows_from_html(
            html
        )
    )

    if not tokens:
        logging.error(
            "DeFiLlama page was received "
            "but token ranking could not be parsed."
        )

        # Diagnostic information only.
        sample = clean_html_text(html)

        logging.error(
            "DeFiLlama text sample: %s",
            sample[:1000],
        )

        return []

    tokens = tokens[:limit]

    logging.info(
        "DeFiLlama tokens obtained: %d",
        len(tokens),
    )

    logging.info(
        "DeFiLlama top symbols: %s",
        ", ".join(
            x["symbol"]
            for x in tokens[:20]
        ),
    )

    return tokens


async def get_top_coins(
    session,
    exchange_info,
):
    """
    DeFiLlama Top 100
             ↓
    Binance USDT Spot filter
             ↓
    Final scanner list
    """

    defillama_tokens = (
        await get_defillama_top_tokens(
            session,
            TOP_N,
        )
    )

    if not defillama_tokens:
        raise RuntimeError(
            "DeFiLlama token ranking "
            "could not be obtained."
        )

    binance_spot = (
        build_binance_usdt_spot_symbols(
            exchange_info
        )
    )

    result = []

    for item in defillama_tokens:

        base = item["symbol"].upper()

        # Exclude obvious non-normal Binance symbols
        if base in {
            "USD",
            "USDT0",
            "FIGR_HELOC",
        }:
            continue

        if base not in binance_spot:
            continue

        pair = binance_spot[base]

        result.append(
            {
                "name": base,
                "symbol": base,
                "pair": pair,
                "rank": item["rank"],
            }
        )

        if len(result) >= TOP_N:
            break

    logging.info(
        "Binance USDT spot intersection: %d",
        len(result),
    )

    if not result:
        raise RuntimeError(
            "No DeFiLlama tokens matched "
            "Binance USDT spot markets."
        )

    return result


# ============================================================
# CANDLE / RSI SCAN
# ============================================================

async def get_klines(
    session,
    symbol,
    timeframe,
):
    return await binance(
        session,
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": timeframe,
            "limit": 200,
        },
    )


async def analyze_timeframe(
    session,
    symbol,
    tf_name,
    tf_binance,
):
    try:
        rows = await get_klines(
            session,
            symbol,
            tf_binance,
        )

        if len(rows) < RSI_PERIOD + 5:
            return None

        # ----------------------------------------------------
        # IMPORTANT:
        # Do NOT remove the current candle.
        #
        # We intentionally calculate RSI on the OPEN candle
        # because the requested alert is approximately
        # 10 minutes before its close.
        # ----------------------------------------------------

        closes = [
            float(row[4])
            for row in rows
        ]

        rsi_values = calculate_rsi(
            closes,
            RSI_PERIOD,
        )

        if not rsi_values:
            return None

        current_rsi = rsi_values[-1]

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

        # ----------------------------------------------------
        # Current candle
        # ----------------------------------------------------

        current_candle = rows[-1]

        open_time_ms = int(
            current_candle[0]
        )

        close_time_ms = int(
            current_candle[6]
        )

        current_volume = float(
            current_candle[5]
        )

        # ----------------------------------------------------
        # Previous 3 candle volumes
        # ----------------------------------------------------

        previous_volumes = []

        for i in range(2, 5):

            if len(rows) >= i:
                previous_volumes.append(
                    float(rows[-i][5])
                )

        while len(previous_volumes) < 3:
            previous_volumes.append(0.0)

        # ----------------------------------------------------
        # Volume state
        # ----------------------------------------------------

        previous_avg = (
            sum(previous_volumes) / 3
            if previous_volumes
            else 0
        )

        if previous_avg > 0:

            if current_volume > previous_avg * 1.2:
                volume_state = "زیاد"

            elif current_volume < previous_avg * 0.8:
                volume_state = "کم"

            else:
                volume_state = "معمولی"

        else:
            volume_state = "معمولی"

        # ----------------------------------------------------
        # Remaining time
        # ----------------------------------------------------

        now_ms = int(
            datetime.now(
                timezone.utc
            ).timestamp() * 1000
        )

        remaining_minutes = (
            close_time_ms - now_ms
        ) / 60000.0

        # ----------------------------------------------------
        # Alert only around 10 minutes before close.
        #
        # Example:
        # 10.8 min -> yes
        # 9.2  min -> yes
        # 15   min -> no
        # 4    min -> no
        # ----------------------------------------------------

        lower = (
            ALERT_MINUTES_BEFORE_CLOSE
            - WINDOW_MINUTES
        )

        upper = (
            ALERT_MINUTES_BEFORE_CLOSE
            + WINDOW_MINUTES
        )

        if not (
            lower
            <= remaining_minutes
            <= upper
        ):
            return None

        return {
            "tf": tf_name,
            "symbol": symbol,
            "rsi": float(current_rsi),
            "zone": current_zone,
            "volume": current_volume,
            "prev_volumes": previous_volumes,
            "volume_state": volume_state,
            "open_time_ms": open_time_ms,
            "close_time_ms": close_time_ms,
            "remaining_minutes": remaining_minutes,
        }

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


async def scan_coin(
    session,
    coin,
    semaphore,
):
    symbol = coin["pair"]

    async def run_tf(tf_name, tf_binance):

        async with semaphore:

            result = await analyze_timeframe(
                session,
                symbol,
                tf_name,
                tf_binance,
            )

            if result:

                result.update(
                    name=coin["name"],
                    rank=coin["rank"],
                )

            return result

    results = await asyncio.gather(
        *[
            run_tf(tf_name, tf_binance)
            for tf_name, tf_binance
            in TFS.items()
        ]
    )

    return [
        x
        for x in results
        if x
    ]


# ============================================================
# STATE
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

        return {
            "users": [
                str(x)
                for x in data.get(
                    "users",
                    [],
                )
            ],
            "offset": int(
                data.get(
                    "offset",
                    0,
                )
            ),
            "signals": dict(
                data.get(
                    "signals",
                    {},
                )
            ),
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
        )
        + "\n",
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

    max_update_id = (
        offset - 1
    )

    for update in updates or []:

        update_id = int(
            update.get(
                "update_id",
                0,
            )
        )

        max_update_id = max(
            max_update_id,
            update_id + 1,
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
            chat.get(
                "id",
                "",
            )
        )

        if not chat_id:
            continue

        text = (
            message.get("text")
            or ""
        ).strip()

        if not text:
            continue

        command = (
            text.lower()
            .split()[0]
        )

        # ----------------------------------------------------
        # /start
        # ----------------------------------------------------

        if command.startswith(
            "/start"
        ):

            if (
                chat_id
                not in state["users"]
            ):

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

            if (
                chat_id
                in state["users"]
            ):

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

            if (
                chat_id
                in state["users"]
            ):
                status = (
                    "فعال ✅"
                )
            else:
                status = (
                    "غیرفعال ⛔"
                )

            await telegram(
                session,
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": (
                        "📡 وضعیت اشتراک هشدار: "
                        f"{status}"
                    ),
                },
            )

    if updates:

        state["offset"] = (
            max_update_id
        )

        changed = True

    # --------------------------------------------------------
    # Legacy TELEGRAM_CHAT_ID
    # --------------------------------------------------------

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
            "to subscriber list."
        )

    if changed:
        save_state(state)

    return state


# ============================================================
# SIGNAL DEDUPLICATION
# ============================================================

def signal_key(alert):
    return (
        f"{alert['symbol']}:"
        f"{alert['tf']}:"
        f"{alert['open_time_ms']}:"
        f"{alert['zone']}"
    )


def is_new_signal(
    state,
    alert,
):
    key = signal_key(alert)

    signals = state.setdefault(
        "signals",
        {},
    )

    if key in signals:
        return False

    signals[key] = {
        "sent_at": int(
            datetime.now(
                timezone.utc
            ).timestamp()
        ),
    }

    return True


def cleanup_old_signals(state):
    now = int(
        datetime.now(
            timezone.utc
        ).timestamp()
    )

    # Keep roughly 48 hours of signal history.
    max_age = 48 * 60 * 60

    signals = state.get(
        "signals",
        {},
    )

    cleaned = {}

    for key, value in signals.items():

        try:
            sent_at = int(
                value.get(
                    "sent_at",
                    0,
                )
            )

            if now - sent_at <= max_age:
                cleaned[key] = value

        except Exception:
            continue

    state["signals"] = cleaned


# ============================================================
# TRADINGVIEW
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

    return (
        "https://www.tradingview.com/"
        "chart/?symbol=BINANCE%3A"
        f"{symbol}"
        f"&interval={interval}"
    )


# ============================================================
# MESSAGE FORMAT
# ============================================================

def close_time_iran(close_ms):
    """
    Iran time is UTC+3:30.
    """

    from datetime import timedelta

    dt = datetime.fromtimestamp(
        close_ms / 1000,
        tz=timezone.utc,
    )

    iran = dt + timedelta(
        hours=3,
        minutes=30,
    )

    return iran.strftime(
        "%H:%M"
    )


def format_volume(value):
    return bold_digits(
        fmt_volume(value)
    )


def build_alert_block(
    alert,
    is_new=True,
):
    rsi_value = alert["rsi"]

    if rsi_value > 70:
        emoji = "🟢"
        arrow = "↑"
    else:
        emoji = "🔴"
        arrow = "↓"

    # Telegram cannot literally color arbitrary text.
    # Green square = newly appearing signal
    # White square = repeated/previous data
    marker = (
        "🟢"
        if is_new
        else "⚪"
    )

    symbol = alert["symbol"].replace(
        "USDT",
        "",
    )

    volume = format_volume(
        alert["volume"]
    )

    close = bold_digits(
        close_time_iran(
            alert["close_time_ms"]
        )
    )

    rsi_text = bold_digits(
        f"{rsi_value:.2f}"
    )

    percent_text = bold_digits(
        f"{rsi_value:.0f}"
    )

    return (
        f"💠 {symbol}\n\n"

        f"{marker} RSI                 "
        f"{rsi_text}\n"

        f"{emoji} {arrow}                 "
        f"{percent_text} %\n"

        f"volume                 "
        f"{volume} ×\n"

        f"close                  "
        f"{close}\n"

        f"📈 TV\n"
        f"{tradingview_url(alert['symbol'], alert['tf'])}\n"
    )


def build_messages(
    alerts,
    new_keys,
):
    """
    Exact requested timeframe order:
    15m
    1h
    4h
    1D
    """

    if not alerts:
        return []

    grouped = {}

    for alert in alerts:

        grouped.setdefault(
            alert["tf"],
            [],
        ).append(alert)

    output = []

    for tf in sorted(
        grouped,
        key=lambda x:
        TF_ORDER.get(x, 99),
    ):

        header = (
            f"━━━━━━━━ {tf} ━━━━━━━━"
        )

        message = (
            "🚨 RSI Scanner Alert\n\n"
            + header
            + "\n\n"
        )

        for alert in sorted(
            grouped[tf],
            key=lambda x:
            x["symbol"],
        ):

            key = signal_key(
                alert
            )

            is_new = (
                key in new_keys
            )

            block = build_alert_block(
                alert,
                is_new=is_new,
            )

            message += (
                block
                + "\n"
                + "--------------------"
                + "\n\n"
            )

        # Telegram limit safety
        while len(message) > 3900:

            cut = message.rfind(
                "\n\n",
                0,
                3900,
            )

            if cut <= 0:
                cut = 3900

            output.append(
                message[:cut]
            )

            message = (
                "🚨 RSI Scanner Alert\n\n"
                + header
                + "\n\n"
                + message[cut:].lstrip()
            )

        if message.strip():
            output.append(
                message
            )

    return output


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

    if not users:
        logging.info(
            "No Telegram subscribers."
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
                    "disable_web_page_preview": True,
                },
            )

        except Exception as exc:

            logging.warning(
                "Could not send message "
                "to %s: %s",
                chat_id,
                exc,
            )


# ============================================================
# SCAN ONCE
# ============================================================

async def scan_once(
    session,
    coins,
    semaphore,
):
    groups = await asyncio.gather(
        *[
            scan_coin(
                session,
                coin,
                semaphore,
            )
            for coin in coins
        ],
        return_exceptions=True,
    )

    alerts = []

    for group in groups:

        if isinstance(
            group,
            Exception,
        ):
            logging.warning(
                "Coin scan error: %s",
                group,
            )
            continue

        alerts.extend(
            group
        )

    # Requested timeframe order
    alerts.sort(
        key=lambda x: (
            TF_ORDER.get(
                x["tf"],
                99,
            ),
            x["symbol"],
        )
    )

    return alerts


# ============================================================
# MAIN
# ============================================================

async def main():

    if not TOKEN:
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN "
            "GitHub Secret."
        )

    state = load_state()

    cleanup_old_signals(
        state
    )

    connector = aiohttp.TCPConnector(
        limit=40,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        # ----------------------------------------------------
        # Telegram commands
        # ----------------------------------------------------

        state = await process_commands(
            session,
            state,
        )

        # ----------------------------------------------------
        # Binance exchange information
        # ----------------------------------------------------

        exchange_info = (
            await get_binance_exchange_info(
                session
            )
        )

        logging.info(
            "Binance exchangeInfo loaded."
        )

        # ----------------------------------------------------
        # DeFiLlama + Binance intersection
        # ----------------------------------------------------

        coins = await get_top_coins(
            session,
            exchange_info,
        )

        logging.info(
            "Final scanner coins: %d",
            len(coins),
        )

        logging.info(
            "Final symbols: %s",
            ", ".join(
                x["symbol"]
                for x in coins[:30]
            ),
        )

        # ----------------------------------------------------
        # Run repeated scans inside this GitHub run.
        #
        # We DO NOT change the workflow schedule.
        # ----------------------------------------------------

        semaphore = asyncio.Semaphore(
            BINANCE_CONCURRENCY
        )

        started = (
            datetime.now(
                timezone.utc
            ).timestamp()
        )

        already_alerted_this_run = set()

        while True:

            now = (
                datetime.now(
                    timezone.utc
                ).timestamp()
            )

            elapsed = (
                now - started
            )

            if elapsed >= RUN_SECONDS:
                break

            logging.info(
                "Starting scan. "
                "Elapsed %.0fs / %ss",
                elapsed,
                RUN_SECONDS,
            )

            try:

                alerts = await scan_once(
                    session,
                    coins,
                    semaphore,
                )

                logging.info(
                    "Candidate alerts: %d",
                    len(alerts),
                )

                new_alerts = []
                new_keys = set()

                # ------------------------------------------------
                # Deduplication
                # ------------------------------------------------

                for alert in alerts:

                    key = signal_key(
                        alert
                    )

                    if key in (
                        already_alerted_this_run
                    ):
                        continue

                    if is_new_signal(
                        state,
                        alert,
                    ):

                        new_alerts.append(
                            alert
                        )

                        new_keys.add(
                            key
                        )

                        already_alerted_this_run.add(
                            key
                        )

                if new_alerts:

                    messages = build_messages(
                        new_alerts,
                        new_keys,
                    )

                    logging.info(
                        "New alerts: %d",
                        len(new_alerts),
                    )

                    for message in messages:

                        await send_to_all(
                            session,
                            state,
                            message,
                        )

                    save_state(
                        state
                    )

                else:

                    logging.info(
                        "No new RSI alerts."
                    )

            except Exception as exc:

                logging.exception(
                    "Scan error: %s",
                    exc,
                )

            # ----------------------------------------------------
            # Wait approximately one minute before next scan
            # ----------------------------------------------------

            now = (
                datetime.now(
                    timezone.utc
                ).timestamp()
            )

            remaining_runtime = (
                RUN_SECONDS
                - (now - started)
            )

            if remaining_runtime <= 0:
                break

            await asyncio.sleep(
                min(
                    SCAN_INTERVAL,
                    remaining_runtime,
                )
            )

    # Save final state
    save_state(
        state
    )

    logging.info(
        "Scanner finished."
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

        logging.info(
            "Stopped by user."
        )

    except Exception:

        logging.exception(
            "FATAL ERROR"
        )

        raise
