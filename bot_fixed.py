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

LEGACY_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
).strip()

TOP_N = int(
    os.getenv(
        "TOP_N",
        "100"
    )
)

RSI_PERIOD = int(
    os.getenv(
        "RSI_PERIOD",
        "14"
    )
)

USERS_FILE = Path(
    os.getenv(
        "USERS_FILE",
        "users.json"
    )
)

# Number of top Binance USDT spot coins.
TOP_BINANCE_COINS = 100

# Alert when current candle has about 10 minutes remaining.
ALERT_MINUTES_BEFORE_CLOSE = 10

# We accept 9-11 minutes remaining.
ALERT_MIN_SECONDS = 9 * 60
ALERT_MAX_SECONDS = 11 * 60

# Scan every minute during a GitHub Actions run.
SCAN_INTERVAL_SECONDS = 60

# GitHub workflow runs every 5 minutes.
# Keep bot runtime below 5 minutes.
RUN_DURATION_SECONDS = 285

# Iran time UTC+3:30
IRAN_TZ = timezone(
    timedelta(
        hours=3,
        minutes=30
    )
)

# Binance public market-data endpoints.
BINANCE_BASES = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api.binance.com",
]

# Timeframes
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
# HTTP HELPERS
# ============================================================

async def get_json(
    session,
    url,
    params=None,
    timeout=20
):
    last_error = None

    for attempt in range(3):

        try:

            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(
                    total=timeout
                ),
                headers={
                    "User-Agent":
                        "Crypto-RSI-Telegram-Bot/4.0"
                },
            ) as response:

                if response.status == 429:

                    retry_after = (
                        response.headers.get(
                            "Retry-After",
                            "3"
                        )
                    )

                    try:
                        delay = float(
                            retry_after
                        )
                    except Exception:
                        delay = 3

                    await asyncio.sleep(
                        min(delay, 10)
                    )

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

                return await response.json(
                    content_type=None
                )

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            json.JSONDecodeError,
        ) as exc:

            last_error = exc

            if attempt < 2:
                await asyncio.sleep(
                    1.5 * (attempt + 1)
                )

    raise last_error


async def post_json(
    session,
    url,
    payload
):

    async with session.post(
        url,
        json=payload,
        timeout=aiohttp.ClientTimeout(
            total=20
        ),
    ) as response:

        text = await response.text()

        if response.status >= 400:

            raise RuntimeError(
                f"HTTP {response.status}: "
                f"{text[:500]}"
            )

        try:
            data = json.loads(text)

        except Exception:

            raise RuntimeError(
                "Invalid JSON response: "
                + text[:500]
            )

        if not data.get("ok"):

            raise RuntimeError(
                "Telegram API error: "
                + text[:500]
            )

        return data.get("result")


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
            "TELEGRAM_BOT_TOKEN is missing."
        )

    url = (
        f"https://api.telegram.org/"
        f"bot{TOKEN}/{method}"
    )

    return await post_json(
        session,
        url,
        payload or {}
    )


# ============================================================
# BINANCE
# ============================================================

async def binance(
    session,
    path,
    params=None
):

    last_error = None

    for base in BINANCE_BASES:

        try:

            return await get_json(
                session,
                base + path,
                params=params,
                timeout=25,
            )

        except aiohttp.ClientResponseError as exc:

            last_error = exc

            logging.warning(
                "%s returned HTTP %s",
                base,
                exc.status
            )

            if exc.status not in (
                400,
                403,
                418,
                429,
                451,
                500,
                502,
                503,
                504,
            ):
                raise

        except Exception as exc:

            last_error = exc

            logging.warning(
                "%s failed: %s",
                base,
                exc
            )

    if last_error:
        raise last_error

    raise RuntimeError(
        "No Binance endpoint available."
    )


# ============================================================
# TOP 100 BINANCE
# ============================================================

# Stablecoins are excluded so that "top coins"
# does not become dominated by stablecoin pairs.
EXCLUDED_BASE_ASSETS = {
    "USDT",
    "USDC",
    "FDUSD",
    "TUSD",
    "USDP",
    "DAI",
    "USDE",
    "PYUSD",
    "BUSD",
    "EUR",
    "EURI",
}

# Leveraged-token suffixes.
LEVERAGED_SUFFIXES = (
    "UP",
    "DOWN",
    "BULL",
    "BEAR",
)


def is_normal_spot_coin(
    symbol_info
):
    symbol = symbol_info.get(
        "symbol",
        ""
    )

    status = symbol_info.get(
        "status",
        ""
    )

    base_asset = symbol_info.get(
        "baseAsset",
        ""
    )

    quote_asset = symbol_info.get(
        "quoteAsset",
        ""
    )

    spot_allowed = symbol_info.get(
        "isSpotTradingAllowed",
        False
    )

    if status != "TRADING":
        return False

    if quote_asset != "USDT":
        return False

    if not spot_allowed:
        return False

    if not symbol.endswith("USDT"):
        return False

    if base_asset in EXCLUDED_BASE_ASSETS:
        return False

    upper_base = base_asset.upper()

    for suffix in LEVERAGED_SUFFIXES:

        if upper_base.endswith(suffix):
            return False

    return True


async def get_exchange_info(
    session
):

    return await binance(
        session,
        "/api/v3/exchangeInfo"
    )


async def get_top_100_binance_coins(
    session
):
    """
    Gets Binance USDT spot markets and ranks them
    by 24h quote volume.

    Result:
        up to 100 normal USDT spot coins.
    """

    logging.info(
        "Getting Binance exchange information..."
    )

    exchange_info = (
        await get_exchange_info(
            session
        )
    )

    valid_symbols = {}

    for item in exchange_info.get(
        "symbols",
        []
    ):

        if is_normal_spot_coin(
            item
        ):

            symbol = item.get(
                "symbol"
            )

            base = item.get(
                "baseAsset"
            )

            if symbol and base:

                valid_symbols[
                    symbol
                ] = {
                    "symbol": symbol,
                    "base": base,
                }

    logging.info(
        "Valid Binance USDT spot markets: %s",
        len(valid_symbols)
    )

    if not valid_symbols:

        raise RuntimeError(
            "No Binance USDT spot markets found."
        )

    logging.info(
        "Getting Binance 24h ticker data..."
    )

    ticker_data = await binance(
        session,
        "/api/v3/ticker/24hr"
    )

    ranked = []

    for ticker in ticker_data:

        symbol = ticker.get(
            "symbol"
        )

        if symbol not in valid_symbols:
            continue

        try:

            quote_volume = float(
                ticker.get(
                    "quoteVolume",
                    0
                )
            )

        except Exception:

            quote_volume = 0.0

        if quote_volume <= 0:
            continue

        info = valid_symbols[
            symbol
        ]

        ranked.append(
            {
                "symbol": symbol,
                "base": info["base"],
                "quote_volume": quote_volume,
            }
        )

    ranked.sort(
        key=lambda x:
            x["quote_volume"],
        reverse=True
    )

    top = ranked[
        :TOP_BINANCE_COINS
    ]

    logging.info(
        "Top Binance coins selected: %s",
        len(top)
    )

    if top:

        preview = ", ".join(
            x["symbol"]
            for x in top[:10]
        )

        logging.info(
            "Top 10 Binance volume coins: %s",
            preview
        )

    return top


# ============================================================
# RSI
# ============================================================

def calculate_rsi(
    values,
    period=14
):
    """
    Wilder RSI.
    """

    if len(values) < period + 1:
        return []

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

        if change > 0:

            gains.append(
                change
            )

            losses.append(
                0.0
            )

        else:

            gains.append(
                0.0
            )

            losses.append(
                -change
            )

    avg_gain = (
        sum(gains)
        / period
    )

    avg_loss = (
        sum(losses)
        / period
    )

    result = [
        None
    ] * period

    if avg_loss == 0:

        first = (
            100.0
            if avg_gain > 0
            else 50.0
        )

    else:

        rs = (
            avg_gain
            / avg_loss
        )

        first = (
            100.0
            - (
                100.0
                / (1.0 + rs)
            )
        )

    result.append(
        first
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
            0.0
        )

        loss = max(
            -change,
            0.0
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

            current = (
                100.0
                if avg_gain > 0
                else 50.0
            )

        else:

            rs = (
                avg_gain
                / avg_loss
            )

            current = (
                100.0
                - (
                    100.0
                    / (1.0 + rs)
                )
            )

        result.append(
            current
        )

    return result


def rsi_zone(
    value
):

    if value > 70:
        return "overbought"

    if value < 30:
        return "oversold"

    return "neutral"


# ============================================================
# CURRENT CANDLE ANALYSIS
# ============================================================

def seconds_until_close(
    close_ms
):

    now_ms = (
        datetime.now(
            timezone.utc
        ).timestamp()
        * 1000
    )

    return (
        float(close_ms)
        - now_ms
    ) / 1000.0


async def analyze_timeframe(
    session,
    symbol,
    timeframe
):

    rows = await binance(
        session,
        "/api/v3/klines",
        {
            "symbol": symbol,
            "interval": timeframe,
            "limit": 200,
        }
    )

    if len(rows) < (
        RSI_PERIOD + 5
    ):

        return None

    # --------------------------------------------------------
    # CURRENT OPEN CANDLE
    # --------------------------------------------------------

    current = rows[-1]

    open_time_ms = int(
        current[0]
    )

    close_time_ms = int(
        current[6]
    )

    remaining = (
        seconds_until_close(
            close_time_ms
        )
    )

    # --------------------------------------------------------
    # 10 MINUTES BEFORE CLOSE
    # --------------------------------------------------------

    if not (
        ALERT_MIN_SECONDS
        <= remaining
        <= ALERT_MAX_SECONDS
    ):

        return None

    # --------------------------------------------------------
    # CLOSE PRICES
    # --------------------------------------------------------

    closes = [
        float(row[4])
        for row in rows
    ]

    rsi_values = calculate_rsi(
        closes,
        RSI_PERIOD
    )

    if len(rsi_values) < 2:
        return None

    current_rsi = (
        rsi_values[-1]
    )

    previous_rsi = (
        rsi_values[-2]
    )

    if current_rsi is None:
        return None

    current_zone = rsi_zone(
        current_rsi
    )

    if current_zone == "neutral":

        return {
            "symbol": symbol,
            "tf": timeframe,
            "neutral": True,
            "open_time_ms": open_time_ms,
        }

    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    current_volume = float(
        current[5]
    )

    previous_volumes = [
        float(rows[-2][5]),
        float(rows[-3][5]),
        float(rows[-4][5]),
    ]

    avg_previous_volume = (
        sum(previous_volumes)
        / len(previous_volumes)
    )

    if avg_previous_volume > 0:

        volume_ratio = (
            current_volume
            / avg_previous_volume
        )

    else:

        volume_ratio = 1.0

    if current_volume > max(
        previous_volumes
    ):

        volume_state = (
            "زیاد 🔥"
        )

    elif current_volume < min(
        previous_volumes
    ):

        volume_state = (
            "کم 📉"
        )

    else:

        volume_state = (
            "معمولی ➖"
        )

    return {
        "symbol": symbol,
        "tf": timeframe,

        "rsi": float(
            current_rsi
        ),

        "previous_rsi": (
            float(previous_rsi)
            if previous_rsi is not None
            else 50.0
        ),

        "zone": current_zone,

        "volume": current_volume,

        "previous_volumes":
            previous_volumes,

        "volume_ratio":
            volume_ratio,

        "volume_state":
            volume_state,

        "open_time_ms":
            open_time_ms,

        "close_time_ms":
            close_time_ms,

        "remaining":
            remaining,

        "neutral":
            False,
    }


# ============================================================
# SCAN ONE COIN
# ============================================================

async def scan_coin(
    session,
    coin,
    semaphore
):

    symbol = coin["symbol"]

    results = []

    async def worker(
        tf_name,
        tf_value
    ):

        async with semaphore:

            try:

                result = (
                    await analyze_timeframe(
                        session,
                        symbol,
                        tf_value
                    )
                )

                if result:

                    result["base"] = (
                        coin["base"]
                    )

                    result["quote_volume"] = (
                        coin[
                            "quote_volume"
                        ]
                    )

                    return result

            except Exception as exc:

                logging.warning(
                    "%s %s failed: %s",
                    symbol,
                    tf_name,
                    exc
                )

            return None

    tasks = [
        worker(
            tf_name,
            tf_value
        )
        for tf_name, tf_value
        in TFS.items()
    ]

    results = await asyncio.gather(
        *tasks
    )

    return [
        x
        for x in results
        if x is not None
    ]


async def scan_all(
    session,
    coins
):

    semaphore = asyncio.Semaphore(
        12
    )

    tasks = [
        scan_coin(
            session,
            coin,
            semaphore
        )
        for coin in coins
    ]

    groups = await asyncio.gather(
        *tasks
    )

    results = []

    for group in groups:
        results.extend(
            group
        )

    return results


# ============================================================
# USERS.JSON
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

        if not isinstance(
            data,
            dict
        ):

            return default_state()

        users = [
            str(x)
            for x in data.get(
                "users",
                []
            )
        ]

        signals = data.get(
            "signals",
            {}
        )

        if not isinstance(
            signals,
            dict
        ):

            signals = {}

        return {
            "users": users,
            "offset": int(
                data.get(
                    "offset",
                    0
                )
            ),
            "signals": signals,
        }

    except Exception as exc:

        logging.warning(
            "Could not load users.json: %s",
            exc
        )

        return default_state()


def save_state(
    state
):

    USERS_FILE.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2
        )
        + "\n",
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
        state.get(
            "offset",
            0
        )
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

        return state

    changed = False

    next_offset = offset

    for update in (
        updates or []
    ):

        try:

            update_id = int(
                update[
                    "update_id"
                ]
            )

            next_offset = max(
                next_offset,
                update_id + 1
            )

        except Exception:

            continue

        message = (
            update.get(
                "message"
            )
            or {}
        )

        chat = (
            message.get(
                "chat"
            )
            or {}
        )

        chat_id = str(
            chat.get(
                "id",
                ""
            )
        )

        if not chat_id:
            continue

        text = (
            message.get(
                "text"
            )
            or ""
        ).strip()

        command = (
            text.split()[0].lower()
            if text
            else ""
        )

        # ----------------------------------------------------
        # START
        # ----------------------------------------------------

        if command.startswith(
            "/start"
        ):

            if chat_id not in state[
                "users"
            ]:

                state[
                    "users"
                ].append(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id":
                            chat_id,

                        "text":
                            "✅ ربات فعال شد.\n"
                            "هشدارهای RSI "
                            "برای شما فعال شد."
                    }
                )

            else:

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id":
                            chat_id,

                        "text":
                            "ℹ️ ربات از قبل "
                            "برای شما فعال است."
                    }
                )

        # ----------------------------------------------------
        # STOP
        # ----------------------------------------------------

        elif command.startswith(
            "/stop"
        ):

            if chat_id in state[
                "users"
            ]:

                state[
                    "users"
                ].remove(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    "sendMessage",
                    {
                        "chat_id":
                            chat_id,

                        "text":
                            "⛔ هشدارها متوقف شد.\n"
                            "برای فعال‌سازی دوباره "
                            "/start را بزنید."
                    }
                )

        # ----------------------------------------------------
        # STATUS
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
                    "chat_id":
                        chat_id,

                    "text":
                        f"📡 وضعیت: {status}"
                }
            )

    if updates:

        state[
            "offset"
        ] = next_offset

        changed = True

    # Backward compatibility.
    if (
        LEGACY_CHAT_ID
        and LEGACY_CHAT_ID
        not in state["users"]
    ):

        state[
            "users"
        ].append(
            LEGACY_CHAT_ID
        )

        changed = True

    if changed:

        save_state(
            state
        )

    return state


# ============================================================
# SIGNAL STATE
# ============================================================

def signal_key(
    alert
):

    return (
        f"{alert['symbol']}|"
        f"{alert['tf']}"
    )


def handle_signal(
    state,
    alert
):
    """
    State machine:

    neutral -> oversold   = NEW
    neutral -> overbought = NEW

    oversold -> oversold   = NO
    overbought -> overbought = NO

    oversold -> neutral -> oversold = NEW
    overbought -> neutral -> overbought = NEW

    oversold -> overbought = NEW
    overbought -> oversold = NEW
    """

    key = signal_key(
        alert
    )

    signals = state.setdefault(
        "signals",
        {}
    )

    current_zone = alert[
        "zone"
    ]

    current_open = int(
        alert[
            "open_time_ms"
        ]
    )

    previous = signals.get(
        key
    )

    # No previous state.
    if previous is None:

        signals[key] = {
            "zone":
                current_zone,

            "open_time_ms":
                current_open,
        }

        return True

    previous_zone = (
        previous.get(
            "zone",
            "neutral"
        )
    )

    previous_open = int(
        previous.get(
            "open_time_ms",
            0
        )
    )

    # New candle.
    if current_open != previous_open:

        # Same extreme zone on a new candle:
        # keep it suppressed.
        if (
            previous_zone
            == current_zone
        ):

            signals[key] = {
                "zone":
                    current_zone,

                "open_time_ms":
                    current_open,
            }

            return False

        # Zone changed.
        signals[key] = {
            "zone":
                current_zone,

            "open_time_ms":
                current_open,
        }

        return True

    # Same candle.
    if (
        previous_zone
        == current_zone
    ):

        return False

    signals[key] = {
        "zone":
            current_zone,

        "open_time_ms":
            current_open,
    }

    return True


def update_neutral_states(
    state,
    all_alerts
):

    observed = {}

    for alert in all_alerts:

        key = signal_key(
            alert
        )

        observed[
            key
        ] = alert

    signals = state.setdefault(
        "signals",
        {}
    )

    for key in list(
        signals.keys()
    ):

        if key not in observed:

            signals[key][
                "zone"
            ] = "neutral"


# ============================================================
# NUMBER FORMAT
# ============================================================

DIGITS = str.maketrans(
    "0123456789",
    "𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵"
)


def bold_digits(
    value
):

    return str(
        value
    ).translate(
        DIGITS
    )


def format_rsi(
    value
):

    return bold_digits(
        f"{value:.2f}"
    )


def format_percent(
    value
):

    return bold_digits(
        f"{value:.0f}"
    )


def format_volume(
    value
):

    if value >= 1_000_000_000:

        text = (
            f"{value / 1e9:.2f}B"
        )

    elif value >= 1_000_000:

        text = (
            f"{value / 1e6:.2f}M"
        )

    elif value >= 1_000:

        text = (
            f"{value / 1e3:.2f}K"
        )

    else:

        text = (
            f"{value:.2f}"
        )

    return bold_digits(
        text
    )


def format_close_time(
    close_ms
):

    dt = datetime.fromtimestamp(
        close_ms / 1000,
        tz=timezone.utc
    ).astimezone(
        IRAN_TZ
    )

    return bold_digits(
        dt.strftime(
            "%H:%M"
        )
    )


# ============================================================
# TRADINGVIEW
# ============================================================

def tradingview_url(
    symbol,
    timeframe
):

    intervals = {
        "15m": "15",
        "1h": "60",
        "4h": "240",
        "1D": "D",
    }

    interval = intervals.get(
        timeframe,
        "15"
    )

    return (
        "https://www.tradingview.com/"
        "chart/?symbol=BINANCE%3A"
        f"{symbol}"
        f"&interval={interval}"
    )


# ============================================================
# ALERT FORMAT
# ============================================================

def build_alert_block(
    alert,
    is_new
):

    symbol = alert[
        "symbol"
    ]

    if symbol.endswith(
        "USDT"
    ):

        base = symbol[
            :-4
        ]

    else:

        base = symbol

    # Direction.
    if alert[
        "rsi"
    ] > 70:

        direction = "↑"
        direction_emoji = "🟢"

    else:

        direction = "↓"
        direction_emoji = "🔴"

    # Newly appearing data = green.
    # Repeated data = white.
    marker = (
        "🟢"
        if is_new
        else "⚪"
    )

    rsi_value = format_rsi(
        alert["rsi"]
    )

    rsi_percent = format_percent(
        alert["rsi"]
    )

    volume_ratio = bold_digits(
        f"{alert['volume_ratio']:.2f}"
    )

    previous_volumes = [
        format_volume(
            value
        )
        for value
        in alert[
            "previous_volumes"
        ]
    ]

    close_time = (
        format_close_time(
            alert[
                "close_time_ms"
            ]
        )
    )

    tv = tradingview_url(
        symbol,
        alert["tf"]
    )

    text = (
        f"{marker} {base}\n\n"

        f"{direction_emoji} RSI"
        f"                 "
        f"{rsi_value}\n"

        f"🔮 {direction_emoji} "
        f"{direction}"
        f"                "
        f"{rsi_percent} %\n"

        f"volume"
        f"                 "
        f"{volume_ratio} ×\n"

        f"close"
        f"                  "
        f"{close_time}\n"

        f"volume state"
        f"            "
        f"{alert['volume_state']}\n"

        f"1"
        f"                      "
        f"{previous_volumes[0]}\n"

        f"2"
        f"                      "
        f"{previous_volumes[1]}\n"

        f"3"
        f"                      "
        f"{previous_volumes[2]}\n"

        f"📈 TV\n"
        f"{tv}"
    )

    return text


# ============================================================
# MESSAGE SPLITTER
# ============================================================

def split_message(
    text,
    max_length=3900
):

    if len(text) <= max_length:

        return [
            text
        ]

    result = []

    remaining = text

    while len(
        remaining
    ) > max_length:

        cut = remaining.rfind(
            "\n\n",
            0,
            max_length
        )

        if cut <= 0:

            cut = remaining.rfind(
                "\n",
                0,
                max_length
            )

        if cut <= 0:

            cut = max_length

        result.append(
            remaining[
                :cut
            ].rstrip()
        )

        remaining = (
            remaining[
                cut:
            ].lstrip()
        )

    if remaining:

        result.append(
            remaining
        )

    return result


# ============================================================
# BUILD ALERT MESSAGES
# ============================================================

def build_messages(
    alerts
):

    if not alerts:

        return []

    grouped = {}

    for alert, is_new in alerts:

        tf = alert[
            "tf"
        ]

        grouped.setdefault(
            tf,
            []
        ).append(
            (
                alert,
                is_new
            )
        )

    messages = []

    for tf in sorted(
        grouped.keys(),
        key=lambda x:
            TF_ORDER.get(
                x,
                99
            )
    ):

        header = (
            "━━━━━━━━ "
            f"{tf}"
            " ━━━━━━━━"
        )

        current = (
            "🚨 RSI Scanner Alert\n\n"
            f"{header}\n\n"
        )

        items = sorted(
            grouped[tf],
            key=lambda x:
                x[0]["symbol"]
        )

        for alert, is_new in items:

            block = build_alert_block(
                alert,
                is_new
            )

            separator = (
                "\n\n"
                "--------------------"
                "\n\n"
            )

            candidate = (
                current
                + block
                + separator
            )

            if len(
                candidate
            ) > 3900:

                messages.append(
                    current.rstrip()
                )

                current = (
                    "🚨 RSI Scanner Alert\n\n"
                    f"{header}\n\n"
                    + block
                    + separator
                )

            else:

                current = candidate

        if current.strip():

            messages.append(
                current.rstrip()
            )

    return messages


# ============================================================
# SEND ALERTS
# ============================================================

async def send_message_to_users(
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
                    "chat_id":
                        chat_id,

                    "text":
                        text,

                    "disable_web_page_preview":
                        True,
                }
            )

        except Exception as exc:

            logging.warning(
                "Telegram send failed "
                "for %s: %s",
                chat_id,
                exc
            )


# ============================================================
# ONE SCAN CYCLE
# ============================================================

async def run_scan_cycle(
    session,
    state,
    coins
):

    alerts = await scan_all(
        session,
        coins
    )

    logging.info(
        "Extreme/open-candle results: %s",
        len(alerts)
    )

    new_alerts = []

    for alert in alerts:

        if alert.get(
            "neutral"
        ):

            continue

        is_new = handle_signal(
            state,
            alert
        )

        if is_new:

            new_alerts.append(
                (
                    alert,
                    True
                )
            )

    # Reset states for pairs that have returned
    # to neutral.
    update_neutral_states(
        state,
        [
            x
            for x in alerts
            if not x.get(
                "neutral"
            )
        ]
    )

    save_state(
        state
    )

    logging.info(
        "New alerts this cycle: %s",
        len(new_alerts)
    )

    if not new_alerts:

        return

    messages = build_messages(
        new_alerts
    )

    for message in messages:

        await send_message_to_users(
            session,
            state,
            message
        )


# ============================================================
# MAIN
# ============================================================

async def main():

    if not TOKEN:

        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN "
            "GitHub Secret is missing."
        )

    state = load_state()

    connector = aiohttp.TCPConnector(
        limit=50,
        ttl_dns_cache=300
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        # ----------------------------------------------------
        # Telegram commands
        # ----------------------------------------------------

        state = await process_commands(
            session,
            state
        )

        # ----------------------------------------------------
        # GET TOP 100 BINANCE
        # ----------------------------------------------------

        coins = (
            await get_top_100_binance_coins(
                session
            )
        )

        if not coins:

            raise RuntimeError(
                "Could not obtain "
                "top Binance coins."
            )

        # ----------------------------------------------------
        # RUN MULTIPLE SCANS
        # ----------------------------------------------------

        start = (
            asyncio.get_running_loop()
            .time()
        )

        cycle = 0

        while True:

            cycle += 1

            elapsed = (
                asyncio.get_running_loop()
                .time()
                - start
            )

            if elapsed >= (
                RUN_DURATION_SECONDS
            ):

                break

            logging.info(
                "========== SCAN %s ==========",
                cycle
            )

            try:

                # Process Telegram commands again.
                state = (
                    await process_commands(
                        session,
                        state
                    )
                )

                # Refresh top Binance ranking.
                # This means the 100 coins can change
                # with Binance volume.
                coins = (
                    await get_top_100_binance_coins(
                        session
                    )
                )

                await run_scan_cycle(
                    session,
                    state,
                    coins
                )

            except Exception as exc:

                logging.exception(
                    "Scan cycle failed: %s",
                    exc
                )

            elapsed = (
                asyncio.get_running_loop()
                .time()
                - start
            )

            remaining_runtime = (
                RUN_DURATION_SECONDS
                - elapsed
            )

            if remaining_runtime <= 0:

                break

            sleep_time = min(
                SCAN_INTERVAL_SECONDS,
                remaining_runtime
            )

            logging.info(
                "Next scan in %s seconds.",
                int(sleep_time)
            )

            await asyncio.sleep(
                sleep_time
            )

    logging.info(
        "Bot run finished."
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
