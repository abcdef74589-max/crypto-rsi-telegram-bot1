import os
import asyncio
import logging
import json
import re
import html
from pathlib import Path
from datetime import datetime, timezone
import aiohttp


# =========================================================
# CONFIG
# =========================================================

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
LEGACY_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '').strip()

TOP_N = int(os.getenv('TOP_N', '100'))
PERIOD = int(os.getenv('RSI_PERIOD', '14'))
ALERT_MODE = os.getenv('ALERT_MODE', 'changes').lower()

USERS_FILE = Path(os.getenv('USERS_FILE', 'users.json'))

CG = 'https://api.coingecko.com/api/v3/coins/markets'

BASES = [
    'https://data-api.binance.vision',
    'https://api-gcp.binance.com',
    'https://api.binance.com'
]

TFS = {
    '15m': '15m',
    '1h': '1h',
    '4h': '4h',
    '1D': '1d'
}

POLYMARKET = 'https://gamma-api.polymarket.com'
DEGEN = 'https://www.degensignal.com'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)


# =========================================================
# RSI
# =========================================================

def rsi(vals, p=14):
    if len(vals) < p + 1:
        return []

    gains = [
        max(vals[i] - vals[i - 1], 0)
        for i in range(1, p + 1)
    ]

    losses = [
        max(vals[i - 1] - vals[i], 0)
        for i in range(1, p + 1)
    ]

    ag = sum(gains) / p
    al = sum(losses) / p

    out = [None] * p

    out.append(
        100
        if al == 0 and ag > 0
        else 50
        if al == 0
        else 100 - 100 / (1 + ag / al)
    )

    for i in range(p + 1, len(vals)):
        ch = vals[i] - vals[i - 1]

        ag = (
            ag * (p - 1) +
            max(ch, 0)
        ) / p

        al = (
            al * (p - 1) +
            max(-ch, 0)
        ) / p

        out.append(
            100
            if al == 0 and ag > 0
            else 50
            if al == 0
            else 100 - 100 / (1 + ag / al)
        )

    return out


def zone(x):
    if x < 30:
        return 'oversold'

    if x > 70:
        return 'overbought'

    return 'neutral'


# =========================================================
# HTTP
# =========================================================

async def get(session, url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
                headers={
                    'User-Agent':
                    'crypto-rsi-telegram-bot/3.0'
                }
            ) as r:

                if r.status == 429:
                    await asyncio.sleep(
                        min(
                            float(
                                r.headers.get(
                                    'Retry-After',
                                    '3'
                                )
                            ),
                            15
                        )
                    )
                    continue

                if r.status >= 400:
                    text = await r.text()

                    raise aiohttp.ClientResponseError(
                        r.request_info,
                        r.history,
                        status=r.status,
                        message=text[:300],
                        headers=r.headers
                    )

                return await r.json()

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError
        ):

            if attempt == 2:
                raise

            await asyncio.sleep(
                1.5 * (attempt + 1)
            )


async def get_text(session, url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
                headers={
                    'User-Agent':
                    'Mozilla/5.0 '
                    '(compatible; crypto-rsi-telegram-bot/3.0)'
                }
            ) as r:

                if r.status == 429:
                    await asyncio.sleep(3)
                    continue

                if r.status >= 400:
                    text = await r.text()
                    raise aiohttp.ClientResponseError(
                        r.request_info,
                        r.history,
                        status=r.status,
                        message=text[:300],
                        headers=r.headers
                    )

                return await r.text()

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError
        ):

            if attempt == 2:
                raise

            await asyncio.sleep(
                1.5 * (attempt + 1)
            )


async def post_json(session, url, payload):
    async with session.post(
        url,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=20)
    ) as r:

        text = await r.text()

        if r.status >= 400:
            raise RuntimeError(
                f'Telegram HTTP {r.status}: {text[:500]}'
            )

        data = json.loads(text)

        if not data.get('ok'):
            raise RuntimeError(
                f'Telegram API error: {text[:500]}'
            )

        return data.get('result')


# =========================================================
# TELEGRAM
# =========================================================

async def telegram(session, method, payload=None):
    return await post_json(
        session,
        f'https://api.telegram.org/bot{TOKEN}/{method}',
        payload or {}
    )


# =========================================================
# BINANCE
# =========================================================

async def binance(session, path, params=None):
    last = None

    for base in BASES:
        try:
            return await get(
                session,
                base + path,
                params
            )

        except aiohttp.ClientResponseError as e:
            last = e

            if e.status not in (
                400,
                403,
                418,
                429,
                451
            ):
                raise

            logging.warning(
                '%s returned HTTP %s; '
                'trying next endpoint',
                base,
                e.status
            )

    raise last


# =========================================================
# TOP COINS
# =========================================================

async def top_coins(session):
    data = await get(
        session,
        CG,
        {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': TOP_N,
            'page': 1,
            'sparkline': 'false'
        }
    )

    return [
        {
            'name': x.get(
                'name',
                x.get('symbol', '')
            ),
            'symbol': x.get(
                'symbol',
                ''
            ).upper()
        }
        for x in data
        if x.get('symbol')
    ]


async def tickers(session):
    data = await binance(
        session,
        '/api/v3/ticker/24hr'
    )

    return {
        x['symbol']: x
        for x in data
        if x.get('symbol')
    }


# =========================================================
# TECHNICAL PREDICTION
# =========================================================

def technical_prediction(closes):
    """
    Local technical estimate.
    This is only used as a fallback component when
    external prediction data is unavailable.
    """

    if len(closes) < 10:
        return None

    last = closes[-1]
    previous = closes[-2]

    change = (
        (last - previous) / previous
    ) * 100

    short_change = (
        (last - closes[-6]) /
        closes[-6]
    ) * 100

    score = 50.0

    score += max(
        -12,
        min(12, change * 4)
    )

    score += max(
        -18,
        min(18, short_change * 2)
    )

    return max(
        1.0,
        min(99.0, score)
    )


# =========================================================
# POLYMARKET PREDICTION
# =========================================================

async def polymarket_prediction(
    session,
    symbol,
    tf
):
    """
    Polymarket 15m Up/Down market.

    Only used where the matching market exists.
    No fabricated value is returned.
    """

    if tf != '15m':
        return None

    base = symbol.replace(
        'USDT',
        ''
    ).lower()

    supported = {
        'BTC',
        'ETH',
        'SOL',
        'XRP'
    }

    if base.upper() not in supported:
        return None

    now = int(
        datetime.now(
            timezone.utc
        ).timestamp()
    )

    slot = (
        now // 900
    ) * 900

    slug = (
        f'{base}-updown-15m-{slot}'
    )

    url = (
        f'{POLYMARKET}/markets'
    )

    try:
        data = await get(
            session,
            url,
            {
                'slug': slug
            }
        )

        if not data:
            return None

        market = (
            data[0]
            if isinstance(data, list)
            else data
        )

        prices = market.get(
            'outcomePrices'
        )

        outcomes = market.get(
            'outcomes'
        )

        if not prices:
            return None

        if isinstance(prices, str):
            prices = json.loads(prices)

        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)

        if not isinstance(
            prices,
            list
        ):
            return None

        up = None

        if outcomes:
            for i, outcome in enumerate(outcomes):
                if str(outcome).lower() == 'up':
                    up = float(prices[i])
                    break

        if up is None and len(prices) >= 2:
            up = float(prices[0])

        if up is None:
            return None

        return max(
            0.0,
            min(100.0, up * 100)
        )

    except Exception as e:
        logging.debug(
            'Polymarket unavailable for %s %s: %s',
            symbol,
            tf,
            e
        )
        return None


# =========================================================
# DEGEN SIGNAL
# =========================================================

async def degen_prediction(
    session,
    symbol,
    tf
):
    """
    Degen Signal currently publishes crypto
    forecasts for BTC/ETH/SOL/XRP 15m.

    We use only a currently published forecast.
    """

    if tf != '15m':
        return None

    base = symbol.replace(
        'USDT',
        ''
    ).lower()

    if base not in {
        'btc',
        'eth',
        'sol',
        'xrp'
    }:
        return None

    try:
        url = (
            f'{DEGEN}/markets/{base}'
        )

        text = await get_text(
            session,
            url
        )

        text = html.unescape(text)

        # Search nearby forecast/confidence
        # patterns from the public page.
        patterns = [
            r'Latest call\s*'
            r'(UP|DOWN)'
            r'(\d{1,3})%',

            r'Call\s*'
            r'(UP|DOWN)'
            r'.{0,100}?'
            r'Confidence\s*'
            r'(\d{1,3})%'
        ]

        for pattern in patterns:
            m = re.search(
                pattern,
                text,
                re.IGNORECASE |
                re.DOTALL
            )

            if not m:
                continue

            direction = m.group(1).upper()
            confidence = float(
                m.group(2)
            )

            if direction == 'UP':
                return confidence

            return 100.0 - confidence

    except Exception as e:
        logging.debug(
            'Degen Signal unavailable for %s %s: %s',
            symbol,
            tf,
            e
        )

    return None


# =========================================================
# MULTI-SOURCE PREDICTION
# =========================================================

async def prediction_average(
    session,
    symbol,
    tf,
    closes
):
    """
    Final prediction = average of all REAL
    available prediction values.

    Sources currently connected:
      1. Polymarket
      2. Degen Signal
      3. Local technical model

    A source that has no matching data is simply
    excluded. No fake 50% is inserted.
    """

    values = []

    technical = technical_prediction(
        closes
    )

    if technical is not None:
        values.append(
            technical
        )

    poly = await polymarket_prediction(
        session,
        symbol,
        tf
    )

    if poly is not None:
        values.append(
            poly
        )

    degen = await degen_prediction(
        session,
        symbol,
        tf
    )

    if degen is not None:
        values.append(
            degen
        )

    if not values:
        return None

    return sum(values) / len(values)


# =========================================================
# ONE TIMEFRAME
# =========================================================

async def one_tf(
    session,
    symbol,
    tf
):
    rows = await binance(
        session,
        '/api/v3/klines',
        {
            'symbol': symbol,
            'interval': tf,
            'limit': 200
        }
    )

    if len(rows) < PERIOD + 4:
        return None

    # Keep the exact previous behavior:
    # ignore the current/unclosed candle.
    rows = rows[:-1]

    closes = [
        float(x[4])
        for x in rows
    ]

    vals = rsi(
        closes,
        PERIOD
    )

    if (
        not vals
        or vals[-1] is None
        or vals[-2] is None
    ):
        return None

    z = zone(
        vals[-1]
    )

    pz = zone(
        vals[-2]
    )

    if z not in (
        'oversold',
        'overbought'
    ):
        return None

    if (
        ALERT_MODE != 'always'
        and z == pz
    ):
        return None

    current_vol = float(
        rows[-1][5]
    )

    prev3_vol = [
        float(rows[-i][5])
        for i in (2, 3, 4)
    ]

    avg3 = (
        sum(prev3_vol) / 3
    )

    if current_vol > avg3 * 1.2:
        vol_state = 'زیاد 🔥'
    elif current_vol < avg3 * 0.8:
        vol_state = 'کم 📉'
    else:
        vol_state = 'معمولی ➖'

    prediction = await prediction_average(
        session,
        symbol,
        tf,
        closes
    )

    return {
        'tf': tf,
        'rsi': vals[-1],
        'prev': vals[-2],
        'volume': current_vol,
        'prev3_vol': prev3_vol,
        'vol_state': vol_state,
        'prediction': prediction,
        'close_ms': int(rows[-1][6])
    }


# =========================================================
# SCAN COIN
# =========================================================

async def scan_coin(
    session,
    coin,
    ticker,
    sem
):
    symbol = (
        coin['symbol']
        + 'USDT'
    )

    # Keep the same Binance filter.
    if symbol not in ticker:
        return []

    async def run(tf):
        async with sem:
            try:
                x = await one_tf(
                    session,
                    symbol,
                    tf
                )

                if x:
                    x.update(
                        name=coin['name'],
                        symbol=symbol
                    )

                return x

            except aiohttp.ClientResponseError as e:
                if e.status not in (
                    400,
                    404
                ):
                    logging.warning(
                        '%s %s HTTP %s',
                        symbol,
                        tf,
                        e.status
                    )

                return None

            except Exception as e:
                logging.warning(
                    '%s %s: %s',
                    symbol,
                    tf,
                    e
                )

                return None

    return [
        x
        for x in await asyncio.gather(
            *(
                run(tf)
                for tf in TFS.values()
            )
        )
        if x
    ]


# =========================================================
# FORMAT
# =========================================================

DIGITS = str.maketrans(
    '0123456789',
    '𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵'
)


def bold_numbers(value):
    return str(value).translate(
        DIGITS
    )


def fmtv(v):
    if v >= 1e9:
        return f'{v / 1e9:.2f}B'

    if v >= 1e6:
        return f'{v / 1e6:.2f}M'

    if v >= 1e3:
        return f'{v / 1e3:.2f}K'

    return f'{v:.2f}'


def prediction_text(value):
    if value is None:
        return '—'

    if value >= 50:
        arrow = '↑'
    else:
        arrow = '↓'

    return (
        f'{arrow} '
        f'{bold_numbers(round(value))} %'
    )


def close_time_text(close_ms):
    dt = datetime.fromtimestamp(
        close_ms / 1000,
        tz=timezone.utc
    )

    # Iran standard time = UTC+3:30
    from datetime import timedelta

    iran = dt + timedelta(
        hours=3,
        minutes=30
    )

    return iran.strftime(
        '%H:%M'
    )


# =========================================================
# STATE
# =========================================================

def load_state():
    if not USERS_FILE.exists():
        return {
            'users': [],
            'offset': 0
        }

    try:
        data = json.loads(
            USERS_FILE.read_text(
                encoding='utf-8'
            )
        )

        return {
            'users': [
                str(x)
                for x in data.get(
                    'users',
                    []
                )
            ],
            'offset': int(
                data.get(
                    'offset',
                    0
                )
            )
        }

    except Exception:
        logging.warning(
            'Could not read %s; '
            'starting with empty state.',
            USERS_FILE
        )

        return {
            'users': [],
            'offset': 0
        }


def save_state(state):
    USERS_FILE.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2
        ) + '\n',
        encoding='utf-8'
    )


# =========================================================
# TELEGRAM COMMANDS
# =========================================================

async def process_commands(
    session,
    state
):
    offset = state.get(
        'offset',
        0
    )

    updates = await telegram(
        session,
        'getUpdates',
        {
            'offset': offset,
            'timeout': 0,
            'allowed_updates': [
                'message'
            ]
        }
    )

    changed = False

    max_update_id = (
        offset - 1
    )

    for u in updates or []:

        max_update_id = max(
            max_update_id,
            int(
                u['update_id']
            ) + 1
        )

        msg = (
            u.get('message')
            or {}
        )

        chat = (
            msg.get('chat')
            or {}
        )

        chat_id = str(
            chat.get('id', '')
        )

        if not chat_id:
            continue

        text = (
            (msg.get('text') or '')
            .strip()
            .lower()
            .split()[0]
            if msg.get('text')
            else ''
        )

        if text.startswith('/start'):

            if chat_id not in state['users']:

                state['users'].append(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    'sendMessage',
                    {
                        'chat_id': chat_id,
                        'text':
                        '✅ ربات فعال شد.\n'
                        'از این به بعد هشدارهای RSI را دریافت می‌کنید.'
                    }
                )

            else:

                await telegram(
                    session,
                    'sendMessage',
                    {
                        'chat_id': chat_id,
                        'text':
                        'ℹ️ شما از قبل فعال هستید.'
                    }
                )

        elif text.startswith('/stop'):

            if chat_id in state['users']:

                state['users'].remove(
                    chat_id
                )

                changed = True

                await telegram(
                    session,
                    'sendMessage',
                    {
                        'chat_id': chat_id,
                        'text':
                        '⛔ هشدارها متوقف شد.\n'
                        'برای فعال‌سازی دوباره /start را بزنید.'
                    }
                )

        elif text.startswith('/status'):

            status = (
                'فعال ✅'
                if chat_id in state['users']
                else 'غیرفعال ⛔'
            )

            await telegram(
                session,
                'sendMessage',
                {
                    'chat_id': chat_id,
                    'text':
                    f'📡 وضعیت اشتراک هشدار: {status}'
                }
            )

    if updates:
        state['offset'] = max_update_id
        changed = True

    if (
        LEGACY_CHAT_ID
        and LEGACY_CHAT_ID
        not in state['users']
    ):
        state['users'].append(
            LEGACY_CHAT_ID
        )

        changed = True

        logging.info(
            'Added TELEGRAM_CHAT_ID '
            'to multi-user subscriber list.'
        )

    if changed:
        save_state(state)

    return state


# =========================================================
# MESSAGES
# =========================================================

def messages(alerts):

    if not alerts:
        return []

    grouped = {}

    for a in alerts:
        grouped.setdefault(
            a['tf'],
            []
        ).append(a)

    result = []

    order = {
        '15m': 0,
        '1h': 1,
        '4h': 2,
        '1D': 3
    }

    for tf in sorted(
        grouped,
        key=lambda x:
        order.get(x, 99)
    ):

        s = (
            '🚨 RSI Scanner Alert\n\n'
            f'━━━━━━━━ {tf} ━━━━━━━━\n\n'
        )

        for a in sorted(
            grouped[tf],
            key=lambda x:
            x['symbol']
        ):

            emoji = (
                '🔴'
                if a['rsi'] < 30
                else '🟢'
            )

            base = a['symbol'].replace(
                'USDT',
                ''
            )

            rsi_value = bold_numbers(
                f'{a["rsi"]:.2f}'
            )

            prediction = prediction_text(
                a.get('prediction')
            )

            volume = bold_numbers(
                fmtv(
                    a['volume']
                )
            )

            v1 = bold_numbers(
                fmtv(
                    a['prev3_vol'][0]
                )
            )

            v2 = bold_numbers(
                fmtv(
                    a['prev3_vol'][1]
                )
            )

            v3 = bold_numbers(
                fmtv(
                    a['prev3_vol'][2]
                )
            )

            close = bold_numbers(
                close_time_text(
                    a['close_ms']
                )
            )

            interval = {
                '15m': '15',
                '1h': '60',
                '4h': '240',
                '1D': 'D'
            }[a['tf']]

            tv = (
                'https://www.tradingview.com/'
                'chart/?symbol=BINANCE%3A'
                f'{a["symbol"]}'
                f'&interval={interval}'
            )

            s += (
                f'💠 {base}\n\n'
                f'{emoji} RSI                 '
                f'{rsi_value}\n'
                f'🔮 {prediction}\n'
                f'volume                 '
                f'{volume} ×\n'
                f'1                      '
                f'{v1}\n'
                f'2                      '
                f'{v2}\n'
                f'3                      '
                f'{v3}\n'
                f'close                  '
                f'{close}\n'
                f'📈 TV\n'
                f'{tv}\n\n'
                f'--------------------\n\n'
            )

        while len(s) > 3900:

            cut = (
                s.rfind(
                    '\n\n',
                    0,
                    3900
                )
                or 3900
            )

            result.append(
                s[:cut]
            )

            s = (
                '🚨 RSI Scanner Alert\n\n'
                f'━━━━━━━━ {tf} ━━━━━━━━\n\n'
                + s[cut:].lstrip()
            )

        if s.strip() != (
            '🚨 RSI Scanner Alert'
        ):
            result.append(s)

    return result


# =========================================================
# SEND
# =========================================================

async def send_to_all(
    session,
    state,
    text
):
    users = list(
        dict.fromkeys(
            state.get(
                'users',
                []
            )
        )
    )

    failed = []

    for chat_id in users:

        try:

            await telegram(
                session,
                'sendMessage',
                {
                    'chat_id': chat_id,
                    'text': text,
                    'disable_web_page_preview': True
                }
            )

        except Exception as e:

            logging.warning(
                'Could not send to %s: %s',
                chat_id,
                e
            )

            failed.append(
                chat_id
            )

    return failed


# =========================================================
# KEEP PREVIOUS TIMING
# =========================================================

async def wait_for_next_15m_boundary():

    """
    EXACT previous timing behavior.

    Manual workflow:
    wait for next 15m candle boundary.

    Scheduled workflow:
    scan immediately.
    """

    if os.getenv(
        'GITHUB_EVENT_NAME',
        ''
    ) != 'workflow_dispatch':
        return

    now = datetime.now(
        timezone.utc
    )

    next_minute = (
        (now.minute // 15) + 1
    ) * 15

    if next_minute >= 60:

        from datetime import timedelta

        target = (
            now.replace(
                minute=0,
                second=0,
                microsecond=0
            )
            + timedelta(hours=1)
        )

    else:

        target = now.replace(
            minute=next_minute,
            second=0,
            microsecond=0
        )

    wait_seconds = max(
        0,
        (
            target - now
        ).total_seconds()
    )

    logging.info(
        'Manual run started. '
        'Waiting %.1f seconds '
        'for the next 15m candle close.',
        wait_seconds
    )

    if wait_seconds > 0:
        await asyncio.sleep(
            wait_seconds
        )

    await asyncio.sleep(2)


# =========================================================
# MAIN
# =========================================================

async def main():

    if not TOKEN:
        raise RuntimeError(
            'Missing TELEGRAM_BOT_TOKEN GitHub Secret.'
        )

    # DO NOT CHANGE TIMING
    await wait_for_next_15m_boundary()

    state = load_state()

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=30
        )
    ) as session:

        # Commands
        state = await process_commands(
            session,
            state
        )

        # Top coins
        coins = await top_coins(
            session
        )

        logging.info(
            'Top coins: %s',
            len(coins)
        )

        # Binance tickers
        tick = await tickers(
            session
        )

        logging.info(
            'Binance tickers: %s',
            len(tick)
        )

        # Keep the same concurrency
        sem = asyncio.Semaphore(
            12
        )

        groups = await asyncio.gather(
            *(
                scan_coin(
                    session,
                    c,
                    tick,
                    sem
                )
                for c in coins
            )
        )

        alerts = [
            a
            for g in groups
            for a in g
        ]

        logging.info(
            'Alerts: %s | Subscribers: %s',
            len(alerts),
            len(state['users'])
        )

        if not state['users']:
            logging.info(
                'No subscribers yet. '
                'Send /start to the bot.'
            )

        # Same previous sending behavior
        for m in messages(alerts):

            await send_to_all(
                session,
                state,
                m
            )

        if not alerts:
            logging.info(
                'No new RSI zone-entry alerts.'
            )


# =========================================================
# START
# =========================================================

if __name__ == '__main__':

    try:
        asyncio.run(
            main()
        )

    except Exception:

        logging.exception(
            'FATAL ERROR'
        )

        raise
