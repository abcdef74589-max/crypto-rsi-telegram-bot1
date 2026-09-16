import os
import asyncio
import logging
import json
from pathlib import Path
from datetime import datetime, timezone
import aiohttp

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
LEGACY_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '').strip()
TOP_N = int(os.getenv('TOP_N', '100'))
PERIOD = int(os.getenv('RSI_PERIOD', '14'))
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

TF_ORDER = {
    '15m': 0,
    '1h': 1,
    '4h': 2,
    '1D': 3
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)


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
            ag * (p - 1) + max(ch, 0)
        ) / p

        al = (
            al * (p - 1) + max(-ch, 0)
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


async def get(session, url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
                headers={
                    'User-Agent':
                    'crypto-rsi-telegram-bot/4.0'
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

    return None


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


async def telegram(session, method, payload=None):
    return await post_json(
        session,
        f'https://api.telegram.org/bot{TOKEN}/{method}',
        payload or {}
    )


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


def active_timeframes(now=None):
    """
    تایم‌فریم‌هایی را برمی‌گرداند که
    کندل فعلی‌شان وارد ۱۰ دقیقه پایانی شده است.

    زمان Binance بر اساس UTC است.
    """

    now = now or datetime.now(timezone.utc)

    minute = now.minute
    hour = now.hour

    result = []

    # 15 دقیقه‌ای
    #
    # مثال:
    # 15:05 تا 15:15
    # 15:20 تا 15:30
    # 15:35 تا 15:45
    # 15:50 تا 16:00
    if minute % 15 >= 5:
        result.append('15m')

    # 1 ساعته
    #
    # دقیقه 50 تا 60
    if minute >= 50:
        result.append('1h')

    # 4 ساعته
    #
    # ساعت‌های پایان:
    # 03, 07, 11, 15, 19, 23
    if hour % 4 == 3 and minute >= 50:
        result.append('4h')

    # روزانه
    #
    # آخرین 10 دقیقه روز UTC
    if hour == 23 and minute >= 50:
        result.append('1D')

    return result


async def one_tf(session, symbol, tf):
    rows = await binance(
        session,
        '/api/v3/klines',
        {
            'symbol': symbol,
            'interval': tf,
            'limit': 200
        }
    )

    if len(rows) < PERIOD + 5:
        return None

    # کندل فعلی و باز
    current = rows[-1]

    now_ms = int(
        datetime.now(timezone.utc).timestamp()
        * 1000
    )

    candle_open_ms = int(current[0])
    close_ms = int(current[6])

    remaining_ms = close_ms - now_ms

    # فقط 0 تا 10 دقیقه مانده به بسته شدن
    if (
        remaining_ms < 0
        or remaining_ms > 10 * 60 * 1000
    ):
        return None

    # قیمت بسته شدن فعلی کندل باز
    closes = [
        float(x[4])
        for x in rows
    ]

    vals = rsi(
        closes,
        PERIOD
    )

    if not vals or vals[-1] is None:
        return None

    current_rsi = vals[-1]

    z = zone(current_rsi)

    # فقط RSI بالای 70 یا پایین 30
    if z not in (
        'oversold',
        'overbought'
    ):
        return None

    # حجم کندل فعلی
    current_volume = float(
        current[5]
    )

    # سه کندل قبلی
    prev3_volume = [
        float(rows[-i][5])
        for i in (2, 3, 4)
    ]

    avg3 = sum(prev3_volume) / 3

    if current_volume > avg3 * 1.2:
        vol_state = 'زیاد 🔥'

    elif current_volume < avg3 * 0.8:
        vol_state = 'کم 📉'

    else:
        vol_state = 'معمولی ➖'

    return {
        'tf': tf,
        'rsi': current_rsi,
        'zone': z,
        'volume': current_volume,
        'prev3_vol': prev3_volume,
        'vol_state': vol_state,
        'candle_open_ms': candle_open_ms,
        'close_ms': close_ms,
        'remaining_ms': remaining_ms
    }


async def scan_coin(
    session,
    coin,
    ticker,
    sem,
    timeframes
):
    symbol = coin['symbol'] + 'USDT'

    if symbol not in ticker:
        return []

    async def run(tf_name):
        async with sem:
            try:
                x = await one_tf(
                    session,
                    symbol,
                    TFS[tf_name]
                )

                if x:
                    x.update(
                        name=coin['name'],
                        symbol=symbol,
                        tf=tf_name
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
                        tf_name,
                        e.status
                    )

                return None

            except Exception as e:
                logging.warning(
                    '%s %s: %s',
                    symbol,
                    tf_name,
                    e
                )

                return None

    return [
        x
        for x in await asyncio.gather(
            *(
                run(tf)
                for tf in timeframes
            )
        )
        if x
    ]


def fmtv(v):
    if v >= 1e9:
        return f'{v / 1e9:.2f}B'

    if v >= 1e6:
        return f'{v / 1e6:.2f}M'

    if v >= 1e3:
        return f'{v / 1e3:.2f}K'

    return f'{v:.2f}'


def load_state():
    if not USERS_FILE.exists():
        return {
            'users': [],
            'offset': 0,
            'signals': {}
        }

    try:
       
