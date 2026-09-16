import os, asyncio, logging, json
from pathlib import Path
from datetime import datetime, timezone
import aiohttp

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
LEGACY_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '').strip()
TOP_N = int(os.getenv('TOP_N', '100'))
PERIOD = int(os.getenv('RSI_PERIOD', '14'))
ALERT_MODE = os.getenv('ALERT_MODE', 'changes').lower()
USERS_FILE = Path(os.getenv('USERS_FILE', 'users.json'))

CG = 'https://api.coingecko.com/api/v3/coins/markets'
BASES = ['https://data-api.binance.vision', 'https://api-gcp.binance.com', 'https://api.binance.com']
TFS = {'15m': '15m', '1h': '1h', '4h': '4h', '1D': '1d'}
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')


def rsi(vals, p=14):
    if len(vals) < p + 1:
        return []
    gains = [max(vals[i] - vals[i-1], 0) for i in range(1, p + 1)]
    losses = [max(vals[i-1] - vals[i], 0) for i in range(1, p + 1)]
    ag = sum(gains) / p
    al = sum(losses) / p
    out = [None] * p
    out.append(100 if al == 0 and ag > 0 else 50 if al == 0 else 100 - 100 / (1 + ag / al))
    for i in range(p + 1, len(vals)):
        ch = vals[i] - vals[i-1]
        ag = (ag * (p - 1) + max(ch, 0)) / p
        al = (al * (p - 1) + max(-ch, 0)) / p
        out.append(100 if al == 0 and ag > 0 else 50 if al == 0 else 100 - 100 / (1 + ag / al))
    return out


def zone(x):
    return 'oversold' if x < 30 else 'overbought' if x > 70 else 'neutral'


async def get(session, url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=20),
                headers={'User-Agent': 'crypto-rsi-telegram-bot/2.0'}
            ) as r:
                if r.status == 429:
                    await asyncio.sleep(min(float(r.headers.get('Retry-After', '3')), 15))
                    continue
                if r.status >= 400:
                    text = await r.text()
                    raise aiohttp.ClientResponseError(
                        r.request_info, r.history, status=r.status,
                        message=text[:300], headers=r.headers
                    )
                return await r.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == 2:
                raise
            await asyncio.sleep(1.5 * (attempt + 1))


async def post_json(session, url, payload):
    async with session.post(
        url, json=payload, timeout=aiohttp.ClientTimeout(total=20)
    ) as r:
        text = await r.text()
        if r.status >= 400:
            raise RuntimeError(f'Telegram HTTP {r.status}: {text[:500]}')
        data = json.loads(text)
        if not data.get('ok'):
            raise RuntimeError(f'Telegram API error: {text[:500]}')
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
            return await get(session, base + path, params)
        except aiohttp.ClientResponseError as e:
            last = e
            if e.status not in (400, 403, 418, 429, 451):
                raise
            logging.warning('%s returned HTTP %s; trying next endpoint', base, e.status)
    raise last


async def top_coins(session):
    data = await get(session, CG, {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': TOP_N,
        'page': 1,
        'sparkline': 'false'
    })
    return [
        {'name': x.get('name', x.get('symbol', '')), 'symbol': x.get('symbol', '').upper()}
        for x in data if x.get('symbol')
    ]


async def tickers(session):
    data = await binance(session, '/api/v3/ticker/24hr')
    return {x['symbol']: x for x in data if x.get('symbol')}


async def one_tf(session, symbol, tf):
    rows = await binance(session, '/api/v3/klines', {
        'symbol': symbol, 'interval': tf, 'limit': 200
    })
    if len(rows) < PERIOD + 3:
        return None

    # Always ignore the current/unclosed candle.
    rows = rows[:-1]
    closes = [float(x[4]) for x in rows]
    vals = rsi(closes, PERIOD)
    if vals[-1] is None or vals[-2] is None:
        return None

    z, pz = zone(vals[-1]), zone(vals[-2])
    if z not in ('oversold', 'overbought'):
        return None
    if ALERT_MODE != 'always' and z == pz:
        return None

    current_vol = float(rows[-1][5])
    prev3_vol = [float(rows[-i][5]) for i in (2, 3, 4)]
    avg3 = sum(prev3_vol) / 3

    if current_vol > avg3 * 1.2:
        vol_state = 'زیاد 🔥'
    elif current_vol < avg3 * 0.8:
        vol_state = 'کم 📉'
    else:
        vol_state = 'معمولی ➖'

    return {
        'tf': tf, 'rsi': vals[-1], 'prev': vals[-2],
        'volume': current_vol, 'prev3_vol': prev3_vol,
        'vol_state': vol_state,
        'close_ms': int(rows[-1][6])
    }


async def scan_coin(session, coin, ticker, sem):
    symbol = coin['symbol'] + 'USDT'
    if symbol not in ticker:
        return []

    async def run(tf):
        async with sem:
            try:
                x = await one_tf(session, symbol, tf)
                if x:
                    x.update(name=coin['name'], symbol=symbol)
                return x
            except aiohttp.ClientResponseError as e:
                if e.status not in (400, 404):
                    logging.warning('%s %s HTTP %s', symbol, tf, e.status)
                return None
            except Exception as e:
                logging.warning('%s %s: %s', symbol, tf, e)
                return None

    return [
        x for x in await asyncio.gather(*(run(tf) for tf in TFS.values()))
        if x
    ]


def fmtv(v):
    if v >= 1e9:
        return f'{v/1e9:.2f}B'
    if v >= 1e6:
        return f'{v/1e6:.2f}M'
    if v >= 1e3:
        return f'{v/1e3:.2f}K'
    return f'{v:.2f}'


def load_state():
    if not USERS_FILE.exists():
        return {'users': [], 'offset': 0}
    try:
        data = json.loads(USERS_FILE.read_text(encoding='utf-8'))
        return {
            'users': [str(x) for x in data.get('users', [])],
            'offset': int(data.get('offset', 0))
        }
    except Exception:
        logging.warning('Could not read %s; starting with empty state.', USERS_FILE)
        return {'users': [], 'offset': 0}


def save_state(state):
    USERS_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2) + '\n',
        encoding='utf-8'
    )


async def process_commands(session, state):
    """Register /start users and handle /stop, /status."""
    offset = state.get('offset', 0)
    updates = await telegram(session, 'getUpdates', {
        'offset': offset,
        'timeout': 0,
        'allowed_updates': ['message']
    })

    changed = False
    max_update_id = offset - 1

    for u in updates or []:
        max_update_id = max(max_update_id, int(u['update_id']) + 1)
        msg = u.get('message') or {}
        chat = msg.get('chat') or {}
        chat_id = str(chat.get('id', ''))
        if not chat_id:
            continue

        text = (msg.get('text') or '').strip().lower().split()[0] if msg.get('text') else ''
        if text.startswith('/start'):
            if chat_id not in state['users']:
                state['users'].append(chat_id)
                changed = True
                await telegram(session, 'sendMessage', {
                    'chat_id': chat_id,
                    'text': '✅ ربات فعال شد.\nاز این به بعد هشدارهای RSI را دریافت می‌کنید.'
                })
            else:
                await telegram(session, 'sendMessage', {
                    'chat_id': chat_id,
                    'text': 'ℹ️ شما از قبل فعال هستید.'
                })
        elif text.startswith('/stop'):
            if chat_id in state['users']:
                state['users'].remove(chat_id)
                changed = True
                await telegram(session, 'sendMessage', {
                    'chat_id': chat_id,
                    'text': '⛔ هشدارها متوقف شد.\nبرای فعال‌سازی دوباره /start را بزنید.'
                })
        elif text.startswith('/status'):
            status = 'فعال ✅' if chat_id in state['users'] else 'غیرفعال ⛔'
            await telegram(session, 'sendMessage', {
                'chat_id': chat_id,
                'text': f'📡 وضعیت اشتراک هشدار: {status}'
            })

    if updates:
        state['offset'] = max_update_id
        changed = True

    # Keep the existing single-user setup working during migration.
    if LEGACY_CHAT_ID and LEGACY_CHAT_ID not in state['users']:
        state['users'].append(LEGACY_CHAT_ID)
        changed = True
        logging.info('Added TELEGRAM_CHAT_ID to multi-user subscriber list.')

    if changed:
        save_state(state)

    return state


def messages(alerts):
    """Create one separate Telegram message for each timeframe."""
    if not alerts:
        return []

    grouped = {}
    for a in alerts:
        grouped.setdefault(a['tf'], []).append(a)

    result = []
    # Keep the requested timeframe order.
    order = {'15m': 0, '1h': 1, '4h': 2, '1D': 3}

    for tf in sorted(grouped, key=lambda x: order.get(x, 99)):
        s = '🚨 RSI Scanner Alert\n\n'

        for a in sorted(grouped[tf], key=lambda x: x['symbol']):
            e = '🔴' if a['rsi'] < 30 else '🟢'
            base = a['symbol'].replace('USDT', '')

            s += f"{e} {a['name']} ({a['symbol']})\n"
            s += f"⏱ TF: {a['tf']}\n"
            s += f"📊 RSI(14): {a['rsi']:.2f}\n"
            s += f"📊 وضعیت حجم: {a['vol_state']}\n"
            s += f"📦 حجم فعلی: {fmtv(a['volume'])} {base}\n"
            s += f"1️⃣: {fmtv(a['prev3_vol'][0])} {base}\n"
            s += f"2️⃣: {fmtv(a['prev3_vol'][1])} {base}\n"
            s += f"3️⃣: {fmtv(a['prev3_vol'][2])} {base}\n"
            s += f"📈 https://www.tradingview.com/symbols/{a['symbol']}/\n\n"

        # If one timeframe has many alerts, split only that timeframe's message.
        while len(s) > 3900:
            cut = s.rfind('\n\n', 0, 3900) or 3900
            result.append(s[:cut])
            s = '🚨 RSI Scanner Alert\n\n' + s[cut:].lstrip()

        if s.strip() != '🚨 RSI Scanner Alert':
            result.append(s)

    return result


async def send_to_all(session, state, text):
    users = list(dict.fromkeys(state.get('users', [])))
    failed = []

    for chat_id in users:
        try:
            await telegram(session, 'sendMessage', {
                'chat_id': chat_id,
                'text': text,
                'disable_web_page_preview': True
            })
        except Exception as e:
            logging.warning('Could not send to %s: %s', chat_id, e)
            failed.append(chat_id)

    return failed


async def wait_for_next_15m_boundary():
    """For a manual workflow run, wait until the next 15-minute boundary.

    Scheduled GitHub Actions runs do NOT wait here; they scan immediately and
    use Binance's latest fully closed candle.
    """
    if os.getenv('GITHUB_EVENT_NAME', '') != 'workflow_dispatch':
        return

    now = datetime.now(timezone.utc)
    next_minute = ((now.minute // 15) + 1) * 15
    if next_minute >= 60:
        from datetime import timedelta
        target = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        target = now.replace(minute=next_minute, second=0, microsecond=0)

    wait_seconds = max(0, (target - now).total_seconds())
    logging.info('Manual run started. Waiting %.1f seconds for the next 15m candle close.', wait_seconds)
    if wait_seconds > 0:
        await asyncio.sleep(wait_seconds)
    # Small buffer so Binance has finalized the just-closed candle.
    await asyncio.sleep(2)


async def main():
    if not TOKEN:
        raise RuntimeError('Missing TELEGRAM_BOT_TOKEN GitHub Secret.')

    await wait_for_next_15m_boundary()

    state = load_state()

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=30)
    ) as session:
        # Register users who sent /start, and process /stop /status.
        state = await process_commands(session, state)

        coins = await top_coins(session)
        logging.info('Top coins: %s', len(coins))

        tick = await tickers(session)
        logging.info('Binance tickers: %s', len(tick))

        sem = asyncio.Semaphore(12)
        groups = await asyncio.gather(
            *(scan_coin(session, c, tick, sem) for c in coins)
        )
        alerts = [a for g in groups for a in g]
        logging.info('Alerts: %s | Subscribers: %s',
                     len(alerts), len(state['users']))

        if not state['users']:
            logging.info('No subscribers yet. Send /start to the bot.')

        for m in messages(alerts):
            await send_to_all(session, state, m)

        if not alerts:
            logging.info('No new RSI zone-entry alerts.')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception:
        logging.exception('FATAL ERROR')
        raise
