import os, asyncio, logging
from datetime import datetime, timezone
import aiohttp

TOKEN=os.getenv('TELEGRAM_BOT_TOKEN','').strip()
CHAT_ID=os.getenv('TELEGRAM_CHAT_ID','').strip()
TOP_N=int(os.getenv('TOP_N','100'))
PERIOD=int(os.getenv('RSI_PERIOD','14'))
ALERT_MODE=os.getenv('ALERT_MODE','changes').lower()
CG='https://api.coingecko.com/api/v3/coins/markets'
BASES=['https://data-api.binance.vision','https://api-gcp.binance.com','https://api.binance.com']
TFS={'15m':'15m','1h':'1h','4h':'4h','1D':'1d'}
logging.basicConfig(level=logging.INFO,format='%(asctime)s | %(levelname)s | %(message)s')

def rsi(vals,p=14):
    if len(vals)<p+1:return []
    gains=[max(vals[i]-vals[i-1],0) for i in range(1,p+1)]
    losses=[max(vals[i-1]-vals[i],0) for i in range(1,p+1)]
    ag=sum(gains)/p; al=sum(losses)/p; out=[None]*p
    out.append(100 if al==0 and ag>0 else 50 if al==0 else 100-100/(1+ag/al))
    for i in range(p+1,len(vals)):
        ch=vals[i]-vals[i-1]; ag=(ag*(p-1)+max(ch,0))/p; al=(al*(p-1)+max(-ch,0))/p
        out.append(100 if al==0 and ag>0 else 50 if al==0 else 100-100/(1+ag/al))
    return out

def zone(x): return 'oversold' if x<30 else 'overbought' if x>70 else 'neutral'

async def get(session,url,params=None):
    for attempt in range(3):
        try:
            async with session.get(url,params=params,timeout=aiohttp.ClientTimeout(total=20),headers={'User-Agent':'crypto-rsi-telegram-bot/1.0'}) as r:
                if r.status==429:
                    await asyncio.sleep(min(float(r.headers.get('Retry-After','3')),15)); continue
                if r.status>=400:
                    text=await r.text(); raise aiohttp.ClientResponseError(r.request_info,r.history,status=r.status,message=text[:300],headers=r.headers)
                return await r.json()
        except (aiohttp.ClientError,asyncio.TimeoutError):
            if attempt==2: raise
            await asyncio.sleep(1.5*(attempt+1))

async def binance(session,path,params=None):
    last=None
    for base in BASES:
        try:return await get(session,base+path,params)
        except aiohttp.ClientResponseError as e:
            last=e
            if e.status not in (400,403,418,429,451): raise
            logging.warning('%s returned HTTP %s; trying next endpoint',base,e.status)
    raise last

async def top_coins(session):
    data=await get(session,CG,{'vs_currency':'usd','order':'market_cap_desc','per_page':TOP_N,'page':1,'sparkline':'false'})
    return [{'name':x.get('name',x.get('symbol','')),'symbol':x.get('symbol','').upper()} for x in data if x.get('symbol')]

async def tickers(session):
    data=await binance(session,'/api/v3/ticker/24hr'); return {x['symbol']:x for x in data if x.get('symbol')}

async def one_tf(session,symbol,tf):
    rows=await binance(session,'/api/v3/klines',{'symbol':symbol,'interval':tf,'limit':200})
    if len(rows)<PERIOD+3:return None
    rows=rows[:-1]  # ignore current/unclosed candle
    closes=[float(x[4]) for x in rows]; vals=rsi(closes,PERIOD)
    if vals[-1] is None or vals[-2] is None:return None
    z,pz=zone(vals[-1]),zone(vals[-2])
    if z not in ('oversold','overbought'):return None
    if ALERT_MODE!='always' and z==pz:return None
    return {'tf':tf,'rsi':vals[-1],'prev':vals[-2],'price':closes[-1],
            'close_ms':int(rows[-1][6])}

async def scan_coin(session,coin,ticker,sem):
    symbol=coin['symbol']+'USDT'
    if symbol not in ticker:return []
    async def run(tf):
        async with sem:
            try:
                x=await one_tf(session,symbol,tf)
                if x:x.update(name=coin['name'],symbol=symbol,vol=float(ticker[symbol].get('quoteVolume',0)))
                return x
            except aiohttp.ClientResponseError as e:
                if e.status not in (400,404):logging.warning('%s %s HTTP %s',symbol,tf,e.status)
                return None
            except Exception as e:
                logging.warning('%s %s: %s',symbol,tf,e); return None
    return [x for x in await asyncio.gather(*(run(tf) for tf in TFS.values())) if x]

def fmtv(v): return f'${v/1e9:.2f}B' if v>=1e9 else f'${v/1e6:.2f}M' if v>=1e6 else f'${v/1e3:.2f}K' if v>=1e3 else f'${v:.0f}'
def fmtp(v): return f'{v:,.2f}' if v>=1000 else f'{v:,.4f}' if v>=1 else f'{v:.8f}'.rstrip('0').rstrip('.')

def messages(alerts):
    if not alerts:return []
    s='🚨 RSI Scanner Alert\n\n'
    for a in sorted(alerts,key=lambda x:(x['symbol'],x['tf'])):
        e='🔴' if a['rsi']<30 else '🟢'; dt=datetime.fromtimestamp(a['close_ms']/1000,tz=timezone.utc)
        s+=f"{e} {a['name']} ({a['symbol']})\n⏱ TF: {a['tf']}\n📊 RSI(14): {a['rsi']:.2f}\n💰 Price: {fmtp(a['price'])} USDT\n📦 24h Volume: {fmtv(a['vol'])}\n🕒 Closed: {dt:%Y-%m-%d %H:%M UTC}\n📈 https://www.tradingview.com/symbols/{a['symbol']}/\n\n"
    out=[]
    while len(s)>3900:
        cut=s.rfind('\n\n',0,3900) or 3900; out.append(s[:cut]); s=s[cut:].lstrip()
    if s:out.append(s)
    return out

async def send(session,text):
    url=f'https://api.telegram.org/bot{TOKEN}/sendMessage'
    async with session.post(url,json={'chat_id':CHAT_ID,'text':text,'disable_web_page_preview':True},timeout=aiohttp.ClientTimeout(total=20)) as r:
        if r.status>=400:raise RuntimeError(f'Telegram HTTP {r.status}: {(await r.text())[:500]}')

async def main():
    if not TOKEN or not CHAT_ID:raise RuntimeError('Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID GitHub Secret.')
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30)) as session:
        coins=await top_coins(session); logging.info('Top coins: %s',len(coins))
        tick=await tickers(session); logging.info('Binance tickers: %s',len(tick))
        sem=asyncio.Semaphore(12)
        groups=await asyncio.gather(*(scan_coin(session,c,tick,sem) for c in coins))
        alerts=[a for g in groups for a in g]; logging.info('Alerts: %s',len(alerts))
        for m in messages(alerts):await send(session,m)
        if not alerts:logging.info('No new RSI zone-entry alerts.')

if __name__=='__main__':
    try: asyncio.run(main())
    except Exception: logging.exception('FATAL ERROR'); raise
