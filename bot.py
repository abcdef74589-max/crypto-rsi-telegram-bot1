import os, asyncio, logging
from datetime import datetime, timezone
import aiohttp
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TOP_N = int(os.getenv("TOP_N", "100"))
PERIOD = int(os.getenv("RSI_PERIOD", "14"))
MARKET = os.getenv("MARKET", "spot").lower()
ALERT_MODE = os.getenv("ALERT_MODE", "changes").lower()

CG = "https://api.coingecko.com/api/v3/coins/markets"
EX_SPOT = "https://api.binance.com/api/v3/exchangeInfo"
EX_FUT = "https://fapi.binance.com/fapi/v1/exchangeInfo"
TK_SPOT = "https://api.binance.com/api/v3/ticker/24hr"
TK_FUT = "https://fapi.binance.com/fapi/v1/ticker/24hr"
KL_SPOT = "https://api.binance.com/api/v3/klines"
KL_FUT = "https://fapi.binance.com/fapi/v1/klines"

TFS = {"15m": "15m", "1h": "1h", "4h": "4h", "1D": "1d"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def rsi_series(closes, p=14):
    if len(closes) <= p:
        return []
    d = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [max(x, 0.0) for x in d]
    losses = [max(-x, 0.0) for x in d]

    ag = sum(gains[:p]) / p
    al = sum(losses[:p]) / p
    out = []

    def calc():
        if al == 0:
            return 100.0 if ag > 0 else 50.0
        return 100.0 - (100.0 / (1.0 + ag / al))

    out.append(calc())
    for i in range(p, len(d)):
        ag = ((p - 1) * ag + gains[i]) / p
        al = ((p - 1) * al + losses[i]) / p
        out.append(calc())
    return out

def zone(v):
    if v < 30:
        return "oversold"
    if v > 70:
        return "overbought"
    return "normal"

def volfmt(v):
    if v >= 1e9:
        return f"${v/1e9:.2f}B"
    if v >= 1e6:
        return f"${v/1e6:.2f}M"
    if v >= 1e3:
        return f"${v/1e3:.2f}K"
    return f"${v:.0f}"

def tv(sym):
    return f"https://www.tradingview.com/chart/?symbol=BINANCE%3A{sym}"

async def get_json(s, url, params=None):
    for n in range(4):
        try:
            async with s.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                if r.status == 429:
                    await asyncio.sleep(3 + n * 3)
                    continue
                r.raise_for_status()
                return await r.json()
        except Exception:
            if n == 3:
                raise
            await asyncio.sleep(1 + n)

async def main():
    if not TOKEN or not CHAT_ID:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID.")

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=50)
    ) as s:
        # Top coins by market cap
        coins = await get_json(
            s, CG,
            {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": TOP_N,
                "page": 1,
                "sparkline": "false"
            }
        )

        # Binance symbols currently tradable against USDT
        ex = await get_json(s, EX_FUT if MARKET == "futures" else EX_SPOT)
        allowed = {
            x["symbol"]
            for x in ex["symbols"]
            if x.get("quoteAsset") == "USDT"
            and x.get("status") == "TRADING"
        }

        # 24h price + quote volume
        tick = await get_json(s, TK_FUT if MARKET == "futures" else TK_SPOT)
        tick = {
            x["symbol"]: {
                "p": float(x.get("lastPrice", 0)),
                "v": float(x.get("quoteVolume", 0))
            }
            for x in tick
        }

        symbols = []
        for c in coins:
            base = (c.get("symbol") or "").upper()
            sym = base + "USDT"
            if sym in allowed and sym not in symbols:
                symbols.append(sym)
            if len(symbols) >= TOP_N:
                break

        sem = asyncio.Semaphore(25)

        async def scan_symbol(sym):
            async with sem:
                results = []

                async def scan_tf(label, interval):
                    rows = await get_json(
                        s,
                        KL_FUT if MARKET == "futures" else KL_SPOT,
                        {
                            "symbol": sym,
                            "interval": interval,
                            "limit": PERIOD + 125
                        }
                    )

                    # Binance returns the current candle as the last row.
                    # Ignore it so alerts are based only on fully closed candles.
                    closed = rows[:-1]
                    closes = [float(x[4]) for x in closed]
                    rsis = rsi_series(closes, PERIOD)

                    if len(rsis) < 2:
                        return None

                    latest_rsi = rsis[-1]
                    previous_rsi = rsis[-2]
                    latest_candle = closed[-1]

                    return {
                        "tf": label,
                        "rsi": latest_rsi,
                        "prev_rsi": previous_rsi,
                        "close_time": int(latest_candle[6]),
                        "close_price": float(latest_candle[4]),
                    }

                vals = await asyncio.gather(
                    *(scan_tf(a, b) for a, b in TFS.items()),
                    return_exceptions=True
                )

                for v in vals:
                    if isinstance(v, dict):
                        latest_zone = zone(v["rsi"])
                        previous_zone = zone(v["prev_rsi"])

                        if latest_zone in ("oversold", "overbought"):
                            if ALERT_MODE == "every_close":
                                results.append(v)
                            elif latest_zone != previous_zone:
                                results.append(v)

                return sym, results

        results = await asyncio.gather(*(scan_symbol(x) for x in symbols))

    alerts = []
    for sym, vals in results:
        for v in vals:
            alerts.append({
                "sym": sym,
                **v,
                "volume": tick.get(sym, {}).get("v", 0),
                "price": tick.get(sym, {}).get("p", 0),
                "zone": zone(v["rsi"])
            })

    # Strongest conditions first
    alerts.sort(
        key=lambda x: x["rsi"],
        reverse=True
    )

    if not alerts:
        logging.info(
            "No new RSI alerts. symbols=%d mode=%s",
            len(symbols), ALERT_MODE
        )
        return

    lines = [
        f"🚨 RSI Alert — Top {TOP_N}",
        f"🔎 Binance {MARKET.upper()} | RSI({PERIOD})",
        f"🕒 Scan: {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}",
        ""
    ]

    oversold = [x for x in alerts if x["zone"] == "oversold"]
    overbought = [x for x in alerts if x["zone"] == "overbought"]

    def add_section(title, items):
        if not items:
            return
        lines.append(title)
        for x in items:
            ct = datetime.fromtimestamp(
                x["close_time"] / 1000,
                tz=timezone.utc
            )
            lines.append(
                f"• {x['sym']} | {x['tf']} | RSI {x['rsi']:.2f}"
            )
            lines.append(
                f"  💰 Price ${x['price']:g} | 24h Vol {volfmt(x['volume'])}"
            )
            lines.append(
                f"  🕯 Closed {ct:%Y-%m-%d %H:%M UTC}"
            )
            lines.append(f"  📈 TradingView: {tv(x['sym'])}")
            lines.append("")

    add_section("🟢 RSI < 30 — Oversold", oversold)
    add_section("🔴 RSI > 70 — Overbought", overbought)

    text = "\n".join(lines).strip()

    # Telegram message limit is 4096 chars.
    chunks = []
    while len(text) > 3900:
        cut = text.rfind("\n", 0, 3900)
        if cut < 1000:
            cut = 3900
        chunks.append(text[:cut])
        text = text[cut:].lstrip()
    chunks.append(text)

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as s:
        for chunk in chunks:
            async with s.post(
                url,
                json={
                    "chat_id": CHAT_ID,
                    "text": chunk,
                    "disable_web_page_preview": True
                },
                timeout=20
            ) as r:
                if r.status >= 400:
                    raise RuntimeError(await r.text())

    logging.info(
        "Alerts sent: symbols=%d alerts=%d oversold=%d overbought=%d",
        len(symbols), len(alerts), len(oversold), len(overbought)
    )

if __name__ == "__main__":
    asyncio.run(main())
