from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import os
import aiohttp
import yfinance as yf

TOKEN = os.environ.get("BOT_TOKEN", "8616657604:AAGQI9e_x9ZX5zw6zcHIloboeDO18OrKRBM")

bot = Bot(token=TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler(timezone="America/New_York")

HEADERS = {"User-Agent": "Mozilla/5.0"}
subscribers = set()

STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AMD",
    "NFLX", "INTC", "CRM", "ADBE", "QCOM", "AVGO", "MU", "PLTR",
    "JPM", "BAC", "GS", "V", "MA", "COIN",
    "XOM", "CVX", "OXY", "UNH", "JNJ", "PFE",
    "WMT", "COST", "HD", "DIS", "SBUX", "NKE",
    "MSTR", "HOOD", "SOFI", "RKLB", "IONQ", "SMCI"
]


# ── Indicadores técnicos ────────────────────────────────────────────────────

def calc_rsi(closes, period=14):
    delta = closes.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    return (100 - (100 / (1 + rs))).iloc[-1]


def calc_macd(closes):
    ema12 = closes.ewm(span=12).mean()
    ema26 = closes.ewm(span=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    return macd.iloc[-1], macd.iloc[-2], signal.iloc[-1], signal.iloc[-2]


def analyze_stock(ticker, df):
    try:
        closes = df["Close"].squeeze()
        volumes = df["Volume"].squeeze()
        if len(closes) < 30:
            return None

        price = float(closes.iloc[-1])
        rsi = calc_rsi(closes)
        macd_now, macd_prev, sig_now, sig_prev = calc_macd(closes)
        high_20d = float(closes.iloc[-21:-1].max())
        low_20d = float(closes.iloc[-21:-1].min())
        vol_avg = float(volumes.iloc[-21:-1].mean())
        vol_now = float(volumes.iloc[-1])
        sma50 = float(closes.iloc[-50:].mean()) if len(closes) >= 50 else float(closes.mean())

        score = 0
        signals = []

        # RSI
        if rsi < 32:
            score += 2
            signals.append(f"RSI {rsi:.0f} (sobreventa)")
        elif rsi < 45:
            score += 1

        # MACD cruce alcista
        if macd_prev < sig_prev and macd_now > sig_now:
            score += 3
            signals.append("MACD cruce alcista")

        # Rotura de resistencia (máximo 20 días)
        if price > high_20d:
            score += 3
            signals.append(f"Rotura resistencia ${high_20d:.2f}")

        # Rebote en soporte (mínimo 20 días)
        prev_close = float(closes.iloc[-2])
        if prev_close <= low_20d * 1.01 and price > prev_close:
            score += 2
            signals.append(f"Rebote en soporte ${low_20d:.2f}")

        # Volumen elevado
        if vol_avg > 0 and vol_now > vol_avg * 1.5:
            score += 1
            signals.append(f"Volumen {vol_now/vol_avg:.1f}x promedio")

        # Precio sobre SMA50
        if price > sma50:
            score += 1

        if score < 3 or not signals:
            return None

        return {"ticker": ticker, "score": score, "signals": signals, "price": price, "rsi": rsi}
    except Exception:
        return None


async def get_recommendations():
    def fetch():
        return yf.download(
            STOCKS, period="60d", interval="1d",
            progress=False, group_by="ticker", auto_adjust=True
        )

    data = await asyncio.to_thread(fetch)

    results = []
    for ticker in STOCKS:
        try:
            df = data[ticker].dropna()
            result = analyze_stock(ticker, df)
            if result:
                results.append(result)
        except Exception:
            continue

    return sorted(results, key=lambda x: x["score"], reverse=True)[:5]


def format_recommendations(recs, titulo="Recomendaciones corto plazo"):
    if not recs:
        return "No se encontraron señales fuertes en este momento."
    lines = [f"{titulo}:\n"]
    for i, r in enumerate(recs, 1):
        sigs = "\n   • ".join(r["signals"])
        lines.append(
            f"{i}. {r['ticker']} — ${r['price']:,.2f}  (RSI {r['rsi']:.0f})\n"
            f"   • {sigs}\n"
        )
    lines.append("\nAviso: esto no es asesoramiento financiero.")
    return "\n".join(lines)


# ── Helpers de precios ──────────────────────────────────────────────────────

async def get_btc_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            data = await r.json()
            return data["bitcoin"]["usd"], data["bitcoin"]["usd_24h_change"]


async def get_stooq_price(ticker):
    url = f"https://stooq.com/q/l/?s={ticker}&f=sd2t2ohlcv&h&e=csv"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            text = await r.text()
            lines = text.strip().split("\n")
            values = lines[1].split(",")
            if "N/D" in values:
                return None, None
            close = float(values[6])
            open_ = float(values[3])
            change = ((close - open_) / open_) * 100
            return close, change


# ── Comandos ────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer(
        "Hola! Soy tu analista de mercados.\n\n"
        "Comandos:\n"
        "/analisis — Precios en tiempo real\n"
        "/acciones — Recomendaciones ahora\n"
        "/suscribir — Alertas diarias a las 9:30 AM NY\n"
        "/cancelar — Cancelar alertas"
    )


@dp.message(Command("analisis"))
async def analisis(message: types.Message):
    await message.answer("Obteniendo datos en tiempo real...")
    try:
        btc_price, btc_change = await get_btc_price()
        nasdaq, nasdaq_change = await get_stooq_price("^ixic")
        sp500, sp500_change = await get_stooq_price("^spx")
        gold, gold_change = await get_stooq_price("gc.f")

        def fmt(price, change, prefix="$", suffix=""):
            if price is None:
                return "Mercado cerrado"
            return f"{prefix}{price:,.2f}{suffix} ({change:+.2f}%)"

        texto = (
            f"Resumen en tiempo real:\n\n"
            f"• Bitcoin: {fmt(btc_price, btc_change)} (24h)\n"
            f"• Nasdaq: {fmt(nasdaq, nasdaq_change, prefix='', suffix=' pts')}\n"
            f"• S&P 500: {fmt(sp500, sp500_change, prefix='', suffix=' pts')}\n"
            f"• Oro: {fmt(gold, gold_change)}/oz\n"
        )
    except Exception as e:
        texto = f"Error obteniendo datos: {e}"
    await message.answer(texto)


@dp.message(Command("acciones"))
async def acciones(message: types.Message):
    await message.answer("Analizando mercado, puede tardar 20-30 segundos...")
    recs = await get_recommendations()
    await message.answer(format_recommendations(recs))


@dp.message(Command("suscribir"))
async def suscribir(message: types.Message):
    subscribers.add(message.chat.id)
    await message.answer("Suscrito. Recibirás recomendaciones cada día a las 9:30 AM (hora Nueva York).")


@dp.message(Command("cancelar"))
async def cancelar(message: types.Message):
    subscribers.discard(message.chat.id)
    await message.answer("Suscripción cancelada.")


# ── Job diario ───────────────────────────────────────────────────────────────

async def send_daily_recommendations():
    if not subscribers:
        return
    recs = await get_recommendations()
    msg = format_recommendations(recs, titulo="Recomendaciones del día")
    for chat_id in list(subscribers):
        try:
            await bot.send_message(chat_id, msg)
        except Exception:
            pass


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    scheduler.add_job(send_daily_recommendations, "cron", hour=9, minute=30)
    scheduler.start()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
