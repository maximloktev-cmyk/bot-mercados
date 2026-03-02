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

# ── Sectores con viento geopolítico a favor (marzo 2026) ────────────────────
# IA/Semis: boom de IA, gasto masivo en infraestructura
# Defensa: tensiones Rusia-Ucrania, Mar Rojo, Taiwan
# Cripto: Bitcoin en máximos históricos, regulación favorable en EE.UU.
# Energía: tensiones Oriente Medio, sanciones Rusia
SECTOR_BOOST = {
    "NVDA": ("IA/Semis", 2),  "AMD":  ("IA/Semis", 2),  "AVGO": ("IA/Semis", 2),
    "MU":   ("IA/Semis", 2),  "SMCI": ("IA/Semis", 2),  "AMAT": ("IA/Semis", 1),
    "LRCX": ("IA/Semis", 1),  "KLAC": ("IA/Semis", 1),  "QCOM": ("IA/Semis", 1),
    "MSFT": ("IA/Cloud", 1),  "GOOGL":("IA/Cloud", 1),  "META": ("IA/Cloud", 1),
    "AMZN": ("IA/Cloud", 1),  "CRM":  ("IA/Cloud", 1),  "PLTR": ("IA/Cloud", 2),
    "LMT":  ("Defensa", 2),   "RTX":  ("Defensa", 2),   "NOC":  ("Defensa", 2),
    "GD":   ("Defensa", 2),   "BA":   ("Defensa", 1),   "LDOS": ("Defensa", 1),
    "HII":  ("Defensa", 1),   "AXON": ("Defensa", 1),
    "COIN": ("Cripto", 2),    "MSTR": ("Cripto", 2),    "HOOD": ("Cripto", 1),
    "RIOT": ("Cripto", 1),    "MARA": ("Cripto", 1),
    "XOM":  ("Energía", 1),   "CVX":  ("Energía", 1),   "OXY":  ("Energía", 1),
    "SLB":  ("Energía", 1),   "HAL":  ("Energía", 1),
}


# ── Universo de acciones ────────────────────────────────────────────────────

def get_stock_universe():
    try:
        import pandas as pd
        sp500  = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]["Symbol"].tolist()
        ndq100 = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]["Ticker"].tolist()
        extra  = ["PLTR", "COIN", "HOOD", "MSTR", "RIOT", "MARA", "SOFI",
                  "RKLB", "IONQ", "SMCI", "RIVN", "AXON", "LDOS", "HII"]
        stocks = list(set(sp500 + ndq100 + extra))
        return [s.replace(".", "-") for s in stocks]
    except Exception:
        return [
            "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AMD",
            "NFLX", "INTC", "CRM", "ADBE", "QCOM", "AVGO", "MU", "PLTR",
            "JPM", "BAC", "GS", "V", "MA", "COIN", "XOM", "CVX", "OXY",
            "LMT", "RTX", "NOC", "GD", "UNH", "JNJ", "WMT", "COST", "HD",
        ]

STOCKS = get_stock_universe()


# ── Indicadores técnicos ────────────────────────────────────────────────────

def calc_rsi(closes, period=14):
    delta = closes.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    return (100 - (100 / (1 + rs))).iloc[-1]


def calc_macd(closes):
    ema12  = closes.ewm(span=12).mean()
    ema26  = closes.ewm(span=26).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9).mean()
    return macd.iloc[-1], macd.iloc[-2], signal.iloc[-1], signal.iloc[-2]


def analyze_stock(ticker, df):
    try:
        closes  = df["Close"].squeeze()
        volumes = df["Volume"].squeeze()
        if len(closes) < 50:
            return None

        price     = float(closes.iloc[-1])
        prev      = float(closes.iloc[-2])
        rsi       = calc_rsi(closes)
        macd_now, macd_prev, sig_now, sig_prev = calc_macd(closes)
        high_20d  = float(closes.iloc[-21:-1].max())
        low_20d   = float(closes.iloc[-21:-1].min())
        sma20     = float(closes.iloc[-20:].mean())
        sma50     = float(closes.iloc[-50:].mean())
        vol_avg   = float(volumes.iloc[-21:-1].mean())
        vol_now   = float(volumes.iloc[-1])

        # ── Filtros obligatorios ────────────────────────────────────────────
        if rsi >= 70:          return None   # sobrecomprado, no entrar
        if price < sma20:      return None   # sin tendencia alcista corto plazo
        if price < sma50 * 0.97: return None # muy por debajo de media larga

        score   = 0
        signals = []
        sector_label = ""

        # ── Señales técnicas ────────────────────────────────────────────────

        # RSI favorable
        if rsi < 35:
            score += 2
            signals.append(f"RSI {rsi:.0f} — rebote desde sobreventa")
        elif 45 <= rsi <= 63:
            score += 1
            signals.append(f"RSI {rsi:.0f} — momentum saludable")

        # MACD cruce alcista
        if macd_prev < sig_prev and macd_now > sig_now:
            score += 3
            signals.append("MACD cruce alcista confirmado")

        # Rotura de resistencia (máximo 20 días) con cierre
        if price > high_20d:
            score += 3
            signals.append(f"Rotura resistencia ${high_20d:.2f}")

        # Rebote en soporte con recuperación
        if prev <= low_20d * 1.005 and price > prev * 1.005:
            score += 2
            signals.append(f"Rebote en soporte ${low_20d:.2f}")

        # Precio sobre ambas medias móviles (tendencia sólida)
        if price > sma20 and price > sma50:
            score += 1
            signals.append(f"Sobre SMA20 y SMA50")

        # Volumen elevado (confirma movimiento)
        if vol_avg > 0 and vol_now > vol_avg * 1.5:
            score += 2
            signals.append(f"Volumen {vol_now/vol_avg:.1f}x — señal fuerte")

        # ── Boost geopolítico ────────────────────────────────────────────────
        if ticker in SECTOR_BOOST:
            label, boost = SECTOR_BOOST[ticker]
            sector_label = label
            score += boost
            signals.append(f"Sector favorecido: {label}")

        # ── Filtro mínimo: al menos 2 señales técnicas reales ────────────────
        tech_signals = [s for s in signals if "Sector" not in s and "SMA" not in s]
        if len(tech_signals) < 2 or score < 5:
            return None

        return {
            "ticker":  ticker,
            "score":   score,
            "signals": signals,
            "price":   price,
            "rsi":     rsi,
            "sector":  sector_label,
        }
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
        return (
            "No se encontraron acciones que cumplan todos los criterios hoy.\n"
            "El mercado puede estar en pausa o sobrecomprado en general."
        )

    context = (
        "Contexto geopolítico considerado:\n"
        "• IA/Semis: gasto masivo en infraestructura IA\n"
        "• Defensa: tensiones Rusia, Mar Rojo, Taiwán\n"
        "• Cripto: BTC en máximos, regulación favorable EE.UU.\n"
        "• Energía: tensiones Oriente Medio\n"
    )

    lines = [f"{titulo} ({len(STOCKS)} acciones analizadas):\n", context, "─" * 30 + "\n"]

    for i, r in enumerate(recs, 1):
        sigs = "\n   • ".join(r["signals"])
        lines.append(
            f"{i}. {r['ticker']}  ${r['price']:,.2f}  |  RSI {r['rsi']:.0f}  |  Score {r['score']}\n"
            f"   • {sigs}\n"
        )

    lines.append("─" * 30)
    lines.append("Aviso: no es asesoramiento financiero. Gestiona tu riesgo.")
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
            close  = float(values[6])
            open_  = float(values[3])
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
        sp500,  sp500_change  = await get_stooq_price("^spx")
        gold,   gold_change   = await get_stooq_price("gc.f")

        def fmt(price, change, prefix="$", suffix=""):
            if price is None:
                return "Mercado cerrado"
            return f"{prefix}{price:,.2f}{suffix} ({change:+.2f}%)"

        texto = (
            f"Resumen en tiempo real:\n\n"
            f"• Bitcoin: {fmt(btc_price, btc_change)} (24h)\n"
            f"• Nasdaq:  {fmt(nasdaq, nasdaq_change, prefix='', suffix=' pts')}\n"
            f"• S&P 500: {fmt(sp500, sp500_change, prefix='', suffix=' pts')}\n"
            f"• Oro:     {fmt(gold, gold_change)}/oz\n"
        )
    except Exception as e:
        texto = f"Error obteniendo datos: {e}"
    await message.answer(texto)


@dp.message(Command("acciones"))
async def acciones(message: types.Message):
    await message.answer(f"Analizando {len(STOCKS)} acciones, espera 30-60 segundos...")
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
    msg  = format_recommendations(recs, titulo="Recomendaciones del día")
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
