from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import os
import aiohttp
import yfinance as yf

TOKEN         = os.environ.get("BOT_TOKEN",     "8616657604:AAGQI9e_x9ZX5zw6zcHIloboeDO18OrKRBM")
FINNHUB_KEY   = os.environ.get("FINNHUB_KEY",   "d6j133pr01qleu95u19gd6j133pr01qleu95u1a0")
NEWSDATA_KEY  = os.environ.get("NEWSDATA_KEY",  "pub_4cbb2798d21e439186e168313963b1bb")

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

STOCKS = list(set([
    # IA / Semiconductores
    "NVDA", "AMD", "AVGO", "QCOM", "MU", "INTC", "AMAT", "LRCX", "KLAC",
    "MRVL", "NXPI", "ON", "TXN", "SMCI", "IONQ", "RKLB", "WOLF", "ENPH",
    "SWKS", "MPWR", "ENTG", "ONTO", "ACLS", "COHU", "AMBA", "SLAB",
    # Big Tech / Cloud / Software
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "ORCL", "IBM",
    "CRM", "ADBE", "NOW", "INTU", "SNPS", "CDNS", "PLTR", "WDAY",
    "TEAM", "DDOG", "ZS", "CRWD", "NET", "OKTA", "MDB", "SNOW",
    "ZM", "DOCU", "BILL", "HUBS", "SMAR", "GTLB", "ESTC", "APPN",
    "CFLT", "DXCM", "VEEV", "COUP", "PAYC", "PCTY", "SAIC", "CACI",
    # Fintech / Financiero
    "V", "MA", "PYPL", "SQ", "AFRM", "SOFI", "HOOD", "COIN",
    "JPM", "BAC", "GS", "MS", "WFC", "C", "AXP", "BLK", "SCHW",
    "USB", "PNC", "TFC", "COF", "DFS", "SYF", "ALLY", "LC",
    "ICE", "CME", "CBOE", "NDAQ", "MKTX", "LPLA", "RJF", "SF",
    "BX", "KKR", "APO", "CG", "ARES", "OWL",
    # Defensa / Aeroespacial
    "LMT", "RTX", "NOC", "GD", "BA", "HII", "LDOS", "AXON",
    "L3H", "TDG", "HEI", "KTOS", "RCAT", "ACHR", "JOBY",
    # Energía
    "XOM", "CVX", "OXY", "SLB", "HAL", "COP", "EOG", "MPC", "VLO", "PSX",
    "PXD", "DVN", "FANG", "MRO", "HES", "APA", "NOV", "BKR",
    "KMI", "WMB", "OKE", "ET", "MPLX", "EPD",
    # Salud / Biotech / Pharma
    "UNH", "JNJ", "PFE", "ABBV", "MRK", "LLY", "BMY", "AMGN", "GILD",
    "REGN", "VRTX", "MRNA", "BIIB", "DXCM", "ISRG", "BSX", "EW",
    "ZBH", "SYK", "MDT", "ABT", "BAX", "BDX", "IQV", "CRL",
    "ILMN", "PACB", "NVAX", "BNTX", "ARWR", "BEAM", "EDIT", "NTLA",
    "RXRX", "CRVS", "ACAD", "SAGE", "SRTX", "PTGX",
    # Consumo Discrecional
    "WMT", "COST", "HD", "TGT", "NKE", "SBUX", "MCD", "DIS",
    "NFLX", "ABNB", "BKNG", "MAR", "HLT", "LVS", "MGM", "WYNN",
    "F", "GM", "RIVN", "LCID", "RACE", "TM",
    "LOW", "BBY", "ETSY", "W", "CHWY", "CHEWY",
    "PG", "KO", "PEP", "PM", "MO", "MDLZ", "CL", "KMB", "CHD",
    # Retail / E-commerce
    "EBAY", "MELI", "SE", "CPNG", "SHOP",
    # Cripto / Blockchain
    "MSTR", "RIOT", "MARA", "CLSK", "BTBT", "HUT", "CIFR",
    # Industriales / Manufactura
    "CAT", "DE", "HON", "MMM", "GE", "ETN", "EMR", "PH", "ROK", "IR",
    "ITW", "DOV", "AME", "ROP", "FTV", "FBHS", "XYL", "IDEX", "IEX",
    "GWW", "MSC", "FAST", "SWK", "PNR", "GNRC",
    # Transporte / Logística
    "UPS", "FDX", "DAL", "UAL", "AAL", "LUV", "JBLU", "ALK",
    "CSX", "NSC", "UNP", "CP", "CNI",
    # Real Estate / REIT
    "AMT", "PLD", "CCI", "EQIX", "DLR", "SPG", "O", "VICI",
    # Utilities
    "NEE", "DUK", "SO", "AEP", "EXC", "SRE", "PCG", "ED",
    # Telecom / Media
    "T", "VZ", "TMUS", "CHTR", "CMCSA", "PARA", "WBD",
    # Materiales
    "LIN", "APD", "ECL", "DD", "DOW", "NUE", "STLD", "RS",
    "FCX", "NEM", "AEM", "GOLD", "WPM", "AA", "ALB", "MP",
    # ETFs
    "SPY", "QQQ", "IWM", "XLK", "XLF", "XLE", "XLV", "XLI",
    "XLB", "XLU", "XLRE", "XLY", "XLP", "GLD", "SLV", "USO",
    "ARKK", "ARKG", "ARKW",
]))


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
        if len(tech_signals) < 2 or score < 4:
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

    ranked = sorted(results, key=lambda x: x["score"], reverse=True)

    # Enriquecer top 15 candidatos con sentiment de Finnhub
    candidates = ranked[:15]
    sentiment_tasks = [get_finnhub_sentiment(r["ticker"]) for r in candidates]
    sentiments = await asyncio.gather(*sentiment_tasks)
    for r, (sent_score, sent_label) in zip(candidates, sentiments):
        r["score"] += sent_score
        if sent_label:
            r["signals"].append(sent_label)

    # Re-ordenar con sentiment incluido
    candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)

    # Máximo 2 acciones por sector para diversificar
    top = []
    sector_count = {}
    for r in candidates:
        sector = r.get("sector") or "General"
        if sector_count.get(sector, 0) < 2:
            top.append(r)
            sector_count[sector] = sector_count.get(sector, 0) + 1
        if len(top) == 5:
            break

    # Actualizar precios con Finnhub (tiempo real)
    for r in top:
        try:
            q = await get_finnhub_quote(r["ticker"])
            if q:
                r["price"]  = q["price"]
                r["change"] = q["change"]
                r["high"]   = q["high"]
                r["low"]    = q["low"]
        except Exception:
            pass

    return top


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
        sigs   = "\n   • ".join(r["signals"])
        change = f"({r['change']:+.2f}%)" if r.get("change") is not None else ""
        high   = f"  Max ${r['high']:,.2f}  Min ${r['low']:,.2f}" if r.get("high") else ""
        lines.append(
            f"{i}. {r['ticker']}  ${r['price']:,.2f} {change}  |  RSI {r['rsi']:.0f}  |  Score {r['score']}\n"
            f"  {high}\n"
            f"   • {sigs}\n"
        )

    lines.append("─" * 30)
    lines.append("Aviso: no es asesoramiento financiero. Gestiona tu riesgo.")
    return "\n".join(lines)


# ── Helpers de precios ──────────────────────────────────────────────────────

async def get_finnhub_quote(ticker):
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            d = await r.json()
            if not d.get("c"):
                return None
            return {
                "price":   d["c"],
                "change":  d["dp"],
                "high":    d["h"],
                "low":     d["l"],
                "open":    d["o"],
                "prev":    d["pc"],
            }


async def get_finnhub_sentiment(ticker):
    """Retorna (score, label): score >0 bullish, <0 bearish"""
    url = f"https://finnhub.io/api/v1/news-sentiment?symbol={ticker}&token={FINNHUB_KEY}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as r:
                d = await r.json()
                score = d.get("companyNewsScore", 0)
                buzz  = d.get("buzz", {}).get("weeklyAverage", 0)
                sent  = d.get("sentiment", {}).get("bullishPercent", 0.5)
                if sent > 0.6:
                    return 2, "Sentimiento positivo en noticias"
                elif sent < 0.4:
                    return -2, "Sentimiento negativo en noticias"
                return 0, None
    except Exception:
        return 0, None


async def get_market_news():
    """Noticias de mercado y geopolítica vía NewsData.io"""
    results = {}
    async with aiohttp.ClientSession() as session:
        # Noticias financieras
        url_fin = (
            f"https://newsdata.io/api/v1/latest"
            f"?apikey={NEWSDATA_KEY}&language=en&category=business"
            f"&q=stocks+market+earnings&size=5"
        )
        # Noticias geopolíticas
        url_geo = (
            f"https://newsdata.io/api/v1/latest"
            f"?apikey={NEWSDATA_KEY}&language=en&category=politics,world"
            f"&q=Ukraine+Russia+Taiwan+OPEC+Fed+rates&size=5"
        )
        try:
            async with session.get(url_fin, headers=HEADERS) as r:
                d = await r.json()
                results["mercado"] = [
                    a["title"] for a in d.get("results", [])[:5] if a.get("title")
                ]
        except Exception:
            results["mercado"] = []
        try:
            async with session.get(url_geo, headers=HEADERS) as r:
                d = await r.json()
                results["geopolitica"] = [
                    a["title"] for a in d.get("results", [])[:5] if a.get("title")
                ]
        except Exception:
            results["geopolitica"] = []
    return results


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
        "/analisis — Índices y Bitcoin en tiempo real\n"
        "/precio TICKER — Precio en tiempo real de cualquier acción\n"
        "/acciones — Top 5 recomendaciones ahora\n"
        "/noticias — Titulares de mercado y geopolítica\n"
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


@dp.message(Command("precio"))
async def precio(message: types.Message):
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("Uso: /precio TICKER\nEjemplo: /precio AAPL")
        return
    ticker = parts[1].upper()
    q = await get_finnhub_quote(ticker)
    if not q:
        await message.answer(f"No se encontró cotización para {ticker}.")
        return
    await message.answer(
        f"{ticker} — Tiempo real (Finnhub)\n\n"
        f"• Precio:    ${q['price']:,.2f}  ({q['change']:+.2f}%)\n"
        f"• Apertura:  ${q['open']:,.2f}\n"
        f"• Máx/Mín:   ${q['high']:,.2f} / ${q['low']:,.2f}\n"
        f"• Cierre ant: ${q['prev']:,.2f}\n"
    )


@dp.message(Command("noticias"))
async def noticias(message: types.Message):
    await message.answer("Obteniendo noticias...")
    news = await get_market_news()

    lines = ["Noticias del mercado:\n"]
    if news.get("mercado"):
        lines.append("Mercado / Empresas:")
        for t in news["mercado"]:
            lines.append(f"• {t}")
    lines.append("")
    if news.get("geopolitica"):
        lines.append("Geopolítica:")
        for t in news["geopolitica"]:
            lines.append(f"• {t}")

    await message.answer("\n".join(lines) if len(lines) > 2 else "No se encontraron noticias.")


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
