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

bot       = Bot(token=TOKEN)
dp        = Dispatcher()
scheduler = AsyncIOScheduler(timezone="America/New_York")

HEADERS       = {"User-Agent": "Mozilla/5.0"}
subscribers   = set()
sent_articles = set()   # evitar enviar la misma alerta dos veces

# ── Palabras clave de noticias de alto impacto ───────────────────────────────
IMPACT_KEYWORDS = [
    "earnings beat", "earnings miss", "supera estimaciones", "resultados record",
    "acquisition", "merger", "adquisición", "fusión", "compra por",
    "fda approval", "aprobación fda", "fda rejects",
    "bankruptcy", "quiebra", "chapter 11",
    "layoffs", "despidos", "recorte de empleos",
    "rate hike", "rate cut", "subida de tipos", "bajada de tipos",
    "fed decision", "decisión fed", "powell",
    "opec", "opep", "recorte de producción", "aumento de producción",
    "ukraine", "ucrania", "taiwan", "taiwán", "sanctions", "sanciones",
    "nuclear", "war escalation", "escalada",
    "stock split", "división de acciones",
    "buyback", "recompra de acciones",
    "revenue guidance", "previsión ingresos", "profit warning",
    "sec investigation", "investigación sec", "fraud", "fraude",
]

# ── Sectores con viento geopolítico a favor ──────────────────────────────────
SECTOR_BOOST = {
    "NVDA": ("IA/Semis", 2),  "AMD":  ("IA/Semis", 2),  "AVBO": ("IA/Semis", 2),
    "MU":   ("IA/Semis", 2),  "SMCI": ("IA/Semis", 2),  "AMAT": ("IA/Semis", 1),
    "LRCX": ("IA/Semis", 1),  "KLAC": ("IA/Semis", 1),  "QCOM": ("IA/Semis", 1),
    "AVGO": ("IA/Semis", 2),
    "MSFT": ("IA/Cloud", 1),  "GOOGL":("IA/Cloud", 1),  "META": ("IA/Cloud", 1),
    "AMZN": ("IA/Cloud", 1),  "CRM":  ("IA/Cloud", 1),  "PLTR": ("IA/Cloud", 2),
    "LMT":  ("Defensa", 2),   "RTX":  ("Defensa", 2),   "NOC":  ("Defensa", 2),
    "GD":   ("Defensa", 2),   "BA":   ("Defensa", 1),   "LDOS": ("Defensa", 1),
    "HII":  ("Defensa", 1),   "AXON": ("Defensa", 1),   "KTOS": ("Defensa", 1),
    "COIN": ("Cripto", 2),    "MSTR": ("Cripto", 2),    "HOOD": ("Cripto", 1),
    "RIOT": ("Cripto", 1),    "MARA": ("Cripto", 1),
    "XOM":  ("Energía", 1),   "CVX":  ("Energía", 1),   "OXY":  ("Energía", 1),
    "SLB":  ("Energía", 1),   "HAL":  ("Energía", 1),   "VLO":  ("Energía", 1),
    "MPC":  ("Energía", 1),   "PSX":  ("Energía", 1),
}

# ── Universo de acciones (~250 tickers) ─────────────────────────────────────
STOCKS = list(set([
    "NVDA","AMD","AVGO","QCOM","MU","INTC","AMAT","LRCX","KLAC","MRVL","NXPI",
    "ON","TXN","SMCI","IONQ","RKLB","WOLF","ENPH","SWKS","MPWR","ENTG","AMBA",
    "AAPL","MSFT","GOOGL","AMZN","META","TSLA","ORCL","IBM","CRM","ADBE","NOW",
    "INTU","SNPS","CDNS","PLTR","WDAY","TEAM","DDOG","ZS","CRWD","NET","OKTA",
    "MDB","SNOW","ZM","DOCU","BILL","HUBS","GTLB","ESTC","CFLT","VEEV","PAYC",
    "PCTY","CACI","SAIC",
    "V","MA","PYPL","SQ","AFRM","SOFI","HOOD","COIN","JPM","BAC","GS","MS",
    "WFC","C","AXP","BLK","SCHW","USB","PNC","TFC","COF","DFS","SYF","ALLY",
    "ICE","CME","CBOE","BX","KKR","APO","ARES",
    "LMT","RTX","NOC","GD","BA","HII","LDOS","AXON","KTOS","TDG","HEI",
    "XOM","CVX","OXY","SLB","HAL","COP","EOG","MPC","VLO","PSX","DVN",
    "MRO","HES","APA","BKR","KMI","WMB","OKE",
    "UNH","JNJ","PFE","ABBV","MRK","LLY","BMY","AMGN","GILD","REGN","VRTX",
    "MRNA","BIIB","ISRG","BSX","EW","SYK","MDT","ABT","BDX","IQV",
    "ILMN","NVAX","BNTX","ARWR","RXRX",
    "WMT","COST","HD","TGT","NKE","SBUX","MCD","DIS","NFLX","ABNB","BKNG",
    "MAR","HLT","LVS","MGM","F","GM","RIVN","LOW","ETSY","SHOP","MELI",
    "PG","KO","PEP","PM","MO","MDLZ","CL",
    "MSTR","RIOT","MARA","CLSK","BTBT",
    "CAT","DE","HON","MMM","GE","ETN","EMR","PH","ROK","ITW","DOV","AME",
    "ROP","GWW","FAST","SWK","GNRC",
    "UPS","FDX","DAL","UAL","CSX","NSC","UNP",
    "AMT","PLD","EQIX","DLR","SPG","O",
    "NEE","DUK","SO","AEP","EXC",
    "T","VZ","TMUS","CMCSA",
    "LIN","FCX","NEM","GOLD","ALB","MP","AA",
    "SPY","QQQ","IWM","XLK","XLF","XLE","XLV","XLI","ARKK",
]))


# ── Indicadores técnicos ─────────────────────────────────────────────────────

def calc_rsi(closes, period=14):
    delta = closes.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss
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

        price    = float(closes.iloc[-1])
        prev     = float(closes.iloc[-2])
        rsi      = calc_rsi(closes)
        macd_now, macd_prev, sig_now, sig_prev = calc_macd(closes)
        high_20d = float(closes.iloc[-21:-1].max())
        low_20d  = float(closes.iloc[-21:-1].min())
        sma20    = float(closes.iloc[-20:].mean())
        sma50    = float(closes.iloc[-50:].mean())
        vol_avg  = float(volumes.iloc[-21:-1].mean())
        vol_now  = float(volumes.iloc[-1])

        # Filtros obligatorios
        if rsi >= 70:             return None
        if price < sma20:         return None
        if price < sma50 * 0.97:  return None

        score        = 0
        tech_signals = []
        sector_label = ""

        if rsi < 35:
            score += 2
            tech_signals.append(f"RSI {rsi:.0f} — rebote desde sobreventa")
        elif 45 <= rsi <= 63:
            score += 1
            tech_signals.append(f"RSI {rsi:.0f} — momentum saludable")

        if macd_prev < sig_prev and macd_now > sig_now:
            score += 3
            tech_signals.append("MACD cruce alcista")

        if price > high_20d:
            score += 3
            tech_signals.append(f"Rotura resistencia ${high_20d:.2f}")

        if prev <= low_20d * 1.005 and price > prev * 1.005:
            score += 2
            tech_signals.append(f"Rebote en soporte ${low_20d:.2f}")

        if price > sma20 and price > sma50:
            score += 1
            tech_signals.append("Precio sobre SMA20 y SMA50")

        if vol_avg > 0 and vol_now > vol_avg * 1.5:
            score += 2
            tech_signals.append(f"Volumen {vol_now/vol_avg:.1f}x promedio")

        if ticker in SECTOR_BOOST:
            sector_label, boost = SECTOR_BOOST[ticker]
            score += boost

        if len(tech_signals) < 2 or score < 4:
            return None

        # Stop loss sugerido: bajo de 20d o SMA20, lo que esté más cerca del precio
        stop = max(low_20d, sma20 * 0.98)

        return {
            "ticker":       ticker,
            "score":        score,
            "tech_signals": tech_signals,
            "social":       [],
            "news_label":   None,
            "price":        price,
            "rsi":          rsi,
            "sector":       sector_label,
            "stop":         stop,
            "high_20d":     high_20d,
        }
    except Exception:
        return None


# ── Sentiment ────────────────────────────────────────────────────────────────

async def get_stocktwits_sentiment(ticker):
    url = f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as r:
                if r.status != 200:
                    return None
                d        = await r.json()
                messages = d.get("messages", [])
                bullish  = sum(1 for m in messages
                               if m.get("entities", {}).get("sentiment", {}) and
                               m["entities"]["sentiment"].get("basic") == "Bullish")
                bearish  = sum(1 for m in messages
                               if m.get("entities", {}).get("sentiment", {}) and
                               m["entities"]["sentiment"].get("basic") == "Bearish")
                total    = bullish + bearish
                count    = len(messages)
                bull_pct = (bullish / total * 100) if total > 0 else 50
                return {"bull_pct": bull_pct, "count": count, "total": total,
                        "bullish": bullish, "bearish": bearish}
    except Exception:
        return None


async def get_reddit_mentions(ticker):
    url = (f"https://www.reddit.com/r/wallstreetbets+stocks+investing"
           f"/search.json?q={ticker}&sort=new&limit=25&t=day")
    headers = {**HEADERS, "User-Agent": "MarketBot/1.0"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as r:
                d       = await r.json()
                posts   = d.get("data", {}).get("children", [])
                upvotes = sum(p["data"].get("score", 0) for p in posts)
                return len(posts), upvotes
    except Exception:
        return 0, 0


async def get_finnhub_sentiment(ticker):
    url = f"https://finnhub.io/api/v1/news-sentiment?symbol={ticker}&token={FINNHUB_KEY}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as r:
                d    = await r.json()
                sent = d.get("sentiment", {}).get("bullishPercent", 0.5)
                if sent > 0.6:   return 2, "Noticias positivas"
                elif sent < 0.4: return -2, "Noticias negativas"
                return 0, None
    except Exception:
        return 0, None


async def enrich_with_sentiment(r):
    """Agrega datos de sentimiento a un resultado de análisis"""
    ticker = r["ticker"]
    st, (reddit_posts, reddit_upvotes), (fh_score, fh_label) = await asyncio.gather(
        get_stocktwits_sentiment(ticker),
        get_reddit_mentions(ticker),
        get_finnhub_sentiment(ticker),
    )
    score_adj = fh_score

    if fh_label:
        r["news_label"] = fh_label

    if st and st["total"] >= 5:
        if st["bull_pct"] >= 65:
            score_adj += 2
            r["social"].append(f"StockTwits {st['bull_pct']:.0f}% alcista ({st['bullish']}/{st['total']})")
        elif st["bull_pct"] <= 35:
            score_adj -= 2
            r["social"].append(f"StockTwits {100-st['bull_pct']:.0f}% bajista")
        if st["count"] >= 20 and (st["bull_pct"] >= 70 or st["bull_pct"] <= 30):
            score_adj += 1
            r["social"].append(f"Buzz inusual ({st['count']} mensajes)")

    if reddit_posts >= 5:
        score_adj += 1
        r["social"].append(f"Trending Reddit ({reddit_posts} posts, {reddit_upvotes:,} upvotes)")
    elif reddit_posts >= 3:
        r["social"].append(f"Reddit ({reddit_posts} posts)")

    r["score"] += score_adj
    return r


# ── Recomendaciones ──────────────────────────────────────────────────────────

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
            df     = data[ticker].dropna()
            result = analyze_stock(ticker, df)
            if result:
                results.append(result)
        except Exception:
            continue

    ranked     = sorted(results, key=lambda x: x["score"], reverse=True)
    candidates = ranked[:15]

    # Enriquecer con sentiment en paralelo
    enriched = await asyncio.gather(*[enrich_with_sentiment(r) for r in candidates])
    enriched = sorted(enriched, key=lambda x: x["score"], reverse=True)

    # Máx 2 por sector
    top          = []
    sector_count = {}
    for r in enriched:
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
            "El mercado puede estar en pausa o sobrecomprado."
        )

    lines = [
        f"{titulo} — {len(STOCKS)} acciones analizadas\n",
        "Criterios: técnico + sentimiento + noticias + geopolítica\n",
        "━" * 28,
    ]

    for i, r in enumerate(recs, 1):
        change = f"({r['change']:+.2f}%)" if r.get("change") is not None else ""
        stop   = r.get("stop", r["price"] * 0.97)
        stop_pct = ((r["price"] - stop) / r["price"]) * 100
        sector = f"  [{r['sector']}]" if r.get("sector") else ""

        lines.append(
            f"\n{i}. {r['ticker']}{sector}\n"
            f"   Precio: ${r['price']:,.2f} {change}  |  RSI {r['rsi']:.0f}  |  Score {r['score']}"
        )

        lines.append("   TÉCNICO:")
        for s in r["tech_signals"]:
            lines.append(f"   • {s}")

        if r["social"]:
            lines.append("   SOCIAL:")
            for s in r["social"]:
                lines.append(f"   • {s}")

        if r.get("news_label"):
            lines.append(f"   NOTICIAS: {r['news_label']}")

        lines.append(
            f"   Entrada: ~${r['price']:,.2f}  |  Stop: ~${stop:,.2f} (-{stop_pct:.1f}%)"
        )
        lines.append("   " + "─" * 24)

    lines.append("\nAviso: no es asesoramiento financiero.")
    return "\n".join(lines)


# ── Alertas de noticias importantes ─────────────────────────────────────────

async def check_news_alerts():
    """Revisa noticias cada 20 min y alerta si hay algo de alto impacto"""
    if not subscribers:
        return
    url = (
        f"https://newsdata.io/api/1/latest"
        f"?apikey={NEWSDATA_KEY}&language=en"
        f"&country=us&category=business,politics,world&size=10"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as r:
                d        = await r.json()
                articles = d.get("results", [])

        for article in articles:
            art_id = article.get("article_id") or article.get("link", "")
            if not art_id or art_id in sent_articles:
                continue

            title = article.get("title", "")
            desc  = article.get("description", "") or ""
            text  = f"{title} {desc}".lower()

            matched = [kw for kw in IMPACT_KEYWORDS if kw in text]
            if not matched:
                continue

            # Detectar acciones afectadas (solo tickers de 3+ letras para evitar falsos positivos)
            text_up  = f" {title} {desc} ".upper()
            affected = [
                t for t in STOCKS
                if len(t) >= 3 and f" {t} " in text_up or f"${t}" in text_up
            ]

            # Solo enviar si menciona un ticker conocido O son keywords muy críticos
            critical = ["fed decision", "rate hike", "rate cut", "opec", "ukraine", "taiwan", "nuclear"]
            is_critical = any(kw in text for kw in critical)
            if not affected and not is_critical:
                continue

            msg = f"ALERTA DE MERCADO\n\n{title}\n\n"
            if desc:
                msg += f"{desc[:280]}...\n\n"
            if affected:
                msg += f"Acciones afectadas: {', '.join(affected[:6])}\n"
            msg += f"Palabras clave: {', '.join(matched[:3])}"

            sent_articles.add(art_id)
            for chat_id in list(subscribers):
                try:
                    await bot.send_message(chat_id, msg)
                except Exception:
                    pass
            break  # máx 1 alerta por ciclo para no spamear

    except Exception:
        pass


# ── Helpers de precios ───────────────────────────────────────────────────────

async def get_finnhub_quote(ticker):
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            d = await r.json()
            if not d.get("c"):
                return None
            return {"price": d["c"], "change": d["dp"],
                    "high": d["h"], "low": d["l"],
                    "open": d["o"], "prev": d["pc"]}


async def get_market_news():
    results = {}
    async with aiohttp.ClientSession() as session:
        for key, params in [
            ("mercado",    "language=en&category=business&q=Wall+Street+earnings+Fed+nasdaq&size=5"),
            ("geopolitica","language=en&q=Iran+Ukraine+Taiwan+OPEC+war+sanctions+oil&size=5"),
        ]:
            try:
                url = f"https://newsdata.io/api/1/latest?apikey={NEWSDATA_KEY}&{params}"
                async with session.get(url, headers=HEADERS) as r:
                    d = await r.json()
                    results[key] = [a["title"] for a in d.get("results", []) if a.get("title")]
            except Exception:
                results[key] = []
    return results


async def get_btc_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            d = await r.json()
            return d["bitcoin"]["usd"], d["bitcoin"]["usd_24h_change"]


async def get_stooq_price(ticker):
    url = f"https://stooq.com/q/l/?s={ticker}&f=sd2t2ohlcv&h&e=csv"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            text   = await r.text()
            lines  = text.strip().split("\n")
            values = lines[1].split(",")
            if "N/D" in values:
                return None, None
            close  = float(values[6])
            open_  = float(values[3])
            return close, ((close - open_) / open_) * 100


# ── Comandos ─────────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer(
        "Hola! Soy tu analista de mercados.\n\n"
        "Comandos:\n"
        "/analisis — Índices y Bitcoin en tiempo real\n"
        "/precio TICKER — Precio en tiempo real\n"
        "/acciones — Top 5 recomendaciones (técnico+social+noticias)\n"
        "/sentimiento TICKER — Sentimiento social completo\n"
        "/noticias — Titulares de mercado y geopolítica\n"
        "/suscribir — Alertas diarias 9:30 AM NY + alertas de noticias\n"
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
            if price is None: return "Mercado cerrado"
            return f"{prefix}{price:,.2f}{suffix} ({change:+.2f}%)"

        await message.answer(
            f"Resumen en tiempo real:\n\n"
            f"• Bitcoin: {fmt(btc_price, btc_change)} (24h)\n"
            f"• Nasdaq:  {fmt(nasdaq, nasdaq_change, prefix='', suffix=' pts')}\n"
            f"• S&P 500: {fmt(sp500, sp500_change, prefix='', suffix=' pts')}\n"
            f"• Oro:     {fmt(gold, gold_change)}/oz\n"
        )
    except Exception as e:
        await message.answer(f"Error: {e}")


@dp.message(Command("acciones"))
async def acciones(message: types.Message):
    await message.answer(f"Analizando {len(STOCKS)} acciones (técnico + sentimiento + noticias)...\nEspera 1-2 minutos.")
    recs = await get_recommendations()
    await message.answer(format_recommendations(recs))


@dp.message(Command("precio"))
async def precio(message: types.Message):
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("Uso: /precio TICKER  — Ejemplo: /precio AAPL")
        return
    ticker = parts[1].upper()
    q = await get_finnhub_quote(ticker)
    if not q:
        await message.answer(f"No se encontró cotización para {ticker}.")
        return
    await message.answer(
        f"{ticker} — Tiempo real\n\n"
        f"• Precio:     ${q['price']:,.2f}  ({q['change']:+.2f}%)\n"
        f"• Apertura:   ${q['open']:,.2f}\n"
        f"• Máx/Mín:    ${q['high']:,.2f} / ${q['low']:,.2f}\n"
        f"• Cierre ant: ${q['prev']:,.2f}\n"
    )


@dp.message(Command("sentimiento"))
async def sentimiento(message: types.Message):
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("Uso: /sentimiento TICKER  — Ejemplo: /sentimiento NVDA")
        return
    ticker = parts[1].upper()
    await message.answer(f"Analizando sentimiento de {ticker}...")

    (fh_score, fh_label), (soc_score, soc_labels), quote = await asyncio.gather(
        get_finnhub_sentiment(ticker),
        get_social_sentiment_only(ticker),
        get_finnhub_quote(ticker),
    )

    lines = [f"Sentimiento — {ticker}\n"]
    if quote:
        lines.append(f"Precio: ${quote['price']:,.2f} ({quote['change']:+.2f}%)\n")
    lines.append(f"Noticias: {fh_label or 'Neutro'}\n")
    if soc_labels:
        lines.append("Redes sociales:")
        for l in soc_labels:
            lines.append(f"• {l}")
    else:
        lines.append("Redes sociales: sin señal clara")

    total = fh_score + soc_score
    verdict = "POSITIVO" if total >= 2 else "NEGATIVO" if total <= -2 else "NEUTRO"
    lines.append(f"\nResumen: {verdict} (score {total:+d})")
    await message.answer("\n".join(lines))


async def get_social_sentiment_only(ticker):
    st, (reddit_posts, reddit_upvotes) = await asyncio.gather(
        get_stocktwits_sentiment(ticker),
        get_reddit_mentions(ticker)
    )
    score  = 0
    labels = []
    if st and st["total"] >= 5:
        if st["bull_pct"] >= 65:
            score += 2
            labels.append(f"StockTwits {st['bull_pct']:.0f}% alcista ({st['bullish']}/{st['total']})")
        elif st["bull_pct"] <= 35:
            score -= 2
            labels.append(f"StockTwits {100-st['bull_pct']:.0f}% bajista")
        if st["count"] >= 20 and (st["bull_pct"] >= 70 or st["bull_pct"] <= 30):
            labels.append(f"Buzz inusual ({st['count']} mensajes)")
    if reddit_posts >= 5:
        score += 1
        labels.append(f"Trending Reddit ({reddit_posts} posts, {reddit_upvotes:,} upvotes)")
    elif reddit_posts >= 3:
        labels.append(f"Reddit ({reddit_posts} posts)")
    return score, labels


@dp.message(Command("noticias"))
async def noticias(message: types.Message):
    await message.answer("Obteniendo noticias...")
    news  = await get_market_news()
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
    await message.answer("\n".join(lines) if len(lines) > 2 else "Sin noticias disponibles.")


@dp.message(Command("suscribir"))
async def suscribir(message: types.Message):
    subscribers.add(message.chat.id)
    await message.answer(
        "Suscrito.\n\n"
        "Recibirás:\n"
        "• Recomendaciones diarias a las 9:30 AM (hora NY)\n"
        "• Alertas inmediatas cuando salga una noticia de alto impacto"
    )


@dp.message(Command("cancelar"))
async def cancelar(message: types.Message):
    subscribers.discard(message.chat.id)
    await message.answer("Suscripción cancelada.")


# ── Jobs programados ──────────────────────────────────────────────────────────

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


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    scheduler.add_job(send_daily_recommendations, "cron", hour=9, minute=30)
    scheduler.add_job(check_news_alerts, "interval", minutes=20)
    scheduler.start()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
