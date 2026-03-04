from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio, os, json, aiohttp, yfinance as yf
from datetime import datetime, timedelta

TOKEN         = os.environ.get("BOT_TOKEN",    "8616657604:AAGQI9e_x9ZX5zw6zcHIloboeDO18OrKRBM")
FINNHUB_KEY   = os.environ.get("FINNHUB_KEY",  "d6j133pr01qleu95u19gd6j133pr01qleu95u1a0")
NEWSDATA_KEY  = os.environ.get("NEWSDATA_KEY", "pub_4cbb2798d21e439186e168313963b1bb")

bot           = Bot(token=TOKEN)
dp            = Dispatcher()
scheduler     = AsyncIOScheduler(timezone="America/New_York")
HEADERS       = {"User-Agent": "Mozilla/5.0"}
sent_articles = set()
PERF_FILE            = "/tmp/performance.json"
CACHE_FILE           = "/tmp/recommendations_cache.json"
PREV_CANDIDATES_FILE = "/tmp/prev_candidates.json"
SUBS_FILE            = "/tmp/subscribers.json"

def load_subscribers():
    try:
        with open(SUBS_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()

def save_subscribers(subs):
    try:
        with open(SUBS_FILE, "w") as f:
            json.dump(list(subs), f)
    except Exception:
        pass

subscribers = load_subscribers()

# ── Palabras clave de alto impacto ───────────────────────────────────────────
IMPACT_KEYWORDS = [
    "earnings beat","earnings miss","supera estimaciones","resultados record",
    "acquisition","merger","adquisición","fusión",
    "fda approval","fda rejects","bankruptcy","quiebra","chapter 11",
    "layoffs","despidos","rate hike","rate cut","fed decision","powell",
    "opec","opep","ukraine","ucrania","taiwan","taiwán",
    "sanctions","sanciones","nuclear","war escalation",
    "stock split","buyback","revenue guidance","profit warning",
    "sec investigation","fraud","fraude","iran","strike",
]

# ── Sectores geopolíticos favorables ────────────────────────────────────────
SECTOR_BOOST = {
    "NVDA":("IA/Semis",2),"AMD":("IA/Semis",2),"AVGO":("IA/Semis",2),
    "MU":("IA/Semis",2),"SMCI":("IA/Semis",2),"AMAT":("IA/Semis",1),
    "LRCX":("IA/Semis",1),"KLAC":("IA/Semis",1),"QCOM":("IA/Semis",1),
    "ARM":("IA/Semis",2),"ASML":("IA/Semis",2),"VRT":("IA/Semis",2),
    "LSCC":("IA/Semis",1),"MKS":("IA/Semis",1),"MCHP":("IA/Semis",1),
    "ADI":("IA/Semis",1),"ONTO":("IA/Semis",1),"COHR":("IA/Semis",1),
    "MSFT":("IA/Cloud",1),"GOOGL":("IA/Cloud",1),"META":("IA/Cloud",1),
    "AMZN":("IA/Cloud",1),"CRM":("IA/Cloud",1),"PLTR":("IA/Cloud",2),
    "DELL":("IA/Cloud",1),"HPE":("IA/Cloud",1),"ACN":("IA/Cloud",1),
    "PATH":("IA/Cloud",1),"AI":("IA/Cloud",2),"SOUN":("IA/Cloud",2),
    "LMT":("Defensa",2),"RTX":("Defensa",2),"NOC":("Defensa",2),
    "GD":("Defensa",2),"BA":("Defensa",1),"LDOS":("Defensa",1),
    "HII":("Defensa",1),"AXON":("Defensa",1),"KTOS":("Defensa",1),
    "LHX":("Defensa",2),"TXT":("Defensa",1),"BWXT":("Defensa",1),
    "COIN":("Cripto",2),"MSTR":("Cripto",2),"HOOD":("Cripto",1),
    "RIOT":("Cripto",1),"MARA":("Cripto",1),
    "XOM":("Energía",1),"CVX":("Energía",1),"OXY":("Energía",1),
    "SLB":("Energía",1),"HAL":("Energía",1),"VLO":("Energía",1),
    "MPC":("Energía",1),"PSX":("Energía",1),"LNG":("Energía",1),
    "APP":("IA/Cloud",2),"TTD":("IA/Cloud",1),
}

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
    "NUE","STLD","CLF","ATI","RIO","BHP","VALE","SCCO",
    "SPY","QQQ","IWM","XLK","XLF","XLE","XLV","XLI","ARKK",
    "XLY","XLP","XLB","XLU","XLRE","XLC","GLD","TLT","GDX",
    # Semis adicionales
    "MCHP","ADI","TER","ONTO","COHR","ACLS",
    # Semis IA puros
    "ARM","ASML","LSCC","RMBS","MKS","IPGP","LITE","CRUS","MTSI","PI","SITM",
    "WDC","STX","ICHR","FORM","POWI","SLAB","ALGM",
    # IA infraestructura / datos
    "VRT","DELL","HPE","PSTG","NTAP","CIEN","VIAV",
    # IA software / servicios
    "AI","SOUN","PATH","ACN","CTSH","EPAM","BBAI",
    # Tech / Software adicional
    "UBER","LYFT","SNAP","PINS","RBLX","TWLO","TTD","APP",
    "TOST","MNDY","APPN","ASAN","SMAR","CDAY","ZI","DOCN","FOUR",
    # Healthcare / Biotech adicional
    "EXAS","ALNY","MDGL","INCY","NBIX","SRPT","BEAM","CRSP","NTLA",
    "HALO","JAZZ","IONS","FOLD","RARE","ACAD",
    # Finanzas adicional
    "UPST","FIS","FISV","GPN","MQ","SPGI","MCO","MSCI","NDAQ","WU",
    # Consumer / Leisure adicional
    "DKNG","PENN","RCL","CCL","AAL","LUV","CHWY","SPOT",
    "LULU","ULTA","TJX","ROST","ELF","CMG","YUM","WING","ONON","DECK","RH",
    # Defensa / Industrial adicional
    "LHX","TXT","BWXT","WAB","DRS",
    # Energía adicional
    "LNG","FANG","CTRA","AR","SM",
    # REITs adicional
    "WELL","VTR","VICI","CCI",
    # Otros solicitados
    "WBI",
]))

# ── ETFs de sector para mapa de calor ────────────────────────────────────────
SECTOR_ETFS = {
    "Tecnología":   "XLK",
    "Finanzas":     "XLF",
    "Energía":      "XLE",
    "Salud":        "XLV",
    "Industriales": "XLI",
    "Cons. Discr.": "XLY",
    "Cons. Básico": "XLP",
    "Materiales":   "XLB",
    "Utilities":    "XLU",
    "Inmuebles":    "XLRE",
    "Comunicación": "XLC",
}


# ════════════════════════════════════════════════════════════════════════════
# 0. PERSISTENCIA DE SEÑALES
# ════════════════════════════════════════════════════════════════════════════

def load_prev_candidates():
    """Carga los tickers candidatos del análisis anterior (hasta 36h atrás)."""
    try:
        with open(PREV_CANDIDATES_FILE) as f:
            data = json.load(f)
        age = (datetime.now() - datetime.fromisoformat(data["timestamp"])).total_seconds()
        if age < 36 * 3600:
            return set(data["tickers"])
    except Exception:
        pass
    return set()

def save_prev_candidates(tickers):
    try:
        with open(PREV_CANDIDATES_FILE, "w") as f:
            json.dump({"timestamp": datetime.now().isoformat(), "tickers": list(tickers)}, f)
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════
# 1. INDICADORES TÉCNICOS
# ════════════════════════════════════════════════════════════════════════════

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
        high_60d = float(closes.max())
        sma20    = float(closes.iloc[-20:].mean())
        sma50    = float(closes.iloc[-50:].mean())
        vol_avg  = float(volumes.iloc[-21:-1].mean())
        vol_now  = float(volumes.iloc[-1])

        if rsi >= 70:            return None
        if price < sma20:        return None
        if price < sma50 * 0.97: return None

        score        = 0
        tech_signals = []
        sector_label = ""

        if rsi < 35:
            score += 2; tech_signals.append(f"RSI {rsi:.0f} — sobreventa")
        elif 45 <= rsi <= 63:
            score += 1; tech_signals.append(f"RSI {rsi:.0f} — momentum saludable")

        if macd_prev < sig_prev and macd_now > sig_now:
            score += 3; tech_signals.append("MACD cruce alcista")

        if price > high_20d:
            score += 3; tech_signals.append(f"Rotura resistencia ${high_20d:.2f}")

        if prev <= low_20d * 1.005 and price > prev * 1.005:
            score += 2; tech_signals.append(f"Rebote soporte ${low_20d:.2f}")

        if price > sma20 and price > sma50:
            score += 1; tech_signals.append("Sobre SMA20 y SMA50")

        if vol_avg > 0 and vol_now > vol_avg * 1.5:
            score += 2; tech_signals.append(f"Volumen {vol_now/vol_avg:.1f}x promedio")

        if ticker in SECTOR_BOOST:
            sector_label, boost = SECTOR_BOOST[ticker]
            score += boost

        if len(tech_signals) < 2 or score < 4:
            return None

        stop = max(low_20d, sma20 * 0.98)
        risk = price - stop
        if risk <= 0:
            return None

        # ── Filtro R:R mínimo 2:1 ────────────────────────────────────────────
        # El target 2:1 debe ser alcanzable: se permite si el precio ya
        # superó el máximo de 60d (breakout a nuevos máximos) o si el
        # target no está más de un 5% por encima del máximo de 60d.
        target      = price + 2 * risk
        is_breakout = price >= high_60d * 0.99
        if not is_breakout and target > high_60d * 1.05:
            return None

        return {
            "ticker": ticker, "score": score,
            "tech_signals": tech_signals, "social": [],
            "news_label": None, "extra": [],
            "price": price, "rsi": rsi,
            "sector": sector_label, "stop": stop,
        }
    except Exception:
        return None


# ════════════════════════════════════════════════════════════════════════════
# 2. MACRO CONTEXT (VIX + DXY + YIELDS)
# ════════════════════════════════════════════════════════════════════════════

async def get_macro_context():
    def fetch():
        result = {}
        for label, ticker in [("vix","^VIX"), ("dxy","DX-Y.NYB"), ("tnx","^TNX")]:
            try:
                hist = yf.Ticker(ticker).history(period="5d")
                if not hist.empty:
                    result[label] = float(hist["Close"].iloc[-1])
            except Exception:
                pass
        return result
    return await asyncio.to_thread(fetch)

def interpret_macro(macro):
    lines = []
    score_adj = 0
    vix = macro.get("vix")
    dxy = macro.get("dxy")
    tnx = macro.get("tnx")

    if vix:
        if vix > 35:
            score_adj -= 3
            lines.append(f"VIX {vix:.1f} — PANICO: evitar nuevas posiciones")
        elif vix > 25:
            score_adj -= 1
            lines.append(f"VIX {vix:.1f} — volatilidad alta, reducir tamaño")
        elif vix < 15:
            score_adj += 1
            lines.append(f"VIX {vix:.1f} — mercado calmo, favorable")
        else:
            lines.append(f"VIX {vix:.1f} — normal")
    if dxy:
        if dxy > 106:
            lines.append(f"DXY {dxy:.1f} — dólar fuerte, presión en commodities/emergentes")
        elif dxy < 100:
            lines.append(f"DXY {dxy:.1f} — dólar débil, favorable para materiales y cripto")
        else:
            lines.append(f"DXY {dxy:.1f} — dólar neutro")
    if tnx:
        if tnx > 4.5:
            lines.append(f"Yield 10Y {tnx:.2f}% — tasas altas, presión en growth/tech")
        elif tnx < 3.5:
            lines.append(f"Yield 10Y {tnx:.2f}% — tasas bajas, favorable para acciones")
        else:
            lines.append(f"Yield 10Y {tnx:.2f}%")

    return score_adj, lines


# ════════════════════════════════════════════════════════════════════════════
# 3. MULTI-TIMEFRAME (SEMANAL)
# ════════════════════════════════════════════════════════════════════════════

async def get_weekly_trends(tickers):
    def fetch():
        return yf.download(
            tickers, period="26wk", interval="1wk",
            progress=False, group_by="ticker", auto_adjust=True
        )
    try:
        data = await asyncio.to_thread(fetch)
        results = {}
        for ticker in tickers:
            try:
                df     = data[ticker].dropna() if len(tickers) > 1 else data.dropna()
                closes = df["Close"].squeeze()
                if len(closes) < 10:
                    results[ticker] = None; continue
                sma10w = float(closes.iloc[-10:].mean())
                price  = float(closes.iloc[-1])
                rsi_w  = calc_rsi(closes)
                results[ticker] = {"bullish": price > sma10w, "rsi_w": rsi_w}
            except Exception:
                results[ticker] = None
        return results
    except Exception:
        return {t: None for t in tickers}


# ════════════════════════════════════════════════════════════════════════════
# 4. INSIDER BUYING (Finnhub)
# ════════════════════════════════════════════════════════════════════════════

async def get_insider_buying(ticker):
    url = f"https://finnhub.io/api/v1/stock/insider-transactions?symbol={ticker}&token={FINNHUB_KEY}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as r:
                d = await r.json()
        cutoff = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
        purchases = [
            t for t in d.get("data", [])
            if str(t.get("transactionType","")).startswith("P")
            and t.get("transactionDate","") >= cutoff
            and (t.get("change") or 0) > 0
        ]
        if not purchases:
            return None
        total = sum(t.get("change", 0) for t in purchases)
        return {"count": len(purchases), "shares": int(total)}
    except Exception:
        return None


# ════════════════════════════════════════════════════════════════════════════
# 5. OPCIONES INUSUALES (yfinance)
# ════════════════════════════════════════════════════════════════════════════

async def get_unusual_options(ticker):
    def fetch():
        import pandas as pd
        t    = yf.Ticker(ticker)
        exps = t.options
        if not exps:
            return None
        chain = t.option_chain(exps[0])
        calls = chain.calls
        if calls.empty:
            return None
        max_vol = calls["volume"].max()
        med_vol = calls["volume"].median()
        if not max_vol or not med_vol or med_vol == 0:
            return None
        ratio = max_vol / med_vol
        if ratio >= 4:
            strike = float(calls.loc[calls["volume"].idxmax(), "strike"])
            return {"ratio": round(ratio, 1), "max_vol": int(max_vol), "strike": strike}
        return None
    try:
        return await asyncio.to_thread(fetch)
    except Exception:
        return None


# ════════════════════════════════════════════════════════════════════════════
# 6. SENTIMIENTO SOCIAL
# ════════════════════════════════════════════════════════════════════════════

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
                               if m.get("entities",{}).get("sentiment",{}) and
                               m["entities"]["sentiment"].get("basic") == "Bullish")
                bearish  = sum(1 for m in messages
                               if m.get("entities",{}).get("sentiment",{}) and
                               m["entities"]["sentiment"].get("basic") == "Bearish")
                total    = bullish + bearish
                bull_pct = (bullish / total * 100) if total > 0 else 50
                return {"bull_pct": bull_pct, "count": len(messages),
                        "total": total, "bullish": bullish, "bearish": bearish}
    except Exception:
        return None

async def get_reddit_mentions(ticker):
    url = (f"https://www.reddit.com/r/wallstreetbets+stocks+investing"
           f"/search.json?q={ticker}&sort=new&limit=25&t=day")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={**HEADERS,"User-Agent":"MarketBot/1.0"}) as r:
                d       = await r.json()
                posts   = d.get("data",{}).get("children",[])
                upvotes = sum(p["data"].get("score",0) for p in posts)
                return len(posts), upvotes
    except Exception:
        return 0, 0

async def get_finnhub_sentiment(ticker):
    url = f"https://finnhub.io/api/v1/news-sentiment?symbol={ticker}&token={FINNHUB_KEY}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as r:
                d    = await r.json()
                sent = d.get("sentiment",{}).get("bullishPercent", 0.5)
                if sent > 0.6:   return 2, "Noticias positivas"
                elif sent < 0.4: return -2, "Noticias negativas"
                return 0, None
    except Exception:
        return 0, None


# ════════════════════════════════════════════════════════════════════════════
# 7. FILTRO DE EARNINGS (Finnhub)
# ════════════════════════════════════════════════════════════════════════════

async def has_upcoming_earnings(ticker, days=3):
    """True si la empresa reporta resultados en los próximos `days` días."""
    today  = datetime.now().strftime("%Y-%m-%d")
    future = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
    url = (f"https://finnhub.io/api/v1/calendar/earnings"
           f"?from={today}&to={future}&symbol={ticker}&token={FINNHUB_KEY}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as r:
                d = await r.json()
                return len(d.get("earningsCalendar", [])) > 0
    except Exception:
        return False  # Si falla el check, no excluir


# ════════════════════════════════════════════════════════════════════════════
# 8. PERFORMANCE TRACKING
# ════════════════════════════════════════════════════════════════════════════

def save_recommendations(recs):
    try:
        try:
            with open(PERF_FILE) as f:
                history = json.load(f)
        except Exception:
            history = []
        history = (history + [{
            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "recs": [{"ticker": r["ticker"], "price": round(r["price"], 2)} for r in recs]
        }])[-60:]
        with open(PERF_FILE, "w") as f:
            json.dump(history, f)
    except Exception:
        pass

async def get_performance_report():
    try:
        with open(PERF_FILE) as f:
            history = json.load(f)
    except Exception:
        return "No hay historial todavía. Usa /acciones para generar recomendaciones."

    if not history:
        return "No hay historial todavía."

    lines  = ["Rendimiento de recomendaciones anteriores:\n"]
    wins   = 0
    total  = 0

    for entry in history[-10:]:
        lines.append(f"Fecha: {entry['date']}")
        for rec in entry["recs"]:
            q = await get_finnhub_quote(rec["ticker"])
            if q:
                pnl = ((q["price"] - rec["price"]) / rec["price"]) * 100
                lines.append(f"  {'OK' if pnl > 0 else 'X '} {rec['ticker']}: "
                              f"${rec['price']:.2f} -> ${q['price']:.2f} ({pnl:+.1f}%)")
                wins  += 1 if pnl > 0 else 0
                total += 1
            else:
                lines.append(f"  {rec['ticker']}: ${rec['price']:.2f} (sin datos)")
        lines.append("")

    if total > 0:
        lines.append(f"Tasa de acierto: {wins}/{total} ({wins/total*100:.0f}%)")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
# 9. RENDIMIENTO POR SECTOR
# ════════════════════════════════════════════════════════════════════════════

async def get_sector_performance():
    def fetch():
        tickers = list(SECTOR_ETFS.values())
        data    = yf.download(
            tickers, period="2d", interval="1d",
            progress=False, group_by="ticker", auto_adjust=True
        )
        out = {}
        for name, etf in SECTOR_ETFS.items():
            try:
                df  = data[etf].dropna() if len(tickers) > 1 else data.dropna()
                cls = df["Close"].squeeze()
                if len(cls) >= 2:
                    chg = (float(cls.iloc[-1]) - float(cls.iloc[-2])) / float(cls.iloc[-2]) * 100
                    out[name] = round(chg, 2)
            except Exception:
                pass
        return out
    try:
        return await asyncio.to_thread(fetch)
    except Exception:
        return {}


# ════════════════════════════════════════════════════════════════════════════
# 10. MOTOR DE RECOMENDACIONES
# ════════════════════════════════════════════════════════════════════════════

async def enrich_candidate(r, weekly, macro_adj):
    ticker = r["ticker"]

    r["score"] += macro_adj

    wk = weekly.get(ticker)
    if wk:
        if wk["bullish"] and wk["rsi_w"] < 65:
            r["score"] += 2
            r["extra"].append(f"Tendencia semanal alcista (RSI semanal {wk['rsi_w']:.0f})")
        elif not wk["bullish"]:
            r["score"] -= 1

    insider, options, st, (reddit_n, reddit_up), (fh_score, fh_label) = await asyncio.gather(
        get_insider_buying(ticker),
        get_unusual_options(ticker),
        get_stocktwits_sentiment(ticker),
        get_reddit_mentions(ticker),
        get_finnhub_sentiment(ticker),
    )

    if insider:
        r["score"] += 2
        r["extra"].append(f"Insiders comprando ({insider['count']} transacciones, {insider['shares']:,} acc.)")

    if options:
        r["score"] += 2
        r["extra"].append(f"Opciones inusuales: calls {options['ratio']}x promedio en strike ${options['strike']:.0f}")

    if fh_score != 0:
        r["score"] += fh_score
        if fh_label:
            r["news_label"] = fh_label

    if st and st["total"] >= 5:
        if st["bull_pct"] >= 65:
            r["score"] += 2
            r["social"].append(f"StockTwits {st['bull_pct']:.0f}% alcista ({st['bullish']}/{st['total']})")
        elif st["bull_pct"] <= 35:
            r["score"] -= 2
            r["social"].append(f"StockTwits {100-st['bull_pct']:.0f}% bajista")
        if st["count"] >= 20 and (st["bull_pct"] >= 70 or st["bull_pct"] <= 30):
            r["social"].append(f"Buzz inusual ({st['count']} mensajes)")

    if reddit_n >= 5:
        r["score"] += 1
        r["social"].append(f"Trending Reddit ({reddit_n} posts, {reddit_up:,} upvotes)")
    elif reddit_n >= 3:
        r["social"].append(f"Reddit ({reddit_n} posts)")

    return r


async def get_recommendations(force_refresh=False):
    # ── 1. Caché de 3 horas ──────────────────────────────────────────────────
    if not force_refresh:
        try:
            with open(CACHE_FILE) as f:
                cache = json.load(f)
            age = (datetime.now() - datetime.fromisoformat(cache["timestamp"])).total_seconds()
            if age < 3 * 3600:
                # Invalidar si S&P500 se movió más de 1%
                sp500_now, _ = await get_stooq_price("^spx")
                sp500_cached = cache.get("sp500")
                market_moved = (
                    sp500_now and sp500_cached and
                    abs(sp500_now - sp500_cached) / sp500_cached * 100 >= 1.0
                )
                if not market_moved:
                    cached_macro = cache.get("macro_lines", [])
                    cached_macro = [f"(Caché — {datetime.fromisoformat(cache['timestamp']).strftime('%H:%M')})"] + cached_macro
                    return cache["recs"], cached_macro
        except Exception:
            pass

    # ── 2. Descarga bulk ─────────────────────────────────────────────────────
    def fetch_daily():
        return yf.download(STOCKS, period="60d", interval="1d",
                           progress=False, group_by="ticker", auto_adjust=True)

    daily_data, macro = await asyncio.gather(
        asyncio.to_thread(fetch_daily),
        get_macro_context(),
    )

    macro_adj, macro_lines = interpret_macro(macro)

    # ── 3. Cargar candidatos previos ─────────────────────────────────────────
    prev_candidates = load_prev_candidates()

    # ── 4. Análisis técnico + bonus persistencia ─────────────────────────────
    results = []
    for ticker in STOCKS:
        try:
            df     = daily_data[ticker].dropna()
            result = analyze_stock(ticker, df)
            if result:
                if ticker in prev_candidates:
                    result["score"] += 2
                    result["tech_signals"].append("Senal persistente (2+ dias)")
                results.append(result)
        except Exception:
            continue

    ranked    = sorted(results, key=lambda x: x["score"], reverse=True)
    tickers15 = [r["ticker"] for r in ranked[:15]]

    # Guardar top-15 para próxima ejecución (persistencia)
    save_prev_candidates(tickers15)

    # ── 5. Filtro earnings (top 15) ──────────────────────────────────────────
    earnings_flags = await asyncio.gather(*[has_upcoming_earnings(t) for t in tickers15])
    candidates = [r for r, skip in zip(ranked[:15], earnings_flags) if not skip]

    # Si el filtro dejó muy pocos, completar con los siguientes de ranked
    if len(candidates) < 5 and len(ranked) > 15:
        extra      = ranked[15:30]
        ex_tickers = [r["ticker"] for r in extra]
        ex_flags   = await asyncio.gather(*[has_upcoming_earnings(t) for t in ex_tickers])
        for r, skip in zip(extra, ex_flags):
            if not skip:
                candidates.append(r)
            if len(candidates) >= 15:
                break

    if not candidates:
        candidates = ranked[:5]  # fallback de emergencia

    # ── 6. Datos semanales + enriquecimiento ─────────────────────────────────
    weekly   = await get_weekly_trends([r["ticker"] for r in candidates])
    enriched = await asyncio.gather(*[
        enrich_candidate(r, weekly, macro_adj) for r in candidates
    ])
    enriched = sorted(enriched, key=lambda x: x["score"], reverse=True)

    # ── 7. Máx 2 por sector ───────────────────────────────────────────────────
    top          = []
    sector_count = {}
    for r in enriched:
        sector = r.get("sector") or "General"
        if sector_count.get(sector, 0) < 2:
            top.append(r)
            sector_count[sector] = sector_count.get(sector, 0) + 1
        if len(top) == 5:
            break

    # ── 8. Precio RT (Finnhub) ────────────────────────────────────────────────
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

    save_recommendations(top)

    # ── 9. Guardar caché ──────────────────────────────────────────────────────
    try:
        sp500_save, _ = await get_stooq_price("^spx")
        with open(CACHE_FILE, "w") as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "sp500": sp500_save,
                "recs": top,
                "macro_lines": macro_lines,
            }, f, default=str)
    except Exception:
        pass

    return top, macro_lines


def format_recommendations(recs, macro_lines=None, titulo="Recomendaciones corto plazo"):
    if not recs:
        return "No se encontraron acciones que cumplan todos los criterios hoy."

    lines = [
        f"{titulo} — {len(STOCKS)} acciones analizadas",
        datetime.now().strftime("%d/%m/%Y %H:%M"),
    ]

    if macro_lines:
        lines.append("\nContexto macro:")
        for l in macro_lines:
            lines.append(f"  {l}")

    lines.append("\n" + "━"*28)

    for i, r in enumerate(recs, 1):
        change   = f"({r['change']:+.2f}%)" if r.get("change") is not None else ""
        stop     = r.get("stop", r["price"] * 0.97)
        risk     = r["price"] - stop
        stop_pct = (risk / r["price"]) * 100
        target   = r["price"] + 2 * risk
        tgt_pct  = (target - r["price"]) / r["price"] * 100
        sector   = f" [{r['sector']}]" if r.get("sector") else ""

        lines.append(f"\n{i}. {r['ticker']}{sector}")
        lines.append(f"   ${r['price']:,.2f} {change} | RSI {r['rsi']:.0f} | Score {r['score']}")

        if r.get("tech_signals"):
            lines.append("   TECNICO:")
            for s in r["tech_signals"]:
                lines.append(f"   • {s}")

        if r.get("extra"):
            lines.append("   EXTRA:")
            for s in r["extra"]:
                lines.append(f"   • {s}")

        if r.get("social"):
            lines.append("   SOCIAL:")
            for s in r["social"]:
                lines.append(f"   • {s}")

        if r.get("news_label"):
            lines.append(f"   NOTICIAS: {r['news_label']}")

        lines.append(
            f"   Stop: ${stop:,.2f} (-{stop_pct:.1f}%) | "
            f"Target: ${target:,.2f} (+{tgt_pct:.1f}%) | R:R 2:1"
        )
        lines.append("   " + "─"*24)

    lines.append("\nAviso: no es asesoramiento financiero.")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
# 11. ALERTAS DE NOTICIAS
# ════════════════════════════════════════════════════════════════════════════

async def check_news_alerts():
    if not subscribers:
        return
    url = (f"https://newsdata.io/api/1/latest"
           f"?apikey={NEWSDATA_KEY}&language=en"
           f"&category=business,politics,world&size=10")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=HEADERS) as r:
                articles = (await r.json()).get("results", [])

        sent_this_cycle = 0
        critical = ["fed decision","rate hike","rate cut","opec","ukraine",
                    "taiwan","nuclear","iran","tariff","sanction","recession"]

        for article in articles:
            if sent_this_cycle >= 3:
                break
            art_id = article.get("article_id") or article.get("link","")
            if not art_id or art_id in sent_articles:
                continue
            title   = article.get("title","")
            desc    = article.get("description","") or ""
            text    = f"{title} {desc}".lower()
            matched = [kw for kw in IMPACT_KEYWORDS if kw in text]
            if not matched:
                continue
            text_up  = f" {title} {desc} ".upper()
            affected = [t for t in STOCKS if len(t) >= 3 and
                        (f" {t} " in text_up or f"${t}" in text_up)]
            if not affected and not any(kw in text for kw in critical):
                continue

            msg = f"ALERTA DE MERCADO\n\n{title}\n\n"
            if desc:
                msg += f"{desc[:300]}...\n\n"
            if affected:
                msg += f"Acciones afectadas: {', '.join(affected[:6])}\n"
            msg += f"Palabras clave: {', '.join(matched[:3])}"

            sent_articles.add(art_id)
            sent_this_cycle += 1
            for chat_id in list(subscribers):
                try:
                    await bot.send_message(chat_id, msg)
                except Exception:
                    pass
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════
# 12. HELPERS DE PRECIOS
# ════════════════════════════════════════════════════════════════════════════

async def get_finnhub_quote(ticker):
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as r:
            d = await r.json()
            if not d.get("c"):
                return None
            return {"price":d["c"],"change":d["dp"],"high":d["h"],"low":d["l"],
                    "open":d["o"],"prev":d["pc"]}

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
                    results[key] = [a["title"] for a in d.get("results",[]) if a.get("title")]
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
            values = text.strip().split("\n")[1].split(",")
            if "N/D" in values:
                return None, None
            c = float(values[6]); o = float(values[3])
            return c, ((c - o) / o) * 100

async def get_social_sentiment_only(ticker):
    st, (reddit_n, reddit_up) = await asyncio.gather(
        get_stocktwits_sentiment(ticker), get_reddit_mentions(ticker)
    )
    score, labels = 0, []
    if st and st["total"] >= 5:
        if st["bull_pct"] >= 65:
            score += 2; labels.append(f"StockTwits {st['bull_pct']:.0f}% alcista ({st['bullish']}/{st['total']})")
        elif st["bull_pct"] <= 35:
            score -= 2; labels.append(f"StockTwits {100-st['bull_pct']:.0f}% bajista")
        if st["count"] >= 20 and (st["bull_pct"] >= 70 or st["bull_pct"] <= 30):
            labels.append(f"Buzz inusual ({st['count']} mensajes)")
    if reddit_n >= 5:
        score += 1; labels.append(f"Trending Reddit ({reddit_n} posts, {reddit_up:,} upvotes)")
    elif reddit_n >= 3:
        labels.append(f"Reddit ({reddit_n} posts)")
    return score, labels


# ════════════════════════════════════════════════════════════════════════════
# 13. COMANDOS
# ════════════════════════════════════════════════════════════════════════════

@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer(
        "Hola! Soy tu analista de mercados.\n\n"
        "Comandos:\n"
        "/analisis — Indices y Bitcoin en tiempo real\n"
        "/macro — VIX, DXY, yields y contexto macro\n"
        "/precio TICKER — Precio en tiempo real\n"
        "/acciones — Top 5 (tecnico+macro+social+insiders+opciones)\n"
        "/sectores — Mapa de rendimiento por sector\n"
        "/sentimiento TICKER — Sentimiento social completo\n"
        "/noticias — Titulares de mercado y geopolitica\n"
        "/rendimiento — Historial de recomendaciones anteriores\n"
        "/suscribir — Alertas diarias 9:30 AM NY + alertas noticias\n"
        "/cancelar — Cancelar alertas"
    )

@dp.message(Command("macro"))
async def macro_cmd(message: types.Message):
    await message.answer("Obteniendo datos macro...")
    macro = await get_macro_context()
    _, lines = interpret_macro(macro)
    if lines:
        await message.answer("Contexto macro actual:\n\n" + "\n".join(f"• {l}" for l in lines))
    else:
        await message.answer("No se pudieron obtener datos macro.")

@dp.message(Command("analisis"))
async def analisis(message: types.Message):
    await message.answer("Obteniendo datos en tiempo real...")
    try:
        btc_price, btc_change = await get_btc_price()
        nasdaq, nasdaq_change = await get_stooq_price("^ixic")
        sp500,  sp500_change  = await get_stooq_price("^spx")
        gold,   gold_change   = await get_stooq_price("gc.f")
        def fmt(p, c, pre="$", suf=""):
            return "Mercado cerrado" if p is None else f"{pre}{p:,.2f}{suf} ({c:+.2f}%)"
        await message.answer(
            f"Resumen en tiempo real:\n\n"
            f"• Bitcoin: {fmt(btc_price, btc_change)} (24h)\n"
            f"• Nasdaq:  {fmt(nasdaq, nasdaq_change, pre='', suf=' pts')}\n"
            f"• S&P 500: {fmt(sp500, sp500_change, pre='', suf=' pts')}\n"
            f"• Oro:     {fmt(gold, gold_change)}/oz\n"
        )
    except Exception as e:
        await message.answer(f"Error: {e}")

@dp.message(Command("acciones"))
async def acciones(message: types.Message):
    await message.answer(
        f"Analizando {len(STOCKS)} acciones...\n"
        "Tecnico + macro + semanal + insiders + opciones + sentimiento\n"
        "Espera 2-3 min (o datos desde cache si hay analisis reciente)."
    )
    recs, macro_lines = await get_recommendations()
    await message.answer(format_recommendations(recs, macro_lines))

@dp.message(Command("sectores"))
async def sectores(message: types.Message):
    await message.answer("Calculando rendimiento por sector...")
    perf = await get_sector_performance()
    if not perf:
        await message.answer("No se pudieron obtener datos de sectores.")
        return

    sorted_s = sorted(perf.items(), key=lambda x: x[1], reverse=True)
    lines    = [
        "Sectores — Rendimiento hoy:",
        datetime.now().strftime("%d/%m/%Y %H:%M"),
        "━" * 30,
    ]
    for name, chg in sorted_s:
        sign  = "+" if chg >= 0 else ""
        arrow = "+" if chg > 0.1 else "-" if chg < -0.1 else " "
        bars  = "#" * min(int(abs(chg) * 3), 9)
        lines.append(f"[{arrow}] {name:<14} {sign}{chg:.2f}%  {bars}")

    best  = sorted_s[0][0]  if sorted_s else "-"
    worst = sorted_s[-1][0] if sorted_s else "-"
    lines.append("━" * 30)
    lines.append(f"Fuerte: {best}  |  Debil: {worst}")
    await message.answer("\n".join(lines))

@dp.message(Command("precio"))
async def precio(message: types.Message):
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("Uso: /precio TICKER  Ej: /precio AAPL"); return
    q = await get_finnhub_quote(parts[1].upper())
    if not q:
        await message.answer(f"No se encontro {parts[1].upper()}."); return
    t = parts[1].upper()
    await message.answer(
        f"{t} — Tiempo real\n\n"
        f"• Precio:     ${q['price']:,.2f}  ({q['change']:+.2f}%)\n"
        f"• Apertura:   ${q['open']:,.2f}\n"
        f"• Max/Min:    ${q['high']:,.2f} / ${q['low']:,.2f}\n"
        f"• Cierre ant: ${q['prev']:,.2f}\n"
    )

@dp.message(Command("sentimiento"))
async def sentimiento(message: types.Message):
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.answer("Uso: /sentimiento TICKER  Ej: /sentimiento NVDA"); return
    ticker = parts[1].upper()
    await message.answer(f"Analizando sentimiento de {ticker}...")
    (fh_score, fh_label), (soc_score, soc_labels), quote, insider = await asyncio.gather(
        get_finnhub_sentiment(ticker),
        get_social_sentiment_only(ticker),
        get_finnhub_quote(ticker),
        get_insider_buying(ticker),
    )
    lines = [f"Sentimiento — {ticker}\n"]
    if quote:
        lines.append(f"Precio: ${quote['price']:,.2f} ({quote['change']:+.2f}%)\n")
    lines.append(f"Noticias: {fh_label or 'Neutro'}\n")
    if soc_labels:
        lines.append("Redes sociales:")
        for l in soc_labels:
            lines.append(f"• {l}")
    if insider:
        lines.append(f"\nInsiders: {insider['count']} compras recientes ({insider['shares']:,} acciones)")
    total   = fh_score + soc_score
    verdict = "POSITIVO" if total >= 2 else "NEGATIVO" if total <= -2 else "NEUTRO"
    lines.append(f"\nResumen: {verdict} (score {total:+d})")
    await message.answer("\n".join(lines))

@dp.message(Command("noticias"))
async def noticias(message: types.Message):
    await message.answer("Obteniendo noticias...")
    news  = await get_market_news()
    lines = ["Noticias del mercado:\n"]
    if news.get("mercado"):
        lines.append("Mercado / Empresas:")
        for t in news["mercado"]: lines.append(f"• {t}")
    lines.append("")
    if news.get("geopolitica"):
        lines.append("Geopolitica:")
        for t in news["geopolitica"]: lines.append(f"• {t}")
    await message.answer("\n".join(lines) if len(lines) > 2 else "Sin noticias.")

@dp.message(Command("rendimiento"))
async def rendimiento(message: types.Message):
    await message.answer("Calculando rendimiento...")
    report = await get_performance_report()
    await message.answer(report)

@dp.message(Command("suscribir"))
async def suscribir(message: types.Message):
    subscribers.add(message.chat.id)
    save_subscribers(subscribers)
    await message.answer(
        "Suscrito.\n\n"
        "Recibiras:\n"
        "• Recomendaciones diarias a las 9:30 AM (hora NY)\n"
        "• Alertas inmediatas de noticias de alto impacto"
    )

@dp.message(Command("cancelar"))
async def cancelar(message: types.Message):
    subscribers.discard(message.chat.id)
    save_subscribers(subscribers)
    await message.answer("Suscripcion cancelada.")


# ════════════════════════════════════════════════════════════════════════════
# 14. JOBS PROGRAMADOS
# ════════════════════════════════════════════════════════════════════════════

async def send_daily_recommendations():
    if not subscribers:
        return
    recs, macro_lines = await get_recommendations(force_refresh=True)
    msg = format_recommendations(recs, macro_lines, titulo="Recomendaciones del dia")
    for chat_id in list(subscribers):
        try:
            await bot.send_message(chat_id, msg)
        except Exception:
            pass


async def main():
    scheduler.add_job(send_daily_recommendations, "cron", hour=9, minute=30)
    scheduler.add_job(check_news_alerts, "interval", minutes=20)
    scheduler.start()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
