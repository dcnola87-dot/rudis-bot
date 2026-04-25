import os, json, requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# ---------- Setup ----------
load_dotenv()
WEBHOOK = os.getenv("STOCKS_WEBHOOK")
KEY     = os.getenv("ALPACA_KEY")
SECRET  = os.getenv("ALPACA_SECRET")
assert WEBHOOK and KEY and SECRET, "Missing WEBHOOK or ALPACA_KEY/ALPACA_SECRET in .env"

client = StockHistoricalDataClient(KEY, SECRET)
ET = ZoneInfo("America/New_York")

# Universe (edit anytime)
WATCH = [
    "AAPL","NVDA","TSLA","MARA","RIOT","COIN","UPST","AI","PLTR",
    "GME","AMC","NIO","LCID","CVNA","NKLA","TLRY"
]

# Filters / knobs
PRICE_MIN, PRICE_MAX = 0.50, 40.00
GAP_MIN         = 4.0          # % gap vs prior close
PREMKT_VOL_MIN  = 150_000      # cumulative premarket volume
TOP_N           = 6
POST_COOLDOWN_MIN = 12

CACHE_FILE = ".posted_cache.json"   # prevents reposting same ticker in one morning

# ---------- Utils ----------
def load_cache():
    if not os.path.exists(CACHE_FILE): return {"date": "", "posted": []}
    try:
        with open(CACHE_FILE, "r") as f: return json.load(f)
    except Exception:
        return {"date": "", "posted": []}

def save_cache(data):
    with open(CACHE_FILE, "w") as f: json.dump(data, f)

def post(msg: str):
    r = requests.post(WEBHOOK, json={"content": msg}, timeout=15)
    r.raise_for_status()

def last_session_close_from_minutes(symbols, max_lookback_days=7):
    """
    Find the most recent trading day and take the LAST regular-hours minute bar
    (IEX feed only to stay on free plan).
    """
    now = datetime.now(ET)
    for back in range(1, max_lookback_days + 1):
        end   = (now - timedelta(days=back)).replace(hour=16, minute=0, second=0, microsecond=0)
        start = end.replace(hour=9, minute=30)
        req = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame.Minute,
            start=start, end=end,
            adjustment="raw",
            feed="iex"
        )
        data = client.get_stock_bars(req).data
        out = {}
        for sym, bars in data.items():
            if bars:
                out[sym] = bars[-1].close
        if out:  # found a real session
            return out
    return {}

def get_intraday_minutes(symbol, lookback_days=30):
    """Lightweight minutes to approximate SMA50/200 using IEX minutes only."""
    end = datetime.now(ET)
    start = end - timedelta(days=lookback_days)
    req = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Minute,
        start=start, end=end,
        adjustment="raw",
        feed="iex",
        limit=10000
    )
    return [b.close for b in client.get_stock_bars(req).data.get(symbol, [])]

def trend_bits(symbol, last_px):
    closes = get_intraday_minutes(symbol, 30)
    if not closes:
        return "—", 0
    def sma(arr, n):
        if len(arr) < n: n = len(arr)
        return sum(arr[-n:]) / max(1, n)
    s50  = sma(closes, 50*6)    # ~50 trading hours (rough)
    s200 = sma(closes, 200*6)   # rough
    bits = [">SMA50" if last_px > s50 else "<SMA50",
            ">SMA200" if last_px > s200 else "<SMA200"]
    score = (10 if last_px > s50 else 0) + (10 if last_px > s200 else 0)
    return " & ".join(bits), score

def score_confidence(gap_pct, vol, price, trend_score):
    # 60 pts gap/vol, 20 pts price sweet spot, 20 pts trend
    s = 0
    if abs(gap_pct) >= 20: s += 25
    elif abs(gap_pct) >= 10: s += 18
    elif abs(gap_pct) >= 5: s += 12
    if vol >= 1_000_000: s += 35
    elif vol >= 500_000: s += 25
    elif vol >= 100_000: s += 15
    if 1.0 <= price <= 30.0: s += 20
    elif 0.5 <= price <= 40.0: s += 12
    s += max(0, min(20, trend_score))
    return min(100, s)

def label_for(score):
    if score >= 75: return "🔥 High"
    if score >= 55: return "✅ Medium"
    return "⚠️ Low"

def dual_card(tic, last_px, y_close, gap_pct, premkt_vol, trend_note, conf_score):
    arrow = "🚀" if gap_pct > 0 else "🔻"
    stars = "⭐" * (1 + int(conf_score>=55) + int(conf_score>=65) + int(conf_score>=75) + int(conf_score>=90))
    stars = stars + "☆" * (5 - len(stars))
    label = label_for(conf_score)
    return (
f"{arrow} ${tic} gapped {gap_pct:.1f}% pre-market — **{label} (score {conf_score})**\n"
f"📈 Price: ${last_px:.2f} (prior close ${y_close:.2f})  •  Vol: {premkt_vol:,}\n"
f"Trend: {trend_note}\n"
f"⭐ Rating: {stars}\n\n"
"👉 Use a plan: scale entries on pullbacks; honor stops; partials at targets.\n"
"⚠️ Educational only — not financial advice.\n"
"---\n"
"⚡ Powered by Rudis"
)

# ---------- Main ----------
def main():
    now = datetime.now(ET)
    premkt_start = now.replace(hour=4, minute=0, second=0, microsecond=0)
    premkt_end   = now if now.hour < 9 or (now.hour == 9 and now.minute < 30) else now.replace(hour=9, minute=29)

    # cache (reset each day)
    cache = load_cache()
    today_key = now.strftime("%Y-%m-%d")
    if cache.get("date") != today_key:
        cache = {"date": today_key, "posted": []}

    # 1) prior close from last session (minutes, IEX)
    yclose = last_session_close_from_minutes(WATCH)
    if not yclose:
        print("No y-closes found in last few sessions.")
        save_cache(cache)
        return

    # 2) today's premarket minutes (IEX)
    mreq = StockBarsRequest(
        symbol_or_symbols=WATCH,
        timeframe=TimeFrame.Minute,
        start=premkt_start, end=premkt_end,
        adjustment="raw",
        feed="iex"
    )
    premkt = client.get_stock_bars(mreq).data

    # 3) compute + filter
    hits = []
    for sym, bars in premkt.items():
        if sym not in yclose or not bars: 
            continue
        if sym in cache["posted"]:
            continue
        last_px = bars[-1].close
        if not (PRICE_MIN <= last_px <= PRICE_MAX):
            continue
        vol = sum(b.volume for b in bars)
        gap_pct = (last_px / yclose[sym] - 1.0) * 100.0
        if abs(gap_pct) < GAP_MIN or vol < PREMKT_VOL_MIN:
            continue
        tnote, tscore = trend_bits(sym, last_px)
        conf = score_confidence(gap_pct, vol, last_px, tscore)
        hits.append((conf, abs(gap_pct), vol, sym, last_px, yclose[sym], gap_pct, tnote))

    # 4) rank + post (confidence → gap → volume)
    hits.sort(reverse=True)
    for conf, _, _, sym, last_px, yc, gap, tnote in hits[:TOP_N]:
        premkt_vol = sum(b.volume for b in premkt[sym])
        post(dual_card(sym, last_px, yc, gap, premkt_vol, tnote, conf))
        cache["posted"].append(sym)

    save_cache(cache)

if __name__ == "__main__":
    main()
