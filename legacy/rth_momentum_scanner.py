import os, json, time, math, requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

CT = ZoneInfo("America/Chicago")

WEBHOOK = os.getenv("STOCKS_WEBHOOK")
ALPACA_KEY = os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")

# Alpaca Market Data (bars) endpoint
# Default is the official Alpaca data API. You can override with ALPACA_DATA_BARS_URL if needed.
DATA_BASE = os.getenv("ALPACA_DATA_BARS_URL", "https://data.alpaca.markets/v2/stocks/bars")
ALPACA_FEED = os.getenv("ALPACA_FEED", "iex")
ALPACA_TIMEOUT = int(os.getenv("ALPACA_TIMEOUT", "20"))

# ---- Tunables (RTH params) ----
TIMEFRAME = os.getenv("RTH_TIMEFRAME", "5Min")
LOOKBACK_BARS = int(os.getenv("RTH_LOOKBACK_BARS", "24"))          # last ~2 hours (24 x 5min)
MIN_LAST_VOL = int(os.getenv("RTH_MIN_LAST_VOL", "20000"))        # ignore tiny prints
VOL_SPIKE_X = float(os.getenv("RTH_VOL_SPIKE_X", "2.5"))          # current bar vol must be >= avg_vol * this
NEAR_HIGH_PCT = float(os.getenv("RTH_NEAR_HIGH_PCT", "0.98"))     # close must be within X% of recent high
MAX_SYMBOLS = int(os.getenv("RTH_MAX_SYMBOLS", "75"))             # cap to avoid rate limits
DEBUG_RTH = os.getenv("RTH_DEBUG", "0") == "1"
POST_NO_SIGNAL = os.getenv("RTH_POST_NO_SIGNAL", "0") == "1"

def discord(msg: str):
    if not WEBHOOK:
        return False
    try:
        requests.post(WEBHOOK, json={"content": msg}, timeout=10)
        return True
    except Exception:
        return False

def load_symbols():
    """
    Tries:
      1) .assets_cache.json with { "symbols": [...] }
      2) env var RTH_WATCHLIST="AAPL,TSLA,..."
      3) fallback small list
    """
    # 1) assets cache
    try:
        with open(".assets_cache.json", "r") as f:
            data = json.load(f)
        syms = data.get("symbols") or []
        syms = [s.strip().upper() for s in syms if isinstance(s, str)]
        if syms:
            return syms[:MAX_SYMBOLS]
    except Exception:
        pass

    # 2) env watchlist
    wl = os.getenv("RTH_WATCHLIST", "").strip()
    if wl:
        syms = [s.strip().upper() for s in wl.split(",") if s.strip()]
        return syms[:MAX_SYMBOLS]

    # 3) fallback
    return ["AAPL", "TSLA", "NVDA", "AMD", "META"]

def alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_KEY or "",
        "APCA-API-SECRET-KEY": ALPACA_SECRET or "",
    }

def fetch_bars(symbols):
    """
    Uses Alpaca data API v2 multi-symbol bars:
      GET /v2/stocks/bars?symbols=...&timeframe=5Min&start=...&limit=...
    """
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Missing ALPACA_KEY / ALPACA_SECRET in .env")

    end = datetime.now(CT)
    start = end - timedelta(minutes=5 * (LOOKBACK_BARS + 2))

    params = {
        "symbols": ",".join(symbols),
        "timeframe": TIMEFRAME,
        "start": start.astimezone().isoformat(),
        "end": end.astimezone().isoformat(),
        "limit": LOOKBACK_BARS + 2,
        "adjustment": "raw",
        # Most Alpaca accounts require specifying the data feed for market data.
        # `iex` is the safest default; if you have SIP access you can set ALPACA_FEED=sip in .env.
        "feed": ALPACA_FEED,
    }

    r = requests.get(DATA_BASE, headers=alpaca_headers(), params=params, timeout=ALPACA_TIMEOUT)
    if DEBUG_RTH:
        discord(f"🧪 RTH debug: bars HTTP {r.status_code} (symbols={len(symbols)}) feed={ALPACA_FEED}")

    # Alpaca will return 403 if the key is invalid for market data, the plan/feed is not permitted,
    # or the request is otherwise forbidden. Surface a helpful message.
    if r.status_code == 403:
        snippet = (r.text or "").strip()
        if len(snippet) > 280:
            snippet = snippet[:280] + "…"
        raise RuntimeError(
            "403 Forbidden from Alpaca data API. "
            "Check that ALPACA_KEY/ALPACA_SECRET are correct for your account, "
            "and that your account has permission for the selected feed (ALPACA_FEED=iex|sip). "
            f"URL={r.url} Response={snippet}"
        )

    r.raise_for_status()
    return r.json().get("bars", {})  # dict: { "AAPL": [ {o,h,l,c,v,t...}, ...], ... }

def analyze_symbol(sym, bars):
    """
    bars: list of dicts (time-ordered)
    returns None or dict with signal info
    """
    if not bars or len(bars) < 8:
        return None

    # Use the most recent completed-ish bar as "current"
    cur = bars[-1]
    prev = bars[-2]

    cur_v = float(cur.get("v") or 0)
    if cur_v < MIN_LAST_VOL:
        return None

    # avg vol over prior N bars (exclude current)
    prior = bars[-(LOOKBACK_BARS+1):-1]
    vols = [float(b.get("v") or 0) for b in prior]
    avg_v = sum(vols) / max(len(vols), 1)

    if avg_v <= 0:
        return None

    spike = cur_v / avg_v
    if spike < VOL_SPIKE_X:
        return None

    # breakout-ish: close near recent highs
    closes = [float(b.get("c") or 0) for b in bars[-LOOKBACK_BARS:]]
    highs = [float(b.get("h") or 0) for b in bars[-LOOKBACK_BARS:]]
    recent_high = max(highs) if highs else 0
    close = float(cur.get("c") or 0)

    near_high = (recent_high > 0) and (close >= recent_high * NEAR_HIGH_PCT)

    # momentum: percent change vs previous bar close
    prev_close = float(prev.get("c") or 0) or close
    pct = (close - prev_close) / prev_close * 100 if prev_close else 0

    return {
        "symbol": sym,
        "close": close,
        "pct_5m": pct,
        "spike": spike,
        "avg_v": avg_v,
        "cur_v": cur_v,
        "recent_high": recent_high,
        "near_high": near_high,
    }

def format_msg(sig):
    sym = sig["symbol"]
    close = sig["close"]
    pct = sig["pct_5m"]
    spike = sig["spike"]
    cur_v = int(sig["cur_v"])
    avg_v = int(sig["avg_v"])
    tag = "🔥" if sig["near_high"] else "📈"

    # Keep it short so your bot can thread/deep dive off it if desired
    return (
        f"{tag} **RTH WATCH** **{sym}** | "
        f"5m: {pct:+.2f}% | "
        f"Vol spike: {spike:.1f}x ({cur_v:,} vs {avg_v:,}) | "
        f"Last: ${close:.2f}"
    )

def main():
    syms = load_symbols()
    if DEBUG_RTH:
        discord(
            "🧪 RTH scanner run: "
            f"timeframe={TIMEFRAME} lookback={LOOKBACK_BARS} "
            f"min_vol={MIN_LAST_VOL} spike_x={VOL_SPIKE_X} near_high={NEAR_HIGH_PCT} "
            f"symbols={len(syms)} feed={os.getenv('ALPACA_FEED','iex')}"
        )
    # Chunk symbols to avoid big query strings / rate limits
    CHUNK = 50
    hits = 0

    for i in range(0, len(syms), CHUNK):
        chunk = syms[i:i+CHUNK]
        try:
            bars_map = fetch_bars(chunk)
        except Exception as e:
            discord(f"⚠️ RTH scanner error: {e}")
            return

        for sym, bars in bars_map.items():
            sig = analyze_symbol(sym, bars)
            if sig:
                discord(format_msg(sig))
                hits += 1

        # small pause between chunks
        time.sleep(0.25)

    if hits == 0 and (POST_NO_SIGNAL or DEBUG_RTH):
        discord("ℹ️ RTH scanner: no signals this run.")

    if DEBUG_RTH:
        discord(f"🧪 RTH scanner finished. hits={hits}")

if __name__ == "__main__":
    main()