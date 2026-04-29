import csv
import os, json, time, math, requests
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

CT = ZoneInfo("America/Chicago")
ET = ZoneInfo("America/New_York")

WEBHOOK = os.getenv("STOCKS_WEBHOOK")
ALPACA_KEY = os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")
ASSETS_URL = os.getenv("ALPACA_ASSETS_URL", "https://paper-api.alpaca.markets/v2/assets")
MOST_ACTIVES_URL = os.getenv("ALPACA_MOST_ACTIVES_URL", "https://data.alpaca.markets/v1beta1/screener/stocks/most-actives")
MOVERS_URL = os.getenv("ALPACA_MOVERS_URL", "https://data.alpaca.markets/v1beta1/screener/stocks/movers")
SNAPSHOTS_URL = os.getenv("ALPACA_SNAPSHOTS_URL", "https://data.alpaca.markets/v2/stocks/snapshots")
EDGAR_SEARCH_URL = os.getenv("SEC_EDGAR_SEARCH_URL", "https://efts.sec.gov/LATEST/search-index")

# Alpaca Market Data (bars) endpoint
# Default is the official Alpaca data API. You can override with ALPACA_DATA_BARS_URL if needed.
DATA_BASE = os.getenv("ALPACA_DATA_BARS_URL", "https://data.alpaca.markets/v2/stocks/bars")
ALPACA_FEED = os.getenv("ALPACA_FEED", "iex")
ALPACA_TIMEOUT = int(os.getenv("ALPACA_TIMEOUT", "20"))
ASSETS_CACHE_PATH = Path(os.getenv("RTH_ASSETS_CACHE_PATH", ".assets_cache.json"))
SYMBOL_SOURCE = os.getenv("RTH_SYMBOL_SOURCE", "dynamic").strip().lower()

# ---- Tunables (RTH params) ----
TIMEFRAME = os.getenv("RTH_TIMEFRAME", "5Min")
LOOKBACK_BARS = int(os.getenv("RTH_LOOKBACK_BARS", "24"))          # last ~2 hours (24 x 5min)
MIN_LAST_VOL = int(os.getenv("RTH_MIN_LAST_VOL", "20000"))        # ignore tiny prints
VOL_SPIKE_X = float(os.getenv("RTH_VOL_SPIKE_X", "2.5"))          # current bar vol must be >= avg_vol * this
NEAR_HIGH_PCT = float(os.getenv("RTH_NEAR_HIGH_PCT", "0.98"))     # close must be within X% of recent high
MAX_SYMBOLS = int(os.getenv("RTH_MAX_SYMBOLS", "75"))             # cap to avoid rate limits
RTH_RANKED_POOL = int(os.getenv("RTH_RANKED_POOL", "100"))        # how many ranked names to request before trimming
RTH_MOST_ACTIVES_TOP = int(os.getenv("RTH_MOST_ACTIVES_TOP", "50"))
RTH_MOVERS_TOP = int(os.getenv("RTH_MOVERS_TOP", "25"))
DEBUG_RTH = os.getenv("RTH_DEBUG", "0") == "1"
POST_NO_SIGNAL = os.getenv("RTH_POST_NO_SIGNAL", "0") == "1"

SIGNAL_META = {
    "EARLY": {
        "emoji": "⚡",
        "label": "EARLY SIGNAL",
        "note": "Move not yet confirmed - trade at your own risk, not advice.",
    },
    "CONFIRMED": {
        "emoji": "🔥",
        "label": "CONFIRMED",
        "note": "Volume + momentum validated. Second leg potential.",
    },
    "FADING": {
        "emoji": "📉",
        "label": "FADING",
        "note": "Volume dropping off. Move may be exhausted - proceed with caution.",
    },
}

FLOAT_CANDIDATES_PATH = Path(os.getenv("RTH_FLOAT_CANDIDATES_PATH", "float_candidates.csv"))

def discord(msg: str):
    if not WEBHOOK:
        return False
    try:
        requests.post(WEBHOOK, json={"content": msg}, timeout=10)
        return True
    except Exception:
        return False


def _clean_symbols(values):
    out = []
    seen = set()
    for raw in values or []:
        if not isinstance(raw, str):
            continue
        sym = raw.strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out


def load_float_candidates() -> set[str]:
    candidates = set()
    if not FLOAT_CANDIDATES_PATH.exists():
        return candidates

    try:
        with FLOAT_CANDIDATES_PATH.open("r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = str(row.get("symbol") or "").strip().upper()
                if symbol:
                    candidates.add(symbol)
    except Exception as e:
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: float candidates load failed: {e}")
        return set()

    if DEBUG_RTH:
        discord(
            f"🧪 RTH debug: float candidates loaded entries={len(candidates)} "
            f"path={FLOAT_CANDIDATES_PATH}"
        )
    return candidates


FLOAT_CANDIDATES = load_float_candidates()


def fetch_active_symbols():
    if not ALPACA_KEY or not ALPACA_SECRET:
        return []

    params = {
        "status": "active",
        "asset_class": "us_equity",
    }

    try:
        r = requests.get(ASSETS_URL, headers=alpaca_headers(), params=params, timeout=ALPACA_TIMEOUT)
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: assets HTTP {r.status_code} source=alpaca")
        r.raise_for_status()
        payload = r.json()
        symbols = []
        for item in payload if isinstance(payload, list) else []:
            sym = str(item.get("symbol") or "").strip().upper()
            exchange = str(item.get("exchange") or "").strip().upper()
            tradable = bool(item.get("tradable"))
            if not tradable or not sym.isalpha() or not (1 <= len(sym) <= 5):
                continue
            if exchange and exchange not in {"NASDAQ", "NYSE", "ARCA", "AMEX", "BATS"}:
                continue
            symbols.append(sym)

        symbols = _clean_symbols(symbols)
        if symbols:
            try:
                ASSETS_CACHE_PATH.write_text(json.dumps({"symbols": symbols}, indent=2))
            except Exception:
                pass
        return symbols
    except Exception as e:
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: asset load failed: {e}")
        return []


def _extract_ranked_symbols(payload):
    if isinstance(payload, dict):
        for key in ("most_actives", "gainers", "losers"):
            items = payload.get(key) or []
            symbols = _clean_symbols(str(item.get("symbol") or "") for item in items if isinstance(item, dict))
            if symbols:
                return symbols
    if isinstance(payload, list):
        return _clean_symbols(str(item.get("symbol") or "") for item in payload if isinstance(item, dict))
    return []


def fetch_most_active_symbols():
    if not ALPACA_KEY or not ALPACA_SECRET:
        return []
    params = {
        "by": "volume",
        "top": max(1, min(RTH_MOST_ACTIVES_TOP, RTH_RANKED_POOL, 100)),
    }
    try:
        r = requests.get(MOST_ACTIVES_URL, headers=alpaca_headers(), params=params, timeout=ALPACA_TIMEOUT)
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: most-actives HTTP {r.status_code} top={params['top']}")
        r.raise_for_status()
        return _extract_ranked_symbols(r.json())
    except Exception as e:
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: most-actives failed: {e}")
        return []


def fetch_mover_symbols():
    if not ALPACA_KEY or not ALPACA_SECRET:
        return []
    params = {
        "top": max(1, min(RTH_MOVERS_TOP, 50)),
    }
    try:
        r = requests.get(MOVERS_URL, headers=alpaca_headers(), params=params, timeout=ALPACA_TIMEOUT)
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: movers HTTP {r.status_code} top={params['top']}")
        r.raise_for_status()
        payload = r.json()
        gainers = _clean_symbols(str(item.get("symbol") or "") for item in (payload.get("gainers") or []) if isinstance(item, dict))
        losers = _clean_symbols(str(item.get("symbol") or "") for item in (payload.get("losers") or []) if isinstance(item, dict))
        return _clean_symbols(gainers + losers)
    except Exception as e:
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: movers failed: {e}")
        return []


def fetch_ranked_symbols():
    ranked = _clean_symbols(fetch_most_active_symbols() + fetch_mover_symbols())
    if ranked:
        return ranked[:RTH_RANKED_POOL]
    return []

def load_symbols():
    """
    Tries, in priority order:
      1) env var RTH_WATCHLIST="AAPL,TSLA,..."
      2) Alpaca ranked lists (most active + movers)
      3) dynamic Alpaca active assets universe
      4) .assets_cache.json with { "symbols": [...] }
      5) fallback small list
    """
    # 1) explicit watchlist override
    wl = os.getenv("RTH_WATCHLIST", "").strip()
    if wl:
        syms = _clean_symbols(wl.split(","))
        if syms:
            return syms[:MAX_SYMBOLS]

    # 2) ranked market list, then fill from active universe if needed
    if SYMBOL_SOURCE != "watchlist":
        ranked = fetch_ranked_symbols()
        active = fetch_active_symbols()
        syms = _clean_symbols(ranked + active)
        if syms:
            return syms[:MAX_SYMBOLS]

    # 4) assets cache fallback
    try:
        with ASSETS_CACHE_PATH.open("r") as f:
            data = json.load(f)
        syms = _clean_symbols(data.get("symbols") or [])
        if syms:
            return syms[:MAX_SYMBOLS]
    except Exception:
        pass

    # 5) fallback
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


def fetch_snapshots(symbols):
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Missing ALPACA_KEY / ALPACA_SECRET in .env")

    params = {
        "symbols": ",".join(symbols),
        "feed": ALPACA_FEED,
    }
    r = requests.get(SNAPSHOTS_URL, headers=alpaca_headers(), params=params, timeout=ALPACA_TIMEOUT)
    if DEBUG_RTH:
        discord(f"🧪 RTH debug: snapshots HTTP {r.status_code} (symbols={len(symbols)}) feed={ALPACA_FEED}")

    if r.status_code == 403:
        snippet = (r.text or "").strip()
        if len(snippet) > 280:
            snippet = snippet[:280] + "…"
        raise RuntimeError(
            "403 Forbidden from Alpaca snapshots API. "
            "Check that ALPACA_KEY/ALPACA_SECRET are correct for your account, "
            "and that your account has permission for the selected feed (ALPACA_FEED=iex|sip). "
            f"URL={r.url} Response={snippet}"
        )

    r.raise_for_status()
    return r.json() or {}


def fetch_recent_8k(symbol: str) -> dict | None:
    today = datetime.now(ET).date()
    yesterday = today - timedelta(days=1)
    params = {
        "q": f"\"{symbol.upper()}\"",
        "dateRange": "custom",
        "startdt": yesterday.isoformat(),
        "enddt": today.isoformat(),
        "forms": "8-K",
    }
    headers = {
        "User-Agent": "rudis-bot/1.0 contact local",
        "Accept": "application/json",
    }

    try:
        r = requests.get(EDGAR_SEARCH_URL, params=params, headers=headers, timeout=5)
        r.raise_for_status()
        payload = r.json() or {}
        hits = ((payload.get("hits") or {}).get("hits") or [])
        if not hits:
            return None
        source = hits[0].get("_source") or {}
        filed_at = str(source.get("filedAt") or source.get("file_date") or "").strip()
        if filed_at:
            try:
                filed_dt = datetime.fromisoformat(filed_at.replace("Z", "+00:00"))
                if (datetime.now(filed_dt.tzinfo) - filed_dt).total_seconds() > 86400:
                    return None
            except ValueError:
                pass
        filing_type = str(source.get("form") or source.get("display_names") or "8-K").strip() or "8-K"
        return {
            "filing_type": filing_type,
        }
    except Exception:
        return None


def passes_prefilter(symbol: str, snapshot: dict) -> bool:
    """Price, volume, and basic liquidity gate before signal logic runs."""
    try:
        price = float(snapshot["latestTrade"]["p"])
        volume = int(snapshot["dailyBar"]["v"])
        vwap = float(snapshot["dailyBar"]["vw"])

        if not (0.50 <= price <= 5.00):
            return False
        if volume < 500_000:
            return False
        if vwap <= 0:
            return False
        if abs(price - vwap) / vwap > 0.40:
            return False
        if symbol.upper() in FLOAT_CANDIDATES:
            return True
        if FLOAT_CANDIDATES:
            if DEBUG_RTH:
                discord(f"🧪 RTH debug: {symbol} not in float candidates, allowing pass-through as unknown")
        elif DEBUG_RTH:
            discord(f"🧪 RTH debug: float candidates list empty, allowing {symbol} through as unknown")
        return True
    except (KeyError, TypeError, ValueError):
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: prefilter skip {symbol} missing snapshot fields")
        return False


def classify_signal(pct_change: float, rvol: float, volume: int, is_premarket: bool) -> str | None:
    """
    Returns: 'EARLY', 'CONFIRMED', 'FADING', or None (no alert)
    """
    if rvol < 1.5 and pct_change > 10:
        return "FADING"

    if is_premarket or (rvol >= 2.0 and volume < 1_000_000 and pct_change >= 5):
        return "EARLY"

    if rvol >= 3.0 and volume >= 1_000_000 and pct_change >= 5:
        return "CONFIRMED"

    return None


def _is_premarket_or_opening_window() -> bool:
    now = datetime.now(ET)
    if now.hour < 9 or (now.hour == 9 and now.minute < 30):
        return True
    return now.hour == 9 and now.minute < 45

def analyze_symbol(sym, bars, snapshot):
    """
    bars: list of dicts (time-ordered)
    returns None or dict with signal info
    """
    if not bars or len(bars) < 8:
        return None

    if not passes_prefilter(sym, snapshot):
        return None

    # Use the most recent completed-ish bar as "current"
    cur = bars[-1]

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

    highs = [float(b.get("h") or 0) for b in bars[-LOOKBACK_BARS:]]
    recent_high = max(highs) if highs else 0
    try:
        price = float(snapshot["latestTrade"]["p"])
        volume = int(snapshot["dailyBar"]["v"])
        prev_day_volume = int(snapshot["prevDailyBar"]["v"])
        prev_close = float(snapshot["prevDailyBar"]["c"])
    except (KeyError, TypeError, ValueError):
        return None

    near_high = (recent_high > 0) and (price >= recent_high * NEAR_HIGH_PCT)
    pct = (price - prev_close) / prev_close * 100 if prev_close else 0
    rvol = (volume / prev_day_volume) if prev_day_volume > 0 else spike
    signal = classify_signal(pct, rvol, volume, _is_premarket_or_opening_window())
    if signal is None:
        return None
    catalyst = None
    signal_label = signal
    if signal == "EARLY":
        catalyst = fetch_recent_8k(sym)
        if catalyst:
            signal_label = "EARLY + CATALYST"

    return {
        "symbol": sym,
        "price": price,
        "pct_change": pct,
        "rvol": rvol,
        "volume": volume,
        "signal": signal,
        "signal_label": signal_label,
        "filing_type": catalyst["filing_type"] if catalyst else None,
        "is_float_candidate": sym.upper() in FLOAT_CANDIDATES,
        "recent_high": recent_high,
        "near_high": near_high,
    }

def format_msg(sig):
    meta = SIGNAL_META[sig["signal"]]
    candidate_tag = "  ·  LOW-FLOAT CANDIDATE" if sig["symbol"].upper() in FLOAT_CANDIDATES else ""
    label = sig.get("signal_label") or meta["label"]
    filing_note = f" | Filing: {sig['filing_type']}" if sig.get("filing_type") else ""
    return (
        f"{meta['emoji']} **{label}** | **{sig['symbol']}**{candidate_tag}\n"
        f"Price: ${sig['price']:.2f} | Change: {sig['pct_change']:+.2f}% | "
        f"Vol: {sig['volume']:,} ({sig['rvol']:.1f}x RVOL){filing_note}\n"
        f"_{meta['note']}_"
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
            snapshots = fetch_snapshots(chunk)
        except Exception as e:
            discord(f"⚠️ RTH scanner error: {e}")
            return

        candidates = [sym for sym in chunk if passes_prefilter(sym, snapshots.get(sym) or {})]
        if not candidates:
            time.sleep(0.25)
            continue

        try:
            bars_map = fetch_bars(candidates)
        except Exception as e:
            discord(f"⚠️ RTH scanner error: {e}")
            return

        for sym, bars in bars_map.items():
            sig = analyze_symbol(sym, bars, snapshots.get(sym) or {})
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
