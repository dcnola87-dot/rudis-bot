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
EARLY_TIMEFRAME = os.getenv("RTH_EARLY_TIMEFRAME", "1Min")
CONFIRMED_TIMEFRAME = os.getenv("RTH_CONFIRMED_TIMEFRAME", "5Min")
EARLY_LOOKBACK_BARS = int(os.getenv("RTH_EARLY_LOOKBACK_BARS", "30"))
CONFIRMED_LOOKBACK_BARS = int(os.getenv("RTH_CONFIRMED_LOOKBACK_BARS", "24"))
MIN_LAST_VOL = int(os.getenv("RTH_MIN_LAST_VOL", "20000"))        # ignore tiny prints
VOL_SPIKE_X = float(os.getenv("RTH_VOL_SPIKE_X", "2.5"))          # current bar vol must be >= avg_vol * this
NEAR_HIGH_PCT = float(os.getenv("RTH_NEAR_HIGH_PCT", "0.98"))     # close must be within X% of recent high
MAX_SYMBOLS = int(os.getenv("RTH_MAX_SYMBOLS", "75"))             # baseline cap to avoid rate limits
PREMARKET_MAX_SYMBOLS = int(os.getenv("RTH_PREMARKET_MAX_SYMBOLS", "200"))
RTH_RANKED_POOL = int(os.getenv("RTH_RANKED_POOL", "100"))        # how many ranked names to request before trimming
RTH_MOST_ACTIVES_TOP = int(os.getenv("RTH_MOST_ACTIVES_TOP", "50"))
RTH_MOVERS_TOP = int(os.getenv("RTH_MOVERS_TOP", "25"))
RTH_TOP_GAINERS_TOP = int(os.getenv("RTH_TOP_GAINERS_TOP", "30"))
RTH_TOP_RVOL_TOP = int(os.getenv("RTH_TOP_RVOL_TOP", "40"))
RTH_SUB10_MOMO_TOP = int(os.getenv("RTH_SUB10_MOMO_TOP", "40"))
RTH_DISCOVERY_ACTIVE_SAMPLE = int(os.getenv("RTH_DISCOVERY_ACTIVE_SAMPLE", "500"))
DEBUG_RTH = os.getenv("STOCK_DEBUG", os.getenv("RTH_DEBUG", "0")) == "1"
DEBUG_MISSES = os.getenv("STOCK_DEBUG_MISSES", "0") == "1"
DEBUG_MISS_SYMBOLS = {
    s.strip().upper()
    for s in (os.getenv("STOCK_DEBUG_SYMBOLS", "") or "").split(",")
    if s.strip()
}
POST_NO_SIGNAL = os.getenv("STOCK_POST_NO_SIGNAL", os.getenv("RTH_POST_NO_SIGNAL", "0")) == "1"
REJECTION_SUMMARY_LIMIT = int(os.getenv("RTH_REJECTION_SUMMARY_LIMIT", "10"))
ALLOWED_SIGNALS = {
    s.strip().upper()
    for s in (os.getenv("RTH_ALLOWED_SIGNALS", "WATCH,EARLY,CONFIRMED,EXTENDED,FADING") or "").split(",")
    if s.strip()
}

SESSION_SIGNAL_META = {
    "WATCH": {
        "building": True,
    },
    "EARLY": {
        "building": True,
    },
    "CONFIRMED": {
        "building": False,
    },
    "EXTENDED": {
        "building": False,
    },
    "FADING": {
        "building": False,
    },
}

TIER_PROFILES = {
    "CONFIRMED": {
        "emoji": "✅",
        "label": "CONFIRMED",
        "price_min": 2.0,
        "price_max": 25.0,
        "min_daily_vol": 750_000,
        "min_rvol": 3.0,
        "min_pct": 8.0,
        "min_spike": max(VOL_SPIKE_X, 2.0),
        "max_pct": None,
        "timeframe": CONFIRMED_TIMEFRAME,
        "lookback_bars": CONFIRMED_LOOKBACK_BARS,
        "require_vwap_hold": True,
        "tagline": "Gap-and-go / continuation",
    },
    "WATCH": {
        "emoji": "👀",
        "label": "WATCH",
        "price_min": 0.30,
        "price_max": 25.0,
        "min_daily_vol": 250_000,
        "min_rvol": 1.5,
        "min_pct": 2.0,
        "min_spike": 1.2,
        "max_pct": 12.0,
        "timeframe": EARLY_TIMEFRAME,
        "lookback_bars": EARLY_LOOKBACK_BARS,
        "require_vwap_hold": False,
        "tagline": "Setting up",
    },
    "EXTENDED": {
        "emoji": "🟠",
        "label": "EXTENDED",
        "price_min": 1.0,
        "price_max": 25.0,
        "min_daily_vol": 500_000,
        "min_rvol": 2.0,
        "min_pct": 12.0,
        "min_spike": 1.3,
        "max_pct": None,
        "timeframe": EARLY_TIMEFRAME,
        "lookback_bars": EARLY_LOOKBACK_BARS,
        "require_vwap_hold": False,
        "tagline": "Late extension / do not chase",
    },
    "CASINO": {
        "emoji": "🎰",
        "label": "CASINO",
        "price_min": 0.30,
        "price_max": 8.0,
        "min_daily_vol": 250_000,
        "min_rvol": 3.0,
        "min_pct": 8.0,
        "min_spike": 1.5,
        "max_pct": None,
        "timeframe": EARLY_TIMEFRAME,
        "lookback_bars": EARLY_LOOKBACK_BARS,
        "require_vwap_hold": False,
        "tagline": "Low Float Squeeze",
    },
    "EARLY": {
        "emoji": "🟡",
        "label": "EARLY",
        "price_min": 1.0,
        "price_max": 25.0,
        "min_daily_vol": 500_000,
        "min_rvol": 2.0,
        "min_pct": 3.0,
        "min_spike": 1.5,
        "max_pct": 12.0,
        "timeframe": EARLY_TIMEFRAME,
        "lookback_bars": EARLY_LOOKBACK_BARS,
        "require_vwap_hold": False,
        "tagline": "Building",
    },
}

FLOAT_CANDIDATES_PATH = Path(os.getenv("RTH_FLOAT_CANDIDATES_PATH", "float_candidates.csv"))
LOW_FLOAT_WATCHLIST_PATH = Path(os.getenv("RTH_LOW_FLOAT_WATCHLIST_PATH", str(FLOAT_CANDIDATES_PATH)))

def discord(msg: str):
    if not WEBHOOK:
        return False
    try:
        requests.post(WEBHOOK, json={"content": msg}, timeout=10)
        return True
    except Exception:
        return False


def debug_miss_enabled(symbol: str | None = None) -> bool:
    if not DEBUG_MISSES:
        return False
    if symbol is None:
        return True
    return symbol.upper() in DEBUG_MISS_SYMBOLS


def debug_miss(symbol: str, stage: str, detail: str):
    if not debug_miss_enabled(symbol):
        return
    print(f"[MISS_DEBUG] {symbol.upper()} | {stage} | {detail}", flush=True)


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


def fetch_gainer_symbols():
    if not ALPACA_KEY or not ALPACA_SECRET:
        return []
    params = {
        "top": max(1, min(RTH_TOP_GAINERS_TOP, 50)),
    }
    try:
        r = requests.get(MOVERS_URL, headers=alpaca_headers(), params=params, timeout=ALPACA_TIMEOUT)
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: gainers HTTP {r.status_code} top={params['top']}")
        r.raise_for_status()
        payload = r.json() or {}
        gainers = payload.get("gainers") or []
        return _clean_symbols(str(item.get("symbol") or "") for item in gainers if isinstance(item, dict))
    except Exception as e:
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: gainers failed: {e}")
        return []


def fetch_ranked_symbols():
    ranked = _clean_symbols(fetch_most_active_symbols() + fetch_mover_symbols())
    if ranked:
        return ranked[:RTH_RANKED_POOL]
    return []


def load_low_float_watchlist() -> list[str]:
    if not LOW_FLOAT_WATCHLIST_PATH.exists():
        return []
    try:
        with LOW_FLOAT_WATCHLIST_PATH.open("r", newline="") as f:
            reader = csv.DictReader(f)
            return _clean_symbols(str(row.get("symbol") or "") for row in reader)
    except Exception as e:
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: low-float watchlist load failed: {e}")
        return []


def _snapshot_metric(snapshot: dict) -> dict | None:
    try:
        price = float(snapshot["latestTrade"]["p"])
        prev_close = float(snapshot["prevDailyBar"]["c"])
        volume = float(snapshot["dailyBar"]["v"])
        prev_volume = float(snapshot["prevDailyBar"]["v"])
    except (KeyError, TypeError, ValueError):
        return None
    if price <= 0 or prev_close <= 0:
        return None
    pct_change = (price - prev_close) / prev_close * 100.0
    rvol = (volume / prev_volume) if prev_volume > 0 else 0.0
    return {
        "price": price,
        "pct_change": pct_change,
        "rvol": rvol,
        "volume": volume,
    }


def rank_snapshot_symbols(symbols: list[str]) -> tuple[list[str], list[str]]:
    if not symbols or not ALPACA_KEY or not ALPACA_SECRET:
        return [], []

    metrics = []
    chunk_size = 50
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        try:
            snapshots = fetch_snapshots(chunk)
        except Exception as e:
            if DEBUG_RTH:
                discord(f"🧪 RTH debug: snapshot ranking failed: {e}")
            break
        for sym in chunk:
            metric = _snapshot_metric(snapshots.get(sym) or {})
            if metric is None:
                continue
            metrics.append((sym, metric))

    top_rvol = [
        sym for sym, metric in sorted(
            metrics,
            key=lambda item: (item[1]["rvol"], item[1]["pct_change"], item[1]["volume"]),
            reverse=True,
        )
        if metric["rvol"] > 1.0 and metric["pct_change"] > 0
    ][:RTH_TOP_RVOL_TOP]

    sub10_momo = [
        sym for sym, metric in sorted(
            metrics,
            key=lambda item: (item[1]["pct_change"], item[1]["rvol"], item[1]["volume"]),
            reverse=True,
        )
        if 0.30 <= metric["price"] <= 10.0 and metric["pct_change"] > 0
    ][:RTH_SUB10_MOMO_TOP]

    return top_rvol, sub10_momo

def load_symbols():
    """
    Tries, in priority order:
      1) env var RTH_WATCHLIST="AAPL,TSLA,..."
      2) Alpaca ranked lists (most active + movers)
      3) dynamic Alpaca active assets universe
      4) .assets_cache.json with { "symbols": [...] }
      5) fallback small list
    """
    session_cap = PREMARKET_MAX_SYMBOLS if _is_premarket_or_opening_window() else MAX_SYMBOLS

    # 1) explicit watchlist override
    wl = os.getenv("RTH_WATCHLIST", "").strip()
    if wl:
        syms = _clean_symbols(wl.split(","))
        if syms:
            return syms[:session_cap]

    # 2) discovery sources: most active, top % gainers, snapshot-ranked RVOL, sub-$10 movers, low-float list
    if SYMBOL_SOURCE != "watchlist":
        active = fetch_active_symbols()
        most_active = fetch_most_active_symbols()
        top_gainers = fetch_gainer_symbols()
        low_float = load_low_float_watchlist()

        discovery_pool = _clean_symbols(
            top_gainers
            + most_active
            + low_float
            + active[:max(RTH_DISCOVERY_ACTIVE_SAMPLE, session_cap)]
        )
        top_rvol, sub10_momo = rank_snapshot_symbols(discovery_pool)

        syms = _clean_symbols(
            top_gainers
            + top_rvol
            + sub10_momo
            + low_float
            + most_active
            + active
        )
        if DEBUG_RTH:
            discord(
                "🧪 RTH debug: discovery "
                f"gainers={len(top_gainers)} rvol={len(top_rvol)} "
                f"sub10={len(sub10_momo)} low_float={len(low_float)} "
                f"most_active={len(most_active)} active={len(active)} cap={session_cap}"
            )
        if syms:
            if debug_miss_enabled():
                final_syms = syms[:session_cap]
                discovery_sets = {
                    "top_gainers": set(top_gainers),
                    "top_rvol": set(top_rvol),
                    "sub10_momo": set(sub10_momo),
                    "low_float": set(low_float),
                    "most_active": set(most_active),
                    "active": set(active),
                }
                for symbol in sorted(DEBUG_MISS_SYMBOLS):
                    membership = [
                        name for name, values in discovery_sets.items()
                        if symbol in values
                    ]
                    if symbol in final_syms:
                        debug_miss(
                            symbol,
                            "universe",
                            f"included final_rank={final_syms.index(symbol) + 1}/{len(final_syms)} sources={membership or ['none']}",
                        )
                    else:
                        reason = "not in discovery sources"
                        if symbol in syms:
                            reason = f"cut by session cap rank={syms.index(symbol) + 1} cap={session_cap}"
                        debug_miss(
                            symbol,
                            "universe",
                            f"excluded reason={reason} sources={membership or ['none']}",
                        )
            return syms[:session_cap]

    # 4) assets cache fallback
    try:
        with ASSETS_CACHE_PATH.open("r") as f:
            data = json.load(f)
        syms = _clean_symbols(data.get("symbols") or [])
        if syms:
            return syms[:session_cap]
    except Exception:
        pass

    # 5) fallback
    return ["AAPL", "TSLA", "NVDA", "AMD", "META"][:session_cap]

def alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_KEY or "",
        "APCA-API-SECRET-KEY": ALPACA_SECRET or "",
    }

def _timeframe_minutes(timeframe: str) -> int:
    tf = timeframe.strip().lower()
    if tf.endswith("min"):
        try:
            return max(int(tf[:-3]), 1)
        except ValueError:
            return 1
    return 5


def fetch_bars(symbols, timeframe: str, lookback_bars: int):
    """
    Uses Alpaca data API v2 multi-symbol bars for the requested timeframe.
    """
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Missing ALPACA_KEY / ALPACA_SECRET in .env")

    end = datetime.now(CT)
    start = end - timedelta(minutes=_timeframe_minutes(timeframe) * (lookback_bars + 2))

    params = {
        "symbols": ",".join(symbols),
        "timeframe": timeframe,
        "start": start.astimezone().isoformat(),
        "end": end.astimezone().isoformat(),
        "limit": lookback_bars + 2,
        "adjustment": "raw",
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


def evaluate_prefilter(symbol: str, snapshot: dict) -> dict:
    """Broad liquidity gate before tier-specific logic runs."""
    try:
        price = float(snapshot["latestTrade"]["p"])
        volume = int(snapshot["dailyBar"]["v"])
        vwap = float(snapshot["dailyBar"]["vw"])
        vwap_distance = abs(price - vwap) / vwap if vwap > 0 else math.inf
        reasons = []

        if not (0.30 <= price <= 25.00):
            reasons.append(f"price {price:.2f} outside 0.30-25.00")
        if volume < 250_000:
            reasons.append(f"volume {volume:,} < 250,000")
        if vwap <= 0:
            reasons.append("VWAP missing")
        elif vwap_distance > 0.40:
            reasons.append(f"VWAP distance {vwap_distance:.1%} > 40%")
        if symbol.upper() in FLOAT_CANDIDATES:
            return {
                "passed": not reasons,
                "reasons": reasons,
                "price": price,
                "volume": volume,
                "vwap": vwap,
                "vwap_distance": vwap_distance,
            }
        if FLOAT_CANDIDATES:
            if DEBUG_RTH:
                discord(f"🧪 RTH debug: {symbol} not in float candidates, allowing pass-through as unknown")
        elif DEBUG_RTH:
            discord(f"🧪 RTH debug: float candidates list empty, allowing {symbol} through as unknown")
        return {
            "passed": not reasons,
            "reasons": reasons,
            "price": price,
            "volume": volume,
            "vwap": vwap,
            "vwap_distance": vwap_distance,
        }
    except (KeyError, TypeError, ValueError):
        if DEBUG_RTH:
            discord(f"🧪 RTH debug: prefilter skip {symbol} missing snapshot fields")
        return {
            "passed": False,
            "reasons": ["snapshot fields missing"],
            "price": None,
            "volume": None,
            "vwap": None,
            "vwap_distance": None,
        }


def evaluate_bar_metrics(bars: list[dict], lookback_bars: int) -> dict:
    if not bars or len(bars) < 3:
        return {"ok": False, "reasons": ["not enough bars"]}

    cur = bars[-1]
    cur_v = float(cur.get("v") or 0)
    if cur_v < MIN_LAST_VOL:
        return {
            "ok": False,
            "current_bar_volume": cur_v,
            "reasons": [f"current bar volume {cur_v:,.0f} < {MIN_LAST_VOL:,}"],
        }

    prior = bars[-(lookback_bars + 1):-1]
    vols = [float(b.get("v") or 0) for b in prior]
    avg_v = sum(vols) / max(len(vols), 1)
    if avg_v <= 0:
        return {"ok": False, "reasons": ["average bar volume unavailable"]}

    spike = cur_v / avg_v
    highs = [float(b.get("h") or 0) for b in bars[-lookback_bars:]]
    recent_high = max(highs) if highs else 0.0
    return {
        "ok": True,
        "current_bar_volume": cur_v,
        "avg_bar_volume": avg_v,
        "spike": spike,
        "recent_high": recent_high,
    }


def _is_premarket_or_opening_window() -> bool:
    now = datetime.now(ET)
    if now.hour < 9 or (now.hour == 9 and now.minute < 30):
        return True
    return now.hour == 9 and now.minute < 45


def choose_signal_and_tier(
    price: float,
    pct_change: float,
    rvol: float,
    volume: int,
    vwap: float,
    metrics_by_timeframe: dict,
    is_float_candidate: bool,
) -> tuple[str | None, str | None, list[str]]:
    reasons = []

    if rvol < 1.2 and pct_change > 10:
        return "FADING", None, ["classified FADING: low RVOL versus extended move"]

    if 1.5 <= rvol < 2.0 and 2.0 <= pct_change < 3.0 and volume >= 250_000:
        return "WATCH", "WATCH", []

    early_metrics = metrics_by_timeframe.get(EARLY_TIMEFRAME) or {}
    confirmed_metrics = metrics_by_timeframe.get(CONFIRMED_TIMEFRAME) or {}

    for tier_name in ("CONFIRMED", "EXTENDED", "CASINO", "EARLY"):
        tier = TIER_PROFILES[tier_name]
        tier_reasons = []
        metrics = metrics_by_timeframe.get(tier["timeframe"]) or {}
        if not metrics.get("ok"):
            tier_reasons.extend(metrics.get("reasons") or ["bar metrics unavailable"])
        if not (tier["price_min"] <= price <= tier["price_max"]):
            tier_reasons.append(f"price {price:.2f} outside {tier['price_min']:.2f}-{tier['price_max']:.2f}")
        if volume < tier["min_daily_vol"]:
            tier_reasons.append(f"volume {volume:,} < {tier['min_daily_vol']:,}")
        if rvol < tier["min_rvol"]:
            tier_reasons.append(f"RVOL {rvol:.1f}x < {tier['min_rvol']:.1f}x")
        if pct_change < tier["min_pct"]:
            tier_reasons.append(f"pct move {pct_change:+.1f}% < {tier['min_pct']:.1f}%")
        max_pct = tier.get("max_pct")
        if max_pct is not None and pct_change > max_pct:
            tier_reasons.append(f"pct move {pct_change:+.1f}% > {max_pct:.1f}% anti-chase")
        spike = float(metrics.get("spike") or 0.0)
        if spike < tier["min_spike"]:
            tier_reasons.append(f"bar spike {spike:.1f}x < {tier['min_spike']:.1f}x")
        if tier["require_vwap_hold"] and price < vwap:
            tier_reasons.append(f"below VWAP by {(vwap - price) / vwap:.1%}")
        if tier_name == "EARLY" and vwap > 0 and price < vwap * 0.98:
            tier_reasons.append(f"too far below VWAP ({(vwap - price) / vwap:.1%})")
        if tier_name == "CASINO" and not is_float_candidate and price > 8.0:
            tier_reasons.append("casino profile reserved for sub-$8 squeezes")

        if not tier_reasons:
            session_signal = "CONFIRMED" if tier_name in {"CONFIRMED", "EXTENDED"} else tier_name
            return session_signal, tier_name, []
        reasons.extend(f"{tier_name}: {reason}" for reason in tier_reasons[:2])

    if early_metrics.get("ok") and pct_change > 0 and rvol > 1.0:
        return None, None, reasons or ["not enough early momentum yet"]
    if confirmed_metrics.get("ok") and pct_change > 0:
        return None, None, reasons or ["confirmed setup not ready"]
    return None, None, reasons or ["no qualifying setup"]


def analyze_symbol(sym, bars_by_timeframe, snapshot):
    prefilter = evaluate_prefilter(sym, snapshot)
    if debug_miss_enabled(sym):
        debug_miss(
            sym,
            "prefilter",
            f"price={prefilter.get('price')} volume={prefilter.get('volume')} "
            f"vwap={prefilter.get('vwap')} vwap_distance={prefilter.get('vwap_distance')} "
            f"passed={prefilter.get('passed')} reasons={prefilter.get('reasons') or ['none']}",
        )
    if not prefilter["passed"]:
        debug_miss(sym, "result", f"not_alerted prefilter_failed reasons={prefilter['reasons']}")
        return None, {
            "symbol": sym,
            "score": 0.0,
            "reasons": prefilter["reasons"],
            "price": prefilter.get("price"),
            "volume": prefilter.get("volume"),
            "vwap_distance": prefilter.get("vwap_distance"),
        }

    try:
        price = float(snapshot["latestTrade"]["p"])
        volume = int(snapshot["dailyBar"]["v"])
        vwap = float(snapshot["dailyBar"]["vw"])
        prev_day_volume = int(snapshot["prevDailyBar"]["v"])
        prev_close = float(snapshot["prevDailyBar"]["c"])
    except (KeyError, TypeError, ValueError):
        return None, {
            "symbol": sym,
            "score": 0.0,
            "reasons": ["snapshot fields missing during analysis"],
        }

    metrics_by_timeframe = {}
    for timeframe, lookback in (
        (EARLY_TIMEFRAME, EARLY_LOOKBACK_BARS),
        (CONFIRMED_TIMEFRAME, CONFIRMED_LOOKBACK_BARS),
    ):
        bars = bars_by_timeframe.get(timeframe) or []
        metrics_by_timeframe[timeframe] = evaluate_bar_metrics(bars, lookback)

    pct = (price - prev_close) / prev_close * 100 if prev_close else 0
    rvol = (volume / prev_day_volume) if prev_day_volume > 0 else float((metrics_by_timeframe.get(EARLY_TIMEFRAME) or {}).get("spike") or 0.0)
    session_signal, tier, fail_reasons = choose_signal_and_tier(
        price=price,
        pct_change=pct,
        rvol=rvol,
        volume=volume,
        vwap=vwap,
        metrics_by_timeframe=metrics_by_timeframe,
        is_float_candidate=sym.upper() in FLOAT_CANDIDATES,
    )
    early_metrics = metrics_by_timeframe.get(EARLY_TIMEFRAME) or {}
    confirmed_metrics = metrics_by_timeframe.get(CONFIRMED_TIMEFRAME) or {}
    primary_metrics = confirmed_metrics if tier == "CONFIRMED" else early_metrics
    recent_high = float(primary_metrics.get("recent_high") or 0.0)
    near_high = (recent_high > 0) and (price >= recent_high * NEAR_HIGH_PCT)
    spike = float(primary_metrics.get("spike") or 0.0)
    if debug_miss_enabled(sym):
        debug_miss(
            sym,
            "metrics",
            f"pct_change={pct:+.2f}% rvol={rvol:.2f} volume={volume} "
            f"early_ok={early_metrics.get('ok')} early_spike={float(early_metrics.get('spike') or 0.0):.2f} "
            f"confirmed_ok={confirmed_metrics.get('ok')} confirmed_spike={float(confirmed_metrics.get('spike') or 0.0):.2f} "
            f"recent_high={recent_high:.4f} near_high={near_high}",
        )
    reject_score = 0.0
    reject_score += min(max(pct, 0.0) / 12.0, 1.0) * 30.0
    reject_score += min(max(rvol, 0.0) / 3.0, 1.0) * 25.0
    reject_score += min(max(float(early_metrics.get("spike") or 0.0), 0.0) / 2.0, 1.0) * 25.0
    reject_score += min(max(volume, 0) / 1_000_000.0, 1.0) * 10.0
    if prefilter.get("vwap_distance") is not None:
        reject_score += max(0.0, 1.0 - min(prefilter["vwap_distance"] / 0.10, 1.0)) * 10.0

    rejection = {
        "symbol": sym,
        "score": reject_score,
        "reasons": fail_reasons,
        "price": price,
        "pct_change": pct,
        "rvol": rvol,
        "volume": volume,
        "vwap_distance": prefilter.get("vwap_distance"),
        "bar_spike_1m": float(early_metrics.get("spike") or 0.0),
        "bar_spike_5m": float(confirmed_metrics.get("spike") or 0.0),
    }

    if session_signal is None or tier is None:
        debug_miss(sym, "result", f"not_alerted tier_reject reasons={fail_reasons}")
        return None, rejection
    if session_signal.upper() not in ALLOWED_SIGNALS:
        rejection["reasons"] = [f"signal {session_signal} filtered by RTH_ALLOWED_SIGNALS"]
        debug_miss(sym, "result", f"not_alerted signal_filtered session_signal={session_signal}")
        return None, rejection

    catalyst = None
    if tier == "EARLY":
        catalyst = fetch_recent_8k(sym)

    if debug_miss_enabled(sym):
        debug_miss(
            sym,
            "classification",
            f"session_signal={session_signal} tier={tier} near_high={near_high} "
            f"cooldown_dedupe=not_applicable_in_rth_momentum_scanner",
        )
        if tier == "EXTENDED":
            debug_miss(sym, "result", "classified EXTENDED instead of EARLY/CASINO")
        else:
            debug_miss(sym, "result", f"alerted tier={tier} session_signal={session_signal}")

    return {
        "symbol": sym,
        "price": price,
        "pct_change": pct,
        "rvol": rvol,
        "volume": volume,
        "session_signal": session_signal,
        "tier": tier,
        "filing_type": catalyst["filing_type"] if catalyst else None,
        "is_float_candidate": sym.upper() in FLOAT_CANDIDATES,
        "recent_high": recent_high,
        "near_high": near_high,
        "spike": spike,
        "spike_1m": float(early_metrics.get("spike") or 0.0),
        "spike_5m": float(confirmed_metrics.get("spike") or 0.0),
        "vwap": vwap,
        "vwap_distance": prefilter.get("vwap_distance"),
    }, rejection

def format_msg(sig):
    tier = TIER_PROFILES[sig["tier"]]
    notes = []
    if sig["tier"] == "CASINO":
        notes.append(tier["tagline"])
    elif SESSION_SIGNAL_META.get(sig["session_signal"], {}).get("building"):
        notes.append("Building")
    else:
        notes.append(tier["tagline"])
    if sig.get("filing_type"):
        notes.append(sig["filing_type"])
    return (
        f"{tier['emoji']} **{tier['label']}** | **{sig['symbol']}** | "
        f"{sig['pct_change']:+.1f}% | RVOL {sig['rvol']:.1f}x | "
        f"Vol Spike {sig['spike']:.1f}x"
        f"{' | ' + ' | '.join(notes) if notes else ''}"
    )


def print_rejection_summary(rejections: list[dict]):
    if REJECTION_SUMMARY_LIMIT <= 0:
        return
    eligible = [item for item in rejections if item.get("reasons")]
    if not eligible:
        print("RTH rejects: none")
        return

    ranked = sorted(eligible, key=lambda item: item.get("score", 0.0), reverse=True)[:REJECTION_SUMMARY_LIMIT]
    print("\nRTH rejection summary")
    for item in ranked:
        parts = []
        if item.get("price") is not None:
            parts.append(f"px {item['price']:.2f}")
        if item.get("pct_change") is not None:
            parts.append(f"move {item['pct_change']:+.1f}%")
        if item.get("rvol") is not None:
            parts.append(f"rvol {item['rvol']:.1f}x")
        if item.get("volume") is not None:
            parts.append(f"vol {int(item['volume']):,}")
        if item.get("vwap_distance") is not None:
            parts.append(f"vwap_dist {item['vwap_distance']:.1%}")
        if item.get("bar_spike_1m") is not None:
            parts.append(f"1m_spike {item['bar_spike_1m']:.1f}x")
        if item.get("bar_spike_5m") is not None:
            parts.append(f"5m_spike {item['bar_spike_5m']:.1f}x")
        reason_text = "; ".join(item["reasons"][:3])
        print(f" - {item['symbol']}: {', '.join(parts)} -> {reason_text}")

def main():
    syms = load_symbols()
    if debug_miss_enabled():
        missing = sorted(symbol for symbol in DEBUG_MISS_SYMBOLS if symbol not in syms)
        for symbol in missing:
            debug_miss(symbol, "summary", "not evaluated because symbol was not in final universe")
    if DEBUG_RTH:
        discord(
            "🧪 RTH scanner run: "
            f"early_tf={EARLY_TIMEFRAME} early_lookback={EARLY_LOOKBACK_BARS} "
            f"confirmed_tf={CONFIRMED_TIMEFRAME} confirmed_lookback={CONFIRMED_LOOKBACK_BARS} "
            f"min_vol={MIN_LAST_VOL} spike_x={VOL_SPIKE_X} near_high={NEAR_HIGH_PCT} "
            f"symbols={len(syms)} feed={os.getenv('ALPACA_FEED','iex')}"
        )
    # Chunk symbols to avoid big query strings / rate limits
    CHUNK = 50
    hits = 0
    rejections = []

    for i in range(0, len(syms), CHUNK):
        chunk = syms[i:i+CHUNK]
        try:
            snapshots = fetch_snapshots(chunk)
        except Exception as e:
            discord(f"⚠️ RTH scanner error: {e}")
            return

        try:
            early_bars_map = fetch_bars(chunk, EARLY_TIMEFRAME, EARLY_LOOKBACK_BARS)
            confirmed_bars_map = fetch_bars(chunk, CONFIRMED_TIMEFRAME, CONFIRMED_LOOKBACK_BARS)
        except Exception as e:
            discord(f"⚠️ RTH scanner error: {e}")
            return

        for sym in chunk:
            sig, rejection = analyze_symbol(
                sym,
                {
                    EARLY_TIMEFRAME: early_bars_map.get(sym) or [],
                    CONFIRMED_TIMEFRAME: confirmed_bars_map.get(sym) or [],
                },
                snapshots.get(sym) or {},
            )
            if sig:
                discord(format_msg(sig))
                hits += 1
            elif rejection:
                rejections.append(rejection)

        # small pause between chunks
        time.sleep(0.25)

    print_rejection_summary(rejections)

    if hits == 0 and (POST_NO_SIGNAL or DEBUG_RTH):
        discord("ℹ️ RTH scanner: no signals this run.")

    if DEBUG_RTH:
        discord(f"🧪 RTH scanner finished. hits={hits}")

if __name__ == "__main__":
    main()
