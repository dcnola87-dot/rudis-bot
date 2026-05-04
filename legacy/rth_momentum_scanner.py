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
BENZINGA_NEWS_URL = os.getenv("BENZINGA_NEWS_URL", "https://api.benzinga.com/api/v2/news")
BENZINGA_TOKEN = (os.getenv("BENZINGA_TOKEN") or "demo").strip()
BENZINGA_NEWS_LOOKBACK_HOURS = int(os.getenv("BENZINGA_NEWS_LOOKBACK_HOURS", "4"))

# Alpaca Market Data (bars) endpoint
# Default is the official Alpaca data API. You can override with ALPACA_DATA_BARS_URL if needed.
DATA_BASE = os.getenv("ALPACA_DATA_BARS_URL", "https://data.alpaca.markets/v2/stocks/bars")
ALPACA_FEED = os.getenv("ALPACA_FEED", "iex")
ALPACA_TIMEOUT = int(os.getenv("ALPACA_TIMEOUT", "20"))
ALPACA_REQUEST_RETRIES = int(os.getenv("ALPACA_REQUEST_RETRIES", "2"))
ALPACA_RETRY_SLEEP_SEC = float(os.getenv("ALPACA_RETRY_SLEEP_SEC", "1.5"))
ASSETS_CACHE_PATH = Path(os.getenv("RTH_ASSETS_CACHE_PATH", ".assets_cache.json"))
SIGNAL_CACHE_PATH = Path(os.getenv("RTH_SIGNAL_CACHE_PATH", ".rth_signal_cache.json"))
SIGNAL_LOG_PATH = Path(os.getenv("RTH_SIGNAL_LOG_PATH", "logs/stock_signal_calls.jsonl"))
ERROR_CACHE_PATH = Path(os.getenv("RTH_ERROR_CACHE_PATH", ".rth_error_cache.json"))
SYMBOL_SOURCE = os.getenv("RTH_SYMBOL_SOURCE", "dynamic").strip().lower()

# ---- Tunables (RTH params) ----
EARLY_TIMEFRAME = os.getenv("RTH_EARLY_TIMEFRAME", "1Min")
CONFIRMED_TIMEFRAME = os.getenv("RTH_CONFIRMED_TIMEFRAME", "5Min")
EARLY_LOOKBACK_BARS = int(os.getenv("RTH_EARLY_LOOKBACK_BARS", "30"))
CONFIRMED_LOOKBACK_BARS = int(os.getenv("RTH_CONFIRMED_LOOKBACK_BARS", "24"))
MIN_LAST_VOL = int(os.getenv("RTH_MIN_LAST_VOL", "20000"))        # legacy knob; adaptive spike logic is preferred
VOL_SPIKE_X = float(os.getenv("RTH_VOL_SPIKE_X", "2.0"))          # current bar vol must be >= avg_vol * this
NEAR_HIGH_PCT = float(os.getenv("RTH_NEAR_HIGH_PCT", "0.98"))     # close must be within X% of recent high
MAX_SYMBOLS = int(os.getenv("RTH_MAX_SYMBOLS", "750"))            # regular-session cap
PREMARKET_MAX_SYMBOLS = int(os.getenv("RTH_PREMARKET_MAX_SYMBOLS", "1000"))
AFTERHOURS_MAX_SYMBOLS = int(os.getenv("RTH_AFTERHOURS_MAX_SYMBOLS", "500"))
RTH_RANKED_POOL = int(os.getenv("RTH_RANKED_POOL", "100"))        # how many ranked names to request before trimming
RTH_MOST_ACTIVES_TOP = int(os.getenv("RTH_MOST_ACTIVES_TOP", "50"))
RTH_MOVERS_TOP = int(os.getenv("RTH_MOVERS_TOP", "25"))
RTH_TOP_GAINERS_TOP = int(os.getenv("RTH_TOP_GAINERS_TOP", "30"))
RTH_TOP_RVOL_TOP = int(os.getenv("RTH_TOP_RVOL_TOP", "40"))
RTH_SUB10_MOMO_TOP = int(os.getenv("RTH_SUB10_MOMO_TOP", "40"))
RTH_MICROCAP_MOMO_TOP = int(os.getenv("RTH_MICROCAP_MOMO_TOP", "60"))
RTH_DISCOVERY_ACTIVE_SAMPLE = int(os.getenv("RTH_DISCOVERY_ACTIVE_SAMPLE", "500"))
OPENING_FALLBACK_MINUTES = int(os.getenv("RTH_OPENING_FALLBACK_MINUTES", "20"))
SIGNAL_COOLDOWN_SEC = int(os.getenv("RTH_SIGNAL_COOLDOWN_SEC", "900"))
SMART_DEDUPE_WINDOW_SEC = 30 * 60
SMART_DEDUPE_PRICE_MOVE_PCT = 20.0
DEBUG_RTH = os.getenv("STOCK_DEBUG", os.getenv("RTH_DEBUG", "0")) == "1"
DEBUG_MISSES = os.getenv("STOCK_DEBUG_MISSES", "0") == "1"
DEBUG_MISS_SYMBOLS = {
    s.strip().upper()
    for s in (os.getenv("STOCK_DEBUG_SYMBOLS", "") or "").split(",")
    if s.strip()
}
POST_NO_SIGNAL = os.getenv("STOCK_POST_NO_SIGNAL", os.getenv("RTH_POST_NO_SIGNAL", "0")) == "1"
REJECTION_SUMMARY_LIMIT = int(os.getenv("RTH_REJECTION_SUMMARY_LIMIT", "10"))
RTH_CHUNK_SIZE = int(os.getenv("RTH_CHUNK_SIZE", "25"))
ERROR_ALERT_COOLDOWN_SEC = int(os.getenv("RTH_ERROR_ALERT_COOLDOWN_SEC", "900"))
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
        "min_pct": 5.0,
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
        "min_pct": 3.0,
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
        "require_vwap_hold": True,
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
        "require_vwap_hold": True,
        "tagline": "Low Float Squeeze",
    },
    "EARLY": {
        "emoji": "🟡",
        "label": "EARLY",
        "price_min": 1.0,
        "price_max": 25.0,
        "min_daily_vol": 500_000,
        "min_rvol": 2.0,
        "min_pct": 4.0,
        "min_spike": 1.5,
        "max_pct": 12.0,
        "timeframe": EARLY_TIMEFRAME,
        "lookback_bars": EARLY_LOOKBACK_BARS,
        "require_vwap_hold": True,
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


def _cache_template() -> dict:
    return {"day": "", "symbols": {}}


def load_signal_cache() -> dict:
    if SIGNAL_CACHE_PATH.exists():
        try:
            raw = json.loads(SIGNAL_CACHE_PATH.read_text())
            if isinstance(raw, dict):
                raw.setdefault("day", "")
                raw.setdefault("symbols", {})
                return raw
        except Exception:
            pass
    return _cache_template()


def save_signal_cache(cache: dict):
    try:
        SIGNAL_CACHE_PATH.write_text(json.dumps(cache))
    except Exception:
        pass


def load_error_cache() -> dict:
    if ERROR_CACHE_PATH.exists():
        try:
            raw = json.loads(ERROR_CACHE_PATH.read_text())
            if isinstance(raw, dict):
                raw.setdefault("errors", {})
                return raw
        except Exception:
            pass
    return {"errors": {}}


def save_error_cache(cache: dict):
    try:
        ERROR_CACHE_PATH.write_text(json.dumps(cache))
    except Exception:
        pass


def _shorten_text(value: str, limit: int = 220) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def summarize_request_exception(exc: Exception) -> str:
    if isinstance(exc, requests.Timeout):
        return f"timeout after {ALPACA_TIMEOUT}s"
    if isinstance(exc, requests.ConnectionError):
        return "connection error"
    if isinstance(exc, requests.HTTPError):
        response = getattr(exc, "response", None)
        if response is not None:
            return f"HTTP {response.status_code}"
        return "HTTP error"
    return _shorten_text(str(exc), limit=120) or exc.__class__.__name__


def notify_scanner_error(scope: str, exc: Exception, *, details: str = ""):
    summary = summarize_request_exception(exc)
    key = f"{scope}|{summary}|{details}"
    now_ts = time.time()
    cache = load_error_cache()
    last_ts = float((cache.get("errors") or {}).get(key) or 0.0)
    if last_ts and now_ts - last_ts < ERROR_ALERT_COOLDOWN_SEC:
        print(f"[RTH_ERROR_SUPPRESSED] {scope}: {summary} {details}".strip(), flush=True)
        return

    cache.setdefault("errors", {})
    cache["errors"][key] = now_ts
    save_error_cache(cache)

    suffix = f" | {details}" if details else ""
    discord(f"⚠️ RTH scanner {scope}: {summary}{suffix}")
    print(f"[RTH_ERROR] {scope}: {exc} {details}".strip(), flush=True)


def append_signal_log(sig: dict):
    payload = {
        "logged_at_utc": datetime.now(ZoneInfo("UTC")).isoformat(),
        "logged_at_et": datetime.now(ET).isoformat(),
        "symbol": sig["symbol"],
        "session_signal": sig["session_signal"],
        "tier": sig["tier"],
        "price": sig["price"],
        "pct_change": sig["pct_change"],
        "rvol": sig["rvol"],
        "volume": sig["volume"],
        "spike": sig["spike"],
        "spike_1m": sig.get("spike_1m"),
        "spike_5m": sig.get("spike_5m"),
        "vwap": sig.get("vwap"),
        "vwap_distance": sig.get("vwap_distance"),
        "recent_high": sig.get("recent_high"),
        "near_high": sig.get("near_high"),
        "is_float_candidate": sig.get("is_float_candidate"),
        "filing_type": sig.get("filing_type"),
    }
    try:
        SIGNAL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with SIGNAL_LOG_PATH.open("a") as f:
            f.write(json.dumps(payload) + "\n")
    except Exception:
        pass


def reset_signal_cache(cache: dict) -> dict:
    today = datetime.now(CT).strftime("%Y-%m-%d")
    if cache.get("day") != today:
        return {"day": today, "symbols": {}}
    cache.setdefault("symbols", {})
    return cache


def signal_type(sig: dict) -> str:
    return f"{sig['session_signal'].upper()}::{sig['tier'].upper()}"


def should_post_signal(sig: dict, cache: dict, now_ts: float) -> tuple[bool, str]:
    symbol = sig["symbol"].upper()
    sig_type = signal_type(sig)
    current_price = float(sig["price"])
    entry = ((cache.get("symbols") or {}).get(symbol) or {})
    last_type = str(entry.get("signal_type") or "")
    last_price_raw = entry.get("price")
    last_ts = float(entry.get("timestamp") or 0.0)

    if not last_type or last_price_raw in (None, "") or not last_ts:
        return True, symbol

    try:
        last_price = float(last_price_raw)
    except (TypeError, ValueError):
        return True, symbol

    if sig_type != last_type:
        return True, symbol

    if last_price > 0:
        price_move_pct = abs((current_price / last_price - 1.0) * 100.0)
        if price_move_pct >= SMART_DEDUPE_PRICE_MOVE_PCT:
            return True, symbol

    if now_ts - last_ts < SMART_DEDUPE_WINDOW_SEC:
        return False, symbol

    return True, symbol


def mark_signal_posted(cache: dict, symbol: str, sig: dict, now_ts: float):
    cache.setdefault("symbols", {})
    cache["symbols"][symbol.upper()] = {
        "signal_type": signal_type(sig),
        "price": sig["price"],
        "timestamp": now_ts,
    }


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


def rank_snapshot_symbols(symbols: list[str]) -> tuple[list[str], list[str], list[str]]:
    if not symbols or not ALPACA_KEY or not ALPACA_SECRET:
        return [], [], []

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

    microcap_momo = [
        sym for sym, metric in sorted(
            metrics,
            key=lambda item: (
                item[0] in FLOAT_CANDIDATES,
                item[1]["pct_change"],
                item[1]["rvol"],
                item[1]["volume"],
            ),
            reverse=True,
        )
        if 0.30 <= metric["price"] <= 2.0
        and metric["pct_change"] >= 6.0
        and metric["rvol"] >= 1.5
        and metric["volume"] >= 250_000
    ][:RTH_MICROCAP_MOMO_TOP]

    return top_rvol, sub10_momo, microcap_momo


def rank_priority_symbols(symbols: list[str]) -> list[str]:
    if not symbols or not ALPACA_KEY or not ALPACA_SECRET:
        return []

    metrics = []
    chunk_size = 50
    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        try:
            snapshots = fetch_snapshots(chunk)
        except Exception as e:
            if DEBUG_RTH:
                discord(f"🧪 RTH debug: priority ranking failed: {e}")
            break
        for sym in chunk:
            metric = _snapshot_metric(snapshots.get(sym) or {})
            if metric is None:
                continue
            score = 0.0
            score += min(max(metric["pct_change"], 0.0), 60.0) * 2.0
            score += min(max(metric["rvol"], 0.0), 25.0) * 8.0
            score += min(metric["volume"] / 250_000.0, 20.0) * 3.0
            if 0.30 <= metric["price"] <= 10.0:
                score += 80.0
            if 0.30 <= metric["price"] <= 2.0:
                score += 140.0
            if 0.30 <= metric["price"] <= 1.0:
                score += 60.0
            if metric["pct_change"] >= 12.0:
                score += 35.0
            if metric["rvol"] >= 3.0:
                score += 45.0
            if sym in FLOAT_CANDIDATES:
                score += 80.0
            metrics.append((sym, score, metric))

    ranked = sorted(
        metrics,
        key=lambda item: (item[1], item[2]["rvol"], item[2]["pct_change"], item[2]["volume"]),
        reverse=True,
    )
    return [sym for sym, _, _ in ranked]

def load_symbols():
    """
    Tries, in priority order:
      1) env var RTH_WATCHLIST="AAPL,TSLA,..."
      2) Alpaca ranked lists (most active + movers)
      3) dynamic Alpaca active assets universe
      4) .assets_cache.json with { "symbols": [...] }
      5) fallback small list
    """
    session_cap = session_symbol_cap()

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
        recent_news = fetch_recent_benzinga_symbols()

        discovery_pool = _clean_symbols(
            recent_news
            + top_gainers
            + most_active
            + low_float
            + active[:max(RTH_DISCOVERY_ACTIVE_SAMPLE, session_cap)]
        )
        priority = rank_priority_symbols(discovery_pool)
        top_rvol, sub10_momo, microcap_momo = rank_snapshot_symbols(discovery_pool)

        syms = _clean_symbols(
            microcap_momo
            + recent_news
            + priority
            + top_gainers
            + top_rvol
            + sub10_momo
            + low_float
            + most_active
            + active
        )
        if DEBUG_RTH:
            discord(
                "🧪 RTH debug: discovery "
                f"microcap={len(microcap_momo)} news={len(recent_news)} priority={len(priority)} gainers={len(top_gainers)} "
                f"rvol={len(top_rvol)} sub10={len(sub10_momo)} low_float={len(low_float)} "
                f"most_active={len(most_active)} active={len(active)} cap={session_cap}"
            )
        if syms:
            if debug_miss_enabled():
                final_syms = syms[:session_cap]
                discovery_sets = {
                    "microcap_momo": set(microcap_momo),
                    "recent_news": set(recent_news),
                    "priority": set(priority),
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

    r = alpaca_get(
        DATA_BASE,
        params=params,
        request_name=f"bars {timeframe}",
        symbol_count=len(symbols),
    )
    return r.json().get("bars", {})  # dict: { "AAPL": [ {o,h,l,c,v,t...}, ...], ... }


def fetch_snapshots(symbols):
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Missing ALPACA_KEY / ALPACA_SECRET in .env")

    params = {
        "symbols": ",".join(symbols),
        "feed": ALPACA_FEED,
    }
    r = alpaca_get(
        SNAPSHOTS_URL,
        params=params,
        request_name="snapshots",
        symbol_count=len(symbols),
    )
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


def _benzinga_cutoff_ts() -> int:
    return int((datetime.now(ET) - timedelta(hours=max(BENZINGA_NEWS_LOOKBACK_HOURS, 1))).timestamp())


def fetch_recent_benzinga_news(symbol: str) -> dict | None:
    if not BENZINGA_TOKEN:
        return None

    params = {
        "token": BENZINGA_TOKEN,
        "tickers": symbol.upper(),
        "pageSize": 5,
        "displayOutput": "headline",
        "publishedSince": _benzinga_cutoff_ts(),
        "sort": "updated:desc",
    }
    try:
        r = requests.get(BENZINGA_NEWS_URL, params=params, timeout=5)
        r.raise_for_status()
        payload = r.json() or []
        if not isinstance(payload, list) or not payload:
            return None
        item = payload[0] or {}
        title = str(item.get("title") or "").strip()
        created = str(item.get("created") or item.get("updated") or "").strip()
        return {
            "filing_type": "CATALYST",
            "headline": title or "Recent Benzinga headline",
            "source": "benzinga",
            "created": created,
        }
    except Exception:
        return None


def fetch_recent_benzinga_symbols() -> list[str]:
    if not BENZINGA_TOKEN:
        return []

    params = {
        "token": BENZINGA_TOKEN,
        "pageSize": 100,
        "displayOutput": "headline",
        "publishedSince": _benzinga_cutoff_ts(),
        "sort": "updated:desc",
    }
    try:
        r = requests.get(BENZINGA_NEWS_URL, params=params, timeout=6)
        r.raise_for_status()
        payload = r.json() or []
    except Exception:
        return []

    symbols: list[str] = []
    for item in payload if isinstance(payload, list) else []:
        stocks = item.get("stocks") or []
        for stock in stocks:
            sym = str((stock or {}).get("name") or (stock or {}).get("symbol") or "").strip().upper()
            if sym:
                symbols.append(sym)
    return _clean_symbols(symbols)


def evaluate_prefilter(symbol: str, snapshot: dict) -> dict:
    """Broad liquidity gate before tier-specific logic runs."""
    try:
        price = float(snapshot["latestTrade"]["p"])
        volume = int(snapshot["dailyBar"]["v"])
        vwap = float(snapshot["dailyBar"]["vw"])
        prev_close = float(snapshot["prevDailyBar"]["c"])
        prev_volume = float(snapshot["prevDailyBar"]["v"])
        vwap_distance = abs(price - vwap) / vwap if vwap > 0 else math.inf
        pct_change = ((price - prev_close) / prev_close * 100.0) if prev_close > 0 else 0.0
        rvol = (volume / prev_volume) if prev_volume > 0 else 0.0
        reasons = []

        opening_microcap_exception = (
            _is_rth_opening_window()
            and 0.30 <= price <= 2.0
            and volume >= 40_000
            and pct_change >= 8.0
            and rvol >= 1.5
        )

        if not (0.30 <= price <= 25.00):
            reasons.append(f"price {price:.2f} outside 0.30-25.00")
        if volume < 250_000 and not opening_microcap_exception:
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
                "pct_change": pct_change,
                "rvol": rvol,
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
            "pct_change": pct_change,
            "rvol": rvol,
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


def current_market_phase() -> str:
    now = datetime.now(ET)
    mins = now.hour * 60 + now.minute
    pm_open = 4 * 60
    rth_open = 9 * 60 + 30
    midday_start = 11 * 60
    power_hour_start = 14 * 60
    ah_start = 16 * 60
    ah_end = 20 * 60

    if pm_open <= mins < rth_open:
        return "premarket"
    if rth_open <= mins < midday_start:
        return "rth"
    if midday_start <= mins < power_hour_start:
        return "midday"
    if power_hour_start <= mins < ah_start:
        return "rth"
    if ah_start <= mins < ah_end:
        return "afterhours"
    return "offhours"


def session_symbol_cap() -> int:
    phase = current_market_phase()
    if phase == "premarket":
        return PREMARKET_MAX_SYMBOLS
    if phase == "afterhours":
        return AFTERHOURS_MAX_SYMBOLS
    return MAX_SYMBOLS


def session_rvol_thresholds() -> dict[str, float]:
    phase = current_market_phase()
    if phase == "premarket":
        return {"watch": 1.2, "early": 1.5, "confirmed": 2.0}
    if phase == "midday":
        return {"watch": 1.0, "early": 1.2, "confirmed": 1.8}
    if phase == "afterhours":
        return {"watch": 1.2, "early": 1.5, "confirmed": 2.0}
    return {"watch": 1.5, "early": 2.0, "confirmed": 2.5}


def _is_opening_fallback_window() -> bool:
    now = datetime.now(ET)
    if now.hour < 9 or (now.hour == 9 and now.minute < 30):
        return True
    open_minutes = (now.hour * 60 + now.minute) - (9 * 60 + 30)
    return 0 <= open_minutes < OPENING_FALLBACK_MINUTES


def _is_rth_opening_window() -> bool:
    now = datetime.now(ET)
    open_minutes = (now.hour * 60 + now.minute) - (9 * 60 + 30)
    return 0 <= open_minutes < OPENING_FALLBACK_MINUTES


def opening_fallback_metrics(price: float, volume: int, pct_change: float, rvol: float, vwap: float) -> dict:
    recent_high = max(price, vwap) if vwap > 0 else price
    approx_spike = max(rvol, 0.0)
    if volume >= 1_000_000:
        approx_spike = max(approx_spike, 2.0)
    elif volume >= 500_000:
        approx_spike = max(approx_spike, 1.5)
    elif volume >= 250_000:
        approx_spike = max(approx_spike, 1.2)
    return {
        "ok": True,
        "current_bar_volume": float(volume),
        "avg_bar_volume": max(float(volume) / max(approx_spike, 1.0), 1.0),
        "spike": approx_spike,
        "recent_high": recent_high,
        "fallback": True,
        "reasons": [f"opening fallback metrics used pct={pct_change:+.1f}% rvol={rvol:.1f}x"],
    }


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
    opening_fallback = _is_opening_fallback_window()
    rth_opening = _is_rth_opening_window()
    rvol_thresholds = session_rvol_thresholds()

    if rvol < 1.2 and pct_change > 10:
        return "FADING", None, ["classified FADING: low RVOL versus extended move"]

    if rvol_thresholds["watch"] <= rvol < rvol_thresholds["early"] and 3.0 <= pct_change < 4.0 and volume >= 250_000:
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
        min_rvol = tier["min_rvol"]
        if tier_name == "EARLY":
            min_rvol = rvol_thresholds["early"]
            if rth_opening:
                min_rvol = max(1.7, min_rvol - 0.3)
        elif tier_name == "CONFIRMED":
            min_rvol = rvol_thresholds["confirmed"]
            if rth_opening:
                min_rvol = max(2.2, min_rvol - 0.3)
        if rvol < min_rvol:
            tier_reasons.append(f"RVOL {rvol:.1f}x < {min_rvol:.1f}x")
        min_pct = tier["min_pct"]
        if tier_name == "EARLY" and rth_opening:
            min_pct = 3.0
        if pct_change < min_pct:
            tier_reasons.append(f"pct move {pct_change:+.1f}% < {min_pct:.1f}%")
        max_pct = tier.get("max_pct")
        if max_pct is not None and pct_change > max_pct:
            if not (
                opening_fallback
                and tier_name in {"EARLY", "CASINO"}
                and rvol >= 3.0
                and volume >= 500_000
            ):
                tier_reasons.append(f"pct move {pct_change:+.1f}% > {max_pct:.1f}% anti-chase")
        min_spike = tier["min_spike"]
        if tier_name == "CONFIRMED":
            min_spike = 2.0
        elif tier_name == "EARLY":
            min_spike = 1.5
        spike = float(metrics.get("spike") or 0.0)
        if spike < min_spike:
            tier_reasons.append(f"bar spike {spike:.1f}x < {min_spike:.1f}x")
        min_vwap_ratio = 1.0
        if tier_name in {"CONFIRMED", "EXTENDED", "CASINO"}:
            min_vwap_ratio = 0.98
        if tier["require_vwap_hold"] and vwap > 0 and price < vwap * min_vwap_ratio:
            tier_reasons.append(f"below VWAP by {(vwap - price) / vwap:.1%}")
        if tier_name == "EARLY" and vwap > 0:
            min_vwap_hold = 0.98 if not rth_opening else 0.975
            if price < vwap * min_vwap_hold:
                tier_reasons.append(f"too far below VWAP ({(vwap - price) / vwap:.1%})")
        if tier_name == "EARLY":
            recent_high = float(metrics.get("recent_high") or 0.0)
            near_high_floor = 0.985 if not rth_opening else 0.975
            if recent_high > 0 and price < recent_high * near_high_floor:
                tier_reasons.append(f"not close enough to highs ({price / recent_high:.1%} of recent high)")
        if tier_name == "CASINO" and not is_float_candidate and price > 8.0:
            tier_reasons.append("casino profile reserved for sub-$8 squeezes")
        if tier_name == "CASINO" and pct_change <= 0:
            tier_reasons.append("casino requires positive move on the day")

        if opening_fallback and tier_name in {"EARLY", "CASINO"}:
            tier_reasons = [reason for reason in tier_reasons if "not enough bars" not in reason]
            fallback_rvol_gate = max(tier["min_rvol"], 3.0)
            fallback_vol_gate = 750_000
            if rth_opening:
                fallback_rvol_gate = max(tier["min_rvol"] - 0.5, 2.0)
                fallback_vol_gate = 400_000 if tier_name == "CASINO" else 600_000
            if volume >= fallback_vol_gate and rvol >= fallback_rvol_gate:
                tier_reasons = [reason for reason in tier_reasons if not reason.startswith("bar spike ")]

        if not tier_reasons:
            return tier_name, tier_name, []
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

    pct = (price - prev_close) / prev_close * 100 if prev_close else 0
    rvol = (volume / prev_day_volume) if prev_day_volume > 0 else 0.0
    metrics_by_timeframe = {}
    for timeframe, lookback in (
        (EARLY_TIMEFRAME, EARLY_LOOKBACK_BARS),
        (CONFIRMED_TIMEFRAME, CONFIRMED_LOOKBACK_BARS),
    ):
        bars = bars_by_timeframe.get(timeframe) or []
        metrics_by_timeframe[timeframe] = evaluate_bar_metrics(bars, lookback)
        if (
            _is_opening_fallback_window()
            and timeframe == EARLY_TIMEFRAME
            and not metrics_by_timeframe[timeframe].get("ok")
            and volume >= 250_000
            and pct > 0
            and rvol >= 1.5
        ):
            metrics_by_timeframe[timeframe] = opening_fallback_metrics(
                price=price,
                volume=volume,
                pct_change=pct,
                rvol=rvol,
                vwap=vwap,
            )
    if prev_day_volume <= 0:
        rvol = float((metrics_by_timeframe.get(EARLY_TIMEFRAME) or {}).get("spike") or 0.0)
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
        catalyst = fetch_recent_benzinga_news(sym) or fetch_recent_8k(sym)

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
        "catalyst_headline": catalyst.get("headline") if catalyst else None,
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
    if sig.get("catalyst_headline"):
        notes.append(sig["catalyst_headline"])
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


def alpaca_get(url: str, *, params: dict, request_name: str, symbol_count: int):
    attempts = max(1, ALPACA_REQUEST_RETRIES + 1)
    last_exc = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=alpaca_headers(), params=params, timeout=ALPACA_TIMEOUT)
            if DEBUG_RTH:
                discord(
                    f"🧪 RTH debug: {request_name} HTTP {response.status_code} "
                    f"(symbols={symbol_count}) feed={ALPACA_FEED} attempt={attempt}/{attempts}"
                )

            if response.status_code == 403:
                snippet = _shorten_text(response.text or "", limit=160)
                raise RuntimeError(
                    f"{request_name} forbidden (HTTP 403). "
                    "Check ALPACA_KEY/ALPACA_SECRET and market-data permission for "
                    f"ALPACA_FEED={ALPACA_FEED}. Response={snippet}"
                )

            response.raise_for_status()
            return response
        except RuntimeError:
            raise
        except requests.Timeout as exc:
            last_exc = exc
        except requests.ConnectionError as exc:
            last_exc = exc
        except requests.HTTPError as exc:
            last_exc = exc
            break

        if attempt < attempts:
            time.sleep(ALPACA_RETRY_SLEEP_SEC * attempt)

    if last_exc is not None:
        raise RuntimeError(
            f"{request_name} failed after {attempts} attempts "
            f"(symbols={symbol_count}, feed={ALPACA_FEED}): {summarize_request_exception(last_exc)}"
        ) from last_exc
    raise RuntimeError(
        f"{request_name} failed after {attempts} attempts "
        f"(symbols={symbol_count}, feed={ALPACA_FEED})"
    )

def main():
    if datetime.now(ET).weekday() >= 5:
        if DEBUG_RTH:
            discord("🧪 RTH scanner skipped: weekend.")
        return

    syms = load_symbols()
    signal_cache = reset_signal_cache(load_signal_cache())
    now_ts = time.time()
    chunk_failures = 0
    chunk_successes = 0
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
    CHUNK = max(1, RTH_CHUNK_SIZE)
    hits = 0
    rejections = []

    for i in range(0, len(syms), CHUNK):
        chunk = syms[i:i+CHUNK]
        try:
            snapshots = fetch_snapshots(chunk)
        except Exception as e:
            chunk_failures += 1
            notify_scanner_error(
                "snapshot fetch failed",
                e,
                details=f"chunk={i // CHUNK + 1} size={len(chunk)} feed={ALPACA_FEED}",
            )
            continue

        try:
            early_bars_map = fetch_bars(chunk, EARLY_TIMEFRAME, EARLY_LOOKBACK_BARS)
            confirmed_bars_map = fetch_bars(chunk, CONFIRMED_TIMEFRAME, CONFIRMED_LOOKBACK_BARS)
        except Exception as e:
            chunk_failures += 1
            notify_scanner_error(
                "bars fetch failed",
                e,
                details=f"chunk={i // CHUNK + 1} size={len(chunk)} feed={ALPACA_FEED}",
            )
            continue

        chunk_successes += 1

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
                should_post, cache_symbol = should_post_signal(sig, signal_cache, now_ts)
                if should_post:
                    discord(format_msg(sig))
                    append_signal_log(sig)
                    mark_signal_posted(signal_cache, cache_symbol, sig, now_ts)
                    hits += 1
                elif debug_miss_enabled(sig["symbol"]):
                    debug_miss(sig["symbol"], "result", f"not_alerted smart_dedupe symbol={cache_symbol}")
            elif rejection:
                rejections.append(rejection)

        # small pause between chunks
        time.sleep(0.25)

    save_signal_cache(signal_cache)
    print_rejection_summary(rejections)

    if hits == 0 and chunk_successes > 0 and (POST_NO_SIGNAL or DEBUG_RTH):
        discord("ℹ️ RTH scanner: no signals this run.")

    if chunk_failures and DEBUG_RTH:
        discord(
            f"🧪 RTH scanner chunk summary: ok={chunk_successes} failed={chunk_failures} "
            f"chunk_size={CHUNK} retries={ALPACA_REQUEST_RETRIES}"
        )

    if DEBUG_RTH:
        discord(f"🧪 RTH scanner finished. hits={hits}")

if __name__ == "__main__":
    main()
