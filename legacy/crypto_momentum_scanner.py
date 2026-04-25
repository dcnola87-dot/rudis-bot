import json
import os
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

CT = ZoneInfo("America/Chicago")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


SCAN_MODE = (os.getenv("CRYPTO_SCAN_MODE") or "graduation").strip().lower()
DEBUG = _env_bool("CRYPTO_DEBUG", False)
POST_NO_SIGNAL = _env_bool("CRYPTO_POST_NO_SIGNAL", False)

CRYPTO_WEBHOOK = (os.getenv("CRYPTO_WEBHOOK") or "").strip()
CRYPTO_CONFIRMED_WEBHOOK = (os.getenv("CRYPTO_CONFIRMED_WEBHOOK") or CRYPTO_WEBHOOK).strip()

DEX_BASE = (os.getenv("DEX_SCREENER_BASE_URL") or "https://api.dexscreener.com/latest/dex").strip().rstrip("/")
HELIUS_API_KEY = (os.getenv("HELIUS_API_KEY") or "").strip()
HELIUS_RPC_URL = (
    os.getenv("HELIUS_RPC_URL")
    or (f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else "")
).strip()
PUMPFUN_GRADUATIONS_URL = (os.getenv("PUMPFUN_GRADUATIONS_URL") or "").strip()
PUMPFUN_GRADUATED_MINTS = (os.getenv("PUMPFUN_GRADUATED_MINTS") or "").strip()
GRADUATED_MINTS_FILE = Path(
    os.getenv("GRADUATED_MINTS_FILE")
    or (Path(__file__).resolve().parents[1] / "logs" / "graduated_mints.txt")
)

GRAD_MIN_LIQUIDITY_USD = _env_float("CRYPTO_GRAD_MIN_LIQUIDITY_USD", 25000.0)
GRAD_MIN_VOLUME_H24_USD = _env_float("CRYPTO_GRAD_MIN_VOLUME_H24_USD", 100000.0)
GRAD_MIN_TXNS_H24 = _env_int("CRYPTO_GRAD_MIN_TXNS_H24", 150)
GRAD_MIN_BUYS_H24 = _env_int("CRYPTO_GRAD_MIN_BUYS_H24", 80)
GRAD_MAX_PAIR_AGE_HOURS = _env_float("CRYPTO_GRAD_MAX_PAIR_AGE_HOURS", 72.0)
GRAD_MIN_SCORE = _env_int("CRYPTO_GRAD_MIN_SCORE", 70)
GRAD_REQUIRE_HELIUS = _env_bool("CRYPTO_GRAD_REQUIRE_HELIUS", False)
GRAD_SEEN_TTL_HOURS = _env_float("CRYPTO_GRAD_SEEN_TTL_HOURS", 24.0)
GRAD_STATE_PATH = Path(os.getenv("CRYPTO_GRAD_STATE_PATH") or (Path(__file__).resolve().parents[1] / "logs" / "crypto_confirmed_seen.json"))


# ---- Binance momentum fallback config ----
_BINANCE_BASE = None
_EXCHANGE_SYMBOLS = None
INTERVAL = os.getenv("CRYPTO_INTERVAL", "5m")
TF_FAST = os.getenv("CRYPTO_TF_FAST", INTERVAL)
TF_SLOW = os.getenv("CRYPTO_TF_SLOW", "15m")
SCORE_IGNITION = _env_int("CRYPTO_SCORE_IGNITION", 80)
SCORE_ROTATION = _env_int("CRYPTO_SCORE_ROTATION", 60)
SCORE_CASINO = _env_int("CRYPTO_SCORE_CASINO", 40)
SLOW_LOOKBACK_BARS = _env_int("CRYPTO_SLOW_LOOKBACK_BARS", 16)
LOOKBACK_BARS = _env_int("CRYPTO_LOOKBACK_BARS", 24)
MIN_QUOTE_VOL = _env_float("CRYPTO_MIN_QUOTE_VOL", 2500000.0)
VOL_SPIKE_X = _env_float("CRYPTO_VOL_SPIKE_X", 2.2)
NEAR_HIGH_PCT = _env_float("CRYPTO_NEAR_HIGH_PCT", 0.985)
MIN_PCT_MOVE = _env_float("CRYPTO_MIN_PCT_MOVE", 0.60)
MAX_SYMBOLS = _env_int("CRYPTO_MAX_SYMBOLS", 80)


def discord(msg: str, *, confirmed: bool = False) -> bool:
    target = CRYPTO_CONFIRMED_WEBHOOK if confirmed else CRYPTO_WEBHOOK
    if not target:
        return False
    try:
        requests.post(target, json={"content": msg}, timeout=10)
        return True
    except Exception:
        return False


def _debug(msg: str):
    if DEBUG:
        discord(f"🧪 crypto debug: {msg}", confirmed=False)


def _resolve_binance_base() -> str:
    global _BINANCE_BASE
    if _BINANCE_BASE:
        return _BINANCE_BASE

    override = (os.getenv("CRYPTO_EXCHANGE_BASE", "") or "").strip().rstrip("/")
    if override:
        _BINANCE_BASE = override
        return _BINANCE_BASE

    exch = (os.getenv("CRYPTO_EXCHANGE", "binanceus") or "binanceus").strip().lower()
    if exch in ("binance", "global", "binance-global"):
        _BINANCE_BASE = "https://api.binance.com"
    else:
        _BINANCE_BASE = "https://api.binance.us"
    return _BINANCE_BASE


def _binance_get(path: str, params: dict | None = None, timeout: int = 15):
    global _BINANCE_BASE, _EXCHANGE_SYMBOLS
    base = _resolve_binance_base()
    url = f"{base}{path}"
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code == 451 and base.endswith("api.binance.com"):
        _BINANCE_BASE = "https://api.binance.us"
        _EXCHANGE_SYMBOLS = None
        base = _BINANCE_BASE
        url = f"{base}{path}"
        r = requests.get(url, params=params, timeout=timeout)
    return r


def get_exchange_symbols() -> set:
    global _EXCHANGE_SYMBOLS
    if _EXCHANGE_SYMBOLS is not None:
        return _EXCHANGE_SYMBOLS
    try:
        r = _binance_get("/api/v3/exchangeInfo", timeout=15)
        r.raise_for_status()
        data = r.json()
        syms = set()
        for item in data.get("symbols", []):
            sym = item.get("symbol")
            status = item.get("status")
            if sym and status == "TRADING":
                syms.add(sym.upper())
        _EXCHANGE_SYMBOLS = syms
        return _EXCHANGE_SYMBOLS
    except Exception as e:
        _EXCHANGE_SYMBOLS = set()
        _debug(f"exchangeInfo failed; symbol validation off. err={e}")
        return _EXCHANGE_SYMBOLS


def load_symbols():
    mode = (os.getenv("CRYPTO_MODE", "static") or "static").strip().lower()
    manual_raw = (os.getenv("CRYPTO_SYMBOLS") or "").strip()
    manual_list = [s.strip().upper() for s in manual_raw.split(",") if s.strip()] if manual_raw else []
    static_fallback = [
        "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
        "AVAXUSDT", "LINKUSDT", "ARBUSDT", "OPUSDT", "FETUSDT", "RNDRUSDT",
    ]

    if mode == "static":
        base = manual_list if manual_list else static_fallback
        return base[:MAX_SYMBOLS]

    try:
        r = _binance_get("/api/v3/ticker/24hr", timeout=15)
        r.raise_for_status()
        data = r.json()
        quote = (os.getenv("CRYPTO_QUOTE") or "USDT").strip().upper()
        top_n = _env_int("CRYPTO_TOP_N", 50)
        min_vol = _env_float("CRYPTO_MIN_QUOTE_VOL_USD", 500000.0)
        exclude = set(s.strip().upper() for s in (os.getenv("CRYPTO_EXCLUDE") or "").split(",") if s.strip())

        def _build_filtered(min_vol_floor: float):
            out = []
            for item in data:
                sym = (item.get("symbol", "") or "").upper()
                if not sym.endswith(quote) or sym in exclude:
                    continue
                if sym.endswith(("UP" + quote, "DOWN" + quote, "BULL" + quote, "BEAR" + quote)):
                    continue
                try:
                    qv = float(item.get("quoteVolume", 0))
                except Exception:
                    continue
                if qv < min_vol_floor:
                    continue
                out.append((sym, qv))
            out.sort(key=lambda x: x[1], reverse=True)
            return out

        filtered = _build_filtered(min_vol)
        relax_floor = min_vol
        for _ in range(3):
            if len(filtered) >= top_n or relax_floor <= 100000:
                break
            relax_floor *= 0.5
            filtered = _build_filtered(relax_floor)

        dynamic_syms = [s for s, _ in filtered[:top_n]]
        valid = get_exchange_symbols()
        if valid:
            dynamic_syms = [s for s in dynamic_syms if s in valid]

        if mode == "dynamic":
            symbols = dynamic_syms[:MAX_SYMBOLS]
        else:
            symbols = []
            seen = set()
            for s in (dynamic_syms + manual_list + static_fallback):
                s = s.upper()
                if s in exclude or not s.endswith(quote):
                    continue
                if valid and s not in valid:
                    continue
                if s not in seen:
                    symbols.append(s)
                    seen.add(s)
                if len(symbols) >= MAX_SYMBOLS:
                    break

        print(f"[CRYPTO] Loaded {len(symbols)} symbols (mode={mode})")
        return symbols
    except Exception as e:
        print("[CRYPTO] Symbol load failed, using fallback:", e)
        base = manual_list if manual_list else static_fallback
        return base[:MAX_SYMBOLS]


def fetch_klines(symbol: str, limit: int, interval: str | None = None):
    use_interval = interval or INTERVAL
    params = {"symbol": symbol, "interval": use_interval, "limit": limit}
    r = _binance_get("/api/v3/klines", params=params, timeout=15)
    _debug(f"{symbol} {use_interval} klines HTTP {r.status_code}")
    r.raise_for_status()
    return r.json()


def _last_complete_bar(klines):
    if not klines or len(klines) < 4:
        return None, None
    return klines[-2], klines[-3]


def _recent_high(klines, bars: int):
    if not klines:
        return 0.0
    window = klines[-(bars + 1):-1] if len(klines) > bars + 1 else klines
    return max(float(k[2]) for k in window)


def _avg_quote_vol(klines, bars: int):
    if not klines:
        return 0.0
    prior = klines[-(bars + 2):-2] if len(klines) > bars + 2 else klines[:-2]
    qvs = [float(k[7]) for k in prior if k and len(k) > 7]
    return (sum(qvs) / max(len(qvs), 1)) if qvs else 0.0


def _sma(values):
    return sum(values) / max(len(values), 1) if values else 0.0


def score_signal(symbol: str, fast_klines, slow_klines):
    if not fast_klines or len(fast_klines) < max(LOOKBACK_BARS + 6, 30):
        return None
    if not slow_klines or len(slow_klines) < max(SLOW_LOOKBACK_BARS + 6, 30):
        return None

    f_cur, f_prev = _last_complete_bar(fast_klines)
    s_cur, s_prev = _last_complete_bar(slow_klines)
    if not f_cur or not f_prev or not s_cur or not s_prev:
        return None

    f_close = float(f_cur[4])
    f_prev_close = float(f_prev[4]) if float(f_prev[4]) != 0 else f_close
    f_quote_vol = float(f_cur[7])
    s_close = float(s_cur[4])

    if f_quote_vol < MIN_QUOTE_VOL:
        return None

    f_avg_qv = _avg_quote_vol(fast_klines, LOOKBACK_BARS)
    if f_avg_qv <= 0:
        return None

    f_spike = f_quote_vol / f_avg_qv
    f_pct = ((f_close - f_prev_close) / f_prev_close) * 100 if f_prev_close else 0.0
    f_rhigh = _recent_high(fast_klines, LOOKBACK_BARS)
    f_near_high = f_close >= f_rhigh * NEAR_HIGH_PCT

    slow_window = slow_klines[-(SLOW_LOOKBACK_BARS + 1):-1] if len(slow_klines) > SLOW_LOOKBACK_BARS + 1 else slow_klines
    slow_closes = [float(k[4]) for k in slow_window if k and len(k) > 4]
    s_sma = _sma(slow_closes[-SLOW_LOOKBACK_BARS:])
    s_trend_up = s_close >= s_sma if s_sma > 0 else False
    s_rhigh = _recent_high(slow_klines, SLOW_LOOKBACK_BARS)
    s_near_high = s_close >= s_rhigh * 0.985

    score = 0
    if f_quote_vol >= MIN_QUOTE_VOL:
        score += 10
    if f_quote_vol >= MIN_QUOTE_VOL * 2:
        score += 5
    if f_quote_vol >= MIN_QUOTE_VOL * 4:
        score += 5
    if f_spike >= 1.3:
        score += 10
    if f_spike >= 1.7:
        score += 10
    if f_spike >= VOL_SPIKE_X:
        score += 15
    if f_spike >= 3.0:
        score += 15
    if f_pct >= 0.25:
        score += 8
    if f_pct >= 0.50:
        score += 8
    if f_pct >= MIN_PCT_MOVE:
        score += 8
    if f_pct >= 1.20:
        score += 10
    if f_near_high:
        score += 12
    if s_trend_up:
        score += 12
    if s_near_high:
        score += 10
    if f_spike < 1.2 and f_pct < 0.25:
        return None

    if score >= SCORE_IGNITION:
        label = "IGNITION"
        emoji = "🔥"
    elif score >= SCORE_ROTATION:
        label = "ROTATION"
        emoji = "📡"
    elif score >= SCORE_CASINO:
        label = "HIGH_BETA"
        emoji = "🎰"
    else:
        return None

    return {
        "symbol": symbol,
        "label": label,
        "emoji": emoji,
        "score": score,
        "fast_interval": TF_FAST,
        "slow_interval": TF_SLOW,
        "fast_close": f_close,
        "fast_pct": f_pct,
        "fast_spike": f_spike,
        "fast_quote_vol": f_quote_vol,
        "fast_avg_qv": f_avg_qv,
        "slow_trend_up": s_trend_up,
    }


def format_momentum_msg(sig):
    trend = "↑" if sig.get("slow_trend_up") else "→"
    return (
        f"{sig['emoji']} **{sig['label']}** **{sig['symbol']}** | Score: **{sig['score']}** | "
        f"{sig['fast_interval']}: {sig['fast_pct']:+.2f}% | "
        f"Vol spike: {sig['fast_spike']:.1f}x (${sig['fast_quote_vol']:,.0f} vs ${sig['fast_avg_qv']:,.0f}) | "
        f"Trend({sig['slow_interval']}): {trend} | Last: {sig['fast_close']:.4f}"
    )


def _requests_get_json(url: str, *, params: dict | None = None, headers: dict | None = None, timeout: int = 12):
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _requests_post_json(url: str, payload: dict, *, headers: dict | None = None, timeout: int = 12):
    r = requests.post(url, json=payload, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _now_ts() -> float:
    return time.time()


def load_seen_state() -> dict[str, float]:
    try:
        if not GRAD_STATE_PATH.exists():
            return {}
        raw = json.loads(GRAD_STATE_PATH.read_text())
        if isinstance(raw, dict):
            return {str(k): float(v) for k, v in raw.items()}
    except Exception:
        pass
    return {}


def save_seen_state(state: dict[str, float]):
    try:
        GRAD_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        GRAD_STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))
    except Exception:
        pass


def prune_seen_state(state: dict[str, float]) -> dict[str, float]:
    ttl = GRAD_SEEN_TTL_HOURS * 3600.0
    now = _now_ts()
    return {mint: ts for mint, ts in state.items() if (now - ts) < ttl}


def parse_candidate_items(payload) -> list[dict]:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        for key in ("items", "data", "mints", "tokens", "results"):
            if isinstance(payload.get(key), list):
                items = payload[key]
                break
        else:
            items = []
    else:
        items = []

    out = []
    for item in items:
        if isinstance(item, str):
            out.append({"mint": item.strip()})
            continue
        if not isinstance(item, dict):
            continue
        mint = str(item.get("mint") or item.get("address") or item.get("tokenAddress") or "").strip()
        if not mint:
            continue
        out.append(
            {
                "mint": mint,
                "symbol": str(item.get("symbol") or "").strip(),
                "name": str(item.get("name") or "").strip(),
                "graduated_at": item.get("graduated_at") or item.get("graduatedAt") or item.get("timestamp"),
                "source": str(item.get("source") or "pumpfun").strip() or "pumpfun",
            }
        )
    return out


def load_graduation_candidates() -> list[dict]:
    candidates: list[dict] = []

    if PUMPFUN_GRADUATED_MINTS:
        for mint in PUMPFUN_GRADUATED_MINTS.split(","):
            m = mint.strip()
            if m:
                candidates.append({"mint": m, "source": "env"})

    if PUMPFUN_GRADUATIONS_URL:
        try:
            payload = _requests_get_json(PUMPFUN_GRADUATIONS_URL, timeout=12)
            candidates.extend(parse_candidate_items(payload))
        except Exception as e:
            _debug(f"graduation feed fetch failed: {e}")

    file_candidates, consumed_lines = load_graduation_candidates_from_file()
    candidates.extend(file_candidates)
    if consumed_lines:
        clear_graduation_file(consumed_lines)

    deduped = []
    seen = set()
    for item in candidates:
        mint = item["mint"]
        if mint in seen:
            continue
        seen.add(mint)
        deduped.append(item)
    return deduped


def load_graduation_candidates_from_file() -> tuple[list[dict], list[str]]:
    if not GRADUATED_MINTS_FILE.exists():
        return [], []

    try:
        raw_lines = GRADUATED_MINTS_FILE.read_text().splitlines()
    except Exception as e:
        _debug(f"graduation file read failed: {e}")
        return [], []

    candidates: list[dict] = []
    consumed: list[str] = []
    for line in raw_lines:
        mint = line.strip()
        if not mint:
            continue
        consumed.append(mint)
        candidates.append({"mint": mint, "source": "helius-file"})
    return candidates, consumed


def clear_graduation_file(consumed_lines: list[str]):
    try:
        if not GRADUATED_MINTS_FILE.exists():
            return
        current_lines = GRADUATED_MINTS_FILE.read_text().splitlines()
        consumed_set = {line.strip() for line in consumed_lines if line.strip()}
        remaining = [line for line in current_lines if line.strip() and line.strip() not in consumed_set]
        GRADUATED_MINTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        GRADUATED_MINTS_FILE.write_text(("\n".join(remaining) + ("\n" if remaining else "")))
    except Exception as e:
        _debug(f"graduation file clear failed: {e}")


def fetch_dex_pair_for_mint(mint: str) -> dict | None:
    url = f"{DEX_BASE}/tokens/{mint}"
    try:
        payload = _requests_get_json(url, timeout=12)
    except Exception as e:
        _debug(f"dex lookup failed for {mint}: {e}")
        return None

    pairs = payload.get("pairs") or []
    sol_pairs = [p for p in pairs if str(p.get("chainId") or "").lower() == "solana"]
    if not sol_pairs:
        return None

    def _score_pair(pair: dict):
        liq = _float((pair.get("liquidity") or {}).get("usd"))
        vol = _float((pair.get("volume") or {}).get("h24"))
        txns = pair.get("txns") or {}
        txns_h24 = txns.get("h24") or {}
        txn_count = _int(txns_h24.get("buys")) + _int(txns_h24.get("sells"))
        return (liq, vol, txn_count)

    sol_pairs.sort(key=_score_pair, reverse=True)
    return sol_pairs[0]


def fetch_helius_asset(mint: str) -> dict | None:
    if not HELIUS_RPC_URL:
        return None

    payload = {
        "jsonrpc": "2.0",
        "id": mint,
        "method": "getAsset",
        "params": {"id": mint},
    }
    try:
        data = _requests_post_json(HELIUS_RPC_URL, payload, headers={"Content-Type": "application/json"}, timeout=12)
        result = data.get("result")
        return result if isinstance(result, dict) else None
    except Exception as e:
        _debug(f"helius getAsset failed for {mint}: {e}")
        return None


def build_graduation_signal(candidate: dict, pair: dict, helius_asset: dict | None) -> dict | None:
    liquidity_usd = _float((pair.get("liquidity") or {}).get("usd"))
    volume_h24 = _float((pair.get("volume") or {}).get("h24"))
    txns_h24 = (pair.get("txns") or {}).get("h24") or {}
    buys_h24 = _int(txns_h24.get("buys"))
    sells_h24 = _int(txns_h24.get("sells"))
    total_txns_h24 = buys_h24 + sells_h24
    price_usd = _float(pair.get("priceUsd"))
    pair_created_at_ms = _float(pair.get("pairCreatedAt"))
    pair_age_hours = max((_now_ts() * 1000.0 - pair_created_at_ms) / 3600000.0, 0.0) if pair_created_at_ms else 0.0

    if liquidity_usd < GRAD_MIN_LIQUIDITY_USD:
        return None
    if volume_h24 < GRAD_MIN_VOLUME_H24_USD:
        return None
    if total_txns_h24 < GRAD_MIN_TXNS_H24:
        return None
    if buys_h24 < GRAD_MIN_BUYS_H24:
        return None
    if pair_age_hours > GRAD_MAX_PAIR_AGE_HOURS:
        return None
    if GRAD_REQUIRE_HELIUS and not helius_asset:
        return None

    score = 0
    score += 20
    if liquidity_usd >= GRAD_MIN_LIQUIDITY_USD:
        score += 15
    if liquidity_usd >= GRAD_MIN_LIQUIDITY_USD * 2:
        score += 10
    if volume_h24 >= GRAD_MIN_VOLUME_H24_USD:
        score += 15
    if volume_h24 >= GRAD_MIN_VOLUME_H24_USD * 2:
        score += 10
    if total_txns_h24 >= GRAD_MIN_TXNS_H24:
        score += 10
    if buys_h24 >= GRAD_MIN_BUYS_H24:
        score += 10
    if buys_h24 > sells_h24:
        score += 5
    if pair_age_hours <= 24:
        score += 10

    token_profile = (helius_asset or {}).get("token_info") or {}
    decimals = _int(token_profile.get("decimals"), 0)
    supply = _float(token_profile.get("supply"))
    symbol = (
        str((pair.get("baseToken") or {}).get("symbol") or "")
        or str(candidate.get("symbol") or "")
        or str((helius_asset or {}).get("content", {}).get("metadata", {}).get("symbol") or "")
    ).strip().upper()
    name = (
        str((pair.get("baseToken") or {}).get("name") or "")
        or str(candidate.get("name") or "")
        or str((helius_asset or {}).get("content", {}).get("metadata", {}).get("name") or "")
    ).strip()

    if score < GRAD_MIN_SCORE:
        return None

    return {
        "mint": candidate["mint"],
        "symbol": symbol or "UNKNOWN",
        "name": name or "Unknown",
        "score": score,
        "liquidity_usd": liquidity_usd,
        "volume_h24": volume_h24,
        "buys_h24": buys_h24,
        "sells_h24": sells_h24,
        "txns_h24": total_txns_h24,
        "price_usd": price_usd,
        "pair_age_hours": pair_age_hours,
        "pair_url": str(pair.get("url") or "").strip(),
        "pair_address": str(pair.get("pairAddress") or "").strip(),
        "dex_id": str(pair.get("dexId") or "").strip(),
        "fdv": _float(pair.get("fdv")),
        "market_cap": _float(pair.get("marketCap")),
        "decimals": decimals,
        "supply": supply,
        "helius_verified": bool(token_profile),
        "source": candidate.get("source") or "pumpfun",
    }


def format_graduation_msg(sig: dict) -> str:
    conviction = "HIGH" if sig["score"] >= max(GRAD_MIN_SCORE + 15, 85) else "CONFIRMED"
    return (
        f"🚀 **PUMP GRADUATION {conviction}** **{sig['symbol']}** | "
        f"Score: **{sig['score']}** | Liquidity: ${sig['liquidity_usd']:,.0f} | "
        f"Vol(24h): ${sig['volume_h24']:,.0f} | Txns(24h): {sig['txns_h24']} "
        f"({sig['buys_h24']} buys/{sig['sells_h24']} sells) | "
        f"Age: {sig['pair_age_hours']:.1f}h | Mint: `{sig['mint']}`"
        + (f" | Dex: {sig['pair_url']}" if sig["pair_url"] else "")
    )


def run_graduation_scanner():
    candidates = load_graduation_candidates()
    state = prune_seen_state(load_seen_state())
    now = _now_ts()
    hits = 0

    if DEBUG:
        discord(
            f"🧪 Crypto graduation run: candidates={len(candidates)} "
            f"min_liq=${GRAD_MIN_LIQUIDITY_USD:,.0f} min_vol=${GRAD_MIN_VOLUME_H24_USD:,.0f} "
            f"min_txns={GRAD_MIN_TXNS_H24} min_score={GRAD_MIN_SCORE} "
            f"helius={'on' if HELIUS_RPC_URL else 'off'}",
            confirmed=False,
        )

    for candidate in candidates:
        mint = candidate["mint"]
        if mint in state:
            continue
        try:
            pair = fetch_dex_pair_for_mint(mint)
            if not pair:
                continue
            helius_asset = fetch_helius_asset(mint)
            signal = build_graduation_signal(candidate, pair, helius_asset)
            if not signal:
                continue
            discord(format_graduation_msg(signal), confirmed=True)
            state[mint] = now
            hits += 1
        except Exception as e:
            _debug(f"graduation candidate error ({mint}): {e}")
        time.sleep(0.25)

    save_seen_state(state)

    if hits == 0 and (POST_NO_SIGNAL or DEBUG):
        discord("ℹ️ Crypto graduation scanner: no high-conviction signals this run.", confirmed=False)

    if DEBUG:
        discord(f"🧪 Crypto graduation scanner finished. hits={hits}", confirmed=False)


def run_momentum_scanner():
    syms = load_symbols()

    if DEBUG:
        discord(
            f"🧪 Crypto scanner run: mode={os.getenv('CRYPTO_MODE', 'static')} exchange={_resolve_binance_base()} "
            f"fast={TF_FAST} slow={TF_SLOW} lookback_fast={LOOKBACK_BARS} lookback_slow={SLOW_LOOKBACK_BARS} "
            f"min_qv=${MIN_QUOTE_VOL:,.0f} scores: ignition>={SCORE_IGNITION} rotation>={SCORE_ROTATION} casino>={SCORE_CASINO} "
            f"symbols={len(syms)}",
            confirmed=False,
        )

    hits = 0
    for sym in syms:
        try:
            fast_kl = fetch_klines(sym, limit=max(LOOKBACK_BARS + 6, 40), interval=TF_FAST)
            slow_kl = fetch_klines(sym, limit=max(SLOW_LOOKBACK_BARS + 6, 40), interval=TF_SLOW)
            sig = score_signal(sym, fast_kl, slow_kl)
            if sig:
                discord(format_momentum_msg(sig), confirmed=False)
                hits += 1
        except Exception as e:
            _debug(f"momentum scanner error ({sym}): {e}")
        time.sleep(0.25)

    if hits == 0 and (POST_NO_SIGNAL or DEBUG):
        discord(f"ℹ️ Crypto scanner: no scored signals this run (min_score={SCORE_CASINO}).", confirmed=False)

    if DEBUG:
        discord(f"🧪 Crypto scanner finished. hits={hits}", confirmed=False)


def main():
    if SCAN_MODE == "momentum":
        run_momentum_scanner()
        return

    run_graduation_scanner()


if __name__ == "__main__":
    main()
