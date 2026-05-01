# premarket_gappers_dynamic.py
# Yahoo (PM/AH) + Alpaca (RTH), fast-mode, short lookback, cooldown, badges, float, Discord cards.

import os, json, time, requests
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import List, Tuple, Optional, Dict

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

# ───────────────── ENV / GLOBALS ─────────────────
load_dotenv()

WEBHOOK = os.getenv("STOCKS_WEBHOOK")
ALPACA_KEY = os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")
assert WEBHOOK, "Missing STOCKS_WEBHOOK in .env"

ET = ZoneInfo("America/New_York")
CT = ZoneInfo("America/Chicago")

ROOT = Path(__file__).resolve().parent
CACHE_PATH      = ROOT / ".posted_cache_dynamic.json"
FAST_DEDUPE     = ROOT / ".fast_recent_posts.json"     # short cooldown memory
LOG_FILE        = ROOT / "logs" / "scanner.log"
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# ── Fast-mode knobs (edit via .env if you like) ──
FAST_MODE           = os.getenv("FAST_MODE", "1") == "1"    # default ON for RTH
FAST_PM_AH          = os.getenv("FAST_PM_AH", "0") == "1"   # default OFF for PM/AH
FAST_TICK           = int(os.getenv("FAST_TICK", "7"))      # loop seconds in fast mode
FAST_LOOKBACK_MIN   = int(os.getenv("FAST_LOOKBACK_MIN", "10"))
FAST_BATCH          = int(os.getenv("FAST_BATCH", "60"))

# Regular cadence (non-fast)
SLOW_TICK           = 60
SLOW_LOOKBACK_MIN   = 60
SLOW_BATCH          = 200

MAX_SYMBOLS = 4000
PRICE_MIN = 0.40
PRICE_MAX = 40.00
TOP_N = 12
API_POST_SLEEP_S = 0.25
BULLISH_ONLY = True
COOLDOWN_MIN = 3        # per-symbol dedupe window to reduce chatter

# ───────────────── Alpaca (optional for RTH) ─────────────────
try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame
    from alpaca.data.enums import Adjustment, DataFeed
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import GetAssetsRequest
    from alpaca.trading.enums import AssetClass, AssetStatus
except Exception:
    StockHistoricalDataClient = None
    TradingClient = None

# ───────────────── Session thresholds ─────────────────
THRESH = {
    "PM":  { "EW_GAP":3.0,"EW_VOL":10_000,"EW_$VOL": 75_000,"EW_MOM":0.6,
             "FP_GAP":6.0,"FP_VOL":35_000,"FP_$VOL":1_500_000,"FP_MOM":2.0,
             "MIN_BARS":5,"AVG_VPM":1000 },
    "RTH": { "EW_GAP":2.0,"EW_VOL":40_000,"EW_$VOL":2_000_000,"EW_MOM":1.0,
             "FP_GAP":6.0,"FP_VOL":60_000,"FP_$VOL":1_500_000,"FP_MOM":2.5,
             "MIN_BARS":6,"AVG_VPM":1800 },
    "AH":  { "EW_GAP":3.0,"EW_VOL":10_000,"EW_$VOL": 75_000,"EW_MOM":0.6,
             "FP_GAP":6.0,"FP_VOL":35_000,"FP_$VOL":1_500_000,"FP_MOM":2.0,
             "MIN_BARS":5,"AVG_VPM":1000 },
}

# ───────────────── Helpers ─────────────────
def log(s: str):
    ts = datetime.now(ET).strftime("%H:%M:%S ET")
    line = f"[{ts}] {s}"
    print(line, flush=True)
    try:
        with LOG_FILE.open("a") as f:
            f.write(line + "\n")
    except Exception:
        pass

def post(msg: str) -> None:
    try:
        requests.post(WEBHOOK, json={"content": msg}, timeout=6)
    except Exception as e:
        log(f"Webhook post error: {e}")

def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return default
    return default

def save_json(path: Path, obj):
    path.write_text(json.dumps(obj))

def _cache_template() -> Dict:
    return {"watch": {}, "full": {}, "_day": ""}

def _fast_memo_template() -> Dict:
    return {"last": {}, "_day": ""}

def load_cache() -> Dict:
    return load_json(CACHE_PATH, _cache_template())

def save_cache(cache: Dict) -> None:
    save_json(CACHE_PATH, cache)

def load_fast_memo() -> Dict:
    return load_json(FAST_DEDUPE, _fast_memo_template())

def save_fast_memo(m: Dict) -> None:
    save_json(FAST_DEDUPE, m)

def reset_day_dict(d: Dict, today: str, template: Dict) -> Dict:
    base = {
        k: (v.copy() if isinstance(v, dict) else v)
        for k, v in template.items()
    }
    if d.get("_day") != today:
        base["_day"] = today
        return base
    for k, v in base.items():
        if k == "_day":
            continue
        d.setdefault(k, v.copy() if isinstance(v, dict) else v)
    return d

def now_et() -> datetime:
    return datetime.now(ET)

def clean_sym(s: str) -> str:
    if not s: return s
    s = s.strip().upper()
    if s.startswith("$"): s = s[1:]
    if len(s) > 5: s = s[:5]
    return s

def current_window_et(ts: datetime) -> Tuple[Optional[str], Optional[datetime], Optional[datetime]]:
    pm = ts.replace(hour=4, minute=0, second=0, microsecond=0)
    rth_open = ts.replace(hour=9, minute=30, second=0, microsecond=0)
    rth_close = ts.replace(hour=15, minute=59, second=59, microsecond=0)
    ah = ts.replace(hour=16, minute=0, second=0, microsecond=0)
    ah_end = ts.replace(hour=20, minute=0, second=0, microsecond=0)
    if pm <= ts < rth_open:  return "PM", pm, min(ts, rth_open)
    if rth_open <= ts <= rth_close: return "RTH", rth_open, ts
    if ah <= ts <= ah_end:  return "AH", ah, ts
    return None, None, None

# ───────────────── Yahoo 1m ─────────────────
def yahoo_1m(symbols: List[str], start_et: datetime, end_et: datetime) -> pd.DataFrame:
    out = []
    start_utc = pd.Timestamp(start_et).tz_convert("UTC")
    end_utc   = pd.Timestamp(end_et).tz_convert("UTC")
    for s in symbols:
        ysym = clean_sym(s)
        try:
            t = yf.Ticker(ysym)
            df = t.history(interval="1m", start=start_utc, end=end_utc,
                           prepost=True, actions=False, auto_adjust=False)
            if df is None or df.empty: 
                continue
            df = df.rename(columns={"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume"})
            df = df.reset_index().rename(columns={"Datetime":"timestamp"})
            df["symbol"] = s
            out.append(df[["symbol","timestamp","open","high","low","close","volume"]])
        except Exception as e:
            log(f"yahoo err {s}: {e}")
    if not out:
        return pd.DataFrame(columns=["symbol","timestamp","open","high","low","close","volume"])
    df = pd.concat(out, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert(ET)
    return df

# ───────────────── Alpaca 1m (RTH) ─────────────────
def alpaca_bars_1m(symbols: List[str], start_et: datetime, end_et: datetime) -> pd.DataFrame:
    if not (StockHistoricalDataClient and ALPACA_KEY and ALPACA_SECRET):
        return pd.DataFrame(columns=["symbol","timestamp","open","high","low","close","volume"])
    client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
    try:
        req = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame.Minute,
            start=start_et, end=end_et,
            adjustment=Adjustment.SPLIT,
            feed=DataFeed.IEX,
        )
        df = client.get_stock_bars(req).df
        if df is None or df.empty:
            return pd.DataFrame(columns=["symbol","timestamp","open","high","low","close","volume"])
        df = df.reset_index()
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_convert(ET)
        df = df.rename(columns={"symbol":"symbol","open":"open","high":"high","low":"low","close":"close","volume":"volume"})
        return df[["symbol","timestamp","open","high","low","close","volume"]]
    except Exception as e:
        log(f"alpaca err: {e}")
        return pd.DataFrame(columns=["symbol","timestamp","open","high","low","close","volume"])

# ───────────────── Universe ─────────────────
def get_universe() -> List[str]:
    try:
        if TradingClient and ALPACA_KEY and ALPACA_SECRET:
            t = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=True)
            assets = t.get_all_assets(GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE))
            syms = []
            for a in assets:
                sym = getattr(a, "symbol", "")
                tradable = getattr(a, "tradable", False)
                if tradable and sym.isalpha() and 1 <= len(sym) <= 5:
                    syms.append(sym)
            return syms[:MAX_SYMBOLS]
    except Exception as e:
        log(f"universe err: {e}")
    return ["AAPL","TSLA","NVDA","AMD","SPY","QQQ"]

# ───────────────── Float / ADV / Badges ─────────────────
_float_cache: Dict[str, Optional[float]] = {}
_adv_cache: Dict[str, Optional[float]] = {}

def get_float(sym: str) -> Optional[float]:
    if sym in _float_cache: return _float_cache[sym]
    f = None
    try:
        info = yf.Ticker(clean_sym(sym)).get_info()
        for k in ("floatShares","float_shares","shares_float"):
            if info and k in info and info[k]:
                f = float(info[k]); break
    except Exception:
        f = None
    _float_cache[sym] = f
    return f

def get_adv30(sym: str) -> Optional[float]:
    if sym in _adv_cache: return _adv_cache[sym]
    try:
        h = yf.Ticker(clean_sym(sym)).history(period="2mo", interval="1d", actions=False)
        if h is not None and not h.empty:
            _adv_cache[sym] = float(h["Volume"].tail(30).mean())
            return _adv_cache[sym]
    except Exception:
        pass
    _adv_cache[sym] = None
    return None

def price_bucket_min_dvol(px: float) -> Tuple[float, float]:
    if px < 5:   return (5_000_000, 2_000_000)
    if px < 10:  return (10_000_000, 5_000_000)
    return (20_000_000, 10_000_000)

def score_badge(sym: str, sdf: pd.DataFrame, last_px: float, dollarv_so_far: float) -> str:
    adv = get_adv30(sym) or 0.0
    day_vol = float(sdf["volume"].sum())
    rvol = (day_vol / adv) if adv > 0 else 0.0
    last15 = float(sdf.tail(15)["volume"].sum())
    prev15 = float(sdf.tail(30).head(15)["volume"].sum())
    accel = (last15 / prev15) if prev15 > 0 else (2.0 if last15 > 0 else 0.0)
    bomb_min, strong_min = price_bucket_min_dvol(last_px)
    if rvol >= 5 and accel >= 2.0 and dollarv_so_far >= bomb_min: return "💣 **KILLER**"
    if (rvol >= 3.0 or accel >= 1.5) and dollarv_so_far >= strong_min: return "🔥 **STRONG**"
    return ""

# ───────────────── Cards ─────────────────
def star_gap(gap_pct: float, dollarv: float) -> str:
    stars = 1
    if gap_pct >= 5: stars += 1
    if gap_pct >= 10: stars += 1
    if dollarv >= 500_000: stars += 1
    if dollarv >= 2_000_000: stars += 1
    return "⭐" * min(stars, 5)

def build_pro_block(sym: str, last_px: float, ref_px: float) -> str:
    base = ref_px if ref_px > 0 else last_px
    entries = [base * 1.01, base * 1.02, base * 1.03]
    stops   = [base * 0.99, base * 0.985]
    targets = [base * 1.05, base * 1.08, base * 1.12]
    f2 = lambda x: f"{x:.2f}"
    try:
        rr = (targets[0] - entries[0]) / max(entries[0] - stops[0], 1e-6)
        rr_txt = f"{rr:.1f}x"
    except Exception:
        rr_txt = "N/A"
    return (
        f"📊 **PRO BREAKDOWN | ${sym}**\n\n"
        f"**Entries**: {', '.join(f2(x) for x in entries)}\n"
        f"**Stops**: {', '.join(f2(x) for x in stops)}\n"
        f"**Targets**: {', '.join(f2(x) for x in targets)}\n"
        f"R/R (first): {rr_txt}\n\n"
        f"⚠️ Educational only — not financial advice.\n—\n⚡ Powered by Rudis"
    )

def post_watch(sym: str, gap_pct: float, vol: int, last_px: float, mom_pct: float,
               dollarv: float, mode: str, sdf: pd.DataFrame):
    flt = get_float(sym)
    badge = score_badge(sym, sdf, last_px, dollarv)
    float_txt = f" | Float: {int(flt):,}" if flt else ""
    badge_txt = f"\n{badge}" if badge else ""
    msg = (
        f"🚀 **EARLY WATCH** {sym} — gap {gap_pct:.1f}% _(mode: {mode})_\n"
        f"Last: ${last_px:.2f} | Vol: {vol:,} | $Vol: ${int(dollarv):,}{float_txt}\n"
        f"Momentum (3m): {mom_pct:.1f}%{badge_txt}\n"
        f"_Uptrend forming; will post **FULL PLAY** if confirmation hits._"
    )
    post(msg); time.sleep(API_POST_SLEEP_S)

def post_full(sym: str, gap_pct: float, vol: int, last_px: float, mom_pct: float,
              dollarv: float, mode: str, ref_px: float):
    stars = star_gap(gap_pct, dollarv)
    header = (
        f"📊 **FULL PLAY** {sym} 🚀 _(mode: {mode})_\n"
        f"Gap: {gap_pct:.1f}% | Last: ${last_px:.2f}\n"
        f"Vol: {vol:,} | $Vol: ${int(dollarv):,}\n"
        f"Momentum (3m): {mom_pct:.1f}% | Rating: {stars}\n"
        f"Notes: ranked by |gap| + volume."
    )
    pro = build_pro_block(sym, last_px, ref_px)
    post(header + "\n\n" + pro); time.sleep(API_POST_SLEEP_S)

# ───────────────── Scan ─────────────────
def in_fast_mode(mode: str) -> bool:
    if mode == "RTH": return FAST_MODE
    return FAST_PM_AH

def scan_once():
    ts = now_et()
    today = ts.astimezone(CT).strftime("%Y-%m-%d")
    mode, start_et, end_et = current_window_et(ts)
    if not mode:
        return [], [], None, today

    # rolling short lookback when fast
    if in_fast_mode(mode):
        lb     = FAST_LOOKBACK_MIN
        batch  = FAST_BATCH
    else:
        lb     = SLOW_LOOKBACK_MIN
        batch  = SLOW_BATCH

    start_et = max(start_et, ts - timedelta(minutes=lb))
    log(f"Mode={mode} lookback={lb}m batch={batch}")

    uni = get_universe()
    if not uni:
        return [], [], mode, today

    dfs = []
    if mode in ("PM","AH"):
        for bi in range(0, len(uni), batch):
            dfy = yahoo_1m(uni[bi:bi+batch], start_et, end_et)
            if not dfy.empty: dfs.append(dfy)
    else:
        got = False
        if ALPACA_KEY and ALPACA_SECRET and StockHistoricalDataClient:
            for bi in range(0, len(uni), batch):
                dfa = alpaca_bars_1m(uni[bi:bi+batch], start_et, end_et)
                if not dfa.empty:
                    got = True
                    dfs.append(dfa)
        if not got:
            for bi in range(0, len(uni), batch):
                dfy = yahoo_1m(uni[bi:bi+batch], start_et, end_et)
                if not dfy.empty: dfs.append(dfy)

    if not dfs:
        return [], [], mode, today

    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["symbol","timestamp"]).sort_values(["symbol","timestamp"])

    T = THRESH[mode]
    watch_hits, full_hits = [], []

    for sym in df["symbol"].unique():
        sdf = df[df["symbol"] == sym]
        if len(sdf) < T["MIN_BARS"]:
            continue

        open_px = float(sdf.iloc[0]["open"])
        last_px = float(sdf.iloc[-1]["close"])
        if not (PRICE_MIN <= last_px <= PRICE_MAX):
            continue

        vol = int(sdf["volume"].sum())
        dollarv = last_px * vol
        avg_vpm = vol / max(len(sdf), 1)
        if avg_vpm < T["AVG_VPM"]:
            continue

        gap_pct = (last_px - open_px) / max(open_px, 1e-9) * 100.0
        if BULLISH_ONLY and gap_pct < 0:
            continue

        tail3 = sdf.tail(3)
        mom_pct = 0.0
        if len(tail3) >= 2:
            mom_pct = (float(tail3["close"].iloc[-1]) / float(tail3["close"].iloc[0]) - 1.0) * 100.0

        early_pass = (
            (gap_pct >= T["EW_GAP"] and vol >= T["EW_VOL"] and dollarv >= T["EW_$VOL"]) or
            (mom_pct >= T["EW_MOM"] and vol >= max(15_000, T["EW_VOL"]) and dollarv >= max(150_000, T["EW_$VOL"]))
        )
        full_pass = (
            (gap_pct >= T["FP_GAP"] and vol >= T["FP_VOL"] and dollarv >= T["FP_$VOL"]) or
            (mom_pct >= T["FP_MOM"] and vol >= max(40_000, T["FP_VOL"]) and dollarv >= max(400_000, T["FP_$VOL"]))
        )

        ref_px = open_px
        tup = (sym, gap_pct, vol, last_px, mom_pct, dollarv, ref_px, sdf)
        if early_pass: watch_hits.append(tup)
        if full_pass:  full_hits.append(tup)

    keyer = lambda x: (abs(x[1]), x[2])
    watch_hits.sort(key=keyer, reverse=True)
    full_hits.sort(key=keyer, reverse=True)

    return watch_hits[:TOP_N], full_hits[:TOP_N], mode, today

# ───────────────── Controller ─────────────────
def main_once():
    cache = load_cache()
    fast = load_fast_memo()
    today = datetime.now(CT).strftime("%Y-%m-%d")
    cache = reset_day_dict(cache, today, _cache_template())
    fast  = reset_day_dict(fast, today, _fast_memo_template())

    watch, full, mode, _ = scan_once()
    if mode is None:
        return

    log(f"Candidates  Early:{len(watch)}  Full:{len(full)}")

    # cooldown clock
    now_s = time.time()

    for sym, gap, vol, last_px, mom, dollarv, ref_px, sdf in watch:
        # day-level dedupe
        if cache["watch"].get(sym) == cache["_day"]:
            continue
        # short cooldown dedupe
        last_t = fast["last"].get(sym, 0)
        if now_s - last_t < COOLDOWN_MIN * 60:
            continue
        try:
            post_watch(sym, gap, vol, last_px, mom, dollarv, mode, sdf)
            cache["watch"][sym] = cache["_day"]
            fast["last"][sym] = now_s
        except Exception as e:
            log(f"watch post err {sym}: {e}")

    for sym, gap, vol, last_px, mom, dollarv, ref_px, sdf in full:
        if cache["full"].get(sym) == cache["_day"]:
            continue
        last_t = fast["last"].get(sym, 0)
        if now_s - last_t < COOLDOWN_MIN * 60:
            continue
        try:
            post_full(sym, gap, vol, last_px, mom, dollarv, mode, ref_px)
            cache["full"][sym] = cache["_day"]
            fast["last"][sym] = now_s
        except Exception as e:
            log(f"full post err {sym}: {e}")

    save_cache(cache)
    save_fast_memo(fast)

def run_loop():
    # 05:00–20:30 CT to include PM, RTH, AH, with a small buffer
    start_ct = datetime.strptime("05:00", "%H:%M").time()
    stop_ct  = datetime.strptime("20:30", "%H:%M").time()

    while True:
        t = datetime.now(CT).time()
        if start_ct <= t <= stop_ct:
            try:
                main_once()
            except Exception as e:
                log(f"loop err: {e}")
                time.sleep(5)
            # tick speed based on fast-mode state
            ts = now_et()
            mode, *_ = current_window_et(ts)
            tick = FAST_TICK if (mode and in_fast_mode(mode)) else SLOW_TICK
            time.sleep(tick)
        elif t > stop_ct:
            log("Window closed.")
            break
        else:
            time.sleep(30)

if __name__ == "__main__":
    log("Scanner started")
    run_loop()
