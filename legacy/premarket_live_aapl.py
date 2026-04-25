import os, requests
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()
WEBHOOK = os.getenv("STOCKS_WEBHOOK")
TICKER = "AAPL"

def post(msg:str):
    r = requests.post(WEBHOOK, json={"content": msg}, timeout=15)
    r.raise_for_status()

# 1) Previous close (yesterday)
d1 = yf.download(TICKER, period="5d", interval="1d", progress=False, auto_adjust=False)
if d1.empty or len(d1) < 2:
    raise SystemExit("No daily data returned.")
y_close = float(d1["Close"].iloc[-2])

# 2) Latest 1-minute price including pre/post
m1 = yf.download(TICKER, period="1d", interval="1m", prepost=True, progress=False, auto_adjust=False)
if m1.empty:
    raise SystemExit("No intraday data returned. Try again shortly.")
last_px   = float(m1["Close"].iloc[-1])
premkt_vol = float(m1["Volume"].sum())

# 3) Rough pre-market volume ratio vs 20D baseline (same minutes)
vol20_df = yf.download(TICKER, period="1mo", interval="1d", progress=False, auto_adjust=False)
vol20 = float(vol20_df["Volume"].tail(20).mean()) if not vol20_df.empty else 0.0
mins = max(1, len(m1))                        # minutes we pulled
baseline_same_window = (vol20 / 390.0) * mins if vol20 > 0 else 0.0
vol_x = (premkt_vol / baseline_same_window) if baseline_same_window > 0 else 0.0

# 4) Gap %, side
gap_pct = (last_px / y_close - 1.0) * 100.0
side = "bull" if gap_pct >= 0 else "bear"
arrow = "🚀" if side=="bull" else "🔻"

# 5) Simple fib-style levels based on the gap
gap_high, gap_low = (max(last_px, y_close), min(last_px, y_close))
def fib(z): return gap_low + z*(gap_high-gap_low)

if side=="bull":
    entries = [fib(0.764), fib(0.618), fib(0.500)]
    stops   = [fib(0.382), y_close*0.995]
    targets = [gap_high*1.012, gap_high*1.025, gap_high*1.050]
else:
    entries = [fib(0.236), fib(0.382), fib(0.500)]
    stops   = [fib(0.618), y_close*1.005]
    targets = [gap_low*0.988, gap_low*0.975, gap_low*0.950]

fmt = lambda xs: [f"{x:.2f}" for x in xs]

# 6) Trend snapshot (force scalars, handle NaNs)
daily_close = yf.download(TICKER, period="1y", interval="1d", progress=False, auto_adjust=False)["Close"]
sma50_s  = daily_close.rolling(50).mean()
sma200_s = daily_close.rolling(200).mean()

def last_valid(series):
    # return last non-NaN value as float
    s = series.dropna()
    return float(s.iloc[-1]) if not s.empty else float(daily_close.iloc[-1])

sma50  = last_valid(sma50_s)
sma200 = last_valid(sma200_s)

trend_bits = []
trend_bits.append(">SMA50" if float(last_px) > sma50 else "<SMA50")
trend_bits.append(">SMA200" if float(last_px) > sma200 else "<SMA200")
trend_note = " & ".join(trend_bits)

# 7) Quick star rating
score = 0
if 3 <= abs(gap_pct) < 10: score += 2
elif 10 <= abs(gap_pct) < 20: score += 1
if vol_x >= 1.0: score += 2
elif vol_x >= 0.5: score += 1
score = max(1, min(5, score))
stars = "⭐"*score + "☆"*(5-score)

# 8) Build dual card
layman = (
f"{arrow} ${TICKER} is {'up' if side=='bull' else 'down'} {abs(gap_pct):.1f}% in pre-market on volume (~{vol_x:.1f}× baseline).\n"
f"📈 Current price: ${last_px:.2f} (yesterday’s close was ${y_close:.2f}).\n"
f"⭐ Gap Rating: {stars} (auto-rated)\n\n"
"👉 What to watch:\n"
f"- {'If it dips, watch' if side=='bull' else 'If it bounces, watch'} ${fmt(entries)[0]} first.\n"
f"- {'Above' if side=='bull' else 'Below'} ${fmt(entries)[0]} often keeps {'bulls' if side=='bull' else 'bears'} in control.\n"
f"- If momentum continues, goals could be {', '.join('$'+t for t in fmt(targets))}.\n\n"
"⚠️ Educational only — not financial advice."
)

pro = (
f"📊 PRO BREAKDOWN | ${TICKER}\n\n"
f"Price: {last_px:.2f}\n"
f"Trend: {trend_note} | Premkt Vol: ~{vol_x:.1f}× 20D-per-minute\n\n"
f"Entries: {', '.join('$'+x for x in fmt(entries))}\n"
f"Stops: {', '.join('$'+x for x in fmt(stops))}\n"
f"Targets: {', '.join('$'+x for x in fmt(targets))}\n\n"
"⚠️ Educational only — not financial advice.\n"
"---\n"
"⚡ Powered by Rudis"
)

post(layman + "\n\n" + pro)
print("✅ Posted real pre-market AAPL card.")
