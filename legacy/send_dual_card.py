import os, requests
from dotenv import load_dotenv

load_dotenv()
WEBHOOK = os.getenv("STOCKS_WEBHOOK")

layman = (
"🚀 $AAPL is up 6.4% in pre-market on heavy trading (2.8× normal).\n"
"📈 Current price: $192.40 (yesterday’s close was $180.90).\n"
"⭐ Gap Rating: ⭐⭐⭐⭐☆ (Strong, early mover)\n\n"
"👉 What to watch:\n"
"- If it dips, watch $191.00–$189.50 as likely support zones.\n"
"- If it holds above $191.00, bulls may stay in control.\n"
"- If momentum continues, upside goals could be $195.00, $197.20, and $200.00.\n\n"
"⚠️ Educational only — not financial advice."
)

pro = (
"📊 PRO BREAKDOWN | $AAPL\n\n"
"Price: 192.40 (Premkt VWAP 190.80)\n"
"Trend: > SMA50 & SMA200 | Momentum +7.1% (5D)\n"
"Premkt Vol: 2.8× 20D Avg | Optionable ✅\n\n"
"Entries: 191.00, 190.20, 189.50\n"
"Stops: 188.80, 188.50\n"
"Targets: 195.00, 197.20, 200.00\n\n"
"⚠️ Educational only — not financial advice.\n"
"---\n"
"⚡ Powered by Rudis"
)

requests.post(WEBHOOK, json={"content": layman + "\n\n" + pro}, timeout=10).raise_for_status()
print("✅ Sent dual-card test to Discord")
