import os, time, subprocess, requests
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

WEBHOOK = os.getenv("STOCKS_WEBHOOK")
CT = ZoneInfo("America/Chicago")

def discord(msg: str):
    if not WEBHOOK:
        return
    try:
        requests.post(WEBHOOK, json={"content": msg}, timeout=10)
    except Exception:
        pass

def in_rth(ts: datetime) -> bool:
    # Regular Trading Hours: 8:30 AM – 3:00 PM CT
    if ts.hour < 8:
        return False
    if ts.hour > 15:
        return False
    if ts.hour == 8 and ts.minute < 30:
        return False
    return True

started_today = None  # date we already announced

while True:
    now = datetime.now(CT)

    if in_rth(now):
        # Send "live" ping once per day
        if started_today != now.date():
            discord("✅ **Rudis RTH scanner is LIVE** (8:30–3:00 CT).")
            started_today = now.date()

        # Run scanner (every ~60s)
        subprocess.run(["python", "rth_momentum_scanner.py"])
        time.sleep(60)
    else:
        time.sleep(60)