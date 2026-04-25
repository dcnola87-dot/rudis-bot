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

def discord(msg:str):
    if not WEBHOOK:
        return
    try:
        requests.post(WEBHOOK, json={"content": msg}, timeout=10)
    except Exception:
        pass

def in_window(ts):
    # Run only between 3:00 AM and 8:30 AM CT
    if ts.hour < 3:
        return False
    if ts.hour > 8 or (ts.hour == 8 and ts.minute >= 30):
        return False
    return True

started_today = None  # date we already announced

while True:
    now = datetime.now(CT)
    if in_window(now):
        # send "live" ping once per day
        if started_today != now.date():
            discord("✅ **Rudis pre-market scanner is LIVE** (3:00–8:30 CT).")
            started_today = now.date()
        subprocess.run(["python", "premarket_gappers_dynamic.py"])
        time.sleep(120)  # every ~2 minutes
    else:
        time.sleep(60)   # idle & check once per minute
