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

def current_session(ts: datetime) -> str | None:
    mins = ts.hour * 60 + ts.minute
    premarket_start = 3 * 60
    rth_start = 8 * 60 + 30
    ah_start = 15 * 60
    ah_end = 18 * 60

    if premarket_start <= mins < rth_start:
        return "premarket"
    if rth_start <= mins < ah_start:
        return "rth"
    if ah_start <= mins < ah_end:
        return "afterhours"
    return None

started_today = None  # date we already announced

while True:
    now = datetime.now(CT)
    session = current_session(now)

    if session:
        # Send "live" ping once per day
        if started_today != now.date():
            discord("✅ **Rudis stock scanner is LIVE** (3:00 AM–6:00 PM CT).")
            started_today = now.date()

        # Run scanner (every ~60s)
        env = os.environ.copy()
        if session == "rth":
            env["RTH_ALLOWED_SIGNALS"] = "WATCH,EARLY,CONFIRMED,FADING"
        else:
            env["RTH_ALLOWED_SIGNALS"] = "EARLY"
        subprocess.run(["python", "rth_momentum_scanner.py"], env=env)
        time.sleep(60)
    else:
        time.sleep(60)
