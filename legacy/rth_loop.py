import os, sys, time, subprocess, requests
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


def scan_interval_seconds(ts: datetime, session: str | None) -> int:
    if session == "premarket":
        return 15
    if session == "rth":
        mins = ts.hour * 60 + ts.minute
        rth_start = 8 * 60 + 30
        first_hour_end = 9 * 60 + 30
        last_hour_start = 14 * 60
        if rth_start <= mins < first_hour_end:
            return 15
        if mins >= last_hour_start:
            return 15
        return 30
    if session == "afterhours":
        return 30
    return 60

started_today = None  # date we already announced

while True:
    now = datetime.now(CT)
    session = current_session(now)

    if session:
        # Send "live" ping once per day
        if started_today != now.date():
            discord("✅ **Rudis stock scanner is LIVE** (3:00 AM–6:00 PM CT).")
            started_today = now.date()

        # Run scanner on a tighter cadence during active windows.
        env = os.environ.copy()
        if session == "rth":
            env["RTH_ALLOWED_SIGNALS"] = "WATCH,EARLY,CASINO,CONFIRMED,EXTENDED,FADING"
        else:
            env["RTH_ALLOWED_SIGNALS"] = "EARLY,CASINO,CONFIRMED,EXTENDED"
        subprocess.run([sys.executable, "rth_momentum_scanner.py"], env=env)
        time.sleep(scan_interval_seconds(now, session))
    else:
        time.sleep(60)
