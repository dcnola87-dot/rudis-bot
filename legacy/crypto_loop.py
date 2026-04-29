import os, time, subprocess, requests
from datetime import datetime
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

WEBHOOK = os.getenv("CRYPTO_WEBHOOK") or os.getenv("CRYPTO_CONFIRMED_WEBHOOK")
CT = ZoneInfo("America/Chicago")


def discord(msg: str):
    if not WEBHOOK:
        return
    try:
        requests.post(WEBHOOK, json={"content": msg}, timeout=10)
    except Exception:
        pass


started_today = None  # date we already announced

while True:
    now = datetime.now(CT)

    if started_today != now.date():
        discord("✅ **Rudis crypto scanner is LIVE** (24/7, every 60s).")
        started_today = now.date()

    subprocess.run(["python", "crypto_momentum_scanner.py"])
    time.sleep(60)
