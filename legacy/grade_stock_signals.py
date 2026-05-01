import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

CT = ZoneInfo("America/Chicago")
ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

ALPACA_KEY = os.getenv("ALPACA_KEY")
ALPACA_SECRET = os.getenv("ALPACA_SECRET")
ALPACA_FEED = os.getenv("ALPACA_FEED", "iex")
ALPACA_TIMEOUT = int(os.getenv("ALPACA_TIMEOUT", "20"))
DATA_BASE = os.getenv("ALPACA_DATA_BARS_URL", "https://data.alpaca.markets/v2/stocks/bars")
SIGNAL_LOG_PATH = Path(os.getenv("RTH_SIGNAL_LOG_PATH", "logs/stock_signal_calls.jsonl"))
GRADE_WEBHOOK = os.getenv("RTH_GRADE_WEBHOOK", os.getenv("STOCKS_WEBHOOK", ""))
POST_DISCORD = os.getenv("RTH_GRADE_POST_DISCORD", "0") == "1"


def alpaca_headers():
    return {
        "APCA-API-KEY-ID": ALPACA_KEY or "",
        "APCA-API-SECRET-KEY": ALPACA_SECRET or "",
    }


def load_signal_rows(target_date: str) -> list[dict]:
    rows = []
    if not SIGNAL_LOG_PATH.exists():
        return rows
    with SIGNAL_LOG_PATH.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            logged_at = str(row.get("logged_at_et") or "")
            if not logged_at:
                continue
            try:
                dt = datetime.fromisoformat(logged_at)
            except ValueError:
                continue
            if dt.astimezone(ET).date().isoformat() == target_date:
                row["_logged_at_et_dt"] = dt.astimezone(ET)
                rows.append(row)
    return rows


def resolve_target_date() -> str:
    raw = os.getenv("RTH_GRADE_DATE", "").strip()
    if raw:
        return raw
    now_et = datetime.now(ET)
    return now_et.date().isoformat()


def fetch_symbol_bars(symbol: str, start_et: datetime, end_et: datetime) -> list[dict]:
    if not ALPACA_KEY or not ALPACA_SECRET:
        raise RuntimeError("Missing ALPACA_KEY / ALPACA_SECRET in environment")

    params = {
        "symbols": symbol.upper(),
        "timeframe": "1Min",
        "start": start_et.astimezone(UTC).isoformat(),
        "end": end_et.astimezone(UTC).isoformat(),
        "limit": 10000,
        "adjustment": "raw",
        "feed": ALPACA_FEED,
    }
    r = requests.get(DATA_BASE, headers=alpaca_headers(), params=params, timeout=ALPACA_TIMEOUT)
    r.raise_for_status()
    payload = r.json() or {}
    return (payload.get("bars") or {}).get(symbol.upper(), []) or []


def _bar_price(bar: dict) -> float | None:
    try:
        return float(bar.get("c"))
    except (TypeError, ValueError):
        return None


def _bar_time_et(bar: dict) -> datetime | None:
    raw = bar.get("t")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(ET)
    except ValueError:
        return None


def _price_after_minutes(bars: list[dict], alert_dt: datetime, minutes: int) -> float | None:
    target = alert_dt + timedelta(minutes=minutes)
    for bar in bars:
        bt = _bar_time_et(bar)
        if bt and bt >= target:
            return _bar_price(bar)
    return None


def _session_close_et(alert_dt: datetime) -> datetime:
    return alert_dt.replace(hour=15, minute=59, second=0, microsecond=0)


def grade_signal(row: dict) -> dict:
    symbol = str(row["symbol"]).upper()
    alert_dt = row["_logged_at_et_dt"]
    entry = float(row["price"])
    end_et = _session_close_et(alert_dt)
    bars = fetch_symbol_bars(symbol, alert_dt - timedelta(minutes=1), end_et)
    closes = [_bar_price(bar) for bar in bars]
    closes = [price for price in closes if price is not None]
    if not closes:
        return {
            **row,
            "grade": "no_data",
            "max_return_pct": None,
            "min_return_pct": None,
            "return_5m_pct": None,
            "return_15m_pct": None,
            "return_30m_pct": None,
            "return_close_pct": None,
        }

    max_return_pct = (max(closes) / entry - 1.0) * 100.0
    min_return_pct = (min(closes) / entry - 1.0) * 100.0
    close_price = closes[-1]
    return_close_pct = (close_price / entry - 1.0) * 100.0

    p5 = _price_after_minutes(bars, alert_dt, 5)
    p15 = _price_after_minutes(bars, alert_dt, 15)
    p30 = _price_after_minutes(bars, alert_dt, 30)
    return_5m_pct = (p5 / entry - 1.0) * 100.0 if p5 else None
    return_15m_pct = (p15 / entry - 1.0) * 100.0 if p15 else None
    return_30m_pct = (p30 / entry - 1.0) * 100.0 if p30 else None

    tier = str(row.get("tier") or "").upper()
    if tier == "CONFIRMED":
        grade = "good" if max_return_pct >= 5 and return_close_pct >= 0 else "bad"
    elif tier == "EARLY":
        grade = "good" if max_return_pct >= 4 else "bad"
    elif tier == "CASINO":
        grade = "good" if max_return_pct >= 8 else "bad"
    elif tier == "EXTENDED":
        grade = "good" if max_return_pct >= 2 else "bad"
    else:
        grade = "good" if max_return_pct >= 3 else "bad"

    return {
        **row,
        "grade": grade,
        "max_return_pct": max_return_pct,
        "min_return_pct": min_return_pct,
        "return_5m_pct": return_5m_pct,
        "return_15m_pct": return_15m_pct,
        "return_30m_pct": return_30m_pct,
        "return_close_pct": return_close_pct,
    }


def fmt_pct(value) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.1f}%"


def print_report(results: list[dict], target_date: str):
    print(f"Stock signal EOD report for {target_date}")
    print(f"Signals graded: {len(results)}")
    print("")

    by_tier = defaultdict(list)
    for row in results:
        by_tier[str(row.get("tier") or "UNKNOWN").upper()].append(row)

    print("By tier")
    for tier in sorted(by_tier):
        rows = by_tier[tier]
        good = sum(1 for row in rows if row.get("grade") == "good")
        avg_max = sum((row.get("max_return_pct") or 0.0) for row in rows) / max(len(rows), 1)
        avg_close = sum((row.get("return_close_pct") or 0.0) for row in rows) / max(len(rows), 1)
        print(f" - {tier}: count={len(rows)} good={good} avg_max={avg_max:+.1f}% avg_close={avg_close:+.1f}%")

    print("")
    print("Best calls")
    best = sorted(results, key=lambda row: row.get("max_return_pct") or -9999, reverse=True)[:10]
    for row in best:
        print(
            f" - {row['symbol']} {row['tier']} entry={row['price']:.2f} "
            f"max={fmt_pct(row.get('max_return_pct'))} close={fmt_pct(row.get('return_close_pct'))} "
            f"grade={row['grade']}"
        )

    print("")
    print("Worst calls")
    worst = sorted(results, key=lambda row: row.get("min_return_pct") or 9999)[:10]
    for row in worst:
        print(
            f" - {row['symbol']} {row['tier']} entry={row['price']:.2f} "
            f"min={fmt_pct(row.get('min_return_pct'))} close={fmt_pct(row.get('return_close_pct'))} "
            f"grade={row['grade']}"
        )


def build_discord_report(results: list[dict], target_date: str) -> str:
    by_tier = defaultdict(list)
    for row in results:
        by_tier[str(row.get("tier") or "UNKNOWN").upper()].append(row)

    lines = [
        f"📘 **Stock EOD Report** `{target_date}`",
        f"Signals graded: **{len(results)}**",
        "",
        "**By Tier**",
    ]
    for tier in sorted(by_tier):
        rows = by_tier[tier]
        good = sum(1 for row in rows if row.get("grade") == "good")
        avg_max = sum((row.get("max_return_pct") or 0.0) for row in rows) / max(len(rows), 1)
        avg_close = sum((row.get("return_close_pct") or 0.0) for row in rows) / max(len(rows), 1)
        lines.append(
            f"- `{tier}` count={len(rows)} good={good} avg_max={avg_max:+.1f}% avg_close={avg_close:+.1f}%"
        )

    best = sorted(results, key=lambda row: row.get("max_return_pct") or -9999, reverse=True)[:5]
    worst = sorted(results, key=lambda row: row.get("min_return_pct") or 9999)[:5]

    lines.append("")
    lines.append("**Best Calls**")
    for row in best:
        lines.append(
            f"- `{row['symbol']}` {row['tier']} max={fmt_pct(row.get('max_return_pct'))} "
            f"close={fmt_pct(row.get('return_close_pct'))} grade={row['grade']}"
        )

    lines.append("")
    lines.append("**Worst Drawdowns**")
    for row in worst:
        lines.append(
            f"- `{row['symbol']}` {row['tier']} min={fmt_pct(row.get('min_return_pct'))} "
            f"close={fmt_pct(row.get('return_close_pct'))} grade={row['grade']}"
        )

    return "\n".join(lines)


def post_discord_report(report_text: str):
    if not POST_DISCORD or not GRADE_WEBHOOK:
        return
    requests.post(GRADE_WEBHOOK, json={"content": report_text[:1900]}, timeout=15)


def main():
    target_date = resolve_target_date()
    rows = load_signal_rows(target_date)
    if not rows:
        print(f"No stock signal logs found for {target_date} in {SIGNAL_LOG_PATH}")
        return
    results = [grade_signal(row) for row in rows]
    print_report(results, target_date)
    post_discord_report(build_discord_report(results, target_date))


if __name__ == "__main__":
    main()
