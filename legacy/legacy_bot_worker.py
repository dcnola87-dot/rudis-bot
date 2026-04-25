# bot_worker.py
# Rudis Discord worker:
# - 🔎 reaction -> create "TICKER — Deep Dive" thread + stub message
# - Optional: AUTO_THREAD=1 to auto-thread every alert
# - Optional: 05:00 ET heartbeat to LOG channel
# - Optional: 20:30 ET cleanup: delete UNPINNED posts in #rudis-stocks

import os, re, asyncio
from datetime import datetime, time
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import discord
from discord import Intents
from discord.ext import tasks

# ---------- ENV ----------
load_dotenv()
TOKEN        = os.getenv("DISCORD_BOT_TOKEN")
STOCKS_CH_ID = int(os.getenv("DISCORD_STOCKS_CHANNEL_ID", "0"))
log_ch_env = os.getenv("DISCORD_LOG_CHANNEL_ID", "").strip()
LOG_CH_ID = int(log_ch_env) if log_ch_env.isdigit() else None
AUTO_THREAD  = os.getenv("AUTO_THREAD", "0") == "1"
ET = ZoneInfo("America/New_York")

# ---------- DISCORD CLIENT ----------
intents = Intents.default()
intents.guilds = True
intents.message_content = True
intents.reactions = True
client = discord.Client(intents=intents)

# ---------- HELPERS ----------
# Match our alert text (supports "**EARLY WATCH** TICK", "**FULL PLAY** TICK", or "$TICK")
TICKER_PATTERNS = [
    re.compile(r"\*\*EARLY WATCH\*\*\s+([A-Z]{1,5})"),
    re.compile(r"\*\*FULL PLAY\*\*\s+([A-Z]{1,5})"),
    re.compile(r"\$([A-Z]{1,5})"),
]

def extract_ticker(txt: str) -> str | None:
    u = (txt or "").upper()
    for p in TICKER_PATTERNS:
        m = p.search(u)
        if m:
            return m.group(1)
    return None

async def log_term(msg: str):
    # Always print to terminal for live debugging
    print(f"[{datetime.now(ET):%H:%M:%S ET}] {msg}", flush=True)

async def log_channel(msg: str):
    """Send a message to the configured LOG channel.

    Note: `client.get_channel()` can return None if the channel isn't cached yet (common with private channels).
    We fall back to `fetch_channel()` to make this reliable.
    """
    if not LOG_CH_ID:
        return

    try:
        ch = client.get_channel(LOG_CH_ID)
        if ch is None:
            ch = await client.fetch_channel(LOG_CH_ID)

        if isinstance(ch, discord.TextChannel):
            await ch.send(msg)
        else:
            await log_term(f"LOG_CH_ID is not a text channel: {LOG_CH_ID} ({type(ch)})")
    except Exception as e:
        await log_term(f"LOG send failed: {e}")

async def make_deep_dive_thread(msg: discord.Message, sym: str, reason: str):
    if msg.thread:  # already has a thread
        return msg.thread

    thread = await msg.create_thread(
        name=f"{sym} — Deep Dive",
        auto_archive_duration=60,
        reason=reason,
    )
    await asyncio.sleep(0.2)
    await thread.send(
        f"🔎 **Deep Dive | ${sym}**\n"
        f"Coming online… collecting float / rel-vol / $vol / catalyst / liquidity / halt risk.\n\n"
        f"⚠️ Educational only — not financial advice."
    )
    return thread

# ---------- EVENTS ----------
@client.event
async def on_ready():
    await log_term(f"bot_worker online (AUTO_THREAD={int(AUTO_THREAD)})")
    await log_channel(f"✅ bot_worker online (AUTO_THREAD={int(AUTO_THREAD)})")
    heartbeat.start()
    nightly_cleanup.start()

@client.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # Scope to stocks channel
    if payload.channel_id != STOCKS_CH_ID:
        return
    # Ignore our own reactions
    if client.user and payload.user_id == client.user.id:
        return
    # Require the 🔎 emoji
    if str(payload.emoji) != "🔎":
        return

    channel = client.get_channel(payload.channel_id) or await client.fetch_channel(payload.channel_id)
    if not isinstance(channel, discord.TextChannel):
        return

    try:
        msg = await channel.fetch_message(payload.message_id)
    except Exception:
        return

    sym = extract_ticker(msg.content or "")
    if not sym:
        await log_term("🔎 reaction seen, but could not parse ticker from message.")
        return

    try:
        await make_deep_dive_thread(msg, sym, reason="🔎 deep dive")
        await log_term(f"🧵 Created Deep Dive for ${sym}")
        await log_channel(f"🧵 Created Deep Dive for ${sym}")
    except discord.Forbidden:
        await log_term("🚫 Missing permissions to create thread in #rudis-stocks")
        await log_channel("🚫 Missing permissions to create thread in #rudis-stocks")
    except Exception as e:
        await log_term(f"❗ Deep Dive error for ${sym}: {e}")

@client.event
async def on_message(message: discord.Message):
    # Optional: auto-thread every alert posted in #rudis-stocks
    if not AUTO_THREAD:
        return
    if message.author == client.user:
        return
    if message.channel.id != STOCKS_CH_ID:
        return

    sym = extract_ticker(message.content or "")
    if not sym:
        return

    try:
        await make_deep_dive_thread(message, sym, reason="auto deep dive")
        await log_term(f"🧵 Auto Deep Dive created for ${sym}")
        await log_channel(f"🧵 Auto Deep Dive created for ${sym}")
    except Exception as e:
        await log_term(f"❗ AUTO_THREAD error: {e}")

# ---------- SCHEDULED TASKS (optional) ----------
@tasks.loop(time=[time(hour=5, minute=0, tzinfo=ET)])
async def heartbeat():
    await log_channel(f"🫡 Morning heartbeat {datetime.now(ET):%H:%M:%S ET}")

@tasks.loop(time=[time(hour=20, minute=30, tzinfo=ET)])
async def nightly_cleanup():
    # Delete UNPINNED alerts in #rudis-stocks
    ch = client.get_channel(STOCKS_CH_ID) or await client.fetch_channel(STOCKS_CH_ID)
    if not isinstance(ch, discord.TextChannel):
        return
    sod = datetime.now(ET).replace(hour=0, minute=0, second=0, microsecond=0)
    removed = 0
    try:
        async for msg in ch.history(after=sod, limit=500):
            if msg.pinned:
                continue
            try:
                await msg.delete()
                removed += 1
            except Exception:
                pass
        await log_channel(f"🧹 Cleanup complete — removed {removed} unpinned messages at {datetime.now(ET):%H:%M ET}")
        await log_term(f"🧹 Cleanup removed {removed} messages")
    except Exception as e:
        await log_term(f"Cleanup error: {e}")

# ---------- MAIN ----------
if __name__ == "__main__":
    if not TOKEN or not STOCKS_CH_ID:
        raise SystemExit("Missing DISCORD_BOT_TOKEN or DISCORD_STOCKS_CHANNEL_ID in .env")
    client.run(TOKEN)
