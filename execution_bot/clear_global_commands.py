import os
import discord
from discord import app_commands
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_BOT_TOKEN missing")

intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

@client.event
async def on_ready():
    print(f"Logged in as {client.user}")
    tree.clear_commands(guild=None)      # remove global commands in-memory
    await tree.sync(guild=None)          # push deletion to Discord
    print("✅ Cleared GLOBAL slash commands.")
    await client.close()

client.run(TOKEN)