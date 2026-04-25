import os
from dotenv import load_dotenv
load_dotenv()
for k in ["DISCORD_BOT_TOKEN","DISCORD_STOCKS_CHANNEL_ID","DISCORD_LOG_CHANNEL_ID","AUTO_THREAD"]:
    print(f"{k} =", repr(os.getenv(k)))
