import os
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

load_dotenv()
key = os.getenv("ALPACA_KEY")
secret = os.getenv("ALPACA_SECRET")

print("Using keys:", key[:6]+"...")  # just to confirm it loaded

client = StockHistoricalDataClient(key, secret)

end = datetime.now(ZoneInfo("America/New_York"))
start = end - timedelta(days=2)

req = StockBarsRequest(
    symbol_or_symbols="AAPL",
    timeframe=TimeFrame.Minute,
    start=start,
    end=end,
    adjustment="raw",
    feed="iex"
)

bars = client.get_stock_bars(req)
print("AAPL bars:", list(bars.data["AAPL"])[-5:])

