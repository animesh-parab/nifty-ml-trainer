from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
load_dotenv()

url = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)
engine = create_engine(url)
with engine.connect() as conn:
    # Today's price range
    r = conn.execute(text("SELECT MIN(close), MAX(close), COUNT(*) FROM nifty_1min WHERE time > '2026-03-13'"))
    row = r.fetchone()
    print(f"Today — Low: {row[0]}, High: {row[1]}, Candles: {row[2]}")

    # Price after first signal (06:57 UTC)
    r = conn.execute(text("SELECT time, close FROM nifty_1min WHERE time >= '2026-03-13 06:57:00' ORDER BY time ASC LIMIT 10"))
    print("\nPrice after first signal (DOWN @ 23288):")
    for row in r:
        print(f"  {row[0]} → {row[1]}")