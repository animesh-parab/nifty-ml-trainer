import pandas as pd
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
    r = conn.execute(text("SELECT COUNT(*), MIN(time), MAX(time) FROM nifty_futures_1min WHERE time >= '2026-03-23'"))
    row = r.fetchone()
    print(f"Futures candles today: {row[0]} | First: {row[1]} | Last: {row[2]}")

    r2 = conn.execute(text("SELECT time, open, high, low, close, volume FROM nifty_futures_1min WHERE time >= '2026-03-23' ORDER BY time DESC LIMIT 5"))
    print("\nLast 5 futures candles:")
    for row in r2.fetchall():
        ist = row[0] + pd.Timedelta(hours=5, minutes=30)
        print(f"  {ist.strftime('%H:%M')} IST  O:{row[1]}  H:{row[2]}  L:{row[3]}  C:{row[4]}  V:{row[5]}")
