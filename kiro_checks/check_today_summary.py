import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)
engine = create_engine(DB_URL)

with engine.connect() as conn:
    r = conn.execute(text("""
        SELECT COUNT(*), MIN(time), MAX(time)
        FROM nifty_1min
        WHERE DATE(time AT TIME ZONE 'Asia/Kolkata') = '2026-03-24'
    """))
    row = r.fetchone()
    print("Spot candles today :", row[0], "| first:", row[1], "| last:", row[2])

    r = conn.execute(text("""
        SELECT COUNT(*), MIN(time), MAX(time)
        FROM nifty_futures_1min
        WHERE DATE(time AT TIME ZONE 'Asia/Kolkata') = '2026-03-24'
    """))
    row = r.fetchone()
    print("Futures candles    :", row[0], "| first:", row[1], "| last:", row[2])

tl = pd.read_csv("data/processed/trade_log.csv")
tl["timestamp"] = pd.to_datetime(tl["timestamp"], utc=True)
tl["ist"] = tl["timestamp"] + pd.Timedelta(hours=5, minutes=30)
today = tl[tl["ist"].dt.date == pd.Timestamp("2026-03-24").date()]

print()
print("Signals today:", len(today))
print()
print(today[["ist","signal","confidence","entry_price","t1","rr_t1","atr","outcome"]].to_string())
