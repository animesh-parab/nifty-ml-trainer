from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import json
import os

load_dotenv()

url = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)
engine = create_engine(url)

with engine.connect() as conn:
    r = conn.execute(text("SELECT COUNT(time), MAX(time) FROM nifty_1min WHERE time >= '2026-03-20'"))
    row = r.fetchone()
    print(f"Spot candles today:    {row[0]} | Latest: {row[1]}")

    r = conn.execute(text("SELECT COUNT(time), MAX(time) FROM nifty_futures_1min"))
    row = r.fetchone()
    print(f"Futures candles total: {row[0]} | Latest: {row[1]}")

df = pd.read_parquet("data/processed/features.parquet")
print(f"Features parquet:      {len(df)} rows | Latest: {df.index[-1]}")
print(f"ADX in features:       {'adx_14' in df.columns}")

tl = pd.read_csv("data/processed/trade_log.csv")
print(f"Trade log:             {len(tl)} total signals")

if os.path.exists("data/processed/news_log.json"):
    with open("data/processed/news_log.json") as f:
        news = json.load(f)
    print(f"News log:              {len(news)} fetch cycles")
else:
    print("News log:              not found")