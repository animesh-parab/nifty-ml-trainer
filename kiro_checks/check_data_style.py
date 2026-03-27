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

# DB sample (websocket data)
with engine.connect() as conn:
    r = conn.execute(text("SELECT time, open, high, low, close FROM nifty_1min ORDER BY time DESC LIMIT 3"))
    db_rows = r.fetchall()

print("=== DB (websocket) ===")
for row in db_rows:
    print(f"  time={row[0]} | type={type(row[0])} | O={row[1]} H={row[2]} L={row[3]} C={row[4]}")

# CSV sample (backfill)
df = pd.read_csv("data/raw/backfill_candles.csv")
print("\n=== CSV (backfill) ===")
print(f"  timestamp dtype : {df['timestamp'].dtype}")
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
print(f"  timestamp parsed: {df['timestamp'].dtype}")
for _, row in df.tail(3).iterrows():
    print(f"  time={row['timestamp']} | O={row['open']} H={row['high']} L={row['low']} C={row['close']}")

print("\n=== Timezone check ===")
print(f"  DB time tzinfo  : {db_rows[0][0].tzinfo}")
print(f"  CSV time tzinfo : {df['timestamp'].iloc[-1].tzinfo}")
