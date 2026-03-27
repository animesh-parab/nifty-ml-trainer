"""
Recalculates what SL/T1/T2/T3 should have been for last 2 days trades
using the now-complete DB (post-backfill).
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from smart_exits import get_exits

load_dotenv()
DB_URL = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)
engine = create_engine(DB_URL)

df = pd.read_csv("data/processed/trade_log.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
df["ist"] = df["timestamp"] + pd.Timedelta(hours=5, minutes=30)

# Last 2 trading days
last2 = df[df["ist"].dt.date >= (df["ist"].dt.date.max() - pd.Timedelta(days=1))]
print(f"Recalculating {len(last2)} trades from last 2 days\n")
print(f"{'Time':<8} {'Sig':<5} {'Entry':>9} | {'OLD SL':>9} {'OLD T1':>9} {'OLD ATR':>8} | {'NEW SL':>9} {'NEW T1':>9} {'NEW T2':>9} {'NEW ATR':>8}")
print("-" * 100)

for _, row in last2.iterrows():
    entry  = float(row["entry_price"])
    signal = row["signal"]
    atr    = float(row["atr"])
    time   = row["ist"].strftime("%H:%M")
    old_sl = float(row["stoploss"])
    old_t1 = float(row["t1"])

    try:
        exits = get_exits(signal, entry, atr, engine)
        new_sl = exits["stoploss"]
        new_t1 = exits["t1"]
        new_t2 = exits["t2"]
        new_atr = atr
        print(f"{time:<8} {signal:<5} {entry:>9.2f} | {old_sl:>9.2f} {old_t1:>9.2f} {atr:>8.2f} | {new_sl:>9.2f} {new_t1:>9.2f} {new_t2:>9.2f} {new_atr:>8.2f}  [{exits['sl_reason']}] T1:[{exits['t1_reason']}]")
    except Exception as e:
        print(f"{time:<8} {signal:<5} {entry:>9.2f} | ERROR: {e}")
