import pandas as pd

df = pd.read_csv("data/processed/trade_log.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
df["ist"] = df["timestamp"] + pd.Timedelta(hours=5, minutes=30)
today = df[df["ist"].dt.date == pd.Timestamp("today").date()]

print(f"Today's trades: {len(today)}")
print()
cols = ["ist", "signal", "confidence", "entry_price", "stoploss", "t1", "t1_pts", "atr", "rsi", "outcome"]
for _, row in today.iterrows():
    print(f"  {row['ist'].strftime('%H:%M')}  {row['signal']}  conf:{row['confidence']}%  entry:{row['entry_price']}  SL:{row['stoploss']}  T1:{row['t1']}  t1_pts:{row.get('t1_pts','?')}  ATR:{row['atr']}  outcome:{row['outcome']}")
