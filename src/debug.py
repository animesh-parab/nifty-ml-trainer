import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import joblib

load_dotenv()

url = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)
engine = create_engine(url)

close_df = pd.read_sql(
    "SELECT time, close FROM nifty_1min ORDER BY time ASC",
    engine
)
close_df["time"] = pd.to_datetime(close_df["time"], utc=True)
close_df.set_index("time", inplace=True)

labeled = pd.read_parquet("data/processed/labeled.parquet")
labeled.index = pd.to_datetime(labeled.index, utc=True)
cutoff = labeled.index.max() - pd.DateOffset(years=1)
labeled = labeled.loc[cutoff:]

model = joblib.load("models/lgbm_w5_acc0.536_20260312_1524.pkl")
feature_cols = [c for c in labeled.columns if not c.startswith("label_")]
X = labeled[feature_cols]
preds = model.predict(X)

# Check first 10 non-sideways trades manually
count = 0
indices = labeled.index.tolist()
for i in range(len(indices)):
    if preds[i] == 0:
        continue
    ts = indices[i]
    if ts not in close_df.index:
        continue
    
    ts_pos    = close_df.index.get_loc(ts)
    exit_pos  = ts_pos + 5
    if exit_pos >= len(close_df):
        continue

    entry = float(close_df["close"].iloc[ts_pos])
    exit_ = float(close_df["close"].iloc[exit_pos])
    pnl   = exit_ - entry if preds[i] == 1 else entry - exit_

    print(f"ts={ts} signal={'UP' if preds[i]==1 else 'DOWN'} "
          f"entry={entry} exit={exit_} pnl={pnl:.2f}")
    count += 1
    if count >= 10:
        break