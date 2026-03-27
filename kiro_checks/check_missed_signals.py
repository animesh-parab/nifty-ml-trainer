"""
kiro_checks/check_missed_signals.py

Investigates ALL 17 missed YES signals from check_adx_and_features.py
Focus: why were they not logged despite passing all filters?

Checks per missed signal:
1. Is the timestamp in features.parquet?
2. What does already_logged() return for it?
3. Is the DB candle there (entry price available)?
4. What is is_market_open() at that time?
5. Timeline — was trade_logger likely still alive?

Run from nifty-ml-trainer/ directory.
"""

import pandas as pd
import joblib
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_URL = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)
engine = create_engine(DB_URL)

# All missed YES signals from check_adx_and_features output (UTC)
MISSED = [
    "2026-03-23 04:39:00+00:00",  # 10:09 IST  UP  76.8%  ADX 20.2
    "2026-03-23 06:57:00+00:00",  # 12:27 IST  DOWN 79.5% ADX 21.4
    "2026-03-23 06:58:00+00:00",  # 12:28 IST  DOWN 83.1% ADX 20.6
    "2026-03-23 07:37:00+00:00",  # 13:07 IST  DOWN 63.1% ADX 22.8
    "2026-03-23 08:24:00+00:00",  # 13:54 IST  UP   71.9% ADX 47.5
    "2026-03-23 08:25:00+00:00",  # 13:55 IST  UP   62.7% ADX 48.5
    "2026-03-23 08:26:00+00:00",  # 13:56 IST  UP   60.2% ADX 49.4
    "2026-03-23 08:27:00+00:00",  # 13:57 IST  UP   60.1% ADX 50.5
    "2026-03-23 08:28:00+00:00",  # 13:58 IST  UP   72.5% ADX 51.6
    "2026-03-23 08:29:00+00:00",  # 13:59 IST  UP   62.5% ADX 51.8
    "2026-03-23 08:41:00+00:00",  # 14:11 IST  DOWN 61.2% ADX 43.8
    "2026-03-23 08:48:00+00:00",  # 14:18 IST  DOWN 77.0% ADX 38.6
    "2026-03-23 08:49:00+00:00",  # 14:19 IST  DOWN 70.2% ADX 38.2
    "2026-03-23 08:50:00+00:00",  # 14:20 IST  DOWN 70.9% ADX 37.4
    "2026-03-23 09:20:00+00:00",  # 14:50 IST  DOWN 71.3% ADX 35.0
    "2026-03-23 09:21:00+00:00",  # 14:51 IST  DOWN 92.2% ADX 33.0
    "2026-03-23 09:22:00+00:00",  # 14:52 IST  DOWN 93.9% ADX 31.2
]

df = pd.read_parquet("data/processed/features.parquet")
df.index = pd.to_datetime(df.index, utc=True)

trade_log = pd.read_csv("data/processed/trade_log.csv")
logged_timestamps = set(trade_log["timestamp"].values)

MODEL_FEATURES = [
    "rsi_14", "ema_9", "ema_21", "ema_50",
    "ema_9_21_cross", "ema_21_50_cross",
    "macd", "macd_signal", "macd_hist",
    "bb_position", "bb_width", "atr_14",
    "return_1", "return_5", "return_15",
    "candle_body", "candle_range", "candle_ratio",
    "hour", "minute", "day_of_week",
    "rsi_14_lag1", "rsi_14_lag2",
    "macd_lag1", "macd_lag2",
    "atr_14_lag1", "atr_14_lag2",
    "bb_position_lag1", "bb_position_lag2",
    "5m_rsi_14", "5m_ema_9", "5m_ema_21",
    "5m_macd", "5m_macd_hist", "5m_atr_14",
    "15m_rsi_14", "15m_ema_9", "15m_ema_21",
    "15m_macd", "15m_macd_hist", "15m_atr_14",
    "vix_close", "vix_change", "vix_regime",
]

LABEL_MAP = {1: "UP", -1: "DOWN", 0: "SIDEWAYS"}
MARKET_START = 9 * 60 + 15   # 9:15 IST
MARKET_END   = 15 * 60 + 30  # 15:30 IST

model_files = sorted([f for f in os.listdir("models") if f.endswith(".pkl") and "w5" in f])
model = joblib.load(os.path.join("models", model_files[-1]))
available = [c for c in MODEL_FEATURES if c in df.columns]

label_cols = [c for c in df.columns if c.startswith("label_")]
df_clean = df.drop(columns=label_cols, errors="ignore")

print("=" * 75)
print("MISSED SIGNAL DEEP DIVE")
print("=" * 75)

# Track what the last logged ts was at each point in time
# trade_logger reads features.index[-1] each loop
# If that ts is already_logged → prints "already logged", skips

last_logged_before = trade_log[
    trade_log["timestamp"] < "2026-03-23 04:35:00+00:00"
]["timestamp"].iloc[-1] if len(trade_log) > 0 else None

print(f"\nLast trade logged today: {last_logged_before}")
print(f"  = {(pd.Timestamp(last_logged_before) + pd.Timedelta(hours=5,minutes=30)).strftime('%H:%M')} IST\n")

issues_found = {}

for ts_str in MISSED:
    ts = pd.Timestamp(ts_str)
    ist = ts + pd.Timedelta(hours=5, minutes=30)

    # 1. In parquet?
    in_parquet = ts in df.index

    # 2. already_logged?
    is_logged = ts_str in logged_timestamps

    # 3. DB candle exists?
    with engine.connect() as conn:
        r = conn.execute(text(
            "SELECT close FROM nifty_1min WHERE time = :t"
        ), {"t": ts})
        db_row = r.fetchone()
    in_db = db_row is not None
    entry_price = float(db_row[0]) if db_row else None

    # 4. is_market_open at that IST time?
    ist_mins = ist.hour * 60 + ist.minute
    market_open = MARKET_START <= ist_mins <= MARKET_END

    # 5. Re-run model prediction
    if in_parquet:
        row = df_clean.loc[ts:ts][available]
        pred  = model.predict(row)[0]
        proba = model.predict_proba(row)[0]
        conf  = round(max(proba) * 100, 1)
        sig   = LABEL_MAP[int(pred)]
        adx   = float(df.loc[ts, "adx_14"]) if "adx_14" in df.columns else 25.0
    else:
        sig, conf, adx = "N/A", 0, 0

    # Determine root cause
    if not in_parquet:
        cause = "NOT IN PARQUET — websocket_feed never wrote this row"
    elif not in_db:
        cause = "NOT IN DB — candle missing, entry price unavailable"
    elif not market_open:
        cause = "MARKET CLOSED at this time per is_market_open()"
    elif is_logged:
        cause = "already_logged() = True — duplicate prevention blocked it"
    elif sig == "SIDEWAYS":
        cause = "Model output SIDEWAYS at this timestamp (re-run confirms)"
    elif conf < 60:
        cause = f"Confidence {conf}% < 60% threshold (re-run confirms)"
    elif adx < 20:
        cause = f"ADX {adx:.1f} < 20 filter blocked it"
    else:
        cause = f"⚠ ALL FILTERS PASS — trade_logger was NOT running at this time"

    issues_found[ts_str] = cause

    print(f"  {ist.strftime('%H:%M')} IST  {sig:<10} {conf:>5}%  ADX:{adx:>5.1f}")
    print(f"    in_parquet:{in_parquet}  in_db:{in_db}  market_open:{market_open}  already_logged:{is_logged}")
    print(f"    → CAUSE: {cause}")
    print()

# ── Summary ──────────────────────────────────────────────────
print("=" * 75)
print("SUMMARY")
print("=" * 75)

from collections import Counter
cause_counts = Counter(issues_found.values())
for cause, count in cause_counts.most_common():
    print(f"  {count}x  {cause}")

# ── Check if trade_logger was alive at 14:50 ─────────────────
print(f"\n{'='*75}")
print("TIMELINE — was trade_logger still alive at 14:50 IST?")
print("=" * 75)

# The last parquet row is at 15:04 IST — websocket was alive
# Check what the parquet index[-1] would have been at each missed signal time
# by finding the latest parquet row AT OR BEFORE each missed timestamp

print(f"\n{'IST Time':<12} {'Parquet index[-1] at that moment':<40} {'Same as missed ts?'}")
print("-" * 75)
for ts_str in MISSED[-6:]:  # focus on afternoon missed signals
    ts = pd.Timestamp(ts_str)
    ist = ts + pd.Timedelta(hours=5, minutes=30)
    # What was the latest parquet row at this time?
    rows_before = df[df.index <= ts]
    latest = rows_before.index[-1] if len(rows_before) > 0 else None
    match = "✓ matches" if latest == ts else f"✗ latest was {latest}"
    print(f"  {ist.strftime('%H:%M')}       {str(latest):<40} {match}")

print(f"\nConclusion:")
print(f"  If parquet index[-1] == missed_ts AND already_logged(missed_ts) == False")
print(f"  AND all filters pass → trade_logger was NOT running at that time.")
print(f"  If parquet index[-1] != missed_ts → trade_logger read a different (newer) row")
print(f"  and the missed_ts was never the 'latest' row when the loop ran.")
