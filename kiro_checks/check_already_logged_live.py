"""
kiro_checks/check_already_logged_live.py

Reconstructs what trade_logger was actually printing all afternoon.
Every 30s it reads features.index[-1] and checks already_logged().

We know:
- parquet was updating every minute (confirmed)
- trade_logger was alive (lots of messages seen)
- nothing was logged after 10:04 IST

So we simulate every 30s loop iteration using the actual parquet timestamps
and check what already_logged() would have returned at each point.

Key question: was features.index[-1] returning an already-logged timestamp
even though the parquet was advancing?
"""

import pandas as pd
import os

FEATURES_PATH  = "data/processed/features.parquet"
TRADE_LOG_PATH = "data/processed/trade_log.csv"

df = pd.read_parquet(FEATURES_PATH)
df.index = pd.to_datetime(df.index, utc=True)

trade_log = pd.read_csv(TRADE_LOG_PATH)
trade_log["timestamp"] = pd.to_datetime(trade_log["timestamp"], utc=True)
logged_timestamps = set(trade_log["timestamp"].astype(str).values)

# Today's parquet rows only
today = df[df.index >= "2026-03-23"].copy()

print("=" * 70)
print("RECONSTRUCTING TRADE_LOGGER TERMINAL OUTPUT (today, after 10:04 IST)")
print("=" * 70)
print(f"Total parquet rows today: {len(today)}")
print(f"Total logged timestamps in CSV: {len(logged_timestamps)}")

# The parquet updates every ~1 min
# trade_logger reads every 30s — so same parquet row gets read twice per minute
# Simulate: for each parquet row, what would 2 loop iterations see?

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

import joblib
model_files = sorted([f for f in os.listdir("models") if f.endswith(".pkl") and "w5" in f])
model = joblib.load(os.path.join("models", model_files[-1]))

label_cols = [c for c in df.columns if c.startswith("label_")]
df_clean = df.drop(columns=label_cols, errors="ignore")
available = [c for c in MODEL_FEATURES if c in df_clean.columns]

# Focus on after 10:04 IST (04:34 UTC)
after_cutoff = today[today.index > "2026-03-23 04:34:00+00:00"]

print(f"\n{'IST':<8} {'UTC ts (features.index[-1])':<35} {'Signal':<10} {'Conf%':>6} {'ADX':>6} {'already_logged?':<17} {'Terminal output'}")
print("-" * 115)

already_logged_count = 0
no_trade_count = 0
would_log_count = 0

for ts, row in after_cutoff.iterrows():
    ist = ts + pd.Timedelta(hours=5, minutes=30)
    ts_str = str(ts)

    # What already_logged() returns
    is_logged = ts_str in logged_timestamps

    # Model prediction
    row_df = pd.DataFrame([row[available]])
    pred  = model.predict(row_df)[0]
    proba = model.predict_proba(row_df)[0]
    conf  = round(max(proba) * 100, 1)
    sig   = LABEL_MAP[int(pred)]
    adx   = float(row["adx_14"]) if "adx_14" in row.index else 25.0

    # What trade_logger would print
    passes_filters = sig != "SIDEWAYS" and conf >= 60.0 and adx >= 20.0

    if passes_filters and is_logged:
        terminal = "already logged"
        already_logged_count += 1
    elif passes_filters and not is_logged:
        terminal = "⚠ SHOULD HAVE LOGGED — was process alive?"
        would_log_count += 1
    else:
        terminal = "no trade"
        no_trade_count += 1

    # Only print interesting rows — filter passes or already_logged
    if passes_filters:
        print(f"  {ist.strftime('%H:%M')}   {ts_str:<35} {sig:<10} {conf:>5}%  {adx:>5.1f}  {str(is_logged):<17} {terminal}")

print(f"\n{'='*70}")
print("SUMMARY")
print("=" * 70)
print(f"  'already logged' would print:          {already_logged_count} times")
print(f"  'no trade' would print:                {no_trade_count} times")
print(f"  Should have logged but didn't:         {would_log_count} times")

print(f"\n{'='*70}")
print("WHAT THE TERMINAL LIKELY SHOWED ALL AFTERNOON")
print("=" * 70)
print(f"  Mostly: '[HH:MM:SS] SIDEWAYS/UP/DOWN XX% ADX:XX (TRENDING/RANGING) — no trade'")
print(f"  For {already_logged_count} signal rows: '[HH:MM:SS] ... — already logged'")
print(f"  For {would_log_count} rows: process should have logged but didn't → was NOT running")

# Show the exact already_logged hits so we know what was printing
if already_logged_count > 0:
    print(f"\nExact 'already logged' timestamps (these were printing in terminal):")
    for ts, row in after_cutoff.iterrows():
        ts_str = str(ts)
        is_logged = ts_str in logged_timestamps
        row_df = pd.DataFrame([row[available]])
        pred  = model.predict(row_df)[0]
        proba = model.predict_proba(row_df)[0]
        conf  = round(max(proba) * 100, 1)
        sig   = LABEL_MAP[int(pred)]
        adx   = float(row["adx_14"]) if "adx_14" in row.index else 25.0
        passes = sig != "SIDEWAYS" and conf >= 60.0 and adx >= 20.0
        if passes and is_logged:
            ist = ts + pd.Timedelta(hours=5, minutes=30)
            print(f"  {ist.strftime('%H:%M')} IST  {sig} {conf}%  ADX:{adx:.1f}  ts:{ts_str}")
