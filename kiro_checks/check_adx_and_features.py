"""
kiro_checks/check_adx_and_features.py
Investigates why trade_logger stopped logging after 10:04 IST on 2026-03-23

Checks:
1. Does features.parquet have adx_14 column?
2. ADX values around 10:04 IST and after — did it drop below 20?
3. What were the model signals (conf) after 10:04 — were there missed opportunities?
4. Was already_logged() potentially blocking anything?
"""

import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

FEATURES_PATH  = "data/processed/features.parquet"
TRADE_LOG_PATH = "data/processed/trade_log.csv"

df = pd.read_parquet(FEATURES_PATH)
df.index = pd.to_datetime(df.index, utc=True)

# ── 1. Column check ──────────────────────────────────────────
print("=" * 60)
print("1. COLUMN CHECK")
print("=" * 60)
print(f"adx_14 in features.parquet: {'adx_14' in df.columns}")
print(f"adx_pos in features.parquet: {'adx_pos' in df.columns}")
print(f"adx_neg in features.parquet: {'adx_neg' in df.columns}")
print(f"Total columns: {len(df.columns)}")
print(f"All columns: {df.columns.tolist()}")

# ── 2. Today's data only ─────────────────────────────────────
today = df[df.index >= "2026-03-23"].copy()
today_ist = today.copy()
today_ist.index = today_ist.index + pd.Timedelta(hours=5, minutes=30)

print(f"\n{'='*60}")
print("2. ADX VALUES TODAY (around 10:04 IST cutoff)")
print("=" * 60)

if "adx_14" in df.columns:
    # Show ADX around the cutoff window
    window = today_ist.between_time("09:00", "11:30")
    print(f"\n{'Time (IST)':<12} {'ADX':>8} {'Signal would pass ADX>=20?':>28}")
    print("-" * 52)
    for ts, row in window.iterrows():
        adx = row["adx_14"]
        passes = "YES" if adx >= 20 else "NO  <-- BLOCKED"
        print(f"  {ts.strftime('%H:%M')}      {adx:>7.2f}   {passes}")
else:
    print("adx_14 NOT in features.parquet — this is the problem!")
    print("trade_logger uses: adx = float(full_row['adx_14'].iloc[-1]) if 'adx_14' in full_row.columns else 25.0")
    print("So it would default to 25.0 always — ADX filter would always PASS")
    print("That means ADX is NOT the blocker. Something else stopped it.")

# ── 3. Model signals after 10:04 IST ────────────────────────
print(f"\n{'='*60}")
print("3. WHAT SIGNALS EXISTED AFTER 10:04 IST (missed trades?)")
print("=" * 60)

import joblib

MODELS_DIR = "models"
model_files = [f for f in os.listdir(MODELS_DIR) if f.endswith(".pkl") and "w5" in f]
model_files.sort()
model = joblib.load(os.path.join(MODELS_DIR, model_files[-1]))
print(f"Model loaded: {model_files[-1]}")

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

label_cols = [c for c in df.columns if c.startswith("label_")]
df_clean = df.drop(columns=label_cols, errors="ignore")
available = [c for c in MODEL_FEATURES if c in df_clean.columns]
missing_features = [c for c in MODEL_FEATURES if c not in df_clean.columns]

print(f"Missing model features in parquet: {missing_features}")

after_cutoff = today_ist[today_ist.index >= today_ist.index[0].replace(hour=10, minute=5)]
after_cutoff_utc = today[today.index >= "2026-03-23 04:35:00+00:00"]  # 10:05 IST

LABEL_MAP = {1: "UP", -1: "DOWN", 0: "SIDEWAYS"}

print(f"\n{'Time (IST)':<12} {'Signal':<10} {'Conf%':>6} {'ADX':>7} {'Would log?':>12}")
print("-" * 55)

missed_signals = []
for ts_utc, row in after_cutoff_utc.iterrows():
    row_df = pd.DataFrame([row[available]])
    pred   = model.predict(row_df)[0]
    proba  = model.predict_proba(row_df)[0]
    conf   = round(max(proba) * 100, 1)
    sig    = LABEL_MAP[int(pred)]
    adx    = row["adx_14"] if "adx_14" in row.index else 25.0
    ts_ist = ts_utc + pd.Timedelta(hours=5, minutes=30)

    would_log = sig != "SIDEWAYS" and conf >= 60.0 and adx >= 20
    flag = "YES <-- MISSED" if would_log else ""
    print(f"  {ts_ist.strftime('%H:%M')}      {sig:<10} {conf:>5}%  {adx:>6.1f}  {flag}")
    if would_log:
        missed_signals.append({"time_ist": ts_ist.strftime("%H:%M"), "signal": sig, "conf": conf, "adx": adx})

print(f"\nTotal missed signals after 10:04 IST: {len(missed_signals)}")

# ── 4. already_logged check ──────────────────────────────────
print(f"\n{'='*60}")
print("4. already_logged() ANALYSIS")
print("=" * 60)
tl = pd.read_csv(TRADE_LOG_PATH)
tl["timestamp"] = pd.to_datetime(tl["timestamp"], utc=True)
today_trades = tl[tl["timestamp"] >= "2026-03-23"]
print(f"Logged timestamps today: {len(today_trades)}")
last_logged_ts = today_trades["timestamp"].iloc[-1]
last_logged_ist = last_logged_ts + pd.Timedelta(hours=5, minutes=30)
print(f"Last logged: {last_logged_ist.strftime('%H:%M')} IST  ({last_logged_ts})")
print(f"\nNote: already_logged() reads the FULL csv every 30s and checks str(timestamp)")
print(f"If features.parquet index timestamp != str format in CSV, it would always return False (not the blocker)")
print(f"\nSample CSV timestamp format:  '{today_trades['timestamp'].iloc[-1]}'")
print(f"Sample parquet index format:  '{str(after_cutoff_utc.index[0])}'")
