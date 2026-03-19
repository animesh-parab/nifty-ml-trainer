"""
trade_logger.py
Automatically logs trade signals to CSV when confidence > 60%
Run alongside live_feed.py during market hours
"""

import os
import time
import pandas as pd
import joblib
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import sys
sys.path.insert(0, os.path.dirname(__file__))
from smart_exits import get_exits

load_dotenv()

DB_URL = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)

FEATURES_PATH  = "data/processed/features.parquet"
TRADE_LOG_PATH = "data/processed/trade_log.csv"
MODELS_DIR     = "models"
MIN_CONFIDENCE = 60.0
MARKET_START   = 9 * 60 + 15
MARKET_END     = 15 * 60 + 30

LABEL_MAP = {1: "UP", -1: "DOWN", 0: "SIDEWAYS"}


def load_model():
    files = [f for f in os.listdir(MODELS_DIR)
             if f.endswith(".pkl") and "w5" in f]
    files.sort()
    return joblib.load(os.path.join(MODELS_DIR, files[-1]))


def is_market_open():
    now  = datetime.now()
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return MARKET_START <= mins <= MARKET_END


def get_latest_row():
    df = pd.read_parquet(FEATURES_PATH)
    label_cols = [c for c in df.columns if c.startswith("label_")]
    df.drop(columns=label_cols, inplace=True, errors="ignore")
    return df.tail(1)


def init_log():
    if not os.path.exists(TRADE_LOG_PATH):
        pd.DataFrame(columns=[
            "timestamp", "signal", "confidence",
            "entry_price", "stoploss", "sl_reason",
            "t1", "t1_reason", "t2", "t2_reason",
            "t3", "t3_reason", "rr_t1", "rr_t2", "rr_t3",
            "atr", "rsi", "vix", "outcome"
        ]).to_csv(TRADE_LOG_PATH, index=False)
        print(f"✓ Trade log created: {TRADE_LOG_PATH}")


def already_logged(timestamp):
    try:
        df = pd.read_csv(TRADE_LOG_PATH)
        return str(timestamp) in df["timestamp"].values
    except:
        return False


def log_trade(ts, signal, confidence, entry, atr, rsi, vix):
    engine = create_engine(DB_URL)
    exits  = get_exits(signal, entry, atr, engine)

    row = {
        "timestamp":   str(ts),
        "signal":      signal,
        "confidence":  confidence,
        "entry_price": round(entry, 2),
        "stoploss":    exits["stoploss"],
        "sl_reason":   exits["sl_reason"],
        "t1":          exits["t1"],
        "t1_reason":   exits["t1_reason"],
        "t2":          exits["t2"],
        "t2_reason":   exits["t2_reason"],
        "t3":          exits["t3"],
        "t3_reason":   exits["t3_reason"],
        "rr_t1":       exits["rr_t1"],
        "rr_t2":       exits["rr_t2"],
        "rr_t3":       exits["rr_t3"],
        "atr":         round(atr, 2),
        "rsi":         round(rsi, 2),
        "vix":         round(vix, 2),
        "outcome":     "PENDING"
    }

    df = pd.read_csv(TRADE_LOG_PATH)
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(TRADE_LOG_PATH, index=False)

    print(f"\n{'='*50}")
    print(f"🚨 TRADE SIGNAL LOGGED")
    print(f"  Time:     {ts}")
    print(f"  Signal:   {signal} ({confidence}%)")
    print(f"  Entry:    ₹{entry:.2f}")
    print(f"  SL:       ₹{exits['stoploss']} (-{exits['sl_pts']} pts) [{exits['sl_reason']}]")
    print(f"  T1:       ₹{exits['t1']} (+{exits['t1_pts']} pts) R/R:{exits['rr_t1']} [{exits['t1_reason']}]")
    print(f"  T2:       ₹{exits['t2']} (+{exits['t2_pts']} pts) R/R:{exits['rr_t2']} [{exits['t2_reason']}]")
    print(f"  T3:       ₹{exits['t3']} (+{exits['t3_pts']} pts) R/R:{exits['rr_t3']} [{exits['t3_reason']}]")
    print(f"{'='*50}\n")


def main():
    print("=" * 50)
    print("Nifty ML Trainer — Trade Logger")
    print("=" * 50)

    model = load_model()
    init_log()
    last_signal_ts = None

    print("Watching for trade signals... (Ctrl+C to stop)\n")

    while True:
        try:
            now = datetime.now()

            if not is_market_open():
                print(f"[{now.strftime('%H:%M:%S')}] Market closed — checking in 60s")
                time.sleep(60)
                continue

            try:
                features = get_latest_row()
            except Exception:
                time.sleep(5)
                features = get_latest_row()

            ts   = features.index[-1]
            pred = model.predict(features)[0]
            proba = model.predict_proba(features)[0]
            conf  = round(max(proba) * 100, 1)
            sig   = LABEL_MAP[int(pred)]

            print(f"[{now.strftime('%H:%M:%S')}] {sig} {conf}% — ", end="")

            if sig != "SIDEWAYS" and conf >= MIN_CONFIDENCE:
                try:
                    engine = create_engine(DB_URL)
                    with engine.connect() as conn:
                        result = conn.execute(text(
                            "SELECT close FROM nifty_1min ORDER BY time DESC LIMIT 1"
                        ))
                        entry = float(result.fetchone()[0])
                except:
                    entry = float(features["ema_9"].iloc[-1])
                atr   = float(features["atr_14"].iloc[-1])
                rsi   = float(features["rsi_14"].iloc[-1])
                vix   = float(features["vix_close"].iloc[-1])
                if not already_logged(ts):
                    log_trade(ts, sig, conf, entry, atr, rsi, vix)
                else:
                    print("already logged")
            else:
                print("no trade")

            last_signal_ts = ts
            time.sleep(30)

        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error: {e}")
            time.sleep(30)

        


if __name__ == "__main__":
    main()
