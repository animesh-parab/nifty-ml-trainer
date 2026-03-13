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

load_dotenv()

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
            "entry_price", "target", "stoploss",
            "rr_ratio", "atr", "rsi", "vix", "outcome"
        ]).to_csv(TRADE_LOG_PATH, index=False)
        print(f"✓ Trade log created: {TRADE_LOG_PATH}")


def already_logged(timestamp):
    try:
        df = pd.read_csv(TRADE_LOG_PATH)
        return str(timestamp) in df["timestamp"].values
    except:
        return False


def log_trade(ts, signal, confidence, entry, atr, rsi, vix):
    if signal == "UP":
        target   = round(entry + atr * 1.5, 2)
        stoploss = round(entry - atr * 0.8, 2)
    else:
        target   = round(entry - atr * 1.5, 2)
        stoploss = round(entry + atr * 0.8, 2)

    rr = round(abs(target - entry) / abs(stoploss - entry), 2)

    row = {
        "timestamp":  str(ts),
        "signal":     signal,
        "confidence": confidence,
        "entry_price": entry,
        "target":     target,
        "stoploss":   stoploss,
        "rr_ratio":   rr,
        "atr":        round(atr, 2),
        "rsi":        round(rsi, 2),
        "vix":        round(vix, 2),
        "outcome":    "PENDING"
    }

    df = pd.read_csv(TRADE_LOG_PATH)
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(TRADE_LOG_PATH, index=False)

    print(f"\n{'='*50}")
    print(f"🚨 TRADE SIGNAL LOGGED")
    print(f"  Time:       {ts}")
    print(f"  Signal:     {signal} ({confidence}%)")
    print(f"  Entry:      ₹{entry}")
    print(f"  Target:     ₹{target}")
    print(f"  Stoploss:   ₹{stoploss}")
    print(f"  R/R:        {rr}")
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

        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error: {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()
