"""
trade_logger.py
Automatically logs trade signals to CSV when confidence > 60%
Run alongside live_feed.py during market hours
"""

import os
import time
import logging
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
COOLDOWN_SECS  = 300  # 5-min cooldown per direction

LABEL_MAP = {1: "UP", -1: "DOWN", 0: "SIDEWAYS"}


def setup_logging():
    log_dir = os.path.join("logs", datetime.now().strftime("%Y-%m-%d"))
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "trade_logger.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(),
        ]
    )
    return logging.getLogger(__name__)


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

def get_latest_row():
    df = pd.read_parquet(FEATURES_PATH)
    label_cols = [c for c in df.columns if c.startswith("label_")]
    df.drop(columns=label_cols, inplace=True, errors="ignore")
    # Use only the 44 features model was trained on
    # ADX columns kept separately for filtering only
    available = [c for c in MODEL_FEATURES if c in df.columns]
    row = df.tail(1)
    return row[available], row  # returns (model_features, full_row)


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


def log_trade(ts, signal, confidence, entry, atr, rsi, vix, engine):
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

    logger = setup_logging()
    model  = load_model()
    engine = create_engine(DB_URL)
    init_log()

    # Cooldown tracking: direction → last logged datetime
    last_logged_time = {"UP": None, "DOWN": None}

    logger.info("Watching for trade signals... (Ctrl+C to stop)")

    while True:
        try:
            now = datetime.now()

            if not is_market_open():
                logger.info("Market closed — checking in 60s")
                time.sleep(60)
                continue

            try:
                features, full_row = get_latest_row()
            except Exception:
                time.sleep(5)
                features, full_row = get_latest_row()

            ts    = features.index[-1]
            pred  = model.predict(features)[0]
            proba = model.predict_proba(features)[0]
            conf  = round(max(proba) * 100, 1)
            sig   = LABEL_MAP[int(pred)]

            adx   = float(full_row["adx_14"].iloc[-1]) if "adx_14" in full_row.columns else 25.0
            trend = "TRENDING" if adx >= 20 else "RANGING"

            if sig != "SIDEWAYS" and conf >= MIN_CONFIDENCE and adx >= 20:
                # Cooldown check — block same direction for 5 mins
                last_time = last_logged_time.get(sig)
                elapsed   = (now - last_time).total_seconds() if last_time else COOLDOWN_SECS + 1
                if elapsed < COOLDOWN_SECS:
                    remaining = int(COOLDOWN_SECS - elapsed)
                    logger.info(f"{sig} {conf}% ADX:{adx:.1f} ({trend}) — cooldown ({remaining}s left for {sig})")
                    time.sleep(30)
                    continue

                try:
                    with engine.connect() as conn:
                        result = conn.execute(text(
                            "SELECT close FROM nifty_1min ORDER BY time DESC LIMIT 1"
                        ))
                        entry = float(result.fetchone()[0])
                except Exception:
                    entry = float(features["ema_9"].iloc[-1])

                atr = float(features["atr_14"].iloc[-1])
                rsi = float(features["rsi_14"].iloc[-1])
                vix = float(features["vix_close"].iloc[-1])

                if not already_logged(ts):
                    log_trade(ts, sig, conf, entry, atr, rsi, vix, engine)
                    last_logged_time[sig] = now
                else:
                    logger.info(f"{sig} {conf}% ADX:{adx:.1f} ({trend}) — already logged")
            else:
                logger.info(f"{sig} {conf}% ADX:{adx:.1f} ({trend}) — no trade")

            time.sleep(30)

        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(30)




if __name__ == "__main__":
    main()
