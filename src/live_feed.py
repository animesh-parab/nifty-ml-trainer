"""
live_feed.py
Angel One → TimescaleDB → features.parquet pipeline
Run every minute during market hours (9:15 AM - 3:30 PM IST)
"""

import os
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from SmartApi import SmartConnect
import pyotp
import yfinance as yf
import ta

load_dotenv()

# ── Config ──────────────────────────────────────────────────
API_KEY     = os.getenv("ANGEL_API_KEY")
CLIENT_ID   = os.getenv("ANGEL_CLIENT_ID")
MPIN        = os.getenv("ANGEL_MPIN")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

DB_URL = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)

NIFTY_TOKEN   = "99926000"
FEATURES_PATH = "data/processed/features.parquet"
MARKET_START  = 9 * 60 + 15
MARKET_END    = 15 * 60 + 30

# ── Angel One Auth ──────────────────────────────────────────
_smart_api = None

def get_smart_api():
    global _smart_api
    if _smart_api is not None:
        return _smart_api
    try:
        obj  = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = obj.generateSession(CLIENT_ID, MPIN, totp)
        if data["status"]:
            print("✓ Angel One login successful")
            _smart_api = obj
            return _smart_api
        else:
            print(f"✗ Login failed: {data.get('message')}")
            return None
    except Exception as e:
        print(f"✗ Angel One error: {e}")
        return None


# ── Fetch 1-min candles ─────────────────────────────────────
def fetch_candles(minutes=60):
    api = get_smart_api()
    if not api:
        return pd.DataFrame()
    try:
        to_date   = datetime.now()
        from_date = to_date - timedelta(minutes=minutes)
        response  = api.getCandleData({
            "exchange":    "NSE",
            "symboltoken": NIFTY_TOKEN,
            "interval":    "ONE_MINUTE",
            "fromdate":    from_date.strftime("%Y-%m-%d %H:%M"),
            "todate":      to_date.strftime("%Y-%m-%d %H:%M"),
        })
        if response and response.get("status"):
            df = pd.DataFrame(
                response["data"],
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = df[col].astype(float)
            print(f"✓ Fetched {len(df)} candles")
            return df
        else:
            print(f"✗ Candle fetch failed: {response.get('message')}")
            return pd.DataFrame()
    except Exception as e:
        print(f"✗ Candle error: {e}")
        return pd.DataFrame()


# ── Fetch VIX ───────────────────────────────────────────────
def fetch_vix():
    try:
        vix = yf.download("^INDIAVIX", period="2d", interval="1d", progress=False)
        if not vix.empty:
            val = float(vix["Close"].iloc[-1].iloc[0])
            print(f"✓ VIX: {val:.2f}")
            return val
    except Exception as e:
        print(f"✗ VIX error: {e}")
    return 15.0


# ── Insert candles into TimescaleDB ────────────────────────
def insert_candles(df):
    engine = create_engine(DB_URL)
    inserted = 0
    print(f"  Sample timestamp: {df['timestamp'].iloc[-1]} (type: {df['timestamp'].iloc[-1].tzinfo})")
    with engine.connect() as conn:
        for _, row in df.iterrows():
            try:
                exists = conn.execute(text(
                    "SELECT 1 FROM nifty_1min WHERE time = :time"
                ), {"time": row["timestamp"]}).fetchone()
                if not exists:
                    conn.execute(text("""
                        INSERT INTO nifty_1min (time, open, high, low, close)
                        VALUES (:time, :open, :high, :low, :close)
                    """), {
                        "time":  row["timestamp"],
                        "open":  row["open"],
                        "high":  row["high"],
                        "low":   row["low"],
                        "close": row["close"],
                    })
                    inserted += 1
            except Exception as e:
                print(f"  Insert error: {e}")
                break
        conn.commit()
    print(f"✓ Inserted {inserted} new candles into DB")


# ── Calculate features ──────────────────────────────────────
def calculate_features(df, vix_val):
    df = df.copy().sort_values("timestamp").reset_index(drop=True)
    c  = df["close"]
    h  = df["high"]
    l  = df["low"]

    df["rsi_14"]          = ta.momentum.RSIIndicator(c, 14).rsi()
    df["ema_9"]           = ta.trend.EMAIndicator(c, 9).ema_indicator()
    df["ema_21"]          = ta.trend.EMAIndicator(c, 21).ema_indicator()
    df["ema_50"]          = ta.trend.EMAIndicator(c, 50).ema_indicator()
    df["ema_9_21_cross"]  = (df["ema_9"] > df["ema_21"]).astype(int)
    df["ema_21_50_cross"] = (df["ema_21"] > df["ema_50"]).astype(int)

    macd = ta.trend.MACD(c)
    df["macd"]            = macd.macd()
    df["macd_signal"]     = macd.macd_signal()
    df["macd_hist"]       = macd.macd_diff()

    bb = ta.volatility.BollingerBands(c, 20, 2)
    df["bb_position"]     = (c - bb.bollinger_lband()) / (bb.bollinger_hband() - bb.bollinger_lband() + 1e-9)
    df["bb_width"]        = (bb.bollinger_hband() - bb.bollinger_lband()) / (bb.bollinger_mavg() + 1e-9)
    df["atr_14"]          = ta.volatility.AverageTrueRange(h, l, c, 14).average_true_range()

    df["return_1"]        = c.pct_change(1)
    df["return_5"]        = c.pct_change(5)
    df["return_15"]       = c.pct_change(15)

    df["candle_body"]     = (c - df["open"]).abs()
    df["candle_range"]    = h - l
    df["candle_ratio"]    = df["candle_body"] / (df["candle_range"] + 1e-9)

    df["hour"]            = df["timestamp"].dt.hour
    df["minute"]          = df["timestamp"].dt.minute
    df["day_of_week"]     = df["timestamp"].dt.dayofweek

    for col in ["rsi_14", "macd", "atr_14", "bb_position"]:
        df[f"{col}_lag1"] = df[col].shift(1)
        df[f"{col}_lag2"] = df[col].shift(2)

    df["5m_rsi_14"]       = ta.momentum.RSIIndicator(c, 14).rsi().rolling(5).mean()
    df["5m_ema_9"]        = ta.trend.EMAIndicator(c, 9).ema_indicator().rolling(5).mean()
    df["5m_ema_21"]       = ta.trend.EMAIndicator(c, 21).ema_indicator().rolling(5).mean()
    df["5m_macd"]         = macd.macd().rolling(5).mean()
    df["5m_macd_hist"]    = macd.macd_diff().rolling(5).mean()
    df["5m_atr_14"]       = df["atr_14"].rolling(5).mean()

    df["15m_rsi_14"]      = ta.momentum.RSIIndicator(c, 14).rsi().rolling(15).mean()
    df["15m_ema_9"]       = ta.trend.EMAIndicator(c, 9).ema_indicator().rolling(15).mean()
    df["15m_ema_21"]      = ta.trend.EMAIndicator(c, 21).ema_indicator().rolling(15).mean()
    df["15m_macd"]        = macd.macd().rolling(15).mean()
    df["15m_macd_hist"]   = macd.macd_diff().rolling(15).mean()
    df["15m_atr_14"]      = df["atr_14"].rolling(15).mean()

    adx_ind               = ta.trend.ADXIndicator(h, l, c, 14)
    df["adx_14"]          = adx_ind.adx()
    df["adx_pos"]         = adx_ind.adx_pos()
    df["adx_neg"]         = adx_ind.adx_neg()

    df["vix_close"]       = vix_val
    df["vix_change"]      = 0.0
    df["vix_regime"]      = int(0 if vix_val < 15 else 1 if vix_val < 20 else 2)

    return df


# ── Update features.parquet ─────────────────────────────────
def update_features_parquet(df):
    FEATURE_COLS = [
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
        "adx_14", "adx_pos", "adx_neg",
        "vix_close", "vix_change", "vix_regime",
    ]

    df = df.dropna(subset=FEATURE_COLS)
    df = df.set_index("timestamp")[FEATURE_COLS]
    df.index = pd.to_datetime(df.index, utc=True)

    existing = pd.read_parquet(FEATURES_PATH)
    label_cols = [c for c in existing.columns if c.startswith("label_")]
    existing.drop(columns=label_cols, inplace=True, errors="ignore")
    existing.index = pd.to_datetime(existing.index, utc=True)

    new_rows = df[~df.index.isin(existing.index)]
    if len(new_rows) == 0:
        print("✓ No new rows to append")
        return

    combined = pd.concat([existing, new_rows]).sort_index()
    combined.to_parquet(FEATURES_PATH)
    print(f"✓ features.parquet updated — +{len(new_rows)} rows, total {len(combined)}")


# ── Is market open? ─────────────────────────────────────────
def is_market_open():
    now  = datetime.now()
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return MARKET_START <= mins <= MARKET_END


# ── Main loop ───────────────────────────────────────────────
def main():
    print("=" * 50)
    print("Nifty ML Trainer — Live Feed")
    print("=" * 50)

    api = get_smart_api()
    if not api:
        print("✗ Could not connect to Angel One.")
        return

    vix_val  = fetch_vix()
    vix_date = datetime.now().date()

    print(f"Market open: {is_market_open()}")
    print("Starting loop... (Ctrl+C to stop)\n")

    while True:
        try:
            now = datetime.now()

            if now.date() != vix_date:
                vix_val  = fetch_vix()
                vix_date = now.date()

            if not is_market_open():
                print(f"[{now.strftime('%H:%M:%S')}] Market closed — checking in 60s")
                time.sleep(120)
                continue

            print(f"\n[{now.strftime('%H:%M:%S')}] Fetching live data...")

            candles = fetch_candles(minutes=60)
            if candles.empty:
                print("✗ No candles — retrying in 60s")
                time.sleep(120)
                continue

            insert_candles(candles)
            features = calculate_features(candles, vix_val)
            update_features_parquet(features)

            print(f"✓ Done — sleeping 120s")
            time.sleep(120)

        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as e:
            print(f"✗ Loop error: {e}")
            time.sleep(120)


if __name__ == "__main__":
    main()