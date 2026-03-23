"""
websocket_feed.py
Angel One WebSocket → real-time ticks → 1-min candles → TimescaleDB → features.parquet
Replaces live_feed.py — no rate limits, real-time data
"""

import os
import time
import threading
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from collections import defaultdict
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
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

# ── Tick buffer — stores raw ticks for current candle ───────
tick_buffer = []
tick_lock   = threading.Lock()
current_candle = None

# ── Angel One Auth ──────────────────────────────────────────
def login():
    try:
        obj  = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = obj.generateSession(CLIENT_ID, MPIN, totp)
        if data["status"]:
            feed_token   = obj.getfeedToken()
            access_token = data["data"]["jwtToken"]
            obj.access_token = access_token
            print(f"✓ Login successful")
            return obj, feed_token
        else:
            print(f"✗ Login failed: {data.get('message')}")
            return None, None
    except Exception as e:
        print(f"✗ Login error: {e}")
        return None, None


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


# ── WebSocket callbacks ─────────────────────────────────────
def on_data(wsapp, message):
    global tick_buffer
    try:
        if not isinstance(message, dict):
            return
        if "last_traded_price" not in message:
            return
        ltp = message.get("last_traded_price", 0) / 100.0
        ts  = datetime.now(timezone.utc)
        with tick_lock:
            tick_buffer.append({"timestamp": ts, "price": ltp})
    except Exception as e:
        pass


def on_open(wsapp):
    print("✓ WebSocket connected")


def on_error(wsapp, error):
    print(f"✗ WebSocket error: {error}")


def on_close(wsapp):
    print("✗ WebSocket closed")


# ── Aggregate ticks into 1-min candle ──────────────────────
def aggregate_ticks_to_candle(ticks, candle_minute):
    if not ticks:
        return None
    prices = [t["price"] for t in ticks if t["price"] > 0]
    if not prices:
        return None
    return {
        "timestamp": candle_minute,
        "open":      prices[0],
        "high":      max(prices),
        "low":       min(prices),
        "close":     prices[-1],
        "volume":    0.0
    }


# ── Insert candle into TimescaleDB ──────────────────────────
def insert_candle(candle):
    try:
        engine = create_engine(DB_URL)
        ts = pd.Timestamp(candle["timestamp"]).tz_convert("UTC")
        with engine.connect() as conn:
            exists = conn.execute(text(
                "SELECT 1 FROM nifty_1min WHERE time = :time"
            ), {"time": ts}).fetchone()
            if not exists:
                conn.execute(text("""
                    INSERT INTO nifty_1min (time, open, high, low, close)
                    VALUES (:time, :open, :high, :low, :close)
                """), {
                    "time":  ts,
                    "open":  candle["open"],
                    "high":  candle["high"],
                    "low":   candle["low"],
                    "close": candle["close"],
                })
                conn.commit()
                print(f"✓ Candle inserted: {ts} O:{candle['open']} H:{candle['high']} L:{candle['low']} C:{candle['close']}")
                return True
    except Exception as e:
        print(f"✗ Insert error: {e}")
    return False


# ── Calculate features ──────────────────────────────────────
def calculate_and_update_features(vix_val):
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT time, open, high, low, close FROM nifty_1min ORDER BY time DESC LIMIT 200"
            ))
            rows = result.fetchall()

        if len(rows) < 50:
            print("✗ Not enough candles for features")
            return

        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp").reset_index(drop=True)

        c = df["close"]
        h = df["high"]
        l = df["low"]

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
        new_df = df.set_index("timestamp")[FEATURE_COLS]

        existing = pd.read_parquet(FEATURES_PATH)
        label_cols = [c for c in existing.columns if c.startswith("label_")]
        existing.drop(columns=label_cols, inplace=True, errors="ignore")
        existing.index = pd.to_datetime(existing.index, utc=True)

        new_rows = new_df[~new_df.index.isin(existing.index)]
        if len(new_rows) > 0:
            combined = pd.concat([existing, new_rows]).sort_index()
            combined.to_parquet(FEATURES_PATH)
            print(f"✓ Features updated — +{len(new_rows)} rows")
        else:
            print("✓ Features up to date")

    except Exception as e:
        print(f"✗ Feature update error: {e}")


# ── Is market open? ─────────────────────────────────────────
def is_market_open():
    now  = datetime.now()
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return MARKET_START <= mins <= MARKET_END


# ── 1-min candle manager (runs in background thread) ────────
def candle_manager(vix_val):
    global tick_buffer
    last_minute = None

    while True:
        try:
            now    = datetime.now(timezone.utc)
            minute = now.replace(second=0, microsecond=0)

            if last_minute is not None and minute != last_minute:
                with tick_lock:
                    ticks_to_process = tick_buffer.copy()
                    tick_buffer = []

                candle = aggregate_ticks_to_candle(ticks_to_process, last_minute)
                if candle:
                    inserted = insert_candle(candle)
                    if inserted:
                        calculate_and_update_features(vix_val)

            last_minute = minute
            time.sleep(1)

        except Exception as e:
            print(f"✗ Candle manager error: {e}")
            time.sleep(1)


# ── Main ────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("Nifty ML Trainer — WebSocket Live Feed")
    print("=" * 50)

    smart_api, feed_token = login()
    if not smart_api:
        return

    vix_val = fetch_vix()
    print(f"Market open: {is_market_open()}")

    manager_thread = threading.Thread(
        target=candle_manager,
        args=(vix_val,),
        daemon=True
    )
    manager_thread.start()
    print("✓ Candle manager started")

    token_list = [{"exchangeType": 1, "tokens": ["99926000"]}]

    sws = SmartWebSocketV2(
        smart_api.access_token,
        API_KEY,
        CLIENT_ID,
        feed_token,
        max_retry_attempt=5
    )

    def on_open_with_subscribe(wsapp):
        print("✓ WebSocket connected")
        sws.subscribe("abc123", 1, token_list)
        print("✓ Subscribed to Nifty 50")

    sws.on_open  = on_open_with_subscribe
    sws.on_data  = on_data
    sws.on_error = on_error
    sws.on_close = on_close

    print("Connecting to WebSocket... (Ctrl+C to stop)\n")
    try:
        sws.connect()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopped by user.")
        sws.close_connection()


if __name__ == "__main__":
    main()