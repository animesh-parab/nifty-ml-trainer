"""
futures_feed.py
Angel One WebSocket → Nifty Futures ticks → 1-min OHLCV with real volume → TimescaleDB
Runs parallel to websocket_feed.py — data collection only, not used for signals yet
Purpose: Build volume dataset for V3 model training
"""

import os
import time
import threading
import requests
import pandas as pd
from datetime import datetime, timezone, date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

load_dotenv()

API_KEY     = os.getenv("ANGEL_API_KEY")
CLIENT_ID   = os.getenv("ANGEL_CLIENT_ID")
MPIN        = os.getenv("ANGEL_MPIN")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

DB_URL = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)

MARKET_START = 9 * 60 + 15
MARKET_END   = 15 * 60 + 30

# ── Known Nifty futures contracts ───────────────────────────
# Auto-fetched at startup, hardcoded as fallback
KNOWN_CONTRACTS = [
    {"token": "51714", "symbol": "NIFTY30MAR26FUT", "expiry": "2026-03-27"},
    {"token": "66691", "symbol": "NIFTY28APR26FUT", "expiry": "2026-04-28"},
    {"token": "66069", "symbol": "NIFTY26MAY26FUT", "expiry": "2026-05-26"},
]

# ── Tick buffer ──────────────────────────────────────────────
tick_buffer = []
tick_lock   = threading.Lock()
current_contract = None


# ── Fetch latest contracts from Angel One master file ────────
def fetch_current_contract():
    """Fetch near month Nifty futures token from Angel One master"""
    try:
        url  = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        data = requests.get(url, timeout=10).json()
        nifty_futures = [
            d for d in data
            if d.get("name") == "NIFTY"
            and d.get("instrumenttype") == "FUTIDX"
            and d.get("exch_seg") == "NFO"
        ]
        # Sort by expiry, pick nearest
        nifty_futures.sort(key=lambda x: datetime.strptime(x.get("expiry", "01JAN2099"), "%d%b%Y").date())
        today = datetime.now().strftime("%d%b%Y").upper()

        for contract in nifty_futures:
            expiry_str = contract.get("expiry", "")
            try:
                expiry_date = datetime.strptime(expiry_str, "%d%b%Y").date()
                if expiry_date >= datetime.now().date():
                    print(f"✓ Near month contract: {contract['symbol']} (token: {contract['token']}, expiry: {expiry_str})")
                    return {
                        "token":  contract["token"],
                        "symbol": contract["symbol"],
                        "expiry": expiry_date.strftime("%Y-%m-%d"),
                    }
            except:
                continue
    except Exception as e:
        print(f"✗ Failed to fetch contracts: {e}")

    # Fallback to hardcoded
    today = datetime.now().date()
    for c in KNOWN_CONTRACTS:
        if datetime.strptime(c["expiry"], "%Y-%m-%d").date() >= today:
            print(f"✓ Using fallback contract: {c['symbol']}")
            return c

    return KNOWN_CONTRACTS[0]


# ── DB setup ─────────────────────────────────────────────────
def init_db():
    """Create futures table if not exists"""
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS nifty_futures_1min (
                time        TIMESTAMPTZ      NOT NULL,
                open        DOUBLE PRECISION NOT NULL,
                high        DOUBLE PRECISION NOT NULL,
                low         DOUBLE PRECISION NOT NULL,
                close       DOUBLE PRECISION NOT NULL,
                volume      BIGINT           NOT NULL DEFAULT 0,
                contract    TEXT             NOT NULL
            )
        """))
        # Create hypertable if not already
        try:
            conn.execute(text(
                "SELECT create_hypertable('nifty_futures_1min', 'time', if_not_exists => TRUE)"
            ))
        except:
            pass
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_futures_time ON nifty_futures_1min (time DESC)"
        ))
        conn.commit()
    print("✓ nifty_futures_1min table ready")


# ── Load session saved by websocket_feed.py ──────────────────
SESSION_PATH = "data/processed/session.json"

def load_session(retries=10, wait=5):
    """Wait for websocket_feed.py to write session.json, then load it."""
    import json
    for i in range(retries):
        if os.path.exists(SESSION_PATH):
            try:
                with open(SESSION_PATH) as f:
                    s = json.load(f)
                if s.get("access_token") and s.get("feed_token"):
                    print("✓ Session loaded from websocket_feed")
                    return s["access_token"], s["feed_token"]
            except Exception as e:
                print(f"✗ Session read error: {e}")
        print(f"  Waiting for session.json... ({i+1}/{retries})")
        time.sleep(wait)
    print("✗ Could not load session — start websocket_feed.py first")
    return None, None


# ── WebSocket callbacks ──────────────────────────────────────
def on_data(wsapp, message):
    global tick_buffer
    try:
        if not isinstance(message, dict):
            return
        if "last_traded_price" not in message:
            return

        ltp    = message.get("last_traded_price", 0) / 100.0
        volume = message.get("volume_trade_for_the_day", 0)
        ts     = datetime.now(timezone.utc)

        with tick_lock:
            tick_buffer.append({
                "timestamp": ts,
                "price":     ltp,
                "volume":    volume,
            })
    except:
        pass


def on_open(wsapp):
    print("✓ WebSocket connected")


def on_error(wsapp, error):
    print(f"✗ WebSocket error: {error}")


def on_close(wsapp):
    print("✗ WebSocket closed")


# ── Aggregate ticks into 1-min candle ────────────────────────
def aggregate_ticks(ticks, candle_minute):
    if not ticks:
        return None
    prices = [t["price"] for t in ticks if t["price"] > 0]
    if not prices:
        return None

    # Volume = difference between first and last tick volume of the minute
    # Angel One sends cumulative day volume, so delta = volume traded this minute
    volumes = [t["volume"] for t in ticks if t["volume"] > 0]
    if len(volumes) >= 2:
        vol = max(0, volumes[-1] - volumes[0])
    elif volumes:
        vol = 0  # can't compute delta from single tick
    else:
        vol = 0

    return {
        "timestamp": candle_minute,
        "open":      prices[0],
        "high":      max(prices),
        "low":       min(prices),
        "close":     prices[-1],
        "volume":    vol,
    }


# ── Insert candle ─────────────────────────────────────────────
def insert_candle(candle, contract_symbol):
    try:
        engine = create_engine(DB_URL)
        ts = pd.Timestamp(candle["timestamp"]).tz_convert("UTC")
        with engine.connect() as conn:
            exists = conn.execute(text(
                "SELECT 1 FROM nifty_futures_1min WHERE time = :time AND contract = :contract"
            ), {"time": ts, "contract": contract_symbol}).fetchone()
            if not exists:
                conn.execute(text("""
                    INSERT INTO nifty_futures_1min (time, open, high, low, close, volume, contract)
                    VALUES (:time, :open, :high, :low, :close, :volume, :contract)
                """), {
                    "time":     ts,
                    "open":     candle["open"],
                    "high":     candle["high"],
                    "low":      candle["low"],
                    "close":    candle["close"],
                    "volume":   candle["volume"],
                    "contract": contract_symbol,
                })
                conn.commit()
                print(f"✓ Futures candle: {ts.strftime('%H:%M')} "
                      f"O:{candle['open']:.1f} H:{candle['high']:.1f} "
                      f"L:{candle['low']:.1f} C:{candle['close']:.1f} "
                      f"V:{candle['volume']:,}")
                return True
    except Exception as e:
        print(f"✗ Insert error: {e}")
    return False


# ── Is market open ────────────────────────────────────────────
def is_market_open():
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    mins = now.hour * 60 + now.minute
    return MARKET_START <= mins <= MARKET_END


# ── Candle manager thread ─────────────────────────────────────
def candle_manager(contract):
    global tick_buffer, current_contract
    last_minute    = None
    last_check_day = datetime.now().date()

    while True:
        try:
            now    = datetime.now(timezone.utc)
            minute = now.replace(second=0, microsecond=0)

            # Check for contract expiry daily at 9:15
            today = datetime.now().date()
            if today != last_check_day:
                last_check_day = today
                new_contract = fetch_current_contract()
                if new_contract["token"] != contract["token"]:
                    print(f"\n🔄 Contract rolled over: {contract['symbol']} → {new_contract['symbol']}")
                    contract = new_contract
                    current_contract = contract

            if last_minute is not None and minute != last_minute:
                with tick_lock:
                    ticks_to_process = tick_buffer.copy()
                    tick_buffer = []

                if ticks_to_process and is_market_open():
                    candle = aggregate_ticks(ticks_to_process, last_minute)
                    if candle and candle["volume"] >= 0:
                        insert_candle(candle, contract["symbol"])

            last_minute = minute
            time.sleep(1)

        except Exception as e:
            print(f"✗ Candle manager error: {e}")
            time.sleep(1)


# ── Stats printer ─────────────────────────────────────────────
def print_stats():
    """Print collection stats every 30 mins"""
    while True:
        time.sleep(1800)
        try:
            engine = create_engine(DB_URL)
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT COUNT(*), MIN(time), MAX(time) FROM nifty_futures_1min"
                ))
                row = result.fetchone()
                print(f"\n📊 Futures DB: {row[0]} candles | "
                      f"{row[1]} → {row[2]}")
        except:
            pass


# ── Main ──────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("Nifty ML Trainer — Futures Feed (Volume Collector)")
    print("=" * 50)

    # Init DB table
    init_db()

    # Get current contract
    contract = fetch_current_contract()
    print(f"Trading: {contract['symbol']} (expires {contract['expiry']})")

    # Load session from websocket_feed.py (no separate login)
    access_token, feed_token = load_session()
    if not access_token:
        return

    print(f"Market open: {is_market_open()}")

    # Start candle manager
    manager_thread = threading.Thread(
        target=candle_manager,
        args=(contract,),
        daemon=True
    )
    manager_thread.start()
    print("✓ Candle manager started")

    # Start stats printer
    stats_thread = threading.Thread(target=print_stats, daemon=True)
    stats_thread.start()

    # WebSocket setup
    token_list = [{"exchangeType": 2, "tokens": [contract["token"]]}]  # exchangeType 2 = NFO

    sws = SmartWebSocketV2(
        access_token,
        API_KEY,
        CLIENT_ID,
        feed_token,
        max_retry_attempt=5
    )

    def on_open_with_subscribe(wsapp):
        print("✓ WebSocket connected")
        sws.subscribe("fut123", 1, token_list)
        print(f"✓ Subscribed to {contract['symbol']}")

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