"""
backfill.py
Run after 3:30 PM with websocket_feed stopped.

Steps:
  1. Fetches missing candles from Angel One (Jan 23 - Mar 12 + Mar 25 gap)
  2. Combines with already-fetched data/raw/backfill_candles.csv
  3. Inserts everything into nifty_1min DB (skips duplicates)

Usage: python src/backfill.py
"""

import os
import time
import pandas as pd
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from SmartApi import SmartConnect
import pyotp

load_dotenv()

API_KEY     = os.getenv("ANGEL_API_KEY")
CLIENT_ID   = os.getenv("ANGEL_CLIENT_ID")
MPIN        = os.getenv("ANGEL_MPIN")
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET")

DB_URL = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)

NIFTY_TOKEN  = "99926000"
CSV_PATH     = "data/raw/backfill_candles.csv"

# ── Date ranges to fetch ─────────────────────────────────────
FETCH_RANGES = [
    (date(2026, 1, 23), date(2026, 2, 13)),   # gap — was rate limited earlier
    (date(2026, 3, 25), date(2026, 3, 25)),   # missed day (didn't run)
]


def login():
    obj  = SmartConnect(api_key=API_KEY)
    totp = pyotp.TOTP(TOTP_SECRET).now()
    data = obj.generateSession(CLIENT_ID, MPIN, totp)
    if data["status"]:
        print("✓ Login successful")
        return obj
    raise Exception(f"Login failed: {data.get('message')}")


def get_trading_days(from_date, to_date):
    days, current = [], from_date
    while current <= to_date:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def fetch_day(api, day):
    from_str = f"{day.strftime('%Y-%m-%d')} 09:15"
    to_str   = f"{day.strftime('%Y-%m-%d')} 15:30"
    try:
        resp = api.getCandleData({
            "exchange":    "NSE",
            "symboltoken": NIFTY_TOKEN,
            "interval":    "ONE_MINUTE",
            "fromdate":    from_str,
            "todate":      to_str,
        })
        if resp and resp.get("status") and resp.get("data"):
            df = pd.DataFrame(resp["data"],
                              columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df
        else:
            print(f"  ✗ {day}: {resp.get('message', 'no data')}")
            return pd.DataFrame()
    except Exception as e:
        print(f"  ✗ {day}: {e}")
        return pd.DataFrame()


def fetch_missing(api):
    all_frames = []
    for from_date, to_date in FETCH_RANGES:
        days = get_trading_days(from_date, to_date)
        print(f"\nFetching {from_date} → {to_date} ({len(days)} days)")
        for i, day in enumerate(days):
            print(f"  [{i+1}/{len(days)}] {day}...", end=" ")
            df = fetch_day(api, day)
            if not df.empty:
                all_frames.append(df)
                print(f"✓ {len(df)} candles")
            else:
                print("skipped")
            time.sleep(0.5)
    return pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()


def insert_to_db(df, engine):
    print(f"\nInserting {len(df)} candles into DB...")
    inserted = 0
    skipped  = 0
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
                        "open":  float(row["open"]),
                        "high":  float(row["high"]),
                        "low":   float(row["low"]),
                        "close": float(row["close"]),
                    })
                    inserted += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"  Insert error at {row['timestamp']}: {e}")
        conn.commit()
    print(f"✓ Inserted: {inserted} | Skipped (already exist): {skipped}")
    return inserted


def main():
    print("=" * 55)
    print("Nifty Backfill — EOD Script")
    print("=" * 55)

    api    = login()
    engine = create_engine(DB_URL)

    # Step 1 — load already-fetched CSV
    existing_csv = pd.DataFrame()
    if os.path.exists(CSV_PATH):
        existing_csv = pd.read_csv(CSV_PATH)
        existing_csv["timestamp"] = pd.to_datetime(existing_csv["timestamp"], utc=True)
        print(f"✓ Loaded existing CSV: {len(existing_csv)} candles")

    # Step 2 — fetch remaining gaps
    new_data = fetch_missing(api)

    # Step 3 — combine
    frames = [f for f in [existing_csv, new_data] if not f.empty]
    if not frames:
        print("✗ No data to insert.")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("timestamp").drop_duplicates("timestamp")
    print(f"\n✓ Combined total: {len(combined)} candles across {combined['timestamp'].dt.date.nunique()} days")

    # Save updated CSV
    combined.to_csv(CSV_PATH, index=False)
    print(f"✓ CSV updated: {CSV_PATH}")

    # Step 4 — insert into DB
    inserted = insert_to_db(combined, engine)

    # Step 5 — verify
    print("\nVerifying DB...")
    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT DATE(time AT TIME ZONE 'Asia/Kolkata') as d, COUNT(*) as n
            FROM nifty_1min
            WHERE time >= '2026-01-23 00:00:00+00'
            AND time <= '2026-03-27 00:00:00+00'
            GROUP BY d ORDER BY d
        """))
        rows = r.fetchall()
    print(f"{'Date':<15} {'Candles':>8}")
    print("-" * 25)
    for row in rows:
        flag = " ⚠" if row[1] < 370 else ""
        print(f"{str(row[0]):<15} {row[1]:>8}{flag}")

    print(f"\n✓ Backfill complete. {inserted} new candles added to DB.")


if __name__ == "__main__":
    main()
