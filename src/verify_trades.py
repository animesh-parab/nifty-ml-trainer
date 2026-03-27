"""
verify_trades.py
Auto-checks all PENDING trades against TimescaleDB price data
Updates trade_log.csv with WIN/LOSS/EXPIRED outcomes
Run once after market close: python src/verify_trades.py
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)

TRADE_LOG_PATH = "data/processed/trade_log.csv"
MARKET_END_IST = "15:30"  # 10:00 UTC


def get_engine():
    return create_engine(DB_URL)


def check_outcome(conn, timestamp, signal, target, stoploss):
    """Check if trade hit target or stoploss within 30 candles after entry"""
    result = conn.execute(text("""
        SELECT time, high, low, close
        FROM nifty_1min
        WHERE time > :ts
        ORDER BY time ASC
        LIMIT 30
    """), {"ts": timestamp})
    rows = result.fetchall()

    if not rows:
        return "EXPIRED"

    is_long = signal == "UP"

    for row in rows:
        h = float(row[1])
        l = float(row[2])

        if is_long:
            if h >= target:
                return "WIN"
            if l <= stoploss:
                return "LOSS"
        else:
            if l <= target:
                return "WIN"
            if h >= stoploss:
                return "LOSS"

    # Check if last candle is near market close
    last_time_utc = rows[-1][0]
    last_close = float(rows[-1][3])
    return "EXPIRED"


def deduplicate_trades(df):
    """Keep only first signal per 15-min window per direction"""
    df = df.sort_values('timestamp')
    df['window'] = df['timestamp'].dt.floor('15min')
    df['date'] = df['timestamp'].dt.date

    # For already resolved trades keep all
    resolved = df[df['outcome'] != 'PENDING']

    # For pending, deduplicate
    pending = df[df['outcome'] == 'PENDING']
    pending_deduped = pending.drop_duplicates(subset=['date', 'window', 'signal'], keep='first')

    combined = pd.concat([resolved, pending_deduped]).sort_values('timestamp')
    removed = len(df) - len(combined)
    print(f"  Deduplicated: removed {removed} duplicate signals")
    return combined.drop(columns=['window', 'date'])


def main():
    print("=" * 50)
    print("Trade Outcome Verifier")
    print("=" * 50)

    df = pd.read_csv(TRADE_LOG_PATH)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

    pending = df[df['outcome'] == 'PENDING']
    print(f"Total PENDING trades: {len(pending)}")

    # Deduplicate first
    print("\nDeduplicating clustered signals...")
    df = deduplicate_trades(df)
    pending = df[df['outcome'] == 'PENDING']
    print(f"After dedup: {len(pending)} unique pending trades")

    print("\nChecking outcomes against DB...")
    engine = get_engine()

    results = {'WIN': 0, 'LOSS': 0, 'EXPIRED': 0}

    with engine.connect() as conn:
        for idx, row in df.iterrows():
            if row['outcome'] != 'PENDING':
                continue

            outcome = check_outcome(
                conn,
                row['timestamp'],
                row['signal'],
                float(row['t1']),
                float(row['stoploss'])
            )

            df.at[idx, 'outcome'] = outcome
            results[outcome] += 1

            ist = (row['timestamp'] + pd.Timedelta(hours=5, minutes=30)).strftime('%m-%d %H:%M')
            print(f"  {ist} {row['signal']:<4} entry:{row['entry_price']:.1f} → {outcome}")

    # Save cleaned log
    df['timestamp'] = df['timestamp'].astype(str)
    df.to_csv(TRADE_LOG_PATH, index=False)

    print("\n" + "=" * 50)
    print("RESULTS SUMMARY")
    print("=" * 50)
    total = sum(results.values())
    print(f"  WIN:     {results['WIN']} ({results['WIN']/total*100:.1f}%)")
    print(f"  LOSS:    {results['LOSS']} ({results['LOSS']/total*100:.1f}%)")
    print(f"  EXPIRED: {results['EXPIRED']} ({results['EXPIRED']/total*100:.1f}%)")
    print(f"  TOTAL:   {total}")
    print(f"\n✓ trade_log.csv updated")


if __name__ == "__main__":
    main()
