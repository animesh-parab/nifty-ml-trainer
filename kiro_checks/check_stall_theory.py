"""
kiro_checks/check_stall_theory.py

Verifies the "silent stall" theory:
- Was features.parquet index stuck at 04:34 UTC (10:04 IST) for a while?
- Did websocket_feed insert DB candles in that window but NOT update parquet?
- Would already_logged() have returned True every iteration, causing infinite stall?

Run from nifty-ml-trainer/ directory.
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
engine = create_engine(DB_URL)

STALL_START_UTC = "2026-03-23 04:34:00+00:00"  # 10:04 IST — last logged trade
STALL_END_UTC   = "2026-03-23 04:50:00+00:00"  # 10:20 IST — check window

# ── 1. What timestamps exist in parquet around the stall window ──
print("=" * 60)
print("1. PARQUET INDEX AROUND STALL WINDOW (10:00–10:20 IST)")
print("=" * 60)

df = pd.read_parquet("data/processed/features.parquet")
df.index = pd.to_datetime(df.index, utc=True)

window = df[(df.index >= "2026-03-23 04:25:00+00:00") &
            (df.index <= "2026-03-23 04:55:00+00:00")]

print(f"Parquet rows in 09:55–10:25 IST window: {len(window)}")
print(f"\n{'UTC Timestamp':<35} {'IST Time':<12} {'In parquet?'}")
print("-" * 60)

# Check every minute in the window
check_range = pd.date_range("2026-03-23 04:25:00", "2026-03-23 04:55:00",
                             freq="1min", tz="UTC")
for ts in check_range:
    ist = ts + pd.Timedelta(hours=5, minutes=30)
    exists = ts in df.index
    flag = "✓" if exists else "✗ MISSING"
    print(f"  {str(ts):<35} {ist.strftime('%H:%M'):<12} {flag}")

# ── 2. What DB candles exist in the same window ──────────────────
print(f"\n{'='*60}")
print("2. DB CANDLES IN SAME WINDOW (did websocket insert them?)")
print("=" * 60)

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT time, open, high, low, close
        FROM nifty_1min
        WHERE time >= '2026-03-23 04:25:00+00'
        AND   time <= '2026-03-23 04:55:00+00'
        ORDER BY time ASC
    """))
    db_candles = result.fetchall()

print(f"DB candles in window: {len(db_candles)}")
db_times = set()
for row in db_candles:
    ist = row[0] + pd.Timedelta(hours=5, minutes=30)
    print(f"  {row[0]}  IST:{ist.strftime('%H:%M')}  C:{row[4]}")
    db_times.add(pd.Timestamp(row[0]).tz_convert("UTC"))

# ── 3. Cross-reference — DB has candle but parquet missing? ──────
print(f"\n{'='*60}")
print("3. CROSS-REFERENCE — DB candle exists but NOT in parquet?")
print("=" * 60)

parquet_times = set(df[(df.index >= "2026-03-23 04:25:00+00:00") &
                       (df.index <= "2026-03-23 04:55:00+00:00")].index)

in_db_not_parquet = db_times - parquet_times
in_parquet_not_db = parquet_times - db_times

if in_db_not_parquet:
    print(f"\n⚠ Candles in DB but NOT in parquet ({len(in_db_not_parquet)}):")
    for ts in sorted(in_db_not_parquet):
        ist = ts + pd.Timedelta(hours=5, minutes=30)
        print(f"  {ts}  IST:{ist.strftime('%H:%M')}")
else:
    print("✓ All DB candles are in parquet for this window")

if in_parquet_not_db:
    print(f"\n⚠ Rows in parquet but NOT in DB ({len(in_parquet_not_db)}):")
    for ts in sorted(in_parquet_not_db):
        ist = ts + pd.Timedelta(hours=5, minutes=30)
        print(f"  {ts}  IST:{ist.strftime('%H:%M')}")

# ── 4. The already_logged stall simulation ───────────────────────
print(f"\n{'='*60}")
print("4. STALL SIMULATION — what would already_logged() return?")
print("=" * 60)

trade_log = pd.read_csv("data/processed/trade_log.csv")
logged_timestamps = set(trade_log["timestamp"].values)
last_logged = "2026-03-23 04:34:00+00:00"

print(f"Last logged timestamp in CSV: {last_logged}")
print(f"\nSimulating trade_logger loop every 30s after 10:04 IST:")
print(f"(assuming features.parquet index[-1] was stuck at {last_logged})\n")

print(f"{'Iteration':<12} {'features.index[-1]':<35} {'already_logged()?':<20} {'Result'}")
print("-" * 80)

# If parquet was stuck at 04:34, every iteration would hit already_logged=True
stuck_ts = "2026-03-23 04:34:00+00:00"
for i in range(1, 8):
    is_logged = stuck_ts in logged_timestamps
    result = "→ 'already logged' printed, no trade" if is_logged else "→ would attempt to log"
    print(f"  Loop {i:<6}  {stuck_ts:<35} {str(is_logged):<20} {result}")

print(f"\n→ If parquet was stuck, the process would loop forever printing 'already logged'")
print(f"  appearing alive but doing nothing — classic silent stall")

# ── 5. Check if parquet has the 04:35 row (first minute after stall) ──
print(f"\n{'='*60}")
print("5. CRITICAL — does parquet have 04:35 UTC (10:05 IST)?")
print("=" * 60)

ts_04_35 = pd.Timestamp("2026-03-23 04:35:00+00:00")
ts_04_34 = pd.Timestamp("2026-03-23 04:34:00+00:00")
ts_04_36 = pd.Timestamp("2026-03-23 04:36:00+00:00")

print(f"04:34 UTC (10:04 IST) in parquet: {ts_04_34 in df.index}")
print(f"04:35 UTC (10:05 IST) in parquet: {ts_04_35 in df.index}")
print(f"04:36 UTC (10:06 IST) in parquet: {ts_04_36 in df.index}")

# Find the actual next timestamp after 04:34 in parquet
after_stall = df[df.index > ts_04_34]
if len(after_stall) > 0:
    next_ts = after_stall.index[0]
    next_ist = next_ts + pd.Timedelta(hours=5, minutes=30)
    gap = (next_ts - ts_04_34).total_seconds() / 60
    print(f"\nNext parquet row after 04:34 UTC: {next_ts}  IST:{next_ist.strftime('%H:%M')}")
    print(f"Gap: {gap:.0f} minutes")
    if gap > 1:
        print(f"\n⚠ PARQUET GAP CONFIRMED: {gap:.0f} min gap after last logged trade!")
        print(f"  During this gap, features.index[-1] was stuck at 04:34 UTC")
        print(f"  trade_logger would have looped ~{gap*2:.0f} times printing 'already logged'")
        print(f"  Then when parquet updated, the new ts was never logged before — so it SHOULD have logged")
        print(f"  But by then the model signal may have changed to SIDEWAYS/low conf")
    else:
        print(f"\n✓ No gap — parquet updated normally at next minute")
        print(f"  Stall theory is WRONG — something else caused the issue")
