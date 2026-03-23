from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
load_dotenv()

url = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)
engine = create_engine(url)

trades = [
    ("10:48 UP",   "2026-03-16 05:18:00", 23052.0, 23073.39, 23040.62),
    ("10:57 DOWN", "2026-03-16 05:27:00", 22993.3, 22968.39, 23006.53),
    ("10:59 DOWN", "2026-03-16 05:29:00", 23000.6, 22976.05, 23013.64),
    ("11:12 UP",   "2026-03-16 05:42:00", 23062.3, 23086.53, 23049.35),
    ("11:28 UP",   "2026-03-16 05:58:00", 23170.2, 23197.00, 23155.98),
]

with engine.connect() as conn:
    for label, ts, entry, target, sl in trades:
        r = conn.execute(text("""
            SELECT time, high, low, close 
            FROM nifty_1min 
            WHERE time > :ts 
            ORDER BY time ASC 
            LIMIT 30
        """), {"ts": ts})
        rows = r.fetchall()

        hit_target = False
        hit_sl     = False
        result     = "PENDING"

        for row in rows:
            h = float(row[1])
            l = float(row[2])
            if target > entry:  # UP trade
                if h >= target:
                    hit_target = True
                    break
                if l <= sl:
                    hit_sl = True
                    break
            else:  # DOWN trade
                if l <= target:
                    hit_target = True
                    break
                if h >= sl:
                    hit_sl = True
                    break

        if hit_target:
            result = "WIN ✅"
        elif hit_sl:
            result = "LOSS ❌"
        else:
            result = "PENDING ⏳"

        print(f"{label} | Entry:{entry} Target:{target} SL:{sl} → {result}")