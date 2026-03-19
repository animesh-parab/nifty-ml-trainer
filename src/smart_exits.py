"""
smart_exits.py
S&R-based stoploss and target calculator
Replaces ATR-only formula in trade_logger.py and api/main.py

Usage:
    from src.smart_exits import get_exits
    exits = get_exits(signal, entry, atr, db_engine)
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text


# ── Config ───────────────────────────────────────────────────
CANDLES_LOOKBACK  = 1000
SWING_WINDOW      = 5
CLUSTER_TOLERANCE = 15
MIN_TOUCHES       = 1
ATR_SL_MULT       = 1.5
ATR_T3_MULT       = 2.5
SL_BUFFER         = 2
ROUND_NUMBER_STEP = 50
MAX_TARGET_PTS    = 150


# ── Round number finder ──────────────────────────────────────
def get_round_numbers(price, direction, count=5):
    base = round(price / ROUND_NUMBER_STEP) * ROUND_NUMBER_STEP
    levels = []
    if direction == "above":
        start = base if base > price else base + ROUND_NUMBER_STEP
        for i in range(count):
            levels.append(start + i * ROUND_NUMBER_STEP)
    else:
        start = base if base < price else base - ROUND_NUMBER_STEP
        for i in range(count):
            levels.append(start - i * ROUND_NUMBER_STEP)
    return sorted(levels, key=lambda x: abs(x - price))


# ── Fetch candles ────────────────────────────────────────────
def fetch_candles(engine, n=1000):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT time, open, high, low, close
                FROM nifty_1min
                WHERE time >= '2026-03-13 00:00:00+00'
                AND EXTRACT(DOW FROM time AT TIME ZONE 'Asia/Kolkata') BETWEEN 1 AND 5
                ORDER BY time DESC
                LIMIT {n}
            """))
            rows = result.fetchall()
        df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close"])
        df["time"] = pd.to_datetime(df["time"], utc=True)
        df = df.sort_values("time").reset_index(drop=True)
        return df
    except Exception as e:
        print(f"  ✗ Candle fetch error: {e}")
        return pd.DataFrame()


# ── Fetch previous day high/low ──────────────────────────────
def fetch_prev_day_levels(engine):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    DATE(time AT TIME ZONE 'Asia/Kolkata') as trade_date,
                    MAX(high) as day_high,
                    MIN(low)  as day_low
                FROM nifty_1min
                WHERE DATE(time AT TIME ZONE 'Asia/Kolkata') < (
    SELECT MAX(DATE(time AT TIME ZONE 'Asia/Kolkata')) 
    FROM nifty_1min 
    WHERE time >= '2026-03-13 00:00:00+00'
)
AND DATE(time AT TIME ZONE 'Asia/Kolkata') >= '2026-03-13'
AND EXTRACT(HOUR FROM time AT TIME ZONE 'UTC') >= 3
AND EXTRACT(HOUR FROM time AT TIME ZONE 'UTC') < 11
GROUP BY trade_date
                LIMIT 1
            """))
            row = result.fetchone()
            if row:
                return {"pdh": float(row[1]), "pdl": float(row[2]), "date": str(row[0])}
    except Exception as e:
        print(f"  ✗ PDH/PDL fetch error: {e}")
    return None


# ── Swing detection ──────────────────────────────────────────
def find_swing_highs(df, window=5):
    swings = []
    for i in range(window, len(df) - window):
        high  = df["high"].iloc[i]
        left  = df["high"].iloc[i-window:i]
        right = df["high"].iloc[i+1:i+window+1]
        if high > left.max() and high > right.max():
            swings.append({"price": high, "index": i, "time": df["time"].iloc[i]})
    return swings


def find_swing_lows(df, window=5):
    swings = []
    for i in range(window, len(df) - window):
        low   = df["low"].iloc[i]
        left  = df["low"].iloc[i-window:i]
        right = df["low"].iloc[i+1:i+window+1]
        if low < left.min() and low < right.min():
            swings.append({"price": low, "index": i, "time": df["time"].iloc[i]})
    return swings


# ── Cluster levels ───────────────────────────────────────────
def cluster_levels(points, tolerance=15):
    if not points:
        return []
    prices = sorted([p["price"] for p in points])
    clusters = []
    current = [prices[0]]
    for price in prices[1:]:
        if price - current[-1] <= tolerance:
            current.append(price)
        else:
            clusters.append({
                "price":    round(sum(current) / len(current), 2),
                "touches":  len(current),
                "strength": len(current),
            })
            current = [price]
    clusters.append({
        "price":    round(sum(current) / len(current), 2),
        "touches":  len(current),
        "strength": len(current),
    })
    return clusters


# ── Score levels ─────────────────────────────────────────────
def score_levels(levels, round_numbers, pdh=None, pdl=None):
    for level in levels:
        score = level["touches"]
        for rn in round_numbers:
            if abs(level["price"] - rn) <= CLUSTER_TOLERANCE:
                score += 2
                level["is_round"] = True
                break
        if pdh and abs(level["price"] - pdh) <= CLUSTER_TOLERANCE:
            score += 2
            level["is_pdh"] = True
        if pdl and abs(level["price"] - pdl) <= CLUSTER_TOLERANCE:
            score += 2
            level["is_pdl"] = True
        level["strength"] = score
    return sorted(levels, key=lambda x: x["strength"], reverse=True)


# ── Find all S&R levels ──────────────────────────────────────
def find_sr_levels(df, entry, pdh=None, pdl=None):
    swing_highs = find_swing_highs(df, SWING_WINDOW)
    swing_lows  = find_swing_lows(df, SWING_WINDOW)

    resistances = cluster_levels(
        [s for s in swing_highs if s["price"] > entry], CLUSTER_TOLERANCE
    )
    supports = cluster_levels(
        [s for s in swing_lows if s["price"] < entry], CLUSTER_TOLERANCE
    )

    if pdh and pdh > entry:
        resistances.append({"price": pdh, "touches": 2, "strength": 4, "is_pdh": True})
    if pdl and pdl < entry:
        supports.append({"price": pdl, "touches": 2, "strength": 4, "is_pdl": True})

    all_round = get_round_numbers(entry, "above", 10) + get_round_numbers(entry, "below", 10)
    resistances = score_levels(resistances, all_round, pdh, pdl)
    supports    = score_levels(supports,    all_round, pdh, pdl)

    resistances = [r for r in resistances if r["touches"] >= MIN_TOUCHES]
    supports    = [s for s in supports    if s["touches"] >= MIN_TOUCHES]

    resistances.sort(key=lambda x: x["price"])
    supports.sort(key=lambda x: x["price"], reverse=True)

    return resistances, supports


# ── Level reason label ───────────────────────────────────────
def level_reason(level):
    parts = [f"{level['touches']}x touch"]
    if level.get("is_round"):
        parts.append("round#")
    if level.get("is_pdh"):
        parts.append("PDH")
    if level.get("is_pdl"):
        parts.append("PDL")
    return " + ".join(parts)


# ── Safe target picker ───────────────────────────────────────
def pick_target(sr_list, idx, entry, atr, atr_mult, is_long, rn_idx=0):
    if len(sr_list) > idx:
        price = round(sr_list[idx]["price"], 2)
        if abs(price - entry) <= MAX_TARGET_PTS:
            return price, level_reason(sr_list[idx])
    rn = get_round_numbers(entry, "above" if is_long else "below", rn_idx + 3)
    if len(rn) > rn_idx:
        t = rn[rn_idx]
        if abs(t - entry) <= MAX_TARGET_PTS:
            return t, f"Round# {t}"
    t = round(entry + atr * atr_mult, 2) if is_long else round(entry - atr * atr_mult, 2)
    return t, f"ATR×{atr_mult}"


# ── Main ─────────────────────────────────────────────────────
def get_exits(signal, entry, atr, engine):
    is_long = signal == "UP"

    df       = fetch_candles(engine, CANDLES_LOOKBACK)
    prev_day = fetch_prev_day_levels(engine)
    pdh = prev_day["pdh"] if prev_day else None
    pdl = prev_day["pdl"] if prev_day else None

    resistances, supports = find_sr_levels(df, entry, pdh, pdl)

    # ── Stoploss — widest of ATR, round#, nearest S&R ────────
    atr_sl = round(entry + atr * ATR_SL_MULT, 2) if not is_long else round(entry - atr * ATR_SL_MULT, 2)
    sl_candidates = {abs(entry - atr_sl): f"ATR×{ATR_SL_MULT}"}

    rn_sl = get_round_numbers(entry, "above" if not is_long else "below", 3)
    if rn_sl:
        sl_candidates[abs(entry - rn_sl[0])] = f"Round# {rn_sl[0]}"

    sr_for_sl = resistances if not is_long else supports
    if sr_for_sl:
        sl_candidates[abs(entry - sr_for_sl[0]["price"])] = level_reason(sr_for_sl[0])

    widest    = max(sl_candidates.keys())
    sl_reason = sl_candidates[widest]
    stoploss  = round(entry + widest + SL_BUFFER, 2) if not is_long else round(entry - widest - SL_BUFFER, 2)

    # ── Targets ───────────────────────────────────────────────
    sr_for_targets = supports if not is_long else resistances

    t1, t1_reason = pick_target(sr_for_targets, 0, entry, atr, 1.5,         is_long, rn_idx=0)
    t2, t2_reason = pick_target(sr_for_targets, 1, entry, atr, 2.0,         is_long, rn_idx=1)
    t3, t3_reason = pick_target(sr_for_targets, 2, entry, atr, ATR_T3_MULT, is_long, rn_idx=2)

    # Ensure T1 < T2 < T3 distance from entry
    t1_dist = abs(t1 - entry)
    t2_dist = abs(t2 - entry)
    t3_dist = abs(t3 - entry)

    if t2_dist <= t1_dist:
        t2        = round(entry - (t1_dist + 25), 2) if not is_long else round(entry + (t1_dist + 25), 2)
        t2_reason = "step +25"
    if t3_dist <= abs(t2 - entry):
        t3        = round(entry - (abs(t2 - entry) + 25), 2) if not is_long else round(entry + (abs(t2 - entry) + 25), 2)
        t3_reason = "step +25"

    # ── R/R ───────────────────────────────────────────────────
    sl_pts = abs(stoploss - entry)
    rr_t1  = round(abs(t1 - entry) / sl_pts, 2) if sl_pts > 0 else 0
    rr_t2  = round(abs(t2 - entry) / sl_pts, 2) if sl_pts > 0 else 0
    rr_t3  = round(abs(t3 - entry) / sl_pts, 2) if sl_pts > 0 else 0

    return {
        "stoploss":   stoploss,
        "sl_pts":     round(sl_pts, 2),
        "sl_reason":  sl_reason,
        "t1":         t1,
        "t1_pts":     round(abs(t1 - entry), 2),
        "t1_reason":  t1_reason,
        "t2":         t2,
        "t2_pts":     round(abs(t2 - entry), 2),
        "t2_reason":  t2_reason,
        "t3":         t3,
        "t3_pts":     round(abs(t3 - entry), 2),
        "t3_reason":  t3_reason,
        "rr_t1":      rr_t1,
        "rr_t2":      rr_t2,
        "rr_t3":      rr_t3,
        "sr_levels": {
            "resistances": resistances[:5],
            "supports":    supports[:5],
            "pdh":         pdh,
            "pdl":         pdl,
        }
    }


# ── Standalone test ──────────────────────────────────────────
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()

    DB_URL = "postgresql://{}:{}@{}:{}/{}".format(
        os.getenv("DB_USER"), os.getenv("DB_PASS"),
        os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
    )
    engine = create_engine(DB_URL)

    test_cases = [
        {"signal": "DOWN", "entry": 23250.0, "atr": 15.0},
        {"signal": "UP",   "entry": 23200.0, "atr": 12.0},
    ]

    for tc in test_cases:
        print(f"\n{'='*55}")
        print(f"Signal: {tc['signal']} | Entry: {tc['entry']} | ATR: {tc['atr']}")
        print(f"{'='*55}")
        exits = get_exits(tc["signal"], tc["entry"], tc["atr"], engine)
        print(f"  STOPLOSS : ₹{exits['stoploss']} (-{exits['sl_pts']} pts) [{exits['sl_reason']}]")
        print(f"  T1       : ₹{exits['t1']} (+{exits['t1_pts']} pts) R/R:{exits['rr_t1']} [{exits['t1_reason']}]")
        print(f"  T2       : ₹{exits['t2']} (+{exits['t2_pts']} pts) R/R:{exits['rr_t2']} [{exits['t2_reason']}]")
        print(f"  T3       : ₹{exits['t3']} (+{exits['t3_pts']} pts) R/R:{exits['rr_t3']} [{exits['t3_reason']}]")
        print(f"\n  Resistances above entry:")
        for r in exits["sr_levels"]["resistances"][:3]:
            print(f"    ₹{r['price']} — {r['touches']} touches, strength:{r['strength']}")
        print(f"  Supports below entry:")
        for s in exits["sr_levels"]["supports"][:3]:
            print(f"    ₹{s['price']} — {s['touches']} touches, strength:{s['strength']}")
        if exits["sr_levels"]["pdh"]:
            print(f"  PDH: ₹{exits['sr_levels']['pdh']} | PDL: ₹{exits['sr_levels']['pdl']}")