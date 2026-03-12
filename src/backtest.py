import pandas as pd
import numpy as np
import joblib
import os
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DB_HOST  = os.getenv("DB_HOST")
DB_PORT  = os.getenv("DB_PORT")
DB_NAME  = os.getenv("DB_NAME")
DB_USER  = os.getenv("DB_USER")
DB_PASS  = os.getenv("DB_PASS")

LABELED_PATH  = "data/processed/labeled.parquet"
MODELS_DIR    = "models"
OUTPUT_PATH   = "data/processed/backtest_results.json"

WINDOW          = 5
SIDEWAYS_SKIP   = True
MIN_CONFIDENCE  = 0.60   # only trade if confidence > 60%
MAX_TRADES_DAY  = 10     # max 10 trades per day
TRADE_START_HR  = 3      # 9:30 AM IST = 4:00 UTC
TRADE_END_HR    = 10      # 2:30 PM IST = 9:00 UTC


def get_lot_size(date):
    if date < pd.Timestamp("2024-11-20", tz="UTC"):
        return 25
    elif date < pd.Timestamp("2026-01-01", tz="UTC"):
        return 75
    else:
        return 65


def get_engine():
    url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def load_model():
    models = [f for f in os.listdir(MODELS_DIR)
              if f.endswith(".pkl") and "w5" in f]
    models.sort()
    path = os.path.join(MODELS_DIR, models[-1])
    print(f"Loading model: {path}")
    return joblib.load(path)


def load_close_prices():
    engine = get_engine()
    print("Loading close prices from TimescaleDB...")
    df = pd.read_sql(
        "SELECT time, close FROM nifty_1min ORDER BY time ASC",
        engine
    )
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df.set_index("time", inplace=True)
    return df


def load_data():
    print("Loading labeled data...")
    df = pd.read_parquet(LABELED_PATH)
    df.index = pd.to_datetime(df.index, utc=True)
    # Only use last 1 year — truly unseen by final model
    # Last 20% of data as unseen test set
    # Use only holdout period — truly unseen data
    df.index = pd.to_datetime(df.index, utc=True)
    df = df[df.index >= pd.Timestamp("2025-07-01", tz="UTC")]
    print(f"Backtest period: {df.index.min()} to {df.index.max()}")
    print(f"Total rows: {len(df)}")
    return df


def run_backtest(df, model, close_df):
    print("Running backtest...")

    feature_cols = [c for c in df.columns if not c.startswith("label_")]
    X = df[feature_cols]

    preds  = model.predict(X)
    probas = model.predict_proba(X)
    confs  = probas.max(axis=1)

    trades        = []
    indices       = df.index.tolist()
    close_indices = close_df.index.tolist()
    trades_per_day = {}

    for i in range(len(indices) - WINDOW):
        ts     = indices[i]
        signal = int(preds[i])
        conf   = float(confs[i])

        # Skip SIDEWAYS
        if SIDEWAYS_SKIP and signal == 0:
            continue

        # Confidence filter
        if conf < MIN_CONFIDENCE:
            continue

        # Time filter — only trade during market hours
        if not (TRADE_START_HR <= ts.hour < TRADE_END_HR):
            continue

        # Max trades per day filter
        day = ts.date()
        if trades_per_day.get(day, 0) >= MAX_TRADES_DAY:
            continue

        try:
            # Get actual entry and exit close prices
            if ts not in close_df.index:
                continue
            entry_price = float(close_df["close"].loc[ts])
            
            # Get exit timestamp directly
            ts_pos = close_df.index.get_loc(ts)
            exit_pos = ts_pos + WINDOW
            if exit_pos >= len(close_df):
                continue
            exit_ts = close_df.index[exit_pos]
            exit_price = float(close_df["close"].loc[exit_ts])

            lot_size = get_lot_size(ts)

            # Real P&L in points
            if signal == 1:    # UP — long
                pnl_points = exit_price - entry_price
            else:              # DOWN — short
                pnl_points = entry_price - exit_price

            slippage   = 1.0   # 1 point slippage per trade
            brokerage  = 40    # ₹40 per trade flat
            pnl_points = pnl_points - slippage
            pnl_rupees = (pnl_points * lot_size) - brokerage

            trades.append({
                "timestamp":   ts,
                "signal":      signal,
                "confidence":  conf,
                "entry_price": entry_price,
                "exit_price":  exit_price,
                "pnl_points":  pnl_points,
                "pnl_rupees":  pnl_rupees,
                "lot_size":    lot_size,
                "won":         pnl_points > 0
            })

            trades_per_day[day] = trades_per_day.get(day, 0) + 1

        except Exception:
            continue

    if len(trades) == 0:
        print("WARNING: No trades generated. Check filters.")
        print(f"Sample index hour: {indices[100].hour}")
        print(f"Sample confidence: {float(confs[100]):.3f}")
        return pd.DataFrame()

    trades_df = pd.DataFrame(trades)
    trades_df["timestamp"] = pd.to_datetime(trades_df["timestamp"])
    print(f"Total trades: {len(trades_df)}")
    return trades_df


def calculate_metrics(trades_df):
    print("Calculating metrics...")

    total_trades  = len(trades_df)
    winning       = trades_df[trades_df["won"] == True]
    losing        = trades_df[trades_df["won"] == False]

    win_rate      = len(winning) / total_trades * 100
    total_pnl_pts = trades_df["pnl_points"].sum()
    total_pnl_inr = trades_df["pnl_rupees"].sum()
    avg_win       = winning["pnl_points"].mean() if len(winning) > 0 else 0
    avg_loss      = losing["pnl_points"].mean()  if len(losing)  > 0 else 0
    risk_reward   = abs(avg_win / avg_loss)       if avg_loss != 0 else 0

    daily_pnl = trades_df.groupby(
        trades_df["timestamp"].dt.date
    )["pnl_points"].sum()
    sharpe = (daily_pnl.mean() / daily_pnl.std() * np.sqrt(252)
              if daily_pnl.std() > 0 else 0)

    cumulative   = trades_df["pnl_points"].cumsum()
    rolling_max  = cumulative.cummax()
    drawdown     = cumulative - rolling_max
    max_drawdown = drawdown.min()

    trades_df["month"] = trades_df["timestamp"].dt.to_period("M")
    monthly_pnl  = trades_df.groupby("month")["pnl_points"].sum()
    monthly_dict = {str(k): round(v, 2) for k, v in monthly_pnl.items()}

    trades_df["cumulative_pnl"] = trades_df["pnl_points"].cumsum()
    cumulative_series = trades_df[["timestamp", "cumulative_pnl"]].copy()
    cumulative_series["timestamp"] = cumulative_series["timestamp"].astype(str)

    trades_df["drawdown"] = drawdown.values
    drawdown_series = trades_df[["timestamp", "drawdown"]].copy()
    drawdown_series["timestamp"] = drawdown_series["timestamp"].astype(str)

    metrics = {
        "summary": {
            "total_trades":     total_trades,
            "win_rate":         round(win_rate, 2),
            "total_pnl_pts":    round(total_pnl_pts, 2),
            "total_pnl_inr":    round(total_pnl_inr, 2),
            "avg_win_pts":      round(avg_win, 2),
            "avg_loss_pts":     round(avg_loss, 2),
            "risk_reward":      round(risk_reward, 2),
            "sharpe_ratio":     round(float(sharpe), 2),
            "max_drawdown_pts": round(max_drawdown, 2),
        },
        "monthly_pnl":       monthly_dict,
        "cumulative_series": cumulative_series.to_dict(orient="records"),
        "drawdown_series":   drawdown_series.to_dict(orient="records"),
    }
    return metrics


def print_summary(metrics):
    s = metrics["summary"]
    print("\n" + "="*50)
    print("BACKTEST RESULTS")
    print("="*50)
    print(f"  Total Trades:     {s['total_trades']}")
    print(f"  Win Rate:         {s['win_rate']}%")
    print(f"  Total P&L:        {s['total_pnl_pts']} pts (₹{s['total_pnl_inr']:,.0f})")
    print(f"  Avg Win:          {s['avg_win_pts']} pts")
    print(f"  Avg Loss:         {s['avg_loss_pts']} pts")
    print(f"  Risk/Reward:      {s['risk_reward']}")
    print(f"  Sharpe Ratio:     {s['sharpe_ratio']}")
    print(f"  Max Drawdown:     {s['max_drawdown_pts']} pts")
    print("="*50)


def save_results(metrics):
    with open(OUTPUT_PATH, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"Results saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    model    = load_model()
    close_df = load_close_prices()
    df       = load_data()
    trades   = run_backtest(df, model, close_df)
    if trades.empty:
        print("No trades to analyze.")
    else:
        metrics  = calculate_metrics(trades)
        print_summary(metrics)
        save_results(metrics)
    print_summary(metrics)
    save_results(metrics)