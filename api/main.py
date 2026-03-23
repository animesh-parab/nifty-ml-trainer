from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import joblib
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import groq

load_dotenv()

DB_HOST  = os.getenv("DB_HOST")
DB_PORT  = os.getenv("DB_PORT")
DB_NAME  = os.getenv("DB_NAME")
DB_USER  = os.getenv("DB_USER")
DB_PASS  = os.getenv("DB_PASS")
GROQ_KEY = os.getenv("GROQ_API_KEY")

MODELS_DIR    = "models"
FEATURES_PATH = "data/processed/features.parquet"
TRADE_LOG_PATH = "data/processed/trade_log.csv"
TRADE_LOG_PATH = "data/processed/trade_log.csv"

app = FastAPI(title="Nifty ML Trainer API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

models = {}


@app.on_event("startup")
def load_models():
    for window in [5, 15, 30]:
        files = [f for f in os.listdir(MODELS_DIR)
                 if f.endswith(".pkl") and f"w{window}" in f]
        if files:
            files.sort()
            path = os.path.join(MODELS_DIR, files[-1])
            models[window] = joblib.load(path)
    print(f"Loaded {len(models)} models")


def get_engine():
    url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


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

def get_latest_features(n=1):
    df = pd.read_parquet(FEATURES_PATH)
    label_cols = [c for c in df.columns if c.startswith("label_")]
    df.drop(columns=label_cols, inplace=True, errors="ignore")
    # Only use the 44 features the model was trained on
    available = [c for c in MODEL_FEATURES if c in df.columns]
    return df[available].tail(n)


def predict_all(features):
    label_map = {1: "UP", -1: "DOWN", 0: "SIDEWAYS"}
    signals   = {}
    for window, model in models.items():
        pred       = model.predict(features)[0]
        proba      = model.predict_proba(features)[0]
        confidence = round(max(proba) * 100, 1)
        signals[window] = {
            "signal":     label_map[int(pred)],
            "confidence": confidence,
        }
    return signals


@app.get("/health")
def health():
    return {
        "status":        "ok",
        "models_loaded": len(models),
        "windows":       list(models.keys())
    }


@app.get("/signal/latest")
def signal_latest():
    try:
        features = get_latest_features(1)
        signals  = predict_all(features)

        # Get smart exits for active signal
        s5 = signals.get(5, {})
        exits = None
        if s5.get("signal") != "SIDEWAYS" and s5.get("confidence", 0) >= 60:
            try:
                import sys
                sys.path.insert(0, "src")
                from smart_exits import get_exits
                engine = get_engine()
                result = engine.connect().execute(text(
                    "SELECT close FROM nifty_1min ORDER BY time DESC LIMIT 1"
                ))
                price = float(result.fetchone()[0])
                atr   = float(features["atr_14"].iloc[-1])
                exits = get_exits(s5["signal"], price, atr, engine)
            except Exception as ex:
                print(f"Smart exits error: {ex}")

        return {
            "timestamp":  str(features.index[-1]),
            "signals":    signals,
            "vix":        round(float(features["vix_close"].iloc[-1]), 2),
            "vix_regime": int(features["vix_regime"].iloc[-1]),
            "exits":      exits,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/market/context")
def market_context():
    try:
        features = get_latest_features(1)
        try:
            engine = get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT time, close FROM nifty_1min ORDER BY time DESC LIMIT 1"
                ))
                row   = result.fetchone()
                price = round(float(row[1]), 2)
                ts    = str(row[0])
        except Exception:
            price = round(float(features["ema_9"].iloc[-1]), 2)
            ts    = str(features.index[-1])
        return {
            "price":      price,
            "timestamp":  ts,
            "vix":        round(float(features["vix_close"].iloc[-1]), 2),
            "vix_regime": int(features["vix_regime"].iloc[-1]),
            "rsi_14":     round(float(features["rsi_14"].iloc[-1]), 2),
            "atr_14":     round(float(features["atr_14"].iloc[-1]), 2),
            "ema_9":      round(float(features["ema_9"].iloc[-1]), 2),
            "ema_21":     round(float(features["ema_21"].iloc[-1]), 2),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signal/history")
def signal_history():
    try:
        features  = get_latest_features(50)
        label_map = {1: "UP", -1: "DOWN", 0: "SIDEWAYS"}
        history   = []
        for ts, row in features.iterrows():
            row_df  = pd.DataFrame([row])
            signals = {}
            for window, model in models.items():
                pred  = model.predict(row_df)[0]
                proba = model.predict_proba(row_df)[0]
                signals[window] = {
                    "signal":     label_map[int(pred)],
                    "confidence": round(max(proba) * 100, 1)
                }
            history.append({
                "timestamp": str(ts),
                "signals":   signals,
                "vix":       round(float(row["vix_close"]), 2),
                "rsi_14":    round(float(row["rsi_14"]), 2),
            })
        return {"history": history}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ai/analysis")
def ai_analysis():
    try:
        features = get_latest_features(1)
        signals  = predict_all(features)
        vix      = float(features["vix_close"].iloc[-1])
        try:
            engine = get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT close FROM nifty_1min ORDER BY time DESC LIMIT 1"
                ))
                price = float(result.fetchone()[0])
        except Exception:
            price = float(features["ema_9"].iloc[-1])

        client = groq.Groq(api_key=GROQ_KEY)
        signal_summary = "\n".join([
            f"  {w}-min: {s['signal']} (confidence: {s['confidence']}%)"
            for w, s in signals.items()
        ])
        prompt = f"""You are an expert Nifty 50 intraday trader.

Current market data:
- Nifty 50 Price: {price:.2f}
- India VIX: {vix:.2f}
- ML Model Signals:
{signal_summary}

Provide:
1. Short trend outlook (next 1 hour)
2. Key levels to watch
3. Risk assessment (Low/Medium/High)
4. One actionable insight

Be concise - max 4 sentences."""

        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200
        )
        return {
            "analysis": response.choices[0].message.content,
            "price":    round(price, 2),
            "vix":      round(vix, 2),
            "signals":  signals,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/backtest/results")
def backtest_results():
    try:
        with open(BACKTEST_PATH) as f:
            results = json.load(f)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/trades/live")
def live_trades():
    try:
        import csv
        from datetime import date as dt_date

        trades = []
        with open(TRADE_LOG_PATH, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                trades.append(row)

        if not trades:
            return {"trades": [], "summary": {}}

        today = str(dt_date.today())

        # Get current price
        try:
            engine = get_engine()
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT close, high, low FROM nifty_1min ORDER BY time DESC LIMIT 1"
                ))
                row = result.fetchone()
                current_price = float(row[0])
                current_high  = float(row[1])
                current_low   = float(row[2])
        except:
            current_price = current_high = current_low = 0

        # Check each trade outcome
        for trade in trades:
            ts      = trade["timestamp"]
            signal  = trade["signal"]
            entry   = float(trade["entry_price"])
            sl      = float(trade["stoploss"])
            outcome = trade["outcome"]

            t1 = float(trade.get("t1") or trade.get("target") or 0)
            t2 = float(trade.get("t2") or 0)
            t3 = float(trade.get("t3") or 0)

            if outcome == "PENDING" and current_price > 0:
                try:
                    engine2 = get_engine()
                    with engine2.connect() as conn2:
                        result2 = conn2.execute(text("""
                            SELECT high, low FROM nifty_1min
                            WHERE time > :ts
                            ORDER BY time ASC
                            LIMIT 100
                        """), {"ts": ts})
                        candles = result2.fetchall()

                    live_status = "ACTIVE"
                    is_long = signal == "UP"

                    for candle in candles:
                        h = float(candle[0])
                        l = float(candle[1])
                        if is_long:
                            if l <= sl:
                                live_status = "SL_HIT"
                                break
                            elif t3 and h >= t3:
                                live_status = "T3_HIT"
                                break
                            elif t2 and h >= t2:
                                live_status = "T2_HIT"
                                break
                            elif t1 and h >= t1:
                                live_status = "T1_HIT"
                        else:
                            if h >= sl:
                                live_status = "SL_HIT"
                                break
                            elif t3 and l <= t3:
                                live_status = "T3_HIT"
                                break
                            elif t2 and l <= t2:
                                live_status = "T2_HIT"
                                break
                            elif t1 and l <= t1:
                                live_status = "T1_HIT"

                    trade["live_status"] = live_status
                except:
                    trade["live_status"] = "ACTIVE"
            else:
                trade["live_status"] = outcome

            trade["current_price"] = current_price
            trade["is_today"] = ts.startswith(today)

        # Auto-update CSV outcomes from live_status
        try:
            updated = False
            for trade in trades:
                ls = trade.get("live_status", "")
                if trade["outcome"] == "PENDING" and ls in ["SL_HIT", "T1_HIT", "T2_HIT", "T3_HIT"]:
                    trade["outcome"] = ls
                    updated = True
            if updated:
                import csv as csv_mod
                fieldnames = [k for k in trades[0].keys()
                              if k not in ["live_status", "current_price", "is_today"]]
                with open(TRADE_LOG_PATH, "w", newline="") as f:
                    writer = csv_mod.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for t in trades:
                        row = {k: t[k] for k in fieldnames}
                        writer.writerow(row)
        except Exception as ex:
            print(f"CSV update error: {ex}")

        # Summary
        today_trades  = [t for t in trades if t.get("is_today")]
        pending       = [t for t in trades if t["outcome"] == "PENDING"]
        wins          = [t for t in trades if "WIN" in t["outcome"]]
        losses        = [t for t in trades if "LOSS" in t["outcome"]]

        return {
            "trades":        trades,
            "current_price": current_price,
            "summary": {
                "total":        len(trades),
                "today":        len(today_trades),
                "pending":      len(pending),
                "wins":         len(wins),
                "losses":       len(losses),
                "win_rate":     round(len(wins) / max(len(wins) + len(losses), 1) * 100, 1),
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))