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
BACKTEST_PATH = "data/processed/backtest_results.json"

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


def get_latest_features(n=1):
    df = pd.read_parquet(FEATURES_PATH)
    label_cols = [c for c in df.columns if c.startswith("label_")]
    df.drop(columns=label_cols, inplace=True, errors="ignore")
    return df.tail(n)


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
        return {
            "timestamp": str(features.index[-1]),
            "signals":   signals,
            "vix":       round(float(features["vix_close"].iloc[-1]), 2),
            "vix_regime": int(features["vix_regime"].iloc[-1]),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/market/context")
def market_context():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT time, close FROM nifty_1min ORDER BY time DESC LIMIT 1"
            ))
            row = result.fetchone()
        features = get_latest_features(1)
        return {
            "price":      round(float(row[1]), 2),
            "timestamp":  str(row[0]),
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
        features   = get_latest_features(50)
        label_map  = {1: "UP", -1: "DOWN", 0: "SIDEWAYS"}
        history    = []
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

        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT close FROM nifty_1min ORDER BY time DESC LIMIT 1"
            ))
            price = float(result.fetchone()[0])

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
            "analysis":  response.choices[0].message.content,
            "price":     round(price, 2),
            "vix":       round(vix, 2),
            "signals":   signals,
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