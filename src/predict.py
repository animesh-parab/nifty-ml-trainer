import pandas as pd
import numpy as np
import joblib
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import groq
from google import genai

load_dotenv()

DB_HOST    = os.getenv("DB_HOST")
DB_PORT    = os.getenv("DB_PORT")
DB_NAME    = os.getenv("DB_NAME")
DB_USER    = os.getenv("DB_USER")
DB_PASS    = os.getenv("DB_PASS")
GROQ_KEY   = os.getenv("GROQ_API_KEY")
GEMINI_KEY = os.getenv("GEMINI_API_KEY")

MODELS_DIR    = "models"
FEATURES_PATH = "data/processed/features.parquet"


def get_engine():
    url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def load_all_models():
    result = {}
    for window in [5, 15, 30]:
        models = [f for f in os.listdir(MODELS_DIR)
                  if f.endswith(".pkl") and f"w{window}" in f]
        if models:
            models.sort()
            path = os.path.join(MODELS_DIR, models[-1])
            result[window] = joblib.load(path)
            print(f"Loaded w{window} model: {path}")
    return result


def get_latest_features(n=1):
    print("Loading latest features...")
    df = pd.read_parquet(FEATURES_PATH)
    label_cols = [c for c in df.columns if c.startswith("label_")]
    df.drop(columns=label_cols, inplace=True, errors="ignore")
    return df.tail(n)


def predict_signal(models, features):
    signals   = {}
    label_map = {1: "UP", -1: "DOWN", 0: "SIDEWAYS"}
    for window, model in models.items():
        pred       = model.predict(features)[0]
        proba      = model.predict_proba(features)[0]
        confidence = round(max(proba) * 100, 1)
        signals[window] = {
            "signal":     label_map[int(pred)],
            "confidence": confidence,
            "raw":        int(pred)
        }
    return signals


def get_ai_analysis(signals, vix_value, price):
    client = groq.Groq(api_key=GROQ_KEY)
    signal_summary = "\n".join([
        f"  {w}-min: {s['signal']} (confidence: {s['confidence']}%)"
        for w, s in signals.items()
    ])
    prompt = f"""You are an expert Nifty 50 intraday trader.

Current market data:
- Nifty 50 Price: {price:.2f}
- India VIX: {vix_value:.2f}
- ML Model Signals:
{signal_summary}

Based on these signals, provide:
1. Short trend outlook (next 1 hour)
2. Key levels to watch
3. Risk assessment (Low/Medium/High)
4. One actionable insight

Be concise - max 4 sentences total."""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=200
    )
    return response.choices[0].message.content


def run_prediction():
    models   = load_all_models()
    features = get_latest_features(n=1)

    print(f"\nPredicting on: {features.index[-1]}")

    vix_value = features["vix_close"].iloc[-1]

    engine   = get_engine()
    price_df = pd.read_sql(
        "SELECT close FROM nifty_1min ORDER BY time DESC LIMIT 1",
        engine
    )
    price = price_df["close"].iloc[0]

    signals = predict_signal(models, features)

    print("\n" + "="*40)
    print("SIGNAL SUMMARY")
    print("="*40)
    for window, s in signals.items():
        print(f"  {window}-min: {s['signal']:<10} confidence: {s['confidence']}%")

    print(f"\n  Price: {price:.2f}")
    print(f"  VIX:   {vix_value:.2f}")

    print("\n" + "="*40)
    print("AI ANALYSIS (Groq DeepSeek R1)")
    print("="*40)
    analysis = get_ai_analysis(signals, vix_value, price)
    print(analysis)
    print("="*40)


if __name__ == "__main__":
    run_prediction()