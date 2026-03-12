import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.metrics import balanced_accuracy_score
import joblib
import os
from datetime import datetime

LABELED_PATH = "data/processed/labeled.parquet"
MODELS_DIR   = "models"
WINDOWS      = [5, 15, 30]


HOLDOUT_DATE = "2025-07-01"  # last 6 months kept for backtest

def load_data(window):
    print(f"Loading labeled data for window={window}min...")
    df = pd.read_parquet(LABELED_PATH)
    df.index = pd.to_datetime(df.index, utc=True)
    
    # Keep only data before holdout date for training
    df = df[df.index < pd.Timestamp(HOLDOUT_DATE, tz="UTC")]
    
    other_labels = [f"label_{w}" for w in WINDOWS if w != window]
    df.drop(columns=other_labels, inplace=True)
    df.rename(columns={f"label_{window}": "label"}, inplace=True)
    df = df[df["label"].notna()]
    print(f"Total rows: {len(df)} | Features: {len(df.columns) - 1}")
    print(f"Training period: {df.index.min()} to {df.index.max()}")
    return df


def walk_forward_validate(df, window):
    print(f"Walk-forward validation for window={window}min...")
    df.index = pd.to_datetime(df.index)
    min_date  = df.index.min()
    max_date  = df.index.max()

    train_end  = min_date + pd.DateOffset(years=2)
    test_size  = pd.DateOffset(months=3)

    fold   = 0
    scores = []

    while train_end < max_date:
        test_end = min(train_end + test_size, max_date)
        train = df.loc[min_date:train_end]
        test  = df.loc[train_end:test_end]

        if len(test) < 100:
            break

        X_train = train.drop(columns=["label"])
        y_train = train["label"]
        X_test  = test.drop(columns=["label"])
        y_test  = test["label"]

        model = lgb.LGBMClassifier(
            n_estimators=500,
            learning_rate=0.05,
            num_leaves=63,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
            verbose=-1
        )
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        score  = balanced_accuracy_score(y_test, y_pred)
        scores.append(score)
        fold  += 1

        print(f"  Fold {fold:>2}: train end {train_end.date()} | "
              f"test {train_end.date()} to {test_end.date()} | "
              f"balanced_acc={score:.4f}")

        train_end = train_end + test_size

    avg_score = np.mean(scores)
    print(f"  Average balanced accuracy: {avg_score:.4f}")
    return avg_score, scores


def train_final_model(df, window):
    print(f"Training final model on full data for window={window}min...")
    X = df.drop(columns=["label"])
    y = df["label"]

    model = lgb.LGBMClassifier(
        n_estimators=500,
        learning_rate=0.05,
        num_leaves=63,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
        verbose=-1
    )
    model.fit(X, y)

    importance = pd.DataFrame({
        "feature":    X.columns,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)

    print("Top 10 features:")
    print(importance.head(10).to_string(index=False))
    return model, importance


def save_model(model, window, score):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename  = f"lgbm_w{window}_acc{score:.3f}_{timestamp}.pkl"
    path      = os.path.join(MODELS_DIR, filename)
    joblib.dump(model, path)
    print(f"Model saved to {path}")
    return path


def run_training():
    results = {}

    for window in WINDOWS:
        print()
        print("="*50)
        print(f"WINDOW: {window} MIN")
        print("="*50)
        df                = load_data(window)
        avg_score, scores = walk_forward_validate(df, window)
        model, importance = train_final_model(df, window)
        path              = save_model(model, window, avg_score)
        results[window]   = {
            "avg_balanced_accuracy": avg_score,
            "all_fold_scores":       scores,
            "model_path":            path
        }

    print()
    print("="*50)
    print("TRAINING SUMMARY")
    print("="*50)
    for window, r in results.items():
        print(f"Window {window:>2}min | "
              f"Avg Balanced Acc: {r['avg_balanced_accuracy']:.4f} | "
              f"Model: {r['model_path']}")

    best_window = max(results, key=lambda w: results[w]["avg_balanced_accuracy"])
    print(f"Best window: {best_window}min "
          f"(balanced_acc={results[best_window]['avg_balanced_accuracy']:.4f})")
    print("="*50)


if __name__ == "__main__":
    run_training()