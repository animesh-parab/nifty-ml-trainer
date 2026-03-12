import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

FEATURES_PATH = 'data/processed/features.parquet'
ATR_MULTIPLIER = 0.5


def get_engine():
    url = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    return create_engine(url)


def load_close_prices():
    engine = get_engine()
    print('Loading close prices from TimescaleDB...')
    df = pd.read_sql('SELECT time, close FROM nifty_1min ORDER BY time ASC', engine)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    df.set_index('time', inplace=True)
    return df


def create_labels(close_df, atr_series, window):
    entry_price  = close_df['close']
    exit_price   = close_df['close'].shift(-window)
    price_change = exit_price - entry_price
    threshold    = ATR_MULTIPLIER * atr_series

    labels = pd.Series(0, index=close_df.index)
    labels[price_change >  threshold] =  1
    labels[price_change < -threshold] = -1

    return labels


def build_labeled_dataset():
    print('Loading features...')
    features = pd.read_parquet(FEATURES_PATH)

    print('Loading close prices...')
    close_df = load_close_prices()

    # Align indexes
    common_idx = features.index.intersection(close_df.index)
    features   = features.loc[common_idx]
    close_df   = close_df.loc[common_idx]
    atr_series = features['atr_14']

    print(f'Aligned rows: {len(features)}')

    for window in [5, 15, 30]:
        print(f'Labeling window={window} mins...')
        labels = create_labels(close_df, atr_series, window)
        features[f'label_{window}'] = labels

        counts = labels.value_counts()
        total  = len(labels)
        print(f'  Window {window}min distribution:')
        print(f'    UP       (1): {counts.get(1,0):>7} ({counts.get(1,0)/total*100:.1f}%)')
        print(f'    SIDEWAYS (0): {counts.get(0,0):>7} ({counts.get(0,0)/total*100:.1f}%)')
        print(f'    DOWN    (-1): {counts.get(-1,0):>7} ({counts.get(-1,0)/total*100:.1f}%)')

    features.dropna(inplace=True)

    print(f'Final labeled dataset shape: {features.shape}')
    return features


def save_labeled(df):
    path = 'data/processed/labeled.parquet'
    df.to_parquet(path)
    print(f'Saved to {path}')


if __name__ == '__main__':
    df = build_labeled_dataset()
    save_labeled(df)
    print(df[['label_5', 'label_15', 'label_30']].head(10))
