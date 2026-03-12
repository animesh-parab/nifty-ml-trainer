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

RAW_VIX_PATH = 'data/raw/india_vix.csv'


def get_engine():
    url = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    return create_engine(url)


def load_from_db(table='nifty_1min'):
    engine = get_engine()
    print(f'Loading {table} from TimescaleDB...')
    df = pd.read_sql(f'SELECT * FROM {table} ORDER BY time ASC', engine)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    df.set_index('time', inplace=True)
    print(f'Loaded {len(df)} rows from {table}')
    return df


def load_vix():
    print('Loading India VIX...')
    vix = pd.read_csv(RAW_VIX_PATH, header=[0,1], index_col=0, parse_dates=True)
    vix.columns = ['_'.join(col).strip().lower() for col in vix.columns]
    vix = vix[['close_^indiavix']].rename(columns={'close_^indiavix': 'vix_close'})
    vix.index = pd.to_datetime(vix.index, utc=True)
    vix['vix_change'] = vix['vix_close'].pct_change()
    vix['vix_regime'] = pd.cut(
        vix['vix_close'],
        bins=[0, 15, 20, 100],
        labels=[0, 1, 2]
    ).astype(float)
    print(f'Loaded {len(vix)} VIX rows')
    return vix


def add_indicators(df, prefix=''):
    # RSI 14
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=13, adjust=False).mean()
    avg_loss = loss.ewm(com=13, adjust=False).mean()
    rs = avg_gain / avg_loss
    df[f'{prefix}rsi_14'] = 100 - (100 / (1 + rs))

    # EMA 9, 21, 50
    df[f'{prefix}ema_9']  = df['close'].ewm(span=9,  adjust=False).mean()
    df[f'{prefix}ema_21'] = df['close'].ewm(span=21, adjust=False).mean()
    df[f'{prefix}ema_50'] = df['close'].ewm(span=50, adjust=False).mean()

    # EMA crossover signals
    df[f'{prefix}ema_9_21_cross']  = df[f'{prefix}ema_9']  - df[f'{prefix}ema_21']
    df[f'{prefix}ema_21_50_cross'] = df[f'{prefix}ema_21'] - df[f'{prefix}ema_50']

    # MACD
    ema_12 = df['close'].ewm(span=12, adjust=False).mean()
    ema_26 = df['close'].ewm(span=26, adjust=False).mean()
    df[f'{prefix}macd']        = ema_12 - ema_26
    df[f'{prefix}macd_signal'] = df[f'{prefix}macd'].ewm(span=9, adjust=False).mean()
    df[f'{prefix}macd_hist']   = df[f'{prefix}macd'] - df[f'{prefix}macd_signal']

    # Bollinger Bands
    bb_mid             = df['close'].rolling(20).mean()
    bb_std             = df['close'].rolling(20).std()
    bb_upper           = bb_mid + 2 * bb_std
    bb_lower           = bb_mid - 2 * bb_std
    df[f'{prefix}bb_position'] = (df['close'] - bb_lower) / (bb_upper - bb_lower)
    df[f'{prefix}bb_width']    = (bb_upper - bb_lower) / bb_mid

    # ATR 14
    high_low   = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close  = (df['low']  - df['close'].shift()).abs()
    tr         = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df[f'{prefix}atr_14'] = tr.ewm(com=13, adjust=False).mean()

    return df


def calculate_features(df_1min, df_5min, df_15min, vix):
    print('Calculating 1-min indicators...')
    df = add_indicators(df_1min.copy(), prefix='')

    # Price returns
    df['return_1']  = df['close'].pct_change(1)
    df['return_5']  = df['close'].pct_change(5)
    df['return_15'] = df['close'].pct_change(15)

    # Candle features
    df['candle_body']  = (df['close'] - df['open']).abs()
    df['candle_range'] = df['high'] - df['low']
    df['candle_ratio'] = df['candle_body'] / df['candle_range'].replace(0, np.nan)

    # Time features
    df['hour']        = df.index.hour
    df['minute']      = df.index.minute
    df['day_of_week'] = df.index.dayofweek

    # Lag features
    for col in ['rsi_14', 'macd', 'atr_14', 'bb_position']:
        df[f'{col}_lag1'] = df[col].shift(1)
        df[f'{col}_lag2'] = df[col].shift(2)

    # 5-min indicators
    print('Calculating 5-min indicators...')
    df_5min = add_indicators(df_5min.copy(), prefix='5m_')
    df_5min_features = df_5min[[
        '5m_rsi_14', '5m_ema_9', '5m_ema_21',
        '5m_macd', '5m_macd_hist', '5m_atr_14'
    ]]

    # 15-min indicators
    print('Calculating 15-min indicators...')
    df_15min = add_indicators(df_15min.copy(), prefix='15m_')
    df_15min_features = df_15min[[
        '15m_rsi_14', '15m_ema_9', '15m_ema_21',
        '15m_macd', '15m_macd_hist', '15m_atr_14'
    ]]

    # Merge 5-min onto 1-min
    df = df.merge(df_5min_features, left_index=True, right_index=True, how='left').ffill()

    # Merge 15-min onto 1-min
    df = df.merge(df_15min_features, left_index=True, right_index=True, how='left').ffill()

# Merge VIX daily
    print('Merging India VIX...')
    vix.index = vix.index.normalize()
    vix_reset = vix[['vix_close', 'vix_change', 'vix_regime']].copy()
    vix_reset.index.name = '_date'
    vix_reset = vix_reset.reset_index()
    df = df.reset_index()
    df['_date'] = df['time'].dt.tz_convert('UTC').dt.normalize()
    df = df.merge(vix_reset, on='_date', how='left')
    df = df.set_index('time')
    df.drop(columns=['_date'], inplace=True)
    df.ffill(inplace=True)

    # Drop OHLC
    df.drop(columns=['open', 'high', 'low', 'close'], inplace=True)

    # Drop NaNs
    df.dropna(inplace=True)

    print(f'Final feature matrix shape: {df.shape}')
    print(f'Total features: {len(df.columns)}')
    print(f'Columns: {df.columns.tolist()}')
    return df


def save_features(df):
    path = 'data/processed/features.parquet'
    df.to_parquet(path)
    print(f'Saved to {path}')


if __name__ == '__main__':
    df_1min  = load_from_db('nifty_1min')
    df_5min  = load_from_db('nifty_5min')
    df_15min = load_from_db('nifty_15min')
    vix      = load_vix()
    df       = calculate_features(df_1min, df_5min, df_15min, vix)
    save_features(df)
    print(df.head(3))
