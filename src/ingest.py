import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

RAW_DATA_PATH = 'data/raw/NIFTY 50_minute.csv'


def get_engine():
    url = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    return create_engine(url)


def load_csv():
    print('Loading CSV...')
    df = pd.read_csv(RAW_DATA_PATH)

    # Rename date column to time for TimescaleDB
    df.rename(columns={'date': 'time'}, inplace=True)

    # Parse datetime
    df['time'] = pd.to_datetime(df['time'])

    # Drop volume - all zeros, not useful
    df.drop(columns=['volume'], inplace=True)

    # Drop any duplicates
    df.drop_duplicates(subset=['time'], inplace=True)

    # Sort by time
    df.sort_values('time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    print(f'Loaded {len(df)} rows from {df[chr(116)+chr(105)+chr(109)+chr(101)].min()} to {df[chr(116)+chr(105)+chr(109)+chr(101)].max()}')
    return df


def ingest_to_db(df):
    engine = get_engine()
    print('Connecting to TimescaleDB...')

    with engine.connect() as conn:
        conn.execute(text('TRUNCATE TABLE nifty_1min'))
        conn.commit()

    print('Inserting data - this may take a few minutes...')
    df.to_sql(
        'nifty_1min',
        engine,
        if_exists='append',
        index=False,
        chunksize=10000,
        method='multi'
    )
    print('Data ingested successfully!')


def verify():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM nifty_1min'))
        count = result.scalar()
        print(f'Total rows in DB: {count}')

        result = conn.execute(text('SELECT MIN(time), MAX(time) FROM nifty_1min'))
        row = result.fetchone()
        print(f'Date range: {row[0]} to {row[1]}')


if __name__ == '__main__':
    df = load_csv()
    ingest_to_db(df)
    verify()
