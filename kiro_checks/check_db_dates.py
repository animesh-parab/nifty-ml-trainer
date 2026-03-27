from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
load_dotenv()
url = "postgresql://{}:{}@{}:{}/{}".format(os.getenv("DB_USER"),os.getenv("DB_PASS"),os.getenv("DB_HOST"),os.getenv("DB_PORT"),os.getenv("DB_NAME"))
engine = create_engine(url)
with engine.connect() as conn:
    r = conn.execute(text("""
        SELECT DATE(time AT TIME ZONE 'Asia/Kolkata') as trade_date, COUNT(*) as candles
        FROM nifty_1min
        GROUP BY trade_date
        ORDER BY trade_date
    """))
    for row in r:
        print(row)
