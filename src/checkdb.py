from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
load_dotenv()

url = "postgresql://{}:{}@{}:{}/{}".format(
    os.getenv("DB_USER"), os.getenv("DB_PASS"),
    os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME")
)
engine = create_engine(url)
with engine.connect() as conn:
    r = conn.execute(text("SELECT time, close FROM nifty_1min WHERE time > '2026-03-13' ORDER BY time DESC LIMIT 5"))
    rows = r.fetchall()
    print("Today rows in DB:", len(rows))
    for row in rows:
        print(row)