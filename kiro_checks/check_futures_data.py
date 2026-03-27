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
    total = conn.execute(text("SELECT COUNT(*) FROM nifty_futures_1min")).fetchone()[0]
    r = conn.execute(text("""
        SELECT DATE(time AT TIME ZONE 'Asia/Kolkata') as d, COUNT(*) as n, contract
        FROM nifty_futures_1min
        GROUP BY d, contract
        ORDER BY d
    """))
    rows = r.fetchall()

print(f"Total futures candles: {total}")
for row in rows:
    print(f"  {row[0]}  {row[1]} candles  [{row[2]}]")
