import json
import os

# News log
print("=== News ===")
news_path = "data/processed/news_log.json"
if os.path.exists(news_path):
    with open(news_path) as f:
        news = json.load(f)
    print(f"  Total entries : {len(news)}")
    if news:
        print(f"  First : {news[0]}")
        print(f"  Last  : {news[-1]}")
else:
    print("  news_log.json not found")

# Parquet file size
print("\n=== features.parquet ===")
path = "data/processed/features.parquet"
if os.path.exists(path):
    size = os.path.getsize(path)
    print(f"  File size: {size} bytes")
    if size < 1000:
        print("  ⚠ File looks corrupted (too small)")
else:
    print("  File not found")
