"""Check available Nifty futures contracts in Angel One scrip master"""
import requests
from datetime import datetime

url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
print("Fetching scrip master...")
data = requests.get(url, timeout=15).json()

nifty_futures = [
    d for d in data
    if d.get("name") == "NIFTY"
    and d.get("instrumenttype") == "FUTIDX"
    and d.get("exch_seg") == "NFO"
]

nifty_futures.sort(key=lambda x: datetime.strptime(x.get("expiry", "01JAN2099"), "%d%b%Y").date())

print(f"\nTotal Nifty futures contracts found: {len(nifty_futures)}")
print(f"\n{'Symbol':<25} {'Token':<10} {'Expiry':<12}")
print("-" * 50)
for c in nifty_futures:
    print(f"{c['symbol']:<25} {c['token']:<10} {c['expiry']:<12}")
