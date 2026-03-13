import yfinance as yf
import pandas as pd

df = yf.download("^NSEI", start="2026-03-13", end="2026-03-14", interval="1m", progress=False)
df.index = df.index.tz_convert("Asia/Kolkata")

# Check 2:58 PM to 3:30 PM IST
mask = (df.index >= "2026-03-13 14:58:00+05:30") & (df.index <= "2026-03-13 15:30:00+05:30")
subset = df[mask][["High", "Low", "Close"]]
print(subset.to_string())