import yfinance as yf
import pandas as pd
from supabase import create_client, Client
import os
from datetime import datetime, timedelta

# Initialize Supabase client
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# List of Nifty 500 tickers (sample)
nifty_500_tickers = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS",  # Add all here
]

# Get yesterday's date (run this at night)
end_date = datetime.now().date()
start_date = end_date - timedelta(days=1)

def fetch_and_upsert(ticker):
    try:
        df = yf.download(ticker, start=start_date, end=end_date)
        if df.empty:
            return

        row = {
            "date": df.index[0].strftime("%Y-%m-%d"),
            "ticker": ticker,
            "open": float(df["Open"].iloc[0]),
            "high": float(df["High"].iloc[0]),
            "low": float(df["Low"].iloc[0]),
            "close": float(df["Close"].iloc[0]),
            "volume": int(df["Volume"].iloc[0])
        }

        supabase.table("stock_ohlcv").upsert(row).execute()
        print(f"Upserted: {ticker}")
    except Exception as e:
        print(f"Error with {ticker}: {e}")

for ticker in nifty_500_tickers:
    fetch_and_upsert(ticker)
