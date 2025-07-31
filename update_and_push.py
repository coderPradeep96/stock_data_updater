import yfinance as yf
import pandas as pd
from supabase import create_client, Client
import os
from datetime import datetime, timedelta

# === Step 0: Initialize Supabase client ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials not set in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Step 1: NIFTY 500 Tickers ===
def get_nifty500_tickers():
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    df = pd.read_csv(url)
    tickers = df['SYMBOL'].dropna().unique().tolist()
    return tickers

nifty_500_tickers = get_nifty500_tickers()

# === Step 2: Set Dates ===
end_date = datetime.now().date()
start_date = end_date - timedelta(days=1)

# === Step 3: Fetch and Upload Data ===
def fetch_and_upsert(ticker):
    modified_ticker = ticker + ".NS"
    try:
        df = yf.download(modified_ticker, start=start_date, end=end_date, progress=False)
        if df.empty:
            print(f"⚠️ No data for {ticker}")
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
        print(f"✅ Upserted: {ticker}")
    except Exception as e:
        print(f"❌ Error with {ticker}: {e}")

# === Step 4: Run ===
print(f"📈 Updating OHLCV data for {len(nifty_500_tickers)} tickers from {start_date} to {end_date}")
for ticker in nifty_500_tickers:
    fetch_and_upsert(ticker)
