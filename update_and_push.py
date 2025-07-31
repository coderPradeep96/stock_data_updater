import yfinance as yf
import pandas as pd
import time
from supabase import create_client, Client
import os
from datetime import datetime, timedelta

# === Step 0: Initialize Supabase client ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials not set in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Step 1: Load Ticker List ===
ticker_list = pd.read_csv("EQUITY_L.csv")
tickers = ticker_list['SYMBOL'].dropna().unique().tolist()

# === Step 2: Update Metadata ===
def update_metadata(tickers):
    for ticker in tickers:
        print(f"Updating metadata: {ticker}")
        modified_ticker = ticker + ".NS"
        try:
            info = yf.Ticker(modified_ticker).info
            metadata = {
                "ticker": ticker,
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "shares_outstanding": info.get("sharesOutstanding") or 0,
            }
            supabase.table("stock_metadata").upsert(metadata).execute()
        except Exception as e:
            print(f"[Metadata] {ticker} failed: {e}")

# === Step 3: Update OHLCV with Batch Upload ===
def update_ohlcv(ticker, batch_size=100):
    time.sleep(1)
    modified_ticker = ticker + ".NS"
    try:
        df = yf.download(modified_ticker, period="10y", progress=False, auto_adjust=True)
        if df.empty:
            print(f"⚠️ No data for {ticker}")
            return

        rows = []
        for i in range(len(df)):
            rows.append({
                "date": df.index[i].strftime("%Y-%m-%d"),
                "ticker": ticker,
                "open": float(df["Open"].iloc[i]),
                "high": float(df["High"].iloc[i]),
                "low": float(df["Low"].iloc[i]),
                "close": float(df["Close"].iloc[i]),
                "volume": int(df["Volume"].iloc[i])
            })

        for i in range(0, len(rows), batch_size):
            supabase.table("stock_ohlcv").upsert(rows[i:i+batch_size]).execute()

        print(f"✅ Upserted {len(rows)} rows for {ticker}")

    except Exception as e:
        print(f"❌ Error with {ticker}: {e}")

# === MAIN ===
if __name__ == "__main__":
    update_metadata(tickers)
    for ticker in tickers:
        update_ohlcv(ticker)
