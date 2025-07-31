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

ticker_list=pd.read_csv("EQUITY_L.csv")

# === Step 1: NIFTY 500 Tickers ==



# === Step 2: Update Metadata ===
def update_metadata(tickers):
    for ticker in tickers:
        print(ticker)
        modified_ticker=ticker+".NS"
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

# === Step 3: Update OHLCV ===
# === Step 3: Fetch and Upload Data ===
def update_ohlcv(ticker):
    time.sleep(1)
    modified_ticker = ticker + ".NS"
    try:
        df = yf.download(modified_ticker,period="5d", progress=False,auto_adjust=True,multi_level_index=False)
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
# === MAIN ===
if __name__ == "__main__":
    tickers = ticker_list['SYMBOL'].tolist()
    update_metadata(tickers)
    update_ohlcv(tickers)
