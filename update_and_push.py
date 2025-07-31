import yfinance as yf
import pandas as pd
from supabase import create_client, Client
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# === Step 0: Supabase Client ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials not set in environment variables.")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Step 1: Get NIFTY 500 Tickers ===
def get_nifty500_tickers():
    url = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
    df = pd.read_csv(url)
    return df['SYMBOL'].dropna().unique().tolist()

tickers = get_nifty500_tickers()

# === Step 2: Set Dates ===
end_date = datetime.now().date()
start_date = end_date - timedelta(days=1)

# === Step 3: Process One Ticker (OHLCV + Metadata) ===
def process_ticker(ticker, retries=1, delay=2):
    modified_ticker = ticker + ".NS"

    # --- OHLCV ---
    for attempt in range(1, retries + 1):
        try:
            df = yf.download(modified_ticker, start=start_date, end=end_date, progress=False)
            if df.empty:
                print(f"⚠️ [OHLCV] No data for {ticker}")
                break

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
            print(f"✅ [OHLCV] Upserted: {ticker}")
            break
        except Exception as e:
            print(f"❌ [OHLCV] {ticker} Attempt {attempt}: {e}")
            if attempt == retries:
                print(f"⛔ [OHLCV] Skipped {ticker}")
            time.sleep(delay * attempt)

    # --- Metadata ---
    for attempt in range(1, retries + 1):
        try:
            info = yf.Ticker(modified_ticker).info
            metadata = {
                "ticker": ticker,
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "shares_outstanding": info.get("sharesOutstanding") or 0,
            }
            supabase.table("stock_metadata").upsert(metadata).execute()
            print(f"✅ [Metadata] Updated: {ticker}")
            break
        except Exception as e:
            print(f"❌ [Metadata] {ticker} Attempt {attempt}: {e}")
            if attempt == retries:
                print(f"⛔ [Metadata] Skipped {ticker}")
            time.sleep(delay * attempt)

# === Step 4: Run in Parallel Batches ===
def run_in_batches(ticker_list, batch_size=10, delay_between_batches=10):
    print(f"🚀 Starting update for {len(ticker_list)} tickers from {start_date} to {end_date}")
    for i in range(0, len(ticker_list), batch_size):
        batch = ticker_list[i:i + batch_size]
        print(f"▶️ Batch {i // batch_size + 1}: {batch}")
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(process_ticker, ticker) for ticker in batch]
            for future in as_completed(futures):
                pass  # Just wait for all to complete

        print(f"⏳ Waiting {delay_between_batches}s before next batch...")
        time.sleep(delay_between_batches)

    print("✅ All updates complete.")

# === Step 5: Go ===
run_in_batches(tickers, batch_size=10, delay_between_batches=15)
