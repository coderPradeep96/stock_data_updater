import yfinance as yf
import pandas as pd
import time
import os
import logging
from datetime import datetime
from supabase import create_client, Client
import argparse

# === Logging Setup ===
logging.basicConfig(
    filename="logs/update_log.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# === Supabase Client ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Supabase credentials not set in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Load Ticker List ===
def load_tickers(filepath: str, limit: int = None):
    ticker_list = pd.read_csv(filepath)
    tickers = ticker_list['SYMBOL'].dropna().unique().tolist()
    return tickers[:limit] if limit else tickers

# === Metadata Updater ===
def update_metadata(tickers):
    for ticker in tickers:
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
            logging.info(f"[Metadata] Updated: {ticker}")
        except Exception as e:
            logging.error(f"[Metadata] Failed: {ticker} - {e}")
            with open("logs/failed_tickers.log", "a") as f:
                f.write(f"Metadata: {ticker} - {e}\n")

# === OHLCV Updater ===
def update_ohlcv(ticker, batch_size=100):
    time.sleep(1)
    modified_ticker = ticker + ".NS"
    try:
        df = yf.download(modified_ticker, period="10y", progress=False, auto_adjust=True)
        if df.empty:
            logging.warning(f"[OHLCV] No data for {ticker}")
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

        # Upload in chunks
        for i in range(0, len(rows), batch_size):
            supabase.table("stock_ohlcv").upsert(rows[i:i+batch_size]).execute()

        logging.info(f"[OHLCV] Upserted: {ticker}, Rows: {len(rows)}")

    except Exception as e:
        logging.error(f"[OHLCV] Error with {ticker}: {e}")
        with open("logs/failed_tickers.log", "a") as f:
            f.write(f"OHLCV: {ticker} - {e}\n")

# === Main Entry Point ===
def main(args):
    tickers = load_tickers("EQUITY_L.csv", args.limit)

    if args.metadata:
        logging.info(">>> Starting metadata update...")
        update_metadata(tickers)

    if args.ohlcv:
        logging.info(">>> Starting OHLCV update...")
        for ticker in tickers:
            update_ohlcv(ticker)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Data Updater with Supabase")
    parser.add_argument("--metadata", action="store_true", help="Update stock metadata")
    parser.add_argument("--ohlcv", action="store_true", help="Update OHLCV data")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of tickers to process")

    args = parser.parse_args()
    os.makedirs("logs", exist_ok=True)
    main(args)
