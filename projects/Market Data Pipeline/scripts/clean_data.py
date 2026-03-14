# =====================================================
# File: clean_data.py
# Purpose: Clean raw market data before analysis
# Author: Suraj Prakash
# =====================================================

import pandas as pd
from pathlib import Path
import logging


# ---------------------------------------------------
# Paths
# ---------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "cleaned"

LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "data_clean.log"

LOG_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------
# Logger
# ---------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# =====================================================
# DataCleaner Class
# =====================================================

class DataCleaner:
    """
    DataCleaner

    Responsible for validating and cleaning raw market datasets.

    Compatible with:
    - MarketDataClient
    - DataLoader

    Supports:
    - CSV
    - Parquet
    """

    def __init__(self, file_format="parquet"):

        if file_format not in ["csv", "parquet"]:
            raise ValueError("file_format must be 'csv' or 'parquet'")

        self.file_format = file_format

        self.raw_dir = RAW_DIR / file_format
        self.clean_dir = CLEAN_DIR / file_format

        self.clean_dir.mkdir(parents=True, exist_ok=True)


    # ---------------------------------------------------
    # Build file path
    # ---------------------------------------------------

    def _build_filename(self, ticker, interval):

        file_ext = "parquet" if self.file_format == "parquet" else "csv"

        return f"{ticker.replace('.', '_')}_{interval}.{file_ext}"


    # ---------------------------------------------------
    # Load Raw Dataset
    # ---------------------------------------------------

    def load_raw_data(self, ticker, interval) -> pd.DataFrame:

        file_name = self._build_filename(ticker, interval)

        file_path = self.raw_dir / file_name

        if not file_path.exists():
            raise FileNotFoundError(f"Raw data not found: {file_path}")

        if self.file_format == "parquet":
            df = pd.read_parquet(file_path)
        else:
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)

        return df
    
    # Checking for missing timestamps
    
    def check_missing_timestamps(self, df):

        start = df.index.min()
        end = df.index.max()

        expected_dates = pd.date_range(start=start, end=end, freq="B")

        missing_dates = expected_dates.difference(df.index)

        if not missing_dates.empty:
            logger.warning(f"{len(missing_dates)} missing timestamps detected")
            logger.warning(f"Example missing dates: {missing_dates[:5]}")


        return missing_dates



    # ---------------------------------------------------
    # Core Cleaning Function
    # ---------------------------------------------------

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply standard financial time-series cleaning.
        """

        # ensure datetime index
        df.index = pd.to_datetime(df.index)

        # sort timestamps
        df.sort_index(inplace=True)

        # remove duplicate timestamps
        df = df[~df.index.duplicated(keep="last")]

        # drop rows where all columns are NaN
        df.dropna(how="all", inplace=True)

        # forward fill small gaps
        df.ffill(inplace=True)

        # enforce OHLCV schema
        expected_columns = ["Open", "High", "Low", "Close", "Volume"]
        
        if "Close" not in df.columns: #If "Close" disappears somehow, your code silently proceeds.
            raise ValueError("Close column missing in dataset")


        df = df[[c for c in expected_columns if c in df.columns]]

        df.index.name = "Date"
        self.check_missing_timestamps(df)
        
        if (df["Close"] <= 0).any():
            raise ValueError("Invalid price detected: Close price must be positive.")


        return df


    # ---------------------------------------------------
    # Save Clean Data
    # ---------------------------------------------------

    def save_clean_data(self, df, ticker, interval):

        file_name = self._build_filename(ticker, interval)

        file_path = self.clean_dir / file_name

        if self.file_format == "parquet":
            df.to_parquet(file_path)
        else:
            df.to_csv(file_path)

        logger.info(f"Clean data saved → {file_path}")


    # ---------------------------------------------------
    # Clean Single Ticker
    # ---------------------------------------------------

    def clean_ticker(self, ticker, interval):

        logger.info(f"Cleaning dataset for {ticker}")

        df = self.load_raw_data(ticker, interval)

        clean_df = self.clean_dataframe(df)

        self.save_clean_data(clean_df, ticker, interval)


    # ---------------------------------------------------
    # Clean Multiple Tickers
    # ---------------------------------------------------

    def clean_multiple(self, tickers, interval):

        for ticker in tickers:

            try:

                self.clean_ticker(ticker, interval)

            except Exception as e:

                logger.error(f"Cleaning failed for {ticker}: {e}")


# =====================================================
# Example Usage
# =====================================================

if __name__ == "__main__":

    tickers = [
        "ADANIPORTS.NS",
        "RELIANCE.NS",
        "TCS.NS",
        "INFY.NS",
        "HDFCBANK.NS"
    ]

    cleaner = DataCleaner(file_format="csv")

    cleaner.clean_multiple(tickers, interval="1d")
