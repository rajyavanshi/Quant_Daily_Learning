# =====================================================
# File: feature_engineering.py
# Purpose: Generate financial features from cleaned data
# Author: Suraj Prakash
# =====================================================

# ---------------------------------------------------
# Import Required Libraries
# ---------------------------------------------------

import pandas as pd                     # pandas for dataframe operations
import numpy as np                      # numpy for numerical operations (log, arrays etc.)
from pathlib import Path                # pathlib for robust file path handling
import logging                          # logging for tracking pipeline execution


# ---------------------------------------------------
# Define Project Paths
# ---------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]   # project root directory

CLEAN_DIR = BASE_DIR / "data" / "cleaned"        # directory containing cleaned datasets
PROCESSED_DIR = BASE_DIR / "data" / "processed"  # directory where feature-engineered data will be stored

LOG_DIR = BASE_DIR / "logs"                      # log directory
LOG_FILE = LOG_DIR / "feature_engineering.log"   # log file for this module

LOG_DIR.mkdir(exist_ok=True)                     # create logs folder if it does not exist


# ---------------------------------------------------
# Configure Logger
# ---------------------------------------------------

logging.basicConfig(
    level=logging.INFO,                          # log INFO level and above
    format="%(asctime)s | %(levelname)s | %(message)s",  # log message format
    handlers=[
        logging.FileHandler(LOG_FILE),           # write logs to file
        logging.StreamHandler()                  # also print logs in console
    ]
)

logger = logging.getLogger(__name__)             # get logger instance for this module


# =====================================================
# FeatureEngineer Class
# =====================================================

class FeatureEngineer:
    """
    FeatureEngineer

    This class generates financial features from cleaned OHLCV data.

    Input Data:
        data/cleaned/

    Output Data:
        data/processed/

    Supported formats:
        csv
        parquet
    """

    # ---------------------------------------------------
    # Constructor
    # ---------------------------------------------------

    def __init__(self, file_format="parquet"):

        # Validate file format
        if file_format not in ["csv", "parquet"]:
            raise ValueError("file_format must be 'csv' or 'parquet'")

        self.file_format = file_format                          # store format type

        self.clean_dir = CLEAN_DIR / file_format                 # cleaned data directory
        self.processed_dir = PROCESSED_DIR / file_format         # processed data directory

        self.processed_dir.mkdir(parents=True, exist_ok=True)    # create processed folder if missing


    # ---------------------------------------------------
    # Build Filename Based on Ticker and Interval
    # ---------------------------------------------------

    def _build_filename(self, ticker, interval):

        ext = "parquet" if self.file_format == "parquet" else "csv"   # determine extension

        return f"{ticker.replace('.', '_')}_{interval}.{ext}"         # build filename


    # ---------------------------------------------------
    # Load Clean Dataset
    # ---------------------------------------------------

    def load_clean_data(self, ticker, interval):

        # generate filename
        file_name = self._build_filename(ticker, interval)

        # construct full path
        file_path = self.clean_dir / file_name

        # ensure file exists
        if not file_path.exists():
            raise FileNotFoundError(f"Clean data not found: {file_path}")

        # load dataset depending on format
        if self.file_format == "parquet":
            df = pd.read_parquet(file_path)                # load parquet file
        else:
            df = pd.read_csv(file_path, index_col=0, parse_dates=True)  # load csv file

        return df                                          # return dataframe


    # ---------------------------------------------------
    # Generate Financial Features
    # ---------------------------------------------------

    def generate_features(self, df: pd.DataFrame):

        """
        Generate financial features.

        Features Generated:

        - Simple Return
        - Log Return
        - Cumulative Return
        - Rolling Volatility (20-day)
        - Moving Average (20-day)
        """

        df = df.copy()                        # work on a copy to avoid modifying original data

        # ------------------------------------------------
        # Simple Returns
        # Formula:
        # R_t = (P_t - P_t-1) / P_t-1
        # ------------------------------------------------

        df["Simple_Return"] = df["Close"].pct_change()
        returns = df["Close"].pct_change()

        extreme = returns.abs() > 0.5   # 50% daily move

        if extreme.any():
            raise ValueError("Extreme returns detected")
            logger.warning(df[extreme])


        # ------------------------------------------------
        # Log Returns
        # Formula:
        # r_t = log(P_t / P_t-1)
        # Used widely in quantitative finance
        # ------------------------------------------------

        df["Log_Return"] = np.log(df["Close"] / df["Close"].shift(1))


        # ------------------------------------------------
        # Cumulative Returns
        # Formula:
        # cumulative return = product(1 + r_t) - 1
        # ------------------------------------------------

        df["Cumulative_Return"] = (1 + df["Simple_Return"]).cumprod() - 1


        # ------------------------------------------------
        # Rolling Volatility
        # Standard deviation of log returns over window
        # ------------------------------------------------

        df["Volatility_20"] = df["Log_Return"].rolling(20).std()


        # ------------------------------------------------
        # Moving Average
        # Used to detect price trend
        # ------------------------------------------------

        df["MA_20"] = df["Close"].rolling(20).mean()

        
    
        # ------------------------------------------------
        # Rolling Sharpe Ratio (20-day)
        # Sharpe = mean(return) / std(return)
        # ------------------------------------------------

        rolling_mean = df["Log_Return"].rolling(20).mean()
        rolling_std = df["Log_Return"].rolling(20).std()

        df["Rolling_Sharpe_20"] = rolling_mean / rolling_std


        # ------------------------------------------------
        # Drawdown Calculation
        # Drawdown = (Price - Running Peak) / Running Peak
        # ------------------------------------------------

        running_max = df["Close"].cummax()

        df["Drawdown"] = (df["Close"] - running_max) / running_max

        return df

    # ---------------------------------------------------
    # Save Processed Data
    # ---------------------------------------------------

    def save_processed_data(self, df, ticker, interval):

        # generate filename
        file_name = self._build_filename(ticker, interval)

        # construct full path
        file_path = self.processed_dir / file_name

        # save based on format
        if self.file_format == "parquet":
            df.to_parquet(file_path)
        else:
            df.to_csv(file_path)

        logger.info(f"Processed data saved → {file_path}")


    # ---------------------------------------------------
    # Process Single Ticker
    # ---------------------------------------------------

    def process_ticker(self, ticker, interval):

        logger.info(f"Generating features for {ticker}")

        df = self.load_clean_data(ticker, interval)       # load cleaned data

        df_features = self.generate_features(df)          # generate features

        self.save_processed_data(df_features, ticker, interval)  # save dataset


    # ---------------------------------------------------
    # Process Multiple Tickers
    # ---------------------------------------------------

    def process_multiple(self, tickers, interval):

        for ticker in tickers:

            try:

                self.process_ticker(ticker, interval)     # process each ticker

            except Exception as e:

                logger.error(f"Feature generation failed for {ticker}: {e}")


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

    engineer = FeatureEngineer(file_format="csv")    # initialize feature engineer

    engineer.process_multiple(tickers, interval="1d")    # generate features for all tickers
