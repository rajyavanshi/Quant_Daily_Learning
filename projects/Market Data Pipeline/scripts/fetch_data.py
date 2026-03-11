# =====================================================
# File: fetch_data.py
# Purpose: Reusable Market Data Client for downloading
#          and storing financial market data.
# Author: Suraj Prakash
# =====================================================


import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

from yfinance import ticker


# ---------------------------------------------------
# Logging Configuration
# ---------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1] # project root
LOG_DIR = BASE_DIR / "logs" # logs will be saved in project_root/logs/
LOG_DIR.mkdir(exist_ok=True) # create logs directory if it doesn't exist

LOG_FILE = LOG_DIR / "data_fetch.log" # log file path

logging.basicConfig( # configure logging
    level=logging.INFO, # log level
    format="%(asctime)s | %(levelname)s | %(message)s", # log format with timestamp and log level 
    handlers=[
        logging.FileHandler(LOG_FILE), # save logs to file
        logging.StreamHandler() # also print logs to console
    ]
)
logger = logging.getLogger(__name__) # get logger instance for this module 


# =====================================================
# MarketDataClient Class
# =====================================================

class MarketDataClient:
    """
    MarketDataClient

    A reusable client for downloading financial market data
    and storing it locally.

    Currently supports:
    - Yahoo Finance

    Can be extended later for:
    - Binance
    - Polygon
    - Broker APIs
    """
    def __init__(self):
        """ 
        Constructor for MarketDataClient
        
        Initializes directory structure for storing data.
        """
        self.base_dir = BASE_DIR
        self.raw_dir = self.base_dir / "data" / "raw" # raw data directory
        self.csv_dir = self.raw_dir / "csv" # csv data directory
        self.parquet_dir = self.raw_dir / "parquet" # parquet data directory
        # create directories if they don't exist
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        
        
    # =====================================================
    # Fetch Data from Yahoo Finance
    # =====================================================
    def fetch_yahoo_data(
        self,
        tickers: list,
        start: str = "2000-01-01",
        end: str = None,
        interval: str = "1d"
    ) -> pd.DataFrame:
        """
        Fetch historical market data from Yahoo Finance.

        Parameters:
        - tickers: List of ticker symbols to fetch data for.
        - start: Start date in "YYYY-MM-DD" format (default: "2000-01-01").
        - end: End date in "YYYY-MM-DD" format (default: None, which means today).
        - interval: Data interval (e.g., "1d", "1h", "1m") (default: "1d").

        Returns:
        - A pandas DataFrame containing the fetched market data.
        If multiple tickers are used, the DataFrame will contain a MultiIndex with the ticker symbol as the first level and the date as the second level.
        """
        
        if end is None:
            end = datetime.today().strftime("%Y-%m-%d") # set end date to today if not provided
        
        logger.info(f"Fetching data for tickers: {tickers} from {start} to {end} with interval {interval}")
        
        try:
            # Use yfinance to download data
            data = yf.download(
                tickers=tickers,
                start=start,
                end=end,
                interval=interval,
                auto_adjust=True, # adjust prices for dividends and splits
                threads=True # use multiple threads for faster download
            )
            logger.info("Data fetching successful")
            return data
        
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise e
        
    # ---------------------------------------------------
    # Code for Missing Candle Detection
    # ---------------------------------------------------       
    def check_missing_candles(self, data: pd.DataFrame,ticker: str, expected_interval: str = "1d") -> dict:
        """
        Check for missing candles in the fetched data.

        Parameters:
        - data: DataFrame containing the market data with a DateTime index.
        - expected_interval: Expected time interval between candles (e.g., "1d", "1h", "1m").
        - ticker: The ticker symbol for which to check missing candles.

        Returns:
        - A dictionary where keys are ticker symbols and values are lists of missing candle timestamps.
        """
        # This function can be implemented to analyze the DataFrame index and identify any missing timestamps based on the expected interval. It would return a dictionary with ticker symbols as keys and lists of missing timestamps as values. This is useful for ensuring data completeness before saving or further processing.
        if data.empty:
            logger.warning(f"No data available to check for missing candles for ticker: {ticker}")
            return {ticker: []}
        start = data.index.min()
        end = data.index.max()
        
        # Determine expected frequenct
        
        freq_map = {
            "1d": "B", # business day frequency for daily data
            "1h": "H",
            "1m": "T"
        }
        if expected_interval not in freq_map:
            logger.error(f"Unsupported interval: {expected_interval}. Use '1d', '1h', or '1m'.")
            raise ValueError(f"Unsupported interval: {expected_interval}. Use '1d', '1h', or '1m'.")
        expected_freq = freq_map[expected_interval]
        expected_dates = pd.date_range(start=start, end=end, freq=expected_freq) # generate expected date range based on interval
        missing_candles = expected_dates.difference(data.index) # find missing timestamps by comparing expected dates with actual data index
        if not missing_candles.empty:
            logger.warning(f"Missing candles detected for ticker: {ticker}. Missing timestamps: {missing_candles}")
        else:
            logger.info(f"No missing candles detected for ticker: {ticker}")
        return {ticker: missing_candles.tolist()} # return missing candles as a list in a dictionary with ticker as key
    
            
    
    
    # =====================================================
    # Save Data to Local Storage
    # =====================================================
    
    def save_data(
        self,
        data: pd.DataFrame,
        tickers: list,
        interval: str = "1d",
        file_format: str = "parquet"
    ):
        """
        Save fetched data to local storage in specified format.
        Parameters:
        - data: DataFrame containing the market data to be saved.
        - tickers: List of ticker symbols corresponding to the data.
        - interval: Data interval (e.g., "1d", "1h", "1m") used in the filename (default: "1d").
        - file_format: Format to save the data ("csv" or "parquet", default: "parquet").
        
        NOTE : Return type is None because this function is only responsible for saving the data to local storage and does not return any value. It performs a side effect (saving the file) rather than producing a new value.
        """
        
        if file_format not in ["csv", "parquet"]:
            logger.error(f"Unsupported file format: {file_format}. Use 'csv' or 'parquet'.")
            raise ValueError(f"Unsupported file format: {file_format}. Use 'csv' or 'parquet'.")
        
        if file_format == "parquet":
            save_dir = self.parquet_dir
        else:
            save_dir = self.csv_dir
            
        
        
        for ticker in tickers:
            
            try:
                
                #--------------------------------------------------
                # Extract data for the specific ticker from the combined DataFrame
                #--------------------------------------------------
                if isinstance(data.columns, pd.MultiIndex):
                    df = data.xs(ticker, level=1, axis=1).copy() # extract data for the specific ticker if columns are multi-indexed
                else:
                    df = data.copy() # if columns are not multi-indexed, use the entire DataFrame (for single ticker case) 
                
                # -------------------------------------------------
                # Normalize column structure (fix yfinance quirks)
                # -------------------------------------------------

                # Flatten MultiIndex if present
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(-1)

                # Remove duplicate columns
                df = df.loc[:, ~df.columns.duplicated()]
                
                if "Close" not in df.columns:
                    logger.warning(f"Close column missing for ticker: {ticker}. Skipping dataset.")
                    continue

                # Standard OHLCV schema
                expected_columns = ["Open", "High", "Low", "Close", "Volume"]

                # Keep only OHLCV columns
                df = df[[c for c in expected_columns if c in df.columns]]

                # ------------------------------------
                # Basic data cleaning
                # ------------------------------------
                df.dropna(how = "all", inplace=True) # drop rows where all values are NaN
                if df.empty:
                    logger.warning(f"No data to save for ticker: {ticker}")
                    continue
                
                df.index = pd.to_datetime(df.index) # ensure index is datetime for proper sorting and saving
                df.sort_index(inplace=True) # sort by date
                df.index.name = "Date" # name the index as Date for clarity in saved files
                
                # -------------------------------------
                # Construct file path and save data
                # -------------------------------------
                file_ext = "parquet" if file_format == "parquet" else "csv"
                file_path = save_dir / f"{ticker.replace('.', '_')}_{interval}.{file_ext}" # construct file path based on interval and format
                
                #--------------------------------------
                # Check for existing file and update if necessary
                #--------------------------------------
                if file_path.exists():
                    logger.info(f"File already exists for ticker: {ticker} at path: {file_path}. Updating dataset.")
                    if file_format == "parquet":
                        old_df = pd.read_parquet(file_path) # read existing parquet file
                    else:
                        old_df = pd.read_csv(file_path,index_col=0,parse_dates=True) # read existing csv file
                        
                    # Normalize old data columns to match new data format
                    if isinstance(old_df.columns, pd.MultiIndex):
                        old_df.columns = old_df.columns.get_level_values(-1) # if columns are multi-indexed, select the second level
                    old_df = old_df.loc[:, ~old_df.columns.duplicated()] # remove duplicate columns from old data if any
                    
                    expected_columns = ["Open", "High", "Low", "Close", "Volume"]
                    old_df = old_df[[c for c in expected_columns if c in old_df.columns]] # keep only OHLCV columns in old data
                    
                    if "Close" not in old_df.columns:
                        logger.warning(f"Close column missing in existing dataset for ticker: {ticker}. Overwriting with new dataset.")
                        old_df = pd.DataFrame() # if Close column is missing in old data, ignore it and overwrite with new data
                        
                    
                    combined_df = pd.concat([old_df, df],copy=False) # combine old and new data
                    combined_df = combined_df[~combined_df.index.duplicated(keep='last')] # remove duplicate rows based on index (date)
                    combined_df.sort_index(inplace=True) # sort by date
                    
                    #---------------------------------------------------
                    # Missing candle detection before saving the updated dataset
                    #---------------------------------------------------
                    
                    self.check_missing_candles(combined_df, ticker,interval) # check for missing candles in the combined dataset
                    self.check_missing_candles(df,  ticker,interval) # check for missing candles in the new data being added
                    if file_format == "parquet":
                        combined_df.to_parquet(file_path) # save updated parquet file
                    else:
                        combined_df.to_csv(file_path) # save updated csv file
                else:
                    # Missing candle detection for new dataset before saving
                    self.check_missing_candles(df, ticker,interval) # check for missing candles in the new dataset being saved for the first time
                    if file_format == "parquet":
                        df.to_parquet(file_path) # save new parquet file
                    else:
                        df.to_csv(file_path) # save new csv file
                
                logger.info(f"Data for ticker: {ticker} saved successfully at {file_path}")
                
            except Exception as e:
                logger.error(f"Error saving data for ticker: {ticker}. Error: {e}")
                continue
            
    # =====================================================
    # Main Data Download interface
    # =====================================================
    
    def download_market_data(
        self,
        tickers: list,
        start: str = "2000-01-01",
        end: str = None,
        interval: str = "1d",
        file_format: str = "parquet"
    ):
        """
        Main interface to download market data and save it locally.
        
        Parameters:
        - tickers: List of ticker symbols to fetch data for.
        - start: Start date in "YYYY-MM-DD" format (default: "2000-01-01").
        - end: End date in "YYYY-MM-DD" format (default: None, which means today).
        - interval: Data interval (e.g., "1d", "1h", "1m") (default: "1d").
        - file_format: Format to save the data ("csv" or "parquet", default: "parquet").
        
        This function orchestrates the data fetching and saving process. It first fetches the data using the fetch_yahoo_data method and then saves it using the save_data method.
        """
        
        data = self.fetch_yahoo_data(tickers=tickers, start=start, end=end, interval=interval) # fetch data
        self.save_data(data=data, tickers=tickers,interval=interval, file_format=file_format) # save data
         

# ---------------------------------------------------
# Example Usage
# ---------------------------------------------------

if __name__ == "__main__":
    
    client = MarketDataClient() # create an instance of MarketDataClient
    

    tickers = [
        "ADANIPORTS.NS",
        "RELIANCE.NS",
        "TCS.NS",
        "INFY.NS",
        "HDFCBANK.NS"
    ]

    
    # end date -> today's date
    end_date = datetime.today().strftime("%Y-%m-%d")
    client.download_market_data(
        tickers=tickers,
        start="2026-01-01",
        end=end_date,
        interval="1d",
        file_format="parquet"
    )
    
    
# NOTE: The above code is a complete implementation of the MarketDataClient class with methods to fetch data from Yahoo Finance and save it locally in either CSV or Parquet format. The example usage at the bottom demonstrates how to use the client to download data for a list of tickers.

# NOTE : How to use this code in other projects?
# You can import the MarketDataClient class from this module and use it to fetch and save market data in your other projects. For example:
# from fetch_data import MarketDataClient
# client = MarketDataClient()
# client.download_market_data(tickers=["AAPL", "MSFT"], start="2020
# -01-01", end="2021-01-01", interval="1d", file_format="parquet")
