# =====================================================
# File: data_loader.py
# Purpose: Load stored market data for analysis
# Author: Suraj Prakash
# =====================================================

import pandas as pd
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1] # project root
RAW_DATA_DIR = BASE_DIR / "data" / "raw" # raw data directory

class DataLoader:
    """
    DataLoader

    A utility class for loading stored market data for analysis.
    """
    def __init__(self, file_format="parquet" ):
        """
        Constructor for DataLoader
        
        Parameters:
        - file_format: str, either "parquet" or "csv", default is "parquet"
        """
        
        if file_format not in ["parquet", "csv"]:
            raise ValueError("file_format must be either 'parquet' or 'csv'")
        self.file_format = file_format
        self.data_dir = RAW_DATA_DIR / file_format # directory where data is stored based on format
        
    
    # ---------------------------------------------------
    # Load Single Ticker Data
    # ---------------------------------------------------
    
    def load_ticker_data(self, ticker, interval)-> pd.DataFrame:
        """
        Load data for a single ticker
        
        Parameters:
        - ticker: str, the ticker symbol to load data for
        - interval: str, the data interval (e.g., "1d", "1h", "1m")

        
        Returns:
        - df: pd.DataFrame, the loaded data for the ticker
        """
        file_ext = "parquet" if self.file_format == "parquet" else "csv" # determine file extension based on format
        file_name = f"{ticker.replace('.', '_')}_{interval}.{file_ext}" # construct file name based on ticker, interval, and file format
        file_path = self.data_dir / file_name # full path to the data file
        
        if not file_path.exists():
            raise FileNotFoundError(f"Data file not found for ticker: {ticker} at path: {file_path}")
        
        if self.file_format == "parquet":
            df = pd.read_parquet(file_path) # load parquet file
        else:
            df = pd.read_csv(file_path,index_col=0,parse_dates=True) # load csv file with index as date and parse dates
            
        # Normalize column names to standard format (Open, High, Low, Close, Volume)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(-1) # if columns are multi-indexed, select the second level (e.g., Open, High, Low, Close, Volume)
        df = df.loc[:,~df.columns.duplicated()] # remove duplicate columns if any
        
        expected_columns = ["Open", "High", "Low", "Close", "Volume"]
        df = df[[c for c in expected_columns if c in df.columns]] # keep only OHLCV columns
        df.index = pd.to_datetime(df.index) # ensure index is in datetime format
        df.sort_index(inplace=True) # sort by date just in case
        df.index.name = "Date" # rename index to "Date"
        return df 
    
    # ---------------------------------------------------
    # Load Multiple Tickers Data -> This will return in stacked format
    # ---------------------------------------------------
    
    def load_multiple_tickers(self, tickers,interval)-> dict:
        """
        Load data for multiple tickers
        
        Parameters:
        - tickers: list of str, the ticker symbols to load data for
        - interval: str, the data interval (e.g., "1d", "1h", "1m")

        
        Returns:
        - dict, a dictionary with ticker symbols as keys and their corresponding dataframes as values
        """
        data = {} # initialize empty dictionary to store dataframes for each ticker
        
        for ticker in tickers:
            try:
                df = self.load_ticker_data(ticker, interval) # load data for each ticker
                # df.index = pd.to_datetime(df.index) # ensure index is in datetime format
                data[ticker] = df # add dataframe to dictionary with ticker as key
            except Exception as e:
                print(f"Error loading data for ticker: {ticker}. Error: {e}")
        
        return data
        
    
    
    
    # =====================================================
    # Load Price Matrix for Multiple Tickers -> This will return in unstacked format
    # =====================================================
    def load_price_matrix(self, tickers,interval)-> pd.DataFrame:
        """
        Load price matrix for multiple tickers
        
        Create aligned price matrix with dates as index and tickers as columns containing the closing prices. This is useful for time series analysis and modeling.
        
        Parameters:
        - tickers: list of str, the ticker symbols to load data for
        - interval: str, the data interval (e.g., "1d", "1h", "1m")
        
        Returns:
        - price_matrix: pd.DataFrame, a dataframe with dates as index and tickers as columns containing the closing prices
        """
        data_dict = self.load_multiple_tickers(tickers, interval) # load data for multiple tickers
        price_matrix = pd.concat({ticker: df['Close'] for ticker, df in data_dict.items() if 'Close' in df.columns}, axis=1) # create price matrix with closing prices for tickers that have 'Close' column
        
        price_matrix = price_matrix.sort_index() # sort by date just in case
        price_matrix = price_matrix.dropna(how="all") # drop rows where all values are NaN (i.e., dates where no tickers have data)
        return price_matrix
       
    
    