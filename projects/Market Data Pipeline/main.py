# =====================================================
# File: main.py
# Purpose: Execute full market data pipeline
# Author: Suraj Prakash
# =====================================================

import logging
from datetime import datetime

from scripts.fetch_data import MarketDataClient
from scripts.clean_data import DataCleaner
from scripts.feature_eng import FeatureEngineer


# ---------------------------------------------------
# Logger
# ---------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------
# Configuration
# ---------------------------------------------------

TICKERS = [
    "ADANIPORTS.NS",
    "RELIANCE.NS",
    "TCS.NS",
    "INFY.NS",
    "HDFCBANK.NS"
]

INTERVAL = "1d"
FILE_FORMAT = "parquet"

START_DATE = "2020-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")


# =====================================================
# Run Pipeline
# =====================================================

def run_pipeline():

    logger.info("Starting Market Data Pipeline")

    # ---------------------------------------------------
    # Step 1 — Fetch Data
    # ---------------------------------------------------

    logger.info("Step 1: Fetching Market Data")

    client = MarketDataClient()

    client.download_market_data(
        tickers=TICKERS,
        start=START_DATE,
        end=END_DATE,
        interval=INTERVAL,
        file_format=FILE_FORMAT
    )


    # ---------------------------------------------------
    # Step 2 — Clean Data
    # ---------------------------------------------------

    logger.info("Step 2: Cleaning Data")

    cleaner = DataCleaner(file_format=FILE_FORMAT)

    cleaner.clean_multiple(
        tickers=TICKERS,
        interval=INTERVAL
    )


    # ---------------------------------------------------
    # Step 3 — Feature Engineering
    # ---------------------------------------------------

    logger.info("Step 3: Generating Features")

    engineer = FeatureEngineer(file_format=FILE_FORMAT)

    engineer.process_multiple(
        tickers=TICKERS,
        interval=INTERVAL
    )


    logger.info("Pipeline execution completed successfully")


# ---------------------------------------------------
# Run Script
# ---------------------------------------------------

if __name__ == "__main__":

    run_pipeline()
