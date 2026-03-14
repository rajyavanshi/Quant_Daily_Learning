# =====================================================
# File: compute_metrics.py
# Purpose: Compute statistical metrics from processed data
# Author: Suraj Prakash
# =====================================================

import pandas as pd
import numpy as np
from data_loader import DataLoader


# ---------------------------------------------------
# Metrics Calculator
# ---------------------------------------------------

class MetricsCalculator:
    """
    MetricsCalculator

    Computes statistical and financial metrics
    from processed market datasets.
    """

    def __init__(self, file_format="parquet"):
        """
        Constructor

        Parameters
        ----------
        file_format : str
            File format of stored dataset ("parquet" or "csv")

        NOTE:
        ---------------------------------------------------
        NEW ADDITION
        ---------------------------------------------------
        We explicitly set data_stage="processed" because
        metrics require engineered features like:

        Log_Return
        Simple_Return
        Drawdown

        These exist only in the processed dataset.
        ---------------------------------------------------
        """

        self.loader = DataLoader(
            file_format=file_format,
            data_stage="processed"  # NEW: load processed dataset for metrics
        )


    # ---------------------------------------------------
    # Compute Metrics
    # ---------------------------------------------------

    def compute_metrics(self, ticker, interval):
        """
        Compute financial statistics for a single ticker.

        Parameters
        ----------
        ticker : str
            Ticker symbol (e.g., RELIANCE.NS)

        interval : str
            Data interval (e.g., "1d")

        Returns
        -------
        dict
            Dictionary containing computed metrics
        """

        # Load processed dataset
        df = self.loader.load_ticker_data(ticker, interval)

        # Extract log returns (used for most statistical metrics)
        returns = df["Log_Return"].dropna()

        metrics = {}

        # ---------------------------------------------------
        # Mean Return
        # ---------------------------------------------------

        metrics["Mean_Return"] = returns.mean()

        # ---------------------------------------------------
        # Volatility (Standard Deviation of Returns)
        # ---------------------------------------------------

        metrics["Volatility"] = returns.std()

        # ---------------------------------------------------
        # Skewness (asymmetry of return distribution)
        # ---------------------------------------------------

        metrics["Skewness"] = returns.skew()

        # ---------------------------------------------------
        # Kurtosis (fat tails of distribution)
        # ---------------------------------------------------

        metrics["Kurtosis"] = returns.kurtosis()

        # ---------------------------------------------------
        # Sharpe Ratio
        # Formula:
        # mean(return) / std(return)
        #
        # NOTE:
        # risk-free rate assumed to be zero
        # ---------------------------------------------------

        metrics["Sharpe_Ratio"] = returns.mean() / returns.std()

        # ---------------------------------------------------
        # Maximum Drawdown
        #
        # Measures largest loss from peak value
        # ---------------------------------------------------

        cumulative = (1 + df["Simple_Return"]).cumprod()

        running_max = cumulative.cummax()

        drawdown = (cumulative - running_max) / running_max

        metrics["Max_Drawdown"] = drawdown.min()

        return metrics


    # ---------------------------------------------------
    # Compute for Multiple Assets
    # ---------------------------------------------------

    def compute_multiple(self, tickers, interval):
        """
        Compute metrics for multiple tickers.

        Parameters
        ----------
        tickers : list[str]

        interval : str

        Returns
        -------
        pandas.DataFrame
        """

        results = {}

        for ticker in tickers:

            try:

                metrics = self.compute_metrics(ticker, interval)

                results[ticker] = metrics

            except Exception as e:

                print(f"Failed computing metrics for {ticker}: {e}")

        # Convert dictionary → DataFrame
        return pd.DataFrame(results).T


# ---------------------------------------------------
# Example Usage
# ---------------------------------------------------

if __name__ == "__main__":

    tickers = [
        "ADANIPORTS.NS",
        "RELIANCE.NS",
        "TCS.NS",
        "INFY.NS",
        "HDFCBANK.NS"
    ]

    calculator = MetricsCalculator()

    metrics_df = calculator.compute_multiple(tickers, "1d")

    print(metrics_df)