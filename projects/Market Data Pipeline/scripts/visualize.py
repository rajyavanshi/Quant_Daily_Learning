# =====================================================
# File: visualize.py
# Purpose: Generate visualizations for financial datasets
# Author: Suraj Prakash
# =====================================================

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pathlib import Path

from data_loader import DataLoader


# ---------------------------------------------------
# Paths for saving outputs
# ---------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]

FIGURES_DIR = BASE_DIR / "outputs" / "figures"
TABLES_DIR = BASE_DIR / "outputs" / "tables"

# create directories if they don't exist
FIGURES_DIR.mkdir(parents=True, exist_ok=True)
TABLES_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------
# Visualizer Class
# ---------------------------------------------------

class Visualizer:
    """
    Visualizer

    Generates financial charts and saves them
    into the outputs/figures directory.
    """

    def __init__(self, file_format="parquet"):

        # ---------------------------------------------------
        # NEW: load processed dataset for visualization
        # ---------------------------------------------------

        self.loader = DataLoader(
            file_format=file_format,
            data_stage="processed"
        )


    # ---------------------------------------------------
    # Price Chart
    # ---------------------------------------------------

    def plot_price(self, ticker, interval):

        df = self.loader.load_ticker_data(ticker, interval)

        plt.figure(figsize=(10,5))

        plt.plot(df.index, df["Close"])

        plt.title(f"{ticker} Price")
        plt.xlabel("Date")
        plt.ylabel("Price")

        file_path = FIGURES_DIR / f"{ticker}_price.png"

        plt.savefig(file_path)

        plt.close()


    # ---------------------------------------------------
    # Return Distribution
    # ---------------------------------------------------

    def plot_return_distribution(self, ticker, interval):

        df = self.loader.load_ticker_data(ticker, interval)

        returns = df["Log_Return"].dropna()

        plt.figure(figsize=(8,5))

        sns.histplot(returns, bins=50, kde=True)

        plt.title(f"{ticker} Return Distribution")

        file_path = FIGURES_DIR / f"{ticker}_return_distribution.png"

        plt.savefig(file_path)

        plt.close()


    # ---------------------------------------------------
    # Volatility Clustering
    # ---------------------------------------------------

    def plot_volatility(self, ticker, interval):

        df = self.loader.load_ticker_data(ticker, interval)

        plt.figure(figsize=(10,5))

        plt.plot(df.index, df["Volatility_20"])

        plt.title(f"{ticker} Rolling Volatility")

        file_path = FIGURES_DIR / f"{ticker}_volatility.png"

        plt.savefig(file_path)

        plt.close()


    # ---------------------------------------------------
    # Drawdown Curve
    # ---------------------------------------------------

    def plot_drawdown(self, ticker, interval):

        df = self.loader.load_ticker_data(ticker, interval)

        plt.figure(figsize=(10,5))

        plt.plot(df.index, df["Drawdown"])

        plt.title(f"{ticker} Drawdown")

        file_path = FIGURES_DIR / f"{ticker}_drawdown.png"

        plt.savefig(file_path)

        plt.close()


    # ---------------------------------------------------
    # Correlation Heatmap (multi-asset)
    # ---------------------------------------------------

    def correlation_heatmap(self, tickers, interval):

        data = self.loader.load_multiple_tickers(tickers, interval)

        returns = pd.concat(
            {ticker: df["Log_Return"] for ticker, df in data.items()},
            axis=1
        )

        corr = returns.corr()

        plt.figure(figsize=(8,6))

        sns.heatmap(corr, annot=True, cmap="coolwarm")

        plt.title("Return Correlation Heatmap")

        file_path = FIGURES_DIR / "correlation_heatmap.png"

        plt.savefig(file_path)

        plt.close()


    # ---------------------------------------------------
    # Generate All Plots
    # ---------------------------------------------------

    def generate_all(self, tickers, interval):

        for ticker in tickers:

            try:

                self.plot_price(ticker, interval)

                self.plot_return_distribution(ticker, interval)

                self.plot_volatility(ticker, interval)

                self.plot_drawdown(ticker, interval)

            except Exception as e:

                print(f"Visualization failed for {ticker}: {e}")

        # multi-asset visualization
        self.correlation_heatmap(tickers, interval)


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

    visualizer = Visualizer()

    visualizer.generate_all(tickers, "1d")