# Quant Daily Learning

This repository tracks my daily learning in quantitative finance and data-driven markets research.  
Each folder under `projects/` is a self-contained mini–research study with:

- Real market data
- Cleaned notebooks
- Statistical diagnostics
- Short, quant-style conclusions

## Projects

### 1. RDD-VA: Return Distribution Diagnostics for a Volatile Asset (NIFTY, 2023–2024)

Folder: `projects/RDD_VA_NIFTY_2023_2024/`

- Analyses daily log returns of NIFTY (^NSEI) over ~1 year
- Tests the Gaussian (Normal) assumption commonly used in risk models
- Compares empirical tails vs Normal
- Fits alternative distributions (Normal, Student’s t, Laplace)
- Produces plots for:
  - Log-return time series
  - Histogram + KDE + Normal overlay
  - Empirical CDF and Survival function
  - Q–Q plot vs Normal
  - Rolling skewness and excess kurtosis

More details are inside the project-level README.
