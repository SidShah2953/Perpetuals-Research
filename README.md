# Perpetuals Research: DeFi vs TradFi Market Analysis

Comprehensive research pipeline for analyzing decentralized perpetual futures markets across multiple chains, comparing liquidity, pricing, and market dynamics with traditional finance benchmarks.

## Overview

This research project analyzes the growing DeFi perpetuals market by collecting and comparing trading data from multiple decentralized exchanges (Hyperliquid, edgeX, zkLighter) against traditional finance benchmarks (stocks, commodities, crypto spot markets via yfinance). The analysis covers three major asset classes: **Traditional Commodities**, **Traditional Equities**, and **Crypto Coins**.

**Key Questions:**
- How does DeFi perpetuals liquidity compare to TradFi markets?
- Are DeFi prices tracking TradFi prices accurately?
- Do volume patterns show lead/lag relationships between DeFi and TradFi?
- Which asset classes have the strongest DeFi adoption?

---

## Project Structure

```
Perpetuals Research/
├── phase1A.py              # Multi-chain market discovery & classification
├── phase1B.py              # OHLCV data collection & aggregation
├── phase2A.ipynb           # Interactive visualizations
├── phase2B.ipynb           # Statistical analysis
│
├── dataCollection/         # API clients for all data sources
│   ├── hyperliquid/        # Hyperliquid API (native + third-party DEXs)
│   ├── edgex/              # EdgeX API
│   ├── zklighter/          # zkLighter API
│   ├── yfinance/           # TradFi data via yfinance
│   └── common/             # Shared utilities (classification, etc.)
│
├── utils/                  # Reusable analysis utilities
│   ├── output.py           # Output directory management
│   ├── labels.py           # Asset type label generation
│   ├── visualization.py    # Phase 2A plotting functions
│   └── statistical_analysis.py  # Phase 2B analysis functions
│
├── chosen/                 # Asset selection CSVs (user-defined)
│   ├── Traditional Commodity.csv
│   ├── Traditional Equity.csv
│   └── Crypto Coin.csv
│
├── cache/                  # Cached inception dates
│   └── inception_dates_cache.csv
│
└── output/                 # All generated outputs
    ├── Phase 1A/           # Market discovery outputs
    ├── Phase 1B/           # OHLCV data by asset type
    └── Phase 2B/           # Statistical analysis exports
```

---

## Research Pipeline

### Phase 1A: Multi-Chain Market Discovery

**Purpose:** Discover all available perpetual markets across DeFi chains and identify top assets by trading volume.

**Process:**
1. **Market Collection** - Fetch all perpetual contracts from Hyperliquid (multiple DEXs), edgeX, and zkLighter
2. **Asset Classification** - Categorize assets by type (Traditional Commodity, Traditional Equity, Crypto Coin, Crypto Memecoin)
3. **Live Market Snapshots** - Collect real-time data: price, 24h volume, open interest, funding rates
4. **Top Asset Selection** - Identify top 5 assets per category by total 24h volume across all chains
5. **Inception Date Analysis** - Determine data availability for each asset/chain combination

**Outputs:**
- `all_chains_markets.csv` - Complete list of all perpetual markets (active + delisted)
- `asset_classification_multichain.csv` - Asset categorization with chain/DEX mapping
- `market_snapshot.csv` - Live market data aggregated by asset and chain
- `top5_assets_by_type.xlsx` - Top assets with inception dates and coverage details

**Key Features:**
- Multi-chain aggregation across 3+ platforms
- Smart caching for inception dates (speeds up re-runs)
- Timeout handling for slow APIs
- Comprehensive metadata (inception dates, days available, DEX coverage)

**Run:**
```bash
python phase1A.py
```

---

### Phase 1B: OHLCV Data Collection

**Purpose:** Collect historical daily OHLCV data for selected assets from both DeFi perpetuals and TradFi benchmarks.

**Asset Selection:**
- Assets selected via CSV files in `chosen/` directory
- Each CSV represents an asset type with columns: `asset`, `coin`, `dex`, `yf_ticker`, `data_since`

**Date Range:** July 1, 2025 to February 15, 2026
- Assets with earlier inception: fetched from July 1, 2025
- Assets with later inception: use actual inception date
- All data collection ends on February 15, 2026

**Process:**
1. **Load Selections** - Read asset selections from `chosen/*.csv` files
2. **Fetch DeFi Data** - Collect daily OHLCV from all selected DEXs
3. **Fetch TradFi Data** - Collect corresponding yfinance data (stocks, commodities, crypto spot)
4. **Calculate Notional Volume** - Standardize volume to USD: `volume × price`
5. **Aggregate DEXs** - Combine multiple DEXs per asset (mean prices, sum volumes)
6. **Export Excel Files** - Merge DeFi + TradFi data into unified files

**Outputs:**
- `ohlcv_1d_multiasset/*.csv` - Individual CSV files per asset/DEX
- `[Asset Type]/[Asset].xlsx` - Excel files with merged DeFi + TradFi data
  - Columns prefixed: `defi_*` and `tradfi_*`
  - Includes: open, high, low, close, volume, notional_volume, num_trades

**Volume Calculation:**
- **DeFi Perpetuals:** `volume (base units) × close price = USD notional`
- **Crypto Spot (yfinance):** Volume already in USD
- **Stocks/Commodities (yfinance):** `volume × close price = USD notional`

**Run:**
```bash
python phase1B.py
```

---

### Phase 2A: Interactive Visualizations

**Purpose:** Create interactive Plotly visualizations comparing DeFi and TradFi trading volumes and prices across all asset types.

**Analysis Period:**
- Crypto assets: July 2024 onwards
- Traditional assets: All available data

**Visualizations:**

1. **Overview Dashboard** (per asset type)
   - Average daily volumes (DeFi vs TradFi)
   - Volume ratios
   - Price correlation
   - Data availability

2. **Volume Comparison Charts** (per asset)
   - Dual y-axis plots showing DeFi and TradFi volumes
   - Time series with different scales for fair comparison
   - Automatically adapts labels based on asset type:
     - Traditional assets: DeFi vs TradFi
     - Crypto assets: DEX Perpetuals vs CEX Spot

**Key Features:**
- Parameterized functions from `utils.visualization`
- Automatic label adaptation (DeFi/TradFi vs DEX/CEX)
- Date filtering support for focused analysis
- Dual y-axis plots for volume scale differences
- Clean, minimal notebook code (all logic in utils)

**Implementation:**
```python
from utils import (
    load_all_assets,
    create_overview_dashboard,
    plot_assets_by_type,
)
```

**Outputs:**
- Interactive Plotly charts (displayed in notebook)
- Overview tables with formatted statistics

**Run:** Open and execute `phase2A.ipynb` in Jupyter

---

### Phase 2B: Statistical Analysis

**Purpose:** Perform comprehensive statistical analysis to identify patterns, correlations, and anomalies in DeFi vs TradFi trading.

**Analysis Period:** July 1, 2025 to February 15, 2026

**Statistical Analyses:**

1. **Volume Statistics**
   - Average daily notional volumes (DeFi and TradFi)
   - Volume ratios (TradFi/DeFi)
   - Days with overlapping data

2. **Daily Volume T-Test Analysis**
   - **Method:** Rolling 3-day trading window
   - **Purpose:** Detect statistically significant volume spikes/drops
   - **Window:** 3 most recent trading days (excludes weekends, holidays, pre-launch zeros)
   - **Formula:** `t = (today_volume - window_mean) / (window_std / √3)`
   - **Threshold:** ±4.303 (95% confidence interval, df=2)
   - **Interpretation:**
     - |t| < 4.303: Normal trading volume
     - |t| ≥ 4.303: Statistically unusual volume (potential pattern/anomaly)
   - **Outputs:** 2×2 subplot (T-scores + volumes), CSV export with detailed stats

3. **Cross-Correlation Analysis**
   - **Method:** Pearson correlation with lag shifts (-7 to +7 days)
   - **Purpose:** Identify lead/lag relationships between DeFi and TradFi volumes
   - **Interpretation:**
     - Negative lag: DeFi leads TradFi (DeFi predicts TradFi)
     - Positive lag: TradFi leads DeFi (TradFi predicts DeFi)
     - Peak correlation: Strongest lead/lag relationship
   - **Outputs:** Bar charts with peak lag annotations

4. **Price Correlation & Tracking Error**
   - Pearson correlation coefficients (DeFi vs TradFi prices)
   - Tracking error (standard deviation of price differences)
   - Average price difference (percentage)
   - **Interpretation:**
     - High correlation (>0.99): Prices move together closely
     - Low tracking error: Small absolute price differences

5. **Asset Type Summary**
   - Aggregated metrics by asset class
   - Total volumes, average correlations
   - DeFi adoption comparison across asset types

**Key Features:**
- Parameterized functions from `utils.statistical_analysis`
- Trading day windows (automatic exclusion of non-trading periods)
- Flexible configuration (window size, confidence levels, lag ranges)
- CSV export for all T-test results
- Clean notebook (minimal code, all logic in utils)

**Implementation:**
```python
from utils import (
    load_all_assets_filtered,
    group_assets_by_type,
    create_volume_statistics_table,
    plot_daily_volume_ttest,
    plot_cross_correlation,
    analyze_assets_by_type,
    create_price_correlation_table,
    create_asset_type_summary,
)
```

**Outputs:**
- Statistical tables (displayed in notebook)
- Interactive Plotly charts
- `output/Phase 2B/Daily Volume Analysis/*.csv` - T-test results with detailed metrics

**Run:** Open and execute `phase2B.ipynb` in Jupyter

---

## Utilities Module

The `utils/` module provides reusable analysis functions used across Phase 2A and 2B:

### `utils.visualization` (Phase 2A)
- `load_all_assets()` - Load all asset data from Phase 1B
- `create_overview_dashboard()` - Generate summary statistics tables
- `plot_volume_comparison()` - Create volume comparison plots
- `plot_assets_by_type()` - Plot all assets of a given type

### `utils.statistical_analysis` (Phase 2B)
- `load_all_assets_filtered()` - Load assets with date filtering
- `group_assets_by_type()` - Group and sort assets by type
- `create_volume_statistics_table()` - Volume statistics by asset type
- `plot_daily_volume_ttest()` - T-test analysis with rolling windows
- `plot_cross_correlation()` - Cross-correlation plots with lag analysis
- `analyze_assets_by_type()` - Apply any analysis function to all assets of a type
- `create_price_correlation_table()` - Price correlation and tracking error
- `create_asset_type_summary()` - Aggregated statistics by asset class
- `get_valid_window()` - Helper for trading day window calculations

### `utils.labels`
- `get_labels()` - Generate appropriate labels based on asset type
  - Traditional assets: "DeFi" vs "TradFi"
  - Crypto assets: "Perpetuals" vs "Spot Trading"

### `utils.output`
- `setup_output_directory()` - Create and manage output directory structure

---

## Asset Types

The analysis covers four major asset categories:

1. **Traditional Commodity**
   - Gold, Silver, Oil, Natural Gas
   - DeFi: Perpetual futures on DEXs
   - TradFi: Futures contracts (yfinance)

2. **Traditional Equity**
   - AAPL, GOOGL, MSFT, NVDA, TSLA, AMZN, META, COIN
   - DeFi: Perpetual futures on DEXs
   - TradFi: Stock prices (yfinance)

3. **Crypto Coin**
   - BTC, ETH, SOL, LINK, ADA
   - DeFi: Perpetual futures on DEXs
   - TradFi: Spot trading (yfinance)

4. **Crypto Memecoin** (if selected)
   - DOGE, PEPE, SHIB, WIF
   - DeFi: Perpetual futures on DEXs
   - TradFi: Spot trading (yfinance)

---

## Data Sources

### DeFi Perpetuals
- **Hyperliquid** - Native DEX + third-party DEXs (xyz, flx, cash, etc.)
- **edgeX** - Decentralized perpetuals exchange
- **zkLighter** - ZK-based perpetuals platform

### TradFi Benchmarks
- **yfinance** - Historical market data for:
  - Stock prices (AAPL, GOOGL, etc.)
  - Commodity futures (GC=F, SI=F, CL=F, NG=F)
  - Crypto spot prices (BTC-USD, ETH-USD, etc.)

---

## Key Metrics

### Volume Metrics
- **Notional Volume** - Volume standardized to USD (`volume × price`)
- **Volume Ratio** - TradFi volume / DeFi volume
- **24h Volume** - Rolling 24-hour trading volume

### Price Metrics
- **Price Correlation** - Pearson correlation between DeFi and TradFi prices
- **Tracking Error** - Standard deviation of price differences
- **Average Price Difference** - Mean percentage difference

### Statistical Metrics
- **T-Score** - Standardized measure of volume deviation from rolling mean
- **Cross-Correlation** - Pearson correlation at various lag shifts
- **Inception Date** - First date with available data for an asset

---

## Setup & Installation

### Requirements
```bash
# Python 3.10+
pip install pandas numpy openpyxl plotly scipy yfinance
```

### Project Setup
1. Clone or download the repository
2. Create `chosen/` directory with asset selection CSVs
3. Run phases sequentially:
   ```bash
   python phase1A.py  # Discover markets
   python phase1B.py  # Collect OHLCV data
   # Then run phase2A.ipynb and phase2B.ipynb in Jupyter
   ```

---

## Insights

<!-- This section is for key findings and insights from the research. -->
<!-- Add your analysis results, observations, and conclusions here. -->

---

## Technical Notes

### Code Architecture
- **Modular Design** - Reusable functions in `utils/` module
- **DRY Principle** - Single source of truth for each analysis type
- **Parameterized Functions** - Automatic adaptation to asset types
- **Type Safety** - Type hints throughout codebase
- **Documentation** - Comprehensive docstrings and README

### Performance Optimizations
- **Caching** - Inception dates cached to reduce API calls
- **Timeout Handling** - Prevents hanging on slow APIs
- **Parallel Fetching** - Concurrent data collection where possible
- **Smart Date Filtering** - Respects inception dates to avoid empty requests

### Data Quality
- **Trading Day Windows** - Automatically exclude weekends, holidays, pre-launch zeros
- **Notional Volume** - Standardized to USD for fair comparison
- **Data Validation** - Type conversion with error handling
- **Metadata Preservation** - Inception dates, DEX names, asset types tracked

### Export Formats
- **CSV** - Individual files per asset/DEX for granular analysis
- **Excel** - Merged DeFi + TradFi data with organized sheets
- **Interactive Charts** - Plotly visualizations in notebooks

---

## License

Research project - All rights reserved.

---

## Contact

For questions or collaboration inquiries, please contact the research team.
