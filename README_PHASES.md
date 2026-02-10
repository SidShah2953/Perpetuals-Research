# Multi-Chain Perpetuals Data Collection - Phase Documentation

## 🎯 Active Phase Files

### Phase 1A: Market Discovery & Classification
**File:** `phase1A.py` (17 KB)

**What it does:**
1. Collects all perpetual markets from **3 chains**: Hyperliquid, edgeX, zkLighter
2. Classifies 359 unique assets by type (Crypto, Equity, Commodity, etc.)
3. Gets live market snapshots (price, volume, OI, funding)
4. Identifies top 50 markets by 24h volume
5. Tracks inception dates for commodities across all chains

**Usage:**
```bash
python phase1A.py
```

**Output:** `output/Phase 1A/`
- `all_chains_markets.csv` - 780 markets across all chains
- `asset_classification_multichain.csv` - 359 assets classified
- `market_snapshots.csv` - Live data from 368 markets
- `top50_by_volume.csv` - Top markets by volume
- `traditional_commodity_inception_dates.csv` - Data availability

---

### Phase 1B: OHLCV Data Collection
**File:** `phase1B.py` (8.5 KB)

**What it does:**
1. Fetches daily OHLCV data from selected markets across all chains
2. Aggregates multi-chain liquidity (mean prices, sum volumes)
3. Joins with TradFi data from yfinance
4. Outputs Excel files per asset with DeFi vs TradFi comparison

**Setup:**
Create `selected_assets_multichain.csv`:
```csv
asset,chain,chain_market_id,data_since,yf_ticker
BTC,hyperliquid,BTC,2024-01-01,BTC-USD
BTC,edgex,10000001,2024-01-01,BTC-USD
BTC,zklighter,1,2024-01-01,BTC-USD
```

**Usage:**
```bash
python phase1B.py
```

---

## 📚 Archived Files

Old single-chain versions moved to `archive/`:

```
archive/
├── old_phases/
│   ├── phase1A_hyperliquid_only.py  # Original Hyperliquid-only
│   ├── phase1B_hyperliquid_only.py  # Original Hyperliquid-only
│   └── phase1A_multichain.py        # Intermediate multi-chain version
│
└── old_outputs/
    ├── Phase 1A (Hyperliquid only)/
    └── Phase 1B (Hyperliquid only)/
```

---

## 🔧 Data Collection Modules

### Supported Chains

#### 1. **Hyperliquid**
- **Markets:** 348 total (288 active)
- **Module:** `dataCollection.hyperliquid`
- **Features:** Markets, candles, funding, classification
- **Coverage:** Crypto + Equities + Commodities + more

#### 2. **edgeX**
- **Markets:** 292 total (185 active)
- **Module:** `dataCollection.edgex`
- **Features:** Markets, candles, funding
- **API:** `https://pro.edgex.exchange`

#### 3. **zkLighter**
- **Markets:** 140 total (137 active)
- **Module:** `dataCollection.zklighter`
- **Features:** Markets, candles, funding
- **API:** `https://mainnet.zklighter.elliot.ai`

### Common Utilities

#### Multi-Chain Classification
- **Module:** `dataCollection.common.classification`
- **Functions:**
  - `classify_multichain()` - Classify assets across all chains
  - `get_asset_summary()` - Summary statistics
  - `get_multi_chain_assets()` - Filter multi-chain assets

---

## 📊 Key Statistics

**Total Coverage:**
- 780 markets across 3 chains
- 610 active markets
- 359 unique assets
- $7.3B total 24h volume

**Multi-Chain Assets:**
- 140 assets available on 2+ chains
- 84 assets available on all 3 chains
- Top: BTC, ETH, NVDA, TSLA (all 3 chains)

**Asset Types:**
- Crypto Coin: 306 (39% multi-chain)
- Traditional Equity: 27 (56% multi-chain)
- Traditional Commodity: 9 (44% multi-chain)
- Sector Basket: 9
- Index: 5
- Forex: 2
- Fixed Income: 1

---

## 🚀 Quick Start

1. **Discover markets:**
   ```bash
   python phase1A.py
   ```

2. **Review classification:**
   ```bash
   # Check the output file
   open output/Phase\ 1A/asset_classification_multichain.csv
   ```

3. **Select assets for data collection:**
   ```bash
   # Create your selection CSV based on classification
   # Example: select top 10 by volume, commodities, etc.
   ```

4. **Collect OHLCV data:**
   ```bash
   python phase1B.py
   ```

---

## 📝 Notes

- All phases now support **multi-chain** by default
- Old single-chain versions archived for reference
- Use `asset_classification_multichain.csv` to identify cross-chain opportunities
- edgeX bulk ticker API returns empty - individual contract queries work fine
- zkLighter uses market IDs (integers), edgeX uses contract IDs (strings)

---

Last updated: 2026-02-10
