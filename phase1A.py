"""Phase 1A: Comprehensive Multi-Chain Perpetuals Market Analysis

Discovers and analyzes perpetual futures markets across multiple DeFi chains to identify
trading opportunities and market characteristics.

Supported Chains:
- Hyperliquid (multiple DEXs: native, xyz, flx, cash, etc.)
- edgeX
- zkLighter

Analysis Pipeline:
1. Market Discovery - Fetch all perpetual contracts from all chains
2. Asset Classification - Categorize assets by type (Traditional Commodity, Traditional Equity, Crypto Coin, etc.)
3. Live Market Snapshots - Collect current price, volume, open interest, and funding rates
4. Top Asset Identification - Identify highest volume assets per category
5. Inception Date Analysis - Determine data availability for selected assets

Outputs:
- all_chains_markets.csv - Complete list of all perpetual markets
- asset_classification_multichain.csv - Asset categorization with chain mapping
- market_snapshot.csv - Live market data across all chains
- top5_assets_by_type.xlsx - Top assets by volume with inception dates and DEX coverage

Key Features:
- Multi-chain aggregation across Hyperliquid, edgeX, and zkLighter
- Smart caching system for inception dates (reduces API calls on subsequent runs)
- Timeout handling for slow API responses
- Excel export with organized sheets per asset type
- Comprehensive metadata (inception dates, days available, chain coverage)

Implementation Notes:
- Uses client classes from dataCollection module (HyperliquidClient, EdgeXClient, ZkLighterClient)
- Classification logic in dataCollection.common.classification module
- Cache stored in cache/inception_dates_cache.csv for faster re-runs
- Output directory structure created by utils.setup_output_directory()
"""

import os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

import pandas as pd
import openpyxl  # For Excel export

from dataCollection.hyperliquid import HyperliquidClient
from dataCollection.hyperliquid.perpetuals import markets as hl_markets, candles as hl_candles
from dataCollection.common import classification as multichain_classification
from dataCollection.edgex import EdgeXClient
from dataCollection.edgex.perpetuals import markets as edgex_markets, candles as edgex_candles
from dataCollection.zklighter import ZkLighterClient
from dataCollection.zklighter.perpetuals import markets as zkl_markets, candles as zkl_candles
from utils import setup_output_directory

OUTPUT_DIR = setup_output_directory()

# Cache directory for inception dates
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)
INCEPTION_CACHE_FILE = os.path.join(CACHE_DIR, "inception_dates_cache.csv")


# ══════════════════════════════════════════════════════════════════════════════
# Cache Management for Inception Dates
# ══════════════════════════════════════════════════════════════════════════════


def fetch_with_timeout(func, timeout_seconds=2, *args, **kwargs):
    """Execute a function with a timeout. Returns None if timeout occurs."""
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_seconds)
        except FuturesTimeoutError:
            return None
        except Exception:
            return None


def load_inception_cache() -> pd.DataFrame:
    """Load cached inception dates from disk."""
    if os.path.exists(INCEPTION_CACHE_FILE):
        try:
            cache_df = pd.read_csv(INCEPTION_CACHE_FILE)
            print(f"  Loaded {len(cache_df)} cached inception dates from {INCEPTION_CACHE_FILE}")
            return cache_df
        except Exception as e:
            print(f"  Warning: Could not load cache: {e}")
            return pd.DataFrame(columns=["asset", "chain", "dex", "inception_date", "days_available"])
    return pd.DataFrame(columns=["asset", "chain", "dex", "inception_date", "days_available"])


def save_inception_cache(cache_df: pd.DataFrame):
    """Save inception dates cache to disk."""
    try:
        cache_df.to_csv(INCEPTION_CACHE_FILE, index=False)
        print(f"  Saved {len(cache_df)} inception dates to cache")
    except Exception as e:
        print(f"  Warning: Could not save cache: {e}")


def get_cached_inception(cache_df: pd.DataFrame, asset: str, chain: str, dex: str) -> str | None:
    """Get cached inception date for a specific asset/chain/dex combination."""
    if cache_df.empty:
        return None

    match = cache_df[
        (cache_df["asset"] == asset) &
        (cache_df["chain"] == chain) &
        (cache_df["dex"] == dex)
    ]

    if not match.empty:
        cached_value = match.iloc[0]["inception_date"]
        # Handle NaN values from CSV
        if pd.isna(cached_value):
            return None
        return str(cached_value)
    return None


def update_cache(cache_df: pd.DataFrame, asset: str, chain: str, dex: str, inception_date: str | None) -> pd.DataFrame:
    """Update cache with a new inception date entry."""
    # Remove existing entry if present
    cache_df = cache_df[
        ~((cache_df["asset"] == asset) &
          (cache_df["chain"] == chain) &
          (cache_df["dex"] == dex))
    ]

    # Add new entry
    if inception_date:
        days = (datetime.now(timezone.utc) - datetime.strptime(inception_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)).days
    else:
        days = 0

    new_row = pd.DataFrame([{
        "asset": asset,
        "chain": chain,
        "dex": dex,
        "inception_date": inception_date,
        "days_available": days
    }])

    return pd.concat([cache_df, new_row], ignore_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: Collect all markets from all chains
# ══════════════════════════════════════════════════════════════════════════════


def step_1_collect_all_markets() -> pd.DataFrame:
    """Fetch every perp contract from all supported chains."""
    print("=" * 70)
    print("STEP 1: Collect all perp contracts from all chains")
    print("=" * 70)

    all_markets = []

    # Hyperliquid
    print("\n  Fetching Hyperliquid markets...")
    hl_client = HyperliquidClient()
    hl_df = hl_markets.get_all_markets(hl_client)
    hl_df["chain"] = "hyperliquid"
    hl_df["chain_market_id"] = hl_df["name"]
    all_markets.append(hl_df[["chain", "chain_market_id", "name", "base_symbol", "dex", "is_delisted"]])
    print(f"    Hyperliquid: {len(hl_df)} markets ({(~hl_df['is_delisted']).sum()} active)")

    # edgeX
    print("\n  Fetching edgeX markets...")
    edgex_client = EdgeXClient()
    edgex_df = edgex_markets.get_markets(edgex_client)
    edgex_df["chain"] = "edgex"
    edgex_df["chain_market_id"] = edgex_df["contract_id"]
    edgex_df["dex"] = "edgeX"
    edgex_df["is_delisted"] = ~edgex_df["enable_trade"]
    edgex_df["base_symbol"] = edgex_df["contract_name"].str.replace(r"(USD|USDT|2USD)$", "", regex=True)
    all_markets.append(edgex_df[["chain", "chain_market_id", "contract_name", "base_symbol", "dex", "is_delisted"]].rename(columns={"contract_name": "name"}))
    print(f"    edgeX: {len(edgex_df)} markets ({(~edgex_df['is_delisted']).sum()} active)")

    # zkLighter
    print("\n  Fetching zkLighter markets...")
    zkl_client = ZkLighterClient()
    zkl_df = zkl_markets.get_markets(zkl_client)
    zkl_df["chain"] = "zklighter"
    zkl_df["chain_market_id"] = zkl_df["market_id"].astype(str)
    zkl_df["dex"] = "zkLighter"
    zkl_df["is_delisted"] = zkl_df["status"] != "active"
    zkl_df["base_symbol"] = zkl_df["symbol"]
    all_markets.append(zkl_df[["chain", "chain_market_id", "symbol", "base_symbol", "dex", "is_delisted"]].rename(columns={"symbol": "name"}))
    print(f"    zkLighter: {len(zkl_df)} markets ({(~zkl_df['is_delisted']).sum()} active)")

    # Combine all markets
    df = pd.concat(all_markets, ignore_index=True)

    print(f"\n  Total: {len(df)} markets ({(~df['is_delisted']).sum()} active)")
    print("\n  Breakdown by chain:")
    for chain in df["chain"].unique():
        chain_slice = df[df["chain"] == chain]
        n_active = (~chain_slice["is_delisted"]).sum()
        print(f"    {chain}: {n_active} active / {len(chain_slice)} total")

    path = os.path.join(OUTPUT_DIR, "all_chains_markets.csv")
    df.to_csv(path, index=False)
    print(f"\n  -> {path}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: Classify assets by type (using Hyperliquid classification)
# ══════════════════════════════════════════════════════════════════════════════


def step_2_classify_assets(
    hl_markets_df: pd.DataFrame,
    edgex_markets_df: pd.DataFrame,
    zkl_markets_df: pd.DataFrame,
) -> pd.DataFrame:
    """Classify assets by type across all chains."""
    print("\n" + "=" * 70)
    print("STEP 2: Classify assets by type (multi-chain)")
    print("=" * 70)

    df = multichain_classification.classify_multichain(
        hl_markets_df,
        edgex_markets_df,
        zkl_markets_df,
    )

    print()
    for asset_type, group in df.groupby("asset_type"):
        print(f"  {asset_type}: {len(group)}")
    
    print(f"\n  Total: {len(df)} unique assets")
    
    # Show multi-chain assets
    multi_chain = df[df["num_chains"] > 1]
    print(f"  Multi-chain assets: {len(multi_chain)}")
    print(f"  Single-chain assets: {len(df) - len(multi_chain)}")
    
    # Show by chain
    print(f"\n  Assets by chain:")
    for chain in ["hyperliquid", "edgex", "zklighter"]:
        count = len(df[df["chains"].str.contains(chain)])
        print(f"    {chain}: {count}")

    path = os.path.join(OUTPUT_DIR, "asset_classification_multichain.csv")
    df.to_csv(path, index=False)
    print(f"\n  -> {path}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: Get live market snapshots with volume, OI, funding rates
# ══════════════════════════════════════════════════════════════════════════════


def step_3_market_snapshots(classification_df: pd.DataFrame) -> pd.DataFrame:
    """Get live market snapshots from all chains."""
    print("\n" + "=" * 70)
    print("STEP 3: Collect live market snapshots (price, volume, OI, funding)")
    print("=" * 70)

    snapshots = []

    # Hyperliquid - get snapshots from ALL DEXs
    print("\n  Fetching Hyperliquid snapshots from all DEXs...")
    hl_client = HyperliquidClient()

    # Get all DEX names
    dex_names = hl_markets.get_dex_names(hl_client)
    print(f"    Found {len(dex_names)} DEXs: {', '.join(dex_names)}")

    # Fetch snapshot for each DEX
    for dex_name in dex_names:
        # Use None for native DEX, actual name for others
        dex_param = None if dex_name == "Hyperliquid (native)" else dex_name
        try:
            hl_snapshot = hl_markets.get_snapshot(dex=dex_param, client=hl_client)
            if len(hl_snapshot) > 0:
                hl_snapshot["chain"] = "hyperliquid"
                hl_snapshot["chain_market_id"] = hl_snapshot["name"]
                # Strip DEX prefix from base_symbol (e.g., "xyz:GOLD" -> "GOLD")
                hl_snapshot["base_symbol"] = hl_snapshot["name"].str.split(":").str[-1]
                hl_df = hl_snapshot[["chain", "chain_market_id", "name", "base_symbol", "mark_price", "funding_rate", "open_interest", "volume_24h"]].reset_index(drop=True)
                snapshots.append(hl_df)
                print(f"      {dex_name}: {len(hl_df)} markets")
        except Exception as e:
            print(f"      {dex_name}: Error - {e}")

    total_hl = sum(len(s) for s in snapshots)
    print(f"    Total Hyperliquid markets: {total_hl}")

    # edgeX - bulk ticker returns empty, skip for now
    print("\n  Fetching edgeX snapshots...")
    edgex_client = EdgeXClient()
    edgex_snapshot = edgex_markets.get_snapshot(client=edgex_client)
    if len(edgex_snapshot) > 0:
        edgex_snapshot["chain"] = "edgex"
        edgex_snapshot["chain_market_id"] = edgex_snapshot["contract_id"]
        edgex_snapshot["base_symbol"] = edgex_snapshot["contract_name"].str.replace(r"(USD|USDT|2USD)$", "", regex=True)
        edgex_snapshot = edgex_snapshot.rename(columns={
            "contract_name": "name",
            "last_price": "mark_price",
            "value_24h": "volume_24h"
        })
        edgex_df = edgex_snapshot[["chain", "chain_market_id", "name", "base_symbol", "mark_price", "funding_rate", "open_interest", "volume_24h"]].reset_index(drop=True)
        snapshots.append(edgex_df)
        print(f"    edgeX: {len(edgex_df)} markets")
    else:
        print(f"    edgeX: 0 markets (bulk ticker API returns empty)")

    # zkLighter
    print("\n  Fetching zkLighter snapshots...")
    zkl_client = ZkLighterClient()
    zkl_snapshot = zkl_markets.get_snapshot(client=zkl_client)
    zkl_snapshot["chain"] = "zklighter"
    zkl_snapshot["chain_market_id"] = zkl_snapshot["market_id"].astype(str)
    zkl_snapshot["base_symbol"] = zkl_snapshot["symbol"]
    zkl_snapshot["mark_px"] = zkl_snapshot["last_trade_price"]
    zkl_snapshot["funding_rate"] = None
    zkl_df = zkl_snapshot[["chain", "chain_market_id", "symbol", "base_symbol", "mark_px", "funding_rate", "open_interest", "daily_quote_volume"]].rename(columns={
        "symbol": "name",
        "mark_px": "mark_price",
        "daily_quote_volume": "volume_24h"
    }).reset_index(drop=True)
    snapshots.append(zkl_df)
    print(f"    zkLighter: {len(zkl_df)} markets")

    # Combine all snapshots
    df = pd.concat(snapshots, ignore_index=True)

    # Convert numeric columns
    for col in ["mark_price", "funding_rate", "open_interest", "volume_24h"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Calculate total volumes by chain
    print("\n  24h Volume by chain:")
    for chain in df["chain"].unique():
        chain_volume = df[df["chain"] == chain]["volume_24h"].sum()
        print(f"    {chain}: ${chain_volume:,.2f}")

    total_volume = df["volume_24h"].sum()
    print(f"\n  Total 24h Volume: ${total_volume:,.2f}")

    # Merge with classification to add Asset_Type for output only
    output_df = df.merge(
        classification_df[["asset", "asset_type"]],
        left_on="base_symbol",
        right_on="asset",
        how="left"
    )

    # Prepare output with only required columns
    # Use base_symbol as name (DEX prefix already stripped)
    output_df["name"] = output_df["base_symbol"]
    output_df = output_df[[
        "name", "asset_type", "chain", "mark_price", "volume_24h", "funding_rate",
        "open_interest", "chain_market_id", "base_symbol"
    ]]
    output_df = output_df.groupby(['name', 'asset_type', 'chain'])\
                    .agg({
                        "mark_price": "mean",
                        "volume_24h": "sum",
                        "funding_rate": "mean",
                        "open_interest": "sum",
                        "chain_market_id": "count"
                    }).reset_index()\
                    .rename(columns={
                        "name": "Name", 
                        "asset_type": "Asset Type", 
                        "chain": "Chain",
                        "mark_price": "Price",
                        "volume_24h": "Volume (24h)",
                        "funding_rate": "Funding Rate",
                        "open_interest": "Open Interest",
                        "chain_market_id": "DEXs Trading"
                        })\
                    .sort_values(['Asset Type', 'Name', 'Chain'])

    path = os.path.join(OUTPUT_DIR, "market_snapshot.csv")
    output_df.to_csv(path, index=False)
    print(f"\n  -> {path}")

    # Return original df (without merge) for step 4 to use
    return df


# ══════════════════════════════════════════════════════════════════════════════
# Helper: Get inception date for an asset across all chains
# ══════════════════════════════════════════════════════════════════════════════


def get_asset_inception_date(
    asset_name: str,
    classification_row: pd.Series,
    cache_df: pd.DataFrame,
) -> tuple[datetime | None, int, pd.DataFrame]:
    """Get earliest inception date for an asset across all chains/DEXs.

    Returns (earliest_date, days_available, updated_cache_df).
    """
    inception_dates = []

    # Initialize clients (reused across calls)
    hl_client = HyperliquidClient()
    edgex_client = EdgeXClient()
    zkl_client = ZkLighterClient()

    # Check Hyperliquid DEXs
    if classification_row["hl_dexs"]:
        for dex in classification_row["hl_dexs"].split(", "):
            # Check cache first
            cached = get_cached_inception(cache_df, asset_name, "hyperliquid", dex)
            if cached and cached != "None":
                inception = datetime.strptime(cached, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                inception_dates.append(inception)
            else:
                # Fetch from API with timeout
                try:
                    inception = fetch_with_timeout(hl_candles.get_inception_date, 2, asset_name, dex=dex, client=hl_client)
                    if inception is None:
                        print(f"[TIMEOUT: {asset_name}/{dex}] ", end="")
                    inception_str = inception.strftime("%Y-%m-%d") if inception else None
                    cache_df = update_cache(cache_df, asset_name, "hyperliquid", dex, inception_str)
                    if inception:
                        inception_dates.append(inception)
                except Exception as e:
                    print(f"[ERROR: {asset_name}/{dex}: {e}] ", end="")
                    cache_df = update_cache(cache_df, asset_name, "hyperliquid", dex, None)

    # Check edgeX
    if classification_row["edgex_contracts"]:
        for contract_id in classification_row["edgex_contracts"].split(", "):
            # Check cache first
            cached = get_cached_inception(cache_df, asset_name, "edgex", "edgeX")
            if cached and cached != "None":
                inception = datetime.strptime(cached, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                inception_dates.append(inception)
            else:
                # Fetch from API with timeout
                try:
                    inception = fetch_with_timeout(edgex_candles.get_inception_date, 2, contract_id, client=edgex_client)
                    if inception is None:
                        print(f"[TIMEOUT: {asset_name}/edgeX] ", end="")
                    inception_str = inception.strftime("%Y-%m-%d") if inception else None
                    cache_df = update_cache(cache_df, asset_name, "edgex", "edgeX", inception_str)
                    if inception:
                        inception_dates.append(inception)
                except Exception as e:
                    print(f"[ERROR: {asset_name}/edgeX: {e}] ", end="")
                    cache_df = update_cache(cache_df, asset_name, "edgex", "edgeX", None)

    # Check zkLighter
    if classification_row["zkl_markets"]:
        for market_id_str in classification_row["zkl_markets"].split(", "):
            # Check cache first
            cached = get_cached_inception(cache_df, asset_name, "zklighter", "zkLighter")
            if cached and cached != "None":
                inception = datetime.strptime(cached, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                inception_dates.append(inception)
            else:
                # Fetch from API with timeout
                try:
                    market_id = int(market_id_str)
                    inception = fetch_with_timeout(zkl_candles.get_inception_date, 2, market_id, client=zkl_client)
                    if inception is None:
                        print(f"[TIMEOUT: {asset_name}/zkLighter] ", end="")
                    inception_str = inception.strftime("%Y-%m-%d") if inception else None
                    cache_df = update_cache(cache_df, asset_name, "zklighter", "zkLighter", inception_str)
                    if inception:
                        inception_dates.append(inception)
                except Exception as e:
                    print(f"[ERROR: {asset_name}/zkLighter: {e}] ", end="")
                    cache_df = update_cache(cache_df, asset_name, "zklighter", "zkLighter", None)

    # Return earliest date
    if inception_dates:
        earliest = min(inception_dates)
        days = (datetime.now(timezone.utc) - earliest).days
        return earliest, days, cache_df
    return None, 0, cache_df


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4: Aggregate volume by asset and identify top 5 per asset type
# ══════════════════════════════════════════════════════════════════════════════


def step_4_top_assets_by_type(
    snapshots: pd.DataFrame,
    classification_df: pd.DataFrame,
    top_n: int = 5
) -> pd.DataFrame:
    """Aggregate 24h volume by asset name and identify top N assets per asset type."""
    print("\n" + "=" * 70)
    print(f"STEP 4: Top {top_n} assets by volume for each asset type")
    print("=" * 70)

    # Merge snapshots with classification to get asset types and chain info
    # Join on base_symbol (from snapshots) = asset (from classification)
    merged = snapshots.merge(
        classification_df[["asset", "asset_type", "num_chains", "total_markets",
                          "hl_dexs", "edgex_contracts", "zkl_markets"]],
        left_on="base_symbol",
        right_on="asset",
        how="left"
    )

    # Aggregate volume by asset name (sum across all chains)
    asset_volumes = merged.groupby([
        "asset", "asset_type", "num_chains", "total_markets",
        "hl_dexs", "edgex_contracts", "zkl_markets"
    ]).agg({
        "volume_24h": "sum"
    }).reset_index()

    # Sort by volume
    asset_volumes = asset_volumes.sort_values("volume_24h", ascending=False)

    print(f"\n  Total unique assets with volume data: {len(asset_volumes)}")

    # Show breakdown by asset type
    print(f"\n  Assets by type:")
    for asset_type in sorted(asset_volumes["asset_type"].dropna().unique()):
        count = len(asset_volumes[asset_volumes["asset_type"] == asset_type])
        print(f"    {asset_type}: {count} assets")

    print(f"\n  Top 10 assets by total 24h volume (across all chains):")
    for i, (_, row) in enumerate(asset_volumes.head(10).iterrows(), 1):
        asset_type = row["asset_type"] if pd.notna(row["asset_type"]) else "Unclassified"
        print(f"    {i:>2}. {row['asset']:<10} ({asset_type:<25}) ${row['volume_24h']:>15,.2f}")

    # Get top N assets per asset type
    top_per_type = []

    print(f"\n  Top {top_n} assets by asset type:")
    for asset_type in sorted(asset_volumes["asset_type"].dropna().unique()):
        type_assets = asset_volumes[asset_volumes["asset_type"] == asset_type]
        # Take up to top_n (could be fewer if category has less than top_n assets)
        top_assets = type_assets.nlargest(min(top_n, len(type_assets)), "volume_24h")
        top_per_type.append(top_assets)

        print(f"\n    {asset_type} ({len(type_assets)} total):")
        for i, (_, row) in enumerate(top_assets.iterrows(), 1):
            print(f"      {i}. {row['asset']:<10} ${row['volume_24h']:>15,.2f}")

    # Handle unclassified assets if any
    unclassified = asset_volumes[asset_volumes["asset_type"].isna()]
    if not unclassified.empty:
        top_unclassified = unclassified.nlargest(top_n, "volume_24h")
        top_per_type.append(top_unclassified)
        print(f"\n    Unclassified:")
        for i, (_, row) in enumerate(top_unclassified.iterrows(), 1):
            print(f"      {i}. {row['asset']:<10} ${row['volume_24h']:>15,.2f}")

    # Combine all top assets
    top_assets_df = pd.concat(top_per_type, ignore_index=True)

    # Add inception dates for top assets
    print(f"\n  Fetching inception dates for top {len(top_assets_df)} assets...")
    print("  Loading cache...")
    cache_df = load_inception_cache()

    inception_data = []
    cache_hits = 0
    api_calls = 0

    for idx, row in top_assets_df.iterrows():
        asset_name = row["asset"]
        print(f"    [{idx + 1}/{len(top_assets_df)}] {asset_name}...", end=" ")

        # Track cache size before call
        cache_size_before = len(cache_df)
        inception_date, days_available, cache_df = get_asset_inception_date(asset_name, row, cache_df)

        # Check if it was a cache hit or API call
        if len(cache_df) > cache_size_before:
            api_calls += 1
        else:
            cache_hits += 1

        inception_str = inception_date.strftime("%Y-%m-%d") if inception_date else None
        inception_data.append({
            "inception_date": inception_str,
            "days_available": days_available
        })
        print(f"{inception_str or 'N/A'} ({days_available} days)")

    # Save updated cache
    save_inception_cache(cache_df)
    print(f"  Cache stats: {cache_hits} hits, {api_calls} new API calls")

    # Add inception data to DataFrame
    inception_df = pd.DataFrame(inception_data)
    top_assets_df = pd.concat([top_assets_df.reset_index(drop=True), inception_df], axis=1)

    # Reorder columns for better readability
    column_order = ["asset", "asset_type", "volume_24h", "num_chains", "total_markets",
                   "inception_date", "days_available", "hl_dexs", "edgex_contracts", "zkl_markets"]
    top_assets_df = top_assets_df[column_order]

    # Export to Excel with formatting
    excel_path = os.path.join(OUTPUT_DIR, f"top{top_n}_assets_by_type.xlsx")

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        # Write summary sheet with all top assets
        top_assets_df.to_excel(writer, sheet_name="Top Assets by Type", index=False)

        # Write separate sheet for each asset type (with full details)
        for asset_type in sorted(asset_volumes["asset_type"].dropna().unique()):
            # Get top N assets for this type from the enriched DataFrame
            type_top = top_assets_df[top_assets_df["asset_type"] == asset_type]
            sheet_name = asset_type[:31]  # Excel sheet names max 31 chars
            if not type_top.empty:
                type_top.to_excel(writer, sheet_name=sheet_name, index=False)

        # Write full asset volume summary (basic info only)
        summary_cols = ["asset", "asset_type", "volume_24h", "num_chains", "total_markets"]
        asset_volumes[summary_cols].to_excel(writer, sheet_name="All Assets", index=False)

    print(f"\n  -> {excel_path}")

    # Also save CSV for convenience
    csv_path = os.path.join(OUTPUT_DIR, f"top{top_n}_assets_by_type.csv")
    top_assets_df.to_csv(csv_path, index=False)
    print(f"  -> {csv_path}")

    return top_assets_df




# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════


def main():
    print("\n" + "=" * 70)
    print("COMPREHENSIVE MULTI-CHAIN PERPETUALS ANALYSIS - PHASE 1A")
    print("=" * 70)

    # Step 1: Collect all markets
    all_markets = step_1_collect_all_markets()

    # Step 2: Classify assets (need to collect markets first)
    # Re-fetch for classification (to get full DataFrames)
    hl_client = HyperliquidClient()
    edgex_client = EdgeXClient()
    zkl_client = ZkLighterClient()

    hl_markets_df = hl_markets.get_all_markets(hl_client)
    edgex_markets_df = edgex_markets.get_markets(edgex_client)
    zkl_markets_df = zkl_markets.get_markets(zkl_client)

    classification_df = step_2_classify_assets(hl_markets_df, edgex_markets_df, zkl_markets_df)

    # Step 3: Get market snapshots
    snapshots = step_3_market_snapshots(classification_df)

    # Step 4: Top assets by type
    top_assets = step_4_top_assets_by_type(snapshots, classification_df, top_n=5)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\n  Total markets: {len(all_markets)} ({(~all_markets['is_delisted']).sum()} active)")
    print(f"  Total assets classified: {len(classification_df)}")
    print(f"  Markets with snapshots: {len(snapshots)}")
    print(f"  Total 24h volume: ${snapshots['volume_24h'].sum():,.2f}")
    print(f"  Top assets tracked: {len(top_assets)}")
    print(f"  Asset types: {len(classification_df['asset_type'].dropna().unique())}")

    print("\n" + "=" * 70)
    print("DONE - All output files saved to:", OUTPUT_DIR)
    print("=" * 70)


if __name__ == "__main__":
    main()
