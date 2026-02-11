"""Phase 1B: Multi-asset type OHLCV data collection and aggregation.

Fetches hourly OHLCV data from all chains/DEXs for selected assets across
multiple asset types (commodities, equities, crypto, etc.), aggregates
liquidity, and compares with TradFi data.

Asset selection is driven by CSV files in the `chosen/` folder.
"""

import os
import glob

import pandas as pd

from dataCollection.hyperliquid import HyperliquidClient
from dataCollection.hyperliquid.perpetuals import candles as hl_candles
from dataCollection.edgex import EdgeXClient
from dataCollection.edgex.perpetuals import candles as edgex_candles
from dataCollection.zklighter import ZkLighterClient
from dataCollection.zklighter.perpetuals import candles as zkl_candles
from dataCollection.yfinance import YFinanceClient
from dataCollection.yfinance.spots import candles as yf_candles
from utils import setup_output_directory

OUTPUT_DIR = setup_output_directory()
CHOSEN_DIR = "chosen"
OHLCV_DIR = os.path.join(OUTPUT_DIR, "ohlcv_1d_multiasset")


# ══════════════════════════════════════════════════════════════════════════════
# Asset Selection Loading
# ══════════════════════════════════════════════════════════════════════════════


def load_all_chosen_assets() -> pd.DataFrame:
    """Load and merge all asset selection CSVs from the chosen/ folder.

    Returns a DataFrame with columns:
    - asset: Common asset name (e.g., "BTC", "Gold", "NVDA")
    - coin: The ticker/symbol on the DEX (e.g., "BTC", "GOLD", "NVDA")
    - dex: DEX name (e.g., "Hyperliquid (native)", "xyz", "flx", "cash")
    - yf_ticker: Corresponding yfinance ticker (optional)
    - data_since: Earliest date with data (YYYY-MM-DD)
    - asset_type: Inferred from the CSV filename (e.g., "Traditional Commodity", "Crypto Coin")
    """
    if not os.path.exists(CHOSEN_DIR):
        raise FileNotFoundError(
            f"Chosen directory not found: {CHOSEN_DIR}\n"
            f"Please create a 'chosen/' folder with CSV files for each asset type."
        )

    csv_files = glob.glob(os.path.join(CHOSEN_DIR, "*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {CHOSEN_DIR}\n"
            f"Please add CSV files with columns: asset, coin, dex, yf_ticker, data_since"
        )

    all_selections = []

    print(f"\nLoading asset selections from {CHOSEN_DIR}:")
    for csv_path in csv_files:
        # Extract asset type from filename (e.g., "Traditional Commodity.csv" -> "Traditional Commodity")
        asset_type = os.path.splitext(os.path.basename(csv_path))[0]

        try:
            df = pd.read_csv(csv_path)

            # Skip empty files
            if df.empty:
                print(f"  ⊘ {asset_type}: 0 selections (empty file)")
                continue

            df["asset_type"] = asset_type
            all_selections.append(df)

            n_assets = len(df["asset"].unique())
            n_selections = len(df)
            print(f"  ✓ {asset_type}: {n_assets} assets, {n_selections} DEX selections")

        except pd.errors.EmptyDataError:
            print(f"  ⊘ {asset_type}: 0 selections (empty file)")
            continue

    if not all_selections:
        raise ValueError(f"No valid asset selections found in {CHOSEN_DIR}")

    combined = pd.concat(all_selections, ignore_index=True)

    print(f"\n  Total: {len(combined['asset'].unique())} unique assets, {len(combined)} DEX selections\n")

    return combined


# ══════════════════════════════════════════════════════════════════════════════
# Hyperliquid DEX Resolution
# ══════════════════════════════════════════════════════════════════════════════


_NATIVE_DEX_ALIASES = {"Hyperliquid (native)", "native", "hl", "hyperliquid"}


def resolve_hyperliquid_coin(coin: str, dex: str) -> str:
    """Build the API coin identifier for Hyperliquid DEXs.

    Native DEX coins are bare symbols (e.g., "BTC"), while third-party DEX
    coins are prefixed (e.g., "xyz:GOLD").
    """
    if ":" in coin:
        return coin  # Already fully qualified
    if dex.lower() in {a.lower() for a in _NATIVE_DEX_ALIASES}:
        return coin  # Native DEX, no prefix needed
    return f"{dex}:{coin}"  # Third-party DEX, add prefix


# ══════════════════════════════════════════════════════════════════════════════
# Data Fetching
# ══════════════════════════════════════════════════════════════════════════════


def fetch_selected_data(
    selected: pd.DataFrame,
    hl_client: HyperliquidClient,
    edgex_client: EdgeXClient,
    zkl_client: ZkLighterClient,
    interval: str = "1d",
) -> dict[str, list[pd.DataFrame]]:
    """Fetch daily OHLCV for every selected market, grouped by asset.

    Returns a dict mapping asset name -> list of DataFrames (one per DEX/chain).
    """
    asset_frames: dict[str, list[pd.DataFrame]] = {}

    print(f"Fetching {interval} OHLCV for {len(selected)} market selections\n")

    for i, row in selected.iterrows():
        asset = row["asset"]
        coin = row["coin"]
        dex = row["dex"]
        data_since = row["data_since"]
        asset_type = row["asset_type"]

        print(f"  [{i + 1:>3}/{len(selected)}] {asset:<12} ({asset_type:<25}) on {dex:<20} from {data_since} ... ", end="", flush=True)

        try:
            # Try to determine which platform this is on
            # For now, we'll assume all are on Hyperliquid unless explicitly specified
            # In a production system, you'd have a more sophisticated routing mechanism

            # Assume Hyperliquid for now (since that's where most DEXs are)
            ticker = resolve_hyperliquid_coin(coin, dex)
            df = hl_candles.fetch_ohlcv(ticker, data_since, interval, client=hl_client)

            # Standardize columns
            keep = ["time", "open", "high", "low", "close", "volume", "num_trades"]
            df = df[[c for c in keep if c in df.columns]]

            # Calculate notional volume
            # DEX perpetuals report volume in base asset units (e.g., BTC, ETH)
            # Multiply by price to get USD notional volume
            if "volume" in df.columns and "close" in df.columns:
                df["notional_volume"] = df["volume"] * df["close"]

            # Add metadata
            df["dex"] = dex
            df["asset_type"] = asset_type

            # Save individual file
            os.makedirs(OHLCV_DIR, exist_ok=True)
            safe_asset = asset.replace("/", "_")
            safe_dex = dex.replace("/", "_").replace(":", "_")
            path = os.path.join(OHLCV_DIR, f"{safe_asset}_{safe_dex}.csv")
            df.to_csv(path, index=False)
            print(f"{len(df)} bars -> {os.path.basename(path)}")

            asset_frames.setdefault(asset, []).append(df)

        except Exception as e:
            print(f"ERROR: {e}")

    return asset_frames


def fetch_tradfi(
    selected: pd.DataFrame,
    client: YFinanceClient,
    interval: str = "1d",
) -> dict[str, pd.DataFrame]:
    """Fetch daily OHLCV for each unique yfinance ticker, keyed by asset."""
    asset_yf: dict[str, pd.DataFrame] = {}

    # Get unique yfinance tickers
    yf_assets = selected[selected["yf_ticker"].notna()].groupby("asset").agg(
        yf_ticker=("yf_ticker", "first"),
        earliest=("data_since", "min"),
    )

    if yf_assets.empty:
        print("\n  No yfinance tickers specified. Skipping TradFi data.\n")
        return asset_yf

    print(f"\nFetching {interval} OHLCV for {len(yf_assets)} yfinance underlyings\n")

    for i, (asset, row) in enumerate(yf_assets.iterrows(), 1):
        yf_ticker, earliest = row["yf_ticker"], row["earliest"]

        print(f"  [{i:>2}/{len(yf_assets)}] {yf_ticker:<12} from {earliest} ... ", end="", flush=True)

        try:
            df = yf_candles.fetch_ohlcv(yf_ticker, earliest, interval, client=client)
            keep = ["time", "open", "high", "low", "close", "volume"]
            df = df[[c for c in keep if c in df.columns]]

            # Calculate notional volume
            # For crypto tickers (BTC-USD, ETH-USDT, etc.), yfinance reports volume in USD already
            # For stocks and commodities, volume needs to be multiplied by price
            if "volume" in df.columns and "close" in df.columns:
                is_crypto = any(yf_ticker.upper().endswith(suffix) for suffix in
                               ["-USD", "-USDT", "-BUSD", "-USDC", "USD", "USDT"])

                if is_crypto:
                    # Crypto: volume is already in USD
                    df["notional_volume"] = df["volume"]
                else:
                    # Stocks/Commodities: volume * price = USD
                    df["notional_volume"] = df["volume"] * df["close"]

            safe_ticker = yf_ticker.replace("/", "_")
            path = os.path.join(OHLCV_DIR, f"{safe_ticker}_yf.csv")
            df.to_csv(path, index=False)
            print(f"{len(df)} bars -> {os.path.basename(path)}")

            asset_yf[asset] = df

        except Exception as e:
            print(f"ERROR: {e}")

    return asset_yf


# ══════════════════════════════════════════════════════════════════════════════
# Data Aggregation
# ══════════════════════════════════════════════════════════════════════════════


def aggregate_dexs(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Aggregate multiple DEX DataFrames: mean prices, sum volumes."""
    combined = pd.concat(frames, ignore_index=True)
    combined["time"] = pd.to_datetime(combined["time"], utc=True)

    # Aggregate by time
    agg_dict = {
        "open": "mean",
        "high": "mean",
        "low": "mean",
        "close": "mean",
        "volume": "sum",
    }

    # Include num_trades if available
    if "num_trades" in combined.columns:
        agg_dict["num_trades"] = "sum"

    # Include notional_volume if available
    if "notional_volume" in combined.columns:
        agg_dict["notional_volume"] = "sum"

    agg = combined.groupby("time").agg(agg_dict).sort_index()

    return agg.reset_index()


# ══════════════════════════════════════════════════════════════════════════════
# Excel Export
# ══════════════════════════════════════════════════════════════════════════════


def build_excel(
    asset: str,
    asset_type: str,
    defi: pd.DataFrame,
    tradfi: pd.DataFrame,
    output_dir: str,
) -> str:
    """Join DeFi (multi-DEX aggregate) and TradFi into one Excel file."""
    defi = defi.copy()
    tradfi = tradfi.copy()

    defi["time"] = pd.to_datetime(defi["time"], utc=True)
    if not tradfi.empty:
        tradfi["time"] = pd.to_datetime(tradfi["time"], utc=True)

    # Normalize to daily timestamps (round down to day)
    defi["time"] = defi["time"].dt.floor("D")
    if not tradfi.empty:
        tradfi["time"] = tradfi["time"].dt.floor("D")

    # Prefix columns
    defi_cols = {c: f"defi_{c}" for c in defi.columns if c != "time"}
    defi = defi.rename(columns=defi_cols)

    if not tradfi.empty:
        tradfi_cols = {c: f"tradfi_{c}" for c in tradfi.columns if c != "time"}
        tradfi = tradfi.rename(columns=tradfi_cols)

    # Merge
    if tradfi.empty:
        merged = defi
    else:
        merged = pd.merge(defi, tradfi, on="time", how="outer")

    merged = merged.sort_values("time").reset_index(drop=True)

    # Excel doesn't support tz-aware datetimes
    merged["time"] = merged["time"].dt.tz_localize(None)

    # Create asset type subfolder
    safe_type = asset_type.replace("/", "_")
    type_dir = os.path.join(output_dir, safe_type)
    os.makedirs(type_dir, exist_ok=True)

    safe_asset = asset.replace("/", "_")
    path = os.path.join(type_dir, f"{safe_asset}.xlsx")
    merged.to_excel(path, index=False, sheet_name=asset)
    return path


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════


def main():
    print("\n" + "=" * 80)
    print("MULTI-ASSET TYPE OHLCV DATA COLLECTION - PHASE 1B")
    print("=" * 80)

    # Load all asset selections from chosen/ folder
    selected = load_all_chosen_assets()

    # Initialize clients
    hl_client = HyperliquidClient()
    edgex_client = EdgeXClient()
    zkl_client = ZkLighterClient()
    yf_client = YFinanceClient()
    os.makedirs(OHLCV_DIR, exist_ok=True)

    # Fetch DeFi data
    print("\n" + "=" * 80)
    print("Fetching OHLCV from DEXs")
    print("=" * 80 + "\n")
    asset_frames = fetch_selected_data(selected, hl_client, edgex_client, zkl_client, interval="1d")

    # Fetch TradFi data
    print("\n" + "=" * 80)
    print("Fetching TradFi data (yfinance)")
    print("=" * 80)
    asset_tradfi = fetch_tradfi(selected, yf_client, interval="1d")

    # Aggregate and build Excel files
    print("\n" + "=" * 80)
    print("Building Excel files per asset")
    print("=" * 80 + "\n")

    # Group by asset type for better organization
    asset_types = selected.groupby("asset")["asset_type"].first()

    for asset in sorted(asset_frames):
        defi = aggregate_dexs(asset_frames[asset])
        tradfi = asset_tradfi.get(asset, pd.DataFrame())
        asset_type = asset_types.get(asset, "Unknown")

        n_dexs = len(asset_frames[asset])
        path = build_excel(asset, asset_type, defi, tradfi, OUTPUT_DIR)
        tradfi_bars = len(tradfi) if not tradfi.empty else 0
        print(f"  {asset:<14} ({asset_type:<25}) {n_dexs} DEXs, {len(defi):>6} defi bars, {tradfi_bars:>6} tradfi bars -> {os.path.basename(os.path.dirname(path))}/{os.path.basename(path)}")

    print("\n" + "=" * 80)
    print("DONE")
    print("=" * 80)
    print(f"\nOutput saved to: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
