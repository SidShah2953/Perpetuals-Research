"""Phase 1B: Multi-chain OHLCV data collection and aggregation.

Fetches 1d OHLCV data from all chains for selected assets, aggregates
liquidity across chains, and compares with TradFi data.
"""

import os

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
OHLCV_DIR = os.path.join(OUTPUT_DIR, "ohlcv_1d_multichain")


def load_asset_selection(csv_path: str) -> pd.DataFrame:
    """Load user-selected assets for data collection.

    Expected CSV columns:
    - asset: Common asset name (e.g., "BTC", "ETH")
    - chain: "hyperliquid", "edgex", or "zklighter"
    - chain_market_id: Market/contract identifier on that chain
    - data_since: Earliest date with data (YYYY-MM-DD)
    - yf_ticker: Corresponding yfinance ticker (optional)
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Asset selection file not found: {csv_path}\n"
            f"Please create a CSV with columns: asset, chain, chain_market_id, data_since, yf_ticker"
        )
    return pd.read_csv(csv_path)


def fetch_chain_data(
    selected: pd.DataFrame,
    hl_client: HyperliquidClient,
    edgex_client: EdgeXClient,
    zkl_client: ZkLighterClient,
) -> dict[str, list[pd.DataFrame]]:
    """Fetch 1d OHLCV for every selected market, grouped by asset."""
    asset_frames: dict[str, list[pd.DataFrame]] = {}

    print(f"Fetching 1d OHLCV for {len(selected)} markets across all chains\n")

    for i, row in selected.iterrows():
        asset = row["asset"]
        chain = row["chain"]
        market_id = row["chain_market_id"]
        data_since = row["data_since"]

        print(f"  [{i + 1:>2}/{len(selected)}] {asset:<10} on {chain:<12} from {data_since} ... ", end="", flush=True)

        try:
            if chain == "hyperliquid":
                df = hl_candles.fetch_ohlcv(market_id, data_since, "1d", client=hl_client)
            elif chain == "edgex":
                df = edgex_candles.fetch_ohlcv(market_id, data_since, "1d", client=edgex_client)
            elif chain == "zklighter":
                df = zkl_candles.fetch_ohlcv(int(market_id), data_since, "1d", client=zkl_client)
            else:
                print(f"Unknown chain: {chain}")
                continue

            # Standardize columns
            keep = ["time", "open", "high", "low", "close", "volume"]
            df = df[[c for c in keep if c in df.columns]]

            # Save individual file
            os.makedirs(OHLCV_DIR, exist_ok=True)
            safe_asset = asset.replace("/", "_")
            path = os.path.join(OHLCV_DIR, f"{safe_asset}_{chain}_{market_id}.csv")
            df.to_csv(path, index=False)
            print(f"{len(df)} bars -> {os.path.basename(path)}")

            asset_frames.setdefault(asset, []).append(df)

        except Exception as e:
            print(f"ERROR: {e}")

    return asset_frames


def aggregate_chains(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Aggregate multiple chain DataFrames: mean prices, sum volumes."""
    combined = pd.concat(frames, ignore_index=True)
    combined["time"] = pd.to_datetime(combined["time"], utc=True)

    agg = combined.groupby("time").agg(
        open=("open", "mean"),
        high=("high", "mean"),
        low=("low", "mean"),
        close=("close", "mean"),
        volume=("volume", "sum"),
    ).sort_index()

    return agg.reset_index()


def fetch_tradfi(
    selected: pd.DataFrame,
    client: YFinanceClient,
) -> dict[str, pd.DataFrame]:
    """Fetch 1d OHLCV for each unique yfinance ticker, keyed by asset."""
    asset_yf: dict[str, pd.DataFrame] = {}

    # Get unique yfinance tickers
    yf_assets = selected[selected["yf_ticker"].notna()].groupby("asset").agg(
        yf_ticker=("yf_ticker", "first"),
        earliest=("data_since", "min"),
    )

    if yf_assets.empty:
        print("\n  No yfinance tickers specified. Skipping TradFi data.\n")
        return asset_yf

    print(f"\nFetching 1d OHLCV for {len(yf_assets)} yfinance underlyings\n")

    for i, (asset, row) in enumerate(yf_assets.iterrows(), 1):
        yf_ticker, earliest = row["yf_ticker"], row["earliest"]

        print(f"  [{i:>2}/{len(yf_assets)}] {yf_ticker:<10} from {earliest} ... ", end="", flush=True)

        try:
            df = yf_candles.fetch_ohlcv(yf_ticker, earliest, "1d", client=client)
            keep = ["time", "open", "high", "low", "close", "volume"]
            df = df[[c for c in keep if c in df.columns]]

            safe_ticker = yf_ticker.replace("/", "_")
            path = os.path.join(OHLCV_DIR, f"{safe_ticker}.csv")
            df.to_csv(path, index=False)
            print(f"{len(df)} bars -> {os.path.basename(path)}")

            asset_yf[asset] = df

        except Exception as e:
            print(f"ERROR: {e}")

    return asset_yf


def build_excel(
    asset: str,
    defi: pd.DataFrame,
    tradfi: pd.DataFrame,
    output_dir: str,
) -> str:
    """Join DeFi (multi-chain aggregate) and TradFi into one Excel file."""
    defi = defi.copy()
    tradfi = tradfi.copy()

    defi["time"] = pd.to_datetime(defi["time"], utc=True)
    if not tradfi.empty:
        tradfi["time"] = pd.to_datetime(tradfi["time"], utc=True)

    # Normalize to date only (remove time component)
    defi["time"] = defi["time"].dt.normalize()
    if not tradfi.empty:
        tradfi["time"] = tradfi["time"].dt.normalize()

    # Prefix columns
    defi = defi.rename(columns={c: f"defi_{c}" for c in defi.columns if c != "time"})
    if not tradfi.empty:
        tradfi = tradfi.rename(columns={c: f"tradfi_{c}" for c in tradfi.columns if c != "time"})

    # Merge
    if tradfi.empty:
        merged = defi
    else:
        merged = pd.merge(defi, tradfi, on="time", how="outer")

    merged = merged.sort_values("time").reset_index(drop=True)

    # Excel doesn't support tz-aware datetimes
    merged["time"] = merged["time"].dt.tz_localize(None)

    safe_asset = asset.replace("/", "_")
    path = os.path.join(output_dir, f"{safe_asset}.xlsx")
    merged.to_excel(path, index=False, sheet_name=asset)
    return path


def main():
    print("\n" + "=" * 70)
    print("MULTI-CHAIN OHLCV DATA COLLECTION - PHASE 1B")
    print("=" * 70)

    # Load asset selection
    # You can create this CSV manually or use the output from phase1A_multichain
    selection_file = "selected_assets_multichain.csv"
    if not os.path.exists(selection_file):
        print(f"\nError: {selection_file} not found.")
        print("Please create a CSV with columns: asset, chain, chain_market_id, data_since, yf_ticker")
        print("\nExample:")
        print("  asset,chain,chain_market_id,data_since,yf_ticker")
        print("  BTC,hyperliquid,BTC,2024-01-01,BTC-USD")
        print("  BTC,edgex,10000001,2024-01-01,BTC-USD")
        print("  BTC,zklighter,1,2024-01-01,BTC-USD")
        return

    selected = load_asset_selection(selection_file)

    # Initialize clients
    hl_client = HyperliquidClient()
    edgex_client = EdgeXClient()
    zkl_client = ZkLighterClient()
    yf_client = YFinanceClient()
    os.makedirs(OHLCV_DIR, exist_ok=True)

    # 1. Fetch chain data
    print("\n" + "=" * 70)
    print("Fetching OHLCV from all chains")
    print("=" * 70 + "\n")
    asset_frames = fetch_chain_data(selected, hl_client, edgex_client, zkl_client)

    # 2. Fetch TradFi data
    print("\n" + "=" * 70)
    print("Fetching TradFi data (yfinance)")
    print("=" * 70)
    asset_tradfi = fetch_tradfi(selected, yf_client)

    # 3. Aggregate and build Excel files
    print("\n" + "=" * 70)
    print("Building Excel files per asset")
    print("=" * 70 + "\n")

    for asset in sorted(asset_frames):
        defi = aggregate_chains(asset_frames[asset])
        tradfi = asset_tradfi.get(asset, pd.DataFrame())

        n_chains = len(asset_frames[asset])
        path = build_excel(asset, defi, tradfi, OUTPUT_DIR)
        tradfi_bars = len(tradfi) if not tradfi.empty else 0
        print(f"  {asset:<14} {n_chains} chains, {len(defi)} defi bars, {tradfi_bars} tradfi bars -> {os.path.basename(path)}")

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
