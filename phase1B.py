"""Fetch 1h OHLCV data, aggregate across DEXs, and join with TradFi data per asset."""

import os

import pandas as pd

from dataCollection.hyperliquid import HyperliquidClient
from dataCollection.hyperliquid.perpetuals import candles as hl_candles
from dataCollection.yfinance import YFinanceClient
from dataCollection.yfinance.spots import candles as yf_candles
from utils import setup_output_directory

OUTPUT_DIR = setup_output_directory()
OHLCV_DIR = os.path.join(OUTPUT_DIR, "ohlcv_1h")


def fetch_all_dex_data(chosen: pd.DataFrame, client: HyperliquidClient) -> dict[str, list[pd.DataFrame]]:
    """Fetch 1h OHLCV for every coin/dex row, grouped by asset."""
    asset_frames: dict[str, list[pd.DataFrame]] = {}

    print(f"Fetching 1h OHLCV for {len(chosen)} coin/dex pairs (Hyperliquid)\n")

    for i, row in chosen.iterrows():
        asset, coin, dex, data_since = row["asset"], row["coin"], row["dex"], row["data_since"]
        ticker = f"{dex}:{coin}"

        print(f"  [{i + 1:>2}/{len(chosen)}] {coin:<10} on {dex:<6} from {data_since} ... ", end="", flush=True)

        df = hl_candles.fetch_ohlcv(ticker, data_since, "1h", client=client)

        # keep only the columns we need
        keep = ["time", "open", "high", "low", "close", "volume", "num_trades"]
        df = df[[c for c in keep if c in df.columns]]

        # save individual file
        os.makedirs(OHLCV_DIR, exist_ok=True)
        path = os.path.join(OHLCV_DIR, f"{coin}_{dex}.csv")
        df.to_csv(path, index=False)
        print(f"{len(df)} bars -> {path}")

        asset_frames.setdefault(asset, []).append(df)

    return asset_frames


def aggregate_dex(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Aggregate multiple DEX DataFrames into one: mean prices, sum volumes."""
    combined = pd.concat(frames, ignore_index=True)
    combined["time"] = pd.to_datetime(combined["time"], utc=True)

    agg = combined.groupby("time").agg(
        open=("open", "mean"),
        high=("high", "mean"),
        low=("low", "mean"),
        close=("close", "mean"),
        volume=("volume", "sum"),
        num_trades=("num_trades", "sum"),
    ).sort_index()

    return agg.reset_index()


def fetch_tradfi(chosen: pd.DataFrame, client: YFinanceClient) -> dict[str, pd.DataFrame]:
    """Fetch 1h OHLCV for each unique yfinance ticker, keyed by asset."""
    asset_yf: dict[str, pd.DataFrame] = {}

    assets = chosen.groupby("asset").agg(
        yf_ticker=("yf_ticker", "first"),
        earliest=("data_since", "min"),
    )

    print(f"\nFetching 1h OHLCV for {len(assets)} yfinance underlyings\n")

    for i, (asset, row) in enumerate(assets.iterrows(), 1):
        yf_ticker, earliest = row["yf_ticker"], row["earliest"]

        print(f"  [{i:>2}/{len(assets)}] {yf_ticker:<10} from {earliest} ... ", end="", flush=True)

        df = yf_candles.fetch_ohlcv(yf_ticker, earliest, "1h", client=client)
        keep = ["time", "open", "high", "low", "close", "volume"]
        df = df[[c for c in keep if c in df.columns]]

        path = os.path.join(OHLCV_DIR, f"{yf_ticker}.csv")
        df.to_csv(path, index=False)
        print(f"{len(df)} bars -> {path}")

        asset_yf[asset] = df

    return asset_yf


def build_excel(
    asset: str,
    defi: pd.DataFrame,
    tradfi: pd.DataFrame,
    output_dir: str,
) -> str:
    """Join DeFi and TradFi into one Excel file with prefixed columns."""
    defi = defi.copy()
    tradfi = tradfi.copy()

    defi["time"] = pd.to_datetime(defi["time"], utc=True)
    tradfi["time"] = pd.to_datetime(tradfi["time"], utc=True)

    # prefix columns
    defi = defi.rename(columns={c: f"defi_{c}" for c in defi.columns if c != "time"})
    tradfi = tradfi.rename(columns={c: f"tradfi_{c}" for c in tradfi.columns if c != "time"})

    merged = pd.merge(defi, tradfi, on="time", how="outer").sort_values("time").reset_index(drop=True)

    # Excel doesn't support tz-aware datetimes
    merged["time"] = merged["time"].dt.tz_localize(None)

    path = os.path.join(output_dir, f"{asset}.xlsx")
    merged.to_excel(path, index=False, sheet_name=asset)
    return path


def main():
    chosen = pd.read_csv("chosen.csv")
    hl_client = HyperliquidClient()
    yf_client = YFinanceClient()
    os.makedirs(OHLCV_DIR, exist_ok=True)

    # 1. Fetch & save individual DEX files, grouped by asset
    asset_frames = fetch_all_dex_data(chosen, hl_client)

    # 2. Fetch & save yfinance underlyings, keyed by asset
    asset_tradfi = fetch_tradfi(chosen, yf_client)

    # 3. Aggregate DEXs and build Excel per asset
    print(f"\nBuilding Excel files per asset\n")

    for asset in sorted(asset_frames):
        defi = aggregate_dex(asset_frames[asset])
        tradfi = asset_tradfi.get(asset, pd.DataFrame())

        n_dexs = len(asset_frames[asset])
        path = build_excel(asset, defi, tradfi, OUTPUT_DIR)
        print(f"  {asset:<14} {n_dexs} DEXs aggregated, {len(defi)} defi bars, {len(tradfi)} tradfi bars -> {path}")

    print(f"\nDone.")


if __name__ == "__main__":
    main()
