import os

import pandas as pd

from dataCollection.hyperliquid import HyperliquidClient
from dataCollection.hyperliquid.perpetuals import markets, classification, candles
from utils import setup_output_directory

OUTPUT_DIR = setup_output_directory()


def step_1_collect_markets(client: HyperliquidClient) -> pd.DataFrame:
    """Fetch every perp across all DEXs and save to CSV."""
    print("=" * 70)
    print("STEP 1: Collect all perp contracts by DEX")
    print("=" * 70)

    df = markets.get_all_markets(client)
    active = df[~df["is_delisted"]]

    print(f"\n  Total: {len(df)}  ({len(active)} active, {len(df) - len(active)} delisted)")
    for dex in df["dex"].unique():
        dex_slice = df[df["dex"] == dex]
        n_active = (~dex_slice["is_delisted"]).sum()
        print(f"    {dex}: {n_active} active / {len(dex_slice)} total")

    path = os.path.join(OUTPUT_DIR, "hl_all_perps.csv")
    df.to_csv(path, index=False)
    print(f"\n  -> {path}")
    return df


def step_2_classify(client: HyperliquidClient) -> pd.DataFrame:
    """Classify every active perp by asset type and save to CSV."""
    print("\n" + "=" * 70)
    print("STEP 2: Classify perps by asset type")
    print("=" * 70)

    df = classification.classify_all(client=client)

    print()
    for asset_type, group in df.groupby("asset_type"):
        print(f"  {asset_type}: {len(group)}")
    print(f"  Total: {len(df)} unique assets")

    path = os.path.join(OUTPUT_DIR, "hl_asset_classification.csv")
    df.to_csv(path, index=False)
    print(f"\n  -> {path}")
    return df


def step_3_commodity_inception(
    clf_df: pd.DataFrame,
    client: HyperliquidClient,
) -> pd.DataFrame:
    """Get inception dates for every commodity perp on every DEX it trades on."""
    print("\n" + "=" * 70)
    print("STEP 3: Commodity perps - inception dates by DEX")
    print("=" * 70)

    commodities = clf_df[clf_df["asset_type"] == "Traditional Commodity"]

    if commodities.empty:
        print("\n  No commodity perps found.")
        return pd.DataFrame()

    # Build (coin, dex) pairs from the classification table
    pairs = []
    for _, row in commodities.iterrows():
        for dex in row["dex_names"].split(", "):
            pairs.append((row["asset"], dex))

    print(f"\n  {len(commodities)} commodities across {len(pairs)} listings\n")

    rows = []
    for i, (coin, dex) in enumerate(pairs, 1):
        inception = candles.get_inception_date(coin, dex=dex, client=client)
        days = 0
        since = None
        if inception:
            from datetime import datetime, timezone
            days = (datetime.now(timezone.utc) - inception).days
            since = inception.strftime("%Y-%m-%d")

        rows.append({
            "coin": coin,
            "dex": dex,
            "data_since": since,
            "days_available": days,
        })
        status = since or "N/A"
        print(f"  [{i:>3}/{len(pairs)}] {coin:<14} on {dex:<22} -> {status} ({days} days)")

    df = pd.DataFrame(rows)
    df = df.sort_values(["coin", "dex"]).reset_index(drop=True)
    path = os.path.join(OUTPUT_DIR, "hl_commodity_inception.csv")
    df.to_csv(path, index=False)
    print(f"\n  -> {path}")
    return df


def main():
    client = HyperliquidClient()

    step_1_collect_markets(client)
    clf_df = step_2_classify(client)
    step_3_commodity_inception(clf_df, client)

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
