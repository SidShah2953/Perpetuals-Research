"""Multi-chain asset classification for perpetuals.

Extends Hyperliquid's classification system to work across all chains.
"""

from __future__ import annotations

import pandas as pd

from ..hyperliquid.perpetuals import classification as hl_classification


def classify_multichain(
    hl_markets: pd.DataFrame,
    edgex_markets: pd.DataFrame,
    zkl_markets: pd.DataFrame,
) -> pd.DataFrame:
    """Classify assets across all chains.

    Uses Hyperliquid's classification as the base, then maps edgeX and zkLighter
    assets to the same categories.

    Parameters
    ----------
    hl_markets : pd.DataFrame
        Hyperliquid markets (from get_all_markets)
    edgex_markets : pd.DataFrame
        edgeX markets (from get_markets)
    zkl_markets : pd.DataFrame
        zkLighter markets (from get_markets)

    Returns
    -------
    pd.DataFrame
        Classification with columns: asset, asset_type, chains, dex_names,
        total_markets, hl_markets, edgex_markets, zkl_markets
    """
    # Get Hyperliquid classification
    hl_clf = hl_classification.classify_all()

    # Create asset -> type mapping
    asset_type_map = dict(zip(hl_clf["asset"], hl_clf["asset_type"]))

    # Collect all unique assets across all chains
    all_assets = {}

    # Process Hyperliquid
    for _, row in hl_markets.iterrows():
        asset = row["base_symbol"]
        if asset not in all_assets:
            all_assets[asset] = {
                "asset": asset,
                "asset_type": asset_type_map.get(asset, "Crypto Coin"),  # default to crypto
                "chains": set(),
                "dexs": set(),
                "hl_dexs": set(),
                "edgex_markets": [],
                "zkl_markets": [],
            }
        all_assets[asset]["chains"].add("hyperliquid")
        all_assets[asset]["dexs"].add(row["dex"])
        all_assets[asset]["hl_dexs"].add(row["dex"])

    # Process edgeX
    for _, row in edgex_markets[edgex_markets["enable_trade"]].iterrows():
        # Extract base symbol from contract_name (e.g., "BTCUSD" -> "BTC")
        import re
        contract_name = row["contract_name"]
        asset = re.sub(r"(USD|USDT|2USD)$", "", contract_name)
        if asset not in all_assets:
            # New asset not in Hyperliquid - classify as crypto by default
            all_assets[asset] = {
                "asset": asset,
                "asset_type": asset_type_map.get(asset, "Crypto Coin"),
                "chains": set(),
                "dexs": set(),
                "hl_dexs": set(),
                "edgex_markets": [],
                "zkl_markets": [],
            }
        all_assets[asset]["chains"].add("edgex")
        all_assets[asset]["dexs"].add("edgeX")
        all_assets[asset]["edgex_markets"].append(row["contract_id"])

    # Process zkLighter
    for _, row in zkl_markets[zkl_markets["status"] == "active"].iterrows():
        asset = row["symbol"]
        if asset not in all_assets:
            all_assets[asset] = {
                "asset": asset,
                "asset_type": asset_type_map.get(asset, "Crypto Coin"),
                "chains": set(),
                "dexs": set(),
                "hl_dexs": set(),
                "edgex_markets": [],
                "zkl_markets": [],
            }
        all_assets[asset]["chains"].add("zklighter")
        all_assets[asset]["dexs"].add("zkLighter")
        all_assets[asset]["zkl_markets"].append(str(row["market_id"]))

    # Build final DataFrame
    rows = []
    for asset_data in all_assets.values():
        rows.append({
            "asset": asset_data["asset"],
            "asset_type": asset_data["asset_type"],
            "chains": ", ".join(sorted(asset_data["chains"])),
            "num_chains": len(asset_data["chains"]),
            "dex_names": ", ".join(sorted(asset_data["dexs"])),
            "hl_dexs": ", ".join(sorted(asset_data["hl_dexs"])) if asset_data["hl_dexs"] else "",
            "edgex_contracts": ", ".join(asset_data["edgex_markets"]) if asset_data["edgex_markets"] else "",
            "zkl_markets": ", ".join(asset_data["zkl_markets"]) if asset_data["zkl_markets"] else "",
            "total_markets": (
                len(asset_data["hl_dexs"]) +
                len(asset_data["edgex_markets"]) +
                len(asset_data["zkl_markets"])
            ),
        })

    df = pd.DataFrame(rows)
    df = df.sort_values(["asset_type", "num_chains", "asset"], ascending=[True, False, True])
    df = df.reset_index(drop=True)

    return df


def get_asset_summary(classification_df: pd.DataFrame) -> dict:
    """Get summary statistics from classification DataFrame."""
    summary = {
        "total_assets": len(classification_df),
        "by_type": classification_df["asset_type"].value_counts().to_dict(),
        "by_num_chains": classification_df["num_chains"].value_counts().to_dict(),
        "multi_chain": len(classification_df[classification_df["num_chains"] > 1]),
        "single_chain": len(classification_df[classification_df["num_chains"] == 1]),
    }

    # Count by chain
    for chain in ["hyperliquid", "edgex", "zklighter"]:
        summary[f"{chain}_assets"] = len(
            classification_df[classification_df["chains"].str.contains(chain)]
        )

    return summary


def get_assets_by_type(
    classification_df: pd.DataFrame,
    asset_type: str,
) -> pd.DataFrame:
    """Filter classification by asset type."""
    return classification_df[classification_df["asset_type"] == asset_type].copy()


def get_multi_chain_assets(classification_df: pd.DataFrame) -> pd.DataFrame:
    """Get assets available on multiple chains."""
    return classification_df[classification_df["num_chains"] > 1].copy()
