"""Asset classification for Hyperliquid perps across all DEXs."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from ..common.types import AssetType
from .constants import (
    CRYPTO_ONLY_DEXS,
    FIXED_INCOME,
    FOREX,
    INDICES,
    SECTOR_BASKETS,
    TRADITIONAL_COMMODITIES,
    TRADITIONAL_EQUITIES,
)

if TYPE_CHECKING:
    from .client import HyperliquidClient


def _ensure_client(client: HyperliquidClient | None) -> HyperliquidClient:
    if client is None:
        from .client import HyperliquidClient as _Cls
        return _Cls()
    return client


def classify_asset(base_name: str, dex: str) -> AssetType:
    """Classify an asset given its base name and the DEX it appears on."""
    if dex in CRYPTO_ONLY_DEXS:
        return AssetType.CRYPTO
    if base_name in TRADITIONAL_EQUITIES:
        return AssetType.EQUITY
    if base_name in TRADITIONAL_COMMODITIES:
        return AssetType.COMMODITY
    if base_name in INDICES:
        return AssetType.INDEX
    if base_name in FIXED_INCOME:
        return AssetType.FIXED_INCOME
    if base_name in FOREX:
        return AssetType.FOREX
    if base_name in SECTOR_BASKETS:
        return AssetType.SECTOR_BASKET
    return AssetType.CRYPTO


def strip_dex_prefix(name: str) -> str:
    """Strip the DEX prefix from an asset name (e.g. ``"xyz:GOLD"`` -> ``"GOLD"``)."""
    return name.split(":")[-1]


def classify_all(
    active_only: bool = True,
    client: HyperliquidClient | None = None,
) -> pd.DataFrame:
    """Classify every Hyperliquid perp across all DEXs.

    Returns a DataFrame with columns:
    ``asset, asset_type, num_dexs, dex_names``.

    When an asset appears on both crypto-only and TradFi DEXs the TradFi
    classification takes priority.
    """
    client = _ensure_client(client)
    from .markets import get_dex_names

    dex_names = get_dex_names(client)
    all_metas = client.all_perp_metas()

    # {base_name -> {dexs: set, raw_names: set}}
    asset_map: dict[str, dict] = {}
    for i, entry in enumerate(all_metas):
        dex = dex_names[i] if i < len(dex_names) else f"dex_{i}"
        for asset in entry.get("universe", []):
            if active_only and asset.get("isDelisted", False):
                continue
            raw_name = asset["name"]
            base = strip_dex_prefix(raw_name)
            if base not in asset_map:
                asset_map[base] = {"dexs": set(), "raw_names": set()}
            asset_map[base]["dexs"].add(dex)
            asset_map[base]["raw_names"].add(raw_name)

    rows = []
    for base, info in sorted(asset_map.items()):
        non_crypto_dexs = info["dexs"] - CRYPTO_ONLY_DEXS
        classification_dex = (
            next(iter(non_crypto_dexs)) if non_crypto_dexs else next(iter(info["dexs"]))
        )
        asset_type = classify_asset(base, classification_dex)
        rows.append({
            "asset": base,
            "asset_type": asset_type.value,
            "num_dexs": len(info["dexs"]),
            "dex_names": ", ".join(sorted(info["dexs"])),
        })

    df = pd.DataFrame(rows)
    type_order = {t.value: i for i, t in enumerate(AssetType)}
    df["_sort"] = df["asset_type"].map(type_order)
    df = df.sort_values(["_sort", "asset"]).drop(columns="_sort").reset_index(drop=True)
    return df
