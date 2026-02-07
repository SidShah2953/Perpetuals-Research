"""Market metadata, DEX info, and live market snapshots for Hyperliquid."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .client import HyperliquidClient


def _ensure_client(client: HyperliquidClient | None) -> HyperliquidClient:
    if client is None:
        from .client import HyperliquidClient as _Cls
        return _Cls()
    return client


# ── DEXs ─────────────────────────────────────────────────────────────────────


def get_dexs(client: HyperliquidClient | None = None) -> list[dict]:
    """Return a list of DEX descriptors.

    The native Hyperliquid DEX is represented as
    ``{"name": "Hyperliquid (native)"}``; third-party DEXs include their
    full metadata as returned by the API.
    """
    client = _ensure_client(client)
    raw = client.perp_dexs()
    dexs = []
    for entry in raw:
        if entry is None:
            dexs.append({"name": "Hyperliquid (native)"})
        else:
            dexs.append(entry)
    return dexs


def get_dex_names(client: HyperliquidClient | None = None) -> list[str]:
    """Return a flat list of DEX short-names (e.g. ``["Hyperliquid (native)", "xyz", ...]``)."""
    dexs = get_dexs(client)
    names = []
    for d in dexs:
        if d.get("name") == "Hyperliquid (native)":
            names.append("Hyperliquid (native)")
        else:
            names.append(d.get("name", "unnamed"))
    return names


# ── Market metadata ──────────────────────────────────────────────────────────


def get_markets(client: HyperliquidClient | None = None) -> pd.DataFrame:
    """Metadata for every perp on the **native** DEX.

    Returns a DataFrame with columns:
    ``name, sz_decimals, max_leverage, is_delisted``.
    """
    client = _ensure_client(client)
    data = client.meta()
    rows = []
    for asset in dataCollection.get("universe", []):
        name = asset.get("name", "")
        base = name.split("-")[0] if "-" in name else name
        rows.append({
            "name": name,
            "base_symbol": base,
            "sz_decimals": asset.get("szDecimals"),
            "max_leverage": asset.get("maxLeverage"),
            "is_delisted": asset.get("isDelisted", False),
        })
    return pd.DataFrame(rows)


def get_all_markets(client: HyperliquidClient | None = None) -> pd.DataFrame:
    """Metadata for every perp across **all** DEXs.

    Returns a DataFrame with columns:
    ``dex, name, base_symbol, sz_decimals, max_leverage, is_delisted``.
    """
    client = _ensure_client(client)
    dex_names = get_dex_names(client)
    all_metas = client.all_perp_metas()

    rows = []
    for i, entry in enumerate(all_metas):
        dex_label = dex_names[i] if i < len(dex_names) else f"dex_{i}"
        for asset in entry.get("universe", []):
            raw_name = asset.get("name", "")
            base = raw_name.split(":")[-1]  # strip DEX prefix if present
            rows.append({
                "dex": dex_label,
                "name": raw_name,
                "base_symbol": base,
                "sz_decimals": asset.get("szDecimals"),
                "max_leverage": asset.get("maxLeverage"),
                "is_delisted": asset.get("isDelisted", False),
            })
    return pd.DataFrame(rows)


# ── Live snapshots ───────────────────────────────────────────────────────────


def get_snapshot(
    dex: str | None = None,
    client: HyperliquidClient | None = None,
) -> pd.DataFrame:
    """Live market snapshot (mark price, funding, OI, volume) for one DEX.

    Parameters
    ----------
    dex : str or None
        Pass ``None`` for the native DEX, or a third-party DEX name.
    """
    client = _ensure_client(client)
    data = client.meta_and_asset_ctxs(dex=dex)
    meta = data[0]
    asset_ctxs = data[1]

    rows = []
    for asset, ctx in zip(meta["universe"], asset_ctxs):
        name = asset.get("name", "")
        base = name.split("-")[0] if "-" in name else name
        rows.append({
            "name": name,
            "base_symbol": base,
            "sz_decimals": asset.get("szDecimals"),
            "max_leverage": asset.get("maxLeverage"),
            "is_delisted": asset.get("isDelisted", False),
            "mark_price": ctx.get("markPx"),
            "funding_rate": ctx.get("funding"),
            "open_interest": ctx.get("openInterest"),
            "prev_day_px": ctx.get("prevDayPx"),
            "volume_24h": ctx.get("dayNtlVlm"),
        })
    return pd.DataFrame(rows)
