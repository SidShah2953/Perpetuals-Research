"""Spot market metadata, token info, and live snapshots for Hyperliquid."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ..client import HyperliquidClient


def _ensure_client(client: HyperliquidClient | None) -> HyperliquidClient:
    if client is None:
        from ..client import HyperliquidClient as _Cls
        return _Cls()
    return client


# ── Tokens ───────────────────────────────────────────────────────────────────


def get_tokens(client: HyperliquidClient | None = None) -> pd.DataFrame:
    """Metadata for every spot token.

    Returns a DataFrame with columns:
    ``index, name, sz_decimals, wei_decimals, token_id, is_canonical``.
    """
    client = _ensure_client(client)
    data = client.spot_meta()
    rows = []
    for token in data.get("tokens", []):
        rows.append({
            "index": token.get("index"),
            "name": token.get("name", ""),
            "sz_decimals": token.get("szDecimals"),
            "wei_decimals": token.get("weiDecimals"),
            "token_id": token.get("tokenId", ""),
            "is_canonical": token.get("isCanonical", False),
        })
    return pd.DataFrame(rows)


# ── Trading pairs ────────────────────────────────────────────────────────────


def get_pairs(client: HyperliquidClient | None = None) -> pd.DataFrame:
    """Metadata for every spot trading pair.

    Returns a DataFrame with columns:
    ``name, base_token, quote_token, base_index, quote_index,
    pair_index, is_canonical``.

    The ``pair_index`` corresponds to the position in the ``spotMeta``
    universe and is used to build the ``@{index}`` coin identifier for
    candle / order-book requests.
    """
    client = _ensure_client(client)
    data = client.spot_meta()

    # Build token index -> name lookup
    token_map: dict[int, str] = {}
    for token in data.get("tokens", []):
        token_map[token["index"]] = token.get("name", "")

    rows = []
    for pair in data.get("universe", []):
        tokens = pair.get("tokens", [])
        base_idx = tokens[0] if len(tokens) > 0 else None
        quote_idx = tokens[1] if len(tokens) > 1 else None
        rows.append({
            "name": pair.get("name", ""),
            "base_token": token_map.get(base_idx, ""),
            "quote_token": token_map.get(quote_idx, ""),
            "base_index": base_idx,
            "quote_index": quote_idx,
            "pair_index": pair.get("index"),
            "is_canonical": pair.get("isCanonical", False),
        })
    return pd.DataFrame(rows)


def _pair_name_to_coin(pair_name: str, client: HyperliquidClient | None = None) -> str:
    """Resolve a pair name like ``"HYPE/USDC"`` to its API coin identifier.

    Canonical USDC pairs can use their name directly.  Non-canonical pairs
    use the ``@{index}`` format based on their position in the spotMeta
    universe.
    """
    client = _ensure_client(client)
    data = client.spot_meta()
    for pair in data.get("universe", []):
        if pair.get("name") == pair_name:
            if pair.get("isCanonical", False):
                return pair_name
            return f"@{pair['index']}"
    raise ValueError(f"Unknown spot pair: {pair_name!r}")


# ── Live snapshots ───────────────────────────────────────────────────────────


def get_snapshot(client: HyperliquidClient | None = None) -> pd.DataFrame:
    """Live spot market snapshot (mark price, volume, etc.) for all pairs.

    Returns a DataFrame with columns:
    ``name, base_token, quote_token, pair_index, is_canonical,
    mark_price, mid_price, circulating_supply, prev_day_px, volume_24h``.
    """
    client = _ensure_client(client)
    data = client.spot_meta_and_asset_ctxs()
    meta = data[0]
    asset_ctxs = data[1]

    # Build token index -> name lookup
    token_map: dict[int, str] = {}
    for token in meta.get("tokens", []):
        token_map[token["index"]] = token.get("name", "")

    rows = []
    for pair, ctx in zip(meta.get("universe", []), asset_ctxs):
        tokens = pair.get("tokens", [])
        base_idx = tokens[0] if len(tokens) > 0 else None
        quote_idx = tokens[1] if len(tokens) > 1 else None
        rows.append({
            "name": pair.get("name", ""),
            "base_token": token_map.get(base_idx, ""),
            "quote_token": token_map.get(quote_idx, ""),
            "pair_index": pair.get("index"),
            "is_canonical": pair.get("isCanonical", False),
            "mark_price": ctx.get("markPx"),
            "mid_price": ctx.get("midPx"),
            "circulating_supply": ctx.get("circulatingSupply"),
            "prev_day_px": ctx.get("prevDayPx"),
            "volume_24h": ctx.get("dayNtlVlm"),
        })

    df = pd.DataFrame(rows)
    for col in ("mark_price", "mid_price", "circulating_supply", "prev_day_px", "volume_24h"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
