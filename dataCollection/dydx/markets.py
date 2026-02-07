"""Market metadata for dYdX v4 perpetuals."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .client import DydxClient


def _ensure_client(client: DydxClient | None) -> DydxClient:
    if client is None:
        from .client import DydxClient as _Cls
        return _Cls()
    return client


def get_markets(client: DydxClient | None = None) -> pd.DataFrame:
    """Fetch all perpetual markets from dYdX v4.

    Returns a DataFrame with columns:
    ``symbol, base_asset, quote_asset, status, initial_margin_fraction``.
    """
    client = _ensure_client(client)
    data = client.perpetual_markets()

    rows = []
    for symbol, info in dataCollection.get("markets", {}).items():
        rows.append({
            "symbol": symbol,
            "base_asset": info.get("baseAsset", "N/A"),
            "quote_asset": info.get("quoteAsset", "N/A"),
            "status": info.get("status", "N/A"),
            "initial_margin_fraction": info.get("initialMarginFraction", "N/A"),
        })
    return pd.DataFrame(rows)
