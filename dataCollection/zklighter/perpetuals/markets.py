"""Market metadata and live market snapshots for zkLighter perps."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ..client import ZkLighterClient


def _ensure_client(client: ZkLighterClient | None) -> ZkLighterClient:
    if client is None:
        from ..client import ZkLighterClient as _Cls
        return _Cls()
    return client


# ── Market metadata ──────────────────────────────────────────────────────────


def get_markets(client: ZkLighterClient | None = None) -> pd.DataFrame:
    """Metadata for every perpetual market on zkLighter.

    Returns a DataFrame with columns:
    ``market_id, symbol, market_type, base_asset_id, quote_asset_id,
    taker_fee, maker_fee, min_size, status``.
    """
    client = _ensure_client(client)
    data = client.order_books(filter_type="perp")

    if data.get("code") != 200:
        raise ValueError(f"API error: {data.get('message', 'Unknown error')}")

    order_books = data.get("order_books", [])
    rows = []

    for book in order_books:
        rows.append({
            "market_id": book.get("market_id"),
            "symbol": book.get("symbol"),
            "market_type": book.get("market_type"),
            "base_asset_id": book.get("base_asset_id"),
            "quote_asset_id": book.get("quote_asset_id"),
            "taker_fee": float(book.get("taker_fee", 0)),
            "maker_fee": float(book.get("maker_fee", 0)),
            "min_size": book.get("min_size"),
            "status": book.get("status"),
        })

    return pd.DataFrame(rows)


# ── Live snapshots ───────────────────────────────────────────────────────────


def get_snapshot(
    market_id: int | None = None,
    client: ZkLighterClient | None = None,
) -> pd.DataFrame:
    """Live market snapshot (last price, volume, OI, price changes) for zkLighter perps.

    Parameters
    ----------
    market_id : int or None
        Pass a specific market ID to filter, or None for all markets.
    """
    client = _ensure_client(client)
    data = client.order_book_details(market_id=market_id, filter_type="perp")

    if data.get("code") != 200:
        raise ValueError(f"API error: {data.get('message', 'Unknown error')}")

    details = data.get("order_book_details", [])
    rows = []

    for detail in details:
        rows.append({
            "market_id": detail.get("market_id"),
            "symbol": detail.get("symbol"),
            "last_trade_price": float(detail.get("last_trade_price", 0)),
            "daily_trades_count": detail.get("daily_trades_count"),
            "daily_base_volume": float(detail.get("daily_base_token_volume", 0)),
            "daily_quote_volume": float(detail.get("daily_quote_token_volume", 0)),
            "price_change_24h": detail.get("price_change_24h"),
            "open_interest": float(detail.get("open_interest", 0)),
            "index_price": float(detail.get("index_price", 0)) if detail.get("index_price") else None,
            "mark_price": float(detail.get("mark_price", 0)) if detail.get("mark_price") else None,
        })

    return pd.DataFrame(rows)


# ── Exchange statistics ──────────────────────────────────────────────────────


def get_exchange_stats(client: ZkLighterClient | None = None) -> dict:
    """Get exchange-wide statistics including total volumes and trade counts."""
    client = _ensure_client(client)
    data = client.exchange_stats()

    if data.get("code") != 200:
        raise ValueError(f"API error: {data.get('message', 'Unknown error')}")

    return data
