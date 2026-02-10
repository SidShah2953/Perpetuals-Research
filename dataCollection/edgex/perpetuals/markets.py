"""Market metadata and live market snapshots for edgeX perps."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ..client import EdgeXClient


def _ensure_client(client: EdgeXClient | None) -> EdgeXClient:
    if client is None:
        from ..client import EdgeXClient as _Cls
        return _Cls()
    return client


# ── Market metadata ──────────────────────────────────────────────────────────


def get_markets(client: EdgeXClient | None = None) -> pd.DataFrame:
    """Metadata for every perpetual contract on edgeX.

    Returns a DataFrame with columns:
    ``contract_id, contract_name, base_coin, quote_coin, min_order_size,
    max_order_size, taker_fee_rate, maker_fee_rate, max_leverage``.
    """
    client = _ensure_client(client)
    data = client.metadata()

    if data.get("code") != "SUCCESS":
        raise ValueError(f"API error: {data.get('msg', 'Unknown error')}")

    metadata = data.get("data", {})
    contracts = metadata.get("contractList", [])

    rows = []
    for contract in contracts:
        rows.append({
            "contract_id": contract.get("contractId"),
            "contract_name": contract.get("contractName"),
            "base_coin": contract.get("baseCoinId"),
            "quote_coin": contract.get("quoteCoinId"),
            "min_order_size": float(contract.get("minOrderSize", 0)),
            "max_order_size": float(contract.get("maxOrderSize", 0)),
            "taker_fee_rate": float(contract.get("defaultTakerFeeRate", 0)),
            "maker_fee_rate": float(contract.get("defaultMakerFeeRate", 0)),
            "max_leverage": int(contract.get("defaultLeverage", 0)),
            "enable_trade": contract.get("enableTrade", False),
            "enable_display": contract.get("enableDisplay", False),
            "enable_open_position": contract.get("enableOpenPosition", False),
        })

    return pd.DataFrame(rows)


def get_coins(client: EdgeXClient | None = None) -> pd.DataFrame:
    """Get list of all coins/tokens on edgeX.

    Returns a DataFrame with columns:
    ``coin_id, coin_name, step_size, icon_url``.
    """
    client = _ensure_client(client)
    data = client.metadata()

    if data.get("code") != "SUCCESS":
        raise ValueError(f"API error: {data.get('msg', 'Unknown error')}")

    metadata = data.get("data", {})
    coins = metadata.get("coinList", [])

    rows = []
    for coin in coins:
        rows.append({
            "coin_id": coin.get("coinId"),
            "coin_name": coin.get("coinName"),
            "step_size": coin.get("stepSize"),
            "icon_url": coin.get("iconUrl"),
        })

    return pd.DataFrame(rows)


# ── Live snapshots ───────────────────────────────────────────────────────────


def get_snapshot(
    contract_id: str | None = None,
    client: EdgeXClient | None = None,
) -> pd.DataFrame:
    """Live market snapshot (last price, volume, OI, funding) for edgeX perps.

    Parameters
    ----------
    contract_id : str or None
        Pass a specific contract ID to filter, or None for all contracts.
    """
    client = _ensure_client(client)
    data = client.ticker(contract_id=contract_id)

    if data.get("code") != "SUCCESS":
        raise ValueError(f"API error: {data.get('msg', 'Unknown error')}")

    tickers = data.get("data", [])
    if not isinstance(tickers, list):
        tickers = [tickers]

    rows = []
    for ticker in tickers:
        rows.append({
            "contract_id": ticker.get("contractId"),
            "contract_name": ticker.get("contractName"),
            "last_price": float(ticker.get("lastPrice", 0)),
            "open_price": float(ticker.get("open", 0)),
            "high_price": float(ticker.get("high", 0)),
            "low_price": float(ticker.get("low", 0)),
            "close_price": float(ticker.get("close", 0)),
            "price_change": float(ticker.get("priceChange", 0)),
            "price_change_percent": float(ticker.get("priceChangePercent", 0)),
            "trades_24h": ticker.get("trades"),
            "volume_24h": float(ticker.get("size", 0)),
            "value_24h": float(ticker.get("value", 0)),
            "funding_rate": float(ticker.get("fundingRate", 0)),
            "open_interest": float(ticker.get("openInterest", 0)),
            "index_price": float(ticker.get("indexPrice", 0)),
            "oracle_price": float(ticker.get("oraclePrice", 0)),
        })

    return pd.DataFrame(rows)
