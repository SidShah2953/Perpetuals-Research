"""Market metadata and live snapshots via Yahoo Finance."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ..client import YFinanceClient


def _ensure_client(client: YFinanceClient | None) -> YFinanceClient:
    if client is None:
        from ..client import YFinanceClient as _Cls
        return _Cls()
    return client


# ── Market metadata ──────────────────────────────────────────────────────────


def get_markets(
    symbols: list[str],
    client: YFinanceClient | None = None,
) -> pd.DataFrame:
    """Metadata for a list of ticker symbols.

    Parameters
    ----------
    symbols : list of str
        Ticker symbols, e.g. ``["AAPL", "MSFT", "GOOGL"]``.

    Returns a DataFrame with columns:
    ``symbol, name, exchange, currency, market_cap, sector, industry, quote_type``.
    """
    client = _ensure_client(client)
    rows = []
    for sym in symbols:
        try:
            info = client.ticker_info(sym)
        except Exception:
            info = {}
        rows.append({
            "symbol": sym,
            "name": info.get("longName") or info.get("shortName", ""),
            "exchange": info.get("exchange", ""),
            "currency": info.get("currency", ""),
            "market_cap": info.get("marketCap"),
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "quote_type": info.get("quoteType", ""),
        })
    df = pd.DataFrame(rows)
    if "market_cap" in df.columns:
        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    return df


# ── Live snapshots ───────────────────────────────────────────────────────────


def get_snapshot(
    symbols: list[str],
    client: YFinanceClient | None = None,
) -> pd.DataFrame:
    """Live price snapshot for a list of ticker symbols.

    Parameters
    ----------
    symbols : list of str
        Ticker symbols, e.g. ``["AAPL", "MSFT"]``.

    Returns a DataFrame with columns:
    ``symbol, price, prev_close, open, day_high, day_low, volume, market_cap``.
    """
    client = _ensure_client(client)
    rows = []
    for sym in symbols:
        try:
            info = client.ticker_info(sym)
        except Exception:
            info = {}
        rows.append({
            "symbol": sym,
            "price": info.get("currentPrice") or info.get("regularMarketPrice"),
            "prev_close": info.get("previousClose") or info.get("regularMarketPreviousClose"),
            "open": info.get("open") or info.get("regularMarketOpen"),
            "day_high": info.get("dayHigh") or info.get("regularMarketDayHigh"),
            "day_low": info.get("dayLow") or info.get("regularMarketDayLow"),
            "volume": info.get("volume") or info.get("regularMarketVolume"),
            "market_cap": info.get("marketCap"),
        })
    df = pd.DataFrame(rows)
    for col in ("price", "prev_close", "open", "day_high", "day_low", "volume", "market_cap"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
