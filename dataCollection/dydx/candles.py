"""OHLCV candlestick data for dYdX v4 perpetuals."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .client import DydxClient


# dYdX v4 resolution strings
RESOLUTIONS = ("1MIN", "5MINS", "15MINS", "30MINS", "1HOUR", "4HOURS", "1DAY")


def _ensure_client(client: DydxClient | None) -> DydxClient:
    if client is None:
        from .client import DydxClient as _Cls
        return _Cls()
    return client


def get_candles(
    ticker: str,
    resolution: str = "1DAY",
    limit: int = 100,
    client: DydxClient | None = None,
) -> pd.DataFrame:
    """Fetch candles for a dYdX perpetual market.

    Parameters
    ----------
    ticker : str
        Market ticker, e.g. ``"BTC-USD"``.
    resolution : str
        One of ``"1MIN"``, ``"5MINS"``, ``"15MINS"``, ``"30MINS"``,
        ``"1HOUR"``, ``"4HOURS"``, ``"1DAY"``.
    limit : int
        Max candles to return (API default 100).
    """
    client = _ensure_client(client)
    data = client.candles(ticker, resolution=resolution, limit=limit)

    raw = dataCollection.get("candles", [])
    if not raw:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(raw)
    if "startedAt" in df.columns:
        df["time"] = pd.to_datetime(df["startedAt"], utc=True)
    for col in ("open", "high", "low", "close", "baseTokenVolume", "usdVolume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.rename(columns={"baseTokenVolume": "volume"})
    return df


def get_inception_date(
    ticker: str,
    client: DydxClient | None = None,
) -> datetime | None:
    """Return the earliest date for which candle data exists, or ``None``."""
    client = _ensure_client(client)
    try:
        data = client.candles(ticker, resolution="1DAY", limit=100)
        candles = dataCollection.get("candles", [])
        if candles:
            oldest = candles[-1]
            started_at = oldest.get("startedAt")
            if started_at:
                return datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except Exception:
        pass
    return None


def get_inception_dates(
    tickers: list[str] | None = None,
    client: DydxClient | None = None,
    progress: bool = False,
) -> pd.DataFrame:
    """Batch-fetch inception dates for many tickers.

    Parameters
    ----------
    tickers : list of str or None
        Tickers to check.  ``None`` means all markets.
    progress : bool
        Print progress to stdout.
    """
    client = _ensure_client(client)

    if tickers is None:
        from .markets import get_markets
        mkt_df = get_markets(client)
        tickers = mkt_df["symbol"].tolist()

    rows = []
    total = len(tickers)
    for i, ticker in enumerate(tickers, 1):
        inception = get_inception_date(ticker, client)
        now = datetime.now(timezone.utc)
        rows.append({
            "ticker": ticker,
            "data_since": inception.strftime("%Y-%m-%d") if inception else None,
            "days_available": (now - inception).days if inception else 0,
        })
        if progress and (i % 10 == 0 or i == total):
            status = rows[-1]["data_since"] or "N/A"
            print(f"  [{i:>4}/{total}] {ticker:<20} -> {status}")

    return pd.DataFrame(rows)
