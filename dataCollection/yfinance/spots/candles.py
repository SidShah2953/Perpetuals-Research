"""OHLCV candlestick data fetching via Yahoo Finance."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ...common.types import Timeframe
from ..client import _yf_interval

if TYPE_CHECKING:
    from ..client import YFinanceClient


def _ensure_client(client: YFinanceClient | None) -> YFinanceClient:
    if client is None:
        from ..client import YFinanceClient as _Cls
        return _Cls()
    return client


def _parse_dt(val: str | datetime) -> datetime:
    """Accept ``"2024-01-15"``, ``"2024-01-15 09:30"`` or a datetime."""
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val
    dt = pd.Timestamp(val).to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise a yfinance history DataFrame to our standard format.

    Converts the DatetimeIndex into a ``time`` column and lowercases all
    column names to match the convention used across the project:
    ``time, open, high, low, close, volume``.
    """
    if df.empty:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    df = df.reset_index()
    # yfinance uses "Date" for daily, "Datetime" for intraday
    time_col = "Datetime" if "Datetime" in df.columns else "Date"
    df = df.rename(columns={time_col: "time"})
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    if df["time"].dt.tz is None:
        df["time"] = df["time"].dt.tz_localize("UTC")
    else:
        df["time"] = df["time"].dt.tz_convert("UTC")
    for col in ("open", "high", "low", "close", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    keep = [c for c in ("time", "open", "high", "low", "close", "volume") if c in df.columns]
    return df[keep].reset_index(drop=True)


# ── Single fetch ─────────────────────────────────────────────────────────────


def get_candles(
    symbol: str,
    interval: Timeframe | str = Timeframe.D1,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    client: YFinanceClient | None = None,
) -> pd.DataFrame:
    """Fetch OHLCV candles for a ticker symbol.

    Parameters
    ----------
    symbol : str
        Ticker symbol, e.g. ``"AAPL"``, ``"BTC-USD"``.
    interval : Timeframe or str
        Candle interval, e.g. ``Timeframe.D1`` or ``"1d"``.
    start : str, datetime, or None
        Start of the range.  ``None`` fetches max available history.
    end : str, datetime, or None
        End of the range.  ``None`` means *now*.

    Returns
    -------
    pd.DataFrame
        Columns: ``time, open, high, low, close, volume``.
    """
    client = _ensure_client(client)
    yf_ivl = _yf_interval(interval)

    start_str = str(_parse_dt(start).date()) if start is not None else "1900-01-01"
    end_str = str(_parse_dt(end).date()) if end is not None else None

    raw = client.ticker_history(symbol, start=start_str, end=end_str, interval=yf_ivl)
    return _normalize_df(raw)


# ── Fetch OHLCV by date range ────────────────────────────────────────────────


def fetch_ohlcv(
    symbol: str,
    start: str | datetime,
    bar: Timeframe | str,
    end: str | datetime | None = None,
    client: YFinanceClient | None = None,
) -> pd.DataFrame:
    """Fetch all OHLCV bars for a ticker over a date range.

    Parameters
    ----------
    symbol : str
        Ticker symbol, e.g. ``"AAPL"``, ``"MSFT"``.
    start : str or datetime
        Start of the range.  Strings like ``"2024-01-01"`` or
        ``"2024-06-15 08:00"`` are accepted.
    bar : Timeframe or str
        Bar / candle duration, e.g. ``Timeframe.D1``, ``"1h"``, ``"1d"``.
    end : str, datetime, or None
        End of the range.  Defaults to *now* when ``None``.
    client : YFinanceClient or None
        Reusable client; one is created automatically if not provided.

    Returns
    -------
    pd.DataFrame
        Columns: ``time, open, high, low, close, volume``.

    Examples
    --------
    >>> from dataCollection.yfinance.spots import candles
    >>> df = candles.fetch_ohlcv("AAPL", "2024-06-01", "1d", end="2024-07-01")
    >>> df = candles.fetch_ohlcv("MSFT", "2025-01-01", "1h")
    """
    return get_candles(symbol, interval=bar, start=start, end=end, client=client)


# ── Inception date ───────────────────────────────────────────────────────────


def get_inception_date(
    symbol: str,
    client: YFinanceClient | None = None,
) -> datetime | None:
    """Return the earliest date for which daily data exists on Yahoo Finance.

    Parameters
    ----------
    symbol : str
        Ticker symbol, e.g. ``"AAPL"``.
    """
    client = _ensure_client(client)
    try:
        raw = client.ticker_history(symbol, start="1900-01-01", interval="1d")
        if not raw.empty:
            first = raw.index[0]
            ts = pd.Timestamp(first)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            return ts.to_pydatetime()
    except Exception:
        pass
    return None


def get_inception_dates(
    symbols: list[str],
    client: YFinanceClient | None = None,
    progress: bool = False,
) -> pd.DataFrame:
    """Batch-fetch inception dates for many ticker symbols.

    Parameters
    ----------
    symbols : list of str
        Ticker symbols to check.
    progress : bool
        Print progress to stdout.

    Returns
    -------
    pd.DataFrame
        Columns: ``symbol, data_since, days_available``.
    """
    client = _ensure_client(client)
    rows = []
    total = len(symbols)
    for i, sym in enumerate(symbols, 1):
        inception = get_inception_date(sym, client=client)
        now = datetime.now(timezone.utc)
        rows.append({
            "symbol": sym,
            "data_since": inception.strftime("%Y-%m-%d") if inception else None,
            "days_available": (now - inception).days if inception else 0,
        })
        if progress and (i % 10 == 0 or i == total):
            status = rows[-1]["data_since"] or "N/A"
            print(f"  [{i:>4}/{total}] {sym:<10} -> {status}")

    return pd.DataFrame(rows)
