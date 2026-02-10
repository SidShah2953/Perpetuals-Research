"""OHLCV candlestick data fetching for zkLighter perps."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ...common.types import Timeframe

if TYPE_CHECKING:
    from ..client import ZkLighterClient


def _ensure_client(client: ZkLighterClient | None) -> ZkLighterClient:
    if client is None:
        from ..client import ZkLighterClient as _Cls
        return _Cls()
    return client


def _ts_ms(dt: datetime | int | None) -> int:
    """Normalise a datetime or epoch-ms int to epoch-ms."""
    if dt is None:
        return int(time.time() * 1000)
    if isinstance(dt, (int, float)):
        return int(dt)
    return int(dt.timestamp() * 1000)


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


# Map our Timeframe enum to zkLighter resolution strings
_RESOLUTION_MAP = {
    Timeframe.M1: "1m",
    Timeframe.M5: "5m",
    Timeframe.M15: "15m",
    Timeframe.M30: "30m",
    Timeframe.H1: "1h",
    Timeframe.H4: "4h",
    Timeframe.H12: "12h",
    Timeframe.D1: "1d",
    Timeframe.W1: "1w",
}


def _resolution_str(interval: Timeframe | str) -> str:
    """Convert Timeframe enum or string to zkLighter resolution format."""
    if isinstance(interval, str):
        return interval
    return _RESOLUTION_MAP.get(interval, interval.value)


def _raw_to_df(raw: dict) -> pd.DataFrame:
    """Convert raw candle response into a clean DataFrame."""
    if raw.get("code") != 200:
        raise ValueError(f"API error: {raw.get('message', 'Unknown error')}")

    candles = raw.get("c", [])
    if not candles:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    rows = []
    for candle in candles:
        rows.append({
            "time": pd.to_datetime(candle["t"], unit="ms", utc=True),
            "open": float(candle["o"]),
            "high": float(candle["h"]),
            "low": float(candle["l"]),
            "close": float(candle["c"]),
            "volume": float(candle["v"]),  # base volume
            "quote_volume": float(candle["V"]),  # quote volume
        })

    return pd.DataFrame(rows)


# ── Single fetch ─────────────────────────────────────────────────────────────


def get_candles(
    market_id: int,
    interval: Timeframe | str = Timeframe.H1,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    count_back: int = 5000,
    client: ZkLighterClient | None = None,
) -> pd.DataFrame:
    """Fetch candles for a single time window.

    Parameters
    ----------
    market_id : int
        Market identifier (e.g., 0 for ETH, 1 for BTC).
    interval : Timeframe or str
        Candle interval, e.g. ``Timeframe.H1`` or ``"1h"``.
    start : datetime, epoch-ms int, or None
        Start of the window. ``None`` means epoch 0.
    end : datetime, epoch-ms int, or None
        End of the window. ``None`` means *now*.
    count_back : int
        Number of historical candles to fetch (default: 5000).
    """
    client = _ensure_client(client)
    start_ms = _ts_ms(start) if start is not None else 0
    end_ms = _ts_ms(end)
    resolution = _resolution_str(interval)

    raw = client.candles(market_id, resolution, start_ms, end_ms, count_back)
    return _raw_to_df(raw)


# ── Paginated fetch ──────────────────────────────────────────────────────────


def get_candles_range(
    market_id: int,
    interval: Timeframe | str = Timeframe.H1,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    client: ZkLighterClient | None = None,
    max_candles_per_request: int = 5000,
) -> pd.DataFrame:
    """Paginated candle fetch that stitches multiple requests together.

    Use this when the requested range exceeds the per-request candle limit.
    """
    client = _ensure_client(client)
    resolution = _resolution_str(interval)

    # Calculate interval in ms for pagination
    interval_ms_map = {
        "1m": 60_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
        "4h": 14_400_000,
        "12h": 43_200_000,
        "1d": 86_400_000,
        "1w": 604_800_000,
    }
    interval_ms = interval_ms_map.get(resolution, 3_600_000)
    step_ms = interval_ms * max_candles_per_request

    start_ms = _ts_ms(start) if start is not None else 0
    end_ms = _ts_ms(end)

    frames: list[pd.DataFrame] = []
    cursor = start_ms

    while cursor < end_ms:
        window_end = min(cursor + step_ms, end_ms)
        raw = client.candles(market_id, resolution, cursor, window_end, max_candles_per_request)

        if raw.get("code") != 200:
            break

        candles = raw.get("c", [])
        if not candles:
            break

        df = _raw_to_df(raw)
        frames.append(df)

        # advance past the last candle we received
        last_ts = candles[-1]["t"]
        if last_ts <= cursor:
            break  # no progress
        cursor = last_ts + interval_ms

    if not frames:
        return _raw_to_df({"code": 200, "c": []})

    return pd.concat(frames, ignore_index=True).drop_duplicates(subset="time")


# ── Fetch OHLCV by date range ────────────────────────────────────────────────


def fetch_ohlcv(
    market_id: int,
    start: str | datetime,
    bar: Timeframe | str,
    end: str | datetime | None = None,
    client: ZkLighterClient | None = None,
) -> pd.DataFrame:
    """Fetch all OHLCV bars for a market over a date range.

    Automatically paginates when the range exceeds a single API response.

    Parameters
    ----------
    market_id : int
        Market identifier (e.g., 0 for ETH, 1 for BTC).
    start : str or datetime
        Start of the range. Strings like ``"2024-01-01"`` or
        ``"2024-06-15 08:00"`` are accepted.
    bar : Timeframe or str
        Bar / candle duration, e.g. ``Timeframe.H1``, ``"15m"``, ``"1d"``.
    end : str, datetime, or None
        End of the range. Defaults to *now* when ``None``.
    client : ZkLighterClient or None
        Reusable client; one is created automatically if not provided.

    Returns
    -------
    pd.DataFrame
        Columns: ``time, open, high, low, close, volume, quote_volume``.

    Examples
    --------
    >>> from dataCollection.zklighter.perpetuals import candles
    >>> df = candles.fetch_ohlcv(0, "2024-06-01", "1h", end="2024-07-01")  # ETH
    >>> df = candles.fetch_ohlcv(1, "2025-01-01", "15m")  # BTC
    """
    client = _ensure_client(client)

    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end) if end is not None else datetime.now(timezone.utc)

    start_ms = _ts_ms(start_dt)
    end_ms = _ts_ms(end_dt)

    df = get_candles_range(
        market_id=market_id,
        interval=bar,
        start=start_ms,
        end=end_ms,
        client=client,
        max_candles_per_request=5000,
    )

    # trim to the exact requested window
    if not df.empty:
        df = df[(df["time"] >= start_dt) & (df["time"] <= end_dt)]

    return df.reset_index(drop=True)


# ── Inception date ───────────────────────────────────────────────────────────


def get_inception_date(
    market_id: int,
    client: ZkLighterClient | None = None,
) -> datetime | None:
    """Return the earliest date for which candle data exists.

    Parameters
    ----------
    market_id : int
        Market identifier (e.g., 0 for ETH, 1 for BTC).
    """
    client = _ensure_client(client)
    try:
        raw = client.candles(market_id, "1d", 0, _ts_ms(None), 10000)
        if raw.get("code") == 200:
            candles = raw.get("c", [])
            if candles:
                return datetime.fromtimestamp(candles[0]["t"] / 1000, tz=timezone.utc)
    except Exception:
        pass
    return None


def get_inception_dates(
    market_ids: list[int] | None = None,
    client: ZkLighterClient | None = None,
    progress: bool = False,
) -> pd.DataFrame:
    """Batch-fetch inception dates for many markets.

    Parameters
    ----------
    market_ids : list of int or None
        Market IDs to check. ``None`` means all active markets.
    progress : bool
        Print progress to stdout.
    """
    client = _ensure_client(client)

    if market_ids is None:
        from .markets import get_markets
        mkt_df = get_markets(client)
        market_ids = mkt_df["market_id"].tolist()

    rows = []
    total = len(market_ids)
    for i, market_id in enumerate(market_ids, 1):
        inception = get_inception_date(market_id, client=client)
        now = datetime.now(timezone.utc)
        rows.append({
            "market_id": market_id,
            "data_since": inception.strftime("%Y-%m-%d") if inception else None,
            "days_available": (now - inception).days if inception else 0,
        })
        if progress and (i % 5 == 0 or i == total):
            status = rows[-1]["data_since"] or "N/A"
            print(f"  [{i:>4}/{total}] Market {market_id:<6} -> {status}")

    return pd.DataFrame(rows)
