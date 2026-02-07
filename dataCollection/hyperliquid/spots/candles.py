"""OHLCV candlestick data fetching for Hyperliquid spot pairs."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ...common.types import Timeframe, TIMEFRAME_MS

if TYPE_CHECKING:
    from ..client import HyperliquidClient


def _ensure_client(client: HyperliquidClient | None) -> HyperliquidClient:
    if client is None:
        from ..client import HyperliquidClient as _Cls
        return _Cls()
    return client


def _ts_ms(dt: datetime | int | None) -> int:
    """Normalise a datetime or epoch-ms int to epoch-ms."""
    if dt is None:
        return int(time.time() * 1000)
    if isinstance(dt, (int, float)):
        return int(dt)
    return int(dt.timestamp() * 1000)


def _interval_str(interval: Timeframe | str) -> str:
    """Get the raw interval string from a Timeframe enum or plain string."""
    return interval.value if isinstance(interval, Timeframe) else interval


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


def _raw_to_df(raw: list[dict]) -> pd.DataFrame:
    """Convert raw candle dicts into a clean DataFrame."""
    if not raw:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])
    df = pd.DataFrame(raw)
    df = df.rename(columns={
        "t": "time", "T": "time_close",
        "o": "open", "h": "high", "l": "low", "c": "close",
        "v": "volume", "n": "num_trades",
    })
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    for col in ("open", "high", "low", "close", "volume", "num_trades"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _resolve_coin(coin: str, client: HyperliquidClient | None = None) -> str:
    """Resolve a spot coin identifier for the API.

    Accepts:
    - ``"@123"`` — already in API format, returned as-is
    - ``"HYPE/USDC"`` — pair name, resolved via ``spotMeta``

    Canonical pairs (e.g. ``"PURR/USDC"``) use their name directly;
    non-canonical pairs use the ``@{index}`` format.
    """
    if coin.startswith("@"):
        return coin
    # Treat as pair name — resolve via markets
    from .markets import _pair_name_to_coin
    return _pair_name_to_coin(coin, client)


# ── Single fetch ─────────────────────────────────────────────────────────────


def get_candles(
    coin: str,
    interval: Timeframe | str = Timeframe.H1,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    client: HyperliquidClient | None = None,
) -> pd.DataFrame:
    """Fetch candles for a spot pair in a single time window (API limit applies).

    Parameters
    ----------
    coin : str
        Pair name (e.g. ``"HYPE/USDC"``) or ``@index`` identifier.
    interval : Timeframe or str
        Candle interval, e.g. ``Timeframe.H1`` or ``"1h"``.
    start : datetime, epoch-ms int, or None
        Start of the window.  ``None`` means epoch 0.
    end : datetime, epoch-ms int, or None
        End of the window.  ``None`` means *now*.
    """
    client = _ensure_client(client)
    api_coin = _resolve_coin(coin, client)
    start_ms = _ts_ms(start) if start is not None else 0
    end_ms = _ts_ms(end)
    raw = client.candle_snapshot(api_coin, _interval_str(interval), start_ms, end_ms)
    return _raw_to_df(raw)


# ── Paginated fetch ──────────────────────────────────────────────────────────


def get_candles_range(
    coin: str,
    interval: Timeframe | str = Timeframe.H1,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    client: HyperliquidClient | None = None,
    max_candles_per_request: int = 5000,
) -> pd.DataFrame:
    """Paginated candle fetch that stitches multiple requests together.

    Use this when the requested range exceeds the per-request candle limit.
    """
    client = _ensure_client(client)
    api_coin = _resolve_coin(coin, client)
    tf = Timeframe(interval) if isinstance(interval, str) else interval
    step_ms = TIMEFRAME_MS[tf] * max_candles_per_request

    start_ms = _ts_ms(start) if start is not None else 0
    end_ms = _ts_ms(end)

    frames: list[pd.DataFrame] = []
    cursor = start_ms

    while cursor < end_ms:
        window_end = min(cursor + step_ms, end_ms)
        raw = client.candle_snapshot(api_coin, _interval_str(tf), cursor, window_end)
        if not raw:
            break
        df = _raw_to_df(raw)
        frames.append(df)
        last_ts = raw[-1]["t"]
        if last_ts <= cursor:
            break
        cursor = last_ts + 1

    if not frames:
        return _raw_to_df([])
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset="time")


# ── Fetch OHLCV by date range ────────────────────────────────────────────────


def fetch_ohlcv(
    coin: str,
    start: str | datetime,
    bar: Timeframe | str,
    end: str | datetime | None = None,
    client: HyperliquidClient | None = None,
) -> pd.DataFrame:
    """Fetch all OHLCV bars for a spot pair over a date range.

    Automatically paginates when the range exceeds a single API response.

    Parameters
    ----------
    coin : str
        Pair name (e.g. ``"HYPE/USDC"``) or ``@index`` identifier.
    start : str or datetime
        Start of the range.  Strings like ``"2024-01-01"`` or
        ``"2024-06-15 08:00"`` are accepted.
    bar : Timeframe or str
        Bar / candle duration, e.g. ``Timeframe.H1``, ``"15m"``, ``"1d"``.
    end : str, datetime, or None
        End of the range.  Defaults to *now* when ``None``.
    client : HyperliquidClient or None
        Reusable client; one is created automatically if not provided.

    Returns
    -------
    pd.DataFrame
        Columns: ``time, open, high, low, close, volume`` (plus any extra
        fields the API returns such as ``num_trades``).

    Examples
    --------
    >>> from dataCollection.hyperliquid.spots import candles
    >>> df = candles.fetch_ohlcv("HYPE/USDC", "2024-06-01", "1h", end="2024-07-01")
    >>> df = candles.fetch_ohlcv("PURR/USDC", "2025-01-01", "15m")
    """
    client = _ensure_client(client)
    api_coin = _resolve_coin(coin, client)

    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end) if end is not None else datetime.now(timezone.utc)

    tf = Timeframe(bar) if isinstance(bar, str) else bar
    interval_s = _interval_str(tf)
    step_ms = TIMEFRAME_MS[tf] * 5000

    start_ms = _ts_ms(start_dt)
    end_ms = _ts_ms(end_dt)

    frames: list[pd.DataFrame] = []
    cursor = start_ms

    while cursor < end_ms:
        window_end = min(cursor + step_ms, end_ms)
        raw = client.candle_snapshot(api_coin, interval_s, cursor, window_end)
        if not raw:
            break
        frames.append(_raw_to_df(raw))
        last_ts = raw[-1]["t"]
        if last_ts <= cursor:
            break
        cursor = last_ts + 1

    if not frames:
        return _raw_to_df([])

    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset="time")
    df = df[(df["time"] >= start_dt) & (df["time"] <= end_dt)]
    return df.reset_index(drop=True)


# ── Inception date ───────────────────────────────────────────────────────────


def get_inception_date(
    coin: str,
    client: HyperliquidClient | None = None,
) -> datetime | None:
    """Return the earliest date for which candle data exists for a spot pair.

    Parameters
    ----------
    coin : str
        Pair name (e.g. ``"HYPE/USDC"``) or ``@index`` identifier.
    """
    client = _ensure_client(client)
    api_coin = _resolve_coin(coin, client)
    try:
        raw = client.candle_snapshot(api_coin, Timeframe.D1.value, 0, _ts_ms(None))
        if raw:
            return datetime.fromtimestamp(raw[0]["t"] / 1000, tz=timezone.utc)
    except Exception:
        pass
    return None


def get_inception_dates(
    coins: list[str] | None = None,
    client: HyperliquidClient | None = None,
    progress: bool = False,
) -> pd.DataFrame:
    """Batch-fetch inception dates for many spot pairs.

    Parameters
    ----------
    coins : list of str or None
        Pair names to check.  ``None`` means all active spot pairs.
    progress : bool
        Print progress to stdout.
    """
    client = _ensure_client(client)

    if coins is None:
        from .markets import get_pairs
        pairs_df = get_pairs(client)
        coins = pairs_df["name"].tolist()

    rows = []
    total = len(coins)
    for i, coin in enumerate(coins, 1):
        inception = get_inception_date(coin, client=client)
        now = datetime.now(timezone.utc)
        rows.append({
            "pair": coin,
            "data_since": inception.strftime("%Y-%m-%d") if inception else None,
            "days_available": (now - inception).days if inception else 0,
        })
        if progress and (i % 10 == 0 or i == total):
            status = rows[-1]["data_since"] or "N/A"
            print(f"  [{i:>4}/{total}] {coin:<20} -> {status}")

    return pd.DataFrame(rows)
