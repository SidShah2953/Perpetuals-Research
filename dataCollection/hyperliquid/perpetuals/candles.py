"""OHLCV candlestick data fetching for Hyperliquid perps."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
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


# ── Single fetch ─────────────────────────────────────────────────────────────


def get_candles(
    coin: str,
    interval: Timeframe | str = Timeframe.H1,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    client: HyperliquidClient | None = None,
) -> pd.DataFrame:
    """Fetch candles for a single time window (API limit applies).

    Parameters
    ----------
    coin : str
        Base symbol, e.g. ``"BTC"``.
    interval : Timeframe or str
        Candle interval, e.g. ``Timeframe.H1`` or ``"1h"``.
    start : datetime, epoch-ms int, or None
        Start of the window.  ``None`` means epoch 0.
    end : datetime, epoch-ms int, or None
        End of the window.  ``None`` means *now*.
    """
    client = _ensure_client(client)
    start_ms = _ts_ms(start) if start is not None else 0
    end_ms = _ts_ms(end)
    raw = client.candle_snapshot(coin, _interval_str(interval), start_ms, end_ms)
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
    tf = Timeframe(interval) if isinstance(interval, str) else interval
    step_ms = TIMEFRAME_MS[tf] * max_candles_per_request

    start_ms = _ts_ms(start) if start is not None else 0
    end_ms = _ts_ms(end)

    frames: list[pd.DataFrame] = []
    cursor = start_ms

    while cursor < end_ms:
        window_end = min(cursor + step_ms, end_ms)
        raw = client.candle_snapshot(coin, _interval_str(tf), cursor, window_end)
        if not raw:
            break
        df = _raw_to_df(raw)
        frames.append(df)
        # advance past the last candle we received
        last_ts = raw[-1]["t"]
        if last_ts <= cursor:
            break  # no progress
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
    """Fetch all OHLCV bars for a coin over a date range.

    Automatically paginates when the range exceeds a single API response.

    Parameters
    ----------
    coin : str
        Base symbol, e.g. ``"BTC"``, ``"ETH"``.
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
    >>> from dataCollection.hyperliquid.perpetuals import candles
    >>> df = candles.fetch_ohlcv("BTC", "2024-06-01", "1h", end="2024-07-01")
    >>> df = candles.fetch_ohlcv("ETH", "2025-01-01", "15m")
    """
    client = _ensure_client(client)

    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end) if end is not None else datetime.now(timezone.utc)

    tf = Timeframe(bar) if isinstance(bar, str) else bar
    interval_s = _interval_str(tf)
    step_ms = TIMEFRAME_MS[tf] * 5000  # 5 000 bars per request window

    start_ms = _ts_ms(start_dt)
    end_ms = _ts_ms(end_dt)

    frames: list[pd.DataFrame] = []
    cursor = start_ms

    while cursor < end_ms:
        window_end = min(cursor + step_ms, end_ms)
        raw = client.candle_snapshot(coin, interval_s, cursor, window_end)
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
    # trim to the exact requested window
    df = df[(df["time"] >= start_dt) & (df["time"] <= end_dt)]
    return df.reset_index(drop=True)


# ── Inception date ───────────────────────────────────────────────────────────

_NATIVE_DEX_ALIASES = {"Hyperliquid (native)", "native", "hl", "hyperliquid", None}


def _resolve_coin(coin: str, dex: str | None) -> str:
    """Build the API coin identifier for a given DEX.

    Native DEX coins are bare symbols (``"BTC"``), while third-party DEX
    coins are prefixed (``"xyz:GOLD"``).  If *coin* already contains a
    colon it is returned as-is.
    """
    if ":" in coin:
        return coin
    if dex is None or dex.lower() in {a.lower() if isinstance(a, str) else a for a in _NATIVE_DEX_ALIASES}:
        return coin
    return f"{dex}:{coin}"


def get_inception_date(
    coin: str,
    dex: str | None = None,
    client: HyperliquidClient | None = None,
) -> datetime | None:
    """Return the earliest date for which candle data exists on a DEX.

    Parameters
    ----------
    coin : str
        Base symbol, e.g. ``"BTC"``, ``"GOLD"``.  A fully-qualified name
        like ``"xyz:GOLD"`` is also accepted (``dex`` is then ignored).
    dex : str or None
        DEX short name, e.g. ``"xyz"``, ``"vntl"``, ``"hyna"``.
        ``None`` (default) targets the native Hyperliquid DEX.
    """
    client = _ensure_client(client)
    ticker = _resolve_coin(coin, dex)
    try:
        raw = client.candle_snapshot(ticker, Timeframe.D1.value, 0, _ts_ms(None))
        if raw:
            return datetime.fromtimestamp(raw[0]["t"] / 1000, tz=timezone.utc)
    except Exception:
        pass
    return None


def get_inception_dates(
    coins: list[str] | None = None,
    dex: str | None = None,
    client: HyperliquidClient | None = None,
    progress: bool = False,
) -> pd.DataFrame:
    """Batch-fetch inception dates for many coins on a given DEX.

    Parameters
    ----------
    coins : list of str or None
        Symbols to check.  ``None`` means all active markets on the
        target DEX.
    dex : str or None
        DEX short name.  ``None`` targets the native DEX.
    progress : bool
        Print progress to stdout.
    """
    client = _ensure_client(client)

    if coins is None:
        from .markets import get_all_markets
        mkt_df = get_all_markets(client)
        dex_label = "Hyperliquid (native)" if dex is None or dex.lower() in {"native", "hl", "hyperliquid"} else dex
        mkt_df = mkt_df[(mkt_df["dex"] == dex_label) & (~mkt_df["is_delisted"])]
        coins = mkt_df["base_symbol"].tolist()

    rows = []
    total = len(coins)
    for i, coin in enumerate(coins, 1):
        inception = get_inception_date(coin, dex=dex, client=client)
        now = datetime.now(timezone.utc)
        rows.append({
            "coin": coin,
            "dex": dex or "Hyperliquid (native)",
            "data_since": inception.strftime("%Y-%m-%d") if inception else None,
            "days_available": (now - inception).days if inception else 0,
        })
        if progress and (i % 10 == 0 or i == total):
            status = rows[-1]["data_since"] or "N/A"
            print(f"  [{i:>4}/{total}] {coin:<16} -> {status}")

    return pd.DataFrame(rows)
