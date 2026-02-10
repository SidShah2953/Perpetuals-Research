"""OHLCV candlestick data fetching for edgeX perps."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from ...common.types import Timeframe

if TYPE_CHECKING:
    from ..client import EdgeXClient


def _ensure_client(client: EdgeXClient | None) -> EdgeXClient:
    if client is None:
        from ..client import EdgeXClient as _Cls
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


# Map our Timeframe enum to edgeX klineType strings
_KLINE_TYPE_MAP = {
    Timeframe.M1: "MINUTE_1",
    Timeframe.M5: "MINUTE_5",
    Timeframe.M15: "MINUTE_15",
    Timeframe.M30: "MINUTE_30",
    Timeframe.H1: "HOUR_1",
    Timeframe.H4: "HOUR_4",
    Timeframe.D1: "DAY_1",
    Timeframe.W1: "WEEK_1",
    # Timeframe.MO1: "MONTH_1",  # if needed
}


def _kline_type_str(interval: Timeframe | str) -> str:
    """Convert Timeframe enum or string to edgeX klineType format."""
    if isinstance(interval, str):
        # If already in edgeX format, return as-is
        if interval.startswith(("MINUTE_", "HOUR_", "DAY_", "WEEK_", "MONTH_")):
            return interval
        # Try to map common formats
        mapping = {
            "1m": "MINUTE_1",
            "5m": "MINUTE_5",
            "15m": "MINUTE_15",
            "30m": "MINUTE_30",
            "1h": "HOUR_1",
            "4h": "HOUR_4",
            "1d": "DAY_1",
            "1w": "WEEK_1",
        }
        return mapping.get(interval, interval)
    return _KLINE_TYPE_MAP.get(interval, "HOUR_1")


def _raw_to_df(raw: dict) -> pd.DataFrame:
    """Convert raw kline response into a clean DataFrame."""
    if raw.get("code") != "SUCCESS":
        raise ValueError(f"API error: {raw.get('msg', 'Unknown error')}")

    klines = raw.get("data", {}).get("dataList", [])
    if not klines:
        return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume", "num_trades"])

    rows = []
    for kline in klines:
        rows.append({
            "time": pd.to_datetime(kline["klineTime"], unit="ms", utc=True),
            "open": float(kline["open"]),
            "high": float(kline["high"]),
            "low": float(kline["low"]),
            "close": float(kline["close"]),
            "volume": float(kline.get("size", 0)),
            "value": float(kline.get("value", 0)),
            "num_trades": int(kline.get("trades", 0)),
        })

    return pd.DataFrame(rows)


# ── Single fetch ─────────────────────────────────────────────────────────────


def get_candles(
    contract_id: str,
    interval: Timeframe | str = Timeframe.H1,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    price_type: str = "LAST_PRICE",
    size: int = 1000,
    client: EdgeXClient | None = None,
) -> pd.DataFrame:
    """Fetch candles for a single time window.

    Parameters
    ----------
    contract_id : str
        Contract identifier (e.g., "10000001" for BTC-PERP).
    interval : Timeframe or str
        Candle interval, e.g. ``Timeframe.H1`` or ``"1h"``.
    start : datetime, epoch-ms int, or None
        Start of the window. ``None`` means fetch recent data.
    end : datetime, epoch-ms int, or None
        End of the window. ``None`` means *now*.
    price_type : str
        LAST_PRICE, MARK_PRICE, INDEX_PRICE, etc.
    size : int
        Number of candles to fetch (max 1000 per request).
    """
    client = _ensure_client(client)
    start_ms = _ts_ms(start) if start is not None else None
    end_ms = _ts_ms(end)
    kline_type = _kline_type_str(interval)

    raw = client.kline(
        contract_id=contract_id,
        price_type=price_type,
        kline_type=kline_type,
        size=size,
        filter_begin_time=start_ms,
        filter_end_time=end_ms,
    )
    return _raw_to_df(raw)


# ── Paginated fetch ──────────────────────────────────────────────────────────


def get_candles_range(
    contract_id: str,
    interval: Timeframe | str = Timeframe.H1,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    price_type: str = "LAST_PRICE",
    client: EdgeXClient | None = None,
    max_candles_per_request: int = 1000,
) -> pd.DataFrame:
    """Paginated candle fetch that stitches multiple requests together.

    Use this when the requested range exceeds the per-request candle limit.
    """
    client = _ensure_client(client)
    kline_type = _kline_type_str(interval)

    # Calculate interval in ms for pagination
    interval_ms_map = {
        "MINUTE_1": 60_000,
        "MINUTE_5": 300_000,
        "MINUTE_15": 900_000,
        "MINUTE_30": 1_800_000,
        "HOUR_1": 3_600_000,
        "HOUR_4": 14_400_000,
        "DAY_1": 86_400_000,
        "WEEK_1": 604_800_000,
        "MONTH_1": 2_592_000_000,
    }
    interval_ms = interval_ms_map.get(kline_type, 3_600_000)
    step_ms = interval_ms * max_candles_per_request

    start_ms = _ts_ms(start) if start is not None else _ts_ms(None) - step_ms
    end_ms = _ts_ms(end)

    frames: list[pd.DataFrame] = []
    cursor = start_ms

    while cursor < end_ms:
        window_end = min(cursor + step_ms, end_ms)

        raw = client.kline(
            contract_id=contract_id,
            price_type=price_type,
            kline_type=kline_type,
            size=max_candles_per_request,
            filter_begin_time=cursor,
            filter_end_time=window_end,
        )

        if raw.get("code") != "SUCCESS":
            break

        klines = raw.get("data", {}).get("dataList", [])
        if not klines:
            break

        df = _raw_to_df(raw)
        frames.append(df)

        # Check if we have pagination info
        has_next = raw.get("data", {}).get("hasNext", False)
        if not has_next:
            break

        # advance past the last candle we received
        last_ts = klines[-1]["klineTime"]
        if last_ts <= cursor:
            break  # no progress
        cursor = last_ts + interval_ms

    if not frames:
        return _raw_to_df({"code": "SUCCESS", "data": {"dataList": []}})

    return pd.concat(frames, ignore_index=True).drop_duplicates(subset="time")


# ── Fetch OHLCV by date range ────────────────────────────────────────────────


def fetch_ohlcv(
    contract_id: str,
    start: str | datetime,
    bar: Timeframe | str,
    end: str | datetime | None = None,
    price_type: str = "LAST_PRICE",
    client: EdgeXClient | None = None,
) -> pd.DataFrame:
    """Fetch all OHLCV bars for a contract over a date range.

    Automatically paginates when the range exceeds a single API response.

    Parameters
    ----------
    contract_id : str
        Contract identifier (e.g., "10000001" for BTC-PERP).
    start : str or datetime
        Start of the range. Strings like ``"2024-01-01"`` or
        ``"2024-06-15 08:00"`` are accepted.
    bar : Timeframe or str
        Bar / candle duration, e.g. ``Timeframe.H1``, ``"15m"``, ``"1d"``.
    end : str, datetime, or None
        End of the range. Defaults to *now* when ``None``.
    price_type : str
        LAST_PRICE, MARK_PRICE, INDEX_PRICE, etc.
    client : EdgeXClient or None
        Reusable client; one is created automatically if not provided.

    Returns
    -------
    pd.DataFrame
        Columns: ``time, open, high, low, close, volume, value, num_trades``.

    Examples
    --------
    >>> from dataCollection.edgex.perpetuals import candles
    >>> df = candles.fetch_ohlcv("10000001", "2024-06-01", "1h", end="2024-07-01")
    >>> df = candles.fetch_ohlcv("10000002", "2025-01-01", "15m")
    """
    client = _ensure_client(client)

    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end) if end is not None else datetime.now(timezone.utc)

    start_ms = _ts_ms(start_dt)
    end_ms = _ts_ms(end_dt)

    df = get_candles_range(
        contract_id=contract_id,
        interval=bar,
        start=start_ms,
        end=end_ms,
        price_type=price_type,
        client=client,
        max_candles_per_request=1000,
    )

    # trim to the exact requested window
    if not df.empty:
        df = df[(df["time"] >= start_dt) & (df["time"] <= end_dt)]

    return df.reset_index(drop=True)


# ── Inception date ───────────────────────────────────────────────────────────


def get_inception_date(
    contract_id: str,
    client: EdgeXClient | None = None,
) -> datetime | None:
    """Return the earliest date for which candle data exists.

    Parameters
    ----------
    contract_id : str
        Contract identifier (e.g., "10000001" for BTC-PERP).
    """
    client = _ensure_client(client)
    try:
        raw = client.kline(
            contract_id=contract_id,
            price_type="LAST_PRICE",
            kline_type="DAY_1",
            size=1000,
            filter_begin_time=0,
            filter_end_time=_ts_ms(None),
        )
        if raw.get("code") == "SUCCESS":
            klines = raw.get("data", {}).get("dataList", [])
            if klines:
                return datetime.fromtimestamp(klines[0]["klineTime"] / 1000, tz=timezone.utc)
    except Exception:
        pass
    return None


def get_inception_dates(
    contract_ids: list[str] | None = None,
    client: EdgeXClient | None = None,
    progress: bool = False,
) -> pd.DataFrame:
    """Batch-fetch inception dates for many contracts.

    Parameters
    ----------
    contract_ids : list of str or None
        Contract IDs to check. ``None`` means all active contracts.
    progress : bool
        Print progress to stdout.
    """
    client = _ensure_client(client)

    if contract_ids is None:
        from .markets import get_markets
        mkt_df = get_markets(client)
        contract_ids = mkt_df["contract_id"].tolist()

    rows = []
    total = len(contract_ids)
    for i, contract_id in enumerate(contract_ids, 1):
        inception = get_inception_date(contract_id, client=client)
        now = datetime.now(timezone.utc)
        rows.append({
            "contract_id": contract_id,
            "data_since": inception.strftime("%Y-%m-%d") if inception else None,
            "days_available": (now - inception).days if inception else 0,
        })
        if progress and (i % 5 == 0 or i == total):
            status = rows[-1]["data_since"] or "N/A"
            print(f"  [{i:>4}/{total}] Contract {contract_id:<10} -> {status}")

    return pd.DataFrame(rows)
