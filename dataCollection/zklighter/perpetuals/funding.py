"""Funding rate data for zkLighter perps."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ..client import ZkLighterClient


def _ensure_client(client: ZkLighterClient | None) -> ZkLighterClient:
    if client is None:
        from ..client import ZkLighterClient as _Cls
        return _Cls()
    return client


def _ts_ms(dt: datetime | int | None) -> int:
    if dt is None:
        return int(time.time() * 1000)
    if isinstance(dt, (int, float)):
        return int(dt)
    return int(dt.timestamp() * 1000)


# ── Current rates ────────────────────────────────────────────────────────────


def get_current_rates(
    client: ZkLighterClient | None = None,
) -> pd.DataFrame:
    """Current funding rate for every zkLighter perpetual market.

    Returns a DataFrame with columns:
    ``market_id, exchange, symbol, rate``.
    """
    client = _ensure_client(client)
    data = client.funding_rates()

    if data.get("code") != 200:
        raise ValueError(f"API error: {data.get('message', 'Unknown error')}")

    rates = data.get("funding_rates", [])
    rows = []

    for rate in rates:
        rows.append({
            "market_id": rate.get("market_id"),
            "exchange": rate.get("exchange", "lighter"),
            "symbol": rate.get("symbol"),
            "funding_rate": float(rate.get("rate", 0)),
        })

    df = pd.DataFrame(rows)
    if "funding_rate" in df.columns:
        df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")

    return df


# ── Historical rates ─────────────────────────────────────────────────────────


def get_funding_history(
    market_id: int,
    resolution: str = "1h",
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    count_back: int = 1000,
    client: ZkLighterClient | None = None,
) -> pd.DataFrame:
    """Fetch historical funding rate samples for a market.

    Parameters
    ----------
    market_id : int
        Market identifier (e.g., 0 for ETH, 1 for BTC).
    resolution : str
        "1h" or "1d" (default: "1h").
    start : datetime, epoch-ms int, or None
        ``None`` means fetch from earliest available.
    end : datetime, epoch-ms int, or None
        ``None`` means *now*.
    count_back : int
        Number of historical records to fetch (default: 1000).
    """
    client = _ensure_client(client)
    start_ms = _ts_ms(start) if start is not None else None
    end_ms = _ts_ms(end) if end is not None else None

    data = client.fundings(
        market_id=market_id,
        resolution=resolution,
        start_timestamp=start_ms,
        end_timestamp=end_ms,
        count_back=count_back,
    )

    if data.get("code") != 200:
        raise ValueError(f"API error: {data.get('message', 'Unknown error')}")

    fundings = data.get("f", [])
    if not fundings:
        return pd.DataFrame(columns=["time", "market_id", "funding_rate"])

    rows = []
    for funding in fundings:
        rows.append({
            "time": pd.to_datetime(funding["t"], unit="ms", utc=True),
            "market_id": market_id,
            "funding_rate": float(funding.get("r", 0)),
        })

    df = pd.DataFrame(rows)
    if "funding_rate" in df.columns:
        df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")

    return df
