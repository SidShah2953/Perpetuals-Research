"""Funding rate data for Hyperliquid perps."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .client import HyperliquidClient


def _ensure_client(client: HyperliquidClient | None) -> HyperliquidClient:
    if client is None:
        from .client import HyperliquidClient as _Cls
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
    client: HyperliquidClient | None = None,
) -> pd.DataFrame:
    """Current funding rate and mark price for every native-DEX perp.

    Returns a DataFrame with columns:
    ``name, mark_price, funding_rate, open_interest, volume_24h``.
    """
    client = _ensure_client(client)
    data = client.meta_and_asset_ctxs()
    meta = data[0]
    ctxs = data[1]

    rows = []
    for asset, ctx in zip(meta["universe"], ctxs):
        if asset.get("isDelisted", False):
            continue
        rows.append({
            "name": asset.get("name"),
            "mark_price": ctx.get("markPx"),
            "funding_rate": ctx.get("funding"),
            "open_interest": ctx.get("openInterest"),
            "volume_24h": ctx.get("dayNtlVlm"),
        })
    df = pd.DataFrame(rows)
    for col in ("mark_price", "funding_rate", "open_interest", "volume_24h"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ── Historical rates ─────────────────────────────────────────────────────────


def get_funding_history(
    coin: str,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    client: HyperliquidClient | None = None,
) -> pd.DataFrame:
    """Fetch historical funding rate samples for a coin.

    Parameters
    ----------
    coin : str
        Base symbol, e.g. ``"BTC"``.
    start : datetime, epoch-ms int, or None
        ``None`` means epoch 0 (all history).
    end : datetime, epoch-ms int, or None
        ``None`` means *now*.
    """
    client = _ensure_client(client)
    start_ms = _ts_ms(start) if start is not None else 0
    end_ms = _ts_ms(end) if end is not None else None

    raw = client.funding_history(coin, start_ms, end_ms)
    if not raw:
        return pd.DataFrame(columns=["time", "coin", "funding_rate", "premium"])

    df = pd.DataFrame(raw)
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    for col in ("fundingRate", "premium"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.rename(columns={"fundingRate": "funding_rate"})
    return df
