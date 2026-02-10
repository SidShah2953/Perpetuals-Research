"""Funding rate data for edgeX perps."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ..client import EdgeXClient


def _ensure_client(client: EdgeXClient | None) -> EdgeXClient:
    if client is None:
        from ..client import EdgeXClient as _Cls
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
    contract_id: str | None = None,
    client: EdgeXClient | None = None,
) -> pd.DataFrame:
    """Current funding rate for edgeX perpetual contracts.

    Parameters
    ----------
    contract_id : str or None
        Specific contract ID, or None for all contracts.

    Returns a DataFrame with columns:
    ``contract_id, contract_name, funding_rate, settlement_time, index_price, oracle_price``.
    """
    client = _ensure_client(client)
    data = client.latest_funding_rate(contract_id=contract_id)

    if data.get("code") != "SUCCESS":
        raise ValueError(f"API error: {data.get('msg', 'Unknown error')}")

    rates = data.get("data", [])
    if not isinstance(rates, list):
        rates = [rates] if rates else []

    rows = []
    for rate in rates:
        rows.append({
            "contract_id": rate.get("contractId"),
            "funding_rate": float(rate.get("fundingRate", 0)),
            "funding_time": pd.to_datetime(rate.get("fundingTime"), unit="ms", utc=True) if rate.get("fundingTime") else None,
            "index_price": float(rate.get("indexPrice", 0)),
            "oracle_price": float(rate.get("oraclePrice", 0)),
            "mark_price": float(rate.get("markPrice", 0)),
            "premium_index": float(rate.get("premiumIndex", 0)) if rate.get("premiumIndex") else None,
            "is_settlement": rate.get("isSettlement", False),
        })

    df = pd.DataFrame(rows)
    if "funding_rate" in df.columns:
        df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")

    return df


# ── Historical rates ─────────────────────────────────────────────────────────


def get_funding_history(
    contract_id: str,
    start: datetime | int | None = None,
    end: datetime | int | None = None,
    filter_settlement: bool = False,
    size: int = 100,
    client: EdgeXClient | None = None,
) -> pd.DataFrame:
    """Fetch historical funding rate samples for a contract.

    Parameters
    ----------
    contract_id : str
        Contract identifier (e.g., "10000001" for BTC-PERP).
    start : datetime, epoch-ms int, or None
        ``None`` means fetch from earliest available.
    end : datetime, epoch-ms int, or None
        ``None`` means *now*.
    filter_settlement : bool
        If True, only return settlement funding rates (every 8 hours).
        If False, return predicted rates (calculated every minute).
    size : int
        Number of records per page (1-100).
    """
    client = _ensure_client(client)
    start_ms = _ts_ms(start) if start is not None else None
    end_ms = _ts_ms(end) if end is not None else None

    # Fetch paginated data
    frames: list[pd.DataFrame] = []
    offset_data = None

    while True:
        data = client.funding_rate_page(
            contract_id=contract_id,
            size=size,
            offset_data=offset_data,
            filter_settlement=filter_settlement,
            filter_begin_time=start_ms,
            filter_end_time=end_ms,
        )

        if data.get("code") != "SUCCESS":
            raise ValueError(f"API error: {data.get('msg', 'Unknown error')}")

        result_data = data.get("data", {})
        rates = result_data.get("result", [])

        if not rates:
            break

        rows = []
        for rate in rates:
            rows.append({
                "time": pd.to_datetime(rate.get("fundingTime"), unit="ms", utc=True),
                "contract_id": rate.get("contractId"),
                "funding_rate": float(rate.get("fundingRate", 0)),
                "index_price": float(rate.get("indexPrice", 0)),
                "oracle_price": float(rate.get("oraclePrice", 0)),
                "mark_price": float(rate.get("markPrice", 0)),
                "premium_index": float(rate.get("premiumIndex", 0)) if rate.get("premiumIndex") else None,
                "is_settlement": rate.get("isSettlement", False),
            })

        df = pd.DataFrame(rows)
        frames.append(df)

        # Check if there's more data
        has_next = result_data.get("hasNext", False)
        if not has_next:
            break

        offset_data = result_data.get("offsetData")
        if not offset_data:
            break

    if not frames:
        return pd.DataFrame(columns=["time", "contract_id", "funding_rate"])

    final_df = pd.concat(frames, ignore_index=True)

    if "funding_rate" in final_df.columns:
        final_df["funding_rate"] = pd.to_numeric(final_df["funding_rate"], errors="coerce")

    return final_df.sort_values("time").reset_index(drop=True)
