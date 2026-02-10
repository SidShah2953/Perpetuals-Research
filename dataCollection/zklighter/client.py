from __future__ import annotations

import time
from typing import Any

import requests

from ..common.http import create_session
from .constants import API_URL


class ZkLighterClient:
    """Low-level client for the zkLighter REST API.

    All public methods return raw JSON (dicts / lists) exactly as the API
    sends them. Higher-level modules (markets, candles, funding, ...) add
    DataFrame conversion, pagination, and convenience logic on top.
    """

    def __init__(self, session: requests.Session | None = None, rate_limit: float = 0.2):
        self.session = session or create_session()
        self.base_url = API_URL
        self._rate_limit = rate_limit  # seconds between requests
        self._last_request_ts: float = 0.0

    # ── internal ─────────────────────────────────────────────────────────

    def _wait(self) -> None:
        elapsed = time.time() - self._last_request_ts
        if elapsed < self._rate_limit:
            time.sleep(self._rate_limit - elapsed)

    def _get(self, endpoint: str, params: dict | None = None, timeout: int = 10) -> Any:
        self._wait()
        url = f"{self.base_url}{endpoint}"
        resp = self.session.get(url, params=params, timeout=timeout)
        self._last_request_ts = time.time()
        resp.raise_for_status()
        return resp.json()

    # ── market metadata ──────────────────────────────────────────────────

    def order_books(self, market_id: int | None = None, filter_type: str = "perp") -> dict:
        """Get list of order books/markets.

        Parameters
        ----------
        market_id : int or None
            Filter by specific market ID
        filter_type : str
            "all", "spot", or "perp" (default: "perp")
        """
        params = {"filter": filter_type}
        if market_id is not None:
            params["market_id"] = market_id
        return self._get("/api/v1/orderBooks", params=params)

    def order_book_details(self, market_id: int | None = None, filter_type: str = "perp") -> dict:
        """Get detailed market statistics including volume, OI, last price.

        Parameters
        ----------
        market_id : int or None
            Filter by specific market ID
        filter_type : str
            "all", "spot", or "perp" (default: "perp")
        """
        params = {"filter": filter_type}
        if market_id is not None:
            params["market_id"] = market_id
        return self._get("/api/v1/orderBookDetails", params=params)

    # ── candles ──────────────────────────────────────────────────────────

    def candles(
        self,
        market_id: int,
        resolution: str,
        start_timestamp: int,
        end_timestamp: int,
        count_back: int = 5000,
    ) -> dict:
        """Get OHLCV candle data.

        Parameters
        ----------
        market_id : int
            Market identifier
        resolution : str
            "1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d", "1w"
        start_timestamp : int
            Start time in milliseconds
        end_timestamp : int
            End time in milliseconds
        count_back : int
            Number of historical candles to fetch
        """
        params = {
            "market_id": market_id,
            "resolution": resolution,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "count_back": count_back,
        }
        return self._get("/api/v1/candles", params=params, timeout=15)

    # ── funding ──────────────────────────────────────────────────────────

    def funding_rates(self) -> dict:
        """Get current funding rates for all perpetual markets."""
        return self._get("/api/v1/funding-rates")

    def fundings(
        self,
        market_id: int,
        resolution: str = "1h",
        start_timestamp: int | None = None,
        end_timestamp: int | None = None,
        count_back: int = 1000,
    ) -> dict:
        """Get historical funding rate data.

        Parameters
        ----------
        market_id : int
            Market identifier
        resolution : str
            "1h" or "1d"
        start_timestamp : int or None
            Start time in milliseconds
        end_timestamp : int or None
            End time in milliseconds
        count_back : int
            Number of historical records to fetch
        """
        params = {
            "market_id": market_id,
            "resolution": resolution,
            "count_back": count_back,
        }
        if start_timestamp is not None:
            params["start_timestamp"] = start_timestamp
        if end_timestamp is not None:
            params["end_timestamp"] = end_timestamp
        return self._get("/api/v1/fundings", params=params)

    # ── exchange statistics ──────────────────────────────────────────────

    def exchange_stats(self) -> dict:
        """Get exchange-wide statistics including daily volumes and trade counts."""
        return self._get("/api/v1/exchangeStats")
