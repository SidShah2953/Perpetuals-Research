from __future__ import annotations

import time
from typing import Any

import requests

from ..common.http import create_session
from .constants import API_URL


class EdgeXClient:
    """Low-level client for the edgeX REST API.

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

    def server_time(self) -> dict:
        """Get server time in milliseconds."""
        return self._get("/api/v1/public/meta/getServerTime")

    def metadata(self) -> dict:
        """Get comprehensive metadata including coins, contracts, and config."""
        return self._get("/api/v1/public/meta/getMetaData")

    # ── quotes & tickers ─────────────────────────────────────────────────

    def ticker(self, contract_id: str | None = None) -> dict:
        """Get 24-hour ticker data for one or all contracts.

        Parameters
        ----------
        contract_id : str or None
            Specific contract ID, or None for all contracts
        """
        params = {}
        if contract_id is not None:
            params["contractId"] = contract_id
        return self._get("/api/v1/public/quote/getTicker", params=params)

    # ── candles ──────────────────────────────────────────────────────────

    def kline(
        self,
        contract_id: str | None = None,
        price_type: str = "LAST_PRICE",
        kline_type: str = "HOUR_1",
        size: int = 1000,
        offset_data: str | None = None,
        filter_begin_time: int | None = None,
        filter_end_time: int | None = None,
    ) -> dict:
        """Get K-line/candlestick data for a contract.

        Parameters
        ----------
        contract_id : str or None
            Contract ID
        price_type : str
            ORACLE_PRICE, INDEX_PRICE, LAST_PRICE, ASK1_PRICE, BID1_PRICE, OPEN_INTEREST
        kline_type : str
            MINUTE_1, MINUTE_5, HOUR_1, HOUR_4, DAY_1, WEEK_1, MONTH_1
        size : int
            Number of records (1-1000)
        offset_data : str or None
            Pagination offset
        filter_begin_time : int or None
            Start time in milliseconds
        filter_end_time : int or None
            End time in milliseconds
        """
        params = {
            "priceType": price_type,
            "klineType": kline_type,
            "size": str(size),
        }
        if contract_id is not None:
            params["contractId"] = contract_id
        if offset_data is not None:
            params["offsetData"] = offset_data
        if filter_begin_time is not None:
            params["filterBeginKlineTimeInclusive"] = str(filter_begin_time)
        if filter_end_time is not None:
            params["filterEndKlineTimeExclusive"] = str(filter_end_time)

        return self._get("/api/v1/public/quote/getKline", params=params, timeout=15)

    def multi_contract_kline(
        self,
        contract_ids: list[str],
        price_type: str = "LAST_PRICE",
        kline_type: str = "HOUR_1",
        size: int = 1000,
        filter_begin_time: int | None = None,
        filter_end_time: int | None = None,
    ) -> dict:
        """Get K-line data for multiple contracts.

        Parameters
        ----------
        contract_ids : list of str
            List of contract IDs
        price_type : str
            ORACLE_PRICE, INDEX_PRICE, LAST_PRICE, etc.
        kline_type : str
            MINUTE_1, MINUTE_5, HOUR_1, HOUR_4, DAY_1, etc.
        size : int
            Number of records per contract (1-1000)
        filter_begin_time : int or None
            Start time in milliseconds
        filter_end_time : int or None
            End time in milliseconds
        """
        params = {
            "contractIdList": ",".join(contract_ids),
            "priceType": price_type,
            "klineType": kline_type,
            "size": str(size),
        }
        if filter_begin_time is not None:
            params["filterBeginKlineTimeInclusive"] = str(filter_begin_time)
        if filter_end_time is not None:
            params["filterEndKlineTimeExclusive"] = str(filter_end_time)

        return self._get("/api/v1/public/quote/getMultiContractKline", params=params, timeout=15)

    # ── funding ──────────────────────────────────────────────────────────

    def latest_funding_rate(self, contract_id: str | None = None) -> dict:
        """Get latest funding rate for one or all contracts.

        Parameters
        ----------
        contract_id : str or None
            Specific contract ID, or None for all contracts
        """
        params = {}
        if contract_id is not None:
            params["contractId"] = contract_id
        return self._get("/api/v1/public/funding/getLatestFundingRate", params=params)

    def funding_rate_page(
        self,
        contract_id: str | None = None,
        size: int = 100,
        offset_data: str | None = None,
        filter_settlement: bool = False,
        filter_begin_time: int | None = None,
        filter_end_time: int | None = None,
    ) -> dict:
        """Get paginated funding rate history.

        Parameters
        ----------
        contract_id : str or None
            Contract ID
        size : int
            Records per page (1-100)
        offset_data : str or None
            Pagination cursor
        filter_settlement : bool
            Filter for settlement rates (every 8 hours)
        filter_begin_time : int or None
            Start time in milliseconds
        filter_end_time : int or None
            End time in milliseconds
        """
        params = {"size": str(size)}
        if contract_id is not None:
            params["contractId"] = contract_id
        if offset_data is not None:
            params["offsetData"] = offset_data
        if filter_settlement:
            params["filterSettlementFundingRate"] = "true"
        if filter_begin_time is not None:
            params["filterBeginTimeInclusive"] = str(filter_begin_time)
        if filter_end_time is not None:
            params["filterEndTimeExclusive"] = str(filter_end_time)

        return self._get("/api/v1/public/funding/getFundingRatePage", params=params)
