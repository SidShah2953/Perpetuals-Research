from __future__ import annotations

import time
from typing import Any

import requests

from ..common.http import create_session

API_URL = "https://indexer.dydx.trade"


class DydxClient:
    """Low-level client for the dYdX v4 indexer API."""

    def __init__(self, session: requests.Session | None = None, rate_limit: float = 0.3):
        self.session = session or create_session()
        self.base_url = API_URL
        self._rate_limit = rate_limit
        self._last_request_ts: float = 0.0

    def _wait(self) -> None:
        elapsed = time.time() - self._last_request_ts
        if elapsed < self._rate_limit:
            time.sleep(self._rate_limit - elapsed)

    def _get(self, path: str, params: dict | None = None, timeout: int = 10) -> Any:
        self._wait()
        resp = self.session.get(f"{self.base_url}{path}", params=params, timeout=timeout)
        self._last_request_ts = time.time()
        resp.raise_for_status()
        return resp.json()

    # ── markets ──────────────────────────────────────────────────────────

    def perpetual_markets(self) -> dict:
        """GET /v4/perpetualMarkets"""
        return self._get("/v4/perpetualMarkets")

    # ── candles ──────────────────────────────────────────────────────────

    def candles(
        self,
        ticker: str,
        resolution: str = "1DAY",
        limit: int = 100,
    ) -> dict:
        """GET /v4/candles/perpetualMarkets/{ticker}"""
        return self._get(
            f"/v4/candles/perpetualMarkets/{ticker}",
            params={"resolution": resolution, "limit": limit},
            timeout=15,
        )
