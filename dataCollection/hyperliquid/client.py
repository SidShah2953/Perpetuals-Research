from __future__ import annotations

import time
from typing import Any

import requests

from ..common.http import create_session
from .constants import API_URL


class HyperliquidClient:
    """Low-level client for the Hyperliquid /info API.

    All public methods return raw JSON (dicts / lists) exactly as the API
    sends them.  Higher-level modules (markets, candles, funding, ...) add
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

    def _post(self, payload: dict, timeout: int = 10) -> Any:
        self._wait()
        resp = self.session.post(self.base_url, json=payload, timeout=timeout)
        self._last_request_ts = time.time()
        resp.raise_for_status()
        return resp.json()

    # ── market metadata ──────────────────────────────────────────────────

    def meta(self) -> dict:
        """Native DEX market metadata (``{type: "meta"}``)."""
        return self._post({"type": "meta"})

    def all_perp_metas(self) -> list[dict]:
        """Metadata for every perp across all DEXs (``{type: "allPerpMetas"}``)."""
        return self._post({"type": "allPerpMetas"})

    def perp_dexs(self) -> list[dict | None]:
        """List of perpetual DEXs (``{type: "perpDexs"}``)."""
        return self._post({"type": "perpDexs"})

    def meta_and_asset_ctxs(self, dex: str | None = None) -> list:
        """Metadata + live context for a single DEX (``{type: "metaAndAssetCtxs"}``)."""
        payload: dict[str, Any] = {"type": "metaAndAssetCtxs"}
        if dex is not None:
            payload["dex"] = dex
        return self._post(payload)

    # ── candles ──────────────────────────────────────────────────────────

    def candle_snapshot(
        self,
        coin: str,
        interval: str,
        start_time: int,
        end_time: int,
    ) -> list[dict]:
        """Raw candle snapshot (``{type: "candleSnapshot"}``)."""
        return self._post(
            {
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": interval,
                    "startTime": start_time,
                    "endTime": end_time,
                },
            },
            timeout=15,
        )

    # ── funding ──────────────────────────────────────────────────────────

    def funding_history(
        self,
        coin: str,
        start_time: int,
        end_time: int | None = None,
    ) -> list[dict]:
        """Historical funding rate samples (``{type: "fundingHistory"}``)."""
        payload: dict[str, Any] = {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_time,
        }
        if end_time is not None:
            payload["endTime"] = end_time
        return self._post(payload)

    # ── spot metadata ─────────────────────────────────────────────────────

    def spot_meta(self) -> dict:
        """Spot token + pair metadata (``{type: "spotMeta"}``)."""
        return self._post({"type": "spotMeta"})

    def spot_meta_and_asset_ctxs(self) -> list:
        """Spot metadata + live context (``{type: "spotMetaAndAssetCtxs"}``)."""
        return self._post({"type": "spotMetaAndAssetCtxs"})

    # ── order book & trades ──────────────────────────────────────────────

    def l2_book(self, coin: str) -> dict:
        """Current L2 order book snapshot (``{type: "l2Book"}``)."""
        return self._post({"type": "l2Book", "coin": coin})

    def recent_trades(self, coin: str) -> list[dict]:
        """Recent trades (``{type: "recentTrades"}``)."""
        return self._post({"type": "recentTrades", "coin": coin})
