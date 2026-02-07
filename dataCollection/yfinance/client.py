"""Low-level wrapper around the yfinance library with rate limiting."""

from __future__ import annotations

import time
from typing import Any

import pandas as pd
import yfinance as yf

from ..common.types import Timeframe


# ── Interval mapping ─────────────────────────────────────────────────────────

TIMEFRAME_TO_YF: dict[str, str] = {
    Timeframe.M1.value:  "1m",
    Timeframe.M5.value:  "5m",
    Timeframe.M15.value: "15m",
    Timeframe.M30.value: "30m",
    Timeframe.H1.value:  "1h",
    Timeframe.D1.value:  "1d",
    Timeframe.W1.value:  "1wk",
    Timeframe.MO1.value: "1mo",
}

_UNSUPPORTED = {
    Timeframe.M3.value, Timeframe.H2.value, Timeframe.H4.value,
    Timeframe.H8.value, Timeframe.H12.value, Timeframe.D3.value,
}


def _yf_interval(interval: Timeframe | str) -> str:
    """Convert a Timeframe enum or raw string to a yfinance interval string."""
    raw = interval.value if isinstance(interval, Timeframe) else interval
    if raw in _UNSUPPORTED:
        raise ValueError(
            f"Interval {raw!r} has no yfinance equivalent. "
            f"Supported: {sorted(TIMEFRAME_TO_YF.keys())}"
        )
    return TIMEFRAME_TO_YF.get(raw, raw)


# ── Client ───────────────────────────────────────────────────────────────────


class YFinanceClient:
    """Thin wrapper around ``yfinance`` with rate limiting.

    Follows the same client pattern as ``HyperliquidClient`` and
    ``DydxClient`` so that higher-level modules can optionally share
    and reuse a single client instance.
    """

    def __init__(self, rate_limit: float = 0.2):
        self._rate_limit = rate_limit
        self._last_request_ts: float = 0.0

    def _wait(self) -> None:
        elapsed = time.time() - self._last_request_ts
        if elapsed < self._rate_limit:
            time.sleep(self._rate_limit - elapsed)

    def _mark(self) -> None:
        self._last_request_ts = time.time()

    # ── raw access ────────────────────────────────────────────────────

    def ticker_info(self, symbol: str) -> dict[str, Any]:
        """Return the ``yf.Ticker(symbol).info`` dict."""
        self._wait()
        info = yf.Ticker(symbol).info
        self._mark()
        return info

    def ticker_history(
        self,
        symbol: str,
        start: str | None = None,
        end: str | None = None,
        interval: str = "1d",
        period: str | None = None,
    ) -> pd.DataFrame:
        """Return ``yf.Ticker(symbol).history(...)``."""
        self._wait()
        kwargs: dict[str, Any] = {"interval": interval, "auto_adjust": True}
        if period is not None:
            kwargs["period"] = period
        else:
            if start is not None:
                kwargs["start"] = start
            if end is not None:
                kwargs["end"] = end
        df = yf.Ticker(symbol).history(**kwargs)
        self._mark()
        return df

    def download(
        self,
        symbols: list[str],
        start: str | None = None,
        end: str | None = None,
        interval: str = "1d",
        period: str | None = None,
    ) -> pd.DataFrame:
        """Batch download via ``yf.download(...)``."""
        self._wait()
        kwargs: dict[str, Any] = {
            "tickers": symbols,
            "interval": interval,
            "auto_adjust": True,
            "progress": False,
        }
        if period is not None:
            kwargs["period"] = period
        else:
            if start is not None:
                kwargs["start"] = start
            if end is not None:
                kwargs["end"] = end
        df = yf.download(**kwargs)
        self._mark()
        return df
