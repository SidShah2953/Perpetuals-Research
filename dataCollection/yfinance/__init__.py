"""Yahoo Finance data sub-package (spot only).

Sub-packages
------------
- ``dataCollection.yfinance.spots`` -- Spot market metadata, candles, inception dates

Quick-start::

    from dataCollection.yfinance import YFinanceClient
    from dataCollection.yfinance.spots import markets, candles

    client = YFinanceClient()

    # Market metadata
    df = markets.get_markets(["AAPL", "MSFT"], client=client)
    df = markets.get_snapshot(["AAPL", "MSFT"], client=client)

    # OHLCV candles
    df = candles.get_candles("AAPL", "1d", client=client)
    df = candles.fetch_ohlcv("AAPL", "2024-06-01", "1d", end="2024-07-01")

    # Inception dates
    dt = candles.get_inception_date("AAPL", client=client)
"""

from .client import YFinanceClient
from . import spots
