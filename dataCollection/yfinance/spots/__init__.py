"""Yahoo Finance spot data sub-package.

Quick-start::

    from dataCollection.yfinance import YFinanceClient
    from dataCollection.yfinance.spots import markets, candles

    client = YFinanceClient()

    df = markets.get_markets(["AAPL", "MSFT", "GOOGL"], client=client)
    df = markets.get_snapshot(["AAPL", "MSFT"], client=client)
    df = candles.fetch_ohlcv("AAPL", "2024-01-01", "1d", end="2024-07-01")
    dt = candles.get_inception_date("AAPL", client=client)
"""

from . import candles, markets
