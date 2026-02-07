"""Hyperliquid perpetual futures data sub-package.

Quick-start::

    from dataCollection.hyperliquid import HyperliquidClient
    from dataCollection.hyperliquid.perpetuals import markets, candles, funding, classification

    client = HyperliquidClient()

    # Market metadata
    df = markets.get_markets(client)              # native DEX
    df = markets.get_all_markets(client)           # all DEXs
    df = markets.get_snapshot(client=client)        # live prices / OI / funding

    # OHLCV candles
    df = candles.get_candles("BTC", "1h", client=client)
    df = candles.fetch_ohlcv("BTC", "2024-06-01", "1h", end="2024-07-01")

    # Funding rates
    df = funding.get_current_rates(client)
    df = funding.get_funding_history("BTC", client=client)

    # Asset classification
    df = classification.classify_all(client=client)
"""

from . import candles, classification, funding, markets
