"""Hyperliquid perpetual futures data sub-package.

Quick-start::

    from dataCollection.hyperliquid import HyperliquidClient, markets, candles, funding, classification

    # Reuse one session for multiple calls
    client = HyperliquidClient()

    # Market metadata
    df = markets.get_markets(client)              # native DEX
    df = markets.get_all_markets(client)           # all DEXs
    df = markets.get_snapshot(client=client)        # live prices / OI / funding

    # OHLCV candles
    df = candles.get_candles("BTC", "1h", client=client)
    df = candles.get_candles_range("BTC", "1m", start=..., end=..., client=client)

    # Funding rates
    df = funding.get_current_rates(client)
    df = funding.get_funding_history("BTC", client=client)

    # Asset classification
    df = classification.classify_all(client=client)
"""

from .client import HyperliquidClient
from . import candles, classification, funding, markets
