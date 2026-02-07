"""Hyperliquid spot trading data sub-package.

Quick-start::

    from dataCollection.hyperliquid import HyperliquidClient
    from dataCollection.hyperliquid.spots import markets, candles

    client = HyperliquidClient()

    # Token & pair metadata
    df = markets.get_tokens(client)                # all spot tokens
    df = markets.get_pairs(client)                 # all trading pairs
    df = markets.get_snapshot(client=client)        # live prices / volume

    # OHLCV candles
    df = candles.get_candles("HYPE/USDC", "1h", client=client)
    df = candles.fetch_ohlcv("PURR/USDC", "2024-06-01", "1h", end="2024-07-01")

    # Inception dates
    dt = candles.get_inception_date("HYPE/USDC", client=client)
"""

from . import candles, markets
