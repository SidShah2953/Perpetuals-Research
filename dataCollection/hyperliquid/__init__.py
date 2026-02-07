"""Hyperliquid data sub-package (perpetuals + spot).

Sub-packages
------------
- ``dataCollection.hyperliquid.perpetuals`` -- Perp markets, candles, funding, classification
- ``dataCollection.hyperliquid.spots``      -- Spot tokens, pairs, candles

Quick-start::

    from dataCollection.hyperliquid import HyperliquidClient
    from dataCollection.hyperliquid.perpetuals import markets, candles, funding, classification
    from dataCollection.hyperliquid.spots import markets as spot_markets, candles as spot_candles

    client = HyperliquidClient()
"""

from .client import HyperliquidClient
from . import perpetuals, spots
