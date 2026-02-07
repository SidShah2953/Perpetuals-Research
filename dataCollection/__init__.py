"""Perpetuals research data package.

Sub-packages
------------
- ``dataCollection.hyperliquid`` -- Hyperliquid perp data (markets, candles, funding, classification)
- ``dataCollection.dydx``        -- dYdX v4 perp data (markets, candles)
- ``dataCollection.common``      -- Shared types and HTTP utilities
"""

from . import common, dydx, hyperliquid
