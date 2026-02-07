"""Perpetuals & spot research data package.

Sub-packages
------------
- ``dataCollection.hyperliquid``              -- Hyperliquid data (perps + spot)
  - ``dataCollection.hyperliquid.perpetuals`` -- Perp markets, candles, funding, classification
  - ``dataCollection.hyperliquid.spots``      -- Spot tokens, pairs, candles
- ``dataCollection.dydx``                     -- dYdX v4 perp data (markets, candles)
- ``dataCollection.yfinance``                 -- Yahoo Finance data (spot)
  - ``dataCollection.yfinance.spots``         -- Spot markets, candles, inception dates
- ``dataCollection.common``                   -- Shared types and HTTP utilities
"""

from . import common, dydx, hyperliquid, yfinance
