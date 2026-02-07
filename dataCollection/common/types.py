from enum import Enum


class AssetType(str, Enum):
    CRYPTO = "Crypto Coin"
    EQUITY = "Traditional Equity"
    COMMODITY = "Traditional Commodity"
    INDEX = "Index"
    FIXED_INCOME = "Fixed Income"
    FOREX = "Forex"
    SECTOR_BASKET = "Sector Basket"


class Timeframe(str, Enum):
    """Candle intervals supported by Hyperliquid (also usable for dYdX mapping)."""
    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    H8 = "8h"
    H12 = "12h"
    D1 = "1d"
    D3 = "3d"
    W1 = "1w"
    MO1 = "1M"


# Mapping from Timeframe to approximate milliseconds (for pagination math)
TIMEFRAME_MS = {
    Timeframe.M1: 60_000,
    Timeframe.M3: 180_000,
    Timeframe.M5: 300_000,
    Timeframe.M15: 900_000,
    Timeframe.M30: 1_800_000,
    Timeframe.H1: 3_600_000,
    Timeframe.H2: 7_200_000,
    Timeframe.H4: 14_400_000,
    Timeframe.H8: 28_800_000,
    Timeframe.H12: 43_200_000,
    Timeframe.D1: 86_400_000,
    Timeframe.D3: 259_200_000,
    Timeframe.W1: 604_800_000,
    Timeframe.MO1: 2_592_000_000,
}
