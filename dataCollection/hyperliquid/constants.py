API_URL = "https://api.hyperliquid.xyz/info"

# ── Asset classification maps ────────────────────────────────────────────────
# Used by classification.py to label every Hyperliquid perp by asset type.

TRADITIONAL_EQUITIES = {
    # Individual stocks
    "AAPL", "AMD", "AMZN", "BABA", "COIN", "COST", "CRCL", "CRWV",
    "GME", "GOOGL", "HOOD", "INTC", "LLY", "META", "MSFT", "MSTR",
    "MU", "NFLX", "NVDA", "ORCL", "PLTR", "RIVN", "SMSN", "SNDK",
    "TSLA", "TSM",
    # Pre-IPO / private-company trackers
    "ANTHROPIC", "OPENAI", "SPACEX",
    # ETFs
    "URNM", "USAR",
}

TRADITIONAL_COMMODITIES = {
    "ALUMINIUM", "CL", "COPPER", "GOLD", "NATGAS", "OIL",
    "PALLADIUM", "PLATINUM", "SILVER", "URANIUM", "USOIL",
}

INDICES = {
    "DXY", "KR200", "SMALL2000", "US500", "USA500", "USTECH", "XYZ100",
}

FIXED_INCOME = {
    "USBOND",
}

FOREX = {
    "EUR", "JPY",
}

SECTOR_BASKETS = {
    "BIOTECH", "DEFENSE", "ENERGY", "INFOTECH", "MAG7",
    "NUCLEAR", "ROBOT", "SEMIS", "USENERGY",
}

# DEXs where every listed asset is a crypto coin
CRYPTO_ONLY_DEXS = {"Hyperliquid (native)", "hyna"}
