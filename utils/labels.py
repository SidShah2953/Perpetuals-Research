"""Helper functions for consistent labeling across asset types."""


def get_labels(asset_type: str) -> dict[str, str]:
    """Return appropriate labels for onchain/offchain data based on asset type.

    Parameters
    ----------
    asset_type : str
        The type of asset (e.g., "Crypto Coin", "Traditional Commodity", "Traditional Equity")

    Returns
    -------
    dict[str, str]
        Dictionary with keys:
        - onchain: Short label for decentralized data (e.g., "DEX" or "DeFi")
        - offchain: Short label for centralized/traditional data (e.g., "CEX" or "TradFi")
        - onchain_long: Longer descriptive label for decentralized data
        - offchain_long: Longer descriptive label for centralized/traditional data

    Examples
    --------
    >>> get_labels("Crypto Coin")
    {'onchain': 'DEX', 'offchain': 'CEX', 'onchain_long': 'DEX Perpetuals', 'offchain_long': 'CEX Spot'}

    >>> get_labels("Traditional Commodity")
    {'onchain': 'DeFi', 'offchain': 'TradFi', 'onchain_long': 'DeFi Perpetuals', 'offchain_long': 'Traditional Finance'}

    Notes
    -----
    Terminology conventions:
    - Crypto assets: 'DEX' vs 'CEX' (Decentralized vs Centralized Exchanges)
    - Traditional assets: 'DeFi' vs 'TradFi' (Decentralized vs Traditional Finance)
    """
    if "Crypto" in asset_type:
        return {
            "onchain": "DEX",
            "offchain": "CEX",
            "onchain_long": "DEX Perpetuals",
            "offchain_long": "CEX Spot"
        }
    else:
        return {
            "onchain": "DeFi",
            "offchain": "TradFi",
            "onchain_long": "DeFi Perpetuals",
            "offchain_long": "Traditional Finance"
        }
