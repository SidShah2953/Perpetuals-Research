"""dYdX v4 perpetual futures data sub-package.

Quick-start::

    from dataCollection.dydx import DydxClient, markets, candles

    client = DydxClient()

    df = markets.get_markets(client)
    df = candles.get_candles("BTC-USD", "1DAY", client=client)
    df = candles.get_inception_dates(client=client, progress=True)
"""

from .client import DydxClient
from . import candles, markets
