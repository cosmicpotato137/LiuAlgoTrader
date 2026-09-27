"""Data type for the volume and daily change of a symbol.

Classes
-------
TickerSnapshot
    Snapshot of the trading volume and daily change of a symbol.
"""

from dataclasses import dataclass


@dataclass
class TickerSnapshot:
    """Snapshot of the trading volume and daily change of a symbol.

    Attributes
    ----------
    symbol: str
        The ticker symbol.
    volume: int
        The traded volume.
    today_change: float
        The change for the day.
    """

    symbol: str
    volume: int
    today_change: float
