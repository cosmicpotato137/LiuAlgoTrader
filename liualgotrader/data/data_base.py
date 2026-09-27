"""Abstract interface for historical market data providers.

Classes
-------
DataAPI
    Abstract base class for historical market data providers.
"""

import math
from abc import ABCMeta, abstractmethod
from datetime import date, datetime
from typing import Awaitable, Callable, Dict, List, Optional

import pandas as pd

from liualgotrader.common.types import TimeScale


class DataAPI(metaclass=ABCMeta):
    """Abstract base class for historical market data providers.

    Subclasses must implement every method except data_concurrency_ranges.

    Attributes
    ----------
    ws_uri: str
        The WebSocket URI.
    ws_msgs_handler: Awaitable
        The WebSocket message handler.

    Methods
    -------
    get_symbol_data
        Return historical bars for one symbol.
    get_market_snapshot
        Return snapshots of the current market.
    get_symbols
        Return the symbols that the provider offers.
    get_symbols_data
        Return historical bars for several symbols.
    get_last_trading
        Return the time of the latest trading activity.
    get_trading_day
        Return a datetime shifted by trading days.
    trading_days_slice
        Adjust a datetime slice to trading sessions.
    num_trading_minutes
        Return the trading minutes in one day.
    num_trading_days
        Return the trading days in a date range.
    get_max_data_points_per_load
        Return the data point limit per request.
    data_concurrency_ranges
        Split a date range for concurrent loading.
    """

    def __init__(self, ws_uri: str, ws_messages_handler: Awaitable):
        """Initialize the provider.

        Parameters
        ----------
        ws_uri: str
            The WebSocket URI.
        ws_messages_handler: Awaitable
            The WebSocket message handler.
        """
        self.ws_uri = ws_uri
        self.ws_msgs_handler = ws_messages_handler

    @abstractmethod
    def get_symbol_data(
        self,
        symbol: str,
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> pd.DataFrame:
        """Return historical bars for a symbol over a date range.

        Parameters
        ----------
        symbol: str
            The symbol to load.
        start: date
            The start of the date range.
        end: date, default date.today()
            The end of the date range.
        scale: TimeScale, default TimeScale.minute
            The bar resolution.

        Raises
        ------
        Implementations must return a DataFrame indexed by timestamp with at
        least the open, high, low, close and volume columns, and should raise
        ValueError if no data is available.
        """
        ...

    @abstractmethod
    async def get_market_snapshot(
        self, filter_func: Optional[Callable]
    ) -> List[Dict]:
        """Return snapshots of the current market.

        Implementations must return a list of dictionaries, each with a
        "ticker" key.

        Parameters
        ----------
        filter_func: Optional[Callable]
            A predicate that selects the snapshots to keep, or None to keep
            all.
        """
        ...

    @abstractmethod
    def get_symbols(self) -> List[str]:
        """Return the symbols that the provider offers."""
        ...

    @abstractmethod
    def get_symbols_data(
        self,
        symbols: List[str],
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> Dict[str, pd.DataFrame]:
        """Return historical bars for several symbols over a date range.

        Implementations must return a dictionary that maps each symbol to a
        DataFrame of the form returned by get_symbol_data.

        Parameters
        ----------
        symbols: List[str]
            The symbols to load.
        start: date
            The start of the date range.
        end: date, default date.today()
            The end of the date range.
        scale: TimeScale, default TimeScale.minute
            The bar resolution.
        """
        ...

    @abstractmethod
    def get_last_trading(self, symbol: str) -> datetime:
        """Return the time of the most recent trading activity for a symbol.

        Parameters
        ----------
        symbol: str
            The symbol to look up.
        """
        ...

    @abstractmethod
    def get_trading_day(
        self, symbol: str, now: datetime, offset: int
    ) -> datetime:
        """Return a reference time shifted by a number of trading days.

        The exact counting convention is defined by each implementation.

        Parameters
        ----------
        symbol: str
            The symbol whose trading calendar applies.
        now: datetime
            The reference datetime.
        offset: int
            The number of trading days to shift by.
        """
        ...

    @abstractmethod
    def trading_days_slice(self, symbol: str, slice) -> slice:
        """Return a datetime slice adjusted to the trading sessions it spans.

        Implementations may return the slice unchanged when no adjustment is
        needed.

        Parameters
        ----------
        symbol: str
            The symbol whose trading calendar applies.
        slice
            The datetime slice to adjust.
        """
        ...

    @abstractmethod
    def num_trading_minutes(self, symbol: str, start: date, end: date) -> int:
        """Return the number of trading minutes in one day for a symbol.

        Parameters
        ----------
        symbol: str
            The symbol whose trading hours apply.
        start: date
            The start of the date range.
        end: date
            The end of the date range.
        """
        ...

    @abstractmethod
    def num_trading_days(self, symbol: str, start: date, end: date) -> int:
        """Return the number of trading days in a date range for a symbol.

        Parameters
        ----------
        symbol: str
            The symbol whose trading calendar applies.
        start: date
            The first date of the range.
        end: date
            The last date of the range.
        """
        ...

    @abstractmethod
    def get_max_data_points_per_load(self) -> int:
        """Return the maximum number of data points to request in one load."""
        ...

    def data_concurrency_ranges(
        self, symbol: str, start: date, end: date, scale: TimeScale
    ) -> List[Optional[pd.DatetimeIndex]]:
        """Split a date range into boundaries for concurrent data loading.

        Each pair of consecutive timestamps bounds a range whose estimated size
        does not exceed get_max_data_points_per_load.

        Parameters
        ----------
        symbol: str
            The symbol to load.
        start: date
            The start of the date range.
        end: date
            The end of the date range.
        scale: TimeScale
            The bar resolution.

        Returns
        -------
        Return evenly spaced timestamps from start to end.
        """
        scale_factor_minutes = self.num_trading_minutes(symbol, start, end)
        data_points = (
            scale_factor_minutes / 60
            if scale == TimeScale.day
            else scale_factor_minutes
        )
        total_data_points = (
            self.num_trading_days(symbol, start, end) * data_points
        )
        periods = math.ceil(
            total_data_points / self.get_max_data_points_per_load()
        )
        return pd.date_range(start, end, periods=periods + 1)
