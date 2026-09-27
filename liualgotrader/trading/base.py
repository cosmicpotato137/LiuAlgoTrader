"""Broker-independent trader interface.

Classes
-------
Trader
    Abstract base class for broker trading integrations.
"""

import asyncio
import uuid
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd

from liualgotrader.common.types import Order, QueueMapper
from liualgotrader.models.algo_run import AlgoRun


class Trader:
    """Abstract base class for broker trading integrations.

    Subclasses must implement every public method except create_session,
    is_market_open and get_instance, since the other base versions only return
    None.

    Attributes
    ----------
    queues: QueueMapper
        The queue mapper that receives trade updates, or None.
    market_open
        Today's session opening time, or None.
    market_close
        Today's session closing time, or None.

    Methods
    -------
    create_session
        Create and save a new algorithm run record.
    get_market_schedule
        Return today's session opening and closing times.
    get_trading_days
        Return the trading days between two dates.
    is_market_open_today
        Return True if the market trades today.
    is_market_open
        Return True if a moment is within today's session.
    get_time_market_close
        Return the time left until today's close.
    get_position
        Return the quantity held in a symbol.
    reconnect
        Re-establish the connection to the broker.
    get_tradeable_symbols
        Return the symbols that can be traded.
    get_shortable_symbols
        Return the symbols that can be sold short.
    is_shortable
        Return True if a symbol can be sold short.
    is_order_completed
        Return the status and fill details of an order.
    is_fractionable
        Return True if a symbol can be traded fractionally.
    submit_order
        Submit a new order to the broker.
    get_order
        Return the current state of an order.
    get_account_order
        Return an order in an external brokerage account.
    cancel_order
        Request cancellation of an order.
    run
        Start the trade-update listener.
    close
        Stop the trade-update listener.
    get_instance
        Return the most recently constructed trader.
    """

    __instance: object = None

    def __init__(self, queues: QueueMapper = None):
        """Initialize the trader and make it the shared instance.

        Parameters
        ----------
        queues: QueueMapper, default None
            The queue mapper that receives trade updates, or None.
        """
        self.queues = queues
        Trader.__instance = self

    def __repr__(self):
        """Return the name of the concrete trader class."""
        return type(self).__name__

    async def create_session(self, algo_name: str) -> AlgoRun:
        """Create a new algorithm run and save it to the database.

        Parameters
        ----------
        algo_name: str
            The name of the algorithm being run.

        Returns
        -------
        Return the saved AlgoRun, which has a new random batch identifier.
        """
        new_batch_id = str(uuid.uuid4())
        algo_run = AlgoRun(algo_name, new_batch_id)
        await algo_run.save()
        return algo_run

    def get_market_schedule(
        self,
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Return the opening and closing times of today's session.

        Implementations must return an (open, close) tuple of timezone-aware
        datetimes, or (None, None) if the market does not trade today.
        """
        ...

    def get_trading_days(
        self, start_date: date, end_date: date = date.today()
    ) -> pd.DataFrame:
        """Return the trading days between two dates as a DataFrame.

        Implementations must index the DataFrame by one timestamp per trading
        day; other columns are optional.

        Parameters
        ----------
        start_date: date
            The first date of the range.
        end_date: date, default date.today()
            The last date of the range, inclusive.
        """
        ...

    def is_market_open_today(self) -> bool:
        """Return True if a trading session is scheduled for today."""
        ...

    def is_market_open(self, now: datetime) -> bool:
        """Return True if now falls within today's trading session.

        Set market_open and market_close from get_market_schedule if
        market_open is missing.

        Parameters
        ----------
        now: datetime
            The moment to test, as a timezone-aware datetime.

        Returns
        -------
        Return False if either of them is None.
        """
        if not hasattr(self, "market_open"):
            self.market_open, self.market_close = self.get_market_schedule()  # type: ignore
        return (
            False
            if not self.market_open or not self.market_close
            else self.market_open <= now <= self.market_close
        )

    def get_time_market_close(self) -> Optional[timedelta]:
        """Return the time remaining until today's market close.

        Implementations must return None if the closing time is unknown.
        """
        ...

    def get_position(self, symbol: str) -> float:
        """Return the quantity of symbol held in the account.

        Implementations must return a negative quantity for a short position.

        Parameters
        ----------
        symbol: str
            The asset symbol.

        Raises
        ------
        When no position is held, they may return zero or raise an exception,
        which the consumer treats as zero.
        """
        ...

    async def reconnect(self):
        """Re-establish the connection to the broker.

        Used by the consumer after a ConnectionError. Implementations must
        restore any client state that later broker requests need.
        """
        ...

    async def get_tradeable_symbols(self) -> List[str]:
        """Return the symbols that can be traded through the broker."""
        ...

    async def get_shortable_symbols(self) -> List[str]:
        """Return the symbols that can be sold short through the broker.

        Implementations must return an empty list if the broker does not
        support short selling.
        """
        ...

    async def is_shortable(self, symbol: str) -> bool:
        """Return True if symbol can currently be sold short.

        Parameters
        ----------
        symbol: str
            The asset symbol.
        """
        ...

    async def is_order_completed(
        self, order_id: str, external_order_id: Optional[str] = None
    ) -> Tuple[
        Order.EventType, Optional[float], Optional[float], Optional[float]
    ]:
        """Return the status and fill details of an order.

        Implementations must return a tuple of the Order.EventType, the average
        fill price, the filled quantity and the trade fee.

        Parameters
        ----------
        order_id: str
            The broker order identifier.
        external_order_id: Optional[str], default None
            The order's external account identifier, which implementations may
            ignore.
        """
        ...

    async def is_fractionable(self, symbol: str) -> bool:
        """Return True if symbol can be traded in fractional quantities.

        Parameters
        ----------
        symbol: str
            The asset symbol.
        """
        ...

    async def submit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        order_type: str,
        time_in_force: str,
        limit_price: str = None,
        stop_price: str = None,
        client_order_id: str = None,
        extended_hours: bool = None,
        order_class: str = None,
        take_profit: dict = None,
        stop_loss: dict = None,
        trail_price: str = None,
        trail_percent: str = None,
        on_behalf_of: str = None,
    ) -> Order:
        """Submit a new order to the broker.

        The optional arguments follow the Alpaca order model, and
        implementations may ignore those that the broker does not support.

        Parameters
        ----------
        symbol: str
            The asset symbol.
        qty: float
            The order quantity.
        side: str
            The order direction, buy or sell.
        order_type: str
            The order type, such as market or limit.
        time_in_force: str
            The order duration, such as day.
        limit_price: str, default None
            The limit price, for limit orders.
        stop_price: str, default None
            The stop price, for stop orders.
        client_order_id: str, default None
            A client-assigned order identifier.
        extended_hours: bool, default None
            Whether the order may fill outside regular hours.
        order_class: str, default None
            The order class, such as bracket.
        take_profit: dict, default None
            The take-profit leg of a bracket order.
        stop_loss: dict, default None
            The stop-loss leg of a bracket order.
        trail_price: str, default None
            The trailing stop offset in dollars.
        trail_percent: str, default None
            The trailing stop offset in percent.
        on_behalf_of: str, default None
            The external brokerage account to trade for, or None for the
            trader's own account.

        Returns
        -------
        Return the Order as recorded by the broker, and raise an exception if
        the request fails.
        """
        ...

    async def get_order(self, order_id: str) -> Order:
        """Return the current state of an order.

        Implementations must return an Order with the current status, filled
        quantity and prices.

        Parameters
        ----------
        order_id: str
            The broker order identifier.
        """
        ...

    async def get_account_order(
        self, external_account_id: str, order_id: str
    ) -> Order:
        """Return an order held in an external brokerage account.

        Required only of implementations that support external accounts.

        Parameters
        ----------
        external_account_id: str
            The external brokerage account.
        order_id: str
            The order identifier within that account.
        """
        ...

    async def cancel_order(self, order: Order) -> bool:
        """Request cancellation of an order with the broker.

        Implementations must return True only if the broker accepts the
        cancellation; the consumer keeps the order open otherwise.

        Parameters
        ----------
        order: Order
            The order to cancel, identified by its order_id and, where
            supported, its external_account_id.
        """
        ...

    async def run(self) -> Optional[asyncio.Task]:
        """Start the listener that receives trade updates from the broker.

        Implementations must forward order and trade events to the queues
        attribute and return a handle to the listener, such as an asyncio Task.
        A repeated call should return the existing listener.
        """
        ...

    async def close(self):
        """Stop the trade-update listener and release its connection."""
        ...

    @classmethod
    def get_instance(cls):
        """Return the most recently constructed trader.

        The instance is shared by all subclasses, so the result may be of any
        Trader type.

        Raises
        ------
        Raise AssertionError if no trader has been constructed.
        """
        if not cls.__instance:
            raise AssertionError("Must instantiate before usage")

        return cls.__instance  # type: ignore
