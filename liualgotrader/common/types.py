"""Enumerations and broker-neutral data types shared by the framework.

Classes
-------
DataConnectorType
    Enumeration of the supported market-data providers.
BrokerType
    Enumeration of the supported brokerage integrations.
TimeScale
    Enumeration of bar time scales, valued by length in seconds.
WSEventType
    Enumeration of the event kinds received over a data web socket.
WSConnectState
    Enumeration of the connection states of a web-socket session.
AssetType
    Enumeration of the asset classes that can be traded.
Order
    Broker-neutral record of an order and its latest status event.
ThreadFlags
    Shared flag that tells a worker thread whether to keep running.
Trade
    Broker-neutral record of a single execution against an order.
QueueMapper
    Case-insensitive mapping from symbols to multiprocessing queues.
"""

from dataclasses import dataclass
from enum import Enum, auto
from multiprocessing import Queue
from typing import Dict, List, Optional

import pandas as pd


class DataConnectorType(Enum):
    """Enumeration of the supported market-data providers."""

    polygon = 1
    alpaca = 2
    finnhub = 3
    gemini = 4
    tradier = 5


class BrokerType(Enum):
    """Enumeration of the supported brokerage integrations."""

    alpaca = 1
    gemini = 2
    tradier = 3


class TimeScale(Enum):
    """Enumeration of bar time scales, valued by their length in seconds."""

    day = 24 * 60 * 60
    minute = 60


class WSEventType(Enum):
    """Enumeration of the event kinds received over a data web socket."""

    TRADE = auto()
    QUOTE = auto()
    MIN_AGG = auto()
    SEC_AGG = auto()


class WSConnectState(Enum):
    """Enumeration of the connection states of a web-socket session."""

    NOT_CONNECTED = auto()
    CONNECTED = auto()
    AUTHENTICATED = auto()


class AssetType(Enum):
    """Enumeration of the asset classes that can be traded."""

    US_EQUITIES = auto()
    CRYPTO = auto()


@dataclass
class Order:
    """A broker-neutral record of an order and its most recent status event.

    Attributes
    ----------
    order_id: str
        The broker's order identifier.
    symbol: str
        The traded symbol.
    event: EventType
        The most recent status event.
    submitted_at: pd.Timestamp
        The time the order was submitted.
    price: Optional[float]
        The limit price, or None.
    trade_fees: Optional[float]
        The fees charged, or None.
    filled_qty: Optional[float]
        The filled quantity, or None.
    side: Optional[FillSide]
        The order side, or None.
    remaining_amount: Optional[float]
        The unfilled quantity, or None.
    avg_execution_price: Optional[float]
        The average execution price, or None.
    external_account_id: Optional[str]
        The account the order was placed for, or None.
    """

    class EventType(Enum):
        """Enumeration of the status events that an order can report."""

        partial_fill = auto()
        fill = auto()
        canceled = auto()
        rejected = auto()
        cancel_rejected = auto()
        pending = auto()
        error = auto()
        open = auto()
        other = auto()

    class FillSide(Enum):
        """Enumeration of the sides on which an order can be filled."""

        buy = auto()
        sell = auto()

    order_id: str
    symbol: str
    event: EventType
    submitted_at: pd.Timestamp
    price: Optional[float] = None
    trade_fees: Optional[float] = None
    filled_qty: Optional[float] = None
    side: Optional[FillSide] = None
    remaining_amount: Optional[float] = None
    avg_execution_price: Optional[float] = None
    external_account_id: Optional[str] = None


@dataclass
class ThreadFlags:
    """A shared flag that tells a worker thread whether to keep running.

    Attributes
    ----------
    run: bool
        True while the thread should keep running.
    """

    run: bool = True


@dataclass
class Trade:
    """A broker-neutral record of a single execution against an order.

    Attributes
    ----------
    order_id: str
        The identifier of the executed order.
    symbol: str
        The traded symbol.
    event: Order.EventType
        The order status event.
    side: Order.FillSide
        The side of the execution.
    filled_qty: float
        The executed quantity.
    trade_fee: float
        The fee charged for the execution.
    filled_avg_price: float
        The average execution price.
    liquidity: str
        The liquidity indicator reported by the broker.
    updated_at: pd.Timestamp
        The time of the execution.
    """

    order_id: str
    symbol: str
    event: Order.EventType
    side: Order.FillSide
    filled_qty: float
    trade_fee: float
    filled_avg_price: float
    liquidity: str
    updated_at: pd.Timestamp


class QueueMapper:
    """A case-insensitive mapping from symbols to multiprocessing queues.

    Attributes
    ----------
    queues: Dict[str, Queue]
        The queues keyed by lowercase symbol.
    queue_list: Optional[List[Queue]]
        The known queues that assignments must use, or None.

    Methods
    -------
    get_allqueues
        Return the queues supplied at construction, or None.
    """

    def __init__(self, queue_list: List[Queue] = None):
        """Initialize an empty mapping.

        Parameters
        ----------
        queue_list: List[Queue], default None
            The known queues to validate assignments against, or None to accept
            any queue.
        """
        self.queues: Dict[str, Queue] = {}
        self.queue_list: Optional[List[Queue]] = queue_list

    def __repr__(self):
        """Return the string form of the list of mapped symbols."""
        return str(list(self.queues.keys()))

    def __getitem__(self, key: str) -> Queue:
        """Return the queue mapped to the given symbol.

        Parameters
        ----------
        key: str
            The symbol, case-insensitive.

        Raises
        ------
        Raise AssertionError if no queue is mapped to the symbol.
        """
        try:
            return self.queues[key.lower()]
        except KeyError as e:
            raise AssertionError(f"No queue exists for symbol {key}") from e

    def __setitem__(self, key: str, newvalue: Queue):
        """Map the given symbol to a queue.

        Parameters
        ----------
        key: str
            The symbol, case-insensitive.
        newvalue: Queue
            The queue to map the symbol to.

        Raises
        ------
        Raise AssertionError if known queues were given and newvalue is not one
        of them.
        """
        if self.queue_list and newvalue not in self.queue_list:
            raise AssertionError(f"key {key} added to unknown Queue")
        self.queues[key.lower()] = newvalue

    def get_allqueues(self) -> Optional[List[Queue]]:
        """Return the list of queues supplied at construction, or None."""
        return self.queue_list
