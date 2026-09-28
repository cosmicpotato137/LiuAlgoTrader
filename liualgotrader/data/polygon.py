"""Polygon market data and streaming providers.

Classes
-------
PolygonData
    Market data provider for the Polygon REST API.
PolygonStream
    Streaming provider for the Polygon WebSocket API.
"""

import asyncio
import json
import queue
import traceback
from datetime import date, datetime
from typing import Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from polygon import RESTClient, WebSocketClient
from polygon.exceptions import AuthError
from polygon.websocket.models import Feed, Market

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog
from liualgotrader.common.types import QueueMapper, TimeScale, WSEventType
from liualgotrader.data.data_base import DataAPI
from liualgotrader.data.streaming_base import StreamingAPI

# Polygon websocket channel prefix of each event type
_WS_CHANNELS: Dict[WSEventType, str] = {
    WSEventType.SEC_AGG: "A",
    WSEventType.MIN_AGG: "AM",
    WSEventType.TRADE: "T",
    WSEventType.QUOTE: "Q",
}

# seconds to wait for the stream task to end after closing the websocket
_CLOSE_TIMEOUT = 5.0


class PolygonData(DataAPI):
    """DataAPI implementation backed by the Polygon REST API.

    Only US equities are covered, and requests authenticate with
    config.polygon_api_key. Batch loads and trading-calendar methods are not
    supported.

    Attributes
    ----------
    polygon_rest_client
        The authenticated Polygon REST client.

    Methods
    -------
    get_market_snapshot
        Return snapshots of all equity tickers.
    get_symbols
        Return the active Polygon tickers.
    get_symbol_data
        Return historical bars for a symbol.
    get_symbols_data
        Raise NotImplementedError.
    get_last_trading
        Return the time of a symbol's last trade.
    get_trading_day
        Raise NotImplementedError.
    trading_days_slice
        Raise NotImplementedError.
    num_trading_minutes
        Raise NotImplementedError.
    num_trading_days
        Raise NotImplementedError.
    get_max_data_points_per_load
        Raise NotImplementedError.
    """

    def __init__(self):
        """Create the Polygon REST client from config.polygon_api_key.

        Raises
        ------
        Raise AssertionError if the client cannot be created, such as when
        config.polygon_api_key is not set.
        """
        try:
            self.polygon_rest_client = RESTClient(config.polygon_api_key)
        except AuthError as e:
            raise AssertionError(
                "Failed to authenticate Polygon restful client"
            ) from e

    async def get_market_snapshot(
        self, filter_func: Optional[Callable]
    ) -> List[Dict]:
        """Return the current snapshots of all equity tickers.

        Require at least a starter Polygon subscription.

        Parameters
        ----------
        filter_func: Optional[Callable]
            A predicate that selects snapshots, or None for all.

        Returns
        -------
        Return each snapshot as the dict sent by Polygon, keyed by the Polygon
        field names such as ticker, day, lastTrade, prevDay and
        todaysChangePerc.

        Raises
        ------
        Raise AssertionError if the REST client is missing.
        """
        if not self.polygon_rest_client:
            raise AssertionError("Must call w/ authenticated polygon client")
        # this API endpoint requires at least starter subscriptions from Polygon
        response = self.polygon_rest_client.get_snapshot_all(
            "stocks", raw=True
        )
        tickers = json.loads(response.data.decode("utf-8")).get("tickers", [])
        return (
            list(filter(filter_func, tickers))
            if filter_func is not None
            else tickers
        )

    def get_symbols(self) -> List[str]:
        """Return the tickers of all active Polygon symbols, in no set order.

        Raises
        ------
        Raise AssertionError if the REST client is missing.
        """
        if not self.polygon_rest_client:
            raise AssertionError("Must call w/ authenticated polygon client")
        # the client iterates over every page of the response
        tickers = self.polygon_rest_client.list_tickers(
            active=True, limit=1000
        )
        # use set to deduplicate in case pages return duplicate symbols
        return list({ticker.ticker for ticker in tickers})

    def get_symbol_data(
        self,
        symbol: str,
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> pd.DataFrame:
        """Return split-adjusted historical bars for a symbol.

        Parameters
        ----------
        symbol: str
            The ticker.
        start: date
            The first date to load.
        end: date, default date.today()
            The last date to load.
        scale: TimeScale, default TimeScale.minute
            The bar size.

        Returns
        -------
        Return a DataFrame indexed by New York time with open, high, low,
        close, volume, average, count and vwap columns; average holds the
        Polygon volume-weighted price and vwap is NaN.

        Raises
        ------
        Raise ValueError if no data is returned, or AssertionError if the REST
        client is missing.
        """
        if not self.polygon_rest_client:
            raise AssertionError("Must call w/ authenticated polygon client")

        # the client iterates over every page of the response
        aggs = list(
            self.polygon_rest_client.list_aggs(
                symbol, 1, scale.name, start, end, adjusted=True, limit=50000
            )
        )
        if not aggs:
            raise ValueError(
                f"[ERROR] {symbol} has no data for {start} to {end} w {scale.name}"
            )

        d = {
            pd.Timestamp(agg.timestamp, unit="ms", tz="America/New_York"): [
                agg.open,
                agg.high,
                agg.low,
                agg.close,
                agg.volume,
                agg.vwap,
                agg.transactions,
            ]
            for agg in aggs
        }
        df = pd.DataFrame.from_dict(
            d,
            orient="index",
            columns=[
                "open",
                "high",
                "low",
                "close",
                "volume",
                "average",
                "count",
            ],
        )
        # pandas 3 infers millisecond resolution from the timestamps
        df.index = df.index.as_unit("ns")
        df["vwap"] = np.nan
        return df

    def get_symbols_data(
        self,
        symbols: List[str],
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> Dict[str, pd.DataFrame]:
        """Raise NotImplementedError, as batch loading is unsupported.

        Parameters
        ----------
        symbols: List[str]
            The symbols to load.
        start: date
            The first date to load.
        end: date, default date.today()
            The last date to load.
        scale: TimeScale, default TimeScale.minute
            The bar size.
        """
        raise NotImplementedError("get_symbols_data")

    def get_last_trading(self, symbol: str) -> datetime:
        """Return the time of the last trade of a symbol, in New York time.

        Parameters
        ----------
        symbol: str
            The ticker.
        """
        snapshot = self.polygon_rest_client.get_snapshot_ticker(
            "stocks", symbol
        )
        return pd.Timestamp(
            snapshot.last_trade.sip_timestamp,
            unit="ns",
            tz="America/New_York",
        )

    def get_trading_day(
        self, symbol: str, now: datetime, offset: int
    ) -> datetime:
        """Raise NotImplementedError, as day offsets are unsupported.

        Parameters
        ----------
        symbol: str
            The symbol.
        now: datetime
            The reference time.
        offset: int
            The number of trading days to shift.
        """
        raise NotImplementedError("get_trading_day")

    def trading_days_slice(self, symbol: str, slice) -> slice:
        """Raise NotImplementedError, as slicing by days is unsupported.

        Parameters
        ----------
        symbol: str
            The symbol.
        slice
            The datetime slice.
        """
        raise NotImplementedError("trading_days_slice")

    def num_trading_minutes(self, symbol: str, start: date, end: date) -> int:
        """Raise NotImplementedError, as counting minutes is unsupported.

        Parameters
        ----------
        symbol: str
            The symbol.
        start: date
            The first date.
        end: date
            The last date.
        """
        raise NotImplementedError("num_trading_minutes")

    def num_trading_days(self, symbol: str, start: date, end: date) -> int:
        """Raise NotImplementedError, as counting days is unsupported.

        Parameters
        ----------
        symbol: str
            The symbol.
        start: date
            The first date.
        end: date
            The last date.
        """
        raise NotImplementedError("num_trading_days")

    def get_max_data_points_per_load(self) -> int:
        """Raise NotImplementedError, as no load limit is defined."""
        raise NotImplementedError("get_max_data_points_per_load")


class PolygonStream(StreamingAPI):
    """StreamingAPI implementation backed by the Polygon websocket.

    Aggregate, trade and quote events are relayed to the per-symbol queues with
    descriptive field names added. The connection starts when run is called,
    and subscriptions made before then take effect once it is established.

    Attributes
    ----------
    polygon_ws_client
        The Polygon websocket client.
    task: Optional[asyncio.Task]
        The background streaming task, or None before run.

    Methods
    -------
    subscribe
        Subscribe symbols to event types.
    run
        Start streaming in the background.
    unsubscribe
        Unsubscribe a symbol from every event type.
    close
        Close the websocket connection.
    handle_event
        Put a normalized event on its symbol's queue.
    process_message
        Handle the supported events in a message.
    on_error
        Log a websocket error.
    on_close
        Log the websocket close status.
    """

    def __init__(self, queues: QueueMapper):
        """Create the websocket client and register the shared instance.

        Parameters
        ----------
        queues: QueueMapper
            The mapping from each symbol to the queue for its events.

        Raises
        ------
        Raise AssertionError if the client cannot be created, such as when
        config.polygon_api_key is not set.
        """
        try:
            self.polygon_ws_client = WebSocketClient(
                api_key=config.polygon_api_key,
                feed=Feed.RealTime,
                market=Market.Stocks,
                raw=True,
            )
        except AuthError as e:
            raise AssertionError(
                "Failed to authenticate Polygon web_socket client"
            ) from e
        self.task: Optional[asyncio.Task] = None
        super().__init__(queues)

    async def subscribe(
        self, symbols: List[str], events: List[WSEventType]
    ) -> bool:
        """Subscribe each symbol to each of the given event types.

        Parameters
        ----------
        symbols: List[str]
            The symbols to subscribe, in any case.
        events: List[WSEventType]
            The aggregate, trade and quote event types.

        Returns
        -------
        Return True.
        """
        args = [
            f"{_WS_CHANNELS[event]}.{symbol.upper()}"
            for symbol in symbols
            for event in events
        ]

        tlog(f"subscribe(): adding subscription {args}")
        self.polygon_ws_client.subscribe(*args)

        return True

    async def run(self):
        """Start streaming in the background unless a stream is running."""
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(
                self._stream(), name="PolygonStream"
            )

    async def unsubscribe(self, symbol: str) -> bool:
        """Unsubscribe a symbol from every event type.

        Parameters
        ----------
        symbol: str
            The symbol to unsubscribe, in any case.

        Returns
        -------
        Return True.
        """
        args = [
            f"{channel}.{symbol.upper()}" for channel in _WS_CHANNELS.values()
        ]

        tlog(f"unsubscribe(): removing subscription {args}")
        self.polygon_ws_client.unsubscribe(*args)

        return True

    async def close(
        self,
    ) -> None:
        """Close the Polygon websocket connection and stop streaming."""
        if self.polygon_ws_client.websocket:
            await self.polygon_ws_client.close()
            if self.task:
                await asyncio.wait({self.task}, timeout=_CLOSE_TIMEOUT)

        if self.task and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    async def _stream(self) -> None:
        """Relay Polygon messages to the queues until the connection ends.

        Report errors to on_error and the end of the connection to on_close
        instead of raising them.
        """
        try:
            await self.polygon_ws_client.connect(PolygonStream._on_message)
        except Exception as e:
            PolygonStream.on_error(self.polygon_ws_client.websocket, e)
        finally:
            ws = self.polygon_ws_client.websocket
            PolygonStream.on_close(
                ws,
                getattr(ws, "close_code", None),
                getattr(ws, "close_reason", None),
            )

    @classmethod
    async def _on_message(cls, message: Union[str, bytes]) -> None:
        """Handle one message, reporting any error to on_error.

        The message is handled in a worker thread, so a full queue does not
        block the event loop.

        Parameters
        ----------
        message: Union[str, bytes]
            The JSON text of a list of events.
        """
        try:
            await asyncio.to_thread(cls.process_message, message)
        except Exception as e:
            cls.on_error(None, e)

    @classmethod
    def handle_event(cls, event: Dict):
        """Put a Polygon event on the queue for its symbol.

        Print the event to standard output.

        Parameters
        ----------
        event: Dict
            The Polygon event; descriptive field names such as open and symbol
            are added to it in place.

        Raises
        ------
        Raise queue.Full if the queue stays full for one second; log and
        suppress any other error.
        """
        print("event:", event)
        try:
            event["EV"] = event["ev"]
            if "s" in event:
                event["start"] = event["s"]
            if "o" in event:
                event["open"] = event["o"]
            if "h" in event:
                event["high"] = event["h"]
            if "l" in event:
                event["low"] = event["l"]
            if "c" in event:
                event["close"] = event["c"]
            if "v" in event:
                event["volume"] = event["v"]
            if "vw" in event:
                event["vwap"] = event["vw"]
            if "a" in event:
                event["average"] = event["a"]
            if "av" in event:
                event["totalvolume"] = event["av"]
            if "sym" in event:
                event["symbol"] = event["sym"]
            if "z" in event:
                event["count"] = event["z"]
            cls.get_instance().queues[event["sym"]].put(event, timeout=1)
        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}, sleeping for 2 seconds and re-trying."
            )
            raise
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            traceback.print_exc()

    @classmethod
    def process_message(cls, message):
        """Handle each aggregate, trade and quote event in a message.

        Ignore events of other types.

        Parameters
        ----------
        message
            The JSON text of a list of events, as str or bytes.
        """
        payload = json.loads(message)
        for event in payload:
            if event["ev"] in ("A", "AM", "T", "Q"):
                cls.handle_event(event)

    @classmethod
    def on_error(cls, ws, error):
        """Log an error reported by the websocket client.

        Parameters
        ----------
        ws
            The websocket; unused.
        error
            The reported error.
        """
        tlog(f"[ERROR] on_error(): {error}")

    @classmethod
    def on_close(cls, ws, close_status_code, close_msg):
        """Log the status code and message sent when the connection closes.

        Parameters
        ----------
        ws
            The websocket; unused.
        close_status_code
            The close status code.
        close_msg
            The close message.
        """
        tlog(f"[INFO] on_close() called with {close_status_code, close_msg}")
