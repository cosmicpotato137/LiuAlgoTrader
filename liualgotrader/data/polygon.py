"""Polygon market data and streaming providers.

Classes
-------
PolygonData
    Market data provider for the Polygon REST API.
PolygonStream
    Streaming provider for the Polygon WebSocket API.
"""

import json
import queue
import traceback
from datetime import date, datetime
from typing import Callable, Dict, List, Optional

import numpy as np
import pandas as pd
import requests
from polygon import RESTClient, WebSocketClient

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog
from liualgotrader.common.types import QueueMapper, TimeScale, WSEventType
from liualgotrader.data.data_base import DataAPI
from liualgotrader.data.streaming_base import StreamingAPI


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
        Raise AssertionError if the client cannot be created.
        """
        self.polygon_rest_client = RESTClient(config.polygon_api_key)
        if not self.polygon_rest_client:
            raise AssertionError(
                "Failed to authenticate Polygon restful client"
            )

    async def get_market_snapshot(
        self, filter_func: Optional[Callable]
    ) -> List[Dict]:
        """Return the current snapshots of all equity tickers.

        Require at least a starter Polygon subscription.

        Parameters
        ----------
        filter_func: Optional[Callable]
            A predicate that selects snapshots, or None for all.

        Raises
        ------
        Raise AssertionError if the REST client is missing.
        """
        if not self.polygon_rest_client:
            raise AssertionError("Must call w/ authenticated polygon client")
        # this API endpoint requires at least starter subscriptions from Polygon
        data = self.polygon_rest_client.stocks_equities_snapshot_all_tickers()
        return (
            list(filter(filter_func, data.tickers))
            if filter_func is not None
            else data.tickers
        )

    def get_symbols(self) -> List[str]:
        """Return the tickers of all active Polygon symbols, in no set order.

        Raises
        ------
        Raise AssertionError if the REST client is missing.
        """
        if not self.polygon_rest_client:
            raise AssertionError("Must call w/ authenticated polygon client")
        # parse symbols on the first page
        data = self.polygon_rest_client.reference_tickers_v3(
            limit=1000, active=True
        )
        # use set to deduplicate in case paginated response return duplicate symbols
        symbols = {d["ticker"] for d in data.results}
        next_url = f"{data.next_url}&apiKey={config.polygon_api_key}"
        # parse the pagination
        while True:
            response = requests.get(next_url).json()
            if "next_url" not in response:
                break
            symbols.update([d["ticker"] for d in response["results"]])
            next_url = (
                f"{response['next_url']}&apiKey={config.polygon_api_key}"
            )
        return list(symbols)

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

        data = self.polygon_rest_client.stocks_equities_aggregates(
            symbol, 1, scale.name, start, end, unadjusted=False, limit=50000
        )
        if not data or not hasattr(data, "results"):
            raise ValueError(
                f"[ERROR] {symbol} has no data for {start} to {end} w {scale.name}"
            )

        d = {
            pd.Timestamp(result["t"], unit="ms", tz="America/New_York"): [
                result.get("o"),
                result.get("h"),
                result.get("l"),
                result.get("c"),
                result.get("v"),
                result.get("vw"),
                result.get("n"),
            ]
            for result in data.results
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
        df["vwap"] = np.NaN
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
        snapshot_data = (
            self.polygon_rest_client.stocks_equities_snapshot_single_ticker(
                symbol
            )
        )
        return pd.Timestamp(
            snapshot_data.ticker.last_trade.timestamp_of_this_trade,
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
    descriptive field names added. The connection starts when the object is
    created.

    Attributes
    ----------
    polygon_ws_client
        The Polygon websocket client.

    Methods
    -------
    subscribe
        Subscribe symbols to event types.
    run
        Do nothing, as the client starts on creation.
    unsubscribe
        Raise NotImplementedError.
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
        """Start the Polygon websocket client and register the shared instance.

        Parameters
        ----------
        queues: QueueMapper
            The mapping from each symbol to the queue for its events.

        Raises
        ------
        Raise AssertionError if the client cannot be created.
        """
        self.polygon_ws_client = WebSocketClient(
            auth_key=config.polygon_api_key,
            process_message=PolygonStream.process_message,
            on_close=PolygonStream.on_close,
            on_error=PolygonStream.on_error,
        )
        if not self.polygon_ws_client:
            raise AssertionError(
                "Failed to authenticate Polygon web_socket client"
            )
        self.polygon_ws_client.run()
        super().__init__(queues)

    async def subscribe(
        self, symbols: List[str], events: List[WSEventType]
    ) -> bool:
        """Subscribe each symbol to each of the given event types.

        Parameters
        ----------
        symbols: List[str]
            The symbols to subscribe.
        events: List[WSEventType]
            The aggregate, trade and quote event types.

        Returns
        -------
        Return True.
        """
        args = []
        for symbol in symbols:
            for event in events:
                if event == WSEventType.SEC_AGG:
                    action = "A"
                elif event == WSEventType.MIN_AGG:
                    action = "AM"
                elif event == WSEventType.TRADE:
                    action = "T"
                elif event == WSEventType.QUOTE:
                    action = "Q"

                args.append(f"{action}.{symbol}")

        tlog(f"subscribe(): adding subscription {args}")
        self.polygon_ws_client.subscribe(*args)

        return True

    async def run(self):
        """Do nothing, as the websocket client starts on creation."""
        pass

    async def unsubscribe(self, symbol: str) -> bool:
        """Raise NotImplementedError, as unsubscribing is unsupported.

        Parameters
        ----------
        symbol: str
            The symbol to unsubscribe.
        """
        raise NotImplementedError("not implemented yet")

    async def close(
        self,
    ) -> None:
        """Close the Polygon websocket connection."""
        self.polygon_ws_client.close_connection()

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
            The JSON text of a list of events.
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
