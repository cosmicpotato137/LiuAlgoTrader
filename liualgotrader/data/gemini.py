"""Gemini cryptocurrency exchange market data and streaming providers.

Classes
-------
GeminiData
    Market data provider for the Gemini cryptocurrency exchange.
GeminiStream
    Streaming provider for Gemini trades.

Functions
---------
requests_get
    Return the response to an unauthenticated GET request.
"""

import asyncio
import base64
import concurrent.futures
import hashlib
import hmac
import json
import os
import queue
import ssl
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from threading import Thread
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pytz
import requests
import websocket

from liualgotrader.common import config
from liualgotrader.common.list_utils import chunks
from liualgotrader.common.tlog import tlog
from liualgotrader.common.types import (QueueMapper, TimeScale, Trade,
                                        WSEventType)
from liualgotrader.data.data_base import DataAPI
from liualgotrader.data.streaming_base import StreamingAPI

utctz = pytz.timezone("UTC")


def requests_get(url):
    """Return the response to an unauthenticated GET request.

    Retry without limit, pausing between attempts, while the server responds
    with status 429 or 502.

    Parameters
    ----------
    url
        The URL to request.
    """
    r = requests.get(url)

    if r.status_code in (429, 502):
        tlog(f"{url} return {r.status_code}, waiting and re-trying")
        time.sleep(10)
        return requests_get(url)

    return r


class GeminiData(DataAPI):
    """DataAPI implementation for the Gemini cryptocurrency exchange.

    Bars are built from the public Gemini trade history, and every calendar day
    counts as a trading day. Snapshots, batch loads and trading-day counts are
    not supported.

    Attributes
    ----------
    running_task: Optional[Thread]
        Reserved for a background thread; always None.
    ws
        Reserved for a websocket connection; always None.

    Methods
    -------
    get_symbols
        Return the symbols that Gemini supports.
    get_market_snapshot
        Raise NotImplementedError.
    aget_symbol_data
        Asynchronously return historical bars for a symbol.
    get_symbols_data
        Raise NotImplementedError.
    trading_days_slice
        Return a slice unchanged.
    get_last_trading
        Return the current UTC time.
    get_trading_day
        Return midnight UTC a number of days from a date.
    get_symbol_data
        Return historical bars for a symbol.
    num_trading_minutes
        Raise NotImplementedError.
    num_trading_days
        Raise NotImplementedError.
    get_max_data_points_per_load
        Raise NotImplementedError.
    """

    gemini_api_key: Optional[str] = os.getenv("GEMINI_API_KEY")
    gemini_api_secret: Optional[str] = os.getenv("GEMINI_API_SECRET")
    base_url = "https://api.gemini.com"
    base_websocket = "wss://api.gemini.com"
    datapoints_per_request = 500
    max_trades_per_minute = 10

    def __init__(self):
        """Initialize the data provider."""
        self.running_task: Optional[Thread] = None
        self.ws = None

    def get_symbols(self) -> List[str]:
        """Return the symbols that Gemini supports.

        Raises
        ------
        Raise AssertionError if the request returns an HTTP error status.
        """
        endpoint = "/v1/symbols"
        url = self.base_url + endpoint
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()

        raise AssertionError(
            f"HTTP ERROR {response.status_code} {response.text}"
        )

    async def get_market_snapshot(self, filter_func) -> List[Dict]:
        """Raise NotImplementedError, as market snapshots are unsupported.

        Parameters
        ----------
        filter_func
            A predicate that selects snapshots, or None.
        """
        raise NotImplementedError

    def _get_ranges(self, start, end):
        """Return the start times of the trade requests for a date range.

        Parameters
        ----------
        start
            The first date.
        end
            The last date, covered through its final microsecond.

        Returns
        -------
        Return an evenly spaced DatetimeIndex in UTC whose entries are about
        datapoints_per_request / max_trades_per_minute minutes apart.
        """
        start_t = datetime.combine(start, datetime.min.time(), tzinfo=utctz)
        end_t = datetime.combine(end, datetime.max.time(), tzinfo=utctz)

        minutes = (end_t - start_t).total_seconds() / 60
        return pd.date_range(
            start_t,
            end_t,
            periods=minutes
            / (self.datapoints_per_request / self.max_trades_per_minute),
        )

    async def _consolidate_response(self, response, scale) -> pd.DataFrame:
        """Return the trades in a Gemini response as bars.

        Omit intervals without trades, and return an empty DataFrame if there
        are no trades.

        Parameters
        ----------
        response
            The requests.Response holding a list of trades.
        scale
            TimeScale.minute, or any other value for daily bars.

        Returns
        -------
        Return a DataFrame indexed by UTC time with open, high, low, close,
        count and volume columns, and with average and vwap set to zero.
        """
        _df = pd.DataFrame(response.json())

        if _df.empty:
            return pd.DataFrame()

        _df = _df.set_index(_df.timestamp).sort_index()

        _df["s"] = pd.to_datetime(
            (_df.index * 1e9).astype("int64"),
            utc=True,
        )

        # _df.timestamp.apply(lambda x: pd.Timestamp(x, tz=utctz, unit="ns"))
        _df["amount"] = pd.to_numeric(_df.amount)
        _df["price"] = pd.to_numeric(_df.price)
        _df = _df[["s", "price", "amount"]].set_index("s")

        rule = "T" if scale == TimeScale.minute else "D"
        _newdf = _df.resample(rule).first()
        _newdf["high"] = _df.resample(rule).max().price
        _newdf["low"] = _df.resample(rule).min().price
        _newdf["close"] = _df.resample(rule).last().price
        _newdf["count"] = _df.resample(rule).count().amount
        _newdf["volume"] = _df.resample(rule).sum().amount
        _newdf = (
            _newdf.rename(columns={"price": "open", "s": "timestamp"})
            .drop(columns=["amount"])
            .sort_index()
        )
        _newdf = _newdf.dropna()
        _newdf["average"] = 0.0
        _newdf["vwap"] = 0.0

        return _newdf

    async def aget_symbol_data(
        self,
        symbol: str,
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> pd.DataFrame:
        """Asynchronously return historical bars for a symbol.

        Parameters
        ----------
        symbol: str
            The symbol, in any letter case.
        start: date
            The first date to load.
        end: date, default date.today()
            The last date to load.
        scale: TimeScale, default TimeScale.minute
            TimeScale.minute, or any other value for daily bars.

        Returns
        -------
        Return a DataFrame of bars indexed by UTC time, or an empty DataFrame
        if no trades were found.

        Raises
        ------
        Raise ValueError if a request returns an HTTP error status.
        """
        symbol = symbol.lower()
        tlog(
            f"GEMINI start loading {symbol} from {start} to {end} w scale {scale}"
        )
        ranges = self._get_ranges(start, end)
        endpoint = f"/v1/trades/{symbol}"
        returned_df: pd.DataFrame = pd.DataFrame()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor,
                    requests_get,
                    f"{self.base_url}{endpoint}?timestamp={int(current_timestamp.timestamp())}&limit_trades=500",
                )
                for current_timestamp in ranges[:-1]
            ]

            for response in await asyncio.gather(*futures):
                if response.status_code != 200:
                    raise ValueError(
                        f"HTTP ERROR {response.status_code} {response.text}"
                    )

                df = await self._consolidate_response(response, scale)

                if df.empty:
                    continue
                if returned_df.empty:
                    returned_df = df
                else:
                    returned_df = pd.concat([returned_df, df])
                    returned_df = returned_df[
                        ~returned_df.index.duplicated(keep="first")
                    ]

        tlog(
            f"GEMINI completed loading {symbol} from {start} to {end} w scale {scale}"
        )
        return returned_df

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
        raise NotImplementedError(
            "get_symbols_data() not implemented yet for Gemini data provider"
        )

    def trading_days_slice(self, symbol: str, s: slice) -> slice:
        """Return a slice unchanged, as every day is a trading day.

        Parameters
        ----------
        symbol: str
            Ignored.
        s: slice
            The slice to return.
        """
        return s

    def get_last_trading(self, symbol: str) -> datetime:
        """Return the current UTC time.

        Parameters
        ----------
        symbol: str
            Ignored.
        """
        return datetime.now(timezone.utc)

    def get_trading_day(
        self, symbol: str, now: datetime, offset: int
    ) -> datetime:
        """Return midnight UTC on the day offset calendar days from now.

        Every calendar day counts as a trading day.

        Parameters
        ----------
        symbol: str
            Ignored.
        now: datetime
            The reference date or datetime.
        offset: int
            The number of calendar days to add.
        """
        return (
            utctz.localize(datetime.combine(now, datetime.min.time()))
            if isinstance(now, date)
            else now
        ) + timedelta(days=offset)

    def get_symbol_data(
        self,
        symbol: str,
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> pd.DataFrame:
        """Return historical bars for a symbol.

        Do not call this method from asynchronous code.

        Parameters
        ----------
        symbol: str
            The symbol, in any letter case.
        start: date
            The first date to load.
        end: date, default date.today()
            The last date to load.
        scale: TimeScale, default TimeScale.minute
            TimeScale.minute, or any other value for daily bars.

        Returns
        -------
        Return the same DataFrame as aget_symbol_data.
        """
        symbol = symbol.lower()
        return asyncio.run(self.aget_symbol_data(symbol, start, end, scale))

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


class GeminiStream(StreamingAPI):
    """StreamingAPI implementation for Gemini trades.

    Only btcusd trades are streamed, whatever the subscriptions. The connection
    authenticates with the GEMINI_API_KEY and GEMINI_API_SECRET environment
    variables.

    Attributes
    ----------
    running_task: Optional[Thread]
        The websocket thread, or None.
    ws
        The websocket connection, or None.

    Methods
    -------
    run
        Start the websocket on a background thread.
    close
        Stop the websocket thread.
    on_message
        Dispatch the trades in a websocket message.
    on_error
        Log a websocket error.
    on_close
        Log the websocket close status.
    trades_handler
        Put a normalized trade event on the btcusd queue.
    subscribe
        Return True without subscribing.
    """

    gemini_api_key: Optional[str] = os.getenv("GEMINI_API_KEY")
    gemini_api_secret: Optional[str] = os.getenv("GEMINI_API_SECRET")
    base_url = "https://api.gemini.com"
    base_websocket = "wss://api.gemini.com"

    def __init__(self, queues: QueueMapper):
        """Initialize the stream and register it as the shared instance.

        Parameters
        ----------
        queues: QueueMapper
            The mapping from each symbol to the queue for its events.
        """
        self.running_task: Optional[Thread] = None
        self.ws = None
        super().__init__(queues)

    def _generate_ws_headers(self, payload: Dict) -> Dict:
        """Return the Gemini authentication headers for a websocket request.

        Parameters
        ----------
        payload: Dict
            The request payload; a nonce is added to it in place.

        Raises
        ------
        Raise AssertionError if the API key or secret is not set.
        """
        if not self.gemini_api_secret or not self.gemini_api_key:
            raise AssertionError(
                "both env variables GEMINI_API_KEY and GEMINI_API_SECRET must be set up"
            )
        t = datetime.now()
        payload_nonce = str(int(time.mktime(t.timetuple()) * 1000))
        payload["nonce"] = payload_nonce
        encoded_payload = json.dumps(payload).encode()
        b64 = base64.b64encode(encoded_payload)
        signature = hmac.new(
            self.gemini_api_secret.encode(), b64, hashlib.sha384
        ).hexdigest()

        return {
            "X-GEMINI-APIKEY": self.gemini_api_key,
            "X-GEMINI-PAYLOAD": b64.decode(),
            "X-GEMINI-SIGNATURE": signature,
        }

    async def run(self):
        """Start the btcusd websocket on a background thread, if not started.

        Connect without TLS certificate verification.

        Returns
        -------
        Return the thread.

        Raises
        ------
        Raise AssertionError if the API key or secret is not set.
        """
        if not self.running_task:
            endpoint = "/v1/marketdata/btcusd"  # TODO need support all symbols in an efficient way
            payload = {"request": endpoint}
            headers = self._generate_ws_headers(payload)
            self.ws = websocket.WebSocketApp(
                f"{self.base_websocket}{endpoint}?trades=true&heartbeat=true",
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                header=headers,
            )
            self.running_task = Thread(
                target=self.ws.run_forever,
                args=(None, {"cert_reqs": ssl.CERT_NONE}),
            )
            self.running_task.start()

        return self.running_task

    async def close(self):
        """Stop the websocket thread, if running, and wait for it to exit."""
        if self.running_task and self.running_task.is_alive():
            tlog(f"close task {self.running_task}")
            self.ws.keep_running = False
            self.running_task.join()
            tlog("task terminated")
            self.ws = None
            self.running_task = None

    @classmethod
    def on_message(cls, ws, msgs):
        """Pass each trade event in a websocket message to trades_handler.

        Ignore messages that are not updates.

        Parameters
        ----------
        ws
            The websocket; unused.
        msgs
            The JSON message text.
        """
        msg = json.loads(msgs)

        if msg["type"] != "update":
            return

        for event in msg["events"]:
            if event["type"] == "trade":
                cls.trades_handler(
                    pd.Timestamp(
                        datetime.fromtimestamp(msg["timestamp"]).astimezone(
                            utctz
                        )
                    ),
                    event,
                )

    @classmethod
    def on_error(cls, ws, error):
        """Log an error reported by the websocket.

        Parameters
        ----------
        ws
            The websocket; unused.
        error
            The reported error.
        """
        tlog(f"[ERROR] GeminiStream {error}")

    @classmethod
    def on_close(cls, ws, close_status_code, close_msg):
        """Log the status code and message sent when the websocket closes.

        Parameters
        ----------
        ws
            The websocket; unused.
        close_status_code
            The close status code.
        close_msg
            The close message.
        """
        tlog(
            f"on_close(): GeminiStream status={close_status_code}, close_msg={close_msg}"
        )

    @classmethod
    def trades_handler(cls, timestamp: datetime, event: Dict):
        """Put a Gemini trade on the btcusd queue as a normalized trade event.

        Log trades received more than two seconds late.

        Parameters
        ----------
        timestamp: datetime
            The time of the trade, timezone-aware.
        event: Dict
            The Gemini trade event.

        Raises
        ------
        Raise an exception if the queue is full; log and suppress any other
        error.
        """
        try:
            if (time_diff := (datetime.now(timezone.utc) - timestamp)) > timedelta(seconds=2):  # type: ignore
                # if randint(1, 100) == 1:  # nosec
                tlog(f"received a trade for btcusd out of sync w {time_diff}")

            event = {
                "symbol": "btcusd",
                "price": float(event["price"]),
                "open": float(event["price"]),
                "close": float(event["price"]),
                "high": float(event["price"]),
                "low": float(event["price"]),
                "timestamp": timestamp,
                "volume": float(event["amount"]),
                "exchange": "gemini",
                "conditions": event["makerSide"],
                "tape": "",
                "average": None,
                "count": 1,
                "vwap": None,
                "EV": "T",
            }

            cls.get_instance().queues["btcusd"].put(event, block=False)

        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
            )
            raise
        except AssertionError as e:
            tlog(f"[EXCEPTION] GEMINI process_message(): {e}")
            time.sleep(1)
            return
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            if config.debug_enabled:
                traceback.print_exc()

    async def subscribe(
        self, symbols: List[str], events: List[WSEventType]
    ) -> bool:
        """Return True without subscribing, as the stream is fixed to btcusd.

        Parameters
        ----------
        symbols: List[str]
            Ignored.
        events: List[WSEventType]
            Ignored.
        """
        return True  # TODO Gemini - handle all symbols and event types
