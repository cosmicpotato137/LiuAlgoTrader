"""Alpaca market data and streaming providers.

Classes
-------
AlpacaData
    Market data provider for the Alpaca REST and crypto APIs.
AlpacaStream
    Streaming provider for the Alpaca WebSocket API.
"""

import asyncio
import concurrent.futures
import queue
import time
import traceback
from datetime import date, datetime, timedelta
from random import randint
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pandas_market_calendars
import pytz
import requests
from alpaca_trade_api.rest import REST, URL, APIError, TimeFrame
from alpaca_trade_api.stream import Stream
from dateutil.parser import parse as date_parser

from liualgotrader.common import config
from liualgotrader.common.list_utils import chunks
from liualgotrader.common.tlog import tlog, tlog_exception
from liualgotrader.common.types import QueueMapper, TimeScale, WSEventType
from liualgotrader.data.data_base import DataAPI
from liualgotrader.data.streaming_base import StreamingAPI

NY = "America/New_York"
nytz = pytz.timezone(NY)


def _is_crypto_symbol(symbol: str) -> bool:
    """Return whether a symbol is a supported crypto pair.

    Only the Bitcoin and Ethereum pairs quoted in US dollars are supported.

    Parameters
    ----------
    symbol: str
        The symbol to check, with or without a slash, in any case.
    """
    return symbol.lower() in {"eth/usd", "btc/usd", "ethusd", "btcusd"}


class AlpacaData(DataAPI):
    """Market data provider for the Alpaca REST and crypto APIs.

    Supports US equities and the Bitcoin and Ethereum US dollar pairs.
    Methods that need the REST client raise AssertionError if it is not set.

    Attributes
    ----------
    alpaca_rest_client
        The Alpaca REST client.
    symbol_chunk_size
        The number of symbols per snapshot request.
    datetime_cache: Dict[datetime, datetime]
        The adjusted slice bounds, keyed by requested datetime.

    Methods
    -------
    get_symbols
        Return the active, tradable US equity symbols.
    get_market_snapshot
        Return snapshots of the tradable US equities.
    get_last_trading
        Return the time of the most recent trade.
    get_trading_holidays
        Return the NYSE holiday dates.
    get_trading_day
        Return a datetime shifted by trading days.
    num_trading_minutes
        Return the trading minutes in one day.
    num_trading_days
        Return the trading days in a date range.
    get_max_data_points_per_load
        Return the data point limit per request.
    trading_days_slice
        Adjust a datetime slice to trading sessions.
    crypto_get_symbol_data
        Return historical bars for a crypto pair.
    get_symbols_data
        Return historical bars for several symbols.
    get_symbol_data
        Return historical bars for one symbol.
    """

    def __init__(self):
        """Initialize the Alpaca REST client.

        Raises
        ------
        Raise AssertionError if the client cannot be created.
        """
        self.alpaca_rest_client = REST(
            key_id=config.alpaca_api_key, secret_key=config.alpaca_api_secret
        )
        if not self.alpaca_rest_client:
            raise AssertionError(
                "Failed to authenticate Alpaca RESTful client"
            )
        # for requesting market snapshots by chunk of symbols
        self.symbol_chunk_size = 1000
        self.datetime_cache: Dict[datetime, datetime] = {}

    def get_symbols(self) -> List[str]:
        """Return the symbols of all active, tradable US equities."""
        if not self.alpaca_rest_client:
            raise AssertionError("Must call w/ authenticated Alpaca client")

        return [
            asset.symbol
            for asset in self.alpaca_rest_client.list_assets(
                status="active", asset_class="us_equity"
            )
            if asset.tradable
        ]

    async def get_market_snapshot(
        self, filter_func: Optional[Callable] = None
    ) -> List[Dict]:
        """Return snapshots of all active, tradable US equities.

        Each snapshot is a dictionary with a "ticker" key and the raw data of
        each snapshot component.

        Parameters
        ----------
        filter_func: Optional[Callable], default None
            A predicate that selects the snapshots to keep, or None to keep
            all.
        """
        # parse market snapshots per chunk of symbols
        symbols = self.get_symbols()
        return await self._get_symbols_snapshot(symbols, filter_func)

    async def _get_symbols_snapshot(
        self, symbols: List[str], filter_func: Optional[Callable]
    ) -> List[Dict]:
        """Return the market snapshots for the given symbols.

        Drop snapshots that lack a component when filter_func is set, and
        return them as None otherwise.

        Parameters
        ----------
        symbols: List[str]
            The symbols to fetch.
        filter_func: Optional[Callable]
            A predicate that selects the snapshots to keep, or None to keep
            all.
        """
        def _parse_ticker_snapshot(_ticker: str, _ticket_snapshot: object):
            """Return a snapshot as a dictionary, or None if incomplete.

            Parameters
            ----------
            _ticker: str
                The symbol of the snapshot.
            _ticket_snapshot: object
                The Alpaca snapshot object.
            """
            try:
                return {
                    "ticker": _ticker,
                    **{
                        sub_snapshot_type: _sub_snapshot_obj.__dict__["_raw"]
                        for sub_snapshot_type, _sub_snapshot_obj in _ticket_snapshot.__dict__.items()
                    },
                }
            # skip over if some snapshot type is missing (e.g. "prev_daily_bar": None)
            except AttributeError:
                return None

        def _parse_snapshot_and_filter(_symbols: List[str]) -> List[Dict]:
            """Return the parsed snapshots for a chunk of symbols.

            When the enclosing filter_func is set, keep only the snapshots it
            accepts, excluding missing ones.

            Parameters
            ----------
            _symbols: List[str]
                The symbols to fetch.
            """
            processed_tickers_snapshot = map(
                lambda key_and_val: _parse_ticker_snapshot(*key_and_val),
                self.alpaca_rest_client.get_snapshots(_symbols).items(),
            )
            return list(
                filter(
                    lambda snapshot: (  # type: ignore
                        (snapshot is not None) and (filter_func(snapshot))
                    ),
                    list(processed_tickers_snapshot),  # type : ignore
                )
                if filter_func is not None
                else processed_tickers_snapshot
            )

        # request snapshots per chunk of tickers by concurrency
        with concurrent.futures.ThreadPoolExecutor() as executor:
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor,
                    _parse_snapshot_and_filter,
                    symbols[symbol_idx : symbol_idx + self.symbol_chunk_size],
                )
                for symbol_idx in range(
                    0, len(symbols), self.symbol_chunk_size
                )
            ]

            market_snapshots = [
                y for x in await asyncio.gather(*futures) for y in x
            ]

        return market_snapshots

    def _localize_start_end(self, start: date, end: date) -> Tuple[str, str]:
        """Return a date range as ISO 8601 strings in New York time.

        Parameters
        ----------
        start: date
            The first date, converted to midnight.
        end: date
            The last date, converted to midnight, or to the current time if it
            is today or later.
        """
        return (
            nytz.localize(
                datetime.combine(start, datetime.min.time())
            ).isoformat(),
            (
                nytz.localize(
                    datetime.now().replace(microsecond=0)
                ).isoformat()
                if end >= date.today()
                else nytz.localize(
                    datetime.combine(end, datetime.min.time())
                ).isoformat()
            ),
        )

    def get_last_trading(self, symbol: str) -> datetime:
        """Return the time of the most recent trade for a symbol.

        Parameters
        ----------
        symbol: str
            The symbol to look up.

        Returns
        -------
        Return the current New York time for crypto pairs.

        Raises
        ------
        Raise ValueError if the latest trade is unavailable.
        """
        if not self.alpaca_rest_client:
            raise AssertionError("Must call w/ authenticated Alpaca client")

        if _is_crypto_symbol(symbol):
            return datetime.now(tz=nytz)
        try:
            snapshot_data = self.alpaca_rest_client.get_snapshot(symbol)
        except APIError as e:
            raise ValueError(f"{symbol} snapshot not found") from e

        min_bar = snapshot_data.latest_trade
        if not min_bar:
            raise ValueError(f"Can't get snapshot for {symbol}")

        return min_bar.t

    def get_trading_holidays(self) -> List[str]:
        """Return the holiday dates of the NYSE calendar."""
        nyse = pandas_market_calendars.get_calendar("NYSE")
        return nyse.holidays().holidays

    def get_trading_day(
        self, symbol: str, now: datetime, offset: int
    ) -> datetime:
        """Return a reference time shifted by a number of trading days.

        Shift crypto pairs by offset calendar days and other symbols by offset
        minus one NYSE trading days.

        Parameters
        ----------
        symbol: str
            The symbol whose trading calendar applies.
        now: datetime
            The reference datetime, treated as New York time if naive.
        offset: int
            The number of days to shift by.
        """
        if _is_crypto_symbol(symbol):
            cbd_offset = timedelta(days=offset)
        else:
            cbd_offset = pd.tseries.offsets.CustomBusinessDay(
                n=offset - 1, holidays=self.get_trading_holidays()
            )
        return (
            nytz.localize(now + cbd_offset)
            if now.tzinfo is None
            else now + cbd_offset
        )

    def num_trading_minutes(self, symbol: str, start: date, end: date) -> int:
        """Return the number of trading minutes in one day for a symbol.

        Count 24 hours for crypto pairs and 16 hours for other symbols.

        Parameters
        ----------
        symbol: str
            The symbol whose trading hours apply.
        start: date
            Unused.
        end: date
            Unused.
        """
        return (24 if _is_crypto_symbol(symbol) else (20 - 4)) * 60

    def num_trading_days(self, symbol: str, start: date, end: date) -> int:
        """Return the number of trading days in a date range for a symbol.

        Count every calendar day for crypto pairs and only NYSE trading days
        for other symbols.

        Parameters
        ----------
        symbol: str
            The symbol whose trading calendar applies.
        start: date
            The first date of the range, as a date or a string.
        end: date
            The last date of the range, as a date or a string.
        """
        if type(start) == str:
            start = date_parser(start)  # type: ignore
        if type(end) == str:
            end = date_parser(end)  # type: ignore

        return (
            (end - start).days + 1
            if _is_crypto_symbol(symbol)
            else len(
                pd.date_range(
                    start,
                    end,
                    freq=pd.tseries.offsets.CustomBusinessDay(
                        holidays=self.get_trading_holidays()
                    ),
                )
            )
        )

    def get_max_data_points_per_load(self) -> int:
        """Return 10000, the maximum number of data points per request."""
        # Alpaca suggests 10000 points
        return 10000

    def trading_days_slice(self, symbol: str, s: slice) -> slice:
        """Return a datetime slice adjusted to the trading sessions it spans.

        For other symbols, return a slice between the opening times of its
        first and last trading days, in New York time, and cache the new bounds
        in datetime_cache.

        Parameters
        ----------
        symbol: str
            The symbol whose trading calendar applies.
        s: slice
            The slice to adjust, with datetime bounds.

        Returns
        -------
        Return s unchanged for crypto pairs.
        """
        if not self.alpaca_rest_client:
            raise AssertionError("Must call w/ authenticated Alpaca client")

        if _is_crypto_symbol(symbol):
            return s

        if s.start in self.datetime_cache and s.stop in self.datetime_cache:
            return slice(
                self.datetime_cache[s.start], self.datetime_cache[s.stop]
            )

        trading_days = self.alpaca_rest_client.get_calendar(
            str(s.start.date()), str(s.stop.date())
        )
        new_slice = slice(
            nytz.localize(
                datetime.combine(
                    trading_days[0].date.date(), trading_days[0].open
                )
            ),
            nytz.localize(
                datetime.combine(
                    trading_days[-1].date.date(), trading_days[-1].open
                )
            ),
        )

        self.datetime_cache[s.start] = new_slice.start
        self.datetime_cache[s.stop] = new_slice.stop

        return new_slice

    def crypto_get_symbol_data(
        self,
        symbol: str,
        start: str,
        end: str,
        timeframe: TimeFrame,
    ) -> pd.DataFrame:
        """Return historical bars for a crypto pair.

        Parameters
        ----------
        symbol: str
            The crypto pair, with or without a slash.
        start: str
            The start of the range, as an ISO 8601 string.
        end: str
            The end of the range, as an ISO 8601 string.
        timeframe: TimeFrame
            TimeFrame.Day for daily bars, or any other value for minute bars.

        Returns
        -------
        Return a DataFrame indexed by timestamp.

        Raises
        ------
        Raise HTTPError if a request fails.
        """
        if "/" not in symbol:
            symbol = f"{symbol[:3]}/{symbol[3:]}"
        symbol = symbol.upper()
        url = f"{config.alpaca_crypto_base_url}/bars"
        page_token = None

        rc_df = pd.DataFrame()

        while True:
            response = requests.get(
                url,
                params={  # type:ignore
                    "symbols": [symbol],
                    "start": start,
                    "end": end,
                    "limit": self.get_max_data_points_per_load(),
                    "timeframe": "1Day"
                    if timeframe == TimeFrame.Day
                    else "1Min",
                    "page_token": page_token,
                },
                headers={
                    "APCA-API-KEY-ID": config.alpaca_api_key,
                    "APCA-API-SECRET-KEY": config.alpaca_api_secret,
                },
            )

            response.raise_for_status()

            json_data = response.json()
            df = pd.DataFrame(json_data["bars"][symbol])
            df.rename(
                columns={
                    "o": "open",
                    "c": "close",
                    "h": "high",
                    "l": "low",
                    "v": "volume",
                    "vw": "vwap",
                    "t": "timestamp",
                    "n": "trade_count",
                },
                inplace=True,
            )
            df["timestamp"] = pd.to_datetime(df.timestamp)
            df = df.set_index(df.timestamp)

            rc_df = pd.concat([rc_df, df], sort=True)
            rc_df = rc_df[~rc_df.index.duplicated(keep="first")]

            page_token = json_data["next_page_token"]
            if page_token is None:
                return rc_df

    def get_symbols_data(
        self,
        symbols: List[str],
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> Dict[str, pd.DataFrame]:
        """Return historical bars for several symbols.

        Retry on transient HTTP errors.

        Parameters
        ----------
        symbols: List[str]
            The list of symbols to load.
        start: date
            The start of the date range.
        end: date, default date.today()
            The end of the date range.
        scale: TimeScale, default TimeScale.minute
            The bar resolution.

        Returns
        -------
        Return a dictionary that maps each symbol found to a DataFrame of the
        form returned by get_symbol_data.

        Raises
        ------
        Raise AssertionError if symbols is not a list.
        """
        if not self.alpaca_rest_client:
            raise AssertionError("Must call w/ authenticated Alpaca client")
        if not isinstance(symbols, list):
            raise AssertionError(f"{symbols} must be a list")

        if scale == TimeScale.minute:
            end += timedelta(days=1)
        _start, _end = self._localize_start_end(start, end)
        dfs: Dict = {}
        t: TimeFrame = (
            TimeFrame.Minute
            if scale == TimeScale.minute
            else TimeFrame.Day
            if scale == TimeScale.day
            else None
        )
        try:
            data = self.alpaca_rest_client.get_bars(
                symbol=symbols,
                timeframe=t,
                start=_start,
                end=_end,
                limit=1000000000,
                adjustment="all",
            ).df
        except requests.exceptions.HTTPError as e:
            tlog(f"received HTTPError: {e}")
            if e.response.status_code in (500, 502, 504, 429):
                tlog("retrying")
                time.sleep(10)
                return self.get_symbols_data(symbols, start, end, scale)

        data = data.tz_convert("America/New_York")
        data["average"] = data.vwap
        data["count"] = data.trade_count
        data["vwap"] = np.NaN
        grouped = data.groupby(data.symbol)
        for symbol in data.symbol.unique():
            dfs[symbol] = grouped.get_group(symbol)[
                [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "count",
                    "average",
                    "vwap",
                ]
            ]

        return dfs

    def get_symbol_data(
        self,
        symbol: str,
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> pd.DataFrame:
        """Return historical bars for an equity symbol or crypto pair.

        Retry on transient HTTP errors.

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

        Returns
        -------
        Return a DataFrame in New York time with the open, high, low, close,
        volume, count, average and vwap columns, where average is the
        volume-weighted price and vwap is NaN.

        Raises
        ------
        Raise ValueError if the data cannot be loaded or is empty.
        """
        _start, _end = self._localize_start_end(start, end)

        if not self.alpaca_rest_client:
            raise AssertionError("Must call w/ authenticated Alpaca client")

        t: TimeFrame = (
            TimeFrame.Minute
            if scale == TimeScale.minute
            else TimeFrame.Day
            if scale == TimeScale.day
            else None
        )

        try:
            if config.detailed_dl_debug_enabled:
                tlog(f"symbol={symbol}, timeframe={t}, range=({_start, _end})")

            data = (
                self.crypto_get_symbol_data(
                    symbol=symbol, start=_start, end=_end, timeframe=t
                )
                if _is_crypto_symbol(symbol)
                else self.alpaca_rest_client.get_bars(
                    symbol=symbol,
                    timeframe=t,
                    start=_start,
                    end=_end,
                    limit=1000000,
                    adjustment="all",
                ).df
            )
        except requests.exceptions.HTTPError as e:
            tlog(f"received HTTPError: {e}")
            if e.response.status_code in (500, 502, 504, 429):
                tlog("retrying")
                time.sleep(10)
                return self.get_symbol_data(symbol, start, end, scale)
            else:
                raise ValueError(
                    f"[EXCEPTION] {e} for {symbol} could not obtain data for {_start} to {_end} w {scale.name}"
                )

        except Exception as e:
            raise ValueError(
                f"[EXCEPTION] {e} for {symbol} has no data for {_start} to {_end} w {scale.name}"
            )
        else:
            if data.empty:
                raise ValueError(
                    f"[ERROR] {symbol} has no data for {_start} to {_end} w {scale.name}"
                )

        data.index = data.index.tz_convert("America/New_York")
        data["average"] = data.vwap
        data["count"] = data.trade_count
        data["vwap"] = np.NaN

        return data[
            [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "count",
                "average",
                "vwap",
            ]
        ]


class AlpacaStream(StreamingAPI):
    """Streaming provider for the Alpaca WebSocket API.

    Supports US equities and the Bitcoin and Ethereum US dollar pairs.

    Attributes
    ----------
    alpaca_ws_client
        The Alpaca WebSocket client.
    task: Optional[asyncio.Task]
        The background streaming task, or None before run.

    Methods
    -------
    run
        Start the WebSocket client in the background.
    bar_handler
        Enqueue an equity minute bar event.
    crypto_bar_handler
        Enqueue a crypto minute bar event.
    trades_handler
        Enqueue an equity trade event.
    crypto_trades_handler
        Enqueue a crypto trade event.
    quotes_handler
        Discard a quote message.
    subscribe
        Subscribe to event types for the given symbols.
    close
        Stop the WebSocket client.
    """

    def __init__(self, queues: QueueMapper):
        """Initialize the WebSocket client and register the shared instance.

        Parameters
        ----------
        queues: QueueMapper
            The mapper from each symbol to its event queue.

        Raises
        ------
        Raise AssertionError if the client cannot be created.
        """
        self.alpaca_ws_client = Stream(
            base_url=URL(config.alpaca_base_url),
            key_id=config.alpaca_api_key,
            secret_key=config.alpaca_api_secret,
            data_feed=config.alpaca_data_feed,
        )

        if not self.alpaca_ws_client:
            raise AssertionError(
                "Failed to authenticate Alpaca web_socket client"
            )

        self.task: Optional[asyncio.Task] = None
        super().__init__(queues)

    async def run(self):
        """Start the WebSocket client in the background, if not yet running.

        Raises
        ------
        Raise AssertionError if no queues are set.
        """
        if not self.task:
            if self.queues:
                self.task = asyncio.create_task(
                    self.alpaca_ws_client._run_forever()
                )
            else:
                raise AssertionError(
                    "can't call `AlpacaStream.run()` without queues"
                )

    @classmethod
    async def bar_handler(cls, msg):
        """Convert an equity bar message into an "AM" event and enqueue it.

        In the event, average holds the volume-weighted price and vwap is NaN.
        Propagate an exception if the queue is full; log and suppress any other
        error.

        Parameters
        ----------
        msg
            The Alpaca bar message.
        """
        try:
            event = {
                "symbol": msg.symbol,
                "open": msg.open,
                "close": msg.close,
                "high": msg.high,
                "low": msg.low,
                "timestamp": pd.to_datetime(
                    msg.timestamp, utc=True
                ).astimezone(nytz),
                "volume": msg.volume,
                "count": int(msg.trade_count),
                "vwap": np.nan,
                "average": msg.vwap,
                "totalvolume": None,
                "EV": "AM",
            }
            cls.get_instance().queues[msg.symbol].put(event, timeout=1)
        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
            )
            raise
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            if config.debug_enabled:
                traceback.print_exc()

    @classmethod
    async def crypto_bar_handler(cls, msg):
        """Convert a crypto bar message into an "AM" event and enqueue it.

        Ignore messages from exchanges other than CBSE. Otherwise behave like
        bar_handler.

        Parameters
        ----------
        msg
            The Alpaca crypto bar message.
        """
        try:
            if msg.exchange != "CBSE":
                return

            event = {
                "symbol": msg.symbol,
                "open": msg.open,
                "close": msg.close,
                "high": msg.high,
                "low": msg.low,
                "timestamp": pd.to_datetime(
                    msg.timestamp, utc=True
                ).astimezone(nytz),
                "volume": msg.volume,
                "count": int(msg.trade_count),
                "vwap": np.nan,
                "average": msg.vwap,
                "totalvolume": None,
                "EV": "AM",
            }
            cls.get_instance().queues[msg.symbol].put(event, timeout=1)
        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
            )
            raise
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            if config.debug_enabled:
                traceback.print_exc()

    @classmethod
    async def trades_handler(cls, msg):
        """Convert an equity trade message into a "T" event and enqueue it.

        Occasionally log a warning for trades more than ten seconds old.
        Propagate an exception if the queue is full; log and suppress any other
        error.

        Parameters
        ----------
        msg
            The Alpaca trade message.
        """
        try:
            ts = pd.to_datetime(msg.timestamp)
            if (time_diff := (datetime.now(tz=nytz) - ts)) > timedelta(
                seconds=10
            ) and randint(  # nosec
                1, 100
            ) == 1:  # nosec
                tlog(
                    f"Received trade for {msg.symbol} too out of sync w {time_diff}"
                )

            event = {
                "symbol": msg.symbol,
                "price": msg.price,
                "open": msg.price,
                "close": msg.price,
                "high": msg.price,
                "low": msg.price,
                "timestamp": ts,
                "volume": msg.size,
                "exchange": msg.exchange,
                "conditions": msg.conditions
                if hasattr(msg, "conditions")
                else None,
                "tape": msg.tape if hasattr(msg, "tape") else None,
                "average": np.nan,
                "count": 1,
                "vwap": np.nan,
                "EV": "T",
            }

            cls.get_instance().queues[msg.symbol].put(event, block=False)

        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
            )
            raise
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            if config.debug_enabled:
                traceback.print_exc()

    @classmethod
    async def crypto_trades_handler(cls, msg):
        """Convert a crypto trade message into a "T" event and enqueue it.

        Ignore messages from exchanges other than CBSE. Otherwise behave like
        trades_handler.

        Parameters
        ----------
        msg
            The Alpaca crypto trade message.
        """
        try:
            if msg.exchange != "CBSE":
                return

            ts = pd.to_datetime(msg.timestamp)
            if (time_diff := (datetime.now(tz=nytz) - ts)) > timedelta(
                seconds=10
            ) and randint(  # nosec
                1, 100
            ) == 1:  # nosec
                tlog(
                    f"Received trade for {msg.symbol} too out of sync w {time_diff}"
                )

            event = {
                "symbol": msg.symbol,
                "price": msg.price,
                "open": msg.price,
                "close": msg.price,
                "high": msg.price,
                "low": msg.price,
                "timestamp": ts,
                "volume": msg.size,
                "exchange": msg.exchange,
                "conditions": msg.conditions
                if hasattr(msg, "conditions")
                else None,
                "tape": msg.tape if hasattr(msg, "tape") else None,
                "average": np.nan,
                "count": 1,
                "vwap": np.nan,
                "EV": "T",
            }

            cls.get_instance().queues[msg.symbol].put(event, block=False)

        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
            )
            raise
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            if config.debug_enabled:
                traceback.print_exc()

    @classmethod
    async def quotes_handler(cls, msg):
        """Discard a quote message.

        Parameters
        ----------
        msg
            The Alpaca quote message.
        """
        pass

    async def subscribe(
        self, symbols: List[str], events: List[WSEventType]
    ) -> bool:
        """Subscribe to event types for the given symbols.

        Parameters
        ----------
        symbols: List[str]
            The equity symbols and crypto pairs to subscribe to.
        events: List[WSEventType]
            The event types to receive; types other than minute bars, trades
            and quotes are ignored.

        Returns
        -------
        Return True.
        """
        tlog(f"Starting subscription for {len(symbols)} symbols")
        upper_symbols = [symbol.upper() for symbol in symbols]
        for syms in chunks(upper_symbols, 1000):
            tlog(f"\tsubscribe {len(syms)}/{len(upper_symbols)}")

            crypto_symbols = list(filter(_is_crypto_symbol, syms))
            equity_symbols = [x for x in syms if x not in crypto_symbols]

            for event in events:
                if event == WSEventType.MIN_AGG:
                    self.alpaca_ws_client._data_ws._running = False

                    if crypto_symbols:
                        self.alpaca_ws_client.subscribe_crypto_bars(
                            AlpacaStream.crypto_bar_handler,
                            *crypto_symbols,
                        )
                    if equity_symbols:
                        self.alpaca_ws_client.subscribe_bars(
                            AlpacaStream.bar_handler,
                            *equity_symbols,
                        )
                elif event == WSEventType.TRADE:
                    if crypto_symbols:
                        self.alpaca_ws_client.subscribe_crypto_trades(
                            AlpacaStream.crypto_trades_handler, *crypto_symbols
                        )
                    if equity_symbols:
                        self.alpaca_ws_client.subscribe_trades(
                            AlpacaStream.trades_handler, *equity_symbols
                        )
                elif event == WSEventType.QUOTE:
                    if crypto_symbols:
                        self.alpaca_ws_client.subscribe_crypto_quotes(
                            AlpacaStream.quotes_handler, *crypto_symbols
                        )
                    if equity_symbols:
                        self.alpaca_ws_client.subscribe_quotes(
                            AlpacaStream.quotes_handler, *equity_symbols
                        )

            await asyncio.sleep(1)

        tlog(f"Completed subscription for {len(symbols)} symbols")
        return True

    async def close(self) -> None:
        """Stop the WebSocket client and wait for the streaming task to end."""
        tlog("Closing AlpacaStream")

        if self.task:
            # self.alpaca_ws_client.stop()

            await self.alpaca_ws_client.stop_ws()

            while not self.task.done():
                await asyncio.sleep(1.0)

            tlog("Task Done. Closed AlpacaStream")
