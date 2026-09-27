"""Momentum stock scanner.

Classes
-------
Momentum
    Scanner that selects stocks showing strong upward momentum.
"""

import asyncio
from datetime import date, datetime, timedelta
from typing import Callable, List, Optional, Tuple

import requests
from alpaca_trade_api.rest import REST as tradeapi
from pytz import timezone

from liualgotrader.common import config
from liualgotrader.common.data_loader import DataLoader  # type: ignore
from liualgotrader.common.tlog import tlog
from liualgotrader.common.types import DataConnectorType
from liualgotrader.data.alpaca import AlpacaData
from liualgotrader.data.polygon import PolygonData
from liualgotrader.models.ticker_data import StockOhlc
from liualgotrader.trading.base import Trader

from .base import Scanner


class Momentum(Scanner):
    """Scanner that selects stocks showing strong upward momentum.

    A live scan filters the market snapshot by share price, previous day dollar
    volume, change from the previous close and volume. A backtest scan returns
    the symbols recorded for the scanner in the database.

    Attributes
    ----------
    max_share_price: float
        The highest share price to select.
    min_share_price: float
        The lowest share price to select.
    min_last_dv: float
        The previous day dollar volume a symbol must exceed.
    min_volume: float
        The current volume a symbol must exceed.
    today_change_percent: float
        The lowest percentage change to select.
    from_market_open: float
        The from-market-open setting; stored but unused.
    max_symbols: int
        The maximum number of symbols; stored but unused.
    trading_api: Trader
        The trader that lists the tradeable symbols.
    trade_able_symbols
        The tradeable symbols, set by a live run.

    Methods
    -------
    apply_filter_on_market_snapshot
        Return the snapshot tickers that pass a filter.
    add_stock_data_for_date
        Save a symbol's price data for a date.
    load_from_db
        Return the symbols recorded for the scanner at a time.
    momentum_filter
        Return whether a snapshot entry meets the criteria.
    polygon_snapshot_details
        Return the filter values of a Polygon entry.
    alpaca_snapshot_details
        Return the filter values of an Alpaca entry.
    run
        Return the symbols selected by the scanner (overrides Scanner).
    """

    name = "momentum"

    def __init__(
        self,
        recurrence: Optional[timedelta],
        target_strategy_name: Optional[str],
        data_loader: DataLoader,
        trading_api: Trader,
        max_share_price: float,
        min_share_price: float,
        min_last_dv: float,
        today_change_percent: float,
        min_volume: float,
        from_market_open: float,
        max_symbols: int = config.total_tickers,
        data_source: object = None,
    ):
        """Initialize the momentum scanner and its filter thresholds.

        Parameters
        ----------
        recurrence: Optional[timedelta]
            The interval between runs, or None.
        target_strategy_name: Optional[str]
            The strategy that receives the results, or None.
        data_loader: DataLoader
            The loader used to fetch market data.
        trading_api: Trader
            The trader that lists the tradeable symbols.
        max_share_price: float
            The highest share price to select.
        min_share_price: float
            The lowest share price to select.
        min_last_dv: float
            The previous day dollar volume a symbol must exceed.
        today_change_percent: float
            The lowest percentage change from the previous close to select.
        min_volume: float
            The current volume a symbol must exceed.
        from_market_open: float
            The from-market-open setting; stored but unused.
        max_symbols: int, default config.total_tickers
            The maximum number of symbols; stored but unused.
        data_source: object, default None
            An optional data source object.
        """
        self.max_share_price = max_share_price
        self.min_share_price = min_share_price
        self.min_last_dv = min_last_dv
        self.min_volume = min_volume
        self.today_change_percent = today_change_percent
        self.from_market_open = from_market_open
        self.max_symbols = max_symbols
        self.trading_api = trading_api

        super().__init__(
            name=self.name,
            recurrence=recurrence,
            target_strategy_name=target_strategy_name,
            data_loader=data_loader,
            data_source=data_source,
        )

    @classmethod
    def __str__(cls) -> str:
        """Return the scanner name defined on the class."""
        return cls.name

    async def _get_trade_able_symbols(self) -> List[str]:
        """Return the tradeable symbols reported by the trading API."""
        symbols = await self.trading_api.get_tradeable_symbols()
        tlog(
            f"loaded list of {len(symbols)} trade-able symbols from {self.trading_api}"
        )
        return symbols

    async def apply_filter_on_market_snapshot(
        self, filter_func: Optional[Callable]
    ) -> List[str]:
        """Return the tickers in the market snapshot that pass a filter.

        Parameters
        ----------
        filter_func: Optional[Callable]
            The predicate that selects snapshot entries, or None.
        """
        filtered = await self.data_loader.data_api.get_market_snapshot(
            filter_func
        )
        tlog(
            f"loaded {len(filtered)} tickers of market snapshots after momentum filtering"
        )
        if not len(filtered):
            tlog("failed to load any market snapshots for any tickers")
            return []

        return [x["ticker"] for x in filtered]

    async def add_stock_data_for_date(self, symbol: str, when: date) -> None:
        """Save a symbol's price data for a date to the database.

        Save a single record with zero prices and volume if no data is
        available.

        Parameters
        ----------
        symbol: str
            The symbol to save data for.
        when: date
            The date to save data for.
        """
        _minute_data = self.data_loader[symbol][
            when : when + timedelta(days=1)  # type: ignore
        ]

        await asyncio.sleep(0)
        if _minute_data[symbol].empty:
            daily_bar = StockOhlc(
                symbol=symbol,
                symbol_date=when,
                open=0.0,
                high=0.0,
                low=0.0,
                close=0.0,
                volume=0,
                indicators={},
            )
            await daily_bar.save()
        else:
            for index, row in _minute_data[symbol].iterrows():
                daily_bar = StockOhlc(
                    symbol=symbol,
                    symbol_date=index,
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=int(row["volume"]),
                    indicators={},
                )
                await daily_bar.save()
                print(f"saved data for {symbol} @ {index}")

    async def load_from_db(self, back_time: datetime) -> List[str]:
        """Return the symbols recorded for this scanner around a given time.

        For midnight, return the symbols recorded from back_time to one day
        later; otherwise, those recorded from 9:30 that day to one minute after
        back_time.

        Parameters
        ----------
        back_time: datetime
            The moment to load symbols for; midnight selects a whole day.
        """
        pool = config.db_conn_pool

        daily_scale = back_time.hour == 0
        start = (
            back_time
            if daily_scale
            else back_time.replace(hour=9, minute=30, second=0, microsecond=0)
        )
        end = (
            back_time + timedelta(days=1)
            if daily_scale
            else back_time + timedelta(minutes=1)
        )
        async with pool.acquire() as con:
            rows = await con.fetch(
                """
                    SELECT
                        symbol
                    FROM 
                        trending_tickers
                    WHERE
                        scanner_name = $1 
                        AND create_tstamp >= $2
                        AND create_tstamp <= $3
                """,
                self.name,
                start,
                end,
            )

            print("load from db", start, end, len(rows))
            return [row[0] for row in rows] if len(rows) > 0 else []

    def momentum_filter(self, snapshot) -> bool:
        """Return whether a market snapshot entry meets the momentum criteria.

        Call only after run has set trade_able_symbols.

        Parameters
        ----------
        snapshot
            The snapshot entry, from the Alpaca or Polygon data API.

        Raises
        ------
        Raise ValueError if the data loader uses any other data API.
        """
        if isinstance(self.data_loader.data_api, AlpacaData):
            (
                tradable,
                last_trade_price,
                prev_day_volume_price,
                today_gap_up_pct,
                today_volume,
            ) = self.alpaca_snapshot_details(snapshot)
        elif isinstance(self.data_loader.data_api, PolygonData):
            (
                tradable,
                last_trade_price,
                prev_day_volume_price,
                today_gap_up_pct,
                today_volume,
            ) = self.polygon_snapshot_details(snapshot)
        else:
            raise ValueError(
                f"Invalid data API: {type(self.data_loader.data_api)}"
            )

        in_price_range = (
            self.max_share_price >= last_trade_price >= self.min_share_price
        )
        return (
            tradable
            and in_price_range
            and prev_day_volume_price > self.min_last_dv
            and today_gap_up_pct >= self.today_change_percent
            and today_volume > self.min_volume
        )

    def polygon_snapshot_details(
        self, snapshot
    ) -> Tuple[bool, float, float, float, float]:
        """Return the momentum filter values of a Polygon snapshot entry.

        Parameters
        ----------
        snapshot
            The Polygon snapshot entry.

        Returns
        -------
        Return whether the ticker is tradeable, the last trade price, the
        previous day dollar volume, today's change percentage and today's
        volume.
        """
        tradeable = snapshot["ticker"].lower() in self.trade_able_symbols
        last_trade_price = float(snapshot["lastTrade"]["p"])
        prev_day_volume_price = float(snapshot["prevDay"]["p"]) * float(
            snapshot["prevDay"]["v"]
        )
        today_gap_up_pct = snapshot["todaysChangePerc"]
        today_volume = snapshot["day"]["v"]

        return (
            tradeable,
            last_trade_price,
            prev_day_volume_price,
            today_gap_up_pct,
            today_volume,
        )

    def alpaca_snapshot_details(
        self, snapshot
    ) -> Tuple[bool, float, float, float, float]:
        """Return the momentum filter values of an Alpaca snapshot entry.

        Parameters
        ----------
        snapshot
            The Alpaca snapshot entry.

        Returns
        -------
        Return whether the ticker is tradeable, the latest trade price, the
        previous day dollar volume, the percentage gap between today's open and
        the previous close, and today's volume.
        """
        tradable = snapshot["ticker"].lower() in self.trade_able_symbols

        last_trade_price = float(snapshot["latest_trade"]["p"])
        prev_day_price_volume = float(snapshot["prev_daily_bar"]["v"]) * float(
            snapshot["prev_daily_bar"]["c"]
        )
        today_gap_up_pct = (
            100.0
            * (snapshot["daily_bar"]["o"] - snapshot["prev_daily_bar"]["c"])
            / snapshot["prev_daily_bar"]["c"]
        )
        today_volume = snapshot["daily_bar"]["v"]

        return (
            tradable,
            last_trade_price,
            prev_day_price_volume,
            today_gap_up_pct,
            today_volume,
        )

    async def run(self, back_time: datetime = None) -> List[str]:
        """Return the symbols selected by the momentum scanner.

        Without back_time, refresh trade_able_symbols and filter the live
        market snapshot; with it, return the symbols recorded in the database.

        Parameters
        ----------
        back_time: datetime, default None
            The moment to scan as of, when backtesting.
        """
        if not back_time:
            self.trade_able_symbols = await self._get_trade_able_symbols()
            return await self.apply_filter_on_market_snapshot(
                self.momentum_filter
            )

        rows = await self.load_from_db(back_time)

        tlog(
            f"Scanner {self.name} -> back_time={back_time} picked {len(rows)}"
        )
        return rows
