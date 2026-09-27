"""Persist and load ticker descriptions and daily price data.

Classes
-------
TickerData
    Descriptive information about a ticker.
StockOhlc
    Daily price and volume data for a symbol.
"""

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional

from asyncpg.exceptions import TooManyConnectionsError
from asyncpg.pool import Pool

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog


@dataclass
class TickerData:
    """Descriptive information about a ticker.

    Attributes
    ----------
    name: str
        The ticker name.
    symbol: str
        The ticker symbol.
    description: str
        The ticker description.
    tags: List[str]
        The tags assigned to the ticker.
    similar_tickers: List[str]
        The symbols of similar tickers.
    industry: str
        The industry.
    sector: str
        The sector.
    exchange: str
        The exchange the ticker trades on.

    Methods
    -------
    load_symbols
        Return every stored ticker symbol.
    save
        Save the ticker, replacing any ticker with the same symbol.
    """

    name: str
    symbol: str
    description: str
    tags: List[str]
    similar_tickers: List[str]
    industry: str
    sector: str
    exchange: str

    @classmethod
    async def load_symbols(cls, pool: Pool = None) -> List[str]:
        """Return every stored ticker symbol.

        Parameters
        ----------
        pool: Pool, default None
            The connection pool, or None for the shared pool.

        Raises
        ------
        Raise Exception if no tickers are stored.
        """
        if not pool:
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    """
                        SELECT symbol
                        FROM ticker_data 
                    """,
                )

                if rows:
                    return [row[0] for row in rows]
                else:
                    raise Exception("no data")

    async def save(self, pool: Pool) -> bool:
        """Save this ticker, replacing any ticker with the same symbol.

        Parameters
        ----------
        pool: Pool
            The connection pool.

        Returns
        -------
        Return True on success, or False if the database has too many
        connections.
        """
        try:
            async with pool.acquire() as con:
                async with con.transaction():
                    await con.execute(
                        """
                            INSERT INTO ticker_data (symbol, name, description, tags,
                                                     similar_tickers, industry, sector, exchange)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            ON CONFLICT (symbol)
                            DO UPDATE
                                SET name=$2, description=$3, tags=$4, similar_tickers=$5, industry=$6,
                                    sector=$7, exchange=$8, modify_tstamp='now()'
                        """,
                        self.symbol,
                        self.name,
                        self.description,
                        self.tags,
                        self.similar_tickers,
                        self.industry,
                        self.sector,
                        self.exchange,
                    )
            return True
        except TooManyConnectionsError as e:
            tlog(
                f"too many db connections: {e}. Failed to write ticker {self.symbol}, will re-try"
            )

        return False


@dataclass
class StockOhlc:
    """Daily price and volume data for a symbol.

    Attributes
    ----------
    symbol: str
        The ticker symbol.
    symbol_date: date
        The trading date.
    open: float
        The opening price.
    high: float
        The high price.
    low: float
        The low price.
    close: float
        The closing price.
    volume: int
        The traded volume.
    indicators: Dict
        The indicators for the date.
    create_tstamp: Optional[datetime]
        The time the record was created, or None.

    Methods
    -------
    check_stock_date_exists
        Return whether data exists for a date.
    get_latest_date
        Return the latest date with data for a symbol.
    load_by_date
        Load the data of every symbol for a date.
    save
        Save the record, replacing any record for the same date.
    """

    symbol: str
    symbol_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    indicators: Dict
    create_tstamp: Optional[datetime] = None

    @classmethod
    async def check_stock_date_exists(
        cls, symbol: str, symbol_date: date, pool: Pool = None
    ) -> bool:
        """Return whether daily data exists for a symbol on a date.

        Parameters
        ----------
        symbol: str
            The ticker symbol.
        symbol_date: date
            The trading date.
        pool: Pool, default None
            The connection pool, or None for the shared pool.
        """
        if not pool:
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            async with con.transaction():
                val = await con.fetchval(
                    """
                        SELECT 
                            COUNT(symbol)
                        FROM 
                            stock_ohlc
                        WHERE 
                            symbol_date = $1 AND
                            symbol = $2
                        LIMIT 1
                    """,
                    symbol_date,
                    symbol,
                )
                return val > 0

    @classmethod
    async def get_latest_date(cls, symbol: str, pool: Pool = None) -> date:
        """Return the latest date with daily data for a symbol.

        Parameters
        ----------
        symbol: str
            The ticker symbol.
        pool: Pool, default None
            The connection pool, or None for the shared pool.

        Returns
        -------
        Return None if the symbol has no data.
        """
        if not pool:
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            async with con.transaction():
                val = await con.fetchval(
                    """
                        SELECT symbol_date
                        FROM stock_ohlc
                        WHERE symbol=$1 
                        ORDER by symbol_date DESC
                        LIMIT 1
                    """,
                    symbol,
                )
                return val

    @classmethod
    async def load_by_date(
        cls, symbol_date: date, pool: Pool = None
    ) -> Dict[str, object]:
        """Load the daily data of every symbol for a date.

        Parameters
        ----------
        symbol_date: date
            The trading date.
        pool: Pool, default None
            The connection pool, or None for the shared pool.

        Returns
        -------
        Return a dictionary that maps each symbol to its StockOhlc record.
        """
        if not pool:
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    """
                        SELECT symbol, symbol_date, open, high, low, close, volume, indicators
                        FROM stock_ohlc
                        WHERE symbol_date = $1 
                    """,
                    symbol_date,
                )

                rc: Dict[str, object] = {}

                for x in rows:
                    rc[x[0]] = StockOhlc(
                        symbol=x[0],
                        symbol_date=x[1],
                        open=x[2],
                        high=x[3],
                        low=x[4],
                        close=x[5],
                        volume=x[6],
                        indicators=json.loads(x[7]),
                    )

                return rc

    async def save(
        self,
        pool: Pool = None,
    ) -> None:
        """Save this record, replacing any record for its symbol and date.

        Parameters
        ----------
        pool: Pool, default None
            The connection pool, or None for the shared pool.
        """
        if not pool:
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    """
                        INSERT INTO stock_ohlc (symbol, symbol_date, open, high, low, close, volume, indicators)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (symbol, symbol_date)
                        DO UPDATE
                            SET open=$3, high=$4, low=$5, close=$6, volume=$7, indicators=$8, modify_tstamp='now()'
                    """,
                    self.symbol,
                    self.symbol_date,
                    self.open,
                    self.high,
                    self.low,
                    self.close,
                    self.volume,
                    json.dumps(self.indicators),
                )
