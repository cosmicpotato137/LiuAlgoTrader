"""Persistence of the trade operations made by strategy runs.

Classes
-------
NewTrade
    A single trade operation made by a strategy run.
"""

import json
from datetime import datetime
from typing import Dict, List, Tuple

from asyncpg.pool import Pool

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog


class NewTrade:
    """Trade operation made by a strategy run.

    Attributes
    ----------
    algo_run_id: int
        The identifier of the run making the trade.
    symbol: str
        The traded symbol.
    operation: str
        The operation, buy or sell.
    qty: float
        The quantity traded.
    price: float
        The trade price.
    indicators: dict
        The indicators at the time of the trade.
    trade_id
        The trade identifier, or None until the trade is saved.

    Methods
    -------
    save
        Save the trade and store its identifier.
    expire_trade
        Mark a trade as expired.
    load_latest
        Return the latest trade of a symbol for a strategy.
    get_run_symbols
        Return the symbols with unexpired trades in a run.
    rename_algo_run_id
        Move the trades of a symbol to another run.
    get_latest_algo_run_id
        Return the run of the latest trade of a symbol.
    """

    def __init__(
        self,
        algo_run_id: int,
        symbol: str,
        operation: str,
        qty: float,
        price: float,
        indicators: dict,
    ):
        """Initialize an unsaved trade.

        Parameters
        ----------
        algo_run_id: int
            The identifier of the run making the trade.
        symbol: str
            The traded symbol.
        operation: str
            The operation, buy or sell.
        qty: float
            The quantity traded.
        price: float
            The trade price.
        indicators: dict
            The indicators at the time of the trade.
        """
        self.algo_run_id = algo_run_id
        self.symbol = symbol
        self.qty = qty
        self.price = price
        self.indicators = indicators
        self.operation = operation
        self.trade_id = None

    async def save(
        self,
        pool: Pool,
        client_buy_time: str,
        stop_price=None,
        target_price=None,
        trade_fee=0.0,
    ):
        """Save this trade as a new record and store its ID in trade_id.

        Store the indicators as an empty object if they contain values that
        JSON cannot represent, such as NaN.

        Parameters
        ----------
        pool: Pool
            The connection pool.
        client_buy_time: str
            The client-side time of the trade.
        stop_price, default None
            The stop price, or None.
        target_price, default None
            The target price, or None.
        trade_fee, default 0.0
            The fee paid for the trade.
        """
        async with pool.acquire() as con:
            async with con.transaction():
                try:
                    indicators_s = json.dumps(
                        self.indicators or {}, allow_nan=False
                    )
                except ValueError:
                    indicators_s = json.dumps({})

                self.trade_id = await con.fetchval(
                    """
                        INSERT INTO new_trades (algo_run_id, symbol, operation, qty, price, indicators, client_time, stop_price, target_price, trade_fee)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        RETURNING trade_id
                    """,
                    self.algo_run_id,
                    self.symbol,
                    self.operation,
                    self.qty,
                    self.price,
                    indicators_s,
                    client_buy_time,
                    stop_price,
                    target_price,
                    trade_fee,
                )

    @classmethod
    async def expire_trade(cls, pool: Pool, trade_id: int) -> None:
        """Mark a trade as expired as of the current time.

        Parameters
        ----------
        pool: Pool
            The connection pool.
        trade_id: int
            The trade identifier.
        """
        async with pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    """
                        UPDATE new_trades SET expire_tstamp='now()' WHERE trade_id=$1
                    """,
                    trade_id,
                )

    @classmethod
    async def load_latest(
        cls, pool: Pool, symbol: str, strategy_name: str
    ) -> Tuple[int, float, float, float, Dict, datetime]:
        """Return the latest trade of a symbol made by a strategy.

        Missing prices are returned as 0.0 and missing indicators as an empty
        dictionary.

        Parameters
        ----------
        pool: Pool
            The connection pool.
        symbol: str
            The traded symbol.
        strategy_name: str
            The name of the strategy.

        Returns
        -------
        Return a tuple of the run identifier, price, stop price, target price,
        indicators and trade time.

        Raises
        ------
        Raise ValueError if the strategy has no trade in the symbol.
        """
        async with pool.acquire() as con:
            async with con.transaction():
                row = await con.fetchrow(
                    """
                        SELECT t.algo_run_id, t.price, t.stop_price, t.target_price, t.indicators, t.tstamp 
                        FROM new_trades as t, algo_run as a
                        WHERE 
                            t.algo_run_id=a.algo_run_id AND
                            a.algo_name=$2 AND
                            symbol=$1 
                        ORDER BY tstamp DESC LIMIT 1
                    """,
                    symbol,
                    strategy_name,
                )

                if row:
                    return (
                        int(row[0]),
                        float(row[1]),
                        float(row[2] or 0.0),
                        float(row[3] or 0.0),
                        json.loads(row[4] or "{}"),
                        row[5],
                    )
                tlog(f"{symbol} no data for strategy {strategy_name}")
                raise ValueError

    @classmethod
    async def get_run_symbols(
        cls, run_id: int, pool: Pool = None
    ) -> List[str]:
        """Return the symbols that have unexpired trades in a run.

        Parameters
        ----------
        run_id: int
            The run identifier.
        pool: Pool, default None
            The connection pool, or None for the shared pool.
        """
        rc: List = []
        if not pool:
            pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    """
                        SELECT DISTINCT symbol
                        FROM new_trades
                        WHERE algo_run_id = $1 and expire_tstamp is null
                    """,
                    run_id,
                )

                if rows:
                    rc = [row[0] for row in rows]

        return rc

    @classmethod
    async def rename_algo_run_id(
        cls, new_run_id: int, old_run_id: int, symbol: str, pool: Pool = None
    ) -> None:
        """Move the trades of a symbol from one run to another.

        Parameters
        ----------
        new_run_id: int
            The identifier of the run receiving the trades.
        old_run_id: int
            The identifier of the run that made the trades.
        symbol: str
            The traded symbol.
        pool: Pool, default None
            The connection pool, or None for the shared pool.
        """
        if not pool:
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    """
                        UPDATE 
                            new_trades 
                        SET 
                            algo_run_id=$1 
                        WHERE 
                            algo_run_id=$2 AND
                            symbol=$3
                    """,
                    new_run_id,
                    old_run_id,
                    symbol,
                )

    @classmethod
    async def get_latest_algo_run_id(
        cls, symbol: str, pool: Pool = None
    ) -> int:
        """Return the run identifier of the latest trade of a symbol.

        Parameters
        ----------
        symbol: str
            The traded symbol.
        pool: Pool, default None
            The connection pool, or None for the shared pool.

        Returns
        -------
        Return None if the symbol has no trades.
        """
        if not pool:
            pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                val = await con.fetchval(
                    """
                        SELECT  algo_run_id
                        FROM new_trades
                        WHERE symbol = $1
                        ORDER BY tstamp DESC
                        LIMIT 1
                    """,
                    symbol,
                )

                return val
