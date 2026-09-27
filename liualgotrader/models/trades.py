"""Deprecated persistence of round-trip trades.

Classes
-------
Trade
    Deprecated record of a round-trip trade.
"""

import json
from typing import Dict

from asyncpg.pool import Pool
from deprecated import deprecated


@deprecated(reason="should not be used")
class Trade:
    """Deprecated record of a round-trip trade.

    The class is deprecated and should not be used.

    Attributes
    ----------
    algo_run_id: int
        The identifier of the run making the trade.
    symbol: str
        The traded symbol.
    qty: int
        The quantity bought.
    buy_price: float
        The buy price.
    buy_indicators: dict
        The indicators at the time of the buy.
    trade_id
        The trade identifier, or None until the buy is saved.
    sell_price: float
        The sell price, set by save_sell.
    sell_indicators: Dict
        The indicators at the time of the sell.
    is_win: bool
        Whether the sell price exceeds the buy price.

    Methods
    -------
    save_buy
        Save the buy side of the trade.
    save_sell
        Save the sell side of the trade.
    """

    sell_price: float
    sell_indicators: Dict
    is_win: bool

    def __init__(
        self,
        algo_run_id: int,
        symbol: str,
        qty: int,
        price: float,
        indicators: dict,
    ):
        """Initialize the buy side of a trade.

        Parameters
        ----------
        algo_run_id: int
            The identifier of the run making the trade.
        symbol: str
            The traded symbol.
        qty: int
            The quantity bought.
        price: float
            The buy price.
        indicators: dict
            The indicators at the time of the buy.
        """
        self.algo_run_id = algo_run_id
        self.symbol = symbol
        self.qty = qty
        self.buy_price = price
        self.buy_indicators = indicators
        self.trade_id = None

    async def save_buy(self, pool: Pool, client_buy_time: str):
        """Save the buy side of this trade and store its ID in trade_id.

        Parameters
        ----------
        pool: Pool
            The connection pool.
        client_buy_time: str
            The client-side time of the buy.
        """
        async with pool.acquire() as con:
            async with con.transaction():
                self.trade_id = await con.fetchval(
                    """
                        INSERT INTO trades (algo_run_id, symbol, qty, buy_price, buy_indicators, client_buy_time)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        RETURNING trade_id
                    """,
                    self.algo_run_id,
                    self.symbol,
                    self.qty,
                    self.buy_price,
                    json.dumps(self.buy_indicators),
                    client_buy_time,
                )

    async def save_sell(
        self, pool: Pool, price: float, indicators: dict, client_sell_time: str
    ):
        """Save the sell side of this trade.

        Set sell_price, sell_indicators and is_win, and save them with the
        current time as the sell time.

        Parameters
        ----------
        pool: Pool
            The connection pool.
        price: float
            The sell price.
        indicators: dict
            The indicators at the time of the sell.
        client_sell_time: str
            The client-side time of the sell.
        """
        self.sell_price = price
        self.sell_indicators = indicators
        self.is_win = self.sell_price > self.buy_price
        async with pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    """
                        UPDATE trades SET client_sell_time=$5, sell_price=$1, sell_indicators=$2, is_win=$3, sell_time='now()'
                        WHERE trade_id = $4
                    """,
                    self.sell_price,
                    json.dumps(self.sell_indicators),
                    self.is_win,
                    self.trade_id,
                    client_sell_time,
                )
