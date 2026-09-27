"""Persist and load the trending symbols of a batch.

Classes
-------
TrendingTickers
    Database model for the trending symbols of a batch.
"""

from typing import List, Tuple
from datetime import datetime
from asyncpg.pool import Pool

from liualgotrader.common import config


class TrendingTickers:
    """Database model for the trending symbols of a batch.

    Attributes
    ----------
    batch_id: str
        The batch identifier.
    trending_id: int
        The identifier of the last saved symbol, or 0.

    Methods
    -------
    save
        Save trending symbols for the batch.
    load
        Return the trending symbols of a batch.
    """

    def __init__(self, batch_id: str):
        """Initialize the model for a batch.

        Parameters
        ----------
        batch_id: str
            The batch identifier.
        """
        self.batch_id = batch_id
        self.trending_id: int = 0

    async def save(self, symbols: List[str], pool: Pool = None) -> int:
        """Save trending symbols for the batch.

        Store the identifier of the last saved symbol in trending_id and return
        it. Leave trending_id unchanged if symbols is empty.

        Parameters
        ----------
        symbols: List[str]
            The symbols to save.
        pool: Pool, default None
            The connection pool, or None for the shared pool.
        """
        if not pool:
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            async with con.transaction():
                for symbol in symbols:
                    self.trending_id = await con.fetchval(
                        """
                            INSERT INTO trending_tickers (batch_id, symbol)
                            VALUES ($1, $2)
                            RETURNING trending_id
                        """,
                        self.batch_id,
                        symbol,
                    )

        return self.trending_id

    @classmethod
    async def load(cls, batch_id, pool: Pool = None) -> List[Tuple[str, datetime]]:
        """Return the trending symbols of a batch.

        Parameters
        ----------
        batch_id
            The batch identifier.
        pool: Pool, default None
            The connection pool, or None for the shared pool.

        Returns
        -------
        Return a list of (symbol, creation time) tuples.

        Raises
        ------
        Raise Exception if the batch has no symbols.
        """
        if not pool:
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    """
                        SELECT symbol, create_tstamp
                        FROM trending_tickers 
                        WHERE batch_id=$1
                    """,
                    batch_id,
                )

                if rows:
                    return [(row[0], row[1]) for row in rows]
                else:
                    raise Exception("no data")
