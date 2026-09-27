"""Load the trade plan entries that assign strategies to portfolios.

Classes
-------
TradePlan
    Trade plan entry that assigns a strategy to a portfolio.
"""

import json
from datetime import datetime
from typing import List, Optional

import asyncpg

from liualgotrader.common import config


class TradePlan:
    """Trade plan entry that assigns a strategy to a portfolio.

    Attributes
    ----------
    strategy_name: str
        The name of the strategy.
    parameters: dict
        The strategy parameters.
    portfolio_id: str
        The identifier of the portfolio the strategy trades.

    Methods
    -------
    load
        Load the active trade plan entries.
    get_new_entries
        Load the active entries created after a given time.
    """

    def __init__(
        self,
        strategy_name: str,
        parameters: dict,
        portfolio_id: str,
    ):
        """Initialize a trade plan entry.

        Parameters
        ----------
        strategy_name: str
            The name of the strategy.
        parameters: dict
            The strategy parameters.
        portfolio_id: str
            The identifier of the portfolio the strategy trades.
        """
        self.strategy_name = strategy_name
        self.parameters = parameters
        self.portfolio_id = portfolio_id

    def __str__(self):
        """Return the strategy name and parameters, separated by a colon."""
        return f"{self.strategy_name}:{self.parameters}"

    @classmethod
    async def load(cls) -> List["TradePlan"]:
        """Load the active trade plan entries.

        An entry is active from its start date until it expires.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    """
                        SELECT
                            strategy_name, portfolio_id, parameters
                        FROM 
                            trade_plan
                        WHERE 
                            expire_tstamp is null
                            AND start_date <= NOW()
                    """,
                )

                return [
                    TradePlan(
                        strategy_name=r["strategy_name"],
                        portfolio_id=r["portfolio_id"],
                        parameters=json.loads(r["parameters"]),
                    )
                    for r in rows
                ]

    @classmethod
    async def get_new_entries(
        cls, since: datetime
    ) -> Optional[List["TradePlan"]]:
        """Load the active trade plan entries created after a given time.

        Parameters
        ----------
        since: datetime
            The time after which entries are included.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            q = f"""
                    SELECT
                        strategy_name, portfolio_id, parameters
                    FROM 
                        trade_plan
                    WHERE 
                        expire_tstamp is null
                        AND start_date <= NOW()
                        AND create_tstamp > '{since}'
                """
            rows = await con.fetch(q)

            return [
                TradePlan(
                    strategy_name=r["strategy_name"],
                    portfolio_id=r["portfolio_id"],
                    parameters=json.loads(r["parameters"]),
                )
                for r in rows
            ]
