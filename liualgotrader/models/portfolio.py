"""Persist and load trading portfolios.

Classes
-------
Portfolio
    Trading portfolio backed by a dedicated account.
"""

import json
from typing import Dict, List, Optional, Tuple

import tabulate
from pandas import DataFrame

from liualgotrader.common import config
from liualgotrader.common.database import create_db_connection
from liualgotrader.common.types import AssetType
from liualgotrader.models.accounts import Accounts


class Portfolio:
    """Trading portfolio backed by a dedicated account.

    Attributes
    ----------
    portfolio_id: str
        The portfolio identifier.
    portfolio_size: float
        The portfolio size.
    parameters: Dict
        The portfolio parameters.
    account_id: Optional[int]
        The identifier of the associated account, or None.
    asset_type
        The type of assets the portfolio trades.

    Methods
    -------
    load_by_batch_id
        Load the portfolio associated with a batch.
    load_by_portfolio_id
        Load a portfolio by its identifier.
    save
        Create a portfolio and its account.
    associate_batch_id_to_profile
        Associate a batch with a portfolio.
    is_associated
        Return whether a portfolio has an associated batch.
    exists
        Return whether a portfolio exists.
    get_portfolio_account_balance
        Return the balance of a portfolio.
    get_external_account_id
        Return the external account and broker.
    list_portfolios
        Return the portfolios that have not expired.
    """

    def __init__(
        self,
        portfolio_id: str,
        portfolio_size: float,
        asset_type: AssetType,
        account_id: Optional[int],
        parameters: Dict,
    ):
        """Initialize a portfolio.

        Parameters
        ----------
        portfolio_id: str
            The portfolio identifier.
        portfolio_size: float
            The portfolio size.
        asset_type: AssetType
            The name of an AssetType member.
        account_id: Optional[int]
            The identifier of the associated account, or None.
        parameters: Dict
            The portfolio parameters.
        """
        self.portfolio_id = portfolio_id
        self.portfolio_size = portfolio_size
        self.parameters = parameters
        self.account_id = account_id
        self.asset_type = AssetType[str(asset_type)]

    def __str__(self):
        """Return a readable description of the portfolio."""
        return f"id={self.portfolio_id} account-id={self.account_id} size={self.portfolio_size} asset_type={self.asset_type}, params={self.parameters}"

    @classmethod
    async def load_by_batch_id(cls, batch_id: str):
        """Load the portfolio associated with a batch.

        Connect to the database first if no pool exists yet.

        Parameters
        ----------
        batch_id: str
            The batch identifier.

        Returns
        -------
        Return a Portfolio, or None if the batch has no portfolio.
        """
        try:
            pool = config.db_conn_pool
        except AttributeError:
            await create_db_connection()
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            data = await con.fetchrow(
                """
                    SELECT p.portfolio_id, p.size, p.assets, p.account_id, p.parameters
                    FROM 
                        portfolio as p, portfolio_batch_ids as b
                    WHERE
                        p.portfolio_id = b.portfolio_id
                        AND b.batch_id = $1
                """,
                batch_id,
            )

            if data:
                return Portfolio(*data)

    @classmethod
    async def load_by_portfolio_id(cls, portfolio_id: str):
        """Load a portfolio by its identifier.

        Connect to the database first if no pool exists yet.

        Parameters
        ----------
        portfolio_id: str
            The portfolio identifier.

        Returns
        -------
        Return a Portfolio, or None if it does not exist.
        """
        try:
            pool = config.db_conn_pool
        except AttributeError:
            await create_db_connection()
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            data = await con.fetchrow(
                """
                    SELECT p.portfolio_id, p.size, p.assets, p.account_id, p.parameters
                    FROM 
                        portfolio as p
                    WHERE
                        p.portfolio_id = $1
                """,
                portfolio_id,
            )

            if data:
                return Portfolio(*data)

    @classmethod
    async def save(
        cls,
        portfolio_id: str,
        portfolio_size: float,
        credit: float,
        parameters: Dict,
        asset_type: AssetType = AssetType.US_EQUITIES,
        external_account_id: Optional[str] = None,
        broker: Optional[str] = None,
    ):
        """Create a portfolio together with a dedicated account.

        Connect to the database first if no pool exists yet.

        Parameters
        ----------
        portfolio_id: str
            The portfolio identifier.
        portfolio_size: float
            The portfolio size and opening account balance.
        credit: float
            The credit line, where a positive value allows a negative balance.
        parameters: Dict
            The portfolio parameters, also stored as account details.
        asset_type: AssetType, default AssetType.US_EQUITIES
            The type of assets traded.
        external_account_id: Optional[str], default None
            The account identifier at the broker, or None.
        broker: Optional[str], default None
            The broker name, or None.
        """
        if not hasattr(config, "db_conn_pool"):
            await create_db_connection()
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                account_id = await Accounts.create(
                    portfolio_size,
                    allow_negative=credit > 0.0,
                    credit_line=credit,
                    details=parameters,
                )
                await con.execute(
                    """
                        INSERT INTO portfolio (portfolio_id, size, account_id, assets, parameters, external_account_id, broker)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    portfolio_id,
                    portfolio_size,
                    account_id,
                    asset_type.name,
                    json.dumps(parameters),
                    external_account_id,
                    broker,
                )

    @classmethod
    async def associate_batch_id_to_profile(
        cls, portfolio_id: str, batch_id: str
    ) -> None:
        """Associate a batch with a portfolio.

        Parameters
        ----------
        portfolio_id: str
            The portfolio identifier.
        batch_id: str
            The batch identifier.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            await con.execute(
                """
                    INSERT INTO portfolio_batch_ids (portfolio_id, batch_id)
                    VALUES ($1, $2)
                """,
                portfolio_id,
                batch_id,
            )

    @classmethod
    async def is_associated(
        cls, portfolio_id: str, batch_id: Optional[str] = None
    ) -> bool:
        """Return whether a portfolio is associated with a batch.

        Parameters
        ----------
        portfolio_id: str
            The portfolio identifier.
        batch_id: Optional[str], default None
            The batch to check, or None for any batch.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            return (
                await con.fetchval(
                    """
                        SELECT EXISTS (
                            SELECT 1 
                            FROM portfolio_batch_ids
                            WHERE portfolio_id = $1 AND batch_id=$2
                        )
                    """,
                    portfolio_id,
                    batch_id,
                )
                if batch_id
                else await con.fetchval(
                    """
                        SELECT EXISTS (
                            SELECT 1 
                            FROM portfolio_batch_ids
                            WHERE portfolio_id = $1
                        )
                    """,
                    portfolio_id,
                )
            )

    @classmethod
    async def exists(cls, portfolio_id: str) -> bool:
        """Return whether a portfolio exists.

        Parameters
        ----------
        portfolio_id: str
            The portfolio identifier.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            result = await con.fetchval(
                """
                    SELECT EXISTS (
                        SELECT 1 
                        FROM portfolio
                        WHERE portfolio_id = $1
                    )
                """,
                portfolio_id,
            )
            return result

    @classmethod
    async def get_portfolio_account_balance(cls, portfolio_id: str) -> float:
        """Return the balance of the account of a portfolio.

        Parameters
        ----------
        portfolio_id: str
            The portfolio identifier.

        Returns
        -------
        Return None if the portfolio does not exist.
        """
        pool = config.db_conn_pool

        async with pool.acquire() as con:
            return await con.fetchval(
                """
                    SELECT
                        balance
                    FROM 
                        accounts as a, portfolio as f 
                    WHERE
                        a.account_id = f.account_id
                        AND portfolio_id = $1
                """,
                portfolio_id,
            )

    @classmethod
    async def get_external_account_id(
        cls, portfolio_id: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """Return the external account identifier and broker of a portfolio.

        Parameters
        ----------
        portfolio_id: str
            The portfolio identifier.

        Returns
        -------
        Return the two values as a tuple; either may be None.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            r = await con.fetchrow(
                """
                    SELECT
                        external_account_id, broker
                    FROM 
                        portfolio
                    WHERE
                        portfolio_id = $1
                """,
                portfolio_id,
            )

            return (r["external_account_id"], r["broker"])

    @classmethod
    async def list_portfolios(cls) -> List[str]:
        """Return the portfolios that have not expired, newest first.

        A portfolio appears once for each of its account transactions, or once
        if it has none. Connect to the database first if no pool exists yet.

        Returns
        -------
        Return a DataFrame with the columns portfolio_id, last_transaction,
        size, parameters, assets and tstamp.
        """
        try:
            pool = config.db_conn_pool
        except AttributeError:
            await create_db_connection()
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    """
                        SELECT
                            portfolio_id , a.tstamp as last_transaction, size, parameters ,assets, p.tstamp 
                        FROM
                            portfolio as p
                        LEFT JOIN account_transactions as a
                        ON
                            p.account_id = a.account_id
                        WHERE
                            p.expire_tstamp is NULL
                        ORDER BY 
                            p.tstamp DESC
                    """,
                )
                return DataFrame(
                    data=rows,
                    columns=[
                        "portfolio_id",
                        "last_transaction",
                        "size",
                        "parameters",
                        "assets",
                        "tstamp",
                    ],
                )
