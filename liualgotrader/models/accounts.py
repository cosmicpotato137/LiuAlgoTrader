"""Persist and load accounts and their transactions.

Classes
-------
Accounts
    Database model for accounts and their transactions.
"""

import datetime
import json
from typing import Dict

import asyncpg
import pandas as pd

from liualgotrader.common import config
from liualgotrader.common.database import fetch_as_dataframe
from liualgotrader.common.tlog import tlog


class Accounts:
    """Database model for accounts and their transactions.

    Methods
    -------
    create
        Create an account and return its identifier.
    check_if_enough_balance_to_withdraw
        Check that a withdrawal is covered.
    get_balance
        Return the balance of an account.
    add_transaction
        Record a transaction against an account.
    get_transactions
        Return the transactions of an account.
    clear_balance
        Overwrite the balance of an account.
    clear_account_transactions
        Delete the transactions of an account.
    """

    @classmethod
    async def create(
        cls,
        balance: float,
        allow_negative: bool = False,
        credit_line: float = 0.0,
        details: Dict = {},
    ) -> int:
        """Create an account and return its identifier.

        Parameters
        ----------
        balance: float
            The opening balance.
        allow_negative: bool, default False
            Whether the balance may go below zero.
        credit_line: float, default 0.0
            The credit available below zero.
        details: Dict, default {}
            Extra account details, stored as JSON.
        """
        q = """
                INSERT INTO 
                    accounts (balance, allow_negative, credit_line, details)
                VALUES
                    ($1, $2, $3, $4)
                RETURNING account_id;
            """
        params = [balance, allow_negative, credit_line, json.dumps(details)]
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                result = await con.fetchval(q, *params)

        return result

    @classmethod
    async def check_if_enough_balance_to_withdraw(
        cls, account_id: int, potential_withdraw: float
    ) -> bool:
        """Return whether an account can cover a withdrawal.

        Parameters
        ----------
        account_id: int
            The account identifier.
        potential_withdraw: float
            The amount to withdraw.

        Returns
        -------
        Return True if the balance exceeds the amount, or if the account allows
        a negative balance and the balance plus the credit line exceeds it.

        Raises
        ------
        Raise AssertionError if potential_withdraw is negative.
        """
        if potential_withdraw < 0:
            raise AssertionError(
                f"check_if_enough_balance(): potential transaction amount ({potential_withdraw}) can't be negative "
            )

        query = """
            SELECT
                balance, allow_negative, credit_line
            FROM 
                accounts 
            WHERE
                account_id = $1
        """
        rc = False
        async with config.db_conn_pool.acquire() as con:
            row = await con.fetchrow(query, account_id)
            if row["balance"] > potential_withdraw:
                rc = True
            elif (
                row["allow_negative"]
                and row["balance"] + row["credit_line"] > potential_withdraw
            ):
                rc = True

        return rc

    @classmethod
    async def get_balance(cls, account_id: int) -> float:
        """Return the balance of an account.

        Parameters
        ----------
        account_id: int
            The account identifier.

        Returns
        -------
        Return None if the account does not exist.
        """
        pool = config.db_conn_pool

        async with pool.acquire() as con:
            result = await con.fetchval(
                """
                    SELECT
                        balance
                    FROM 
                        accounts 
                    WHERE
                        account_id = $1
                """,
                account_id,
            )
        return result

    @classmethod
    async def add_transaction(
        cls,
        account_id: int,
        amount: float,
        tstamp: datetime.datetime = None,
        details: Dict = {},
    ):
        """Record a transaction against an account and update its balance.

        Parameters
        ----------
        account_id: int
            The account identifier.
        amount: float
            The amount added to the balance, negative for a withdrawal.
        tstamp: datetime.datetime, default None
            The transaction time, or None for the current time.
        details: Dict, default {}
            Extra transaction details, stored as JSON.

        Raises
        ------
        Log and re-raise any database error.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                try:
                    if tstamp:
                        await con.execute(
                            """
                                INSERT INTO 
                                    account_transactions (account_id, amount, tstamp, details)
                                VALUES
                                    ($1, $2, $3, $4);
                            """,
                            account_id,
                            amount,
                            tstamp,
                            json.dumps(details),
                        )
                    else:
                        await con.execute(
                            """
                                INSERT INTO 
                                    account_transactions (account_id, amount, details)
                                VALUES
                                    ($1, $2, $3);
                            """,
                            account_id,
                            amount,
                            json.dumps(details),
                        )
                except Exception as e:
                    tlog(
                        f"[EXCEPTION] add_transaction({account_id}, {amount}, {tstamp} ) failed with {e} current balance={await cls.get_balance(account_id)}"
                    )
                    raise

    @classmethod
    async def get_transactions(cls, account_id: int) -> pd.DataFrame:
        """Return the transactions of an account as a DataFrame.

        The DataFrame holds the amounts, indexed and sorted by timestamp.

        Parameters
        ----------
        account_id: int
            The account identifier.

        Returns
        -------
        Return an empty DataFrame if the account has no transactions.
        """
        q = """
            SELECT
                tstamp, amount
            FROM 
                account_transactions 
            WHERE
                account_id = $1
            """

        df = await fetch_as_dataframe(q, account_id)
        return (
            df.set_index("tstamp", drop=True).sort_index()
            if not df.empty
            else df
        )

    @classmethod
    async def clear_balance(cls, account_id: int, new_balance: float):
        """Overwrite the balance of an account without recording a transaction.

        Parameters
        ----------
        account_id: int
            The account identifier.
        new_balance: float
            The balance to set.

        Raises
        ------
        Log and re-raise any database error.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                try:
                    await con.execute(
                        """
                            UPDATE  
                                accounts
                            SET
                                balance = $2
                            WHERE
                                account_id = $1
                        """,
                        account_id,
                        new_balance,
                    )
                except Exception as e:
                    tlog(
                        f"[EXCEPTION] clear_balance() failed with {e} current balance={await cls.get_balance(account_id)}"
                    )
                    raise

    @classmethod
    async def clear_account_transactions(cls, account_id: int):
        """Delete all transactions of an account without changing its balance.

        Parameters
        ----------
        account_id: int
            The account identifier.

        Raises
        ------
        Log and re-raise any database error.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                try:
                    await con.execute(
                        """
                            DELETE FROM  
                                account_transactions
                            WHERE
                                account_id = $1
                        """,
                        account_id,
                    )
                except Exception as e:
                    tlog(
                        f"[EXCEPTION] clear_account_transactions() failed with {e} current balance={await cls.get_balance(account_id)}"
                    )
                    raise
