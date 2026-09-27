"""Database connection pool and SQL results as pandas DataFrames.

Functions
---------
create_db_connection
    Create the shared database connection pool.
fetch_as_dataframe
    Run an SQL statement and return its rows as a DataFrame.
"""

import asyncpg
import pandas as pd

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog


async def create_db_connection(dsn: str = None) -> None:
    """Create the shared database connection pool.

    Store the pool in config.db_conn_pool, replacing any existing pool, and log
    the DSN.

    Parameters
    ----------
    dsn: str, default None
        The database DSN, or None for config.dsn.
    """
    # if not hasattr(config, "db_conn_pool"):
    _dsn = dsn or config.dsn
    config.db_conn_pool = await asyncpg.create_pool(
        dsn=_dsn,
        min_size=2,
        max_size=10,
    )

    tlog(f"db connection pool initialized w {_dsn}")


async def fetch_as_dataframe(query: str, *args) -> pd.DataFrame:
    """Execute an SQL statement and return its rows as a pandas DataFrame.

    Create the shared connection pool if it does not exist. Print the pool and
    connection objects.

    Parameters
    ----------
    query: str
        The SQL statement to execute.
    *args
        Positional bind parameters for the statement.

    Returns
    -------
    Return an empty DataFrame if there are no rows.
    """
    try:
        config.db_conn_pool
    except (NameError, AttributeError):
        await create_db_connection()

    print("db_conn_pool", config.db_conn_pool, id(config.db_conn_pool))
    async with config.db_conn_pool.acquire() as con:
        print("connection object", con, id(con))
        stmt = await con.prepare(query)
        columns = [a.name for a in stmt.get_attributes()]
        data = await stmt.fetch(*args)

        return (
            pd.DataFrame(data=data, columns=columns)
            if data and len(data) > 0
            else pd.DataFrame()
        )
