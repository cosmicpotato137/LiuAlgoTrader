"""Persist string values under unique keys in the database.

Classes
-------
KeyStore
    Persistent key-value store.
"""

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog


class KeyStore:
    """Persistent key-value store.

    Methods
    -------
    load
        Return the value stored under a key.
    save
        Store a value under a key.
    """

    @classmethod
    async def load(cls, key: str):
        """Return the value stored under a key, or None if there is none.

        Parameters
        ----------
        key: str
            The key to look up.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            val = await con.fetchval(
                """
                    SELECT 
                        value
                    FROM 
                        keystore
                    WHERE 
                        key = $1
                """,
                key,
            )

            return val

    @classmethod
    async def save(cls, key: str, value: str):
        """Store a value under a key, replacing any existing value.

        Parameters
        ----------
        key: str
            The key to store under.
        value: str
            The value to store.
        """
        pool = config.db_conn_pool
        async with pool.acquire() as con:
            await con.execute(
                """
                    INSERT INTO 
                        keystore(key, value)
                    VALUES 
                        ($1, $2)
                    ON CONFLICT(key) 
                        DO UPDATE 
                            SET value = $2;
                """,
                key,
                value,
            )
