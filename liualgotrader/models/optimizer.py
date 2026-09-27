"""Persist and load the batches run by optimizer sessions.

Classes
-------
OptimizerRun
    Database model for the batches run by optimizer sessions.
"""

from typing import List, Tuple

from liualgotrader.common import config
from liualgotrader.common.database import create_db_connection


class OptimizerRun:
    """Database model for the batches run by optimizer sessions.

    Methods
    -------
    save
        Record a batch run by an optimizer session.
    get_portfolio_ids_parameters
        Return the portfolios of a session.
    """

    @classmethod
    async def save(
        cls, optimizer_session_id: str, batch_id: str, parameters: str
    ):
        """Record a batch run as part of an optimizer session.

        Parameters
        ----------
        optimizer_session_id: str
            The optimizer session identifier.
        batch_id: str
            The batch identifier.
        parameters: str
            The parameters used for the batch.
        """
        async with config.db_conn_pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    """
                        INSERT INTO optimizer_run (optimizer_session_id, batch_id, parameters)
                        VALUES ($1, $2, $3)
                    """,
                    optimizer_session_id,
                    batch_id,
                    parameters,
                )

    @classmethod
    async def get_portfolio_ids_parameters(
        cls,
        optimizer_session_id: str,
    ) -> List[Tuple[str, str]]:
        """Return the portfolios of an optimizer session and their parameters.

        Connect to the database first if no pool exists yet.

        Parameters
        ----------
        optimizer_session_id: str
            The optimizer session identifier.

        Returns
        -------
        Return a list of (portfolio_id, parameters) tuples, one for each batch
        of the session.
        """
        try:
            _ = config.db_conn_pool
        except AttributeError:
            await create_db_connection()

        async with config.db_conn_pool.acquire() as con:
            recrods = await con.fetch(
                """
                    SELECT 
                        p.portfolio_id, o.parameters
                    FROM 
                        optimizer_run as o, portfolio_batch_ids as p
                    WHERE
                        o.batch_id = p.batch_id
                        AND optimizer_session_id = $1;
                """,
                optimizer_session_id,
            )

        return [(row[0], row[1]) for row in recrods]
