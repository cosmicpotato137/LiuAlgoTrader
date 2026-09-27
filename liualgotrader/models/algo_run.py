"""Persist and load strategy run records and their batches.

Classes
-------
AlgoRun
    Record of a single strategy run within a batch.
"""

import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Tuple

from asyncpg.pool import Pool
from pandas import DataFrame

from liualgotrader.common import config


class AlgoRun:
    """Record of a single strategy run within a batch.

    Attributes
    ----------
    run_id: Any[None, int]
        The run identifier, or None until the run is saved.
    strategy_name: str
        The name of the strategy.
    batch_id: str
        The identifier of the batch the run belongs to.

    Methods
    -------
    save
        Save the run and store its identifier.
    update_end_time
        Mark the run as ended.
    get_batch_ids
        Return the identifiers of batches started after a date.
    get_batches
        Return a summary of the most recent batches.
    get_batch_list_by_date
        Return the runs started on a date, by batch.
    get_batch_details
        Return the runs of a batch.
    """

    def __init__(self, strategy_name: str, batch_id: str):
        """Initialize an unsaved run.

        Parameters
        ----------
        strategy_name: str
            The name of the strategy.
        batch_id: str
            The identifier of the batch the run belongs to.
        """
        self.run_id: Any[None, int] = None
        self.strategy_name = strategy_name
        self.batch_id = batch_id

    async def save(
        self,
        pool: Pool = None,
        env: str = "unknown",
        ref_algo_run_id: int = None,
    ) -> None:
        """Save this run as a new record and store its identifier in run_id.

        The record also holds the configured build label and DSN.

        Parameters
        ----------
        pool: Pool, default None
            The connection pool, or None for the shared pool.
        env: str, default "unknown"
            The environment name, with "unknown" used if empty.
        ref_algo_run_id: int, default None
            The identifier of a related run, or None.
        """
        if not pool:
            pool = config.db_conn_pool

        async with pool.acquire() as con:
            async with con.transaction():
                if not ref_algo_run_id:
                    q = """
                        INSERT INTO algo_run (algo_name, algo_env, build_number, parameters, batch_id)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING algo_run_id
                        """

                    self.run_id = await con.fetchval(
                        q,
                        self.strategy_name,
                        env or "unknown",
                        config.build_label,
                        json.dumps(
                            {
                                "DSN": config.dsn,
                            }
                        ),
                        self.batch_id,
                    )

                else:
                    q = """
                        INSERT INTO algo_run (algo_name, algo_env, build_number, parameters, batch_id, ref_algo_run)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        RETURNING algo_run_id
                        """
                    self.run_id = await con.fetchval(
                        q,
                        self.strategy_name,
                        env or "unknown",
                        config.build_label,
                        json.dumps(
                            {
                                "DSN": config.dsn,
                            }
                        ),
                        self.batch_id,
                        ref_algo_run_id,
                    )

    async def update_end_time(self, pool: Pool, end_reason: str):
        """Mark this run as ended at the current time.

        Parameters
        ----------
        pool: Pool
            The connection pool.
        end_reason: str
            The reason the run ended.
        """
        async with pool.acquire() as con:
            async with con.transaction():
                await con.execute(
                    """
                        UPDATE algo_run SET end_time='now()',end_reason=$1
                        WHERE algo_run_id = $2
                    """,
                    end_reason,
                    self.run_id,
                )

    @classmethod
    async def get_batch_ids(
        cls, pool: Pool = None, start_date: date = date(2019, 1, 1)
    ) -> DataFrame:
        """Return the distinct identifiers of batches with runs after a date.

        Parameters
        ----------
        pool: Pool, default None
            The connection pool, or None for the shared pool.
        start_date: date, default date(2019, 1, 1)
            The date after which runs are included.

        Returns
        -------
        Return a DataFrame with a single batch_id column.
        """
        if not pool:
            pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    """
                        SELECT DISTINCT batch_id
                        FROM algo_run
                        WHERE start_time > $1
                        GROUP BY batch_id
                    """,
                    start_date,
                )
                return DataFrame(data=rows, columns=["batch_id"])

    @classmethod
    async def get_batches(
        cls,
        pool: Pool = None,
        nax_batches: int = 30,
        start_date: date = date(2019, 1, 1),
    ) -> List:
        """Return a summary of the most recent batches.

        Each entry lists the build number, batch identifier, strategy name and
        start time, all as strings.

        Parameters
        ----------
        pool: Pool, default None
            The connection pool, or None for the shared pool.
        nax_batches: int, default 30
            The maximum number of entries to return.
        start_date: date, default date(2019, 1, 1)
            The date after which runs are included.

        Returns
        -------
        Return a list, newest first, with one entry for each batch, strategy,
        environment and build.
        """
        if not pool:
            pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    """
                        SELECT build_number, batch_id, algo_name, date_trunc('minute', min(start_time)) as start
                        FROM algo_run
                        WHERE start_time > $2
                        GROUP BY batch_id, algo_name, algo_env, build_number
                        ORDER BY start DESC
                        LIMIT $1
                    """,
                    nax_batches,
                    start_date,
                )

                if rows:
                    return [list(map(str, row.values())) for row in rows]

        return []

    @classmethod
    async def get_batch_list_by_date(
        cls,
        batch_date: date,
        pool: Pool = None,
    ) -> Dict[str, List[str]]:
        """Return the runs started on a date, grouped by batch.

        Each run lists its identifier, strategy name, environment, build number
        and start time.

        Parameters
        ----------
        batch_date: date
            The date the runs started.
        pool: Pool, default None
            The connection pool, or None for the shared pool.

        Returns
        -------
        Return a dictionary that maps each batch identifier to a list of runs,
        newest first.
        """
        rc: Dict = {}
        if not pool:
            pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    """
                        SELECT batch_id, algo_run_id, algo_name, algo_env, build_number, start_time
                        FROM algo_run
                        ORDER BY start_time DESC
                        WHERE start_time >= $1 and start_time < $2
                    """,
                    batch_date,
                    batch_date + timedelta(days=1),
                )

                if rows:
                    for row in rows:
                        if row[0] not in rc:
                            rc[row[0]] = [list(row.values())[1:]]
                        else:
                            rc[row[0]].append(list(row.values())[1:])

        return rc

    @classmethod
    async def get_batch_details(
        cls, batch_id: str, pool: Pool = None
    ) -> List[Tuple[int, datetime, datetime, str]]:
        """Return the runs of a batch, newest first.

        Each run is a tuple of its identifier, start time, end time and
        parameters.

        Parameters
        ----------
        batch_id: str
            The batch identifier.
        pool: Pool, default None
            The connection pool, or None for the shared pool.

        Returns
        -------
        Return an empty list if the batch has no runs.
        """
        rc: List = []
        if not pool:
            pool = config.db_conn_pool
        async with pool.acquire() as con:
            async with con.transaction():
                rows = await con.fetch(
                    """
                        SELECT algo_run_id, start_time, end_time, parameters, algo_name
                        FROM algo_run
                        WHERE batch_id = $1
                        ORDER BY start_time DESC
                    """,
                    batch_id,
                )

                if rows:
                    rc = [
                        (
                            row[0],
                            row[1],
                            row[2],
                            row[3],
                        )
                        for row in rows
                    ]

        return rc
