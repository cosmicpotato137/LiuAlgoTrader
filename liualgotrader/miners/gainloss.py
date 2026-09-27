"""Miner for the gain and loss data of recent trading batches.

Classes
-------
Gainloss
    Miner that computes gain and loss data for recent trading batches.
"""

import sys
import traceback
from datetime import date, timedelta
from typing import Dict

from liualgotrader.analytics import consolidate
from liualgotrader.common.tlog import tlog
from liualgotrader.miners.base import Miner
from liualgotrader.models.algo_run import AlgoRun


class Gainloss(Miner):
    """Miner that computes gain and loss data for recent trading batches.

    Attributes
    ----------
    days
        The number of days to look back.
    debug
        Whether debugging is enabled.

    Methods
    -------
    run
        Consolidate the trades of each recent batch.
    """

    def __init__(
        self,
        data: Dict,
        debug=False,
    ):
        """Initialize the miner from its settings.

        Parameters
        ----------
        data: Dict
            The miner settings, with the number of days to look back under
            "days".
        debug, default False
            Whether debugging is enabled.

        Raises
        ------
        Raise ValueError if "days" is missing or not an integer.
        """
        try:
            self.days = int(data["days"])
            self.debug = debug
        except Exception:
            raise ValueError(
                "[ERROR] Miner must receive positive `days` parameter"
            )
        super().__init__(name="GainLossMiner")

    async def run(self) -> bool:
        """Consolidate the trades of every batch in the look-back period.

        Store the gain and loss data of each batch in the database, logging
        progress.

        Returns
        -------
        Return True when done.

        Raises
        ------
        Log and re-raise any consolidation error.
        """
        data = await AlgoRun.get_batch_ids(
            start_date=date.today() - timedelta(days=self.days)
        )
        tlog(f"Miner {self.name} will consolidate {len(data)} batches")
        for index, row in data.iterrows():
            tlog(f"{int(index)+1}/{len(data)}")
            try:
                await consolidate.trades(row.batch_id)
            except Exception as e:
                tlog(f"[ERROR] aborted w/ exception {e}")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                raise

        return True
