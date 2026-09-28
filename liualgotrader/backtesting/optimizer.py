"""Run and record the back-tests of a hyper-parameter optimizer session.

Functions
---------
save_optimizer_session
    Record a back-test batch in an optimizer session.
backtest_iteration
    Run one daily US-equities back-test and record it.
"""

import asyncio
import traceback
import uuid
from datetime import date
from typing import Dict

import liualgotrader
from liualgotrader import enhanced_backtest
from liualgotrader.common import config
from liualgotrader.common.concurrency import get_event_loop
from liualgotrader.common.hyperparameter import Hyperparameters
from liualgotrader.common.tlog import tlog
from liualgotrader.common.types import AssetType, TimeScale
from liualgotrader.models.optimizer import OptimizerRun


async def save_optimizer_session(
    optimizer_session_id: str, batch_id: str, hypers: Hyperparameters
):
    """Record a back-test batch as part of an optimizer session.

    Parameters
    ----------
    optimizer_session_id: str
        The optimizer session identifier.
    batch_id: str
        The back-test batch identifier.
    hypers: Hyperparameters
        The hyper-parameter values, saved in string form.
    """
    await OptimizerRun.save(optimizer_session_id, batch_id, str(hypers))


def backtest_iteration(
    optimizer_session_id: str,
    start_date: date,
    end_date: date,
    conf_dict: Dict,
    hypers: Hyperparameters,
):
    """Run one daily US-equities back-test and record it in the session.

    Intended as the target of a separate process. Set config.build_label,
    record the batch only if the back-test succeeds, log any exception instead
    of raising it, and print the new batch identifier.

    Parameters
    ----------
    optimizer_session_id: str
        The optimizer session identifier.
    start_date: date
        The first date of the back-test.
    end_date: date
        The last date of the back-test.
    conf_dict: Dict
        The trade-plan configuration.
    hypers: Hyperparameters
        The hyper-parameter values being tested.
    """
    tlog(
        f"starting backtest with start_date={start_date}, end_date={end_date} w/ configuration={conf_dict}"
    )
    uid = str(uuid.uuid4())

    config.build_label = liualgotrader.__version__ if hasattr(liualgotrader, "__version__") else ""  # type: ignore
    try:
        get_event_loop().close()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop.run_until_complete(
            enhanced_backtest.backtest_main(
                uid,
                start_date,
                end_date,
                TimeScale.day,
                conf_dict,
                0.0,
                0.0,
                AssetType.US_EQUITIES,
            )
        )
    except KeyboardInterrupt:
        tlog("backtest() - Caught KeyboardInterrupt")
    except Exception as e:
        tlog(
            f"backtest() - exception of type {type(e).__name__} with args {e.args}"
        )
        traceback.print_exc()
    else:
        loop.run_until_complete(
            save_optimizer_session(optimizer_session_id, uid, hypers)
        )
    finally:
        print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
        print(f"new batch-id: {uid}")
