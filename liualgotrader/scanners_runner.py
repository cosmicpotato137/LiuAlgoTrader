"""Run scanners periodically and pump new symbols to the producer process.

Functions
---------
scanner_runner
    Run a scanner and publish the symbols it selects.
create_momentum_scanner
    Create a Momentum scanner from its settings.
create_scanners
    Create the configured scanners.
scanners_runner
    Run all configured scanners concurrently.
async_main
    Connect to the database and run the scanners.
main
    Run the scanner process.
"""

import asyncio
import json
import multiprocessing as mp
import os
import traceback
from datetime import timedelta
from typing import Dict, List

from pytz import timezone

from liualgotrader.common import config
from liualgotrader.common.data_loader import DataLoader  # type: ignore
from liualgotrader.common.database import create_db_connection
from liualgotrader.common.tlog import tlog
from liualgotrader.common.types import DataConnectorType
from liualgotrader.scanners.base import Scanner
from liualgotrader.scanners.momentum import Momentum
from liualgotrader.trading.base import Trader
from liualgotrader.trading.trader_factory import trader_factory

nyc = timezone("America/New_York")


async def scanner_runner(scanner: Scanner, queue: mp.Queue) -> None:
    """Run scanner and publish the symbols it selects to queue.

    Repeat at the recurrence of the scanner, or run once if it has none. Log
    exceptions instead of raising them.

    Parameters
    ----------
    scanner: Scanner
        The scanner to run.
    queue: mp.Queue
        The queue that receives the results as JSON.
    """
    try:
        while True:
            symbols = await scanner.run()

            if len(symbols):
                tlog(f"Scanner {scanner.name} picked {len(symbols)} symbols")
                queue.put(
                    json.dumps(
                        [
                            {
                                "symbol": symbol.lower(),
                                "target_strategy_name": scanner.target_strategy_name,
                            }
                            for symbol in symbols
                        ]
                    )
                )

            if not scanner.recurrence:
                break

            await asyncio.sleep(scanner.recurrence.total_seconds())
            tlog(f"scanner {scanner.name} re-running")
    except asyncio.CancelledError:
        tlog(
            f"scanner_runner() cancelled, closing scanner task {scanner.name}"
        )
    except Exception as e:
        traceback.print_exc()
        tlog(
            f"[ERROR]Exception in scanner_runner({scanner.name}): exception of type {type(e).__name__} with args {e.args}"
        )
    finally:
        tlog(f"scanner_runner {scanner.name} completed")


async def create_momentum_scanner(
    trader: Trader, data_loader: DataLoader, scanner_details: Dict
) -> Momentum:
    """Create a Momentum scanner from its configuration.

    Exit the process if a required setting is missing.

    Parameters
    ----------
    trader: Trader
        The trader passed to the scanner.
    data_loader: DataLoader
        The loader passed to the scanner.
    scanner_details: Dict
        The scanner settings: the filter values and optional recurrence,
        target_strategy_name and max_symbols.
    """
    try:
        recurrence = scanner_details.get("recurrence", None)
        target_strategy_name = scanner_details.get(
            "target_strategy_name", None
        )
        return Momentum(
            data_loader=data_loader,
            trading_api=trader,
            min_last_dv=scanner_details["min_last_dv"],
            min_share_price=scanner_details["min_share_price"],
            max_share_price=scanner_details["max_share_price"],
            min_volume=scanner_details["min_volume"],
            from_market_open=scanner_details["from_market_open"],
            today_change_percent=scanner_details["today_change_percent"],
            recurrence=timedelta(minutes=recurrence) if recurrence else None,
            target_strategy_name=target_strategy_name,
            max_symbols=scanner_details.get(
                "max_symbols", config.total_tickers
            ),
        )
    except KeyError as e:
        tlog(
            f"Error {e} in processing of scanner configuration {scanner_details}"
        )
        exit(0)


async def create_scanners(
    trader: Trader, data_loader: DataLoader, scanners_conf: Dict
) -> List[Scanner]:
    """Create the scanners listed in scanners_conf.

    Build the momentum entry as the built-in Momentum scanner and load every
    other entry as a custom scanner class.

    Parameters
    ----------
    trader: Trader
        The trader passed to the momentum scanner.
    data_loader: DataLoader
        The loader for custom scanners, and the connector for the momentum
        scanner's loader.
    scanners_conf: Dict
        A mapping of scanner name to its settings.

    Returns
    -------
    Return the scanners in configuration order.
    """
    scanners: List[Scanner] = []

    for scanner_name in scanners_conf:
        if scanner_name == "momentum":
            scanners.append(
                await create_momentum_scanner(
                    trader,
                    DataLoader(connector=data_loader),
                    scanners_conf[scanner_name],
                )
            )
            tlog("instantiated momentum scanner")
        else:
            tlog(f"custom scanner {scanner_name} selected")
            scanners.append(
                await Scanner.get_scanner(
                    data_loader, scanner_name, scanners_conf[scanner_name]
                )
            )

    return scanners


async def scanners_runner(
    scanners_conf: Dict, queue: mp.Queue, trader: Trader
) -> None:
    """Run all configured scanners concurrently until they finish.

    Collect scanner exceptions instead of raising them, and cancel the scanners
    if cancelled. Close queue before returning.

    Parameters
    ----------
    scanners_conf: Dict
        A mapping of scanner name to its settings.
    queue: mp.Queue
        The queue that receives the scanner results.
    trader: Trader
        The trader passed to the scanners.
    """
    print("** scanners_runner() task starting **")
    scanners: List[Scanner] = await create_scanners(
        trader, DataLoader(), scanners_conf
    )
    scanner_tasks = [
        asyncio.create_task(scanner_runner(scanner, queue))
        for scanner in scanners
    ]

    try:
        await asyncio.gather(
            *scanner_tasks,
            return_exceptions=True,
        )

    except asyncio.CancelledError:
        tlog(
            "scanners_runner.scanners_runner() cancelled, closing scanner tasks"
        )

        for task in scanner_tasks:
            tlog(
                f"scanners_runner.scanners_runner()  requesting task {task.get_name()} to cancel"
            )
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                tlog(
                    "scanners_runner.scanners_runner()  task is cancelled now"
                )

    finally:
        queue.close()
        tlog("scanners_runner.scanners_runner()  done.")


async def async_main(scanners_conf: Dict, queue: mp.Queue) -> None:
    """Connect to the database and run the configured scanners.

    Collect exceptions instead of raising them.

    Parameters
    ----------
    scanners_conf: Dict
        A mapping of scanner name to its settings.
    queue: mp.Queue
        The queue that receives the scanner results.
    """
    await create_db_connection(str(config.dsn))

    main_task = asyncio.create_task(
        scanners_runner(
            scanners_conf,
            queue,
            trader=(at := trader_factory()),
        ),
        name="main_task",
    )

    await asyncio.gather(
        main_task,
        return_exceptions=True,
    )


def main(
    conf_dict: Dict,
    scanner_queue: mp.Queue,
) -> None:
    """Run the scanner process until all scanners complete.

    Do nothing if the scanners section is empty. Log exceptions and keyboard
    interrupts instead of raising them.

    Parameters
    ----------
    conf_dict: Dict
        The configuration, with a scanners section.
    scanner_queue: mp.Queue
        The queue that receives the scanner results.

    Raises
    ------
    Raise KeyError if conf_dict has no scanners section.
    """
    tlog(f"*** scanners_runner.main() starting w pid {os.getpid()} ***")

    if scanners_conf := conf_dict["scanners"]:
        try:
            asyncio.run(async_main(scanners_conf, scanner_queue))
        except KeyboardInterrupt:
            tlog("scanners_runner.main() - Caught KeyboardInterrupt")
        except Exception as e:
            tlog(
                f"scanners_runner.main() - exception of type {type(e).__name__} with args {e.args}"
            )

    tlog("*** scanners_runner.main() completed ***")
