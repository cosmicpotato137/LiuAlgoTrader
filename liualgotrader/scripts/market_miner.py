"""Off-hours calculations and data collection.

Usage: market_miner
Run it in the directory that holds the miner configuration, miner.toml.

Functions
---------
motd
    Log the welcome banner.
main
    Load and validate the miners named in the configuration.
main_cli
    Run the market miner from the command line.
"""

import asyncio
import importlib.util
import os
import sys
import traceback
from typing import Dict, List, Optional

import pygit2
import toml

from liualgotrader.common import config
from liualgotrader.common.concurrency import get_event_loop
from liualgotrader.common.database import create_db_connection
from liualgotrader.common.tlog import tlog
from liualgotrader.miners.base import Miner

# rom liualgotrader.miners.stock_cluster import StockCluster


def motd(filename: str, version: str) -> None:
    """Log the welcome banner and the database DSN.

    Parameters
    ----------
    filename: str
        The script file name.
    version: str
        The build label.
    """
    print("+=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=+")
    tlog(f"{filename} {version} starting")
    print("+=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=+")
    tlog(f"DSN: {config.dsn}")
    print("+=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=+")


async def main(conf_dict: Dict):
    """Load and validate the miner classes named in the configuration.

    Exit the process if a miner class does not inherit from Miner, and log any
    other loading error. The miners are not run.

    Parameters
    ----------
    conf_dict: Dict
        The miner configuration, with a "miners" section keyed by miner name.
    """
    task_list: List[Optional[asyncio.Task]] = []

    await create_db_connection()
    for miner in conf_dict["miners"]:
        try:
            if "filename" in conf_dict["miners"][miner]:
                spec = importlib.util.spec_from_file_location(
                    "module.name", conf_dict["miners"][miner]["filename"]
                )
                if not spec:
                    raise AssertionError(
                        f"could not load module {conf_dict['miners'][miner]['filename']}"
                    )
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)  # type: ignore
            else:
                module = importlib.import_module(
                    f"liualgotrader.miners.{miner}"
                )
            class_name = f"{miner[0].upper()}{miner[1:]}"
            miner_class = getattr(module, class_name)

            if not issubclass(miner_class, Miner):
                tlog(f"Miner must inherit from class {Miner.__name__}")
                exit(0)
        except Exception as e:
            tlog(f"[ERROR] miner {miner} resulted in exception:`{e}`")


def main_cli() -> None:
    """Run the market miner from the command line.

    Load the miner configuration from the current directory and run main. Exit
    if the configuration file is not found.
    """
    try:
        build_label = pygit2.Repository("../").describe(
            describe_strategy=pygit2.GIT_DESCRIBE_TAGS
        )
    except pygit2.GitError:
        import liualgotrader

        build_label = liualgotrader.__version__ if hasattr(liualgotrader, "__version__") else ""  # type: ignore
    config.build_label = build_label
    filename = os.path.basename(__file__)
    motd(filename=filename, version=build_label)

    # load configuration
    tlog(
        f"loading configuration file from {os.getcwd()}/{config.miner_configuration_filename}"
    )
    try:
        conf_dict = toml.load(config.miner_configuration_filename)
    except FileNotFoundError:
        tlog(
            f"[ERROR] could not locate market_miner configuration file {config.miner_configuration_filename}"
        )
        sys.exit(0)

    try:
        get_event_loop().close()
        asyncio.run(main(conf_dict))
    except KeyboardInterrupt:
        tlog("market_miner.main() - Caught KeyboardInterrupt")
    except Exception as e:
        tlog(
            f"market_miner.main() - exception of type {type(e).__name__} with args {e.args}"
        )
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)
        del exc_info

    tlog("*** market_miner completed ***")
