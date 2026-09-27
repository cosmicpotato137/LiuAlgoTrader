"""Optimize strategy hyperparameters by backtesting their combinations.

Usage: optimizer [--concurrency=<processes>]
Backtest each hyperparameter combination in the optimizer section of the
trade plan, tradeplan.toml in TRADEPLAN_DIR or the current directory, with
at most 4 concurrent backtests by default.

Functions
---------
motd
    Log the welcome banner.
show_usage
    Print the command-line usage of the optimizer.
create_parameters
    Return the parameters defined in a configuration.
create_hyperparameters
    Return the hyperparameters defined in a configuration.
dateFromString
    Return the date described by a natural-language string.
realize
    Return a copy of a configuration with parameter values applied.
optimize
    Run a backtest for each hyperparameter combination in parallel.
load_configuration
    Return the configuration loaded from a TOML file.
main_cli
    Run the hyperparameter optimizer from the command line and exit.
"""

import copy
import getopt
import multiprocessing as mp
import os
import sys
import uuid
from datetime import date, datetime
from typing import Dict, List

import parsedatetime.parsedatetime as pdt
import pygit2

from liualgotrader.backtesting.optimizer import backtest_iteration
from liualgotrader.common import config
from liualgotrader.common.hyperparameter import Hyperparameters, Parameter
from liualgotrader.common.tlog import tlog


def motd(filename: str, version: str) -> None:
    """Log the welcome banner.

    Parameters
    ----------
    filename: str
        The script file name.
    version: str
        The build label.
    """
    tlog("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    tlog("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    tlog(f"{filename} {version} starting")
    tlog("----------------------------------------------------------")
    tlog("----------------------------------------------------------")


def show_usage():
    """Print the command-line usage of the optimizer."""
    print(
        f"usage:\n{sys.argv[0]}  [--concurrency=<int> DEFAULT 4]",
    )


def create_parameters(parameters: Dict) -> List[Parameter]:
    """Return the parameters defined in a parameter configuration.

    Name each parameter by its dotted path, strategies.<strategy>.<name>.

    Parameters
    ----------
    parameters: Dict
        The configuration, with a "strategies" section that maps each strategy
        to its parameter settings.
    """
    params = []

    for strategy in parameters["strategies"]:
        for name in parameters["strategies"][strategy]:
            params.append(
                Parameter(
                    f"strategies.{strategy}.{name}",
                    **parameters["strategies"][strategy][name],
                )
            )

    tlog(f"created {len(params)} parameters")
    return params


def create_hyperparameters(parameters: Dict) -> Hyperparameters:
    """Return the hyperparameters defined in a parameter configuration.

    Parameters
    ----------
    parameters: Dict
        The configuration, in the form accepted by create_parameters.
    """
    params = create_parameters(parameters)
    tlog(f"created {len(params)} hyper-parameters")
    return Hyperparameters(params)


def dateFromString(s: str) -> date:
    """Return the date described by a natural-language string.

    Parameters
    ----------
    s: str
        The date string, either explicit or relative, such as "yesterday".

    Raises
    ------
    Raise ValueError if the string cannot be parsed as a date or a time.
    """
    c = pdt.Calendar()
    result, what = c.parse(s)

    dt = None

    # what was returned (see http://code-bear.com/code/parsedatetime/docs/)
    # 0 = failed to parse
    # 1 = date (with current time, as a struct_time)
    # 2 = time (with current date, as a struct_time)
    # 3 = datetime
    if what in (1, 2):
        # result is struct_time
        dt = datetime(*result[:6]).date()
    elif what == 3:
        # result is a datetime
        dt = result.date()

    if dt is None:
        raise ValueError("Don't understand date '" + s + "'")

    return dt


def realize(config: Dict, parameters: tuple) -> Dict:
    """Return a copy of a configuration with parameter values applied.

    Parameters
    ----------
    config: Dict
        The configuration; not modified.
    parameters: tuple
        Pairs of a dotted configuration path and its value.
    """
    d = copy.deepcopy(config)

    for parameter in parameters:
        name = parameter[0].split(".")

        element: Dict = d
        for i in name[:-1]:
            element = element[i]
        element[name[-1]] = parameter[1]

    return d


def optimize(
    start_date: date,
    end_date: date,
    hyperparameters: Hyperparameters,
    parameters: List[Parameter],
    conf_dict: Dict,
    concurrency: int,
):
    """Run a backtest for each hyperparameter combination in parallel.

    Run each backtest in its own process. A portfolio parameter creates a new
    portfolio in the database for each backtest.

    Parameters
    ----------
    start_date: date
        The first day to backtest.
    end_date: date
        The last day to backtest.
    hyperparameters: Hyperparameters
        The hyperparameter combinations to test.
    parameters: List[Parameter]
        The parameters, such as portfolios, created anew for each backtest.
    conf_dict: Dict
        The trade plan to apply the values to.
    concurrency: int
        The maximum number of backtests to run at the same time.
    """
    optimizer_session_id = str(uuid.uuid4())
    tlog("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
    tlog(f"optimizer session_id {optimizer_session_id} starting")
    tlog("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")

    processes = []
    for hyperparameter in iter(hyperparameters):
        parameters_values = (x() for x in parameters)
        p = mp.Process(
            target=backtest_iteration,
            args=(
                optimizer_session_id,
                start_date,
                end_date,
                realize(realize(conf_dict, hyperparameter), parameters_values),  # type: ignore
                hyperparameter,
            ),
        )
        processes.append(p)
        p.start()

        if len(processes) == concurrency:
            for p in processes:
                p.join()
            processes = []

    print("done.")


def load_configuration(filename: str):
    """Return the configuration loaded from a TOML file.

    Exit the process if the file is not found.

    Parameters
    ----------
    filename: str
        The path of the file.
    """
    try:
        import toml

        conf_dict = toml.load(filename)
        tlog(f"loaded configuration file from {filename}")
    except FileNotFoundError:
        tlog(f"[ERROR] could not locate tradeplan file {filename}")
        sys.exit(0)

    return conf_dict


def main_cli() -> None:
    """Run the hyperparameter optimizer from the command line and exit.

    Run optimize with the optimizer section of the trade plan, taking an
    optional --concurrency option (default 4) for the number of parallel
    backtests. Exit early on invalid arguments, a missing optimizer section or
    unparsable dates.
    """
    mp.set_start_method("spawn")

    if len(sys.argv) > 2:
        show_usage()
        sys.exit(0)

    concurrency: int = 4
    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            "c",
            ["concurrency="],
        )
        for opt, arg in opts:
            if opt in ("--concurrency", "-c"):
                concurrency = int(arg)
    except getopt.GetoptError as e:
        print(f"Error parsing options:{e}\n")
        show_usage()
        sys.exit(0)
    try:
        config.build_label = pygit2.Repository("../").describe(
            describe_strategy=pygit2.GIT_DESCRIBE_TAGS
        )
    except pygit2.GitError:
        import liualgotrader

        config.build_label = liualgotrader.__version__ if hasattr(liualgotrader, "__version__") else ""  # type: ignore

    config.filename = os.path.basename(__file__)
    motd(filename=config.filename, version=config.build_label)
    tlog(f"using concurrency {concurrency}")
    folder = (
        config.tradeplan_folder
        if config.tradeplan_folder[-1] == "/"
        else f"{config.tradeplan_folder}/"
    )
    fname = f"{folder}{config.configuration_filename}"
    conf_dict = load_configuration(fname)

    if "optimizer" not in conf_dict:
        tlog(f"optimizer directive not found in {fname}. aborting.")
        sys.exit(0)

    tlog(f"optimizer directive found in {fname}")

    try:
        start_date = dateFromString(conf_dict["optimizer"]["start_date"])
        end_date = dateFromString(conf_dict["optimizer"]["end_date"])
    except Exception:
        tlog("failed to load start & end dates")
        sys.exit(0)

    optimize(
        start_date=start_date,
        end_date=end_date,
        hyperparameters=create_hyperparameters(
            conf_dict["optimizer"]["hyperparameters"]
        ),
        parameters=create_parameters(conf_dict["optimizer"]["parameters"]),
        conf_dict=conf_dict,
        concurrency=concurrency,
    )

    sys.exit(0)
