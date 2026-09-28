"""Size the consumer pool and provide the event loop for synchronous code.

Functions
---------
calc_num_consumer_processes
    Return how many consumer processes to launch.
get_event_loop
    Return the current thread's event loop, creating one if needed.
"""

import asyncio

import psutil

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog


def calc_num_consumer_processes() -> int:
    """Return the number of consumer processes to launch.

    Use config.num_consumers when it is positive. Otherwise, derive the count
    from the physical cores and the idle CPU fraction, blocking for five
    seconds to sample the load, or return 1 if the core count is unknown.
    """
    if config.num_consumers > 0:
        return config.num_consumers

    num_cpu = psutil.cpu_count(False)

    if not num_cpu:
        tlog(
            "Can't automatically detect number of CPUs, use fixed configuration"
        )
        return 1

    load_pct: float = psutil.cpu_percent(interval=5)

    tlog(f"Total CPU Load:{load_pct}, num_cpu:{num_cpu}")

    return int(5.0 * (1 - load_pct / 100.0) * num_cpu)


def get_event_loop() -> asyncio.AbstractEventLoop:
    """Return the current thread's event loop, creating one if needed.

    Set a newly created loop as the current loop, so later calls in the same
    thread return it. Replace the current loop if it is closed. Do not call it
    from asynchronous code.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None

    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop
