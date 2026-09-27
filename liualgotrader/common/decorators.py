"""Decorators that instrument function execution.

Functions
---------
timeit
    Wrap a function so that its execution time is logged.
"""

import asyncio
import time

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog


def timeit(func):
    """Wrap a function so that its execution time is logged.

    Parameters
    ----------
    func
        The function or coroutine function to wrap.

    Returns
    -------
    Return a coroutine function, which must be awaited even when func is
    synchronous.
    """
    async def process(func, *args, **params):
        """Return the result of func, awaiting it if it is a coroutine.

        Parameters
        ----------
        func
            The function or coroutine function.
        *args
            Positional arguments for func.
        **params
            Keyword arguments for func.
        """
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **params)
        else:
            return func(*args, **params)

    async def helper(*args, **params):
        """Run the wrapped function, log its duration and return its result.

        Parameters
        ----------
        *args
            Positional arguments for the wrapped function.
        **params
            Keyword arguments for the wrapped function.
        """
        tlog(f"{func.__name__} started")
        start = time.time()
        result = await process(func, *args, **params)
        tlog(f"{func.__name__} >>> {round(time.time() - start, 3)} seconds")
        return result

    return helper
