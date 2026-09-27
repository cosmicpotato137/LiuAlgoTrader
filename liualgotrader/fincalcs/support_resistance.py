"""Locate support, resistance and stop price levels.

Classes
-------
StopRangeType
    Enumeration of the look-back windows for price levels.

Functions
---------
grouper
    Yield runs of consecutive values that lie close to one another.
find_resistances
    Asynchronously find resistance levels at or above a price.
find_supports
    Return the local price minima below the current value.
find_stop
    Return the most recent local price minimum as a stop level.
get_local_maxima
    Return the local maxima of a series.
"""

from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional

import numpy as np
import pandas as pd
import pytz
from pandas import DataFrame as df
from pandas import Timestamp as ts

from liualgotrader.common import config

est = pytz.timezone("US/Eastern")


class StopRangeType(Enum):
    """Enumeration of the look-back windows for locating price levels."""

    LAST_100_MINUTES = 1
    LAST_2_HOURS = 2
    LAST_3_HOURS = 3
    DAILY = 10
    WEEKLY = 20
    DATE_RANGE = 50


def grouper(iterable):
    """Yield runs of consecutive values that lie close to one another.

    Start a new group, yielded as a list, when a value differs from its
    predecessor by more than config.group_margin, relative to the predecessor.

    Parameters
    ----------
    iterable
        The numeric values to group.
    """
    prev = None
    group = []
    for item in iterable:

        if (
            not prev
            or -config.group_margin
            <= float(item - prev) / prev
            <= config.group_margin
        ):
            group.append(item)
        else:
            yield group
            group = [item]
        prev = item
    if group:
        yield group


async def find_resistances(
    symbol: str,
    strategy_name: str,
    current_value: float,
    minute_history: df,
    debug=False,
) -> Optional[List[float]]:
    """Return the resistance levels at or above the current value.

    Parameters
    ----------
    symbol: str
        The symbol; has no effect.
    strategy_name: str
        The calling strategy; has no effect.
    current_value: float
        The current price.
    minute_history: df
        The minute bars of the symbol.
    debug, default False
        Has no effect.

    Returns
    -------
    Return, in ascending order, the local maxima of the 15-minute highs of the
    close during market hours over the last three days, or None if none reach
    current_value.
    """
    est = pytz.timezone("America/New_York")
    back_time = ts(datetime.now(est)).to_pydatetime() - timedelta(days=3)
    back_time_index = minute_history["close"].index.get_loc(
        back_time, method="nearest"
    )

    series = (
        minute_history["close"][back_time_index:]
        .dropna()
        .between_time("9:30", "16:00")
        .resample("15min")
        .max()
    ).dropna()

    diff = np.diff(series.values)
    high_index = np.where((diff[:-1] >= 0) & (diff[1:] <= 0))[0] + 1
    if len(high_index) > 0:
        local_maximas = sorted(
            [series[i] for i in high_index if series[i] >= current_value]
        )
        if len(local_maximas) > 0:
            return local_maximas

    return None


def find_supports(
    current_value,
    minute_history,
    now: datetime,
    range_type: StopRangeType = StopRangeType.LAST_100_MINUTES,
):
    """Return the local price minima that lie below the current value.

    Search the five-minute lows of the window.

    Parameters
    ----------
    current_value
        The current price.
    minute_history
        The minute bars of the symbol.
    now: datetime
        The current time, whose day limits the search.
    range_type: StopRangeType, default StopRangeType.LAST_100_MINUTES
        The look-back window.

    Returns
    -------
    Return None if the window has no local minimum.

    Raises
    ------
    Raise NotImplementedError for the weekly and date ranges.
    """
    # get low Series based on select time-range
    if range_type == StopRangeType.DAILY:
        series = (
            minute_history["low"][
                ts(
                    now.replace(
                        hour=9, minute=30, second=0, microsecond=0, tzinfo=est
                    )
                ) :
            ]
            .resample("5min")
            .min()
        )
    elif range_type == StopRangeType.LAST_100_MINUTES:
        series = minute_history["low"][-100:].dropna().resample("5min").min()
        series = series[ts(now).floor("1D") :]
    elif range_type == StopRangeType.LAST_2_HOURS:
        series = minute_history["low"][-120:].dropna().resample("5min").min()
        series = series[ts(now).floor("1D") :]
    elif range_type == StopRangeType.LAST_3_HOURS:
        series = minute_history["low"][-180:].dropna().resample("5min").min()
        series = series[ts(now).floor("1D") :]
    else:
        raise NotImplementedError(
            f"stop-range type {range_type} is not implemented"
        )

    # find local minima
    diff = np.diff(series.values)
    low_index = np.where((diff[:-1] <= 0) & (diff[1:] > 0))[0] + 1
    if len(low_index) > 0:
        return [series[x] for x in low_index if series[x] < current_value]
    return None


def find_stop(
    current_value,
    minute_history,
    now: datetime,
    range_type: StopRangeType = StopRangeType.LAST_100_MINUTES,
):
    """Return the most recent local price minimum as a stop level.

    Search the five-minute lows of the window.

    Parameters
    ----------
    current_value
        The current price; has no effect.
    minute_history
        The minute bars of the symbol.
    now: datetime
        The current time, whose day limits the search.
    range_type: StopRangeType, default StopRangeType.LAST_100_MINUTES
        The look-back window, where any window but the daily one means the last
        100 minutes.

    Returns
    -------
    Return None if there is no local minimum.

    Raises
    ------
    Raise NotImplementedError for the weekly and date ranges.
    """
    if range_type in (StopRangeType.DATE_RANGE, StopRangeType.WEEKLY):
        raise NotImplementedError(
            f"stop-range type {range_type} is not implemented"
        )

    if range_type == StopRangeType.DAILY:
        series = (
            minute_history["low"][
                ts(
                    now.replace(
                        hour=9, minute=30, second=0, microsecond=0, tzinfo=est
                    )
                ) :
            ]
            .dropna()
            .resample("5min")
            .min()
        )
    else:
        series = minute_history["low"][-100:].dropna().resample("5min").min()
        series = series[ts(now).floor("1D") :]

    diff = np.diff(series.values)
    low_index = np.where((diff[:-1] <= 0) & (diff[1:] > 0))[0] + 1
    if len(low_index) > 0:
        return series[low_index[-1]]  # - max(0.05, current_value * 0.02)
    return None  # current_value * config.default_stop


def get_local_maxima(
    series: pd.Series,
    debug=False,
) -> pd.Series:
    """Return the local maxima of a series.

    Search the five-minute highs of series.

    Parameters
    ----------
    series: pd.Series
        The time-indexed values to search.
    debug, default False
        Has no effect.

    Returns
    -------
    Return the maxima indexed by time, or an empty series if there are none.
    """
    if series.empty:
        return pd.Series([], dtype=np.float64)

    series = series.resample("5min").max()
    diff = np.diff(series.values)
    high_index = np.where((diff[:-1] >= 0) & (diff[1:] <= 0))[0] + 1

    return (
        pd.Series(
            index=[series.index[i] for i in high_index],
            data=[series[i] for i in high_index],
            dtype=np.float64,
        )
        if len(high_index) > 0
        else pd.Series([], dtype=np.float64)
    )
