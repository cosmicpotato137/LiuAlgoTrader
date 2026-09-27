"""Classify price trends and measure volatility.

Classes
-------
SeriesTrendType
    Enumeration of the trend categories of a series.
VolatilityClassificationType
    Enumeration of qualitative volatility levels.

Functions
---------
get_series_trend
    Classify the trend of a series by its regression slope.
volatility
    Return the recent volatility of a symbol's closing prices.
"""

import math
from datetime import datetime, timedelta
from enum import Enum
from typing import Tuple

import numpy as np
import pandas as pd
import pytz
from scipy.stats import linregress

from liualgotrader.common.data_loader import DataLoader  # type: ignore

est = pytz.timezone("US/Eastern")


class SeriesTrendType(Enum):
    """Enumeration of the trend categories of a series."""

    UNKNOWN = 0
    SHARP_DOWN = 1
    DOWN = 5
    UP = 15
    SHARP_UP = 20


class VolatilityClassificationType(Enum):
    """Enumeration of qualitative volatility levels."""

    UNKNOWN = 0
    LOW = 1
    MEDIUM = 5
    HIGH = 10


def get_series_trend(series: pd.Series) -> Tuple[float, SeriesTrendType]:
    """Classify the trend of a series by the slope of its regression line.

    Parameters
    ----------
    series: pd.Series
        The values to classify, in order.

    Returns
    -------
    Return the slope, rounded to three decimals, and the trend: UP in (0, 1],
    SHARP_UP above 1, DOWN in [-1, 0) and SHARP_DOWN otherwise. Return UNKNOWN
    with slope 0 for under four values, or math.inf on a floating-point error.

    Raises
    ------
    Set NumPy to raise on every floating-point error.
    """
    if len(series) < 4:
        return 0, SeriesTrendType.UNKNOWN

    try:
        np.seterr(all="raise")
        slope, _, _, _, _ = linregress(np.arange(len(series)), series)
        slope = round(slope, 3)
    except FloatingPointError:
        return math.inf, SeriesTrendType.UNKNOWN

    if 0 < slope <= 1.0:
        t = SeriesTrendType.UP
    elif slope > 1.0:
        t = SeriesTrendType.SHARP_UP
    elif -1.0 <= slope < 0:
        t = SeriesTrendType.DOWN
    else:
        t = SeriesTrendType.SHARP_DOWN

    return slope, t


def volatility(data_loader: DataLoader, symbol: str, now: datetime) -> float:
    """Return the recent volatility of a symbol's closing prices.

    Parameters
    ----------
    data_loader: DataLoader
        The loader to read prices from, which may fetch data.
    symbol: str
        The symbol to measure.
    now: datetime
        The end of the thirty-day look-back period.

    Returns
    -------
    Return the latest 20-bar rolling standard deviation of the percentage
    changes of the close.
    """
    return (
        data_loader[symbol]
        .close[now - timedelta(days=30) : now]  # type: ignore
        .pct_change()
        .rolling(20)
        .std()
        .iloc[-1]
    )
