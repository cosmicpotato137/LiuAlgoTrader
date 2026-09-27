"""Volume-weighted average price (VWAP) calculations.

Functions
---------
add_daily_vwap
    Compute the VWAP of price bars from an anchor time.
anchored_vwap
    Return the VWAP series anchored at a start time.
"""

from datetime import datetime

import pandas as pd
from pandas import DataFrame as df
from pandas import Timestamp as ts

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog


def add_daily_vwap(
    minute_data: df, debug=False, back_time=None, in_place=True
):
    """Compute the volume-weighted average price from an anchor time.

    Add average, pv, apv, av and vwap columns; the last three are missing
    before the anchor.

    Parameters
    ----------
    minute_data: df
        The price bars, with close, high, low and volume columns.
    debug, default False
        Log the input and any lookup error.
    back_time, default None
        The anchor time, matched to the nearest row.
    in_place, default True
        Modify minute_data rather than a copy.

    Returns
    -------
    Return True if in_place is set, otherwise the vwap series from the anchor
    onward. Return False if the anchor cannot be located.
    """
    if debug:
        tlog(f"before vwap {minute_data}")

    try:
        back_time_index = minute_data["close"].index.get_indexer(
            [back_time], method="nearest"
        )[0]
    except Exception as e:
        if debug:
            tlog(
                f"IndexError exception {e} in add_daily_vwap for {minute_data}"
            )
        return False

    df = minute_data if in_place else minute_data.copy()

    df["average"] = df.apply(
        lambda x: (x.close + x.high + x.low) / 3.0, axis=1
    )
    df["pv"] = df.apply(lambda x: x.average * x.volume, axis=1)
    df["apv"] = df["pv"][back_time_index:].cumsum()
    df["av"] = df["volume"][back_time_index:].cumsum()
    df["vwap"] = df["apv"] / df["av"]

    return (
        True
        if in_place
        else df[
            df.close.index.get_indexer([back_time], method="nearest")[0] :
        ].vwap
    )


def anchored_vwap(
    ohlc_data: df, start_time: datetime, debug=False
) -> pd.Series:
    """Return the volume-weighted average price anchored at a start time.

    Parameters
    ----------
    ohlc_data: df
        The price bars, which are left unmodified.
    start_time: datetime
        The anchor time, matched to the nearest row.
    debug, default False
        Log the input and any lookup error.

    Returns
    -------
    Return the vwap series from the anchor onward, or False if the anchor
    cannot be located.
    """
    return add_daily_vwap(
        ohlc_data, debug=debug, back_time=start_time, in_place=False
    )
