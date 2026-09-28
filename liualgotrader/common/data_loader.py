# type: ignore
"""Market data containers that load bar data on demand.

Classes
-------
SymbolData
    Bar data for a single symbol, loaded on demand.
DataLoader
    Collection of per-symbol market data, loaded on demand.

Functions
---------
convert_offset_to_datetime
    Return the timestamp that matches an offset.
handle_slice_conversion
    Return key with its bounds made timezone-aware.
load_item_by_offset
    Fetch data up to an offset and return the row there.
get_item_by_offset
    Return the row at an offset, fetching data if needed.
fetch_data_range
    Fetch a symbol's data over a range and merge it.
fetch_data_datetime
    Fetch the data needed to cover a timestamp and merge it.
getitem_slice
    Return the rows of a symbol's data selected by a slice.
getitem
    Return the row of a symbol's data selected by a single key.
"""

import concurrent.futures
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dateutil.parser import parse as date_parser
from pytz import timezone

from liualgotrader.common import config
from liualgotrader.common.tlog import tlog, tlog_exception
from liualgotrader.common.types import DataConnectorType, TimeScale
from liualgotrader.data.data_base import DataAPI
from liualgotrader.data.data_factory import data_loader_factory

nyc = timezone("America/New_York")

m_and_a_data = pd.read_csv(
    "https://raw.githubusercontent.com/amor71/LiuAlgoTrader/master/database/market_m_a_data.csv"
).set_index("date")


def _calc_data_to_fetch(s: slice, index: pd.Index) -> List[slice]:
    """Return the ranges that must be fetched to cover a slice.

    Otherwise, return the part of s before the first loaded timestamp, if s
    starts on an earlier day, and the part after the last loaded timestamp, if
    any.

    Parameters
    ----------
    s: slice
        The requested range, with datetime bounds.
    index: pd.Index
        The index of the data already loaded.

    Returns
    -------
    Return [s] if index is empty.
    """
    if index.empty:
        return [s]

    slices = []
    if s.start.date() < index[0].date():
        slices.append(slice(s.start, index[0]))

    if s.stop > index[-1]:
        slices.append(slice(index[-1], s.stop))

    return slices


def convert_offset_to_datetime(
    data_api: DataAPI,
    symbol: str,
    index: pd.Index,
    scale: TimeScale,
    offset: int,
    start: Optional[datetime] = None,
) -> datetime:
    """Return the timestamp that corresponds to an integer offset.

    Parameters
    ----------
    data_api: DataAPI
        The data source used for offsets beyond index.
    symbol: str
        The symbol whose trading calendar applies.
    index: pd.Index
        The index of the data already loaded.
    scale: TimeScale
        The time scale of the data.
    offset: int
        The position to convert.
    start: Optional[datetime], default None
        The reference time for offsets beyond index, or None for the last
        trading time of symbol.

    Returns
    -------
    Return index[offset] if it exists. Otherwise, return the time offset + 1
    minutes or trading days, depending on scale, after the reference time.
    """
    try:
        return index[offset]
    except IndexError:
        last_trading_time = start or data_api.get_last_trading(symbol)
        if scale == TimeScale.minute:
            return last_trading_time.replace(
                second=0, microsecond=0
            ) + timedelta(minutes=1 + offset)

        return data_api.get_trading_day(symbol, last_trading_time, 1 + offset)


def handle_slice_conversion(
    data_api: DataAPI,
    symbol: str,
    key: slice,
    scale: TimeScale,
    index: pd.Index,
) -> slice:
    """Return key with its bounds converted to timezone-aware datetimes.

    Interpret string, date and naive datetime bounds in New York time, and
    integer bounds as offsets into index. Leave other bounds unchanged.

    Parameters
    ----------
    data_api: DataAPI
        The data source used to resolve integer bounds.
    symbol: str
        The symbol whose trading calendar applies.
    key: slice
        The slice to convert.
    scale: TimeScale
        The time scale of the data.
    index: pd.Index
        The index of the data already loaded.
    """
    # handle slice end
    if type(key.stop) == str:
        key = slice(
            key.start,
            nyc.localize(date_parser(key.stop)),
        )
    elif type(key.stop) == int:
        key = slice(
            key.start,
            convert_offset_to_datetime(
                data_api, symbol, index, scale, key.stop
            ),
        )
    elif type(key.stop) == date:
        key = slice(
            key.start,
            nyc.localize(datetime.combine(key.stop, datetime.min.time())),
        )
    elif type(key.stop) == datetime and key.stop.tzinfo is None:
        key = slice(key.start, nyc.localize(key.stop))

    # handle slide start
    if type(key.start) == str:
        key = slice(nyc.localize(date_parser(key.start)), key.stop)
    elif type(key.start) == int:
        key = slice(
            convert_offset_to_datetime(
                data_api, symbol, index, scale, key.start, key.stop
            ),
            key.stop,
        )
    elif type(key.start) == date:
        key = slice(
            nyc.localize(datetime.combine(key.start, datetime.min.time())),
            key.stop,
        )
    elif type(key.start) == datetime and key.start.tzinfo is None:
        key = slice(nyc.localize(key.start), key.stop)

    return key


def load_item_by_offset(
    data_api: DataAPI,
    symbol_data: pd.DataFrame,
    symbol: str,
    scale: TimeScale,
    offset: int,
    concurrency: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch the data needed to reach an offset and return the row there.

    Parameters
    ----------
    data_api: DataAPI
        The data source to fetch from.
    symbol_data: pd.DataFrame
        The data already loaded for the symbol.
    symbol: str
        The symbol to fetch.
    scale: TimeScale
        The time scale of the data.
    offset: int
        The position of the row.
    concurrency: int
        Nonzero to fetch in parallel.

    Returns
    -------
    Return a tuple of the updated data frame and the row at position offset.

    Raises
    ------
    Raise IndexError if the position is still out of range.
    """
    i = convert_offset_to_datetime(
        data_api=data_api,
        symbol=symbol,
        index=symbol_data.index,
        scale=scale,
        offset=offset,
    )
    symbol_data = fetch_data_datetime(
        data_api=data_api,
        symbol_data=symbol_data,
        symbol=symbol,
        scale=scale,
        d=i,
        concurrency=concurrency,
    )
    # index = symbol_data.index.get_indexer([i], method="nearest")[0]
    return (
        symbol_data,
        symbol_data.iloc[offset],  # index
    )


def get_item_by_offset(
    data_api: DataAPI,
    symbol_data: pd.DataFrame,
    symbol: str,
    scale: TimeScale,
    offset: int,
    concurrency: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return the row at an offset, fetching more data if needed.

    Parameters
    ----------
    data_api: DataAPI
        The data source to fetch from.
    symbol_data: pd.DataFrame
        The data already loaded for the symbol.
    symbol: str
        The symbol to fetch.
    scale: TimeScale
        The time scale of the data.
    offset: int
        The position of the row; a negative offset counts back from the most
        recent row.
    concurrency: int
        Nonzero to fetch in parallel.

    Returns
    -------
    Return a tuple of the possibly updated data frame and the row.
    """
    try:
        return symbol_data, symbol_data.iloc[len(symbol_data.index) + offset]
    except IndexError:
        return load_item_by_offset(
            data_api, symbol_data, symbol, scale, offset, concurrency
        )


def _data_fetch_executor(
    data_api: DataAPI,
    symbol: str,
    scale: TimeScale,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Return the data of a symbol for a range of dates.

    Parameters
    ----------
    data_api: DataAPI
        The data source to fetch from.
    symbol: str
        The symbol to fetch.
    scale: TimeScale
        The time scale of the data.
    start: datetime
        The start of the range; only its date is used.
    end: datetime
        The end of the range, inclusive; only its date is used.

    Returns
    -------
    Return a data frame with the standard bar columns, sorted by time.
    """
    df = data_api.get_symbol_data(
        symbol,
        start=(start.date() if isinstance(start, datetime) else start),
        end=(end.date() if isinstance(end, datetime) else end)
        + timedelta(days=1),
        scale=scale,
    )

    return df.reindex(
        columns=[
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "average",
            "count",
        ]
    ).sort_index()


def _concurrent_fetch_data(
    data_api: DataAPI,
    symbol_data: pd.DataFrame,
    symbol: str,
    scale: TimeScale,
    start: datetime,
    end: datetime,
):
    """Fetch a range of data in parallel and merge it with existing data.

    Parameters
    ----------
    data_api: DataAPI
        The data source to fetch from.
    symbol_data: pd.DataFrame
        The data already loaded for the symbol.
    symbol: str
        The symbol to fetch.
    scale: TimeScale
        The time scale of the data.
    start: datetime
        The start of the range.
    end: datetime
        The end of the range.

    Returns
    -------
    Return the merged data frame with the standard bar columns, keeping
    existing rows over fetched duplicates.

    Raises
    ------
    Raise ValueError if data_api splits the range into no parts.
    """
    ranges = data_api.data_concurrency_ranges(
        symbol=symbol, start=start, end=end, scale=scale
    )
    if not len(ranges):
        raise ValueError(f"can't load empty range list {ranges}")

    ranges = list(zip(ranges, ranges[1:]))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                _data_fetch_executor,
                data_api,
                symbol,
                scale,
                range[0],
                range[1],
            ): range
            for range in ranges
        }
        for future in concurrent.futures.as_completed(futures):
            response = future.result()
            symbol_data = pd.concat([symbol_data, response])
            symbol_data = symbol_data[
                ~symbol_data.index.duplicated(keep="first")
            ].sort_index()

    return symbol_data.reindex(
        columns=[
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "average",
            "count",
        ]
    ).sort_index()


def _legacy_fetch_data_range(
    data_api: DataAPI,
    symbol_data: pd.DataFrame,
    symbol: str,
    scale: TimeScale,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Fetch a range of data sequentially and merge it with existing data.

    If symbol was renamed on or after end, also fetch and prefer the data of
    its original symbol. Convert the index of the shared
    mergers-and-acquisitions table to datetimes.

    Parameters
    ----------
    data_api: DataAPI
        The data source to fetch from.
    symbol_data: pd.DataFrame
        The data already loaded for the symbol.
    symbol: str
        The symbol to fetch.
    scale: TimeScale
        The time scale of the data.
    start: datetime
        The start of the range.
    end: datetime
        The end of the range.

    Returns
    -------
    Return the merged data frame with the standard bar columns, preferring
    fetched rows over existing ones.
    """
    adjusted_symbol = symbol
    m_and_a_data.index = m_and_a_data.index.astype("datetime64[ns]", copy=True)
    while True:
        adjusted_data = m_and_a_data.loc[
            (
                end.replace(
                    hour=0, minute=0, second=0, microsecond=0, tzinfo=None
                )
                <= m_and_a_data.index
            )
            & (m_and_a_data.to_symbol == adjusted_symbol)
        ]
        if not adjusted_data.empty:
            adjusted_symbol = adjusted_data.from_symbol.item()
        else:
            break

    if adjusted_symbol != symbol:
        adjusted_df = data_api.get_symbol_data(
            adjusted_symbol,
            start=(start.date() if isinstance(start, datetime) else start),
            end=(end.date() if isinstance(end, datetime) else end)
            + timedelta(days=1),
            scale=scale,
        )
    else:
        adjusted_df = pd.DataFrame()

    new_df = data_api.get_symbol_data(
        symbol,
        start=(start.date() if isinstance(start, datetime) else start),
        end=(end.date() if isinstance(end, datetime) else end)
        + timedelta(days=1),
        scale=scale,
    )
    symbol_data = pd.concat([adjusted_df, new_df, symbol_data], sort=True)
    symbol_data = symbol_data[~symbol_data.index.duplicated(keep="first")]
    return symbol_data.reindex(
        columns=[
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "average",
            "count",
        ]
    ).sort_index()


def fetch_data_range(
    data_api: DataAPI,
    symbol_data: pd.DataFrame,
    symbol: str,
    scale: TimeScale,
    start: datetime,
    end: datetime,
    concurrency: int,
) -> pd.DataFrame:
    """Fetch data for a symbol over a range and merge it with existing data.

    Parameters
    ----------
    data_api: DataAPI
        The data source to fetch from.
    symbol_data: pd.DataFrame
        The data already loaded for the symbol.
    symbol: str
        The symbol to fetch.
    scale: TimeScale
        The time scale of the data.
    start: datetime
        The start of the range.
    end: datetime
        The end of the range.
    concurrency: int
        Nonzero to fetch in parallel, or zero to fetch sequentially and follow
        symbol renames.

    Returns
    -------
    Return the merged data frame.
    """
    if concurrency:
        return _concurrent_fetch_data(
            data_api=data_api,
            symbol_data=symbol_data,
            symbol=symbol,
            scale=scale,
            start=start,
            end=end,
        )
    else:
        return _legacy_fetch_data_range(
            data_api=data_api,
            symbol_data=symbol_data,
            symbol=symbol,
            scale=scale,
            start=start,
            end=end,
        )


def fetch_data_datetime(
    data_api: DataAPI,
    symbol_data: pd.DataFrame,
    symbol: str,
    scale: TimeScale,
    d: datetime,
    concurrency: int,
) -> pd.DataFrame:
    """Fetch the data needed to cover a timestamp and merge it.

    If d lies outside the loaded data, fetch the gap between them; otherwise
    fetch the bar at d.

    Parameters
    ----------
    data_api: DataAPI
        The data source to fetch from.
    symbol_data: pd.DataFrame
        The data already loaded for the symbol.
    symbol: str
        The symbol to fetch.
    scale: TimeScale
        The time scale of the data.
    d: datetime
        The timestamp to cover.
    concurrency: int
        Nonzero to fetch in parallel.

    Returns
    -------
    Return the merged data frame.
    """
    if not symbol_data.empty and d < symbol_data.index.min():
        start = d
        end = symbol_data.index.min()
    elif not symbol_data.empty and d > symbol_data.index.max():
        start = symbol_data.index.max()
        end = d + (
            timedelta(days=1)
            if scale == TimeScale.day
            else timedelta(minutes=1)
        )
    else:
        start = d
        end = d + (
            timedelta(days=1)
            if scale == TimeScale.day
            else timedelta(minutes=1)
        )

    return fetch_data_range(
        data_api=data_api,
        symbol_data=symbol_data,
        symbol=symbol,
        scale=scale,
        start=start,
        end=end,
        concurrency=concurrency,
    )


def getitem_slice(
    data_api: DataAPI,
    symbol: str,
    symbol_data: pd.DataFrame,
    scale: TimeScale,
    key: slice,
    concurrency: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return the rows of a symbol's data selected by a slice.

    Fetch any missing data. A timestamp selects the nearest row, a missing
    start the first row, and a missing or zero stop the last row. Include the
    stop row.

    Parameters
    ----------
    data_api: DataAPI
        The data source to fetch from.
    symbol: str
        The symbol to fetch.
    symbol_data: pd.DataFrame
        The data already loaded for the symbol.
    scale: TimeScale
        The time scale of the data.
    key: slice
        The slice, bounded by integer offsets or by timestamps given as
        strings, dates or datetimes.
    concurrency: int
        Nonzero to fetch in parallel.

    Returns
    -------
    Return a tuple of the updated data frame and the rows.
    """
    key = slice(key.start or 0, key.stop or -1)

    # ensure key represents datetime
    converted_key = handle_slice_conversion(
        data_api, symbol, key, scale, symbol_data.index
    )

    # load data if needed
    for s in _calc_data_to_fetch(converted_key, symbol_data.index):
        symbol_data = fetch_data_range(
            data_api=data_api,
            symbol_data=symbol_data,
            symbol=symbol,
            scale=scale,
            start=s.start,
            end=s.stop,
            concurrency=concurrency,
        )

    # return data range
    if not isinstance(key.start, int):
        key_start_index = symbol_data.index.get_indexer(
            [converted_key.start], method="nearest"
        )[0]
    elif key.start < 0:
        key_start_index = len(symbol_data.index) + key.start
    else:
        key_start_index = key.start

    if not isinstance(key.stop, int):
        key_end_index = symbol_data.index.get_indexer(
            [converted_key.stop], method="nearest"
        )[0]
    elif key.stop < 0:
        key_end_index = len(symbol_data.index) + key.stop
    else:
        key_end_index = key.stop

    return (
        symbol_data,
        symbol_data.iloc[key_start_index : key_end_index + 1],
    )


def getitem(
    data_api: DataAPI,
    symbol_data: pd.DataFrame,
    symbol: str,
    scale: TimeScale,
    key,
    concurrency,
):
    """Return the row of a symbol's data selected by a single key.

    Fetch any missing data. A timestamp, in New York time if naive, selects the
    last row at or before it.

    Parameters
    ----------
    data_api: DataAPI
        The data source to fetch from.
    symbol_data: pd.DataFrame
        The data already loaded for the symbol.
    symbol: str
        The symbol to fetch.
    scale: TimeScale
        The time scale of the data.
    key
        An integer offset, or a timestamp given as a string, date or datetime.
    concurrency
        Nonzero to fetch in parallel.

    Returns
    -------
    Return a tuple of the updated data frame and the row.

    Raises
    ------
    Raise ValueError if symbol has no data.
    """
    if type(key) == str:
        key = nyc.localize(date_parser(key))
    elif type(key) == int:
        symbol_data, rc = get_item_by_offset(
            data_api=data_api,
            symbol_data=symbol_data,
            symbol=symbol,
            scale=scale,
            offset=key,
            concurrency=concurrency,
        )
        return symbol_data, rc

    elif type(key) == date:
        key = nyc.localize(datetime.combine(key, datetime.min.time()))
    elif type(key) == datetime and key.tzinfo is None:
        key = nyc.localize(key)

    for s in _calc_data_to_fetch(slice(key, key), symbol_data.index):
        symbol_data = fetch_data_range(
            data_api=data_api,
            symbol_data=symbol_data,
            symbol=symbol,
            scale=scale,
            start=s.start,
            end=s.stop,
            concurrency=concurrency,
        )

    if symbol_data.empty:
        raise ValueError(f"details for symbol {symbol} do not exist")

    index = symbol_data.index.get_indexer([key], method="ffill")[0]
    return symbol_data, symbol_data.iloc[index]


class SymbolData:
    """Bar data for a single symbol, loaded on demand.

    Indexing by offset, timestamp or slice returns rows, fetching missing data.
    Other attributes, except those starting with loc, iloc or apply, are views
    of the column of that name, with the same indexing.

    Attributes
    ----------
    data_api: DataAPI
        The data source used to fetch missing data.
    symbol: str
        The symbol.
    scale: TimeScale
        The time scale of the data.
    concurrency: int
        Nonzero to fetch in parallel.
    columns: Dict[str, self._Column]
        The column views created so far, by name.
    symbol_data
        The data frame of the rows loaded so far.
    """

    class _Column:
        """View of a single column of a SymbolData instance.

        Indexing works as for SymbolData but yields values of this column only.
        The view can also be called to get the loaded column as a series.

        Attributes
        ----------
        name: str
            The column name.
        data_api: DataAPI
            The data source used to fetch missing data.
        data: object
            The parent SymbolData instance.
        scale: TimeScale
            The time scale of the data.
        concurrency: int
            Nonzero to fetch in parallel.
        """

        def __init__(
            self,
            data_api: DataAPI,
            name: str,
            data: object,
            scale: TimeScale,
            concurrency: int,
        ):
            """Initialize the column view.

            Parameters
            ----------
            data_api: DataAPI
                The data source used to fetch missing data.
            name: str
                The column name.
            data: object
                The parent SymbolData instance.
            scale: TimeScale
                The time scale of the data.
            concurrency: int
                Nonzero to fetch in parallel.
            """
            self.name = name
            self.data_api = data_api
            self.data = data
            self.scale = scale
            self.concurrency = concurrency

        def __repr__(self):
            """Return the string representation of the column's loaded data."""
            return str(self.data.symbol_data[self.name])

        def _get_index(self, index: datetime, method: str = "ffill") -> int:
            """Return the position of a timestamp in the parent's data.

            If index is not found, fetch data around it into the parent and
            return the nearest position.

            Parameters
            ----------
            index: datetime
                The timestamp to locate.
            method: str, default "ffill"
                The pandas lookup method.
            """
            try:
                return self.data.symbol_data.index.get_loc(
                    index, method=method
                )
            except KeyError:
                self.data.symbol_data = fetch_data_datetime(
                    data_api=self.data_api,
                    symbol_data=self.data.symbol_data,
                    symbol=self.data.symbol,
                    scale=self.scale,
                    d=index,
                    concurrency=self.concurrency,
                )
                return self.data.symbol_data.index.get_loc(
                    index, method="nearest"
                )

        def __getitem__(self, key):
            """Return the column values selected by a key or slice.

            Store any fetched data in the parent. Log exceptions when debugging
            is enabled before re-raising them.

            Parameters
            ----------
            key
                A slice, or an integer offset or timestamp for a single value.
            """
            try:
                if type(key) == slice:
                    self.data.symbol_data, rc = getitem_slice(
                        data_api=self.data_api,
                        symbol=self.data.symbol,
                        symbol_data=self.data.symbol_data,
                        scale=self.scale,
                        key=key,
                        concurrency=self.concurrency,
                    )
                    return rc[self.name]

                self.data.symbol_data, rc = getitem(
                    data_api=self.data_api,
                    symbol=self.data.symbol,
                    symbol_data=self.data.symbol_data,
                    scale=self.scale,
                    key=key,
                    concurrency=self.concurrency,
                )
                return rc[self.name]
            except Exception:
                if config.debug_enabled:
                    tlog_exception("__getitem__")
                raise

        def __getattr__(self, attr):
            """Return the named attribute of the loaded column series.

            Parameters
            ----------
            attr
                The attribute name.
            """
            return self.data.symbol_data[self.name].__getattr__(attr)

        def __call__(self):
            """Return the column's loaded data as a pandas series."""
            return self.data.symbol_data[self.name]

    def __init__(
        self,
        data_api: DataAPI,
        symbol: str,
        scale: TimeScale,
        concurrency: int,
        prefetched_data: Optional[pd.DataFrame] = None,
    ):
        """Initialize the container.

        Parameters
        ----------
        data_api: DataAPI
            The data source used to fetch missing data.
        symbol: str
            The symbol.
        scale: TimeScale
            The time scale of the data.
        concurrency: int
            Nonzero to fetch in parallel.
        prefetched_data: Optional[pd.DataFrame], default None
            The initial data, or None to start with no rows.
        """
        self.data_api = data_api
        self.symbol = symbol
        self.scale = scale
        self.concurrency = concurrency
        self.columns: Dict[str, self._Column] = {}  # type: ignore

        self.symbol_data = (
            prefetched_data
            if prefetched_data is not None
            else pd.DataFrame(
                columns=[
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "vwap",
                    "average",
                    "count",
                ]
            )
        )

    #    def __setattr__(self, name, value):
    #        return self.symbol_data.__setattr__(name, value)

    def __getattr__(self, attr) -> _Column:
        """Return a view of the column named attr.

        Names starting with loc, iloc or apply resolve on the loaded data frame
        instead. Each column view is created once and reused.

        Parameters
        ----------
        attr
            The column name.
        """
        if attr[:3] == "loc" or attr[:4] == "iloc" or attr[:5] == "apply":
            return self.symbol_data.__getattr__(attr)
        elif attr not in self.columns:
            self.columns[attr] = self._Column(
                self.data_api, attr, self, self.scale, self.concurrency
            )
        return self.columns[attr]

    def _get_index(self, index: datetime, method: str = "ffill") -> int:
        """Return the position of a timestamp in the loaded data.

        If index is not found, fetch data around it and attempt a nearest-match
        lookup. Log a ValueError from the lookup before re-raising it.

        Parameters
        ----------
        index: datetime
            The timestamp to locate.
        method: str, default "ffill"
            The pandas lookup method.
        """
        try:
            return self.symbol_data.index.get_loc(index, method=method)
        except ValueError:
            tlog(f"[EXCEPTION] ValueError {index},{self.symbol_data.index}")
            raise
        except KeyError:
            self.symbol_data = fetch_data_datetime(
                data_api=self.data_api,
                symbol_data=self.symbol_data,
                symbol=self.symbol,
                scale=self.scale,
                d=index,
                concurrency=self.concurrency,
            )
            return self.data.symbol_data.index.get_loc(index, method="nearest")

    def __getitem__(self, key):
        """Return the rows selected by a key or slice.

        Store any fetched data in symbol_data. Log exceptions when debugging is
        enabled before re-raising them.

        Parameters
        ----------
        key
            A slice for a data frame of rows, or an integer offset or timestamp
            for a single row.
        """
        try:
            if type(key) == slice:
                self.symbol_data, rc = getitem_slice(
                    data_api=self.data_api,
                    symbol=self.symbol,
                    symbol_data=self.symbol_data,
                    scale=self.scale,
                    key=key,
                    concurrency=self.concurrency,
                )
                return rc

            self.symbol_data, rc = getitem(
                data_api=self.data_api,
                symbol=self.symbol,
                symbol_data=self.symbol_data,
                scale=self.scale,
                key=key,
                concurrency=self.concurrency,
            )
            return rc
        except Exception:
            if config.debug_enabled:
                tlog_exception("__getitem__")
            raise

    def __repr__(self):
        """Return the string representation of the loaded data frame."""
        return str(self.symbol_data)


class DataLoader:
    """Collection of per-symbol market data, loaded on demand.

    Indexing the loader by symbol, or reading the symbol as an attribute,
    returns its SymbolData container, which is created on first access.

    Attributes
    ----------
    data_api
        The data source used to fetch data.
    data: Dict[str, SymbolData]
        The SymbolData containers, by symbol.
    scale: TimeScale
        The time scale of the data.
    concurrency: Optional[int]
        Nonzero to fetch in parallel.

    Methods
    -------
    keys
        Return the symbols that have data containers.
    pre_fetch
        Load data for several symbols at once.
    exist
        Return whether a symbol has a data container.
    """

    def __init__(
        self,
        scale: TimeScale = TimeScale.minute,
        connector: DataConnectorType = config.data_connector,
        concurrency: Optional[int] = 0,
    ):
        """Initialize the loader and its data source.

        Parameters
        ----------
        scale: TimeScale, default TimeScale.minute
            The time scale of the data.
        connector: DataConnectorType, default config.data_connector
            The data provider, by default the one configured at import time.
        concurrency: Optional[int], default 0
            Nonzero to fetch in parallel.

        Raises
        ------
        Raise Exception if connector is not supported, and AssertionError if no
        data source is created.
        """
        self.data_api = data_loader_factory(connector)
        self.data: Dict[str, SymbolData] = {}
        self.scale = scale
        self.concurrency = concurrency
        if not self.data_api:
            raise AssertionError("Failed to create data loader")

    def keys(self) -> List[str]:
        """Return the symbols that currently have data containers."""
        return list(self.data.keys())

    def pre_fetch(self, symbols: List[str], start: date, end: date):
        """Load data for several symbols at once and store it in the loader.

        Replace the containers of the returned symbols with ones that hold the
        new data.

        Parameters
        ----------
        symbols: List[str]
            The symbols to load.
        start: date
            The start of the date range.
        end: date
            The end of the date range.
        """
        data = self.data_api.get_symbols_data(
            symbols=symbols, start=start, end=end, scale=self.scale
        )
        for symbol, df in data.items():
            self.data[symbol] = SymbolData(
                self.data_api, symbol, self.scale, self.concurrency, df
            )

    def exist(self, symbol: str) -> bool:
        """Return whether a data container exists for a symbol.

        Parameters
        ----------
        symbol: str
            The symbol to check.
        """
        return symbol in self.data

    def __len__(self) -> int:
        """Return the number of symbols held by the loader."""
        return len(self.data.keys())

    def __getattr__(self, attr):
        """Return the data container for the symbol named attr.

        Behave like item access, so an unknown name creates a new container.

        Parameters
        ----------
        attr
            The symbol.
        """
        return self.__getitem__(attr)

    def __getitem__(self, symbol: str) -> SymbolData:
        """Return the data container for a symbol, creating it if needed.

        Parameters
        ----------
        symbol: str
            The symbol.

        Raises
        ------
        Raise AssertionError if the loader has no data source.
        """
        if not self.data_api:
            raise AssertionError("Must call a well constructed object")

        if symbol not in self.data:
            self.data[symbol] = SymbolData(
                self.data_api, symbol, self.scale, self.concurrency
            )

        return self.data[symbol]
