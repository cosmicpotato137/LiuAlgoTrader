"""Load trade records and compute trading returns for analysis.

Functions
---------
load_trades_for_period
    Load an environment's unexpired trades in a period.
portfolio_return
    Compute daily revenue, investment and return per strategy.
load_trades
    Load the unexpired trades in a date range, with run details.
load_trades_by_batch_id
    Load the unexpired, non-zero-price trades of a batch.
load_trades_by_portfolio
    Load the distinct trades of a portfolio's batches.
load_runs
    Load the algorithm runs that started within a date range.
load_batch_list
    Load the batches that recorded unexpired trades on a day.
load_traded_symbols
    Load the distinct symbols traded within a batch.
calc_batch_revenue
    Return the net cash flow of a symbol's trades.
calc_revenue
    Return the net cash flow of a symbol's trades in an environment.
count_trades
    Return the number of trades in a batch for a symbol.
trades_analysis
    Summarize the revenue and trade count of each symbol.
symbol_trade_analytics
    Compute per-trade cash flows and plot the trades.
calc_symbol_trades_returns
    Add a symbol's daily position value to equity.
calc_symbol_state
    Return a symbol's net position and latest closing price.
calc_batch_returns
    Compute the daily equity, cash and total value of a batch.
compare_to_symbol_returns
    Return a symbol's prices over a portfolio's period.
get_cash
    Return the daily cash balance of an account.
calc_portfolio_returns
    Compute a portfolio's daily equity, cash and value.
calc_hyperparameters_analysis
    Combine the returns of a session's portfolios.
get_portfolio_equity
    Return a portfolio's open positions at the latest price.
get_portfolio_cash
    Return the account transactions of a portfolio.
"""

import copy
from datetime import date, timedelta
from typing import Dict, Tuple

import pandas as pd
from pytz import timezone
from tqdm import tqdm

from liualgotrader.common import config
from liualgotrader.common.data_loader import DataLoader  # type: ignore
from liualgotrader.common.database import fetch_as_dataframe
from liualgotrader.common.tlog import tlog
from liualgotrader.common.types import AssetType
from liualgotrader.models.accounts import Accounts
from liualgotrader.models.optimizer import OptimizerRun
from liualgotrader.models.portfolio import Portfolio
from liualgotrader.trading.trader_factory import trader_factory

est = timezone("America/New_York")


async def load_trades_for_period(
    env: str, from_date: date, to_date: date
) -> pd.DataFrame:
    """Load the unexpired trades of an environment within a date range.

    Parameters
    ----------
    env: str
        The environment of the algorithm runs to include.
    from_date: date
        The first date to include.
    to_date: date
        The end date, exclusive.

    Returns
    -------
    Return a DataFrame with the client time, symbol, operation, quantity, price
    and algorithm name of each trade, ordered by symbol and time.
    """
    query = f"""
    SELECT client_time, symbol, operation, qty, price, algo_name
    FROM 
        new_trades as t, algo_run as a
    WHERE 
        t.algo_run_id = a.algo_run_id AND 
        t.tstamp >= '{from_date}' AND 
        t.tstamp < '{to_date}' AND
        t.expire_tstamp is null AND
        a.algo_env = '{env}'
    ORDER BY symbol, tstamp
    """
    return await fetch_as_dataframe(query)


async def portfolio_return(
    env: str, start_date: date
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute the daily revenue, investment and return of each strategy.

    Cover the trades from start_date through today.

    Parameters
    ----------
    env: str
        The environment whose trades are included.
    start_date: date
        The first date to include.

    Returns
    -------
    Return a tuple of three DataFrames indexed by date, holding revenue,
    invested amount and return percentage, each with one column per strategy
    plus a daily total.
    """
    df = await load_trades_for_period(
        env, start_date, date.today() + timedelta(days=1)
    )

    # day X strategy = total return
    table: Dict = {}
    invested_table: Dict = {}
    percentages_table: Dict = {}
    for _, row in df.iterrows():
        d = pd.Timestamp(pd.to_datetime(row["client_time"]).date(), tz=est)
        if d not in table:
            table[d] = {}
            invested_table[d] = {}
            percentages_table[d] = {}
            table[d]["revenue"] = 0.0
            invested_table[d]["invested"] = 0.0

        strat = row["algo_name"]
        if strat not in table[d]:
            table[d][strat] = 0.0
            invested_table[d][strat] = 0.0
            percentages_table[d][strat] = 0.0
        delta = (
            (1.0 if row["operation"] == "sell" and row["qty"] > 0 else -1.0)
            * float(row["price"])
            * row["qty"]
        )
        table[d][strat] += delta
        table[d]["revenue"] += delta
        if row["operation"] == "buy":
            invested_table[d][strat] += row["qty"] * float(row["price"])
            invested_table[d]["invested"] += row["qty"] * float(row["price"])
        percentages_table[d][strat] = (
            (100.0 * table[d][strat] / invested_table[d][strat])
            if invested_table[d][strat] != 0
            else 0.0
        )
        percentages_table[d]["revenue"] = (
            (100.0 * table[d]["revenue"] / invested_table[d]["invested"])
            if invested_table[d][strat] != 0
            else 0.0
        )

    return (
        pd.DataFrame.from_dict(table, orient="index").sort_index(),
        pd.DataFrame.from_dict(invested_table, orient="index").sort_index(),
        pd.DataFrame.from_dict(percentages_table, orient="index").sort_index(),
    )


async def load_trades(day: date, end_date: date = None) -> pd.DataFrame:
    """Load the unexpired trades within a date range, with run details.

    Parameters
    ----------
    day: date
        The first date to include.
    end_date: date, default None
        The end date, exclusive, or None to load a single day.

    Returns
    -------
    Return a DataFrame of every trade column plus the batch identifier and
    algorithm name, ordered by symbol and time.
    """
    query = f"""\x1f    SELECT t.*, a.batch_id, a.algo_name\x1f    FROM \x1f    new_trades as t, algo_run as a\x1f    WHERE \x1f        t.algo_run_id = a.algo_run_id AND \x1f        t.tstamp >= '{day}' AND \x1f        t.tstamp < '{end_date or day + timedelta(days=1)}' AND\x1f        t.expire_tstamp is null \x1f    ORDER BY symbol, tstamp\x1f    """
    return await fetch_as_dataframe(query)


async def load_trades_by_batch_id(batch_id: str) -> pd.DataFrame:
    """Load the unexpired, non-zero-price trades of a batch.

    Log an error if the conversion fails.

    Parameters
    ----------
    batch_id: str
        The batch identifier.

    Returns
    -------
    Return a DataFrame of every trade column plus the batch identifier, start
    time and algorithm name, ordered by symbol and time, with client_time
    converted to datetime.
    """
    query = f"""
        SELECT 
            t.*, a.batch_id, a.start_time, a.algo_name
        FROM 
            new_trades as t, algo_run as a
        WHERE 
            t.algo_run_id = a.algo_run_id AND 
            a.batch_id = '{batch_id}' AND
            t.expire_tstamp is null AND 
            price != 0.0 
        ORDER BY symbol, tstamp
    """
    df: pd.DataFrame = await fetch_as_dataframe(query)
    try:
        if not df.empty:
            df["client_time"] = pd.to_datetime(df["client_time"])
    except Exception:
        tlog(
            f"[Error] load_trades_by_batch_id({batch_id}) can't convert 'client_time' column to datetime"
        )
    return df


async def load_trades_by_portfolio(portfolio_id: str) -> pd.DataFrame:
    """Load the distinct trades of every batch linked to a portfolio.

    Include only unexpired trades with a non-zero price and a target price. Log
    an error if the conversion fails.

    Parameters
    ----------
    portfolio_id: str
        The portfolio identifier.

    Returns
    -------
    Return a DataFrame with run details, ordered by symbol and time, with
    client_time converted to datetime.
    """
    query = f"""
        SELECT 
            distinct t.trade_id, t.algo_run_id, symbol, operation, qty, price, indicators, client_time, tstamp, stop_price, target_price, trade_fee, expire_tstamp, a.batch_id, a.start_time, a.algo_name
        FROM 
            new_trades as t, algo_run as a, portfolio_batch_ids as p
        WHERE 
            t.algo_run_id = a.algo_run_id AND 
            a.batch_id = p.batch_id AND
            p.portfolio_id = '{portfolio_id}' AND
            price != 0.0 AND 
            target_price is not null AND
            t.expire_tstamp is null 
        ORDER BY symbol, tstamp
    """
    df: pd.DataFrame = await fetch_as_dataframe(query)
    try:
        if not df.empty:
            df["client_time"] = pd.to_datetime(df["client_time"])
    except Exception:
        tlog(
            f"[Error] load_trades_by_portfolio({portfolio_id}) can't convert 'client_time' column to datetime"
        )
    return df


async def load_runs(day: date, end_date: date = None) -> pd.DataFrame:
    """Load the algorithm runs that started within a date range.

    Parameters
    ----------
    day: date
        The first date to include.
    end_date: date, default None
        The end date, exclusive, or None to load a single day.

    Returns
    -------
    Return a DataFrame indexed by algo_run_id and ordered by start time.
    """
    query = f"""\x1f     SELECT * \x1f     FROM \x1f     algo_run as t\x1f     WHERE \x1f         start_time >= '{day}' AND \x1f         start_time < '{end_date or day + timedelta(days=1)}'\x1f     ORDER BY start_time\x1f     """

    df = await fetch_as_dataframe(query)
    df.set_index("algo_run_id", inplace=True)
    return df


async def load_batch_list(day: date, env: str) -> pd.DataFrame:
    """Load the batches that recorded unexpired trades on a given day.

    Parameters
    ----------
    day: date
        The date to inspect.
    env: str
        The environment name, currently ignored.

    Returns
    -------
    Return a DataFrame of each distinct batch identifier and its start time.
    """
    query = f"""
        SELECT DISTINCT a.batch_id, a.start_time
        FROM 
        new_trades as t, algo_run as a
        WHERE 
            t.algo_run_id = a.algo_run_id AND 
            t.tstamp >= '{day}' AND 
            t.tstamp < '{day + timedelta(days=1)}'AND
            t.expire_tstamp is null 
        """
    return await fetch_as_dataframe(query)


async def load_traded_symbols(batch_id: str) -> pd.DataFrame:
    """Load the distinct symbols traded within a batch.

    Expired trades are included.

    Parameters
    ----------
    batch_id: str
        The batch identifier.

    Returns
    -------
    Return a DataFrame with a single symbol column.
    """
    query = f"""
        SELECT 
            DISTINCT t.symbol
        FROM 
            new_trades as t, algo_run as a
        WHERE 
            t.algo_run_id = a.algo_run_id AND 
            a.batch_id = '{batch_id}'
    """
    return await fetch_as_dataframe(query)


def calc_batch_revenue(
    symbol: str, trades: pd.DataFrame, batch_id: str = None
) -> float:
    """Return the net cash flow of a symbol's trades, rounded to cents.

    Sells of a positive quantity count as inflows and other trades as outflows.

    Parameters
    ----------
    symbol: str
        The symbol to total.
    trades: pd.DataFrame
        The trades DataFrame.
    batch_id: str, default None
        The batch to restrict to, or None for all batches.
    """
    symbol_df = trades[
        (trades["symbol"] == symbol)
        & (not batch_id or trades["batch_id"] == batch_id)
    ]
    rc = sum(
        (
            row["price"]
            * row["qty"]
            * (1 if row["operation"] == "sell" and row["qty"] > 0 else -1)
        )
        for index, row in symbol_df.iterrows()
    )
    return round(rc, 2)


def calc_revenue(symbol: str, trades: pd.DataFrame, env) -> float:
    """Return the net cash flow of a symbol's trades in an environment.

    Round the result to cents. Sells of a positive quantity count as inflows
    and other trades as outflows.

    Parameters
    ----------
    symbol: str
        The symbol to total.
    trades: pd.DataFrame
        The trades DataFrame, including an algo_env column.
    env
        The environment to restrict to.
    """
    symbol_df = trades[
        (trades["symbol"] == symbol) & (trades["algo_env"] == env)
    ]
    rc = sum(
        (
            row["price"]
            * row["qty"]
            * (1 if row["operation"] == "sell" and row["qty"] > 0 else -1)
        )
        for index, row in symbol_df.iterrows()
    )
    return round(rc, 2)


def count_trades(symbol, trades: pd.DataFrame, batch_id: str) -> int:
    """Return the number of trades in a batch for the given symbol.

    Parameters
    ----------
    symbol
        The symbol to count.
    trades: pd.DataFrame
        The trades DataFrame.
    batch_id: str
        The batch to count within.
    """
    symbol_df = trades[
        (trades["symbol"] == symbol) & (trades["batch_id"] == batch_id)
    ]
    return len(symbol_df.index)


def trades_analysis(trades: pd.DataFrame, batch_id: str) -> pd.DataFrame:
    """Summarize the revenue and trade count of each symbol in a batch.

    Convert the client_time column of trades to UTC datetime in place.

    Parameters
    ----------
    trades: pd.DataFrame
        The trades DataFrame.
    batch_id: str
        The batch to summarize.

    Returns
    -------
    Return a DataFrame with symbol, revenues and count columns.
    """
    min(trades["client_time"].tolist())
    trades_analytics = pd.DataFrame()
    trades["client_time"] = pd.to_datetime(trades["client_time"], utc=True)
    trades_analytics["symbol"] = trades.symbol.unique()
    trades_analytics["revenues"] = trades_analytics["symbol"].apply(
        lambda x: calc_batch_revenue(x, trades, batch_id)
    )
    trades_analytics["count"] = trades_analytics["symbol"].apply(
        lambda x: count_trades(x, trades, batch_id)
    )

    return trades_analytics


def symbol_trade_analytics(
    symbol_df: pd.DataFrame, plt
) -> Tuple[pd.DataFrame, float]:
    """Compute per-trade cash flows for a symbol and plot its trades.

    Plot buys in green and sells in red.

    Parameters
    ----------
    symbol_df: pd.DataFrame
        The symbol's trades.
    plt
        The plotting object that receives one scatter point per trade.

    Returns
    -------
    Return a tuple of a copy of the trades, without identifier and timestamp
    columns and with trade and balance columns added, and the total cash flow.
    """
    d = copy.deepcopy(symbol_df).drop(
        columns=[
            "trade_id",
            "algo_run_id",
            "symbol",
            "tstamp",
            "expire_tstamp",
            "batch_id",
            "start_time",
        ]
    )

    d["trade"] = symbol_df.apply(
        lambda row: row.price
        * row.qty
        * (1 if row.operation == "sell" and row.qty > 0 else -1),
        axis=1,
    )
    d["balance"] = d.trade.cumsum()

    for _, row in symbol_df.iterrows():
        plt.scatter(
            row["client_time"],
            row["price"],
            c="g" if row["operation"] == "buy" else "r",
            s=100,
        )

    return d, d.trade.sum()


def calc_symbol_trades_returns(
    symbol: str,
    symbol_trades: pd.DataFrame,
    daily_returns: pd.DataFrame,
    data_loader: DataLoader,
):
    """Add the daily market value of a symbol's position to an equity series.

    Value the position at the 9:30 a.m. US Eastern close of each day it is
    held. Log and skip valuation errors, except a ValueError raised for a
    position still open after the last trade, which propagates.

    Parameters
    ----------
    symbol: str
        The symbol whose position is valued.
    symbol_trades: pd.DataFrame
        The symbol's trades in time order; its qty column is converted to float
        in place.
    daily_returns: pd.DataFrame
        The DataFrame indexed by day whose equity column is updated in place.
    data_loader: DataLoader
        The loader that supplies closing prices.
    """
    t1 = t2 = None
    qty: float = 0.0
    eastern = timezone(
        "US/Eastern",
    )

    symbol_trades["qty"] = symbol_trades["qty"].astype(float)
    for _, row in symbol_trades.iterrows():
        if t1 is None:
            t1 = daily_returns.index.get_loc(str(row.client_time.date()))
        else:
            t2 = daily_returns.index.get_loc(str(row.client_time.date()))

        if t1 is not None and t2 is not None:
            for i in range(t1, t2):
                try:
                    value = (
                        qty
                        * data_loader[symbol].close[
                            eastern.localize(
                                daily_returns.index[i].to_pydatetime()
                            ).replace(
                                hour=9, minute=30, second=0, microsecond=0
                            )
                        ]
                    )
                    daily_returns.loc[
                        daily_returns.index[i],
                        "equity",
                    ] += value
                except Exception as e:
                    tlog(f"Error during calc_symbol_trades_returns(): {e}")

        if row.operation == "buy":
            qty += row.qty
        elif row.operation == "sell":
            qty -= row.qty

        if qty == 0:
            t1 = t2 = None
        elif t2 is not None:
            t1 = t2
            t2 = None

    if qty and t1 is not None:
        for i in range(t1, len(daily_returns.index)):
            try:
                value = (
                    qty
                    * data_loader[symbol].close[
                        eastern.localize(
                            daily_returns.index[i].to_pydatetime()
                        ).replace(hour=9, minute=30, second=0, microsecond=0)
                    ]
                )
            except ValueError:
                print("qty:", qty, t1)
                raise
            daily_returns.loc[daily_returns.index[i], "equity"] += value


def calc_symbol_state(
    symbol: str,
    symbol_trades: pd.DataFrame,
    data_loader: DataLoader,
) -> Tuple[float, float]:
    """Return a symbol's net position and its most recent closing price.

    Parameters
    ----------
    symbol: str
        The symbol to evaluate.
    symbol_trades: pd.DataFrame
        The symbol's trades.
    data_loader: DataLoader
        The loader that supplies closing prices.
    """
    qty: float = 0
    for _, row in symbol_trades.iterrows():
        if row.operation == "buy":
            qty += row.qty
        elif row.operation == "sell":
            qty -= row.qty

    return qty, data_loader[symbol].close[-1]


async def calc_batch_returns(batch_id: str) -> pd.DataFrame:
    """Compute the daily equity, cash and total value of a batch.

    Print each symbol with a progress count.

    Parameters
    ----------
    batch_id: str
        The batch identifier.

    Returns
    -------
    Return a DataFrame indexed by trading day with equity, cash and totals
    columns, where cash is the portfolio size, or an empty DataFrame if the
    batch has no trades.
    """
    portfolio = await Portfolio.load_by_batch_id(batch_id)
    data_loader = DataLoader()
    trades = await load_trades_by_batch_id(batch_id)

    if trades.empty:
        return pd.DataFrame()
    start_date = trades.client_time.min().date()
    end_date = trades.client_time.max().date()
    trader = trader_factory()

    td = trader.get_trading_days(start_date=start_date, end_date=end_date)

    td["equity"] = 0.0
    td["cash"] = portfolio.portfolio_size

    num_symbols = len(trades.symbol.unique().tolist())
    for c, symbol in enumerate(trades.symbol.unique().tolist(), start=1):
        print(f"{symbol} ({c}/{num_symbols})")
        symbol_trades = trades[trades.symbol == symbol].sort_values(
            by="client_time"
        )
        calc_symbol_trades_returns(symbol, symbol_trades, td, data_loader)

    td["totals"] = td["equity"] + td["cash"]
    # td["date"] = pd.to_datetime(td.index)
    # td = td.set_index("date")
    return pd.DataFrame(td, columns=["equity", "cash", "totals"])


async def compare_to_symbol_returns(
    portfolio_id: str, symbol: str
) -> pd.DataFrame:
    """Return a symbol's daily prices over a portfolio's trading period.

    Parameters
    ----------
    portfolio_id: str
        The portfolio whose first and last trades set the period.
    symbol: str
        The symbol to price.

    Returns
    -------
    Return a Series of closing prices named after the symbol and indexed by
    trading day.
    """
    data_loader = DataLoader()
    trades = await load_trades_by_portfolio(portfolio_id)
    start_date = trades.client_time.min().date()
    end_date = trades.client_time.max().date()
    trader = trader_factory()

    td = trader.get_trading_days(start_date=start_date, end_date=end_date)
    td[symbol] = td.apply(
        lambda row: data_loader[symbol].close[
            row.name.to_pydatetime().replace(tzinfo=est)
        ],
        axis=1,
    )
    return td[symbol]


async def get_cash(account_id: int, initial_cash: float) -> pd.DataFrame:
    """Return the daily cash balance of an account.

    Parameters
    ----------
    account_id: int
        The account identifier.
    initial_cash: float
        The balance before the first transaction.

    Returns
    -------
    Return a DataFrame indexed by date with amount and cash columns, holding
    one row per date that has transactions.
    """
    df = await Accounts.get_transactions(account_id)
    df = df.groupby(df.index.date).sum()
    df.iloc[0].amount += initial_cash
    r = pd.date_range(start=df.index.min(), end=df.index.max(), name="date")
    df.reindex(r).fillna(method="ffill")
    df["cash"] = df.amount.cumsum()
    df.index.rename("date", inplace=True)
    return df


async def calc_portfolio_returns(portfolio_id: str) -> pd.DataFrame:
    """Compute the daily equity, cash and total value of a portfolio.

    Show a progress bar.

    Parameters
    ----------
    portfolio_id: str
        The portfolio identifier.

    Returns
    -------
    Return a DataFrame with equity, cash and totals columns over trading days
    for US equities and calendar days otherwise, or an empty DataFrame if the
    portfolio has no trades.
    """
    portfolio = await Portfolio.load_by_portfolio_id(portfolio_id)

    data_loader = DataLoader()
    trades = await load_trades_by_portfolio(portfolio_id)

    if trades.empty:
        return pd.DataFrame()
    start_date = trades.client_time.min().date()
    end_date = trades.client_time.max().date()
    trader = trader_factory()

    if portfolio.asset_type == AssetType.US_EQUITIES:
        td = trader.get_trading_days(start_date=start_date, end_date=end_date)
    else:
        td = pd.DataFrame(index=pd.date_range(start_date, end_date))

    td["equity"] = 0.0

    cash_df = await get_cash(portfolio.account_id, portfolio.portfolio_size)

    symbols = trades.symbol.unique().tolist()
    for i in tqdm(
        range(len(symbols)), desc=f"loading portfolio {portfolio_id}"
    ):
        symbol = symbols[i]
        symbol_trades = trades[trades.symbol == symbol].sort_values(
            by="client_time"
        )
        calc_symbol_trades_returns(symbol, symbol_trades, td, data_loader)
    td = td.join(cash_df)
    td = td.fillna(method="ffill")
    td["totals"] = td["equity"] + td["cash"]
    return pd.DataFrame(td, columns=["equity", "cash", "totals"])


async def calc_hyperparameters_analysis(optimizer_run_id: str) -> pd.DataFrame:
    """Combine the daily returns of every portfolio in an optimizer session.

    Show a progress bar.

    Parameters
    ----------
    optimizer_run_id: str
        The optimizer session identifier.

    Returns
    -------
    Return a DataFrame indexed by portfolio identifier and date, with a
    configurations column, or None if no portfolio has trades.
    """
    portfolio_ids_parameters = await OptimizerRun.get_portfolio_ids_parameters(
        optimizer_run_id
    )

    portfolio_ids, hypers = list(map(list, zip(*portfolio_ids_parameters)))
    df = None
    if len(portfolio_ids):
        for i in tqdm(range(len(portfolio_ids)), desc="Loading Portfolios"):
            _df = await calc_portfolio_returns(portfolio_ids[i])

            if _df.empty:
                continue

            _df["portfolio_id"] = portfolio_ids[i]
            _df["configurations"] = hypers[i]
            _df.reset_index(inplace=True)
            _df = _df.set_index(["portfolio_id", "date"])
            df = pd.concat([df, _df], axis=0) if df is not None else _df

    return df


async def get_portfolio_equity(portfolio_id: str) -> pd.DataFrame:
    """Return the open positions of a portfolio valued at the latest price.

    Log and skip symbols that cannot be valued. Show a progress bar.

    Parameters
    ----------
    portfolio_id: str
        The portfolio identifier.

    Returns
    -------
    Return a DataFrame with symbol, qty, price and total columns for positive
    positions, rounded to cents.
    """
    await Portfolio.load_by_portfolio_id(portfolio_id)
    data_loader = DataLoader()
    trades = await load_trades_by_portfolio(portfolio_id)

    symbols = trades.symbol.unique().tolist()
    rows = []
    for i in tqdm(
        range(len(symbols)), desc=f"loading portfolio {portfolio_id}"
    ):
        symbol = symbols[i]
        symbol_trades = trades[trades.symbol == symbol].sort_values(
            by="client_time"
        )
        try:
            qty, last_price = calc_symbol_state(
                symbol, symbol_trades, data_loader
            )
        except Exception as e:
            tlog(str(e))
        else:
            rows.append(
                {
                    "symbol": symbol,
                    "qty": qty,
                    "price": last_price,
                    "total": float(qty) * last_price,
                }
            )

    df = pd.DataFrame(rows)
    return df.loc[df.qty > 0].round(2)


async def get_portfolio_cash(portfolio_id: str) -> pd.DataFrame:
    """Return the account transactions of a portfolio, rounded to cents.

    Parameters
    ----------
    portfolio_id: str
        The portfolio identifier.
    """
    portfolio = await Portfolio.load_by_portfolio_id(portfolio_id)
    return (await Accounts.get_transactions(portfolio.account_id)).round(2)
