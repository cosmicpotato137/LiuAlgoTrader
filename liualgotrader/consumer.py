"""Execute strategies on streaming data received from the producer.

Functions
---------
end_time
    Record the end of every strategy's algorithm run.
get_position
    Return the position a trader reports for a symbol.
execute_run_all_results
    Execute the actions returned by run_all.
do_strategy_all
    Run a strategy on all its symbols and execute the result.
cancel_lingering_orders
    Reconcile or cancel stale open orders.
periodic_runner
    Run the all-symbol strategies every five minutes.
should_cancel_order
    Return whether an order is at least a minute old.
save
    Save a trade record to the database.
do_callbacks
    Clear stored indicators and notify a strategy of a fill.
update_partially_filled_order
    Apply a partial fill to the trading state.
update_filled_order
    Apply a complete fill and close the open order.
handle_trade_update_for_order
    Apply a trade update for an open order.
handle_trade_update_wo_order
    Log a trade update without an open order.
handle_trade_update
    Dispatch a trade update.
handle_quote
    Update the volume order imbalance from a quote.
aggregate_bar_data
    Merge a streamed bar into the minute data.
order_inflight
    Reconcile or cancel a stale open order.
submit_order
    Submit a day order described by a strategy.
update_trading_data
    Register a newly submitted order.
execute_strategy_result
    Submit and record the order of a strategy.
do_strategy
    Run a strategy on one symbol.
do_strategies
    Run the eligible per-symbol strategies on one symbol.
handle_aggregate
    Process an aggregate bar and run the strategies.
handle_data_queue_msg
    Handle a market data message.
queue_consumer
    Consume and dispatch messages from the data queue.
create_strategies_from_file
    Create the strategies in the configuration.
load_symbol_position
    Return the net open positions of a portfolio.
create_strategies_from_db
    Create the strategies in the trade plan.
handle_new_strategy
    Create a strategy requested at run time.
consumer_async_main
    Set up the consumer and run its tasks.
consumer_main
    Run a consumer process.
"""

import asyncio
import math
import os
from datetime import datetime, timedelta
from multiprocessing import Queue
from queue import Empty
from random import randint
from typing import Any, Dict, List, Optional

import pandas as pd
import pygit2
from pandas import DataFrame as df
from pytz import timezone

from liualgotrader.analytics.analysis import load_trades_by_portfolio
from liualgotrader.common import config, market_data, trading_data
from liualgotrader.common.data_loader import DataLoader  # type: ignore
from liualgotrader.common.database import create_db_connection
from liualgotrader.common.tlog import tlog, tlog_exception
from liualgotrader.common.types import Order, Trade
from liualgotrader.fincalcs.data_conditions import QUOTE_SKIP_CONDITIONS
from liualgotrader.models.new_trades import NewTrade
from liualgotrader.models.portfolio import Portfolio
from liualgotrader.models.tradeplan import TradePlan
from liualgotrader.models.trending_tickers import TrendingTickers
from liualgotrader.strategies.base import Strategy, StrategyType
from liualgotrader.trading.base import Trader
from liualgotrader.trading.trader_factory import (get_trader_by_name,
                                                  trader_factory)

shortable: Dict = {}
symbol_data_error: Dict = {}
rejects: Dict[str, List[str]] = {}
time_tick: Dict[str, datetime] = {}
nyc = timezone("America/New_York")


async def end_time(reason: str):
    """Record the end of each loaded strategy's algorithm run in the database.

    Parameters
    ----------
    reason: str
        The end reason to store with each run.
    """
    for s in trading_data.strategies:
        tlog(f"updating end time for strategy {s.name}")
        await s.algo_run.update_end_time(
            pool=config.db_conn_pool, end_reason=reason
        )


def get_position(trader: Trader, symbol: str) -> float:
    """Return the position quantity that trader reports for symbol.

    Parameters
    ----------
    trader: Trader
        The trader to ask.
    symbol: str
        The symbol to look up.

    Returns
    -------
    Return zero if the trader raises an exception.
    """
    try:
        return trader.get_position(symbol)
    except Exception:
        return 0


async def execute_run_all_results(
    strategy: Strategy,
    run_all_results: Dict[str, Dict],
    trader: Trader,
    data_loader: DataLoader,
):
    """Execute the per-symbol actions returned by the run_all of a strategy.

    Place the orders through the broker and on behalf of the external account
    of the strategy's portfolio, when it has one.

    Parameters
    ----------
    strategy: Strategy
        The strategy that produced the actions.
    run_all_results: Dict[str, Dict]
        A mapping of symbol to action details.
    trader: Trader
        The trader to use unless the portfolio names a broker.
    data_loader: DataLoader
        The loader of market data.
    """
    external_account_id = None
    if hasattr(strategy, "portfolio_id"):
        (
            external_account_id,
            broker_name,
        ) = await Portfolio.get_external_account_id(
            strategy.portfolio_id  # type: ignore
        )
        if broker_name:
            trader = get_trader_by_name(broker_name)

    for symbol, what in run_all_results.items():
        await execute_strategy_result(
            strategy,
            trader,
            data_loader,
            symbol.lower(),
            what,
            external_account_id,
        )


async def do_strategy_all(
    data_loader: DataLoader,
    trader: Trader,
    strategy: Strategy,
    symbols: List[str],
    carrier=None,
):
    """Run the run_all method of strategy and execute the resulting actions.

    Parameters
    ----------
    data_loader: DataLoader
        The loader of market data.
    trader: Trader
        The trader used for positions and orders.
    strategy: Strategy
        The strategy to run.
    symbols: List[str]
        The symbols to pass with their current positions.
    carrier, default None
        Unused.

    Raises
    ------
    Log and re-raise any exception.
    """
    try:
        now = datetime.now(nyc)
        symbols_position = {
            symbol.lower(): trading_data.positions[symbol]
            if symbol in trading_data.positions
            else get_position(trader, symbol)
            for symbol in symbols
        }
        do = await strategy.run_all(
            symbols_position=symbols_position,
            now=now,
            portfolio_value=config.portfolio_value,
            backtesting=True,
            data_loader=data_loader,
            trader=trader,
        )
        await execute_run_all_results(strategy, do, trader, data_loader)

    except Exception as e:
        if config.debug_enabled:
            tlog_exception("do_strategy_all")
        tlog(f"[Exception] {now} {strategy}->{e}")

        raise


async def cancel_lingering_orders(trader: Trader):
    """Reconcile or cancel stale open orders once a minute until cancelled.

    Act only while the market is open.

    Parameters
    ----------
    trader: Trader
        The trader used to check and cancel orders.
    """
    tlog("cancel_lingering_orders() task starting")

    while True:
        await asyncio.sleep(60)

        if not len(trading_data.open_orders):
            continue

        ny_now = datetime.now(nyc)
        if not trader.is_market_open(ny_now):
            tlog("cancel_lingering_orders() market is closed")
            continue

        t = list(trading_data.open_orders.items())
        for symbol, order in t:
            await order_inflight(
                symbol=symbol,
                existing_order=order,
                now=ny_now,
                trader=trader,
            )


async def periodic_runner(data_loader: DataLoader, trader: Trader) -> None:
    """Run the all-symbol strategies every five minutes until cancelled.

    Run each strategy whose should_run_all returns True on the symbols it last
    traded. Log exceptions and stop instead of raising them.

    Parameters
    ----------
    data_loader: DataLoader
        The loader of market data.
    trader: Trader
        The trader used for positions and orders.
    """
    try:
        while True:
            tlog("periodic_runner() task starting")
            # run strategies
            tasks = []
            for s in trading_data.strategies:
                try:
                    skip = not await s.should_run_all()
                except Exception:
                    skip = True
                if skip:
                    continue

                symbols = [
                    symbol.lower()
                    for symbol in trading_data.last_used_strategy
                    if trading_data.last_used_strategy[symbol.lower()] == s
                ]

                tasks.append(
                    asyncio.create_task(
                        do_strategy_all(
                            trader=trader,
                            data_loader=data_loader,
                            strategy=s,
                            symbols=symbols,
                        )
                    )
                )

            asyncio.gather(*tasks)
            await asyncio.sleep(5 * 60.0)

    except asyncio.CancelledError:
        tlog("periodic_runner() cancelled")
    except KeyboardInterrupt:
        tlog("periodic_runner() - Caught KeyboardInterrupt")
    except Exception as e:
        if config.debug_enabled:
            tlog_exception("periodic_runner")
        tlog(f"[EXCEPTION] periodic_runner: {e}")

    tlog("periodic_runner() task completed")


async def should_cancel_order(order: Order, market_clock: datetime) -> bool:
    """Return whether order has been outstanding for at least a minute.

    Parameters
    ----------
    order: Order
        The open order.
    market_clock: datetime
        The current market time.
    """
    # Make sure the order's not too old
    submitted_at = order.submitted_at.astimezone(market_clock.tzinfo)
    order_lifetime = market_clock - submitted_at
    return (
        market_clock > submitted_at
        and order_lifetime.total_seconds() // 60 >= 1
    )


async def save(
    symbol: str,
    new_qty: float,
    last_op: str,
    price: float,
    indicators: Dict[Any, Any],
    now: str,
    trade_fee=0.0,
) -> None:
    """Save a trade record for symbol to the database.

    Attach the record to the algorithm run of the strategy that owns the open
    order of symbol, with the recorded stop and target prices.

    Parameters
    ----------
    symbol: str
        The traded symbol.
    new_qty: float
        The traded quantity.
    last_op: str
        The operation name, such as buy or sell.
    price: float
        The trade price.
    indicators: Dict[Any, Any]
        The indicators to store with the trade.
    now: str
        The client time of the trade.
    trade_fee, default 0.0
        The fee charged for the trade.

    Raises
    ------
    Raise KeyError if no such strategy is registered.
    """
    symbol = symbol.lower()
    db_trade = NewTrade(
        algo_run_id=trading_data.open_order_strategy[symbol].algo_run.run_id,
        symbol=symbol,
        qty=new_qty,
        operation=last_op,
        price=price,
        indicators=indicators,
    )

    await db_trade.save(
        config.db_conn_pool,
        now,
        trading_data.stop_prices[symbol]
        if symbol in trading_data.stop_prices
        else 0.0,
        trading_data.target_prices[symbol]
        if symbol in trading_data.target_prices
        else 0.0,
        trade_fee,
    )


async def do_callbacks(
    symbol: str,
    strategy: Strategy,
    filled_qty: float,
    side: Order.FillSide,
    filled_avg_price: float,
):
    """Clear the stored indicators of symbol and notify strategy of a fill.

    Use the buy indicators and buy_callback for a buy fill, and the sell
    counterparts otherwise.

    Parameters
    ----------
    symbol: str
        The filled symbol.
    strategy: Strategy
        The strategy to notify, or None.
    filled_qty: float
        The filled quantity.
    side: Order.FillSide
        The side of the fill.
    filled_avg_price: float
        The average fill price.
    """
    symbol = symbol.lower()
    if side == Order.FillSide.buy:
        trading_data.buy_indicators.pop(symbol, None)
        if strategy:
            await strategy.buy_callback(symbol, filled_avg_price, filled_qty)
    else:
        trading_data.sell_indicators.pop(symbol, None)
        if strategy:
            await strategy.sell_callback(symbol, filled_avg_price, filled_qty)


async def update_partially_filled_order(
    symbol: str,
    strategy: Strategy,
    filled_qty: float,
    side: Order.FillSide,
    filled_avg_price: float,
    updated_at: pd.Timestamp,
    trade_fee: float,
) -> None:
    """Apply a partial fill for symbol to the shared trading state.

    Notify strategy, adjust the recorded position, and save the trade to the
    database with the stored indicators.

    Parameters
    ----------
    symbol: str
        The filled symbol.
    strategy: Strategy
        The strategy that owns the order.
    filled_qty: float
        The filled quantity.
    side: Order.FillSide
        The side of the fill.
    filled_avg_price: float
        The average fill price.
    updated_at: pd.Timestamp
        The time of the fill.
    trade_fee: float
        The fee charged for the fill.
    """
    symbol = symbol.lower()

    await do_callbacks(
        symbol=symbol,
        strategy=strategy,
        filled_qty=filled_qty,
        side=side,
        filled_avg_price=filled_avg_price,
    )

    trading_data.positions[symbol] = round(
        trading_data.positions.get(symbol, 0.0)
        + filled_qty * (1 if side == Order.FillSide.buy else -1),
        8,
    )

    try:
        indicators = {
            "buy": trading_data.buy_indicators.get(symbol, None),
            "sell": trading_data.sell_indicators.get(symbol, None),
        }
    except KeyError:
        indicators = {}

    await save(
        symbol,
        filled_qty,
        side.name,
        filled_avg_price,
        indicators,
        str(updated_at),
        trade_fee,
    )


async def update_filled_order(
    symbol: str,
    strategy: Strategy,
    filled_qty: float,
    side: Order.FillSide,
    filled_avg_price: float,
    updated_at: pd.Timestamp,
    trade_fee: float,
) -> None:
    """Apply a complete fill for symbol and close its open order.

    Notify strategy, adjust the recorded position, save the trade, and remove
    the open order from the trading state.

    Parameters
    ----------
    symbol: str
        The filled symbol.
    strategy: Strategy
        The strategy that owns the order.
    filled_qty: float
        The filled quantity.
    side: Order.FillSide
        The side of the fill.
    filled_avg_price: float
        The average fill price.
    updated_at: pd.Timestamp
        The time of the fill.
    trade_fee: float
        The fee charged for the fill.

    Raises
    ------
    Raise KeyError if symbol has no open order.
    """
    symbol = symbol.lower()

    await update_partially_filled_order(
        symbol=symbol,
        strategy=strategy,
        filled_qty=filled_qty,
        side=side,
        filled_avg_price=filled_avg_price,
        updated_at=updated_at,
        trade_fee=trade_fee,
    )
    trading_data.open_orders.pop(symbol)
    if symbol in trading_data.open_order_strategy:
        trading_data.open_order_strategy.pop(symbol)

    tlog(
        f"update_filled_order open order for {symbol} popped. Position now {trading_data.positions[symbol]}"
    )


async def handle_trade_update_for_order(trade: Trade) -> bool:
    """Apply a trade update for a symbol that has an open order.

    Record partial and complete fills. For any other event, such as a
    cancellation or rejection, discard the open order.

    Parameters
    ----------
    trade: Trade
        The trade update.

    Returns
    -------
    Return True.
    """
    symbol = trade.symbol.lower()
    event = trade.event

    tlog(f"trade update for {symbol} with event {event}")

    if event == Order.EventType.partial_fill:
        await update_partially_filled_order(
            symbol=symbol,
            strategy=trading_data.open_order_strategy[symbol],
            filled_qty=trade.filled_qty,
            filled_avg_price=trade.filled_avg_price,
            side=trade.side,
            updated_at=trade.updated_at,
            trade_fee=trade.trade_fee,
        )

    elif event == Order.EventType.fill:
        await update_filled_order(
            symbol=symbol,
            strategy=trading_data.open_order_strategy[symbol],
            filled_qty=trade.filled_qty,
            filled_avg_price=trade.filled_avg_price,
            side=trade.side,
            updated_at=trade.updated_at,
            trade_fee=trade.trade_fee,
        )

    else:
        trading_data.partial_fills.pop(symbol, None)
        trading_data.partial_fills_fee.pop(symbol, None)
        trading_data.open_orders.pop(symbol, None)
        trading_data.open_order_strategy.pop(symbol, None)

    return True


async def handle_trade_update_wo_order(trade: Trade) -> bool:
    """Log a trade update that has no matching open order.

    Parameters
    ----------
    trade: Trade
        The trade update.

    Returns
    -------
    Return True.
    """
    tlog(f"trade update without order for {trade}")
    return True


async def handle_trade_update(trade: Trade) -> bool:
    """Dispatch trade according to whether its symbol has an open order.

    Parameters
    ----------
    trade: Trade
        The trade update.

    Returns
    -------
    Return True.
    """
    if trade.symbol.lower() in trading_data.open_orders:
        return await handle_trade_update_for_order(trade)
    else:
        return await handle_trade_update_wo_order(trade)


async def handle_quote(data: Dict) -> bool:
    """Update the volume order imbalance of a symbol from a quote message.

    Ignore quotes that lack a bid or ask price or carry a skipped condition.
    Record the latest bid and ask, and keep up to ten recent imbalance values
    for the symbol.

    Parameters
    ----------
    data: Dict
        The quote message.

    Returns
    -------
    Return True.
    """
    if "askprice" not in data or "bidprice" not in data:
        return True
    if "condition" in data and data["condition"] in QUOTE_SKIP_CONDITIONS:
        return True

    symbol = data["symbol"].lower()
    # tlog(f"quote={data}")
    prev_ask = trading_data.voi_ask.get(symbol, None)
    prev_bid = trading_data.voi_bid.get(symbol, None)
    trading_data.voi_ask[symbol] = (
        data["askprice"],
        data["asksize"],
        data["timestamp"],
    )
    trading_data.voi_bid[symbol] = (
        data["bidprice"],
        data["bidsize"],
        data["timestamp"],
    )

    bid_delta_volume = (
        0
        if not prev_bid or data["bidprice"] < prev_bid[0]
        else 100 * data["bidsize"]
        if data["bidprice"] > prev_bid[0]
        else 100 * (data["bidsize"] - prev_bid[1])
    )
    ask_delta_volume = (
        0
        if not prev_ask or data["askprice"] > prev_ask[0]
        else 100 * data["asksize"]
        if data["askprice"] < prev_ask[0]
        else 100 * (data["asksize"] - prev_ask[1])
    )
    voi_stack = trading_data.voi.get(symbol, None)
    if not voi_stack:
        voi_stack = [0.0]
    elif len(voi_stack) == 10:
        voi_stack[0:9] = voi_stack[1:10]
        voi_stack.pop()

    k = 2.0 / (100 + 1)
    voi_stack.append(
        round(
            voi_stack[-1] * (1.0 - k)
            + k * (bid_delta_volume - ask_delta_volume),
            2,
        )
    )
    trading_data.voi[symbol] = voi_stack
    # tlog(f"{symbol} voi:{trading_data.voi[symbol]}")

    return True


async def aggregate_bar_data(
    data_loader: DataLoader, data: Dict, ts: pd.Timestamp, carrier=None
) -> None:
    """Merge a streamed bar into the minute data held by data_loader.

    Let a minute aggregate replace the bar for its minute, and combine other
    messages with the existing bar. Add the bar volume to the daily volume of
    the symbol.

    Parameters
    ----------
    data_loader: DataLoader
        The loader that holds the minute data.
    data: Dict
        The bar message.
    ts: pd.Timestamp
        The bar timestamp; its seconds are ignored.
    carrier, default None
        Unused.
    """
    ts = ts.replace(second=0)
    symbol = data["symbol"].lower()

    if not data_loader.exist(symbol):
        # print(f"loading data for {symbol}...")
        data_loader[symbol][-1]

    try:
        current = data_loader[symbol].loc[ts]
    except KeyError:
        current = None

    if current is None or data["EV"] == "AM":
        new_data = [
            data["open"],
            data["high"],
            data["low"],
            data["close"],
            data["volume"],
            data["vwap"],
            data["average"],
            int(data["count"]),
        ]
    else:
        new_data = [
            current["open"],
            max(data["high"], current["high"]),
            min(data["low"], current["low"]),
            data["close"],
            current["volume"] + data["volume"],
            data["vwap"] or current["vwap"],
            current["average"]
            if math.isnan(data["average"])
            else data["average"],
            int(data["count"] + current["count"]),
        ]

    try:
        data_loader[symbol].loc[ts] = new_data
    except ValueError:
        print(f"loaded for {symbol} {new_data}")
        data_loader[symbol][-1]
        data_loader[symbol].loc[ts] = new_data

    market_data.volume_today[symbol] = (
        market_data.volume_today[symbol] + data["volume"]
        if symbol in market_data.volume_today
        else data["volume"]
    )


async def order_inflight(
    symbol: str,
    existing_order: Order,
    now: pd.Timestamp,
    trader: Trader,
) -> None:
    """Reconcile or cancel an open order that has been pending too long.

    Act only on orders outstanding for at least a minute. Record a fill or
    partial fill reported by the broker, and cancel the order otherwise. Log
    exceptions instead of raising them.

    Parameters
    ----------
    symbol: str
        The symbol of the order.
    existing_order: Order
        The open order.
    now: pd.Timestamp
        The current time.
    trader: Trader
        The trader used to check and cancel the order.
    """
    symbol = symbol.lower()
    try:
        if await should_cancel_order(existing_order, now):
            if config.debug_enabled:
                tlog(
                    f"should_cancel_order - checking status w/ order-id {existing_order.order_id}"
                )
            (
                order_status,
                filled_price,
                filled_qty,
                trade_fee,
            ) = await trader.is_order_completed(
                existing_order.order_id, existing_order.external_account_id
            )

            if order_status == Order.EventType.fill:
                tlog(
                    f"order_id {existing_order.order_id} for {symbol} already filled"
                )
                await update_filled_order(
                    symbol=symbol,
                    strategy=trading_data.open_order_strategy[symbol],  # type: ignore
                    filled_avg_price=filled_price,  # type: ignore
                    filled_qty=filled_qty,  # type: ignore
                    side=existing_order.side,  # type: ignore
                    updated_at=existing_order.submitted_at,  # type: ignore
                    trade_fee=trade_fee,  # type: ignore
                )
            elif order_status == Order.EventType.partial_fill:
                tlog(
                    f"order_id {existing_order.id} for {symbol} already partially_filled"  # type: ignore
                )
                await update_partially_filled_order(
                    symbol=symbol,
                    strategy=trading_data.open_order_strategy[symbol],
                    side=existing_order.side,  # type: ignore
                    filled_avg_price=filled_price,  # type: ignore
                    filled_qty=filled_qty,  # type: ignore
                    updated_at=existing_order.submitted_at,
                    trade_fee=trade_fee,  # type: ignore
                )
            else:
                # Cancel it so we can try again for a fill
                tlog(
                    f"Cancel order id {existing_order.order_id} for {symbol} ts={now} submission_ts={existing_order.submitted_at.astimezone(timezone('America/New_York'))}"  # type: ignore
                )
                if await trader.cancel_order(existing_order):
                    trading_data.open_orders.pop(symbol)

    except AttributeError as e:
        if config.debug_enabled:
            tlog_exception(f"order_inflight() w {e}")
        tlog(f"Attribute Error in symbol {symbol} w/ {existing_order}")
    except Exception as e:
        if config.debug_enabled:
            tlog_exception(f"order_inflight() w {e}")
        tlog(
            f"[EXCEPTION] order_inflight() : {e} for symbol {symbol} w/ {existing_order}"
        )


async def submit_order(
    trader: Trader, symbol: str, what: Dict, external_account_id: str = None
) -> Order:
    """Submit a day order for symbol as described by what.

    Parameters
    ----------
    trader: Trader
        The trader that submits the order.
    symbol: str
        The symbol to trade.
    what: Dict
        The order details: qty, side, type and, for a limit order, limit_price.
    external_account_id: str, default None
        The account to trade on behalf of, or None.

    Returns
    -------
    Return the order returned by trader.
    """
    return (
        await trader.submit_order(
            symbol=symbol,
            qty=what["qty"],
            side=what["side"],
            order_type="limit",
            time_in_force="day",
            limit_price=what["limit_price"],
            on_behalf_of=external_account_id,
        )
        if what["type"] == "limit"
        else await trader.submit_order(
            symbol=symbol,
            qty=what["qty"],
            side=what["side"],
            order_type=what["type"],
            time_in_force="day",
            on_behalf_of=external_account_id,
        )
    )


async def update_trading_data(
    symbol: str, o: Order, strategy: Strategy, buy: bool
) -> None:
    """Register a newly submitted order and its strategy for symbol.

    Parameters
    ----------
    symbol: str
        The symbol of the order.
    o: Order
        The submitted order.
    strategy: Strategy
        The strategy that requested the order.
    buy: bool
        True for a buy order, which also records the buy time.
    """
    trading_data.open_orders[symbol] = o
    trading_data.open_order_strategy[symbol] = strategy
    trading_data.last_used_strategy[symbol] = strategy
    if buy:
        trading_data.buy_time[symbol] = datetime.now(
            tz=timezone("America/New_York")
        ).replace(second=0, microsecond=0)


async def execute_strategy_result(
    strategy: Strategy,
    trader: Trader,
    data_loader: DataLoader,
    symbol: str,
    what: Dict,
    external_account_id: str = None,
) -> bool:
    """Submit the order requested by a strategy and record it.

    Register the order in the trading state and save a placeholder trade to the
    database.

    Parameters
    ----------
    strategy: Strategy
        The strategy that requested the order.
    trader: Trader
        The trader that submits the order.
    data_loader: DataLoader
        The loader of market data, used for debug logging.
    symbol: str
        The symbol to trade.
    what: Dict
        The order details.
    external_account_id: str, default None
        The account to trade on behalf of, or None.

    Returns
    -------
    Return True if an order was submitted and False otherwise.
    """
    tlog(f"execute_strategy_result for {symbol} do {what}")
    symbol = symbol.lower()

    o = await submit_order(trader, symbol, what, external_account_id)
    if not o:
        return False

    await update_trading_data(symbol, o, strategy, (what["side"] == "buy"))
    await save(
        symbol=symbol,
        new_qty=0,
        last_op=str(what["side"]),
        price=0.0,
        indicators={},
        now=str(datetime.utcnow()),
    )

    if config.debug_enabled:
        tlog(
            f"executed strategy {strategy.name} on {symbol} w data {data_loader[symbol][-10:]}"
        )

    return True


async def do_strategy(
    strategy: Strategy,
    symbol: str,
    shortable: bool,
    position: float,
    data_loader: DataLoader,
    trader: Trader,
    minute_history: df,
    now: pd.Timestamp,
    portfolio_value: Optional[float],
    carrier=None,
) -> bool:
    """Run strategy on one symbol and execute any requested action.

    Parameters
    ----------
    strategy: Strategy
        The strategy to run.
    symbol: str
        The symbol to evaluate.
    shortable: bool
        Whether symbol can be sold short.
    position: float
        The current position in symbol.
    data_loader: DataLoader
        The loader of market data.
    trader: Trader
        The trader that submits orders.
    minute_history: df
        The minute bars of symbol.
    now: pd.Timestamp
        The current time.
    portfolio_value: Optional[float]
        The portfolio value, or None.
    carrier, default None
        Unused.

    Returns
    -------
    Return False if a requested order is not submitted or the strategy rejects
    symbol, and True otherwise.
    """
    do, what = await strategy.run(
        symbol=symbol,
        shortable=shortable,
        position=position,
        minute_history=minute_history,
        now=now,
        portfolio_value=portfolio_value,
    )

    return (
        await execute_strategy_result(
            strategy=strategy,
            trader=trader,
            data_loader=data_loader,
            symbol=symbol,
            what=what,
        )
        if do
        else not what.get("reject", False)
    )


async def _filter_strategies(symbol: str) -> List:
    """Return the per-symbol strategies that have not rejected symbol.

    Parameters
    ----------
    symbol: str
        The symbol to filter for.
    """
    return [
        s
        for s in trading_data.strategies
        if not await s.should_run_all()
        and symbol not in rejects.get(s.name, [])
    ]


async def do_strategies(
    trader: Trader,
    data_loader: DataLoader,
    symbol: str,
    position: float,
    now: pd.Timestamp,
    data: Dict,
    carrier=None,
) -> None:
    """Run each eligible per-symbol strategy on symbol.

    Exclude symbol from later runs of any strategy that rejects it. Log an
    exception from one strategy without stopping the others.

    Parameters
    ----------
    trader: Trader
        The trader that submits orders.
    data_loader: DataLoader
        The loader of market data.
    symbol: str
        The symbol to evaluate.
    position: float
        The current position in symbol.
    now: pd.Timestamp
        The current time.
    data: Dict
        Unused.
    carrier, default None
        Unused.
    """
    # run strategies
    strategies = await _filter_strategies(symbol)
    for s in strategies:
        try:
            if not await do_strategy(
                strategy=s,
                symbol=symbol,
                shortable=shortable[symbol],
                position=position,
                minute_history=data_loader[symbol].symbol_data,
                now=pd.to_datetime(now.replace(nanosecond=0)).replace(
                    second=0, microsecond=0
                ),
                portfolio_value=config.portfolio_value,
                data_loader=data_loader,
                trader=trader,
            ):
                if s.name not in rejects:
                    rejects[s.name] = [symbol]
                else:
                    rejects[s.name].append(symbol)

        except Exception as e:
            tlog(f"[EXCEPTION] in do_strategies() : {e}")
            if config.debug_enabled:
                tlog_exception("do_strategies()")


async def handle_aggregate(
    trader: Trader,
    data_loader: DataLoader,
    symbol: str,
    ts: pd.Timestamp,
    data: Dict,
    carrier=None,
) -> bool:
    """Process an aggregate bar for symbol and run its strategies.

    Also reconcile or cancel any stale open order for symbol.

    Parameters
    ----------
    trader: Trader
        The trader that manages orders.
    data_loader: DataLoader
        The loader that holds the minute data.
    symbol: str
        The symbol of the bar.
    ts: pd.Timestamp
        The bar timestamp.
    data: Dict
        The bar message.
    carrier, default None
        Unused.

    Returns
    -------
    Return True.
    """
    symbol = symbol.lower()

    await aggregate_bar_data(data_loader, data, ts)

    # Next, check for existing orders for the stock
    if symbol in trading_data.open_orders and await order_inflight(
        symbol, trading_data.open_orders[symbol.lower()], ts, trader
    ):
        return True

    await do_strategies(
        trader=trader,
        data_loader=data_loader,
        symbol=symbol,
        position=trading_data.positions.get(symbol, 0),
        now=ts,
        data=data,
    )

    return True


async def handle_data_queue_msg(
    data: Dict, trader: Trader, data_loader: DataLoader, carrier=None
) -> bool:
    """Handle a market data message from the data queue.

    Mark the symbol as shortable. Ignore a repeated timestamp for a symbol.

    Parameters
    ----------
    data: Dict
        The market data message.
    trader: Trader
        The trader that manages orders.
    data_loader: DataLoader
        The loader that holds the minute data.
    carrier, default None
        Unused.

    Returns
    -------
    Return False for a message, other than a minute aggregate, that arrives
    more than 15 seconds late. Return True otherwise.
    """
    global shortable
    global symbol_data_error
    global rejects

    ts = pd.to_datetime(
        data["timestamp"].replace(second=0, microsecond=0, nanosecond=0)
    )
    symbol = data["symbol"].lower()
    shortable[symbol] = True  # TODO revisit collecting 'is shortable' data

    # timezone("America/New_York")
    time_diff = datetime.now(tz=data["timestamp"].tz) - data["timestamp"]

    if data["EV"] != "AM" and time_diff > timedelta(seconds=15):
        if randint(1, 100) == 1:  # nosec
            tlog(f"{data['EV']} {symbol} too out of sync w {time_diff}")
        return False

    if (
        time_tick.get(symbol)
        and data["timestamp"].replace(microsecond=0, nanosecond=0)
        == time_tick[symbol]
    ):
        return True

    time_tick[symbol] = data["timestamp"].replace(microsecond=0, nanosecond=0)
    await handle_aggregate(
        trader=trader,
        data_loader=data_loader,
        symbol=symbol,
        ts=time_tick[symbol],
        data=data,
        carrier=None,
    )

    return True


async def queue_consumer(
    batch_id: str, queue: Queue, data_loader: DataLoader, trader: Trader
) -> None:
    """Consume and dispatch messages from the data queue until cancelled.

    Handle trade updates, new strategy requests and market data. On a
    ConnectionError, reconnect trader and put the message back on queue. Log
    other exceptions and continue.

    Parameters
    ----------
    batch_id: str
        The batch identifier for new strategies.
    queue: Queue
        The queue to read from.
    data_loader: DataLoader
        The loader that holds the minute data.
    trader: Trader
        The trader that manages orders.
    """
    tlog("queue_consumer() starting")
    try:
        while True:
            try:
                data = queue.get(timeout=2)
                if data["EV"] == "trade_update":
                    tlog(f"received trade_update: {data}")
                    t = Trade(**data["trade"])
                    await handle_trade_update(t)
                elif data["EV"] == "new_strategy":
                    tlog(f"received new_strategy: {data}")
                    await handle_new_strategy(
                        batch_id=batch_id,
                        data_loader=data_loader,
                        portfolio_id=data["portfolio_id"],
                        parameters=data["parameters"],
                    )
                else:
                    await handle_data_queue_msg(data, trader, data_loader)

            except Empty:
                await asyncio.sleep(0)
                continue
            except ConnectionError:
                await trader.reconnect()
                # re-post back to queue
                queue.put(data, timeout=1)
                await asyncio.sleep(1)
                continue
            except Exception as e:
                tlog(
                    f"Exception in queue_consumer(): exception of type {type(e).__name__} with args {e.args} inside loop"
                )
                if config.debug_enabled:
                    tlog_exception("queue_consumer")

    except asyncio.CancelledError:
        tlog("queue_consumer() cancelled ")
    except Exception as e:
        tlog(
            f"Exception in queue_consumer(): exception of type {type(e).__name__} with args {e.args}"
        )

        if config.debug_enabled:
            tlog_exception("queue_consumer")
    finally:
        tlog("queue_consumer() task done.")


async def create_strategies_from_file(
    batch_id: str,
    trader: Trader,
    data_loader: DataLoader,
    strategies_conf: Dict,
) -> List[Strategy]:
    """Create the strategies defined in the configuration file.

    Add the open positions of any configured portfolio to the trading state.

    Parameters
    ----------
    batch_id: str
        The batch identifier of the run.
    trader: Trader
        Unused.
    data_loader: DataLoader
        The loader passed to each strategy.
    strategies_conf: Dict
        A mapping of strategy name to its settings.

    Returns
    -------
    Return the strategies that were created.
    """
    strategy_list = []
    for strategy_name in strategies_conf:
        s = await Strategy.get_strategy(
            batch_id=batch_id,
            strategy_name=strategy_name,
            strategy_details=strategies_conf[strategy_name],
            data_loader=data_loader,
        )
        if s:
            strategy_list.append(s)

        if "portfolio_id" in strategies_conf[strategy_name]:
            positions = await load_symbol_position(
                strategies_conf[strategy_name]["portfolio_id"]
            )
            for symbol, qty in positions.items():
                trading_data.positions[symbol] = (
                    trading_data.positions.get(symbol, 0.0) + qty
                )
            tlog(f"Loaded {positions} positions for {strategy_name}")

    return strategy_list


async def load_symbol_position(portfolio_id: str) -> Dict[str, float]:
    """Return the net open quantity of each symbol held by a portfolio.

    Omit symbols with a net quantity of zero.

    Parameters
    ----------
    portfolio_id: str
        The portfolio whose trades are loaded.
    """
    trades = await load_trades_by_portfolio(portfolio_id)

    if not len(trades):
        return {}
    new_df = pd.DataFrame()
    new_df["symbol"] = trades.symbol.unique()
    new_df["qty"] = new_df.symbol.apply(
        lambda x: (
            trades[
                (trades.symbol == x) & (trades.operation == "buy")
            ].qty.sum()
        )
        - trades[(trades.symbol == x) & (trades.operation == "sell")].qty.sum()
    )
    new_df = new_df.loc[new_df.qty != 0]

    return {row.symbol: float(row.qty) for _, row in new_df.iterrows()}


async def create_strategies_from_db(
    batch_id: str,
    trader: Trader,
    data_loader: DataLoader,
) -> List[Strategy]:
    """Create the strategies defined by the active trade plan entries.

    Add the open positions of each created strategy's portfolio to the trading
    state.

    Parameters
    ----------
    batch_id: str
        The batch identifier of the run.
    trader: Trader
        Unused.
    data_loader: DataLoader
        The loader passed to each strategy.

    Returns
    -------
    Return the strategies that were created.
    """
    # load from tradeplan file
    trade_plan = await TradePlan.load()

    strategy_list = []
    for trade_plan_entry in trade_plan:
        strategy_details = trade_plan_entry.parameters
        strategy_details["portfolio_id"] = trade_plan_entry.portfolio_id

        strategy_name = strategy_details.pop("name")
        s = await Strategy.get_strategy(
            batch_id=batch_id,
            strategy_name=strategy_name,
            strategy_details=strategy_details,
            data_loader=data_loader,
        )
        if s:
            strategy_list.append(s)
            positions = await load_symbol_position(
                trade_plan_entry.portfolio_id
            )
            tlog(f"Loaded {len(positions)} positions for {strategy_name}")
            for symbol, qty in positions.items():
                trading_data.positions[symbol] = (
                    trading_data.positions.get(symbol, 0.0) + qty
                )

    return strategy_list


async def handle_new_strategy(
    batch_id: str, portfolio_id: str, parameters: dict, data_loader: DataLoader
):
    """Create a strategy requested at run time and add it to the active set.

    Parameters
    ----------
    batch_id: str
        The batch identifier of the run.
    portfolio_id: str
        The portfolio the strategy trades for.
    parameters: dict
        The strategy settings, including its name; modified in place.
    data_loader: DataLoader
        The loader passed to the strategy.
    """
    strategy_name = parameters.pop("name")
    parameters["portfolio_id"] = portfolio_id
    strategy = await Strategy.get_strategy(
        batch_id=batch_id,
        strategy_name=strategy_name,
        strategy_details=parameters,
        data_loader=data_loader,
    )

    if strategy:
        trading_data.strategies.append(strategy)


async def consumer_async_main(
    queue: Queue,
    unique_id: str,
    strategies_conf: Dict,
    file_only: bool,
):
    """Set up the consumer and run its tasks until they finish.

    Create the database connection pool and add the created strategies to the
    trading state. Collect task exceptions instead of raising them.

    Parameters
    ----------
    queue: Queue
        The queue of messages from the producer.
    unique_id: str
        The batch identifier of the run.
    strategies_conf: Dict
        A mapping of strategy name to its settings.
    file_only: bool
        Whether to skip the strategies in the trade plan.
    """
    await create_db_connection(str(config.dsn))
    data_loader = DataLoader()

    trader = trader_factory()

    trading_data.strategies += await create_strategies_from_file(
        batch_id=unique_id,
        trader=trader,
        data_loader=data_loader,
        strategies_conf=strategies_conf,
    )
    tlog(f"loaded strategies: {trading_data.strategies}")
    if not file_only:
        trading_data.strategies += await create_strategies_from_db(
            batch_id=unique_id,
            trader=trader,
            data_loader=data_loader,
        )

    queue_consumer_task = asyncio.create_task(
        queue_consumer(unique_id, queue, data_loader, trader)
    )

    periodic_runner_task = asyncio.create_task(
        periodic_runner(data_loader, trader)
    )

    cancel_lingering_orders_task = asyncio.create_task(
        cancel_lingering_orders(trader)
    )

    await asyncio.gather(
        queue_consumer_task,
        periodic_runner_task,
        cancel_lingering_orders_task,
        return_exceptions=True,
    )

    tlog("consumer_async_main() completed")


def consumer_main(
    queue: Queue,
    unique_id: str,
    conf: Dict,
) -> None:
    """Run a consumer process until its tasks complete.

    Set the build label, portfolio value and risk in config. Log a keyboard
    interrupt instead of raising it.

    Parameters
    ----------
    queue: Queue
        The queue of messages from the producer.
    unique_id: str
        The batch identifier of the run.
    conf: Dict
        The configuration, with a strategies section and optional
        portfolio_value, risk and file_only settings.
    """
    tlog(f"*** consumer_main() starting w pid {os.getpid()} ***")

    try:
        config.build_label = pygit2.Repository("../").describe(
            describe_strategy=pygit2.GIT_DESCRIBE_TAGS
        )
    except pygit2.GitError:
        import liualgotrader

        config.build_label = liualgotrader.__version__ if hasattr(liualgotrader, "__version__") else ""  # type: ignore

    config.portfolio_value = conf.get("portfolio_value", None)
    if "risk" in conf:
        config.risk = conf["risk"]

    try:
        asyncio.run(
            consumer_async_main(
                queue,
                unique_id,
                conf["strategies"],
                conf.get("file_only", False),
            ),
        )
    except KeyboardInterrupt:
        tlog("consumer_main() - Caught KeyboardInterrupt")

    tlog("*** consumer_main() completed ***")
