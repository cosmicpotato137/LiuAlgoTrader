"""Tradier brokerage trader.

Classes
-------
TradierTrader
    Trader implementation for the Tradier brokerage.
"""

import asyncio
import json
import queue
import time as time_action
from datetime import date, datetime, time, timedelta
from threading import Thread
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pytz
import requests
import websocket

from liualgotrader.common import config
from liualgotrader.common.assets import get_asset_min_qty, round_asset
from liualgotrader.common.tlog import tlog
from liualgotrader.common.types import Order, QueueMapper, ThreadFlags, Trade
from liualgotrader.trading.base import Trader

NY = "America/New_York"
nytz = pytz.timezone(NY)


class TradierTrader(Trader):
    """Trader implementation for the Tradier brokerage.

    The Tradier access token and account number come from the configuration.
    Orders are whole-share equity day orders, no symbol is fractionable, and
    only orders submitted through this class produce trade updates.

    Attributes
    ----------
    running_task: Optional[Thread]
        The account event listener thread, or None.
    ws: Optional[websocket]
        The account event WebSocket application, or None.
    ws_session_id: Optional[str]
        The streaming session identifier, or None.

    Methods
    -------
    is_fractionable
        Return False for every symbol (overrides Trader).
    is_order_completed
        Return the status, price and quantity of an order (overrides Trader).
    get_market_schedule
        Return today's extended trading window (overrides Trader).
    get_trading_days
        Return the open days in a range (overrides Trader).
    get_position
        Return the quantity held of a symbol (overrides Trader).
    get_order
        Return the current state of an order (overrides Trader).
    is_market_open_today
        Report whether the market opens today (overrides Trader).
    get_time_market_close
        Return today's closing time (overrides Trader).
    get_tradeable_symbols
        Return the easy-to-borrow stocks (overrides Trader).
    get_shortable_symbols
        Return the easy-to-borrow stocks (overrides Trader).
    is_shortable
        Report whether a symbol is easy to borrow (overrides Trader).
    cancel_order
        Request the cancellation of an order (overrides Trader).
    subscribe_order_status_update
        Subscribe the stream to order events.
    submit_order
        Submit an equity day order (overrides Trader).
    reconnect
        Restart the account event listener (overrides Trader).
    run
        Start the account event listener (overrides Trader).
    close
        Stop the account event listener (overrides Trader).
    on_message
        Publish order events from the stream as trade updates.
    on_error
        Log a stream error.
    on_close
        Log the closing of the stream.
    """

    order_symbol: Dict[int, str] = {}
    order_side: Dict[int, Order.FillSide] = {}

    def __init__(self, qm: QueueMapper = None):
        """Initialize the trader with no listener running.

        Register the trader as the current Trader instance.

        Parameters
        ----------
        qm: QueueMapper, default None
            The queue mapper that receives trade updates.
        """
        self.running_task: Optional[Thread] = None
        self.ws: Optional[websocket] = None
        self.ws_session_id: Optional[str] = None
        super().__init__(qm)

    def _get(self, url: str, params: Optional[Dict] = None):
        """Send an authenticated GET request to the Tradier API.

        Parameters
        ----------
        url: str
            The endpoint URL.
        params: Optional[Dict], default None
            The query parameters.

        Returns
        -------
        Return the response.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        if params is None:
            params = {}
        r = requests.get(
            url,
            params=params,
            headers={
                "Authorization": f"Bearer {config.tradier_access_token}",
                "Accept": "application/json",
            },
        )
        if r.status_code in (429, 502):
            tlog(f"{r.url} return {r.status_code}, waiting and re-trying")
            time_action.sleep(10)
            return self._get(url, params)
        elif r.status_code != 200:
            raise ValueError(f"HTTP ERROR {r.url} {r.status_code} {r.text}")

        return r

    def _post(self, url: str, params: Optional[Dict] = None):
        """Send an authenticated POST request to the Tradier API.

        Parameters
        ----------
        url: str
            The endpoint URL.
        params: Optional[Dict], default None
            The query parameters.

        Returns
        -------
        Return the response.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        if params is None:
            params = {}
        r = requests.post(
            url,
            params=params,
            headers={
                "Authorization": f"Bearer {config.tradier_access_token}",
                "Accept": "application/json",
            },
        )
        if r.status_code in (429, 502):
            tlog(f"{r.url} return {r.status_code}, waiting and re-trying")
            time_action.sleep(10)
            return self._post(url, params)
        elif r.status_code != 200:
            raise ValueError(f"HTTP ERROR {r.url} {r.status_code} {r.text}")

        return r

    def _delete(self, url: str, params: Optional[Dict] = None):
        """Send an authenticated DELETE request to the Tradier API.

        Parameters
        ----------
        url: str
            The endpoint URL.
        params: Optional[Dict], default None
            The query parameters.

        Returns
        -------
        Return the response.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        if params is None:
            params = {}
        r = requests.delete(
            url,
            params=params,
            headers={
                "Authorization": f"Bearer {config.tradier_access_token}",
                "Accept": "application/json",
            },
        )
        if r.status_code in (429, 502):
            tlog(f"{r.url} return {r.status_code}, waiting and re-trying")
            time_action.sleep(10)
            return self._delete(url, params)
        elif r.status_code != 200:
            raise ValueError(f"HTTP ERROR {r.url} {r.status_code} {r.text}")

        return r

    async def is_fractionable(self, symbol: str) -> bool:
        """Return False, since fractional shares are not supported.

        Parameters
        ----------
        symbol: str
            The symbol to check; ignored.
        """
        return False

    async def is_order_completed(
        self, order_id: str, external_order_id: Optional[str] = None
    ) -> Tuple[Order.EventType, float, float, float]:
        """Return the status, average price, quantity and fees of an order.

        The fee is always zero, and a missing price or quantity is reported as
        zero.

        Parameters
        ----------
        order_id: str
            The Tradier order identifier.
        external_order_id: Optional[str], default None
            Ignored.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        order = await self.get_order(order_id, external_order_id)
        return (
            order.event,
            float(order.avg_execution_price or 0.0),
            float(order.filled_qty or 0.0),
            0.0,
        )

    def get_market_schedule(
        self,
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Return today's trading window in New York time.

        The window runs from 07:00 to 19:55:59.999999.

        Returns
        -------
        Return two None values if the Tradier market clock reports a different
        date.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        url = f"{config.tradier_base_url}markets/clock"
        response = self._get(
            url,
        )
        data = response.json()

        last_open_date = datetime.strptime(
            data["clock"]["date"], "%Y-%m-%d"
        ).date()
        if last_open_date == date.today():
            start: Optional[datetime] = nytz.localize(
                datetime.combine(
                    date=last_open_date,
                    time=time(hour=7, minute=0, second=0, microsecond=0),
                )
            )

            end: Optional[datetime] = nytz.localize(
                datetime.combine(
                    date=last_open_date,
                    time=time(
                        hour=19, minute=55, second=59, microsecond=999999
                    ),
                )
            )
        else:
            start = end = None

        return start, end

    def get_trading_days(
        self, start_date: date, end_date: date = date.today()
    ) -> pd.DataFrame:
        """Return the open trading days in a date range with their hours.

        The DataFrame is indexed by date, with the premarket start in the open
        column and the postmarket end, or the regular close, in the close
        column.

        Parameters
        ----------
        start_date: date
            The first day of the range.
        end_date: date, default date.today()
            The last day of the range.

        Raises
        ------
        Raise ValueError if a request fails.
        """
        url = f"{config.tradier_base_url}markets/calendar"
        d = start_date
        df = pd.DataFrame()

        while True:
            response = self._get(
                url, params={"month": d.month, "year": d.year}
            )

            data = response.json()["calendar"]["days"]["day"]
            dict_data: Dict = {
                element["date"]: element
                for element in data
                if element["status"] == "open"
            }
            df1 = pd.DataFrame.from_dict(dict_data, orient="index")
            df1["close"] = df1.apply(
                lambda x: x.get("postmarket", x["open"])["end"], axis=1
            )
            df1["open"] = df1.apply(lambda x: x["premarket"]["start"], axis=1)

            df1.index = pd.to_datetime(df1.index)
            df1 = df1[["open", "close"]].loc[
                (datetime.combine(start_date, time.min) <= df1.index)
                & (df1.index <= datetime.combine(end_date, time.min))
            ]
            df = pd.concat([df, df1])
            if d.month == end_date.month and d.year == end_date.year:
                break

            try:
                d = date(month=d.month + 1, day=d.day, year=d.year)
            except ValueError:
                d = date(month=d.month + 1, day=d.day - 1, year=d.year)

        return df

    def get_position(self, symbol: str) -> float:
        """Return the quantity of a symbol held in the account.

        Parameters
        ----------
        symbol: str
            The symbol to look up.

        Returns
        -------
        Return zero if no position is held.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        url = f"{config.tradier_base_url}accounts/{config.tradier_account_number}/positions"
        response = self._get(
            url,
        )
        data = response.json()
        if data["positions"] == "null":
            return 0.0

        data = data["positions"]["position"]
        if isinstance(data, dict):
            return data["quantity"] if data["symbol"] == symbol else 0.0

        return next(
            (
                position["quantity"]
                for position in data
                if position["symbol"] == symbol
            ),
            0.0,
        )

    @classmethod
    def _get_event(cls, order_event: str) -> Order.EventType:
        """Return the event type for a Tradier order status.

        Parameters
        ----------
        order_event: str
            The Tradier order status.

        Returns
        -------
        Return the other event type for an unrecognized status.
        """
        return (
            Order.EventType.fill
            if order_event == "filled"
            else Order.EventType.partial_fill
            if order_event == "partially_filled"
            else Order.EventType.canceled
            if order_event == "canceled"
            else Order.EventType.rejected
            if order_event == "rejected"
            else Order.EventType.pending
            if order_event == "pending"
            else Order.EventType.error
            if order_event == "error"
            else Order.EventType.open
            if order_event == "open"
            else Order.EventType.other
        )

    async def get_order(
        self, order_id: str, client_order_id: Optional[str] = None
    ) -> Order:
        """Return the current state of an order.

        The price is the last fill price, the fees are zero, and the order tag
        becomes the external account identifier.

        Parameters
        ----------
        order_id: str
            The Tradier order identifier.
        client_order_id: Optional[str], default None
            Ignored.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        url = f"{config.tradier_base_url}accounts/{config.tradier_account_number}/orders/{order_id}"
        response = self._get(
            url,
        )
        data = response.json()
        return Order(
            order_id=data["order"]["id"],
            symbol=data["order"]["symbol"],
            event=self._get_event(data["order"]["status"]),
            submitted_at=data["order"]["create_date"],
            price=data["order"]["last_fill_price"],
            trade_fees=0.0,
            filled_qty=data["order"]["exec_quantity"],
            side=self._get_event_side(data["order"]["side"]),
            remaining_amount=data["order"]["remaining_quantity"],
            avg_execution_price=data["order"]["avg_fill_price"],
            external_account_id=data["order"]["tag"]
            if "tag" in data["order"]
            else None,
        )

    def is_market_open_today(self) -> bool:
        """Report whether the Tradier calendar lists today as open.

        Today is the current date in New York.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        url = f"{config.tradier_base_url}markets/calendar"
        response = self._get(
            url,
        )
        data = response.json()
        today = str(datetime.now(nytz).date())
        return next(
            (
                day["status"] == "open"
                for day in data["calendar"]["days"]["day"]
                if day["date"] == today
            ),
            False,
        )

    def get_time_market_close(self) -> Optional[timedelta]:
        """Return today's closing time as listed in the Tradier calendar.

        The closing time is the end of the postmarket session, or of the
        regular session when none is listed, in the calendar's format.

        Returns
        -------
        Return None if today has no session.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        url = f"{config.tradier_base_url}markets/calendar"
        response = self._get(
            url,
        )
        data = response.json()
        today = str(datetime.now(nytz).date())
        return next(
            (
                day.get("postmarket", day["open"])["end"]
                for day in data["calendar"]["days"]["day"]
                if day["date"] == today and "open" in day
            ),
            None,
        )

    async def get_tradeable_symbols(self) -> List[str]:
        """Return the easy-to-borrow stocks as the tradeable symbols.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        return await self.get_shortable_symbols()

    async def get_shortable_symbols(self) -> List[str]:
        """Return the stock symbols on the Tradier easy-to-borrow list.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        url = f"{config.tradier_base_url}markets/etb"
        response = self._get(
            url,
        )
        data = response.json()
        return [
            security["symbol"]
            for security in data["securities"]["security"]
            if security["type"] == "stock"
        ]

    async def is_shortable(self, symbol) -> bool:
        """Report whether a symbol is on the Tradier easy-to-borrow list.

        Parameters
        ----------
        symbol
            The symbol to check, in any letter case.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        return symbol.upper() in await self.get_shortable_symbols()

    async def cancel_order(self, order: Order) -> bool:
        """Request the cancellation of an order.

        Parameters
        ----------
        order: Order
            The order to cancel.

        Returns
        -------
        Return True if Tradier accepts the request.

        Raises
        ------
        Raise ValueError if the request fails.
        """
        url = f"{config.tradier_base_url}accounts/{config.tradier_account_number}/orders/{order.order_id}"

        response = self._delete(
            url,
        )
        data = response.json()
        return data["order"]["status"] == "ok"

    @classmethod
    def _get_event_side(cls, side: str) -> Order.FillSide:
        """Return the fill side for a Tradier order side.

        Report buy and buy_to_cover as buy, and every other side as sell.

        Parameters
        ----------
        side: str
            The Tradier order side.
        """
        if side in {"buy", "buy_to_cover"}:
            return Order.FillSide.buy

        return Order.FillSide.sell

    async def subscribe_order_status_update(self) -> None:
        """Subscribe the streaming session to order status events.

        Do nothing unless the listener is running with an open session.
        """
        if (
            self.ws is not None
            and self.ws_session_id
            and self.running_task
            and self.running_task.is_alive()
        ):
            payload = {
                "events": ["order"],
                "sessionid": self.ws_session_id,
                "excludeAccounts": [],
            }
            self.ws.send(json.dumps(payload))
            tlog(
                f"subscribed for order update on session {self.ws_session_id}"
            )

    async def submit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        order_type: str,
        time_in_force: str = None,
        limit_price: str = None,
        stop_price: str = None,
        client_order_id: str = None,
        extended_hours: bool = None,
        order_class: str = None,
        take_profit: dict = None,
        stop_loss: dict = None,
        trail_price: str = None,
        trail_percent: str = None,
        on_behalf_of: str = None,
    ) -> Order:
        """Submit an equity day order to Tradier.

        Parameters
        ----------
        symbol: str
            The stock symbol.
        qty: float
            The quantity, truncated to whole shares.
        side: str
            The Tradier order side, such as buy or sell.
        order_type: str
            The Tradier order type, such as market or limit.
        time_in_force: str, default None
            Ignored.
        limit_price: str, default None
            The limit price, rounded to cents.
        stop_price: str, default None
            The stop price, rounded to cents.
        client_order_id: str, default None
            Ignored.
        extended_hours: bool, default None
            Ignored.
        order_class: str, default None
            Ignored.
        take_profit: dict, default None
            Ignored.
        stop_loss: dict, default None
            Ignored.
        trail_price: str, default None
            Ignored.
        trail_percent: str, default None
            Ignored.
        on_behalf_of: str, default None
            The external account, sent as the order tag.

        Returns
        -------
        Return the new order with a pending status, and record it so that its
        stream events become trade updates.

        Raises
        ------
        Raise ValueError if the request fails or Tradier reports errors.
        """
        url = f"{config.tradier_base_url}accounts/{config.tradier_account_number}/orders"

        params: Dict = {
            "class": "equity",
            "symbol": symbol,
            "side": side,
            "quantity": str(int(qty)),
            "duration": "day",
            "type": order_type,
        }

        if limit_price:
            params["price"] = str(round(float(limit_price), 2))
        if stop_price:
            params["stop"] = str(round(float(stop_price), 2))
        if on_behalf_of:
            params["tag"] = on_behalf_of

        response = self._post(
            url,
            params,
        )
        data = response.json()

        if "errors" in data:
            raise ValueError(f"order failed with {data['errors']}")

        event_side = self._get_event_side(side)
        o = Order(
            symbol=symbol,
            order_id=data["order"]["id"],
            event=Order.EventType.pending,
            side=event_side,
            submitted_at=datetime.now(nytz),
            remaining_amount=qty,
        )

        if on_behalf_of:
            o.external_account_id = on_behalf_of
        if limit_price:
            o.price = float(limit_price)

        TradierTrader.order_symbol[int(o.order_id)] = symbol
        TradierTrader.order_side[int(o.order_id)] = event_side
        return o

    async def reconnect(self):
        """Restart the account event listener."""
        await self.close()
        await self.run()

    async def run(self) -> Optional[asyncio.Task]:
        """Start the account event listener and subscribe it to order events.

        Returns
        -------
        Return the listener thread; if it already exists, return it without
        starting another.

        Raises
        ------
        Raise ValueError if no streaming session can be created.
        """
        if not self.running_task:
            tlog("starting Tradier Account listener")

            url = f"{config.tradier_base_url}accounts/events/session"

            response = self._post(
                url,
            )
            data = response.json()

            ws_url = data["stream"]["url"]
            self.ws_session_id = data["stream"]["sessionid"]

            tlog(
                f"starting WebSocketApp on {ws_url} with session-id {self.ws_session_id}"
            )
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            self.running_task = Thread(
                target=self.ws.run_forever,
            )
            self.running_task.start()
            await asyncio.sleep(1.0)
            await self.subscribe_order_status_update()

        return self.running_task  # type: ignore

    async def close(self):
        """Stop the account event listener if it is running.

        Block until the listener thread has finished.
        """
        if self.running_task and self.running_task.is_alive():
            tlog(f"close task {self.running_task}")
            self.ws.keep_running = False
            self.running_task.join()
            tlog("task terminated")
            self.ws = None
            self.running_task = None
            self.ws_session_id = None

    @classmethod
    def _trade_from_dict(cls, trade_dict: Dict) -> Trade:
        """Return a Trade built from a Tradier order event.

        The fee is zero, and a missing fill quantity or price is reported as
        zero.

        Parameters
        ----------
        trade_dict: Dict
            An order event received from the account stream.

        Raises
        ------
        Raise KeyError if the order was not submitted through submit_order.
        """
        return Trade(
            order_id=trade_dict["id"],
            symbol=TradierTrader.order_symbol[trade_dict["id"]],
            event=cls._get_event(trade_dict["status"]),
            side=TradierTrader.order_side[trade_dict["id"]],
            filled_qty=float(trade_dict.get("last_fill_quantity", 0.0)),
            trade_fee=0.0,
            filled_avg_price=float(trade_dict.get("last_fill_price", 0.0)),
            liquidity="",
            updated_at=trade_dict["create_date"],
        )

    @classmethod
    def on_message(cls, ws, msgs):
        """Publish order events from the Tradier stream as trade updates.

        Log each filled, cancelled, cancel_rejected or rejected event of an
        order submitted through submit_order, and put it as a trade update on
        every queue of the current trader.

        Parameters
        ----------
        ws
            The WebSocket application; unused.
        msgs
            The raw JSON text of a stream event.

        Raises
        ------
        Raise queue.Full if a queue stays full.
        """
        msg = json.loads(msgs)

        if msg["event"] != "order":
            return

        if (
            msg["status"]
            in {
                "filled",
                "cancel_rejected",
                "cancelled",
                "rejected",
            }
            and msg["id"] in TradierTrader.order_symbol
        ):
            trade = cls._trade_from_dict(msg)
            symbol = trade.symbol.lower()
            tlog(f"TRADIER TRADING UPDATE:{trade}")
            to_send = {
                "EV": "trade_update",
                "symbol": symbol,
                "trade": trade.__dict__,
            }
            try:
                if qs := cls.get_instance().queues:
                    for q in qs.get_allqueues():
                        q.put(to_send, timeout=1)
            except queue.Full as f:
                tlog(
                    f"[EXCEPTION] queue for {symbol} is FULL:{f}, sleeping for 2 seconds and re-trying."
                )
                raise

    @classmethod
    def on_error(cls, ws, error):
        """Log an error reported by the WebSocket connection.

        Parameters
        ----------
        ws
            The WebSocket application; unused.
        error
            The reported error.
        """
        tlog(f"[ERROR] TradierTrader {error}")

    @classmethod
    def on_close(cls, ws, close_status_code, close_msg):
        """Log the closing of the WebSocket connection.

        Parameters
        ----------
        ws
            The WebSocket application; unused.
        close_status_code
            The close status code.
        close_msg
            The close message.
        """
        tlog(
            f"on_close(): TradierTrader status={close_status_code}, close_msg={close_msg}"
        )
