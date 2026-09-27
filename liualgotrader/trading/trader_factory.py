"""Create and look up the shared broker trader.

Functions
---------
trader_factory
    Return the shared trader for the configured broker.
get_trader_by_name
    Return the trader stored by trader_factory under a name.
"""

from typing import Dict

from liualgotrader.common import config
from liualgotrader.common.types import BrokerType
from liualgotrader.trading.alpaca import AlpacaTrader
from liualgotrader.trading.base import Trader
from liualgotrader.trading.gemini import GeminiTrader
from liualgotrader.trading.tradier import TradierTrader

traders: Dict[str, Trader] = {}


def trader_factory(*args, **kwargs) -> Trader:
    """Return the shared trader for the configured broker.

    Create the trader on first use and store it in traders under ALPACA, GEMINI
    or TRADIER; return an existing trader as is, ignoring the arguments.

    Parameters
    ----------
    *args
        Positional arguments passed to the trader constructor.
    **kwargs
        Keyword arguments passed to the trader constructor.

    Raises
    ------
    Raise Exception if config.broker is not supported.
    """
    global traders

    if config.broker == BrokerType.alpaca:
        if "ALPACA" not in traders:
            traders["ALPACA"] = AlpacaTrader(*args, **kwargs)
        return traders["ALPACA"]
    elif config.broker == BrokerType.gemini:
        if "GEMINI" not in traders:
            traders["GEMINI"] = GeminiTrader(*args, **kwargs)
        return traders["GEMINI"]
    elif config.broker == BrokerType.tradier:
        if "TRADIER" not in traders:
            traders["TRADIER"] = TradierTrader(*args, **kwargs)
        return traders["TradierTrader"]
    else:
        raise Exception(f"unsupported broker  {config.broker}")


def get_trader_by_name(trader_name: str) -> Trader:
    """Return the trader stored by trader_factory under a name.

    Parameters
    ----------
    trader_name: str
        The key of the trader, such as ALPACA.

    Raises
    ------
    Raise ValueError if no trader exists under that name.
    """
    global traders

    if trader_name not in traders:
        raise ValueError(f"Trader {trader_name} was not initialized")

    return traders[trader_name]
