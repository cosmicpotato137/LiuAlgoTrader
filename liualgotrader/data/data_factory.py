"""Create the configured market data and streaming providers.

Functions
---------
data_loader_factory
    Return a new historical data provider for a connector.
streaming_factory
    Return the streaming provider class for a connector.
"""

from liualgotrader.common import config
from liualgotrader.common.types import DataConnectorType
from liualgotrader.data.alpaca import AlpacaData, AlpacaStream
from liualgotrader.data.data_base import DataAPI
from liualgotrader.data.finnhub import FinnhubData
from liualgotrader.data.gemini import GeminiData, GeminiStream
from liualgotrader.data.polygon import PolygonData, PolygonStream
from liualgotrader.data.tradier import TradierData


def data_loader_factory(connector: DataConnectorType = None) -> DataAPI:
    """Return a new historical data provider for a connector.

    Parameters
    ----------
    connector: DataConnectorType, default None
        The data connector, or None for the configured connector.

    Raises
    ------
    Raise Exception if the connector is not supported.
    """
    _connector = connector or config.data_connector
    if _connector == DataConnectorType.polygon:
        return PolygonData()
    elif _connector == DataConnectorType.alpaca:
        return AlpacaData()
    elif _connector == DataConnectorType.finnhub:
        return FinnhubData()
    elif _connector == DataConnectorType.gemini:
        return GeminiData()
    elif _connector == DataConnectorType.tradier:
        return TradierData()
    else:
        raise Exception(f"unsupported data provider {_connector}")


def streaming_factory(connector: DataConnectorType = None):
    """Return the streaming provider class for a connector.

    Parameters
    ----------
    connector: DataConnectorType, default None
        The data connector, or None for the configured connector.

    Returns
    -------
    Return the class itself, not an instance.

    Raises
    ------
    Raise Exception if the connector does not support streaming.
    """
    _connector = connector or config.data_connector
    if _connector == DataConnectorType.polygon:
        return PolygonStream
    elif _connector == DataConnectorType.alpaca:
        return AlpacaStream
    elif _connector == DataConnectorType.gemini:
        return GeminiStream
    else:
        raise Exception(f"unsupported streaming provider {_connector}")
