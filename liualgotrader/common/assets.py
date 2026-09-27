"""Quote precision and minimum order size of tradable assets.

Functions
---------
get_asset_precision
    Return the number of decimal places used for an asset.
round_asset
    Round a value to the tick precision of an asset.
get_asset_min_qty
    Return the minimum order size permitted for an asset.
"""

from typing import Dict

from liualgotrader.common.types import AssetType

assets_details: Dict[str, Dict] = {
    "btc/usd": {
        "type": AssetType.CRYPTO,
        "min_order_size": 0.00001,
        "tick_precision": 8,
    },
    "btcusd": {
        "type": AssetType.CRYPTO,
        "min_order_size": 0.00001,
        "tick_precision": 8,
    },
    "eth/usd": {
        "type": AssetType.CRYPTO,
        "min_order_size": 0.001,
        "tick_precision": 6,
    },
    "ethusd": {
        "type": AssetType.CRYPTO,
        "min_order_size": 0.001,
        "tick_precision": 6,
    },
}


def get_asset_precision(asset_name: str) -> int:
    """Return the number of decimal places used to quote the given asset.

    Parameters
    ----------
    asset_name: str
        The asset name, case-insensitive.

    Raises
    ------
    Raise ValueError if the asset is not defined.
    """
    asset_name = asset_name.lower()
    if asset_name not in assets_details:
        raise ValueError(f"asset name {asset_name} is undefined")

    return assets_details[asset_name]["tick_precision"]


def round_asset(asset_name: str, value: float) -> float:
    """Round a value to the tick precision of the given asset.

    Parameters
    ----------
    asset_name: str
        The asset name, case-insensitive.
    value: float
        The value to round.

    Raises
    ------
    Raise ValueError if the asset is not defined.
    """
    asset_name = asset_name.lower()
    return round(value, get_asset_precision(asset_name))


def get_asset_min_qty(asset_name: str) -> float:
    """Return the minimum order size permitted for the given asset.

    Parameters
    ----------
    asset_name: str
        The asset name, case-insensitive.

    Raises
    ------
    Raise ValueError if the asset is not defined.
    """
    asset_name = asset_name.lower()
    if asset_name not in assets_details:
        raise ValueError(f"asset name {asset_name} is undefined")

    return assets_details[asset_name]["min_order_size"]
