"""Detect candlestick patterns in open, high, low and close prices.

Each function returns True if the prices match its pattern.

Functions
---------
gravestone_doji
    Detect a gravestone doji candle.
four_price_doji
    Detect a four-price doji candle.
doji
    Detect a doji candle.
spinning_top
    Detect a spinning top candle.
bullish_candle
    Detect a strong bullish candle.
bearish_candle
    Detect a bearish candle.
dragonfly_candle
    Detect a dragonfly candle.
spinning_top_bearish_followup
    Detect a spinning top then a bearish candle.
bullish_candle_followed_by_dragonfly
    Detect bullish then dragonfly candles.
"""

from typing import Tuple


def gravestone_doji(open: float, high: float, low: float, close: float) -> bool:
    """Return whether the prices form a gravestone doji candle.

    Compare prices rounded to two decimals. Require a body under 0.02, total
    shadows above both 0.02 and twice the body, and an upper shadow over twice
    the lower one.

    Parameters
    ----------
    open: float
        The opening price.
    high: float
        The high price.
    low: float
        The low price.
    close: float
        The closing price.
    """
    open = round(open, 2)
    high = round(high, 2)
    low = round(low, 2)
    close = round(close, 2)

    body_size = close - open if close > open else open - close
    upper_shadow = high - max(close, open)
    lower_shadow = min(close, open) - low
    shadow_size = upper_shadow + lower_shadow

    return (
        body_size < 0.02 < shadow_size
        and lower_shadow * 2 < upper_shadow
        and shadow_size > 2 * body_size
    )


def four_price_doji(open: float, close: float, high: float, low: float) -> bool:
    """Return whether the prices form a four-price doji candle.

    Unlike most functions in this module, close precedes high and low.

    Parameters
    ----------
    open: float
        The opening price.
    close: float
        The closing price.
    high: float
        The high price.
    low: float
        The low price.

    Returns
    -------
    Return True if all four prices are equal when rounded to two decimals.
    """
    open = round(open, 2)
    high = round(high, 2)
    low = round(low, 2)
    close = round(close, 2)

    return close == open == high == low


def doji(open: float, close: float, high: float, low: float) -> bool:
    """Return whether the prices form a doji candle.

    Unlike most functions in this module, close precedes high and low.

    Parameters
    ----------
    open: float
        The opening price.
    close: float
        The closing price.
    high: float
        The high price.
    low: float
        The low price.

    Returns
    -------
    Return True if, rounded to two decimals, close equals open and the range
    extends at least 0.01 above and below it.
    """
    open = round(open, 2)
    high = round(high, 2)
    low = round(low, 2)
    close = round(close, 2)

    return close == open and low <= open - 0.01 and high >= open + 0.01


def spinning_top(open: float, high: float, low: float, close: float) -> bool:
    """Return whether the prices form a spinning top candle.

    Compare prices rounded to two decimals. Require total shadows of at least
    twice the body, each shadow above 0.02, and an upper-to-lower shadow ratio
    strictly between 0.8 and 1.2.

    Parameters
    ----------
    open: float
        The opening price.
    high: float
        The high price.
    low: float
        The low price.
    close: float
        The closing price.
    """
    open = round(open, 2)
    high = round(high, 2)
    low = round(low, 2)
    close = round(close, 2)

    upper_shadow = high - max(close, open)
    lower_shadow = min(close, open) - low
    shadow_size = upper_shadow + lower_shadow
    body_size = close - open if close > open else open - close
    return (
        shadow_size >= 2 * body_size
        and lower_shadow > 0.02
        and upper_shadow > 0.02
        and 0.8 < upper_shadow / lower_shadow < 1.2
    )


def bullish_candle(open: float, high: float, low: float, close: float) -> bool:
    """Return whether the prices form a strong bullish candle.

    Compare prices rounded to two decimals. Require close to exceed open by
    more than 0.02 and the body to exceed 1.2 times the total shadows.

    Parameters
    ----------
    open: float
        The opening price.
    high: float
        The high price.
    low: float
        The low price.
    close: float
        The closing price.
    """
    open = round(open, 2)
    high = round(high, 2)
    low = round(low, 2)
    close = round(close, 2)

    upper_shadow = high - close
    lower_shadow = open - low
    shadow_size = upper_shadow + lower_shadow
    body_size = close - open

    return close > open + 0.02 and body_size > shadow_size * 1.2


def bearish_candle(open: float, high: float, low: float, close: float) -> bool:
    """Return whether the prices form a bearish candle.

    High and low do not affect the result.

    Parameters
    ----------
    open: float
        The opening price.
    high: float
        The high price.
    low: float
        The low price.
    close: float
        The closing price.

    Returns
    -------
    Return True if, rounded to two decimals, close is below open by at least
    0.01.
    """
    open = round(open, 2)
    high = round(high, 2)
    low = round(low, 2)
    close = round(close, 2)

    body_size = close - open if close > open else open - close

    return close < open and body_size >= 0.01


def dragonfly_candle(open: float, high: float, low: float, close: float) -> bool:
    """Return whether the prices form a dragonfly candle.

    Compare prices rounded to two decimals. Require a body under 0.01, a lower
    shadow over twice the upper one, and total shadows over three times the
    body.

    Parameters
    ----------
    open: float
        The opening price.
    high: float
        The high price.
    low: float
        The low price.
    close: float
        The closing price.
    """
    open = round(open, 2)
    high = round(high, 2)
    low = round(low, 2)
    close = round(close, 2)

    upper_shadow = high - max(close, open)
    lower_shadow = min(close, open) - low
    shadow_size = upper_shadow + lower_shadow
    body_size = close - open if close > open else open - close

    return (
        body_size < 0.01
        and lower_shadow > 2 * upper_shadow
        and shadow_size > 3 * body_size
    )


def spinning_top_bearish_followup(
    minute1: Tuple[float, float, float, float],
    minute2: Tuple[float, float, float, float],
) -> bool:
    """Return whether a spinning top is followed by a bearish candle.

    The second candle counts as bearish if it closes below its open.

    Parameters
    ----------
    minute1: Tuple[float, float, float, float]
        The open, high, low and close of the first candle.
    minute2: Tuple[float, float, float, float]
        The open, high, low and close of the second candle.
    """
    return (
        spinning_top(minute1[0], minute1[1], minute1[2], minute1[3])
        and minute2[0] > minute2[3]
    )


def bullish_candle_followed_by_dragonfly(
    minute1: Tuple[float, float, float, float],
    minute2: Tuple[float, float, float, float],
) -> bool:
    """Return whether a bullish candle is followed by a dragonfly candle.

    The second candle must also open above the close of the first.

    Parameters
    ----------
    minute1: Tuple[float, float, float, float]
        The open, high, low and close of the first candle.
    minute2: Tuple[float, float, float, float]
        The open, high, low and close of the second candle.
    """
    return (
        bullish_candle(minute1[0], minute1[1], minute1[2], minute1[3])
        and dragonfly_candle(minute2[0], minute2[1], minute2[2], minute2[3])
        and minute2[0] > minute1[3]
    )
