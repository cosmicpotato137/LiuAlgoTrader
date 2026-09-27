"""Exceptions raised by the framework.

Exceptions
----------
LiuException
    Base class for framework exceptions.
MarketClosedToday
    Exception for a market that is closed for the day.
"""

class LiuException(Exception):
    """Base class for framework exceptions."""


class MarketClosedToday(LiuException):
    """Exception for a market that is closed for the day."""
