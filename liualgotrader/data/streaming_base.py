"""Base interface for real-time market data streaming providers.

Classes
-------
StreamingAPI
    Base class for real-time market data streaming providers.
"""

from typing import List

from liualgotrader.common.types import QueueMapper, WSEventType


class StreamingAPI:
    """Base class for real-time market data streaming providers.

    Creating an instance of any subclass registers it as the single shared
    instance that get_instance returns. The default subscribe, unsubscribe, run
    and close do nothing.

    Attributes
    ----------
    queues: QueueMapper
        The mapper from each symbol to its event queue.

    Methods
    -------
    subscribe
        Subscribe to event types for the given symbols.
    get_instance
        Return the most recently created instance.
    unsubscribe
        Cancel the subscriptions for a symbol.
    run
        Start delivering streaming events to the queues.
    close
        Stop streaming and close the connection.
    """

    __instance: object = None

    def __init__(self, queues: QueueMapper):
        """Initialize the provider and register it as the shared instance.

        Parameters
        ----------
        queues: QueueMapper
            The mapper from each symbol to its event queue.
        """
        self.queues = queues
        StreamingAPI.__instance = self

    async def subscribe(
        self, symbols: List[str], events: List[WSEventType]
    ) -> bool:
        """Subscribe to event types for the given symbols.

        Subclasses must override this method and return whether the
        subscription succeeded. The default returns None.

        Parameters
        ----------
        symbols: List[str]
            The symbols to subscribe to.
        events: List[WSEventType]
            The event types to receive.
        """
        pass

    @classmethod
    def get_instance(cls):
        """Return the most recently created streaming instance.

        Returns
        -------
        Return the same instance whichever subclass this is called on.

        Raises
        ------
        Raise AssertionError if no instance has been created.
        """
        if not cls.__instance:
            raise AssertionError("Must instantiate before usage")

        return cls.__instance  # type: ignore

    async def unsubscribe(self, symbol: str) -> bool:
        """Cancel the subscriptions for a symbol.

        Subclasses may override this method and return whether the cancellation
        succeeded. The default returns None.

        Parameters
        ----------
        symbol: str
            The symbol to unsubscribe.
        """
        pass

    async def run(self):
        """Start delivering streaming events to the queues.

        Subclasses must override this method. The default does nothing.
        """
        pass

    async def close(self) -> None:
        """Stop streaming and close the connection.

        Subclasses must override this method. The default does nothing.
        """
        pass
