"""Abstract base class for miners.

Classes
-------
Miner
    Abstract base class for named, asynchronous miners.
"""

from abc import ABCMeta, abstractmethod


class Miner(metaclass=ABCMeta):
    """Abstract base class for named, asynchronous miners.

    Subclasses must implement run.

    Attributes
    ----------
    name
        The miner name.

    Methods
    -------
    run
        Execute the mining task.
    """

    def __init__(
        self,
        name: str,
    ):
        """Initialize the miner.

        Parameters
        ----------
        name: str
            The miner name.
        """
        self._name = name

    @property
    def name(self):
        """Return the name of the miner."""
        return self._name

    @abstractmethod
    async def run(self) -> bool:
        """Execute the mining task and return whether it succeeded.

        Subclasses must override this coroutine.
        """
        pass
