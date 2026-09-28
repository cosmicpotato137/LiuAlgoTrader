"""Hyper-parameter definitions for the back-test optimizer.

Classes
-------
Parameter
    A single named hyper-parameter explored by the optimizer.
Hyperparameters
    A collection of parameters that spans their search grid.
"""

import itertools
import uuid
from typing import List

from liualgotrader.common.concurrency import get_event_loop
from liualgotrader.common.database import create_db_connection
from liualgotrader.models.portfolio import Portfolio


class Parameter:
    """A single named hyper-parameter explored by the optimizer.

    Numeric parameters step from the minimum toward the maximum, producing
    (name, value) pairs. A "portfolio" parameter creates a portfolio when
    called instead.

    Attributes
    ----------
    name
        The parameter name.
    param_type
        The value type: int, float or "portfolio".
    initial_value
        The minimum value, if one was given.
    last_value
        The maximum value, or the last portfolio identifier.
    value
        The current value, or None before the first step.
    """

    def __init__(self, name, param_type, min=None, max=None, **kwargs):
        """Initialize the parameter.

        Parameters
        ----------
        name
            The parameter name.
        param_type
            The value type: int, float or "portfolio", as a type or a string.
        min, default None
            The first value, ignored if falsy.
        max, default None
            The value at which stepping stops, ignored if falsy.
        **kwargs
            Extra settings, such as delta, size and credit, stored as
            attributes unless the name is already taken.
        """
        self.name = name
        self.param_type = param_type

        if min:
            self.initial_value = min

        if max:
            self.last_value = max

        self.value = None

        for key in kwargs:
            if not hasattr(self, key):
                setattr(self, key, kwargs[key])

    def __repr__(self):
        """Return the name of the parameter."""
        return self.name

    def __iter__(self):
        """Reset the current value and return the parameter itself."""
        self.value = None
        return self

    def __call__(self):
        """Create a new portfolio and return (name, portfolio_id).

        Create the shared database connection pool and save a portfolio sized
        by the size attribute, with the optional credit attribute as its credit
        line. Must not be called from asynchronous code.

        Raises
        ------
        Raise NotImplementedError unless the type is "portfolio".
        """
        if self.param_type != "portfolio":
            raise NotImplementedError(
                f"Parameter for type {self.param_type} is not implemented yet"
            )

        amount = getattr(self, "size")
        credit = getattr(self, "credit", 0)

        loop = get_event_loop()

        loop.run_until_complete(create_db_connection())
        portfolio_id = str(uuid.uuid4())
        loop.run_until_complete(
            Portfolio.save(
                portfolio_id=portfolio_id,
                portfolio_size=amount,
                credit=credit,
                parameters={},
            )
        )
        self.last_value = self.value = portfolio_id
        return (self.name, self.value)

    def __next__(self):
        """Advance to the next value and return (name, value).

        Start at the minimum and step by one for int parameters, or by the
        delta attribute for float parameters.

        Raises
        ------
        Raise StopIteration once the maximum is reached, AttributeError if a
        float parameter has no delta, and NotImplementedError for other types.
        """
        if (
            hasattr(self, "last_value")
            and self.value
            and self.value >= self.last_value
        ):
            raise StopIteration

        if self.param_type in ("int", int):
            self.value = (
                self.value + 1 if self.value else int(self.initial_value)
            )
        elif self.param_type in (float, "float"):
            if not hasattr(self, "delta"):
                raise AttributeError(
                    f"midding `delta` parameter for hyper-parameter {self.name}"
                )
            self.value = (
                self.value + self.delta if self.value else float(self.initial_value)  # type: ignore
            )
        else:
            raise NotImplementedError(
                f"Parameter for type {self.param_type} is not implemented yet as iterator"
            )

        return (self.name, self.value)


class Hyperparameters:
    """A collection of parameters that spans their full search grid.

    Attributes
    ----------
    hyperparameters: List[Parameter]
        The parameters that make up the search space.
    """

    def __init__(self, hyperparameters: List[Parameter]):
        """Initialize the search space.

        Parameters
        ----------
        hyperparameters: List[Parameter]
            The parameters that make up the search space.
        """
        self.hyperparameters = hyperparameters

    def __iter__(self):
        """Yield every combination of the parameters' values.

        Each item is a tuple holding one (name, value) pair per parameter.
        """
        yield from itertools.product(*self.hyperparameters)
