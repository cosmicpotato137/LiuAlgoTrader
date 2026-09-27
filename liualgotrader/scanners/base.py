"""Base class for symbol scanners.

Classes
-------
Scanner
    Abstract base class for scanners that select symbols to trade.
"""

import importlib
import traceback
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from liualgotrader.common.data_loader import DataLoader  # type: ignore
from liualgotrader.common.tlog import tlog


class Scanner(metaclass=ABCMeta):
    """Abstract base class for scanners that select symbols to trade.

    Subclasses must implement run.

    Attributes
    ----------
    name: str
        The scanner name.
    recurrence: Optional[timedelta]
        The interval between runs, or None.
    target_strategy_name: Optional[str]
        The strategy that receives the results, or None.
    data_loader: DataLoader
        The loader used to fetch market data.
    data_source: object
        An optional data source object, or None.

    Methods
    -------
    run
        Return the symbols selected by the scanner.
    get_supported_scanners
        Return the names of the built-in scanners.
    get_scanner
        Load and instantiate a custom scanner from a file.
    """

    def __init__(
        self,
        name: str,
        data_loader: DataLoader,
        recurrence: Optional[timedelta],
        target_strategy_name: Optional[str],
        data_source: object = None,
    ):
        """Initialize the scanner.

        Parameters
        ----------
        name: str
            The scanner name.
        data_loader: DataLoader
            The loader used to fetch market data.
        recurrence: Optional[timedelta]
            The interval between runs, or None.
        target_strategy_name: Optional[str]
            The strategy that receives the results, or None.
        data_source: object, default None
            An optional data source object.
        """
        self.name = name
        self.recurrence = recurrence
        self.target_strategy_name = target_strategy_name
        self.data_loader = data_loader
        self.data_source = data_source

    def __repr__(self):
        """Return the name of the scanner."""
        return self.name

    @abstractmethod
    async def run(self, back_time: datetime = None) -> List[str]:
        """Return the symbols selected by the scanner.

        Subclasses must override this method.

        Parameters
        ----------
        back_time: datetime, default None
            The moment to scan as of, when backtesting.
        """
        return []

    @classmethod
    def get_supported_scanners(cls):
        """Return the names of the built-in scanners."""
        return ["momentum"]

    @classmethod
    async def get_scanner(
        cls,
        data_loader: DataLoader,
        scanner_name: str,
        scanner_details: Dict,
    ):
        """Load and instantiate a custom scanner class from a Python file.

        Exit the process if the file cannot be loaded or the class does not
        inherit from Scanner.

        Parameters
        ----------
        data_loader: DataLoader
            The loader passed to the new scanner.
        scanner_name: str
            The name of the class to instantiate.
        scanner_details: Dict
            The scanner settings, including the source file path under
            "filename" and an optional "recurrence" in minutes; modified in
            place.

        Returns
        -------
        Return the new scanner instance.
        """
        try:
            spec = importlib.util.spec_from_file_location(  # type: ignore
                "module.name", scanner_details["filename"]
            )
            custom_scanner_module = importlib.util.module_from_spec(spec)  # type: ignore
            spec.loader.exec_module(custom_scanner_module)  # type: ignore
            class_name = scanner_name
            custom_scanner = getattr(custom_scanner_module, class_name)

            if not issubclass(custom_scanner, Scanner):
                tlog(
                    f"custom scanner must inherit from class {Scanner.__name__}"
                )
                exit(0)

            scanner_details.pop("filename")
            if "recurrence" not in scanner_details:
                scanner_object = custom_scanner(
                    data_loader=data_loader,
                    **scanner_details,
                )
            else:
                recurrence = scanner_details.pop("recurrence")
                scanner_object = custom_scanner(
                    data_loader=data_loader,
                    recurrence=timedelta(minutes=recurrence),
                    **scanner_details,
                )
        except FileNotFoundError as e:
            tlog(
                f"[EXCEPTION] {e} : file not found `{scanner_details['filename']}`"
            )
            exit(0)
        except Exception as e:
            tlog(
                f"[Error]exception of type {type(e).__name__} with args {e.args}"
            )
            traceback.print_exc()
            exit(0)

        else:
            return scanner_object
