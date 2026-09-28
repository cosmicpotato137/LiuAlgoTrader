"""Miner for the details of active stocks listed by Polygon.

Classes
-------
StockCluster
    Miner that stores the details of active Polygon stocks.
"""

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

from polygon import RESTClient
from polygon.exceptions import BadResponse
from polygon.rest.models import TickerDetails
from urllib3.exceptions import HTTPError

from liualgotrader.common import config
from liualgotrader.common.decorators import timeit
from liualgotrader.common.tlog import tlog
from liualgotrader.miners.base import Miner
from liualgotrader.models.ticker_data import TickerData


class StockCluster(Miner):
    """Miner that stores the details of active stocks listed by Polygon.

    Requests authenticate with config.polygon_api_key.

    Attributes
    ----------
    num_workers
        The number of worker threads for concurrent requests.

    Methods
    -------
    run
        Download active stock details and save them to the database.
    """

    def __init__(self):
        """Initialize the miner with twenty worker threads."""
        self._num_workers = 20
        self._thread_data = threading.local()
        super().__init__(name="StockCluster")

    @property
    def num_workers(self):
        """Return the number of worker threads used for concurrent requests."""
        return self._num_workers

    @num_workers.setter
    def num_workers(self, new_num_workers: int):
        """Validate a requested number of worker threads.

        Parameters
        ----------
        new_num_workers: int
            The requested number of threads.

        Raises
        ------
        Raise ValueError unless new_num_workers is between 1 and 100.
        """
        if new_num_workers <= 0 or new_num_workers > 100:
            raise ValueError("number of workers must be positive and less than 100")

    @timeit
    async def run(self) -> bool:
        """Download the details of active stocks from Polygon and store them.

        Save the details of each active company to the ticker data table,
        logging progress and the run time.

        Returns
        -------
        Return True when done.
        """
        loop = asyncio.get_running_loop()
        tickers = await loop.run_in_executor(None, self._fetch_tickers)
        tlog(f"loaded {len(tickers)} tickers")

        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            tasks = [
                loop.run_in_executor(
                    executor, self._fetch_symbol_details, ticker
                )
                for ticker in tickers
            ]
            info = [
                response
                for response in await asyncio.gather(*tasks)
                if response is not None
            ]
        tlog(f"loaded {len(info)} ticker details")
        await asyncio.gather(*[self._update_ticker_details(i) for i in info])

        return True

    def _rest_client(self) -> RESTClient:
        """Return the Polygon REST client of the calling thread.

        Raises
        ------
        Raise polygon.exceptions.AuthError if config.polygon_api_key is not
        set.
        """
        # a client pools one connection per host, so each thread gets its own
        client = getattr(self._thread_data, "client", None)
        if client is None:
            client = RESTClient(config.polygon_api_key)
            self._thread_data.client = client
        return client

    def _fetch_tickers(self) -> List[str]:
        """Return the symbols of all active stock tickers.

        Block and retry indefinitely on connection errors.
        """
        while True:
            try:
                return [
                    ticker.ticker
                    for ticker in self._rest_client().list_tickers(
                        market="stocks", active=True, limit=1000
                    )
                ]
            except HTTPError as e:
                tlog(
                    f"_fetch_tickers(): got HTTP exception {e}, going to "
                    "sleep, then retry"
                )
                time.sleep(30)

    async def _update_ticker_details(self, ticker_info: Dict) -> None:
        """Save the details of one company to the ticker data table.

        Do nothing for an inactive company. Retry indefinitely if saving fails.

        Parameters
        ----------
        ticker_info: Dict
            The company details returned by _fetch_symbol_details.
        """
        if ticker_info["active"] is False:
            return

        ticker_data = TickerData(
            name=ticker_info["name"],
            symbol=ticker_info["symbol"],
            description=ticker_info["description"],
            tags=ticker_info["tags"],
            similar_tickers=ticker_info["similar"],
            industry=ticker_info["industry"],
            sector=ticker_info["sector"],
            exchange=ticker_info["exchange"],
        )

        if await ticker_data.save(config.db_conn_pool) is False:
            tlog(f"going to wait 30 seconds and retry saving {ticker_info['name']}")
            await asyncio.sleep(30)
            return await self._update_ticker_details(ticker_info)

    def _fetch_symbol_details(self, ticker: str) -> Optional[Dict]:
        """Return the company details of a ticker if the company is active.

        Block and retry indefinitely on connection errors.

        Parameters
        ----------
        ticker: str
            The symbol to look up.

        Returns
        -------
        Return a dict with the active, name, symbol, description, tags,
        similar, industry, sector and exchange keys. Polygon provides no tags
        or sector, so tags is empty and sector is None; industry is the SIC
        description and exchange is the MIC of the primary exchange. Return
        None for a failed request, missing details or an inactive company.
        """
        while True:
            try:
                client = self._rest_client()
                details = client.get_ticker_details(ticker)
                if not isinstance(details, TickerDetails):
                    tlog(f"no details for {ticker}")
                    return None
                if not details.active:
                    return None

                try:
                    similar = [
                        related.ticker
                        for related in client.get_related_companies(ticker)
                    ]
                except BadResponse:
                    similar = []

                return {
                    "active": details.active,
                    "name": details.name,
                    "symbol": details.ticker,
                    "description": details.description or "",
                    "tags": [],
                    "similar": similar,
                    "industry": details.sic_description,
                    "sector": None,
                    "exchange": details.primary_exchange,
                }
            except BadResponse:
                return None
            except HTTPError as e:
                tlog(
                    f"_fetch_symbol_details(): got HTTP exception {e} for "
                    f"{ticker}, going to sleep, then retry"
                )
                time.sleep(30)
