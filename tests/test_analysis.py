import os

os.environ["DATA_CONNECTOR"] = "alpaca"

import pytest

from liualgotrader.analytics import analysis
from liualgotrader.common.database import create_db_connection


@pytest.fixture(autouse=True)
async def db_connection():
    await create_db_connection()


@pytest.mark.asyncio
@pytest.mark.devtest
async def test_returns() -> bool:
    td = await analysis.calc_batch_returns(
        "0664dc2c-f126-4c7f-a069-c5dc246e2df4"
    )
    print(td)
    return True


@pytest.mark.asyncio
@pytest.mark.devtest
async def test_load_trades_by_portfolio():
    trades = await analysis.load_trades_by_portfolio(
        "71f0eaa7-281a-40ad-b120-d2f74eb0b05d"
    )
    print(trades)

    return True
