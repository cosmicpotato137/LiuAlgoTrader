import pytest

from liualgotrader.common.data_loader import DataLoader  # type:ignore
from liualgotrader.common.database import create_db_connection
from liualgotrader.scanners.base import Scanner


@pytest.fixture(autouse=True)
async def db_connection():
    await create_db_connection()


@pytest.mark.asyncio
async def test_get_scanner():
    dl = DataLoader()
    s = await Scanner.get_scanner(
        data_loader=dl,
        scanner_name="MyScanner",
        scanner_details={
            "filename": "./examples/scanners/my_scanner.py",
        },
    )

    if not s:
        raise AssertionError("Failed to instantiate scanner")

    print(f"Loaded scanner {s.name}")

    return True
