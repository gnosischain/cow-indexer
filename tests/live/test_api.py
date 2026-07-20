import os

import pytest

from cow_indexer.sources.cow_api import CowApiClient

pytestmark = pytest.mark.skipif(os.getenv("COW_RUN_LIVE") != "1", reason="set COW_RUN_LIVE=1")


@pytest.mark.asyncio
async def test_mainnet_api_version() -> None:
    client = CowApiClient("https://api.cow.fi/mainnet", "mainnet")
    try:
        assert await client.version()
    finally:
        await client.close()
