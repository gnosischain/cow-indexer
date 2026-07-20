from pathlib import Path

import pytest

from cow_indexer.config import load_config
from cow_indexer.services.preflight import run_preflight

ROOT = Path(__file__).parents[2]


@pytest.mark.asyncio
async def test_preflight_flags_missing_rpc_url(monkeypatch) -> None:
    # With no RPC URL configured the check reports it without any network call.
    monkeypatch.delenv("COW_RPC_URL_SEPOLIA", raising=False)
    chain = load_config(ROOT / "config" / "chains.yaml").select("sepolia")[0]
    [result] = await run_preflight([chain], api_key=None)
    assert result["chain"] == "sepolia"
    assert result["rpc"] == "no-url"
    assert result["ok"] is False
