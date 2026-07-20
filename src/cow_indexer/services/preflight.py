"""Read-only readiness check for every configured chain: RPC connectivity, the
ability to serve historical eth_getLogs at the deployment block (the archive test
a pruned/public node fails), and CoW API reachability. Runs no ClickHouse and
mutates nothing, so it is safe to run against a live deployment."""

from __future__ import annotations

import asyncio

from cow_indexer.config import ChainConfig
from cow_indexer.sources.cow_api import CowApiClient, CowApiError
from cow_indexer.sources.rpc import RpcClient, RpcRangeTooLarge

# Small historical window probed at the deployment block.
HISTORICAL_PROBE_RANGE = 2000
RPC_TIMEOUT = 20.0


async def preflight_chain(chain: ChainConfig, api_key: str | None) -> dict:
    result: dict = {
        "chain": chain.key,
        "chain_id": chain.chain_id,
        "environment": chain.environment,
        "rpc": "unknown",
        "head": None,
        "deploy_start": None,
        "historical_logs": "unknown",
        "api": "unknown",
        "ok": False,
        "note": "",
    }

    try:
        rpc_url = chain.rpc_url
    except RuntimeError as exc:
        result["rpc"] = "no-url"
        result["note"] = str(exc)
        return result

    if chain.deployment is None:
        chain.load_deployment()
    deployment = chain.deployment
    assert deployment is not None
    result["deploy_start"] = deployment.start_block
    addresses = [c.address for c in deployment.contracts if "Settlement" in c.name] or [
        deployment.contracts[0].address
    ]

    rpc = RpcClient(rpc_url, chain.key, request_timeout=RPC_TIMEOUT)
    try:
        head = await rpc.get_block_number()
        result["head"] = head
        result["rpc"] = "ok"
        upper = min(deployment.start_block + HISTORICAL_PROBE_RANGE - 1, head)
        try:
            logs = await rpc.get_logs(deployment.start_block, upper, addresses)
            result["historical_logs"] = f"ok ({len(logs)} logs)"
        except RpcRangeTooLarge:
            # The node serves the range but caps response size; the scanner adapts.
            result["historical_logs"] = "ok (range-capped)"
        except Exception as exc:  # noqa: BLE001 - report any provider failure verbatim
            result["historical_logs"] = "fail"
            result["note"] = f"getLogs: {type(exc).__name__}: {exc}"[:180]
    except Exception as exc:  # noqa: BLE001
        result["rpc"] = "fail"
        result["note"] = f"rpc: {type(exc).__name__}: {exc}"[:180]
    finally:
        await rpc.close()

    api = CowApiClient(
        chain.api_base_url, chain.key, interval_seconds=0.1, max_attempts=2, api_key=api_key
    )
    try:
        version = await api.version()
        result["api"] = f"ok ({str(version)[:24]})"
    except CowApiError as exc:
        result["api"] = f"http {exc.status}"
    except Exception as exc:  # noqa: BLE001
        result["api"] = f"fail ({type(exc).__name__})"
    finally:
        await api.close()

    # Readiness is gated on the RPC path (on-chain indexing); a failing API only
    # degrades enrichment for that chain and is reported separately.
    result["ok"] = result["rpc"] == "ok" and result["historical_logs"].startswith("ok")
    return result


async def run_preflight(chains: list[ChainConfig], api_key: str | None) -> list[dict]:
    return list(await asyncio.gather(*(preflight_chain(chain, api_key) for chain in chains)))
