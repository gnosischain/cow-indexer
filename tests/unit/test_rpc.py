import pytest

from cow_indexer.sources.http import HttpResponse
from cow_indexer.sources.rpc import RpcClient, RpcError, RpcRangeTooLarge, is_range_error


def test_detects_common_log_range_errors() -> None:
    assert is_range_error(-32005, "anything")
    assert is_range_error(-1, "query returned more than 10000 results")
    assert is_range_error(-1, "please limit the block range")
    assert is_range_error(-1, "Too many logs requested. Max logs per response is 20000.")
    assert not is_range_error(-32601, "method not found")


class _StubTransport:
    def __init__(self, response: HttpResponse) -> None:
        self.response = response

    async def request(self, method, url, *, params=None, json=None):
        return self.response

    async def close(self) -> None:
        pass


def _client(response: HttpResponse) -> RpcClient:
    return RpcClient("https://rpc.example", "test", transport=_StubTransport(response))


@pytest.mark.asyncio
async def test_range_error_with_non_200_status_is_classified() -> None:
    # Provider returns the -32005 limit error with an HTTP 503 status; the scanner
    # relies on this being RpcRangeTooLarge to halve the range instead of aborting.
    body = {"jsonrpc": "2.0", "error": {"code": -32005, "message": "Max logs per response is 20000"}}
    with pytest.raises(RpcRangeTooLarge):
        await _client(HttpResponse(503, body)).get_logs(0, 40000, ["0x" + "11" * 20])


@pytest.mark.asyncio
async def test_non_range_error_with_non_200_status_stays_plain_rpc_error() -> None:
    body = {"jsonrpc": "2.0", "error": {"code": -32000, "message": "execution reverted"}}
    with pytest.raises(RpcError) as exc:
        await _client(HttpResponse(400, body)).get_logs(0, 40000, ["0x" + "11" * 20])
    assert not isinstance(exc.value, RpcRangeTooLarge)


@pytest.mark.asyncio
async def test_non_json_503_is_plain_rpc_error() -> None:
    with pytest.raises(RpcError) as exc:
        await _client(HttpResponse(503, None, text="service unavailable")).get_logs(
            0, 100, ["0x" + "11" * 20]
        )
    assert not isinstance(exc.value, RpcRangeTooLarge)
