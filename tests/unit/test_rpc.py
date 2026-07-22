import pytest

from cow_indexer.sources.http import HttpResponse
from cow_indexer.sources.rpc import RpcClient, RpcError, RpcRangeTooLarge, is_range_error


def test_detects_common_log_range_errors() -> None:
    assert is_range_error(-32005, "anything")
    assert is_range_error(-1, "query returned more than 10000 results")
    assert is_range_error(-1, "please limit the block range")
    assert is_range_error(-1, "Too many logs requested. Max logs per response is 20000.")
    assert not is_range_error(-32601, "method not found")
    # The real reason can live in `data`: a -32602 "invalid params" whose data field
    # says the result cap was exceeded is a range error, not a malformed request.
    assert is_range_error(
        -32602, "invalid params", "Query returned more than 50000 results. Try [0x1, 0x2]"
    )
    assert not is_range_error(-32602, "invalid params")


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


class _AddrArrayRejector:
    """Rejects an eth_getLogs address ARRAY with -32602, accepts a single address."""

    def __init__(self) -> None:
        self.addresses: list = []

    async def request(self, method, url, *, params=None, json=None):
        addr = json["params"][0].get("address")
        self.addresses.append(addr)
        if isinstance(addr, list):
            return HttpResponse(200, {"jsonrpc": "2.0", "error": {"code": -32602, "message": "invalid params"}})
        return HttpResponse(200, {"jsonrpc": "2.0", "result": []})

    async def close(self) -> None:
        pass


@pytest.mark.asyncio
async def test_getlogs_falls_back_to_per_address_on_32602() -> None:
    transport = _AddrArrayRejector()
    client = RpcClient("https://rpc.example", "test", transport=transport)
    a, b = "0x" + "11" * 20, "0x" + "22" * 20
    logs = await client.get_logs(0, 100, [a, b])
    assert logs == []
    # first the rejected array, then one query per address
    assert transport.addresses == [[a, b], a, b]


@pytest.mark.asyncio
async def test_getlogs_result_limit_reported_in_data_is_range_error() -> None:
    # Provider caps results and reports it as -32602 "invalid params" with the reason in
    # `data`; the scanner relies on this being RpcRangeTooLarge to halve the range
    # instead of spinning on the same oversized call.
    body = {
        "jsonrpc": "2.0",
        "error": {
            "code": -32602,
            "message": "invalid params",
            "data": "Query returned more than 50000 results. Try with this block range [0x1, 0x2]",
        },
    }
    with pytest.raises(RpcRangeTooLarge):
        await _client(HttpResponse(200, body)).get_logs(0, 50000, ["0x" + "11" * 20])


@pytest.mark.asyncio
async def test_getlogs_plain_invalid_params_stays_rpc_error() -> None:
    # A -32602 with no range/result marker is a genuine bad request, not a range hint.
    body = {"jsonrpc": "2.0", "error": {"code": -32602, "message": "invalid params"}}
    with pytest.raises(RpcError) as exc:
        await _client(HttpResponse(200, body)).get_logs(0, 100, ["0x" + "11" * 20])
    assert not isinstance(exc.value, RpcRangeTooLarge)


@pytest.mark.asyncio
async def test_non_json_503_is_plain_rpc_error() -> None:
    with pytest.raises(RpcError) as exc:
        await _client(HttpResponse(503, None, text="service unavailable")).get_logs(
            0, 100, ["0x" + "11" * 20]
        )
    assert not isinstance(exc.value, RpcRangeTooLarge)
