import json
from pathlib import Path

from eth_abi import encode

from cow_indexer.decoders.events import EventDecoder, event_topic
from cow_indexer.models import RpcLog

ROOT = Path(__file__).parents[2]


def topic_address(address: str) -> str:
    return "0x" + (b"\x00" * 12 + bytes.fromhex(address[2:])).hex()


def test_decodes_settlement_trade_event() -> None:
    abi = json.loads((ROOT / "abis" / "GPv2Settlement.json").read_text())
    trade = next(item for item in abi if item["name"] == "Trade")
    owner = "0x" + "11" * 20
    sell_token = "0x" + "22" * 20
    buy_token = "0x" + "33" * 20
    uid = b"\x44" * 56
    data = encode(
        ["address", "address", "uint256", "uint256", "uint256", "bytes"],
        [sell_token, buy_token, 100, 90, 2, uid],
    )
    log = RpcLog(
        address="0x9008d19f58aabd9ed0d60971565aa8510560ab41",
        topics=[event_topic(trade), topic_address(owner)],
        data="0x" + data.hex(),
        block_number=10,
        block_hash="0x" + "aa" * 32,
        transaction_hash="0x" + "bb" * 32,
        transaction_index=1,
        log_index=2,
    )
    name, values = EventDecoder(abi).decode(log) or (None, None)
    assert name == "Trade"
    assert values == {
        "owner": owner,
        "sellToken": sell_token,
        "buyToken": buy_token,
        "sellAmount": 100,
        "buyAmount": 90,
        "feeAmount": 2,
        "orderUid": "0x" + uid.hex(),
    }


def test_tuple_event_signature_uses_canonical_tuple_types() -> None:
    abi = json.loads((ROOT / "abis" / "ComposableCoW.json").read_text())
    event = next(item for item in abi if item["name"] == "ConditionalOrderCreated")
    assert event_topic(event).startswith("0x")
    assert len(event_topic(event)) == 66
