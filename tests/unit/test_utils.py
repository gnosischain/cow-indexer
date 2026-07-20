from cow_indexer.utils import (
    batched,
    normalize_address,
    normalize_auction_order,
    validate_order_uid,
)


def make_uid(owner: str, valid_to: int) -> str:
    return "0x" + (b"\x11" * 32 + bytes.fromhex(owner[2:]) + valid_to.to_bytes(4, "big")).hex()


def test_order_uid_validates_embedded_owner_and_expiry() -> None:
    owner = "0x" + "ab" * 20
    uid = make_uid(owner, 1_700_000_000)
    assert validate_order_uid(uid.upper().replace("0X", "0x"), owner, 1_700_000_000) == uid


def test_order_uid_rejects_wrong_owner() -> None:
    owner = "0x" + "ab" * 20
    uid = make_uid(owner, 42)
    try:
        validate_order_uid(uid, "0x" + "cd" * 20, 42)
    except ValueError as exc:
        assert "owner suffix" in str(exc)
    else:
        raise AssertionError("wrong owner was accepted")


def test_batching_and_address_normalization() -> None:
    assert list(batched(range(5), 2)) == [[0, 1], [2, 3], [4]]
    assert normalize_address("AB" * 20) == "0x" + "ab" * 20


def test_auction_orders_accept_uid_strings_and_expanded_objects() -> None:
    uid = "0x" + "11" * 56
    assert normalize_auction_order(uid) == (uid, {"uid": uid})
    assert normalize_auction_order({"orderUid": uid, "kind": "sell"}) == (
        uid,
        {"orderUid": uid, "kind": "sell"},
    )
    assert normalize_auction_order(42) is None
    assert normalize_auction_order("0xdead") is None
