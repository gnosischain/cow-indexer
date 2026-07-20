from cow_indexer.sources.rpc import is_range_error


def test_detects_common_log_range_errors() -> None:
    assert is_range_error(-32005, "anything")
    assert is_range_error(-1, "query returned more than 10000 results")
    assert is_range_error(-1, "please limit the block range")
    assert not is_range_error(-32601, "method not found")
