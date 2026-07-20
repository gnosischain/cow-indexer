from pathlib import Path

from cow_indexer.config import load_config

ROOT = Path(__file__).parents[2]


def test_all_configured_chains_have_valid_deployments() -> None:
    config = load_config(ROOT / "config" / "chains.yaml")
    assert len(config.chains) == 11
    assert {chain.chain_id for chain in config.chains} >= {1, 100, 42161, 11155111}
    for chain in config.chains:
        assert chain.deployment is not None
        assert chain.deployment.chain_id == chain.chain_id
        assert chain.deployment.contracts
        for contract in chain.deployment.contracts:
            assert contract.abi.is_file()


def test_select_rejects_unknown_chain() -> None:
    config = load_config(ROOT / "config" / "chains.yaml")
    try:
        config.select("not-a-chain")
    except ValueError as exc:
        assert "unknown or disabled" in str(exc)
    else:
        raise AssertionError("unknown chain was accepted")
