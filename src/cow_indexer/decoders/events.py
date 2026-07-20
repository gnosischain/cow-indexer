from __future__ import annotations

from typing import Any

from eth_abi import decode
from eth_utils import keccak

from cow_indexer.config import Deployment, load_abi
from cow_indexer.models import DecodedEvent, RpcLog
from cow_indexer.utils import normalize_address


def canonical_type(item: dict[str, Any]) -> str:
    abi_type = item["type"]
    if not abi_type.startswith("tuple"):
        return abi_type
    suffix = abi_type[len("tuple") :]
    components = ",".join(canonical_type(component) for component in item["components"])
    return f"({components}){suffix}"


def event_signature(event: dict[str, Any]) -> str:
    types = ",".join(canonical_type(item) for item in event["inputs"])
    return f"{event['name']}({types})"


def event_topic(event: dict[str, Any]) -> str:
    return f"0x{keccak(text=event_signature(event)).hex()}"


def _normalize_value(item: dict[str, Any], value: Any) -> Any:
    abi_type = item["type"]
    if abi_type.startswith("tuple"):
        components = item["components"]
        if abi_type.endswith("[]"):
            return [
                {
                    component["name"]: _normalize_value(component, part)
                    for component, part in zip(components, row, strict=True)
                }
                for row in value
            ]
        return {
            component["name"]: _normalize_value(component, part)
            for component, part in zip(components, value, strict=True)
        }
    if abi_type == "address":
        return normalize_address(value)
    if isinstance(value, bytes):
        return f"0x{value.hex()}"
    if isinstance(value, tuple):
        return list(value)
    return value


class EventDecoder:
    def __init__(self, abi: list[dict[str, Any]]) -> None:
        events = [item for item in abi if item.get("type") == "event" and not item.get("anonymous")]
        self.events = {event_topic(event): event for event in events}

    def decode(self, log: RpcLog) -> tuple[str, dict[str, Any]] | None:
        if not log.topics:
            return None
        event = self.events.get(log.topics[0].lower())
        if event is None:
            return None
        indexed = [item for item in event["inputs"] if item.get("indexed")]
        unindexed = [item for item in event["inputs"] if not item.get("indexed")]
        if len(log.topics) - 1 != len(indexed):
            raise ValueError(f"wrong topic count for {event['name']}")

        values: dict[str, Any] = {}
        for item, topic in zip(indexed, log.topics[1:], strict=True):
            abi_type = canonical_type(item)
            if abi_type in {"bytes", "string"} or abi_type.endswith("[]"):
                values[item["name"]] = topic.lower()
            else:
                decoded = decode([abi_type], bytes.fromhex(topic.removeprefix("0x")))[0]
                values[item["name"]] = _normalize_value(item, decoded)

        if unindexed:
            payload = bytes.fromhex(log.data.removeprefix("0x"))
            decoded_values = decode([canonical_type(item) for item in unindexed], payload)
            for item, value in zip(unindexed, decoded_values, strict=True):
                values[item["name"]] = _normalize_value(item, value)
        return event["name"], values


class MultiContractDecoder:
    def __init__(self, deployment: Deployment, environment: str) -> None:
        self.environment = environment
        self.chain_id = deployment.chain_id
        self.contracts = {
            contract.address: (contract.name, EventDecoder(load_abi(contract.abi)))
            for contract in deployment.contracts
        }

    def decode(self, log: RpcLog) -> DecodedEvent | None:
        registered = self.contracts.get(log.address)
        if registered is None:
            return None
        contract_name, decoder = registered
        decoded = decoder.decode(log)
        if decoded is None:
            return None
        event_name, args = decoded
        return DecodedEvent(
            environment=self.environment,
            chain_id=self.chain_id,
            contract_name=contract_name,
            contract_address=log.address,
            event_name=event_name,
            args=args,
            block_number=log.block_number,
            block_hash=log.block_hash,
            transaction_hash=log.transaction_hash,
            transaction_index=log.transaction_index,
            log_index=log.log_index,
            removed=log.removed,
        )
