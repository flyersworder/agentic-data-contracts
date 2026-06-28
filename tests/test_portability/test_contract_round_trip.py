"""Portability — does contract enforcement survive a serialize→ship→rehydrate
round-trip?

Matters independently of ARD: an analytics agent almost always runs in a
different process / container than where its contract was authored. Two specs:

* A **frozen** contract (semantics snapshotted inline) IS portable — serialize →
  rehydrate preserves relationship/metric enforcement with no filesystem access.
* An **unfrozen** contract that still references its semantic source by path is
  NOT portable, and must fail closed (Path A) rather than silently under-enforce.

Modelled flow::

    producer  : load contract, freeze its semantic source (authoring box)
    ship      : serialize the contract to canonical bytes + a SHA-256 content address
    consumer  : holding ONLY those bytes, rehydrate and re-enforce
    property  : identical verdicts (blocked + warnings) on both sides
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import (
    DataContract,
    SemanticSourceUnavailableError,
)
from agentic_data_contracts.validation.validator import Validator

# A query whose verdict depends on the semantic source: joining orders -> customers
# without the relationship's required_filter ("status != 'cancelled'") makes the
# producer emit a warning (mirrors test_validator.py:412). The table allowlist and
# rules (tenant_id filter present, explicit columns) are satisfied, so it is never
# *blocked* — the only enforcement signal is the semantic-source-derived warning.
SEMANTIC_DEPENDENT_SQL = (
    "SELECT o.id, c.name FROM analytics.orders o"
    " JOIN analytics.customers c ON o.customer_id = c.id"
    " WHERE o.tenant_id = 'acme'"
)


def _canonical_bytes(contract: DataContract) -> bytes:
    """Deterministic content of a contract: sorted-key JSON over the validated
    schema, independent of YAML formatting / comments. The basis for a content
    address (sha256) an ARD ``data-contract`` attestation would carry."""
    payload = contract.schema.model_dump(mode="json")
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def test_frozen_contract_enforcement_survives_round_trip(fixtures_dir: Path) -> None:
    # ── Producer: load + FREEZE (snapshot semantics inline) on the authoring box ──
    producer = DataContract.from_yaml(fixtures_dir / "roundtrip_contract.yml")
    producer.freeze_semantic_source()
    producer_validator = Validator(
        producer, semantic_source=producer.load_semantic_source()
    )
    producer_result = producer_validator.validate(SEMANTIC_DEPENDENT_SQL)
    # Guard: the producer must actually exercise the semantic source, otherwise the
    # round-trip below would pass vacuously.
    assert any("status" in w for w in producer_result.warnings), (
        "guard: producer must exercise semantic-source enforcement"
    )

    # ── Ship: serialize the FROZEN contract (now self-contained) ──
    canonical = _canonical_bytes(producer)
    digest = hashlib.sha256(canonical).hexdigest()

    # ── Consumer: holds ONLY the bytes — no filesystem access to the source ──
    assert hashlib.sha256(canonical).hexdigest() == digest  # integrity check
    consumer = DataContract.from_yaml_string(canonical.decode())
    consumer_validator = Validator(
        consumer, semantic_source=consumer.load_semantic_source()
    )
    consumer_result = consumer_validator.validate(SEMANTIC_DEPENDENT_SQL)

    # ── The property: identical enforcement on both sides ──
    assert consumer_result.blocked == producer_result.blocked
    assert consumer_result.warnings == producer_result.warnings


def test_unfrozen_contract_round_trip_fails_closed(fixtures_dir: Path) -> None:
    """Without freezing, the serialized contract still references its semantic
    source by path; a consumer cannot resolve it and must fail closed (Path A),
    never silently under-enforce."""
    producer = DataContract.from_yaml(fixtures_dir / "roundtrip_contract.yml")
    canonical = _canonical_bytes(producer)
    consumer = DataContract.from_yaml_string(canonical.decode())
    with pytest.raises(SemanticSourceUnavailableError):
        consumer.load_semantic_source()
