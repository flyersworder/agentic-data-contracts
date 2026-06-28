"""ARD publish side — build a spec-valid ai-catalog.json entry for a
contract-governed MCP server, with the frozen contract as a digest-pinned
``data-contract`` attestation in the trust manifest.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from agentic_data_contracts.ard import (
    build_ai_catalog,
    build_catalog_entry,
    contract_canonical_bytes,
    contract_digest,
)
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    SemanticConfig,
)
from agentic_data_contracts.validation.validator import Validator

_SEMANTIC_DEPENDENT_SQL = (
    "SELECT o.id, c.name FROM analytics.orders o"
    " JOIN analytics.customers c ON o.customer_id = c.id"
    " WHERE o.tenant_id = 'acme'"
)

PUBLISHER = "acme.com"
MCP_URL = "https://acme.com/.well-known/mcp/server-card.json"
CONTRACT_URL = "https://acme.com/contracts/revenue.json"


def _contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "roundtrip_contract.yml")


def _entry(fixtures_dir: Path, contract: DataContract | None = None) -> dict:
    return build_catalog_entry(
        contract if contract is not None else _contract(fixtures_dir),
        publisher_domain=PUBLISHER,
        mcp_card_url=MCP_URL,
        contract_url=CONTRACT_URL,
    )


def test_entry_has_required_ard_fields(fixtures_dir: Path) -> None:
    entry = _entry(fixtures_dir)
    # ARD requires identifier (URN), displayName, mediaType, and exactly one of
    # url / data.
    assert entry["identifier"] == "urn:air:acme.com:mcp:revenue-analysis-roundtrip"
    assert entry["displayName"]
    assert entry["mediaType"] == "application/mcp-server-card+json"
    assert entry["url"] == MCP_URL
    assert "data" not in entry  # url XOR data
    assert entry["tags"] == ["analytics"]  # derived from allowed-table schemas


def test_trust_manifest_identity_matches_identifier(fixtures_dir: Path) -> None:
    entry = _entry(fixtures_dir)
    # ARD: a Trust Manifest's identity MUST equal the containing entry's identifier.
    assert entry["trustManifest"]["identity"] == entry["identifier"]


def test_data_contract_attestation_digest_closes_the_loop(fixtures_dir: Path) -> None:
    """The attestation digest must be recomputable from the frozen contract bytes
    a consumer would fetch at contract_url — the publish→verify loop closes."""
    contract = _contract(fixtures_dir)
    entry = _entry(fixtures_dir, contract)
    att = entry["trustManifest"]["attestations"][0]
    assert att["type"] == "data-contract"
    assert att["uri"] == CONTRACT_URL
    # Independently recompute over the frozen, canonical bytes.
    expected = (
        "sha256:" + hashlib.sha256(contract_canonical_bytes(contract)).hexdigest()
    )
    assert att["digest"] == expected


def test_build_catalog_entry_freezes_contract(fixtures_dir: Path) -> None:
    """Publishing freezes the contract so the artifact you serve is self-contained."""
    contract = _contract(fixtures_dir)
    assert contract.schema.semantic.source is not None
    assert contract.schema.semantic.source.inline is None  # not yet frozen
    _entry(fixtures_dir, contract)
    assert contract.schema.semantic.source.inline is not None  # frozen as a side effect


def test_build_ai_catalog_wraps_entries(fixtures_dir: Path) -> None:
    entry = _entry(fixtures_dir)
    catalog = build_ai_catalog(
        [entry], host_display_name="Acme", host_identifier="did:web:acme.com"
    )
    assert catalog["specVersion"] == "1.0"
    assert catalog["host"] == {
        "displayName": "Acme",
        "identifier": "did:web:acme.com",
    }
    assert catalog["entries"] == [entry]


def test_publish_then_consumer_verifies_and_enforces(fixtures_dir: Path) -> None:
    """The full loop: a publisher builds an entry and serves the canonical bytes;
    a consumer holding ONLY those bytes verifies the digest and reconstructs
    identical, real enforcement — no trust in the publisher's assertion."""
    # ── Publisher ──
    publisher_contract = _contract(fixtures_dir)
    entry = _entry(fixtures_dir, publisher_contract)
    served_bytes = contract_canonical_bytes(
        publisher_contract
    )  # hosted at contract_url
    producer_result = Validator(
        publisher_contract, semantic_source=publisher_contract.load_semantic_source()
    ).validate(_SEMANTIC_DEPENDENT_SQL)

    # ── Consumer: holds the entry + the fetched bytes, nothing else ──
    attestation = entry["trustManifest"]["attestations"][0]
    recomputed = "sha256:" + hashlib.sha256(served_bytes).hexdigest()
    assert recomputed == attestation["digest"]  # integrity verified, issuer-free
    consumer_contract = DataContract.from_yaml_string(served_bytes.decode())
    consumer_result = Validator(
        consumer_contract, semantic_source=consumer_contract.load_semantic_source()
    ).validate(_SEMANTIC_DEPENDENT_SQL)

    # Enforcement is identical — and actually happened (the relationship warning).
    assert consumer_result.warnings == producer_result.warnings
    assert any("status" in w for w in consumer_result.warnings)


def test_digest_is_independent_of_source_path(fixtures_dir: Path) -> None:
    """Two contracts with identical semantics but different `path` strings
    (relative vs absolute to the same file) must content-address identically once
    frozen — the path is machine-specific and must not enter the digest."""
    a = DataContract.from_yaml(fixtures_dir / "roundtrip_contract.yml")
    b = DataContract.from_yaml(fixtures_dir / "roundtrip_contract.yml")
    src_b = b.schema.semantic.source
    assert src_b is not None
    src_b.path = str(fixtures_dir / "relationships_checker.yml")  # absolute, same file
    b._source_dir = None
    assert contract_digest(a) == contract_digest(b)


def test_canonical_bytes_use_documented_yaml_aliases(fixtures_dir: Path) -> None:
    """Review #4: the published, content-addressed bytes must use the documented
    YAML keys (``schema``), not pydantic field names (``schema_``), so a non-library
    consumer reads the same shape the contract was authored in."""
    contract = DataContract.from_yaml(fixtures_dir / "roundtrip_contract.yml")
    raw = contract_canonical_bytes(contract)
    assert b'"schema"' in raw
    assert b'"schema_"' not in raw


def test_tags_include_wildcard_schemas() -> None:
    """A wildcard-table schema has a real surface but no resolved table names; its
    schema should still tag the entry so the server is discoverable."""
    contract = DataContract(
        DataContractSchema(
            name="wild",
            semantic=SemanticConfig(
                allowed_tables=[AllowedTable(schema="warehouse", tables=["*"])],
            ),
        )
    )
    entry = build_catalog_entry(
        contract,
        publisher_domain="acme.com",
        mcp_card_url="https://acme.com/mcp.json",
        contract_url="https://acme.com/c.json",
    )
    assert "warehouse" in entry["tags"]
