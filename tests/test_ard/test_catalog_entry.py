"""ARD publish side — build a spec-valid ai-catalog.json entry for a
contract-governed MCP server, with the frozen contract as a digest-pinned
``data-contract`` attestation in the trust manifest.
"""

from __future__ import annotations

import hashlib
import json
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
from agentic_data_contracts.core.schema import SemanticSource as SemanticSourceConfig
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


def _contract_with_inline_semantics(inline: dict) -> DataContract:
    """A ``DataContract`` whose semantic source is already frozen inline.

    No filesystem, no ``tmp_path`` — that is the point of ``inline``: it is
    the self-contained snapshot ``freeze_semantic_source`` would otherwise
    produce from a file. Mirrors the inline-construction style already used
    by ``test_tags_include_wildcard_schemas`` below.
    """
    return DataContract(
        DataContractSchema(
            name="inline-semantics",
            semantic=SemanticConfig(
                source=SemanticSourceConfig(type="yaml", inline=inline),
            ),
        )
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


# A digest published against this fixture. Hardcoded on purpose: comparing two
# freshly-built contracts to each other passes just as happily when *both* have
# shifted, which is precisely the failure a new schema field causes.
#
# Re-pinned once, in the commit that fixed #73. The old value was computed over a
# semantic source with no `metrics:` key at all, so it could not see any change
# to metric serialization — every optional field added to `_dump_metric` since it
# was written could have been made unconditional with this test still green, while
# moving the digest of every real contract. The fixture now carries metrics
# exercising each of those fields, and the value below is the digest over them.
#
# Changing this constant is the one edit that can launder a real regression into
# the baseline. Any commit that touches it must show the fixture change beside it
# and say why the new value is expected.
_PINNED_ROUNDTRIP_DIGEST = (
    "sha256:d5aac3e3b76c8ff2458d8388c19de0111c58e9a5fd376f9cebe7c76220d793f8"
)


def test_roundtrip_contract_digest_is_pinned(fixtures_dir: Path) -> None:
    """Any new field that serializes moves every published ARD attestation."""
    assert contract_digest(_contract(fixtures_dir)) == _PINNED_ROUNDTRIP_DIGEST


def test_pinned_contract_actually_exercises_metric_serialization(
    fixtures_dir: Path,
) -> None:
    """Guard on the guard: the pin is only broad if the fixture has metrics.

    This is the regression test for #73. The pin spent its whole life over a
    semantic source with no ``metrics:`` key, so it silently covered nothing in
    ``_dump_metric`` — a state indistinguishable from working, because the
    assertion passed either way. Anyone who strips the metrics back out fails
    here rather than quietly narrowing the pin to relationships again.

    Asserting on the *canonical bytes* rather than the loaded source is
    deliberate: what matters is that these fields reach the digest, not merely
    that the fixture declares them.
    """
    payload = json.loads(contract_canonical_bytes(_contract(fixtures_dir)))
    metrics = payload["semantic"]["source"]["inline"]["metrics"]
    assert metrics, "pinned fixture declares no metrics — the pin covers nothing"

    parent = next(m for m in metrics if m["name"] == "roundtrip_revenue")
    # Every optional field `_dump_metric` writes, so making any of them
    # unconditional moves the pin instead of passing unnoticed.
    for field in (
        "source_model",
        "filters",
        "domains",
        "tier",
        "indicator_kind",
        "business_owner",
        "operational_owner",
        "last_reviewed",
        "decompositions",
        "drill_by",
    ):
        assert field in parent, f"pinned fixture no longer exercises {field!r}"
    assert parent["decompositions"][0]["convention"] == "fold_into"

    # And a leaf metric, so the omit-when-empty branches are exercised too: a
    # regression that emits `"decompositions": []` for leaves must move the pin.
    leaf = next(m for m in metrics if m["name"] == "roundtrip_arpu")
    assert "decompositions" not in leaf
    assert "drill_by" not in leaf


def test_expected_extras_is_not_digest_bearing(fixtures_dir: Path) -> None:
    """``expected_extras`` is a load-time lint policy, not semantics.

    It does not change what the agent sees, so declaring it must leave the
    content address exactly where it was — including the empty-list (strict)
    form, which is a real, meaningful value rather than an absent one.
    """
    for declared in ([], ["column_hints", "join_paths"]):
        contract = _contract(fixtures_dir)
        source = contract.schema.semantic.source
        assert source is not None
        source.expected_extras = declared
        assert contract_digest(contract) == _PINNED_ROUNDTRIP_DIGEST


def test_expected_extras_is_absent_from_the_canonical_bytes(fixtures_dir: Path) -> None:
    """The key itself must not appear — not even as ``null``."""
    contract = _contract(fixtures_dir)
    assert b"expected_extras" not in contract_canonical_bytes(contract)


def test_extras_change_the_contract_digest() -> None:
    """Extras are resident prompt text, so they belong to the contract identity.

    Routes both inline payloads through ``dump_semantic_source`` — the same
    function ``freeze_semantic_source`` calls — rather than hand-rolling the
    ``inline`` dicts directly. A hand-rolled pair of differing dicts would
    make the digests differ regardless of whether ``dump_semantic_source``
    itself carries extras, which would not actually exercise the behaviour
    this test exists to protect: that freezing a source with extras (as
    opposed to constructing ``inline=`` by hand) moves the digest.
    """
    from agentic_data_contracts.ard import contract_digest
    from agentic_data_contracts.semantic.base import dump_semantic_source
    from agentic_data_contracts.semantic.yaml_source import YamlSource

    hints = [{"table": "analytics.orders", "prefer": "order_total", "over": "total"}]
    base_inline = dump_semantic_source(YamlSource.from_raw({"metrics": []}))
    hinted_inline = dump_semantic_source(
        YamlSource.from_raw({"metrics": [], "column_hints": hints})
    )
    base = _contract_with_inline_semantics(base_inline)
    hinted = _contract_with_inline_semantics(hinted_inline)
    assert contract_digest(base) != contract_digest(hinted)


def test_contract_without_convention_keeps_stable_canonical_bytes(
    tmp_path: Path,
) -> None:
    # A metric declaring a decomposition but no convention must serialize
    # exactly as it did pre-0.43, or every published ARD attestation moves.
    semantic = tmp_path / "semantic.yml"
    semantic.write_text(
        "metrics:\n"
        "  - name: activations\n"
        "    decompositions:\n"
        "      - operator: product\n"
        "        operands: [volume, rate]\n"
        "  - name: volume\n"
        "  - name: rate\n"
    )
    contract_file = tmp_path / "contract.yml"
    contract_file.write_text(
        f"name: t\nsemantic:\n  source:\n    type: yaml\n    path: {semantic}\n"
    )
    contract = DataContract.from_yaml(str(contract_file))
    payload = json.loads(contract_canonical_bytes(contract))
    decomp = payload["semantic"]["source"]["inline"]["metrics"][0]["decompositions"][0]
    assert decomp == {"operator": "product", "operands": ["volume", "rate"]}
