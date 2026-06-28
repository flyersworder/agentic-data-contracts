"""Agentic Resource Discovery (ARD) publish side.

Turns a :class:`~agentic_data_contracts.core.contract.DataContract` into a
spec-valid ``ai-catalog.json`` entry so the *contract-governed* tool surface is
discoverable across ARD registries (GitHub Agent Finder, Google Agent Registry,
Hugging Face Discover) without taking a dependency on any of them.

Design (matching the AI Catalog data model, ``agenticresourcediscovery.org``):

* The entry's artifact is the governed **MCP server** — ``mediaType``
  ``application/mcp-server-card+json``, referenced by ``url``.
* The data contract rides **alongside** as a ``data-contract`` attestation in the
  ``trustManifest`` (the trust manifest sits beside the artifact, it does not wrap
  it), digest-pinned to the contract's frozen, canonical bytes. A consumer fetches
  the contract, recomputes the digest, and rebuilds enforcement — the publish→verify
  loop closes with no trust in the publisher's assertion.

ARD does publisher *authentication*; the ``data-contract`` attestation is the hook
for per-operation *authorization* that ARD itself leaves open. ``data-contract`` is
a custom attestation type (the spec's ``attestations[].type`` is an open string),
not yet a registered well-known value.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from agentic_data_contracts.core.contract import DataContract

#: IANA media type for an MCP Server Card (the governed tool surface).
MCP_SERVER_CARD_MEDIA_TYPE = "application/mcp-server-card+json"
#: Custom attestation type carrying the governing data contract. The ARD
#: ``attestations[].type`` field is an open string; this is not (yet) a
#: registered well-known value.
DATA_CONTRACT_ATTESTATION_TYPE = "data-contract"


def contract_canonical_bytes(contract: DataContract) -> bytes:
    """The frozen, canonical JSON bytes of a contract — the portable artifact you
    publish and content-address.

    Freezes the semantic source first (idempotent, see
    :meth:`DataContract.freeze_semantic_source`) so the bytes are self-contained,
    then dumps the validated schema as sorted-key JSON (stable across YAML
    formatting and dict ordering).
    """
    contract.freeze_semantic_source()
    # by_alias so the published, content-addressed bytes use the documented YAML
    # keys (e.g. ``schema``), not pydantic field names (``schema_``) — a non-library
    # consumer reads the same shape the contract was authored in.
    payload = contract.schema.model_dump(mode="json", by_alias=True)
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def contract_digest(contract: DataContract) -> str:
    """``sha256:<hex>`` content address over :func:`contract_canonical_bytes`."""
    return "sha256:" + hashlib.sha256(contract_canonical_bytes(contract)).hexdigest()


def build_catalog_entry(
    contract: DataContract,
    *,
    publisher_domain: str,
    mcp_card_url: str,
    contract_url: str,
    description: str | None = None,
) -> dict[str, Any]:
    """Build an ARD catalog entry for a contract-governed MCP server.

    Args:
        contract: The data contract governing the tool surface. **Frozen in place**
            (idempotent) so the published digest is over the portable artifact;
            serve :func:`contract_canonical_bytes` at *contract_url*.
        publisher_domain: Trust-anchoring domain, e.g. ``"acme.com"``.
        mcp_card_url: URL where the governed MCP server card is served.
        contract_url: URL where the frozen contract bytes are served.
        description: Optional human description for the entry.

    Returns:
        A JSON-serializable ARD catalog entry.
    """
    digest = contract_digest(contract)  # freezes the contract (idempotent)
    identifier = f"urn:air:{publisher_domain}:mcp:{contract.name}"

    entry: dict[str, Any] = {
        "identifier": identifier,
        "displayName": f"{contract.name} (contract-governed)",
        "mediaType": MCP_SERVER_CARD_MEDIA_TYPE,
        "url": mcp_card_url,
        "version": contract.schema.version,
        "trustManifest": {
            # ARD: a Trust Manifest's identity MUST equal the entry's identifier.
            "identity": identifier,
            "attestations": [
                {
                    "type": DATA_CONTRACT_ATTESTATION_TYPE,
                    "uri": contract_url,
                    "mediaType": "application/json",
                    "digest": digest,
                }
            ],
        },
    }
    if description is not None:
        entry["description"] = description
    # Tag by schema name, read structurally from the allowed-table entries (not
    # re-split from qualified names) so wildcard-table schemas — which expose a
    # real surface but have no resolved table names — still tag the entry.
    tags = sorted(
        {t.schema_ for t in contract.schema.semantic.allowed_tables if t.tables}
    )
    if tags:
        entry["tags"] = tags
    return entry


def build_ai_catalog(
    entries: Iterable[dict[str, Any]],
    *,
    host_display_name: str,
    host_identifier: str | None = None,
) -> dict[str, Any]:
    """Assemble a top-level ``ai-catalog.json`` document from catalog entries.

    Serve the result at ``/.well-known/ai-catalog.json`` for automated discovery.
    """
    host: dict[str, Any] = {"displayName": host_display_name}
    if host_identifier is not None:
        host["identifier"] = host_identifier
    return {
        "specVersion": "1.0",
        "host": host,
        "entries": list(entries),
    }
