"""Contract validation for an external verified-examples corpus.

The framework never stores, loads, retrieves, or serves examples. It takes a
batch of already-parsed ``VerifiedExample`` records and re-validates each one's
SQL against a ``DataContract`` using the same ``Validator`` that gates live agent
queries. See docs/superpowers/specs/2026-07-19-verified-examples-validation-design.md.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

_KNOWN_KEYS = frozenset({"sql", "question", "id", "principal", "metadata"})


@dataclass
class VerifiedExample:
    """One example to validate. Only ``sql`` is load-bearing."""

    sql: str
    question: str = ""
    id: str | None = None
    principal: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> VerifiedExample:
        """Map an already-parsed dict to a VerifiedExample.

        A shape adapter, not a loader: unknown keys are preserved under
        ``metadata`` (merged over an explicit ``metadata`` block) and never
        interpreted. ``sql`` is required.
        """
        if "sql" not in raw:
            raise ValueError("VerifiedExample requires a 'sql' field")
        metadata = dict(raw.get("metadata") or {})
        for key, value in raw.items():
            if key not in _KNOWN_KEYS:
                metadata[key] = value
        return cls(
            sql=raw["sql"],
            question=raw.get("question", ""),
            id=raw.get("id"),
            principal=raw.get("principal"),
            metadata=metadata,
        )
