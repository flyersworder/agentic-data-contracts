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


@dataclass
class ExampleResult:
    """The verdict for one example.

    ``status``:
      - ``"valid"``     — passed every layer that ran.
      - ``"violation"`` — a contract check or engine EXPLAIN rejected it.
      - ``"unchecked"`` — no layer could render a verdict (parse failed, no adapter).

    ``contract_checked`` is True only when the static checkers ran (needs a
    successful sqlglot parse). ``engine_checked`` is True when EXPLAIN ran
    (directly or via the decision-B fallback). A ``valid`` result with
    ``contract_checked is False`` is a decision-B pass: plannable, but contract
    policy was not statically verified.
    """

    example: VerifiedExample
    status: str
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    contract_checked: bool = False
    engine_checked: bool = False


@dataclass
class ExampleValidationReport:
    results: list[ExampleResult]

    @property
    def valid(self) -> list[ExampleResult]:
        return [r for r in self.results if r.status == "valid"]

    @property
    def violations(self) -> list[ExampleResult]:
        return [r for r in self.results if r.status == "violation"]

    @property
    def unchecked(self) -> list[ExampleResult]:
        return [r for r in self.results if r.status == "unchecked"]

    @property
    def unverified_compliance(self) -> list[ExampleResult]:
        """Valid results the engine planned but the contract could not check."""
        return [
            r for r in self.results if r.status == "valid" and not r.contract_checked
        ]

    @property
    def ok(self) -> bool:
        """True when no example is a contract/engine violation.

        ``unchecked`` results do not flip this — the caller decides whether an
        un-verifiable example should fail their gate.
        """
        return not self.violations

    def summary(self) -> str:
        """A compact markdown report, suitable for an MR comment.

        Iterates ``enumerate(self.results)`` once so each row can fall back to
        its positional index (``id → question → #index``, per the spec) — two
        unnamed rows never render identically.
        """

        def _label(result: ExampleResult, index: int) -> str:
            return result.example.id or result.example.question or f"#{index}"

        lines = [
            f"**Example validation:** {len(self.valid)} valid, "
            f"{len(self.violations)} violation(s), "
            f"{len(self.unchecked)} unchecked, "
            f"{len(self.unverified_compliance)} plannable-but-unverified.",
        ]
        for i, r in enumerate(self.results):
            if r.status == "violation":
                lines.append(f"- violation `{_label(r, i)}`: {'; '.join(r.reasons)}")
            elif r.status == "unchecked":
                lines.append(f"- unchecked `{_label(r, i)}`: {'; '.join(r.reasons)}")
            elif r.status == "valid" and not r.contract_checked:
                lines.append(f"- unverified `{_label(r, i)}`: {'; '.join(r.warnings)}")
        return "\n".join(lines)
