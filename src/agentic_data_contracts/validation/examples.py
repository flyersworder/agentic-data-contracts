"""Contract validation for an external verified-examples corpus.

The framework never stores, loads, retrieves, or serves examples. It takes a
batch of already-parsed ``VerifiedExample`` records and re-validates each one's
SQL against a ``DataContract`` using the same ``Validator`` that gates live agent
queries. See docs/superpowers/specs/2026-07-19-verified-examples-validation-design.md.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from agentic_data_contracts.adapters._normalizer import SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.validation.explain import ExplainAdapter
from agentic_data_contracts.validation.validator import ValidationResult, Validator

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


_PARSE_FALLBACK_CAVEAT = (
    "contract policy not statically verified "
    "(sqlglot could not parse; engine confirmed plannability only)"
)


def validate_examples(
    examples: Iterable[VerifiedExample],
    contract: DataContract,
    *,
    dialect: str | None = None,
    sql_normalizer: SqlNormalizer | None = None,
    explain_adapter: ExplainAdapter | None = None,
    semantic_source: SemanticSource | None = None,
) -> ExampleValidationReport:
    """Validate each example's SQL against *contract* via the live Validator.

    One Validator is built per distinct ``example.principal`` (cheap; few
    principals per corpus) so per-principal rules are checked under the right
    identity. Layer 1 always runs; Layer 2 (EXPLAIN) runs when *explain_adapter*
    is given; on a sqlglot parse failure with an adapter present, the engine is
    asked directly (decision B). Input order is preserved.
    """
    validators: dict[str | None, Validator] = {}

    def _validator_for(principal: str | None) -> Validator:
        if principal not in validators:
            validators[principal] = Validator(
                contract,
                dialect=dialect,
                explain_adapter=explain_adapter,
                sql_normalizer=sql_normalizer,
                semantic_source=semantic_source,
                caller_principal=principal,
            )
        return validators[principal]

    results: list[ExampleResult] = []
    for example in examples:
        vr = _validator_for(example.principal).validate(example.sql)
        results.append(_to_result(example, vr, explain_adapter))
    return ExampleValidationReport(results=results)


def _to_result(
    example: VerifiedExample,
    vr: ValidationResult,
    explain_adapter: ExplainAdapter | None,
) -> ExampleResult:
    if not vr.parse_error:
        # Static checkers ran (we have an AST).
        if not vr.blocked:
            return ExampleResult(
                example=example,
                status="valid",
                warnings=list(vr.warnings),
                contract_checked=True,
                # A non-blocked result with an adapter means EXPLAIN ran and
                # passed (validate() runs it whenever there are no static reasons).
                engine_checked=explain_adapter is not None,
            )
        return ExampleResult(
            example=example,
            status="violation",
            reasons=list(vr.reasons),
            warnings=list(vr.warnings),
            contract_checked=True,
            # EXPLAIN ran iff there was no *static* reason at the gate. A static
            # block leaves schema_valid True and estimated_* None (EXPLAIN was
            # skipped); an EXPLAIN-caused block always sets one of them —
            # schema_valid False (schema reject), or a non-None estimate (the
            # cost/row-limit checks only fire when their estimate is present).
            engine_checked=explain_adapter is not None
            and (
                not vr.schema_valid
                or vr.estimated_cost_usd is not None
                or vr.estimated_rows is not None
            ),
        )

    # Parse failure — no AST, so no contract check possible.
    if explain_adapter is None:
        return ExampleResult(
            example=example,
            status="unchecked",
            reasons=list(vr.reasons),
            contract_checked=False,
            engine_checked=False,
        )

    # Decision B: sqlglot cannot parse it, but the engine is the authoritative
    # parser — ask it directly. Verifies plannability, NOT contract policy.
    explain_result = explain_adapter.explain(example.sql)
    if explain_result.schema_valid:
        return ExampleResult(
            example=example,
            status="valid",
            warnings=[_PARSE_FALLBACK_CAVEAT],
            contract_checked=False,
            engine_checked=True,
        )
    return ExampleResult(
        example=example,
        status="violation",
        reasons=[
            f"Engine rejected (parse-fallback): {', '.join(explain_result.errors)}"
        ],
        contract_checked=False,
        engine_checked=True,
    )
