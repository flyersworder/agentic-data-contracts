"""Contract validation for an external verified-examples corpus.

The framework never stores, loads, retrieves, or serves examples. It takes a
batch of already-parsed ``VerifiedExample`` records and re-validates each one's
SQL against a ``DataContract`` using the same ``Validator`` that gates live agent
queries. See the "Validating a verified-examples corpus" section of the README
for usage and the boundary rationale.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from agentic_data_contracts.adapters._normalizer import SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.explain import ExplainAdapter
from agentic_data_contracts.validation.validator import ValidationResult, Validator

if TYPE_CHECKING:
    from agentic_data_contracts.semantic.base import SemanticSource

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

    ``status`` (each result has exactly one):
      - ``"valid"``      — statically contract-checked *and* passed (plus the
                           EXPLAIN dry-run, if an adapter was given).
      - ``"violation"``  — a contract check or engine EXPLAIN rejected it.
      - ``"unverified"`` — decision-B: the engine planned it (sqlglot could not
                           parse the SQL), but contract policy was NOT statically
                           checked, so it is neither vouched-valid nor rejected.
      - ``"unchecked"``  — no verdict was possible (parse failed with no adapter,
                           or the adapter raised while planning).

    ``contract_checked`` is True only when the static checkers ran (needs a
    successful sqlglot parse). ``engine_checked`` is True when EXPLAIN ran. So a
    trusted pass is ``status == "valid"``; ``"unverified"`` rows are plannable
    but require human judgement.
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
        """Decision-B passes: the engine planned them, contract policy unchecked."""
        return [r for r in self.results if r.status == "unverified"]

    @property
    def ok(self) -> bool:
        """True only when every example was contract-checked and passed.

        Safe as a CI gate — ``if not report.ok: sys.exit(1)``. It is False if ANY
        example is a ``violation``, is ``unchecked`` (no verdict could be
        rendered), or is ``unverified`` (the engine planned it but contract
        policy was never statically checked). A laxer view — e.g. fail only on
        hard violations — can test ``report.violations`` directly.
        """
        return not (self.violations or self.unchecked or self.unverified_compliance)

    def summary(self) -> str:
        """A compact markdown report, suitable for an MR comment.

        Iterates ``enumerate(self.results)`` once so each row can fall back to
        its positional index (``id → question → #index``, per the spec) — two
        unnamed rows never render identically.
        """

        def _label(result: ExampleResult, index: int) -> str:
            return result.example.id or result.example.question or f"#{index}"

        # Each result has exactly one status, so the four counts sum to the total.
        lines = [
            f"**Example validation:** {len(self.valid)} valid, "
            f"{len(self.violations)} violation(s), "
            f"{len(self.unverified_compliance)} unverified, "
            f"{len(self.unchecked)} unchecked.",
        ]
        for i, r in enumerate(self.results):
            if r.status == "violation":
                lines.append(f"- violation `{_label(r, i)}`: {'; '.join(r.reasons)}")
            elif r.status == "unverified":
                lines.append(f"- unverified `{_label(r, i)}`: {'; '.join(r.warnings)}")
            elif r.status == "unchecked":
                lines.append(f"- unchecked `{_label(r, i)}`: {'; '.join(r.reasons)}")
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
        try:
            vr = _validator_for(example.principal).validate(example.sql)
            results.append(_to_result(example, vr, explain_adapter))
        except Exception as exc:  # noqa: BLE001 — batch resilience
            # A misbehaving adapter (a Layer-2 or decision-B EXPLAIN that raises
            # instead of returning schema_valid=False) or any other unexpected
            # error degrades THIS example to "unchecked" — one bad example must
            # never abort validation of the rest of the corpus.
            results.append(
                ExampleResult(
                    example=example,
                    status="unchecked",
                    reasons=[f"validation error: {exc}"],
                )
            )
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
    # parser — ask it directly. Verifies plannability, NOT contract policy. A
    # raise here (a thin adapter that does not wrap driver errors) is caught by
    # validate_examples' per-example guard and degraded to "unchecked".
    explain_result = explain_adapter.explain(example.sql)
    if explain_result.schema_valid:
        # Engine vouches for plannability, but the static contract checks never
        # ran (no AST) — its own status, never counted as a trusted "valid".
        return ExampleResult(
            example=example,
            status="unverified",
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
