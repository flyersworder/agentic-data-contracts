"""Contract validation for an external verified-examples corpus.

The framework never stores, loads, retrieves, or serves examples. It takes a
batch of already-parsed ``VerifiedExample`` records and re-validates each one's
SQL against a ``DataContract`` using the same ``Validator`` that gates live agent
queries. See the "Validating a verified-examples corpus" section of the README
for usage and the boundary rationale.
"""

from __future__ import annotations

import math
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from agentic_data_contracts.adapters._normalizer import SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.explain import ExplainAdapter
from agentic_data_contracts.validation.validator import ValidationResult, Validator

if TYPE_CHECKING:
    from agentic_data_contracts.semantic.base import SemanticSource

_KNOWN_KEYS = frozenset(
    {
        "sql",
        "question",
        "id",
        "principal",
        "metadata",
        "expected",
        "rel_tol",
        "abs_tol",
        "time_scoped",
    }
)


def _numeric(raw: Any, field_name: str, *, allow_negative: bool = True) -> float | None:
    """Validate one optional numeric field from an untrusted corpus row.

    ``bool`` is rejected explicitly: it is an ``int`` subclass, so
    ``expected: true`` would otherwise pass an isinstance check and silently
    assert against ``1.0``.
    """
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"'{field_name}' must be a number, got {type(raw).__name__}")
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError(f"'{field_name}' must be finite, got {value}")
    if not allow_negative and value < 0:
        raise ValueError(f"'{field_name}' must be non-negative, got {value}")
    return value


@dataclass
class VerifiedExample:
    """One example to validate. Only ``sql`` is load-bearing.

    An example that sets ``expected`` is additionally an *assertion*: the
    certified answer its SQL must return, checked by ``check_example_answers``.
    ``rel_tol`` / ``abs_tol`` override the call-level tolerances for this row
    alone; ``time_scoped`` is the author's assertion that the query's time
    window is pinned, which suppresses the relative-time-window refusal.
    """

    sql: str
    question: str = ""
    id: str | None = None
    principal: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    expected: float | None = None
    rel_tol: float | None = None
    abs_tol: float | None = None
    time_scoped: bool = False

    @classmethod
    def from_dict(cls, raw: Any) -> VerifiedExample:
        """Map an already-parsed mapping to a VerifiedExample.

        A shape adapter, not a loader: unknown keys are preserved under
        ``metadata`` (merged over an explicit ``metadata`` mapping) and never
        interpreted. ``sql`` is required. A malformed corpus row — not a mapping,
        missing ``sql``, or a non-mapping ``metadata`` — raises ``ValueError``
        with an actionable message, rather than crashing with a cryptic
        ``TypeError`` / ``ValueError`` deeper in (external YAML is untrusted, so
        ``raw`` is typed ``Any`` and validated at runtime).
        """
        if not isinstance(raw, dict):
            raise ValueError(
                f"VerifiedExample.from_dict expects a mapping, got {type(raw).__name__}"
            )
        if "sql" not in raw:
            raise ValueError("VerifiedExample requires a 'sql' field")
        meta = raw.get("metadata")
        if meta is not None and not isinstance(meta, dict):
            raise ValueError(f"'metadata' must be a mapping, got {type(meta).__name__}")
        metadata = dict(meta or {})
        for key, value in raw.items():
            if key not in _KNOWN_KEYS:
                metadata[key] = value
        time_scoped = raw.get("time_scoped", False)
        if not isinstance(time_scoped, bool):
            raise ValueError(
                f"'time_scoped' must be a boolean, got {type(time_scoped).__name__}"
            )
        return cls(
            sql=raw["sql"],
            question=raw.get("question", ""),
            id=raw.get("id"),
            principal=raw.get("principal"),
            metadata=metadata,
            expected=_numeric(raw.get("expected"), "expected"),
            rel_tol=_numeric(raw.get("rel_tol"), "rel_tol", allow_negative=False),
            abs_tol=_numeric(raw.get("abs_tol"), "abs_tol", allow_negative=False),
            time_scoped=time_scoped,
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
        """True only when there is ≥1 example and every one is ``valid``.

        Safe as a CI gate — ``if not report.ok: sys.exit(1)``. It is False if ANY
        example is a ``violation``, is ``unchecked`` (no verdict could be
        rendered), or is ``unverified`` (the engine planned it but contract
        policy was never statically checked). An **empty** report is also NOT ok:
        a corpus that loaded to zero examples (bad path, emptied file, an
        upstream filter that dropped every row) must surface as a failure rather
        than silently pass a no-op gate. A laxer view — e.g. fail only on hard
        violations — can test ``report.violations`` directly.
        """
        return bool(self.results) and not (
            self.violations or self.unchecked or self.unverified_compliance
        )

    def summary(self) -> str:
        """A compact markdown report, suitable for an MR comment.

        Iterates ``enumerate(self.results)`` once so each row can fall back to
        its positional index (``id → question → #index``, per the spec) — two
        unnamed rows never render identically.
        """

        def _label(result: ExampleResult, index: int) -> str:
            return result.example.id or result.example.question or f"#{index}"

        # One pass over results; each has exactly one status, so the four counts
        # sum to the total (avoids re-scanning via the status properties).
        counts = Counter(r.status for r in self.results)
        lines = [
            f"**Example validation:** {counts['valid']} valid, "
            f"{counts['violation']} violation(s), "
            f"{counts['unverified']} unverified, "
            f"{counts['unchecked']} unchecked.",
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
    is given. Input order is preserved.

    On a sqlglot parse failure with an adapter present, the engine is asked
    directly (**decision B**). This deliberately DIVERGES from the live agent
    gate, where a parse failure hard-blocks the query: here it yields an
    ``unverified`` result (engine-plannable, policy-unchecked) so a
    contract-unmodelable dialect (e.g. Denodo/VDP) is surfaced for human triage
    rather than silently refused. ``unverified`` never counts toward ``ok``.
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
