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

import sqlglot

from agentic_data_contracts.adapters._normalizer import SqlNormalizer
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation._scalar import _scalar
from agentic_data_contracts.validation._timewindow import _relative_time_node
from agentic_data_contracts.validation.explain import ExplainAdapter
from agentic_data_contracts.validation.validator import ValidationResult, Validator

if TYPE_CHECKING:
    from agentic_data_contracts.adapters.base import DatabaseAdapter
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
        "expects_metrics",
    }
)

_DEFAULT_REL_TOL = 1e-9
_DEFAULT_ABS_TOL = 0.0


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
    ``expects_metrics`` declares which metrics an agent should have consulted
    before querying; it is independent of ``expected``, so a protocol-only row
    may set it.
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
    expects_metrics: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate the four assertion fields, however the record was built.

        ``from_dict`` is not the only door into this record: a corpus loader
        that constructs it directly would otherwise bypass every check below.
        The failure that buys is silent — an ``expected`` of ``nan`` reaches
        ``_compare``, where ``nan <= threshold`` is False, so the row reports a
        permanent ``mismatch`` with ``nan`` diffs instead of a load-time error
        naming the bad row. Validating here makes the invariant belong to the
        record rather than to one constructor.
        """
        if not isinstance(self.expects_metrics, list) or any(
            not isinstance(m, str) or not m.strip() for m in self.expects_metrics
        ):
            raise ValueError(
                "'expects_metrics' must be a list of non-empty metric names, "
                f"got {self.expects_metrics!r}"
            )
        if not isinstance(self.time_scoped, bool):
            raise ValueError(
                f"'time_scoped' must be a boolean, "
                f"got {type(self.time_scoped).__name__}"
            )
        self.expected = _numeric(self.expected, "expected")
        self.rel_tol = _numeric(self.rel_tol, "rel_tol", allow_negative=False)
        self.abs_tol = _numeric(self.abs_tol, "abs_tol", allow_negative=False)
        if self.expected is None:
            # These three only ever modify how an `expected` is compared, so
            # setting one without it is dead configuration — and the likeliest
            # cause is a typo'd or dropped `expected` key, which would
            # otherwise land inertly in metadata and quietly cost the corpus an
            # assertion that no gate can miss. Fail loudly instead.
            orphaned = [
                key
                for key, value in (
                    ("rel_tol", self.rel_tol),
                    ("abs_tol", self.abs_tol),
                    ("time_scoped", self.time_scoped or None),
                )
                if value is not None
            ]
            if orphaned:
                raise ValueError(
                    f"{', '.join(orphaned)} set without 'expected' — these only "
                    "affect how a certified answer is compared, so the row "
                    "asserts nothing. Add 'expected', or remove them."
                )

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
        # The four assertion fields are validated in __post_init__ rather than
        # here, so a corpus row and a directly-constructed record are held to
        # the same rules.
        return cls(
            sql=raw["sql"],
            question=raw.get("question", ""),
            id=raw.get("id"),
            principal=raw.get("principal"),
            metadata=metadata,
            expected=raw.get("expected"),
            rel_tol=raw.get("rel_tol"),
            abs_tol=raw.get("abs_tol"),
            time_scoped=raw.get("time_scoped", False),
            expects_metrics=raw.get("expects_metrics", []),
        )


def _label(example: VerifiedExample, index: int) -> str:
    """A row's display name: ``id`` → ``question`` → positional ``#index``.

    Shared by both reports' ``summary()`` and by the answer checker's error
    messages, so two unnamed rows never render identically.
    """
    return example.id or example.question or f"#{index}"


def _fmt(value: float | None) -> str:
    """Render a numeric for a report line, tolerating an unset field.

    ``summary()`` is what a CI operator reads when a check has already failed;
    it must not raise. A field left unset renders as ``?`` rather than
    crashing the whole report.
    """
    return "?" if value is None else f"{value:.3g}"


def _compare(
    actual: float, expected: float, rel_tol: float, abs_tol: float
) -> tuple[float, float, bool]:
    """Compare a measured value against a certified answer.

    Returns ``(abs_diff, rel_diff, matched)``.

    ``rel_diff`` is guarded against a zero reference — a certified answer of
    zero is legitimate, and dividing by it would raise or report a meaningless
    ``inf`` for an exact match. The three-branch form is the same one
    ``reconcile_decomposition`` uses for a zero parent.

    The relative term is anchored on ``expected``, deliberately unlike
    ``reconcile_decomposition`` (which compares two measurements and has no
    privileged side) and unlike ``math.isclose`` (which anchors on the larger
    magnitude). An assertion *has* a reference: the certified answer is the
    fixed point and the query result is what varies against it. Anchoring on
    ``expected`` keeps the tolerance's meaning stable — "within 0.1% of the
    certified number" — however far the query has drifted.
    """
    abs_diff = abs(actual - expected)
    if abs_diff == 0:
        rel_diff = 0.0
    elif expected != 0:
        rel_diff = abs_diff / abs(expected)
    else:
        rel_diff = math.inf
    matched = abs_diff <= max(abs_tol, rel_tol * abs(expected))
    return abs_diff, rel_diff, matched


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

        Uses the shared ``_label`` helper so each row can fall back to its
        positional index (``id → question → #index``) — two unnamed rows
        never render identically.
        """
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
                lines.append(
                    f"- violation `{_label(r.example, i)}`: {'; '.join(r.reasons)}"
                )
            elif r.status == "unverified":
                lines.append(
                    f"- unverified `{_label(r.example, i)}`: {'; '.join(r.warnings)}"
                )
            elif r.status == "unchecked":
                lines.append(
                    f"- unchecked `{_label(r.example, i)}`: {'; '.join(r.reasons)}"
                )
        return "\n".join(lines)


@dataclass
class ExampleAnswerResult:
    """The verdict for one asserted example.

    ``status`` (each result has exactly one):
      - ``"match"``        — executed and equal within tolerance.
      - ``"mismatch"``     — executed and outside tolerance. Both numbers and
                             both diffs are populated.
      - ``"unassertable"`` — the SQL uses a relative time window, so the
                             expected value decays. NOT executed.
      - ``"error"``        — no verdict was possible: not scalar-shaped, no
                             rows, NULL, non-finite, unparseable, or the
                             adapter raised.

    ``rel_tol`` / ``abs_tol`` record the tolerances actually applied to this
    row, so a mismatch names the threshold it missed.

    ``label`` is the display name computed by the caller (``_label(example,
    index)`` against ``ExampleValidationReport.results``) and stored here so
    ``ExampleAnswerReport.summary()`` can reuse it verbatim instead of
    recomputing it against a different, filtered index — see the module's
    ``check_example_answers`` for why those two indices differ.
    """

    example: VerifiedExample
    status: str
    expected: float | None = None
    actual: float | None = None
    abs_diff: float | None = None
    rel_diff: float | None = None
    rel_tol: float = _DEFAULT_REL_TOL
    abs_tol: float = _DEFAULT_ABS_TOL
    reason: str | None = None
    label: str = ""


@dataclass
class ExampleAnswerReport:
    results: list[ExampleAnswerResult]

    @property
    def matches(self) -> list[ExampleAnswerResult]:
        return [r for r in self.results if r.status == "match"]

    @property
    def mismatches(self) -> list[ExampleAnswerResult]:
        return [r for r in self.results if r.status == "mismatch"]

    @property
    def unassertable(self) -> list[ExampleAnswerResult]:
        return [r for r in self.results if r.status == "unassertable"]

    @property
    def errors(self) -> list[ExampleAnswerResult]:
        return [r for r in self.results if r.status == "error"]

    @property
    def ok(self) -> bool:
        """True only when there is ≥1 checked assertion and every one is ``match``.

        Scope worth being precise about: this covers the assertions that were
        *executed*, not every assertion in the corpus. A row carrying an
        ``expected`` that failed contract validation is filtered out before
        this pass and produces no result at all, so it cannot be seen here —
        ``report.ok`` is what catches it. That is why every documented gate
        composes the two (``report.ok and answers.ok``) rather than trusting
        this one alone.

        An **empty** report is NOT ok, mirroring ``ExampleValidationReport.ok``:
        calling the checker on a corpus where nothing declared an ``expected``
        means a filter, a schema change, or an emptied file dropped every
        assertion, and that must surface rather than pass a no-op gate. It
        does mean the second gate can only be wired into CI once at least one
        row carries an ``expected`` — add the first assertion, then the gate. A
        consumer wanting the laxer view meanwhile tests ``mismatches`` directly.
        """
        return bool(self.results) and all(r.status == "match" for r in self.results)

    def summary(self) -> str:
        """A compact markdown report, suitable for an MR comment.

        Uses each result's own ``label`` (computed once by the caller against
        the full validation report) rather than recomputing ``_label`` against
        ``self.results`` here — ``self.results`` is already filtered down to
        the asserted rows, so a positional index into it does not match the
        index a row had when its label was first computed. Recomputing here
        would give an unnamed row two different ``#N`` labels.
        """
        if not self.results:
            # `ok` is False here by design, so this text is what a first-time
            # user sees when CI goes red. Four zeroes do not explain that.
            return (
                "**Answer checks:** no assertions found — no example declared "
                "an `expected` value, so nothing was checked. Add one to a "
                "corpus row, or drop `answers.ok` from the gate until you do."
            )
        counts = Counter(r.status for r in self.results)
        lines = [
            f"**Answer checks:** {counts['match']} match, "
            f"{counts['mismatch']} mismatch(es), "
            f"{counts['unassertable']} unassertable, "
            f"{counts['error']} error(s).",
        ]
        for r in self.results:
            if r.status == "mismatch":
                lines.append(
                    f"- mismatch `{r.label}`: expected {r.expected}, "
                    f"actual {r.actual} (rel diff {_fmt(r.rel_diff)}, "
                    f"rel_tol {_fmt(r.rel_tol)}, abs_tol {_fmt(r.abs_tol)})"
                )
            elif r.status in ("unassertable", "error"):
                lines.append(f"- {r.status} `{r.label}`: {r.reason}")
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


def check_example_answers(
    report: ExampleValidationReport,
    *,
    adapter: DatabaseAdapter,
    dialect: str | None = None,
    sql_normalizer: SqlNormalizer | None = None,
    rel_tol: float = _DEFAULT_REL_TOL,
    abs_tol: float = _DEFAULT_ABS_TOL,
) -> ExampleAnswerReport:
    """Execute each *asserted*, contract-compliant example and check its answer.

    Takes the report from ``validate_examples`` rather than raw examples, and
    that is the load-bearing choice: an example that failed contract validation
    must never be executed, and consuming the report makes that ordering a
    property of the signature rather than a rule in a docstring. A row is
    executed only when it is ``status == "valid"`` AND declares an ``expected``;
    everything else produces no result at all (it is already accounted for by
    ``ExampleValidationReport``).

    ``validate_examples`` keeps its own property of never executing a query —
    it plans, via ``ExplainAdapter``, and nothing more. The execute-capable
    ``DatabaseAdapter`` enters only here.

    ``sql_normalizer`` must be the same value passed to ``validate_examples``:
    a corpus whose SQL only parses after normalization reached ``valid``
    through the normalizer. ``dialect`` defaults to ``adapter.dialect``, since
    ``DatabaseAdapter`` exposes one (unlike the bare ``ExplainAdapter`` that
    forces ``validate_examples`` to be told); pass it explicitly only when the
    corpus is authored in a different dialect than the adapter speaks.
    """
    effective_dialect = dialect if dialect is not None else adapter.dialect
    results: list[ExampleAnswerResult] = []
    for index, row in enumerate(report.results):
        example = row.example
        if row.status != "valid" or example.expected is None:
            continue
        row_rel_tol = example.rel_tol if example.rel_tol is not None else rel_tol
        row_abs_tol = example.abs_tol if example.abs_tol is not None else abs_tol
        try:
            results.append(
                _check_one(
                    example,
                    _label(example, index),
                    adapter=adapter,
                    dialect=effective_dialect,
                    sql_normalizer=sql_normalizer,
                    rel_tol=row_rel_tol,
                    abs_tol=row_abs_tol,
                )
            )
        except Exception as exc:  # noqa: BLE001 — batch resilience
            # A non-scalar result (_scalar raises), an unparseable statement, a
            # driver error, a timeout: this row gets no verdict, and the rest of
            # the corpus still gets one. Mirrors validate_examples' own guard.
            results.append(
                ExampleAnswerResult(
                    example=example,
                    status="error",
                    expected=example.expected,
                    rel_tol=row_rel_tol,
                    abs_tol=row_abs_tol,
                    reason=f"answer check error: {exc}",
                    label=_label(example, index),
                )
            )
    return ExampleAnswerReport(results=results)


def _check_one(
    example: VerifiedExample,
    label: str,
    *,
    adapter: DatabaseAdapter,
    rel_tol: float,
    abs_tol: float,
    dialect: str | None = None,
    sql_normalizer: SqlNormalizer | None = None,
) -> ExampleAnswerResult:
    expected = example.expected
    assert expected is not None  # guarded by the caller's filter

    def _make(status: str, **kw: Any) -> ExampleAnswerResult:
        return ExampleAnswerResult(
            example=example,
            status=status,
            expected=expected,
            rel_tol=rel_tol,
            abs_tol=abs_tol,
            label=label,
            **kw,
        )

    if not example.time_scoped:
        normalized = (
            sql_normalizer.normalize_sql(example.sql) if sql_normalizer else example.sql
        )
        statement = sqlglot.parse_one(normalized, dialect=dialect)
        found = _relative_time_node(statement)
        if found is not None:
            return _make(
                "unassertable",
                reason=(
                    f"relative time window ({found}) — the expected value "
                    "decays; pin the window or set time_scoped: true"
                ),
            )

    actual, reason = _scalar(adapter, example.sql, label)
    if actual is None:
        return _make("error", reason=reason)
    diff, rel_diff, matched = _compare(actual, expected, rel_tol, abs_tol)
    return _make(
        "match" if matched else "mismatch",
        actual=actual,
        abs_diff=diff,
        rel_diff=rel_diff,
        reason=None if matched else "answer differs from the certified value",
    )
