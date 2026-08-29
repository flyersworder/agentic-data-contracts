"""Pass 3 over a verified-examples corpus: can an agent reproduce the certified
answer from the contract alone, through the governed path?

``validate_examples`` asks whether certified SQL is still allowed and plannable.
``check_example_answers`` asks whether it still returns the right number. Both
check SQL a human already got right. Neither can see a contract that stays
enforceable and accurate while quietly ceasing to *teach* -- rename a metric or
trim a domain description and both stay green.

Nothing here calls a model or touches a database. The consumer runs their own
agent and hands back ``Attempt`` records; every function below is pure.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from agentic_data_contracts.core.recorder import ToolCall
from agentic_data_contracts.validation._rows import compare_rows
from agentic_data_contracts.validation._tolerance import _compare
from agentic_data_contracts.validation.examples import (
    _DEFAULT_ABS_TOL,
    _DEFAULT_REL_TOL,
    _MAX_NAMED_DIFFERENCES,
    VerifiedExample,
    _label,
)

if TYPE_CHECKING:
    from agentic_data_contracts.core.session import ContractSession


@dataclass
class Attempt:
    """One agent run against one corpus question."""

    example: VerifiedExample
    calls: list[ToolCall] = field(default_factory=list)
    final_text: str = ""
    final_answer: float | None = None
    foreign_tool_calls: list[str] = field(default_factory=list)
    cost_usd: float = 0.0
    elapsed_seconds: float = 0.0
    error: str | None = None
    # Appended rather than placed beside `final_answer`, so every pre-existing
    # positional constructor call keeps binding to the same field it always
    # did — this dataclass is public API and not `kw_only`. Same rule as
    # `VerifiedExample.expected_rows`.
    final_rows: list[list[Any]] | None = None
    final_columns: list[str] | None = None

    def __post_init__(self) -> None:
        """Reject a declared breakdown that cannot be compared.

        Both rules fail at construction naming the fault, rather than at
        grading: ``compare_rows`` checks ``final_columns`` against the
        certified width but nothing constrains an individual row, so a short
        row would index past its end and raise out of an
        ``evaluate_conformance`` that is otherwise total over its attempts.
        """
        if self.final_rows is None:
            return
        if self.final_columns is None:
            raise ValueError(
                "final_rows needs final_columns — the column names the "
                "breakdown was returned with, used to check the result's "
                "width and to name the column a difference is in"
            )
        width = len(self.final_columns)
        for index, row in enumerate(self.final_rows):
            if len(row) != width:
                raise ValueError(
                    f"final_rows[{index}] has {len(row)} cell(s) but "
                    f"final_columns names {width} column(s)"
                )

    @classmethod
    def from_session(
        cls,
        example: VerifiedExample,
        session: ContractSession,
        *,
        final_text: str = "",
        final_answer: float | None = None,
        final_rows: list[list[Any]] | None = None,
        final_columns: list[str] | None = None,
        foreign_tool_calls: Iterable[str] = (),
        error: str | None = None,
    ) -> Attempt:
        """Snapshot one attempt off the session that served it.

        ``foreign_tool_calls`` is coerced with ``list()``: the parameter takes
        any iterable and defaults to ``()``, while the field is ``list[str]``.
        """
        if session.recorder is None:
            raise ValueError(
                "Attempt.from_session needs a session built with a recorder: "
                "ContractSession(contract, recorder=ToolRecorder())"
            )
        return cls(
            example=example,
            calls=session.recorder.consume(),
            final_text=final_text,
            final_answer=final_answer,
            final_rows=final_rows,
            final_columns=final_columns,
            foreign_tool_calls=list(foreign_tool_calls),
            cost_usd=session.cost_usd,
            elapsed_seconds=session.recorder.elapsed_seconds,
            error=error,
        )


def _declared_breakdown(attempt: Attempt) -> bool:
    """A host-declared breakdown answer, on a row that certifies one.

    Scoped to ``expected_rows`` deliberately. A host may wire ``final_rows``
    uniformly -- an agent's result is a table for every question, not only for
    a breakdown -- and on a scalar-certified row that must not divert the
    attempt into the declared branch, which would bypass scalar selection and
    report `error: no scalar result produced` for a perfectly good answer.
    """
    return attempt.final_rows is not None and attempt.example.expected_rows is not None


def _tolerances(example: VerifiedExample) -> tuple[float, float]:
    rel = _DEFAULT_REL_TOL if example.rel_tol is None else example.rel_tol
    abs_ = _DEFAULT_ABS_TOL if example.abs_tol is None else example.abs_tol
    return rel, abs_


def _select_answer(
    attempt: Attempt,
) -> tuple[str, float | None, int, ToolCall | None]:
    """Decide which number the agent answered with, and say how sure that is.

    Returns ``(answer_source, actual, scalar_candidates, anchor)``. The *anchor*
    is the call the ordering and relative-time rules measure against; it is
    defined for the ``declared`` case too, because that is the path ambiguous
    rows are steered toward.

    Candidates are clustered by tolerance, not exact float equality: two
    scalars within the row's own ``rel_tol``/``abs_tol`` of each other count
    as the same answer, so a retried query after a transient failure does not
    demote an otherwise-unambiguous row.
    """
    successful = [
        c for c in attempt.calls if c.tool == "run_query" and c.outcome == "ok"
    ]
    # (call, scalar) pairs, narrowed once here so `scalar` is `float`, not
    # `float | None`, everywhere below.
    scalar_calls: list[tuple[ToolCall, float]] = [
        (c, c.scalar) for c in successful if c.scalar is not None
    ]

    rel_tol, abs_tol = _tolerances(attempt.example)
    clusters: list[tuple[ToolCall, float]] = []
    for call, scalar in scalar_calls:
        if not any(
            math.isclose(scalar, seen_scalar, rel_tol=rel_tol, abs_tol=abs_tol)
            for _, seen_scalar in clusters
        ):
            clusters.append((call, scalar))

    if attempt.final_answer is not None or _declared_breakdown(attempt):
        # A declared breakdown takes the same branch as `final_answer`: the
        # host has said what the agent answered, so no selection is needed.
        # The scalar it returns stays `final_answer` (None for a breakdown) --
        # the rows are read off the attempt by `_breakdown_verdict`.
        anchor = successful[-1] if successful else None
        return "declared", attempt.final_answer, len(clusters), anchor
    if not clusters:
        # No scalar, but a *lone* successful query still anchors ordering and
        # the relative-time check: "revenue by region" answers with many rows
        # and no scalar, and a window that returned NULL is still a window.
        #
        # Several successful queries and no scalar is a different situation:
        # nothing here says which one answered, and picking the last silently
        # is the same guess `last_scalar` exists to mark. Unmarked it fails in
        # the one direction that must never fail -- a lookup landing after the
        # real answering query but before a trailing drill-down would read as
        # compliant, turning a violation into a pass. So the guess is refused
        # rather than graded (there is no answer here to grade), and both
        # rules downstream report `unchecked`: couldn't judge, which fails.
        return "none", None, 0, (successful[0] if len(successful) == 1 else None)
    if len(clusters) == 1:
        call, scalar = clusters[0]
        return "sole_scalar", scalar, 1, call
    call, scalar = scalar_calls[-1]
    return "last_scalar", scalar, len(clusters), call


def _answer_verdict(
    attempt: Attempt,
    actual: float | None,
    anchor: ToolCall | None,
) -> tuple[str, list[str], float | None, float | None]:
    """Score the number ``_select_answer`` chose, in order; the first
    condition that applies wins.

    Returns ``(status, reasons, abs_diff, rel_diff)``. The diffs populate
    ``ConformanceResult`` so a mismatch can name the threshold it missed;
    they are ``None`` on every path that did not reach a comparison.
    """
    if attempt.error is not None:
        return "error", [attempt.error], None, None
    if attempt.example.expected is None:
        return "skipped", [], None, None
    # Ordered ahead of the missing-scalar check on purpose: pass 2 diagnoses a
    # decaying window before it executes anything, and a window that returned
    # NULL or no rows must reach the same diagnosis here. Blaming the absent
    # scalar would report a symptom of the window as an independent fault.
    if (
        anchor is not None
        and anchor.relative_time is not None
        and not attempt.example.time_scoped
    ):
        return (
            "unassertable",
            [
                f"agent's answering query uses a relative time window "
                f"({anchor.relative_time}); the certified answer decays against it"
            ],
            None,
            None,
        )
    if actual is None:
        return "error", ["no scalar result produced"], None, None

    rel_tol, abs_tol = _tolerances(attempt.example)
    # Argument order is load-bearing: _compare(actual, expected, ...) anchors
    # both rel_diff and the tolerance on `expected`, so "within X% of the
    # CERTIFIED number" stays stable however far the query drifted. Reversed,
    # it would anchor on whatever the agent measured instead.
    abs_diff, rel_diff, matched = _compare(
        actual, attempt.example.expected, rel_tol, abs_tol
    )
    return ("match" if matched else "mismatch"), [], abs_diff, rel_diff


def _breakdown_verdict(
    attempt: Attempt,
    anchor: ToolCall | None,
) -> tuple[str, list[str], list[str], int | None]:
    """Score a certified breakdown the host declared via ``final_rows``.

    Returns ``(status, reasons, row_differences, actual_row_count)``.

    The guard order differs from ``_answer_verdict``'s on purpose. There, the
    relative-time check precedes the missing-scalar check, because a window
    that returned NULL is still a decaying window and blaming the absent
    scalar would report a symptom as the fault. Here the undeclared case wins
    first: with no ``final_rows`` there is no answer to decay, so the row is
    ``skipped`` exactly as it was before this existed -- which is what keeps a
    host that never wired ``final_rows`` on its old verdict.
    """
    if attempt.error is not None:
        return "error", [attempt.error], [], None
    if attempt.final_rows is None:
        return "skipped", [], [], None
    if (
        anchor is not None
        and anchor.relative_time is not None
        and not attempt.example.time_scoped
    ):
        return (
            "unassertable",
            [
                f"agent's answering query uses a relative time window "
                f"({anchor.relative_time}); the certified answer decays against it"
            ],
            [],
            None,
        )
    rel_tol, abs_tol = _tolerances(attempt.example)
    expected_rows = attempt.example.expected_rows
    columns = attempt.final_columns
    # Real checks, not asserts: `Attempt` is not frozen, so `__post_init__`'s
    # guarantee does not hold at use time, and an assert would be stripped
    # under `python -O` -- turning a clear message into an opaque TypeError
    # from inside the comparison.
    if expected_rows is None or columns is None:
        return "error", ["a declared breakdown needs final_columns"], [], None
    try:
        comparison = compare_rows(
            expected_rows,
            columns,
            attempt.final_rows,
            ordered=attempt.example.ordered,
            rel_tol=rel_tol,
            abs_tol=abs_tol,
        )
    except ValueError as e:
        # `compare_rows` raises for a fault no pairing can resolve. Pass 2's
        # batch guard turns that into `status="error"`; pass 3 is pure and has
        # no such guard, so it converts here rather than propagating out of an
        # `evaluate_conformance` documented as total over its attempts.
        return "error", [str(e)], [], None
    if comparison.matched:
        return "match", [], [], comparison.actual_row_count
    # `summary()` renders `reasons`, not `row_differences`, so a mismatch that
    # spoke only through the new field would post a bare "mismatch" with an
    # empty note. Named differences are capped the same way pass 2 caps them,
    # against the same constant, while `row_differences` stays uncapped.
    shown = "; ".join(comparison.differences[:_MAX_NAMED_DIFFERENCES])
    extra = len(comparison.differences) - _MAX_NAMED_DIFFERENCES
    more = f" (and {extra} more)" if extra > 0 else ""
    return (
        "mismatch",
        [
            f"breakdown differs from the certified answer: "
            f"{comparison.expected_group_count} expected group(s), "
            f"{comparison.actual_row_count} row(s) returned, "
            f"{len(comparison.differences)} difference(s): {shown}{more}"
        ],
        comparison.differences,
        comparison.actual_row_count,
    )


def _protocol_verdict(
    attempt: Attempt, anchor: ToolCall | None
) -> tuple[str, list[str]]:
    """Judge whether the agent used the governed path.

    Only rules the corpus row *activated* can fail it. A row that declares
    nothing lands on ``not_applicable``, which passes -- a guessed violation
    here would become wrongly-rewritten contract prose downstream.
    """
    reasons: list[str] = []

    successful = [
        c for c in attempt.calls if c.tool == "run_query" and c.outcome == "ok"
    ]

    # P3 -- friction. Recorded on every path, never fails on its own. Scoped to
    # run_query: a describe_table block restricted for the principal is not a
    # query attempt. The wording is derived from sequence order, not from the
    # mere existence of an accepted query: a block that happened *after* the
    # accepted one is drill-down friction, and calling it a block the agent
    # fought through on the way in would send a prose fix to the wrong place.
    blocked = [
        c for c in attempt.calls if c.tool == "run_query" and c.outcome == "blocked"
    ]
    if blocked:
        if not successful:
            tail = "with none accepted"
        else:
            # Measured against the anchor, not against the last successful
            # call: P2 orders lookups against the anchor, and two rules
            # disagreeing about which query was the answer is how the
            # backdating this wording exists to prevent comes back. The
            # fallback covers the anchor-less case, where every successful
            # query is equally a candidate and the last is the safest tail.
            cut = (anchor or successful[-1]).sequence
            before = sum(1 for c in blocked if c.sequence < cut)
            after = len(blocked) - before
            if not after:
                tail = "before an accepted one"
            elif not before:
                tail = "after an accepted one"
            else:
                tail = f"— {before} before an accepted one, {after} after"
        reasons.append(f"{len(blocked)} blocked run_query attempt(s) {tail}")
    misses = [c for c in attempt.calls if c.outcome == "miss"]
    for call in misses:
        reasons.append(f"lookup miss on {call.tool}({call.args})")

    if attempt.error is not None:
        return "unchecked", reasons

    # P1 -- contamination.
    if attempt.foreign_tool_calls:
        reasons.append(
            "non-contract tools were available: "
            f"{', '.join(attempt.foreign_tool_calls)}"
        )
        return "contaminated", reasons
    if (attempt.final_answer is not None or _declared_breakdown(attempt)) and (
        not successful
    ):
        reasons.append(
            "an answer was declared with no successful run_query — it came from "
            "outside the governed path"
        )
        return "contaminated", reasons

    # P2 -- metric consultation. Activated only by expects_metrics.
    if not attempt.example.expects_metrics:
        return "not_applicable", reasons
    if anchor is None:
        reasons.append("no answering query to order the metric lookups against")
        return "unchecked", reasons

    consulted = {
        str(c.args.get("metric_name", ""))
        for c in attempt.calls
        if c.tool == "lookup_metric"
        and c.outcome == "ok"
        and c.sequence < anchor.sequence
    }
    missing = [m for m in attempt.example.expects_metrics if m not in consulted]
    if missing:
        reasons.append(
            f"answered without consulting {', '.join(missing)} — "
            "lookup_metric was never called for it before the answering query"
        )
        return "violated", reasons
    return "followed", reasons


@dataclass
class ConformanceResult:
    """The verdict for one attempt, on two orthogonal axes.

    ``answer_source`` stays separate from ``answer`` rather than fusing into it:
    a ``last_scalar`` row that numerically matched still reports
    ``answer="match"`` and is still excluded from ``ok``. The verdict and the
    evidence for it are different fields, so nothing hides how it was derived.
    """

    attempt: Attempt
    answer: str
    protocol: str
    answer_source: str
    scalar_candidates: int
    expected: float | None = None
    actual: float | None = None
    abs_diff: float | None = None
    rel_diff: float | None = None
    rel_tol: float = _DEFAULT_REL_TOL
    abs_tol: float = _DEFAULT_ABS_TOL
    reasons: list[str] = field(default_factory=list)
    label: str = ""
    row_differences: list[str] = field(default_factory=list)
    actual_row_count: int | None = None


@dataclass
class ConformanceReport:
    results: list[ConformanceResult]

    @property
    def passed(self) -> list[ConformanceResult]:
        return [r for r in self.results if _result_ok(r)]

    @property
    def answer_failures(self) -> list[ConformanceResult]:
        return [
            r for r in self.results if r.answer in {"mismatch", "error", "unassertable"}
        ]

    @property
    def protocol_failures(self) -> list[ConformanceResult]:
        return [r for r in self.results if r.protocol == "violated"]

    @property
    def contaminated(self) -> list[ConformanceResult]:
        return [r for r in self.results if r.protocol == "contaminated"]

    @property
    def ambiguous(self) -> list[ConformanceResult]:
        """Rows whose answer axis was actually judged but whose selected
        scalar was ambiguous.

        Scoped to ``answer != "skipped"`` rather than ``answer == "match"``:
        an ambiguous row can just as easily resolve to a ``mismatch`` (the
        wrong candidate was picked) as to a ``match`` (it was picked and
        happened to be right). Either way the row asserted an expected value
        and the selection among several distinct scalars was uncertain. A
        ``skipped`` row never asserted anything, so its ambiguity is
        irrelevant and must not appear here.
        """
        return [
            r
            for r in self.results
            if r.answer != "skipped" and r.answer_source == "last_scalar"
        ]

    @property
    def skipped(self) -> list[ConformanceResult]:
        return [r for r in self.results if r.answer == "skipped"]

    def by_example(self) -> dict[str, list[ConformanceResult]]:
        """Group repeats of the same question.

        Keys on ``id``, falling back to ``question`` -- never on ``label``,
        which embeds a positional index and would split repeats apart.
        """
        grouped: dict[str, list[ConformanceResult]] = {}
        for result in self.results:
            example = result.attempt.example
            key = example.id or example.question or result.label
            grouped.setdefault(key, []).append(result)
        return grouped

    def pass_rate(self) -> float:
        if not self.results:
            return 0.0
        return len(self.passed) / len(self.results)

    @property
    def ok(self) -> bool:
        """The strict safe gate. An empty report is not ok."""
        return bool(self.results) and all(_result_ok(r) for r in self.results)

    def summary(self) -> str:
        lines = [
            f"**Conformance:** {len(self.passed)}/{len(self.results)} attempts passed "
            f"({self.pass_rate():.0%})",
            "",
            "| Example | Answer | Protocol | Source | Notes |",
            "| --- | --- | --- | --- | --- |",
        ]
        for r in self.results:
            notes = "; ".join(r.reasons) if r.reasons else ""
            lines.append(
                f"| {r.label} | {r.answer} | {r.protocol} | "
                f"{r.answer_source} | {notes} |"
            )
        return "\n".join(lines)


def _result_ok(result: ConformanceResult) -> bool:
    """Nothing-to-judge passes; couldn't-judge fails.

    The ``last_scalar`` exclusion is scoped to rows whose answer axis was
    actually judged: ``answer_source`` is derived for every attempt, so a
    protocol-only row where the agent ran several exploratory queries would
    otherwise fail the gate over an answer nobody was asserting.
    """
    if result.protocol not in {"followed", "not_applicable"}:
        return False
    if result.answer == "skipped":
        return True
    return result.answer == "match" and result.answer_source != "last_scalar"


def evaluate_conformance(attempts: list[Attempt]) -> ConformanceReport:
    """Score recorded attempts. Pure: no network, no database, no model."""
    results = []
    for index, attempt in enumerate(attempts):
        source, actual, candidates, anchor = _select_answer(attempt)
        row_differences: list[str] = []
        actual_row_count: int | None = None
        if attempt.example.expected_rows is not None:
            answer, answer_reasons, row_differences, actual_row_count = (
                _breakdown_verdict(attempt, anchor)
            )
            abs_diff = rel_diff = None
        else:
            answer, answer_reasons, abs_diff, rel_diff = _answer_verdict(
                attempt, actual, anchor
            )
        protocol, protocol_reasons = _protocol_verdict(attempt, anchor)
        rel_tol, abs_tol = _tolerances(attempt.example)
        results.append(
            ConformanceResult(
                attempt=attempt,
                answer=answer,
                protocol=protocol,
                answer_source=source,
                scalar_candidates=candidates,
                expected=attempt.example.expected,
                actual=actual,
                abs_diff=abs_diff,
                rel_diff=rel_diff,
                rel_tol=rel_tol,
                abs_tol=abs_tol,
                reasons=answer_reasons + protocol_reasons,
                label=_label(attempt.example, index),
                row_differences=row_differences,
                actual_row_count=actual_row_count,
            )
        )
    return ConformanceReport(results=results)
