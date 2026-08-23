# Contract Conformance Evaluator — Design

**Date:** 2026-08-23
**Status:** Approved, not yet implemented
**Scope:** Sub-project 1 of 2. Sub-project 2 (the contract refinement loop) is
deliberately deferred — see [Out of scope](#out-of-scope).

## Problem

The library has two passes over a verified-examples corpus:

1. `validate_examples` — is the certified SQL still **allowed and plannable**?
   (contract policy + warehouse schema)
2. `check_example_answers` — does it still return the **right number**?
   (warehouse truth)

Both check *certified SQL a human already got right*. Neither can detect a
third failure: the contract is still enforceable and still accurate, but an
agent can no longer **find its way to the right answer through it**.

That regression is silent today. Rename a metric, trim a domain
`description`, or tighten a rule in a way that remains valid, and passes 1 and
2 stay green while the contract quietly stops teaching. This design adds the
missing rung:

3. `evaluate_conformance` — can an **agent reproduce the certified answer from
   the contract alone**, using the governed path? (teaching quality)

## Why this belongs in the library

- It reads the same `VerifiedExample` rows and reuses the same comparison
  semantics, labelling, and report idiom as passes 1 and 2.
- The evidence it needs — which contract tools were called, in what order,
  with what outcome — can only be produced by instrumenting the tools the
  library itself creates.
- "Prove in CI that my agent actually uses the governed path" is a governance
  question, and it is currently unanswerable with anything the library ships.

It is a **contract-conformance evaluator**, not a general LLM eval framework.
It answers exactly two questions per attempt: did the agent follow the query
protocol, and did it reach the certified answer. It must not grow prompt
management, dataset versioning, or scoring plugins.

## Scope

**In scope**

- A tool-call recorder attached to `ContractSession`.
- Typed `ToolCall` / `Attempt` records.
- Pure, synchronous verdict derivation over recorded attempts.
- A `ConformanceReport` with a strict `ok` gate, a `pass_rate()`, and a
  markdown `summary()`.
- One additive optional field on `VerifiedExample` (`expects_metrics`).
- A demo script that runs with no API key.

**Out of scope**

- Running an agent. The library never drives a model.
- Any new dependency. The base install is unchanged.
- A corpus runner, concurrency helper, retry logic, or report persistence.
  The consumer writes the `for` loop. If everyone writes the same loop, that
  becomes an `examples/` script before it ever becomes a library function.

<a id="out-of-scope"></a>
**Deferred to sub-project 2** — diagnosis of eval failures into typed
findings, a `Refiner` protocol for model-proposed edits to non-normative
contract prose, a write guard rejecting edits to normative fields, and
lossless YAML diff output. That work consumes `ConformanceReport` and cannot
be built or tested without it. It is deferred until this evaluator has
produced real failures against the example agents, so the refiner is designed
against observed prose gaps rather than speculative ones.

## Architecture

### The seam: the recorder rides on the session

All four public entry points — `create_tools`, `create_sdk_mcp_server`,
`create_langchain_tools`, `create_pydantic_ai_tools` — already accept
`session: ContractSession | None = None` and thread it into every tool
closure. `ContractSession` is already the per-run state object.

Therefore the recorder attaches to the session. **No public function gains a
parameter, and one implementation instruments all four frameworks.** No
contextvars, no framework choice, no signature churn.

Because the harness creates a fresh session per question, per-attempt budget
isolation and `cost_usd` come for free. Wall-clock does **not**: the session
timer starts in `_ensure_timer()`, reached only via `check_limits()`, which only
`run_query` calls — so it reads `0.0` for any attempt that never ran a query.
`ToolRecorder` therefore times itself from construction.

### The library never runs an agent

`evaluate_conformance` is pure and synchronous: it takes recorded attempts and
returns verdicts, touching no network and no database. Everything
nondeterministic happens in the consumer's loop, above the library.

```python
from agentic_data_contracts.core.session import ContractSession
from agentic_data_contracts.validation import Attempt, ToolRecorder, evaluate_conformance

attempts = []
for ex in corpus:                                  # the same VerifiedExample rows
    session = ContractSession(contract, recorder=ToolRecorder())
    tools = create_langchain_tools(contract, adapter=adapter, session=session)
    text = await my_agent(ex.question, tools)      # consumer's wiring, any framework
    attempts.append(Attempt.from_session(ex, session, final_text=text))

report = evaluate_conformance(attempts)
print(report.summary())
if not report.ok:
    sys.exit(1)
```

### Module placement

New module `src/agentic_data_contracts/validation/conformance.py`, exported
from `validation/__init__.py` alongside its two siblings. Naming follows the
existing progression: `validate_examples` → `check_example_answers` →
`evaluate_conformance`.

`conformance.py` imports `_compare`, `_label`, `_relative_time_node`, and
`_is_clock_read` from `validation/examples.py`, and `_scalar_value` from
`validation/_scalar.py` (see the refactor in
[Changes to existing code](#changes-to-existing-code)). This coupling is
intentional: pass 3 must apply *identical* tolerance, labelling, scalar-shape,
and relative-time semantics to passes 1 and 2, so a fix to any helper fixes all
three. `examples.py` does not import `conformance.py`, so there is no cycle.
The helpers are **not** extracted to a new shared module beyond the
`_scalar_value` split described below.

`ToolCall` and `ToolRecorder` live in a **new `core/recorder.py`**, not in
`conformance.py`. `ContractSession` must reference `ToolRecorder`, and
`conformance.py` must reference `ContractSession` for `Attempt.from_session`;
putting the recorder in `validation/` would therefore create a hard cycle
(`core.session` → `validation.conformance` → `validation/__init__` →
`examples.py` → `adapters` → `core.session`). `core/recorder.py` depends on
nothing but the standard library, which removes the cycle rather than papering
over it with a `TYPE_CHECKING` import. Both names are re-exported from
`validation/__init__.py` so consumers still have a single import site.

`tools/factory.py` already imports from `validation/` (it uses `Validator`), so
the recorder introduces no new dependency direction there either.

## The closed-world requirement

An agent in production may hold tools this library did not create — a generic
SQL tool, a warehouse MCP server, a shell, a retriever. The recorder cannot
see those, which makes the signal "never called `lookup_metric`" ambiguous
between a genuine prose gap and the agent simply going around the contract.

The evaluator therefore assumes a **closed world**: the eval runs the agent
with only the contract toolset. This is legitimate because it is an eval, not
production — the question being measured is "can an agent answer this from the
contract alone?", which is meaningless otherwise.

The requirement is enforced by **derived evidence, never by assertion**. The
consumer is not asked to promise a closed world:

- An answer produced with **zero successful `run_query`** proves, by
  construction, that the number came from outside the governed path →
  `contaminated`.
- A consumer who *does* have full framework logs may populate
  `Attempt.foreign_tool_calls`. It is used **only** to mark a row
  `contaminated`, never as input to any other verdict. The typed call log
  remains the sole authority for diagnosis, keeping arbitrary trace formats
  out of the reasoning path.

**Known limit, documented as such:** an agent that used `run_query` but drew
its business context from a foreign retriever leaves no detectable trace. The
evaluator's findings are trustworthy in proportion to how closed the eval
world actually was.

## Data model

`ToolCall` and `ToolRecorder` in `core/recorder.py`; `Attempt`,
`ConformanceResult`, and `ConformanceReport` in `validation/conformance.py`.

### `ToolCall`

```python
@dataclass(frozen=True)
class ToolCall:
    sequence: int
    tool: str                        # "lookup_metric" | "run_query" | ...
    args: dict[str, Any]
    outcome: str                     # "ok" | "miss" | "blocked" | "error"
    detail: str | None = None        # block reason, or fuzzy candidates returned
    scalar: float | None = None      # run_query only, when scalar-shaped
    row_count: int | None = None
    relative_time: str | None = None # run_query only; see below
```

`outcome` is the load-bearing field:

- `ok` — the call resolved exactly (an exact metric/domain hit; a query that
  validated and executed).
- `miss` — a lookup did not resolve exactly: the fuzzy fallback fired, or
  nothing matched. The most direct evidence of a naming or prose gap.
- `blocked` — validation rejected the SQL.
- `error` — the tool raised, **or returned an error payload without raising**.

That last clause is load-bearing and is why `_error_response` gains a machine-
readable `kind` (see [Changes to existing code](#changes-to-existing-code)).
The tools return errors far more often than they raise them — `lookup_metric`
returns `_error_response("No semantic source configured.")`, `run_query`
returns `_error_response("No database adapter configured…")`, and there are
eighteen such sites. Classifying outcome by "did it raise" would record every
one of those as `ok`: an eval wired without a semantic source would log every
`lookup_metric` as a successful consultation, P2 would pass, and the report
would certify `followed` for an agent that never saw a metric definition. The
outcome must be derived from the returned payload, not from the absence of an
exception.

`relative_time` is captured **at record time, inside `run_query`**, where the
already-parsed statement, the contract dialect, and the `SqlNormalizer` are all
in scope. Concretely — worked out while planning — `Validator.validate` reports
it as a new `ValidationResult.relative_time` field, computed from the statement
it already parses, and `run_query` reads `vresult.relative_time`. That in turn
requires `_relative_time_node` and `_is_clock_read` to move from `examples.py`
to a shared private `validation/_timewindow.py`: `examples.py` imports
`Validator`, so importing the helper back into `validator.py` would be circular.
Same extraction pattern as `_scalar.py`.
This is why `evaluate_conformance` needs no `dialect` or `sql_normalizer`
parameter and can stay pure: it reads a field rather than re-parsing agent SQL.
Where sqlglot cannot parse the SQL at all (the Denodo/VQL path), `relative_time`
stays `None` and the relative-time check is skipped, degrading exactly as
`unverified` does in pass 1.

### `ToolRecorder`

```python
class ToolRecorder:
    calls: list[ToolCall]

    def __init__(self) -> None: ...          # stamps a monotonic start time
    def log(self, tool, args, outcome, **fields) -> None: ...
    @property
    def elapsed_seconds(self) -> float: ...
```

The recorder times itself from construction rather than reading
`ContractSession.elapsed_seconds`. The session's timer is started by
`_ensure_timer()`, which is reached only from `check_limits()`, which only
`run_query` calls — so a session-derived duration reads `0.0` for precisely the
attempts worth timing: the `contaminated` and `error` rows where the agent
burned minutes and never reached a successful query.

**The recorder never retains result rows** — only `scalar` and `row_count`. A
conformance report is the kind of artifact that gets committed or posted as a
PR comment, and one containing warehouse rows would leak data out of the
governed boundary this library exists to defend.

Query *arguments* are retained, since SQL is schema rather than data. To keep
SQL literals out of PR comments by default, `summary()` never prints raw SQL;
it remains available programmatically on the record.

### `Attempt`

```python
@dataclass
class Attempt:
    example: VerifiedExample
    calls: list[ToolCall]
    final_text: str = ""
    final_answer: float | None = None            # consumer-declared, wins if set
    foreign_tool_calls: list[str] = field(default_factory=list)
    cost_usd: float = 0.0
    elapsed_seconds: float = 0.0
    error: str | None = None                     # agent crashed / budget exhausted

    @classmethod
    def from_session(cls, example, session, *, final_text="",
                     final_answer=None, foreign_tool_calls=(),
                     error=None) -> Attempt: ...
```

`from_session` coerces `foreign_tool_calls` with `list(...)`: the parameter
accepts any iterable (its default is `()`), while the field is declared
`list[str]`, and storing the tuple as-is would break the declared type for any
consumer that appends to it.

`from_session` snapshots `list(session.recorder.calls)`, copies `cost_usd` and
`elapsed_seconds` off the session, and marks the recorder consumed. A second
call on the same recorder raises: a merged call log across two questions would
silently produce wrong protocol verdicts, and this codebase prefers a loud
error to a silent wrong answer (see `VerifiedExample.__post_init__`).

### `ConformanceResult`

One verdict per **attempt**, not per example. Grouping lives in the report,
which makes repeated runs of the same question free and avoids a second
aggregate type.

```python
@dataclass
class ConformanceResult:
    attempt: Attempt
    answer: str            # "match" | "mismatch" | "unassertable" | "error" | "skipped"
    protocol: str          # "followed" | "violated" | "contaminated"
                           #   | "not_applicable" | "unchecked"
    answer_source: str     # "declared" | "sole_scalar" | "last_scalar" | "none"
    scalar_candidates: int
    expected: float | None = None
    actual: float | None = None
    abs_diff: float | None = None
    rel_diff: float | None = None
    rel_tol: float = _DEFAULT_REL_TOL
    abs_tol: float = _DEFAULT_ABS_TOL
    reasons: list[str] = field(default_factory=list)
    label: str = ""
```

`answer_source` stays **orthogonal** to `answer` rather than fusing into it. A
`last_scalar` row that numerically matched still reports `answer="match"` — and
is still excluded from `ok`. This is the separation `ExampleResult` already
makes between `status` and `contract_checked` / `engine_checked`: the verdict
and the evidence for it are different fields, so nothing hides how it was
derived.

### `ConformanceReport`

```python
@dataclass
class ConformanceReport:
    results: list[ConformanceResult]

    # filter properties, mirroring the existing reports
    passed, answer_failures, protocol_failures, contaminated, ambiguous, skipped

    def by_example(self) -> dict[str, list[ConformanceResult]]   # groups repeats
    def pass_rate(self) -> float
    @property
    def ok(self) -> bool
    def summary(self) -> str                                      # markdown, PR-shaped
```

`by_example` groups on `VerifiedExample.id`, falling back to
`VerifiedExample.question` when `id` is unset. It must **not** fall back to
`label`, which embeds a positional index and would therefore place two repeats
of the same question into different groups.

## Verdict derivation

All pure functions over an `Attempt`. No I/O, no model.

### Answer selection

```
if attempt.final_answer is not None        -> "declared"
else:
    candidates = [c.scalar for c in calls
                  if c.tool == "run_query" and c.outcome == "ok"
                  and c.scalar is not None]
    distinct = cluster(candidates)         # tolerance-based; see below
    len(distinct) == 0                     -> "none",        actual = None
    len(distinct) == 1                     -> "sole_scalar", actual = distinct[0]
    len(distinct) >  1                     -> "last_scalar", actual = candidates[-1]

scalar_candidates = len(distinct)
```

Three details do real work:

- Non-scalar results carry `scalar = None` and never become candidates. This
  removes the "agent's last query returned a table" outlier, which would
  otherwise score a correct attempt as `error`.
- Candidates are **clustered before counting**, so an agent that reruns the same
  query after a transient failure still reports `sole_scalar`. Retries are
  ordinary; ambiguity should mean genuinely different numbers.
- Clustering is **tolerance-based**, not exact float equality: two candidates
  are the same when `math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)` using
  the row's own tolerances. Exact equality would let `100.0` and
  `100.00000000000001` count as two answers and demote the row to
  `last_scalar`, failing the `ok` gate over the precise case clustering exists
  to absorb. `math.isclose` rather than `_compare` is deliberate: these are two
  measurements with no privileged reference, which is the one situation
  `_compare`'s docstring says it is *not* for.

**The anchor call.** Several rules below need "the call that produced the
answer". It is defined once, here:

- `sole_scalar` / `last_scalar` → the `run_query` call whose scalar was
  selected.
- `declared` → the **last successful `run_query`**, if any; otherwise `None`.
- `none` → `None`.

Defining it for the `declared` case matters because that is the path this spec
steers consumers toward for ambiguous rows (see the `ok` gate). Without it, P2's
ordering rule and the relative-time check would be undefined on exactly those
rows. When the anchor is `None`, both are skipped.

Selecting "the last `run_query`" unconditionally was rejected: verification
queries after the answer, drill-downs, and non-scalar tails all break it. The
harness cannot know which query was the answer unless someone says so, so the
guess is graded rather than hidden.

### Answer verdict

Evaluated in order; first match wins.

1. `attempt.error` is set → `error`.
2. `example.expected is None` → `skipped` (a legitimate protocol-only row, not
   a failure).
3. `actual is None` → `error`, reason `"no scalar result produced"`.
4. The anchor call's `relative_time` is set and the row is not `time_scoped`
   → `unassertable`. Pass 2 applies `_relative_time_node` to the *certified*
   SQL; here the SQL that matters is the agent's, because that is what the
   certified number is being compared against — a case pass 2 structurally
   cannot see. Skipped when there is no anchor, or when the SQL did not parse.
5. Otherwise `_compare(actual, expected, rel_tol, abs_tol)` → `match` or
   `mismatch`.

   **Argument order is load-bearing.** The signature is
   `_compare(actual, expected, rel_tol, abs_tol)`, and it deliberately anchors
   both `rel_diff` and the tolerance on `expected` — "within 0.1% of the
   certified number", stable however far the query has drifted. Passing the
   operands in the other order silently anchors on the agent's measured value:
   with `expected=1`, `actual=100`, `rel_tol=1.0`, the correct call reports a
   mismatch and the swapped call reports a **match**. The zero-reference guard
   flips to the wrong operand too.

### Protocol verdict

Governing principle:

> **A protocol failure requires a rule the corpus row activated.** No rule
> activated means there is nothing to judge — never a guessed violation.

This matters because in sub-project 2 these findings will drive prose edits. A
false positive there writes wrong documentation into the contract.

- **P1 — contamination.** `foreign_tool_calls` non-empty → `contaminated`. Or:
  `final_answer` was declared and there is **no successful `run_query`** in the
  attempt → `contaminated`, since that number provably came from outside the
  governed path.

  The declared branch is the only reachable one. A *derived* scalar cannot
  coexist with zero successful `run_query` calls, because candidates are drawn
  exclusively from `run_query` calls with `outcome == "ok"` — an earlier draft
  of this rule included that case and it was dead by construction. An agent
  that states a number only in prose, with no query and no declared answer, is
  not detectable here and lands on `not_applicable` / `error` instead.
- **P2 — metric consultation.** Applies **only** when the row sets
  `expects_metrics`. Each named metric must have a successful `lookup_metric`
  (outcome `ok`) at a `sequence` lower than the **anchor call's**. Missing →
  `violated`, with the metric named in `reasons`. When `expects_metrics` is
  empty this rule does not run; when it is set but there is no anchor call, the
  ordering cannot be judged and the row is `unchecked`.

  Inferring whether a metric was "required" from the SQL was rejected: "how
  many rows in `orders`?" legitimately needs no metric, and any inference rule
  will eventually call that a violation.
- **P3 — friction, recorded but never failing.** `blocked` SQL attempts
  preceding a passing one, and `miss` outcomes on lookups, are appended to
  `reasons` and shown in `summary()`. A miss followed by successful recovery is
  a prose smell, not a breach — and it is the highest-value signal for the
  refiner later, so it must be captured without being punitive now.

Resolution order: `attempt.error` set → `unchecked`. Else P1 → `contaminated`.
Else P2 activated and failing → `violated`. Else P2 activated and passing →
`followed`. Else → `not_applicable`.

### The two-axis symmetry

Both axes distinguish "nothing to judge" (passes) from "couldn't judge"
(fails):

| Axis | Nothing to judge → passes | Couldn't judge → fails |
|---|---|---|
| answer | `skipped` (no `expected`) | `error` |
| protocol | `not_applicable` (no rule activated) | `unchecked` (attempt errored) |

Without that split, `ok` would be unreachable: most rows activate no protocol
rule, and folding them into `unchecked` would fail every gate forever.
Conflating the two is the classic eval bug — a suite reporting green because
every case was skipped.

### The `ok` gate

```python
ok = bool(results) and all(
    r.protocol in {"followed", "not_applicable"}
    and (
        r.answer == "skipped"
        or (r.answer == "match" and r.answer_source != "last_scalar")
    )
    for r in results
)
```

The `last_scalar` exclusion is scoped to rows whose answer axis was actually
judged. `answer_source` is derived for every attempt (P1 needs to know whether
any scalar was produced), so a protocol-only row on which the agent happened to
run several different queries would otherwise fail the gate over an ambiguity
about an answer nobody was asserting.

An empty report is **not** ok, matching both existing reports: an emptied or
fully-filtered corpus must surface rather than pass a no-op gate.

`answer_source == "last_scalar"` is excluded because the verdict rests on a
guess about which query was the answer. This follows the precedent that
`ExampleValidationReport.ok` fails on `unverified` rows: an undecided verdict
has never been treated as a pass in this library, and this is not the place to
start. The remedy is for the consumer to declare `final_answer` on the few
ambiguous rows, which is the correct outcome.

With repeats, every attempt must satisfy the gate, so one flake fails it. A
threshold gate uses `pass_rate()` instead. Both are documented; neither is the
default silently.

## Changes to existing code

<a id="changes-to-existing-code"></a>

1. **`core/session.py`** — `ContractSession.__init__` gains
   `recorder: ToolRecorder | None = None` and stores it. Default `None` means
   zero overhead and byte-identical behavior for every existing user.
2. **New `core/recorder.py`** — `ToolCall` and `ToolRecorder`. Standard library
   only, so `core/session.py` can import it at runtime without creating the
   `core.session` ↔ `validation.conformance` cycle described in
   [Module placement](#architecture).
3. **`tools/factory.py` — `_error_response` gains a `kind`** (`"error"` |
   `"blocked"`), threaded through all eighteen call sites and used by the
   recorder to classify outcome. Mechanical, but not optional: without it the
   recorder cannot distinguish a successful call from one that returned an
   error payload, and every "no semantic source configured" response would be
   logged as a successful metric consultation.
4. **`validation/_scalar.py`** — split the existing `_scalar` into a pure
   shape-and-value rule and a thin executing wrapper:

   ```python
   def _scalar_value(columns, rows, label) -> tuple[float | None, str | None]:
       # The existing empty / NULL / non-finite / non-scalar rule, applied
       # to an already-fetched result. Raises ValueError if not scalar-shaped.
       ...

   def _scalar(adapter, sql, label) -> tuple[float | None, str | None]:
       result = adapter.execute(sql)
       return _scalar_value(result.columns, result.rows, label)
   ```

   This is required, not cosmetic: today `_scalar` **executes the query
   itself**, so the recorder cannot reuse it without re-running every query the
   agent already ran — double cost and double side effects. Extracting the pure
   half honours the module’s own stated rule that these semantics have exactly
   one implementation. Existing callers (`reconcile_decomposition`,
   `check_example_answers`) keep the same signature and behavior.
5. **`tools/factory.py`** — each of the nine tool closures gains a guarded log
   line (`if session.recorder is not None: session.recorder.log(...)`).
   `run_query` additionally captures `row_count`; `relative_time` from the
   statement it already parsed for validation; and, for `scalar`, calls
   `_scalar_value` on the result it *already holds*, treating `ValueError`
   (not scalar-shaped) as `scalar = None` rather than an error — a multi-row
   answer is ordinary for `run_query` and must not be recorded as a failure.
   Lookup tools map exact hit → `ok` and fuzzy fallback / no match → `miss`;
   every `_error_response` return maps by its `kind` to `blocked` or `error`;
   raised exceptions map to `error`.
6. **`validation/examples.py`** — `VerifiedExample` gains
   `expects_metrics: list[str] = field(default_factory=list)`, supported in
   `from_dict` and validated in `__post_init__` (must be a list of non-empty
   strings). It is independent of `expected`, so it does not participate in
   the existing orphaned-key check: a protocol-only row may set it.
   **`_KNOWN_KEYS` must gain `"expects_metrics"`** — `from_dict` copies every
   key not in that frozenset into `metadata`, so omitting it would leave the
   value both set on the record and duplicated into
   `metadata["expects_metrics"]`.
7. **New `validation/conformance.py`** — `Attempt`, `ConformanceResult`,
   `ConformanceReport`, and `evaluate_conformance`.
8. **`validation/__init__.py`** — export `Attempt`, `ConformanceReport`,
   `ConformanceResult`, `ToolCall`, `ToolRecorder`, `evaluate_conformance`
   (the first two recorder types re-exported from `core/recorder.py`); add to
   `__all__` and to `tests/test_public_api.py`.
9. **New `examples/revenue_agent/evaluate_conformance.py`** — demo alongside
   the existing `verify_examples.py`.
10. **`README.md`** — a section following "Validating a verified-examples
   corpus", presenting the three passes as a progression.

No new dependency, so no dependency-floor work.

## Error handling

| Condition | Handling |
|---|---|
| Agent raises or times out | Consumer catches, builds `Attempt(error=...)` → `answer="error"`, `protocol="unchecked"` → fails `ok` |
| `ContractSessionLimitError` | Same path — the consumer catches it and passes `error=str(e)`. `from_session` does **not** auto-detect an exhausted budget: `Attempt` has no `reasons` field to record it on, and a budget breach after a correct answer is not necessarily a failed attempt |
| Empty `attempts` list | `ok is False` |
| Recorder reused across questions | `from_session` marks the recorder consumed; a second call raises `ValueError` |
| `example.question` empty | The row cannot be evaluated. The consumer skips it; `summary()` reports the skipped count so an unevaluatable corpus is visible |

## Testing

Three tiers, none of which call a model.

**Tier 1 — verdict truth table.** `tests/test_validation/test_conformance.py`.
Hand-built `Attempt` objects asserting every state transition. The
tests cover every *reachable* combination of the 5 answer states, 5 protocol
states, and 4 `answer_source` values — many pairings are unreachable by
construction and are asserted as such — plus the specific outliers this design
defends against — non-scalar tail, deduped reruns, sanity-check query
after the answer, relative-time agent SQL, and `declared` overriding
`sole_scalar`. Pure functions; no fixtures.

**Tier 2 — recorder completeness.** `tests/test_tools/`. The real failure mode
is a *missing* log line: someone adds a tenth tool or a tenth early return, it
silently never records, and protocol verdicts quietly degrade.

Two tests, because one does not cover the other:

- **Per tool.** A parametrized test over the tool registry asserts every tool
  emits at least one `ToolCall` when a recorder is attached. Catches a wholly
  uninstrumented new tool.
- **Per outcome path.** A parametrized test over each multi-outcome tool's
  return paths asserts the *right* `outcome` on each. `run_query` alone has six
  (limit exceeded, validation blocked, no adapter, execute failure, result-check
  blocked, success), and the per-tool test above passes on the success path
  alone — so without this second test a dropped `blocked` log line ships green,
  which is the exact regression that silently turns `violated` rows into
  `followed` ones.

This mirrors the reasoning already recorded against `getattr(exp, ..., None)`
in `pyproject.toml`: what the library can enforce — or here, observe — must not
depend on which pieces happened to get wired up.

**Tier 3 — end-to-end with a scripted fake agent.** A plain async function
calling the real tools in a scripted order. Four scripts: follows protocol;
skips a declared `expects_metrics` lookup; produces an answer with zero
`run_query` (contamination); reruns an identical query (dedupe). Exercises
`from_session`, the recorder, and the verdicts together, deterministically and
at no cost.

**Demo.** `examples/revenue_agent/evaluate_conformance.py` runs a scripted
agent against the existing `verified_examples.yml` and DuckDB fixture,
preserving the README's promise that examples run with no API key.

**Not tested:** real agent behavior. Tier 3 proves the harness is correct, not
that any model follows a given contract. That answer comes only from the
consumer's own run against a real model, and the library must not imply
otherwise by shipping a green test.

## CI integration guidance

Pass 3 differs in kind from passes 1 and 2: it runs an agent, so it costs money
per invocation, needs a credential, takes minutes, and is **nondeterministic** —
the same contract can pass and fail on consecutive runs from sampling alone.
Wired as a hard all-must-match gate on every PR it will flake, and a flaky gate
gets disabled.

Recommended shape, documented in the README:

| Pass | Trigger | Gate |
|---|---|---|
| 1 `validate_examples` | every PR | hard, all-must-pass |
| 2 `check_example_answers` | every PR | hard, all-must-pass |
| 3 `evaluate_conformance` | path-filtered on contract / semantic YAML changes, plus nightly | threshold over repeats via `pass_rate()`, or advisory with `summary()` posted as a PR comment |

## Corpus readiness

`VerifiedExample.question` currently defaults to `""` and is documented as
non-load-bearing. Pass 3 can only evaluate rows carrying a question, and P2
only judges rows carrying `expects_metrics`. Adopting this feature therefore
begins with populating `question` — and, where protocol matters,
`expects_metrics` — on the rows to be evaluated. Rows without a question are
skipped and counted, never errors.
