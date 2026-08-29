# Certified breakdown answers (`expected_rows`)

Design for issue #82. Written 2026-08-29, against v0.46.0.

## Problem

`VerifiedExample.expected` is a single `float`, so a certified answer can only
ever be a scalar. A corpus row whose answer is a *breakdown* — revenue by
region, a top-N list, any group-by — has nowhere to put its answer, so it omits
`expected` and is then silently unasserted on both passes:

- `check_example_answers` runs a row only when `status == "valid"` **and**
  `example.expected is not None`, so the row produces no result at all.
- `evaluate_conformance` returns `answer="skipped"`, which `_result_ok` passes.

A breakdown row is therefore indistinguishable, in both reports, from a row that
was asserted and matched. The compliance layer still checks it — it is `valid`
in pass 1 — so the row is not invisible; it is never checked for *correctness*.

This is not an exotic shape. `examples/revenue_agent/verified_examples.yml`'s
**first entry** is `revenue-by-region`, a `GROUP BY`. `conformance._select_answer`
names the case in a comment. The README calls drill dimensions "the exhaustive
GROUP BY axes ('revenue by region') that dominate weekly-review diagnosis". The
shape the docs call dominant is the one shape the corpus cannot certify.

**Measured impact.** In a consuming analytics-agent corpus of 20 reviewed cases,
15 carry a certified answer and **9 of those 15 are breakdowns**. The pass that
exists to catch "compliant SQL, wrong number" reaches 40% of the corpus that has
an answer to check. The failure it misses is ordinary: a `GROUP BY` returning the
right *total* with the wrong *split*, or silently dropping a group. Both are
contract-compliant and both survive every check the library has today.

## Decisions taken before design

Three forks were settled with the maintainer up front, because each changes the
surface materially.

**Scope: `check_example_answers` only.** `evaluate_conformance` keeps returning
`skipped` for breakdown rows. Grading a breakdown there means the recorder must
carry something about the agent's *result*, where today a `ToolCall` holds only
`scalar`, `row_count` and `relative_time`.

This was first framed as a privacy question — whether a governance log may hold
warehouse rows — and that framing was wrong on the facts. Nothing in the library
serializes an `Attempt` or a `ToolCall`: there is no `asdict`, no dump, no file
write, and `evaluate_conformance` is a pure function over an in-memory structure
built in the consumer's own process. The attempt already carries result data
besides — `final_text` is the agent's answer, which for a breakdown *is* the
numbers, and `ToolCall.args` holds the SQL verbatim including literal filter
predicates. `run_query` returns every row uncapped, so the rows have already
reached the model's context. Retaining them alongside adds no exposure.

What is real is **size**: every row of every successful query, across a whole
corpus run, in a structure that today holds a handful of scalars. The likely
answer is not to retain rows at all but the **keyed digest** — the `{key: value}`
map that this design's comparison already consumes — which is bounded by group
count and drops the raw result. A breakdown answer is low-cardinality by nature;
that is what separates a certified answer from a data dump, so a cap on group
count is both a memory guard and a sanity check.

That is a separate piece of work with its own design, not a side effect of
closing the corpus gap. The field below is shaped so it can land later with no
schema change.

**Unordered comparison by default, `ordered: true` opt-in.** `GROUP BY` without
`ORDER BY` has no guaranteed row order — demonstrated in this very repo, where
`revenue_agent`'s demo rows reordered between runs and broke a snapshot gate.
Defaulting to unordered means a correct query never fails on row order. A
genuine top-N row, where the order *is* the answer, opts in.

**Row identity is inferred from cell types, not declared.** Non-numeric cells
are the key; numeric cells are the values. This is deliberately the opposite of
the call made for `convention`, which is *declared* because which factor absorbs
a cross term is not derivable from the schema at any level of model
intelligence. A row's key is different in kind: it is derivable at runtime from
the result's own cell types, so inferring it is reading what the query returned,
not guessing at intent. Declare what the data cannot tell you; infer what it
can.

## Data model

`VerifiedExample` gains two optional fields:

```python
expected_rows: list[list[Any]] | None = None   # the certified breakdown
ordered: bool = False                          # order is part of the answer
```

`expected` is unchanged (`float | None`), and `_scalar`'s "exactly one column,
at most one row" rule is untouched. A row declares one assertion or the other,
never both — a sibling field rather than a widened `expected`, so the two can
never be ambiguous and `_scalar`'s strictness stays enforceable.

`_KNOWN_KEYS` gains `expected_rows` and `ordered`, so neither lands inertly in
`metadata` when misspelled or misplaced.

Corpus shape:

```yaml
- id: revenue-by-region
  question: "total revenue by region"
  sql: >
    SELECT c.region, SUM(o.amount) AS revenue
    FROM analytics.orders o JOIN analytics.customers c ON o.customer_id = c.id
    WHERE o.tenant_id = 'acme' AND o.status = 'completed'
    GROUP BY c.region
  expected_rows:
    - [EMEA, 5000.00]
    - [APAC, 3000.00]
    - [AMER, 2700.00]

- id: top-3-regions
  ordered: true                # order IS the answer
  expected_rows:
    - [EMEA, 5000.00]
    - [APAC, 3000.00]
    - [AMER, 2700.00]
```

## Comparison: a new `validation/_rows.py`

Placed beside `_scalar.py` and for the reason that module states outright: one
implementation of the rule, so a later conformance-side consumer cannot acquire
a second chance to get it wrong.

### Key partition

Key positions are derived **once, from the expected rows**. The certified answer
is the reference — the same choice `_compare` makes when it anchors both the
tolerance and `rel_diff` on `expected` rather than on whatever the query
returned. Positions holding a non-numeric value are key positions; positions
holding a number are value positions. Every expected row must agree on the
partition, which is checked at load.

### Cell rules

- **Key cells** compare as `str(cell).strip()`, so `2025` parsed from YAML and
  `Decimal("2025")` returned by DuckDB agree rather than failing on type. This
  is textual, not numeric: a key whose two sides render differently
  (`Decimal("2025.00")` against `2025`) reports as a missing-plus-unexpected
  group pair. That is the honest reading — the comparison cannot know whether
  two differently-rendered keys are one group — and the named pair says exactly
  what to fix. A key cell is never compared with tolerance; tolerance belongs to
  measurements, and a group name is not one.
- **Value cells** go through the existing `_compare`, inheriting the row's
  `rel_tol` / `abs_tol`. One tolerance rule, one place to set it.
- **`null`** in YAML is `None` and matches only a SQL `NULL`. A `None` where a
  number was certified is a value mismatch, not an error: the query ran and
  returned an answer, and that answer is wrong.

### The three difference kinds

Named separately, because "which groups differ" is the diagnostic this feature
exists to produce and collapsing it into a bare `False` would waste the work:

| Kind | Meaning |
|---|---|
| missing group | a key in `expected_rows`, absent from the result |
| unexpected group | a key in the result, absent from `expected_rows` |
| value mismatch | key matched, number outside tolerance; reported with its diff |

### Ordered mode

Pairs rows by position rather than by key, with the same cell rules. A row-count
difference is reported as a count, not as N missing groups — under `ordered`,
position is identity, so "row 4 is missing" is a claim the comparison cannot
support.

### Where it refuses rather than guesses

- **Every cell numeric** (`GROUP BY year` → `[2025, 5000]`): no key exists and
  every row collides. Raises, naming `ordered: true` as the fix — which is also
  better SQL, since a query grouped by year should be ordered by it. Detected at
  load where the expected rows alone prove it, so the failure arrives before any
  adapter is touched.
- **Duplicate key among the actual rows**: the query returned two rows for one
  group. No pairing resolves that honestly, so it is an `error` result naming
  the duplicated key.
- **Column-count mismatch** between `expected_rows` and the result: an `error`,
  naming both counts.
- **A non-numeric cell at a value position in the result**: the certified answer
  says column 2 is a measurement and the query returned `"N/A"` there. Caught
  explicitly as an `error` naming the column and the offending value, rather
  than left to `float()` raising inside `_compare` — the batch guard would turn
  that into a result whose `reason` is a bare coercion message that names
  neither the column nor the row.

A row whose cells are **all** key positions — `[[EMEA], [APAC]]`, no measure —
is legal and asserts that the query returns exactly these groups, with nothing
checked about their values. It falls out of the rules above rather than needing
its own, and it is a useful assertion in its own right: a `GROUP BY` that
silently drops a group fails it.

## Load-time validation

All of it in `__post_init__`, so a corpus row and a directly-constructed record
are held to identical rules — the reason the existing assertion fields validate
there rather than in `from_dict`.

- `expected` and `expected_rows` both set → raise.
- `expected_rows` must be a non-empty list of equal-length lists with at least
  one cell per row.
- **An empty `expected_rows: []` is rejected**, and this costs nothing.
  "This query returns no rows" is a real certified answer — it is the shape of
  every data-quality invariant worth pinning (no orders without a tenant, no
  negative amounts, no orphaned customer IDs) — but it is already expressible in
  the field built for it:

  ```yaml
  sql: "SELECT COUNT(*) FROM analytics.orders WHERE tenant_id = 'acme' AND amount < 0"
  expected: 0
  ```

  Not a workaround: `_compare` was deliberately written for a zero reference,
  with the three-branch `rel_diff` guard whose comment says a certified answer
  of zero is legitimate. The count form is also strictly better on failure —
  `expected_rows: []` failing says only that rows came back, while `expected: 0`
  failing says *how many*, and for an invariant the difference between 1 bad row
  and 40,000 is the difference between two incidents.

  So the empty list carries no capability, and rejecting it keeps it as
  unambiguous evidence of a mistake: a truncated file, a templating bug, or a
  key someone meant to fill in later all produce exactly these bytes. Left
  legal, an unfilled row sitting against a sparse fixture — where the query
  genuinely returns nothing — would pass. Two independent mistakes cancelling
  into a green gate is the failure class this whole feature exists to close.
- Duplicate keys *within* `expected_rows` → raise **when `ordered` is unset**.
  Unordered comparison pairs by key, so a corpus stating one group twice has
  stated something the comparison cannot resolve. Under `ordered: true` position
  is identity and keys are not, so a repeated key is legitimate there and must
  not raise — a ranking may name the same category twice.
- A row with no non-numeric cell while `ordered` is unset → raise, per above.
- `ordered: true` without `expected_rows` → orphan error, the same family as a
  `rel_tol` set without an assertion to apply it to.

### The widened orphan guard

Today's check keys on `self.expected is None`, and its list of orphans is
`rel_tol` / `abs_tol` / `time_scoped`. With a second assertion field, that check
would fire on every valid breakdown row carrying a tolerance. It becomes "no
assertion declared at all" — `expected is None and expected_rows is None`.

The same substitution is needed in `check_example_answers`, whose filter reads
`example.expected is None` today, and in `ExampleAnswerReport.summary()`'s
empty-report text, which says "no example declared an `expected` value" and
would be wrong the moment a corpus asserts only breakdowns.

## Result and reporting

`ExampleAnswerResult` keeps its four statuses — `match`, `mismatch`,
`unassertable`, `error` — with unchanged meanings, so `ok`, the four filter
properties, and every documented `report.ok and answers.ok` gate keep working
untouched. It gains three fields:

```python
expected_rows: list[list[Any]] | None = None
actual_row_count: int | None = None
row_differences: list[str] = field(default_factory=list)
```

`abs_diff` / `rel_diff` stay `None` for a breakdown row: there is no single diff
to report, which is precisely why the differences are carried as their own field
rather than crammed into `reason`.

`unassertable` applies unchanged — a breakdown over a rolling window decays
against its certified answer exactly as a scalar does, so the relative-time
refusal runs before execution as it does today.

`summary()` renders a breakdown mismatch as counts plus its **first three**
named differences, then `(and N more)`, so a fifty-group query cannot flood an
MR comment. The counts are always complete; only the naming is capped, so a
reader is never misled about how much differed. `row_differences` on the result
holds every difference, uncapped, for a consumer that wants them all:

```
- mismatch `revenue-by-region`: 3 expected groups, 1 missing (APAC),
  1 unexpected (LATAM), 1 value mismatch (EMEA: expected 5000, actual 5200,
  rel diff 0.04, rel_tol 1e-09)
```

## Testing

TDD, red first, in the repo's layer order — each layer independently testable
under its own suite, per CLAUDE.md.

1. **`tests/test_validation/test_rows.py`** — the comparison rule as a pure
   function: key partition; tolerance applied to value cells and not to key
   cells; each of the three difference kinds named; ordered mode pairing by
   position and reporting a row-count difference as a count; both refusals
   (all-numeric row, duplicate actual key); `null` handling.
2. **`tests/test_validation/test_examples.py`** — load validation: mutual
   exclusion, ragged rows, empty list, duplicate expected keys, orphaned
   `ordered`, and the widened orphan guard accepting a tolerance alongside
   `expected_rows`.
3. **`check_example_answers` integration** against the existing DuckDB fixture,
   including a row that matches, one with a dropped group, and one with the
   right total and the wrong split — the failure the feature exists to catch.
4. **`summary()`** rendering, including the difference cap.
5. **The corpus itself.** `examples/revenue_agent/verified_examples.yml` #1
   `revenue-by-region` gains a real `expected_rows` and becomes an asserted row.
   That example is both the motivating case and the proof: the file that
   demonstrated the gap should demonstrate the fix.

   This does **not** touch the golden output files. Those are generated from
   each `agent.py`, and no `agent.py` reads the corpus — `verify_examples.py`
   and `evaluate_conformance.py` are its only readers, and the CI step that runs
   `verify_examples.py` is still gated by its own status greps rather than by a
   snapshot. Those greps assert that `[MATCH `, `MISMATCH` and `UNASSERTABLE`
   each still appear, and a matching breakdown row keeps all three present, so
   the step passes unchanged. Worth adding a grep for the new breakdown row's
   own status line, so the corpus's first entry becoming asserted is something
   CI states rather than something it merely tolerates.

## Out of scope

- **`evaluate_conformance` grading breakdowns**, and any change to what
  `ToolCall` retains. Tracked separately; see the scope decision above for why
  the constraint is size rather than privacy, and for the keyed-digest approach
  that work should start from.
- **`key_columns`.** The declared-key escape hatch for an unordered comparison
  over a numeric dimension. `ordered: true` already answers that case, and
  adding an optional field later is additive for every row that does not use it.
- **Nearest-match pairing.** Rejected outright, not deferred: it trades a
  precise "APAC is missing" for "some row did not match", and the precise
  version is the entire point of the feature.
