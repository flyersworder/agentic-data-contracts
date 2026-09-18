# Sensitivity Checks for Dialects That Parse Only After Normalization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `check_sensitivity` check queries in a dialect sqlglot parses only
after a `SqlNormalizer` rewrites them — Denodo VQL in this library's known
deployment — instead of returning `unchecked` for every one.

**Architecture:** Count the target table's references in the *normalized* text
(which sqlglot can parse), locate them in the *original* text by tokenizing it
(tokenizing is far more permissive than parsing), edit the original, then prove
the edit: the edited body must normalize, parse and hold no reference to the
target, and the final text must normalize and parse as one statement. The
original text is what executes. The `SqlNormalizer` protocol does not change.

**Tech Stack:** Python ≥3.12, sqlglot (floor `>=28.6`), DuckDB for tests, pytest,
ruff + ty via prek.

**Spec:** `docs/superpowers/specs/2026-09-08-sensitivity-checks-design.md`,
section **"Dialects that parse only after normalization"**. Read it alongside
this plan. This plan corrects the spec in three places, each flagged inline as
**DEVIATION** with its reason, and the spec is amended to match in the same
commit as this plan.

## Global Constraints

- Python `>=3.12`. The branch is `feat/sensitivity-checks`, version `0.52.0`
  (unreleased — this work extends it; no version bump).
- **No new dependencies.** **No change to the `SqlNormalizer` protocol**
  (`normalize_sql(sql) -> str`).
- sqlglot floor is **`>=28.6`**; the lockfile resolves **30.18**. Every task runs
  its tests at both: `uv run pytest <files> -q` and
  `uv run --resolution lowest-direct pytest <files> -q` (resolves 28.6). The
  floor run rewrites `uv.lock`; restore it afterwards with
  `git checkout uv.lock && uv sync --all-extras -q`, and never commit a floor lock.
- **The library never writes, and never regenerates SQL.** The executed string
  is always the caller's original text with spans replaced. Never call `.sql()`,
  `transpile` or any generator on the caller's query. **The normalized text is
  parsed, never executed.**
- **Fail closed.** Every failure this plan adds returns `unchecked` and executes
  nothing for the affected property, or raises `ValueError` for malformed input —
  never a verdict the check did not earn.
- Run linters through **prek**, never bare: `prek run --all-files`. Ruff
  `select = ["E", "F", "I", "UP"]`, line length 88. **E402 is on**: imports go in
  each file's top-of-file block.
- Run everything Python through `uv run`. There is no bare `python` on PATH.
- **TDD, red first.** Show RED and GREEN for every code change.
- Do not write `file.py:NNN` line references in committed prose — a prek hook
  rejects them.
- **Out of scope — do NOT touch:** the parked minors (WITH-splice double space,
  `_free_alias` substring over-conservatism, the dead `if parsed is None`
  branches, `summary()` test gaps, the `startswith("not_applicable")` status
  coupling, the `''` table-function name, catalog dropping, duplicate
  `properties=` names, a `caller_principal` parameter). An optional offsets
  method on `SqlNormalizer` is a named follow-up — do not build it.

---

## File Structure

| file | change |
|---|---|
| `src/agentic_data_contracts/validation/sensitivity.py` | alias stem; `Normalize` type, `_identity`, `_normalized`, `_normalizer_fn`, `_prove_edit`, `_inject`, `_prove_parses`, `_all_unchecked`; `_spans`, `_rewrite`, `_shadow_tables`, `validate_sensitivity_tables`, `check_sensitivity` gain normalization |
| `tests/test_validation/test_sensitivity.py` | new stem in existing assertions; `_strip_context`, `_VqlNormalizer`, `VqlDuckDBAdapter`, `vql_adapter` fixture; new test classes |
| `README.md`, `CHANGELOG.md` | the "not yet supported" paragraphs become the supported behaviour |
| `docs/superpowers/specs/2026-09-08-sensitivity-checks-design.md` | amended with this plan (DEVIATIONS 1–3) |

---

## The three deviations from the spec

**DEVIATION 1 — Layer 1 sees the normalized text; the Validator gets no
normalizer.** The spec says "Build the `Validator` with the normalizer". But
`Validator.validate` calls the normalizer inside a `try` that catches only
`ParseError` and `TokenError`, so a normalizer raising anything else would escape
`check_sensitivity` as a raw exception — where the spec's guard table requires
every property `unchecked`. The raw text's only other use inside `validate` is
`explain_adapter.explain(sql)`, and `check_sensitivity` builds its Validator with
no explain adapter. So `check_sensitivity` normalizes the query once itself,
turns a failure into `unchecked`, and hands Layer 1 the normalized text. The
result is identical for every non-raising normalizer.

**DEVIATION 2 — `not_applicable` needs the target absent from BOTH texts.** The
spec's guard table maps "the normalized query does not reference the target" to
`not_applicable`, and its test table maps "a normalizer that renames the target"
to `unchecked`. Those contradict: a renamed target leaves *zero* references in
the normalized text, which the first rule would read as `not_applicable`. The
rewrite therefore tokenizes the original before deciding. Zero references and
zero spans is `not_applicable`; any disagreement between the two is the count
guard, `unchecked`. Prototyped before writing this plan: the renaming normalizer
yields "count guard: 1 vs 0".

**DEVIATION 3 — a multi-statement shadow is treated as unparseable.** The spec's
final-text proof says "parses". At the sqlglot 28.6 floor `parse_one` keeps only
the first statement, so "parses" alone cannot mean "is one statement", and
`_shadow_tables` today waves `SELECT … ; DELETE FROM <governed table>` through
governance because it sees only governed tables. **This is not exploitable** —
verified: the CTE's parentheses make the semicolon a syntax error, the parser
and DuckDB both reject the whole string, and no row changes. But it fails as an
opaque engine error. `_shadow_tables` and the final-text proof therefore count
statements with the Validator's `_is_multi_statement`, so both gates report it
clearly.

---

### Task 1: Letter-leading alias stem

**Files:**
- Modify: `src/agentic_data_contracts/validation/sensitivity.py`
- Test: `tests/test_validation/test_sensitivity.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `_ALIAS_STEM = "sens_shadow_"`; `_free_alias(sql)` returns
  `sens_shadow_N`. Every later task's assertions use this stem.

Some engines require an unquoted identifier to start with a letter — Oracle's
documentation says so — so `__sens_0` is a portability bug, and the letter-leading
stem also removes the one `WITH` detail the VQL grammar leaves unconfirmed.

- [ ] **Step 1: Update the assertions (the failing test)**

In `tests/test_validation/test_sensitivity.py`, replace every `__sens_` with
`sens_shadow_` — in `TestRewrite` (the `startswith("WITH __sens_0 AS (")`,
`"FROM __sens_0 …"` and `"v AS (SELECT 1 FROM __sens_0)"` assertions, and the
`WITH RECURSIVE __sens_0` one), in `test_alias_collision_picks_the_next_free_name`,
and in `test_base_is_executed_once_and_reused` (`"__sens_" not in s`).

```bash
sed -i '' 's/__sens_/sens_shadow_/g' tests/test_validation/test_sensitivity.py
grep -c "__sens_" tests/test_validation/test_sensitivity.py   # expect 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_sensitivity.py -q`
Expected: FAIL — `TestRewrite` and the alias-collision test fail with
`AssertionError` (the output still says `__sens_0`).

- [ ] **Step 3: Change the stem**

In `sensitivity.py`, replace `_ALIAS_STEM = "__sens_"` with:

```python
#: Letter-leading, because some engines require an unquoted identifier to start
#: with a letter (Oracle documents it) and the VQL grammar names the CTE a
#: `<query name>` without spelling out its identifier rules.
_ALIAS_STEM = "sens_shadow_"
```

and change `_free_alias`'s docstring to `"""The first ``sens_shadow_N`` the query does not already contain."""`.

- [ ] **Step 4: Run the tests to verify they pass, at both sqlglot versions**

```bash
uv run pytest tests/test_validation/test_sensitivity.py -q
uv run --resolution lowest-direct pytest tests/test_validation/test_sensitivity.py -q
git checkout uv.lock && uv sync --all-extras -q
```

Expected: PASS at both.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/sensitivity.py tests/test_validation/test_sensitivity.py
git commit -m "fix(sensitivity): letter-leading CTE alias, portable to Oracle and VQL"
```

---

### Task 2: Normalizer-aware rewrite, with the edit proved

**Files:**
- Modify: `src/agentic_data_contracts/validation/sensitivity.py`
- Test: `tests/test_validation/test_sensitivity.py`

**Interfaces:**
- Consumes: `_ALIAS_STEM` from Task 1; the existing `_Refused`, `_table_refs`,
  `_spans`, `_free_alias`, `_cte_aliases`; `_is_multi_statement(sql, dialect)`
  from `agentic_data_contracts.validation.validator`.
- Produces:
  - `Normalize = Callable[[str], str]` and `_identity(sql: str) -> str`;
  - `_normalized(sql: str, normalize: Normalize, *, what: str) -> str`, raising
    `_Refused("normalizer failed on <what>: …")`;
  - `_prove_edit(body: str, target: str, *, dialect: str | None, normalize: Normalize) -> None`;
  - `_inject(body: str, alias: str, shadow: Shadow, *, dialect: str | None) -> str`;
  - `_prove_parses(final: str, *, dialect: str | None, normalize: Normalize) -> None`;
  - `_rewrite(sql: str, shadow: Shadow, *, dialect: str | None, normalize: Normalize = _identity) -> str`,
    which now raises ONLY `_Refused`.
  Tasks 3 and 4 call `_rewrite(..., normalize=...)` and use `Normalize`/`_identity`.

This task is pure string and AST work — no database. The test helper
`_strip_context` simulates a normalizer for VQL's `CONTEXT (…)` clause, which
`sqlglot.parse_one` rejects but `sqlglot.tokenize` accepts (both probed).

- [ ] **Step 1: Write the failing tests**

Add `import re` to the top-of-file import block of
`tests/test_validation/test_sensitivity.py`, merge `_identity`, `_prove_edit` and
`_prove_parses` into its existing `from agentic_data_contracts.validation.sensitivity import (...)`
block (E402 is on), and add this helper directly after the `SHADOW = Shadow(...)`
definition near the top of the file:

```python
#: VQL's CONTEXT clause: sqlglot cannot parse it, but it tokenizes, and Denodo
#: accepts it. Stripping it is the smallest faithful stand-in for a Denodo
#: normalizer -- syntax only, table names untouched.
_CONTEXT = re.compile(r"\s*\bCONTEXT\s*\([^)]*\)", re.IGNORECASE)
_CTX = " CONTEXT ('i18n' = 'us_est')"


def _strip_context(sql: str) -> str:
    return _CONTEXT.sub("", sql)
```

Then append these classes after `TestRewrite`:

```python
class TestNormalizedRewrite:
    """References are COUNTED in the normalized text and LOCATED in the original."""

    def test_edits_the_original_when_only_the_normalized_text_parses(self) -> None:
        sql = f"SELECT count(*) FROM mkt.touchpoints{_CTX}"
        with pytest.raises(sqlglot.errors.ParseError):
            sqlglot.parse_one(sql, dialect="duckdb")
        out = _rewrite(sql, MKT_SHADOW, dialect="duckdb", normalize=_strip_context)
        assert out.startswith("WITH sens_shadow_0 AS (")
        assert "FROM sens_shadow_0 CONTEXT ('i18n' = 'us_est')" in out  # original kept
        sqlglot.parse_one(_strip_context(out), dialect="duckdb")

    def test_absent_from_both_texts_is_not_applicable(self) -> None:
        with pytest.raises(_Refused, match="not_applicable"):
            _rewrite(
                f"SELECT count(*) FROM mkt.lead_scores{_CTX}",
                MKT_SHADOW,
                dialect="duckdb",
                normalize=_strip_context,
            )

    def test_a_renaming_normalizer_trips_the_count_guard(self) -> None:
        # The normalized text holds zero references and the original holds one.
        # That is a disagreement, not an absence (DEVIATION 2).
        with pytest.raises(_Refused, match="count guard"):
            _rewrite(
                "SELECT count(*) FROM mkt.touchpoints",
                MKT_SHADOW,
                dialect="duckdb",
                normalize=lambda s: s.replace("mkt.touchpoints", "mkt.tp"),
            )

    def test_an_untokenizable_original_is_refused(self) -> None:
        # Normalizes to parseable SQL, but the original's unterminated literal
        # cannot be tokenized -- so its spans cannot be found.
        with pytest.raises(_Refused, match="could not be tokenized"):
            _rewrite(
                "SELECT 'x FROM mkt.touchpoints",
                MKT_SHADOW,
                dialect="duckdb",
                normalize=lambda s: "SELECT 1 FROM mkt.touchpoints",
            )

    def test_a_normalizer_that_raises_is_refused(self) -> None:
        def boom(sql: str) -> str:
            raise RuntimeError("no VQL today")

        with pytest.raises(_Refused, match="normalizer failed"):
            _rewrite(
                "SELECT count(*) FROM mkt.touchpoints",
                MKT_SHADOW,
                dialect="duckdb",
                normalize=boom,
            )


class TestProof:
    """The edit is proved, not trusted. Tested directly, since a count that
    agrees while the wrong span was edited is hard to reach naturally."""

    def test_refuses_a_body_that_still_references_the_target(self) -> None:
        with pytest.raises(_Refused, match="remain after the edit"):
            _prove_edit(
                "SELECT 1 FROM main.payments",
                "main.payments",
                dialect="duckdb",
                normalize=_identity,
            )

    def test_accepts_a_body_with_no_reference_left(self) -> None:
        _prove_edit(
            "SELECT 1 FROM sens_shadow_0",
            "main.payments",
            dialect="duckdb",
            normalize=_identity,
        )

    def test_refuses_when_the_normalizer_fails_on_the_body(self) -> None:
        def boom(sql: str) -> str:
            raise RuntimeError("no")

        with pytest.raises(_Refused, match="could not be proved"):
            _prove_edit(
                "SELECT 1 FROM sens_shadow_0",
                "main.payments",
                dialect="duckdb",
                normalize=boom,
            )

    def test_refuses_a_final_text_of_two_statements(self) -> None:
        with pytest.raises(_Refused, match="could not be proved"):
            _prove_parses("SELECT 1; SELECT 2", dialect="duckdb", normalize=_identity)

    def test_refuses_a_final_text_that_does_not_parse(self) -> None:
        with pytest.raises(_Refused, match="could not be proved"):
            _prove_parses("SELECT FROM )(", dialect="duckdb", normalize=_identity)
```

`import sqlglot` is already in this file's import block (Task 4 of the first plan
added it for `_rw()`); confirm rather than duplicate it.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_sensitivity.py -q`
Expected: FAIL at collection — `ImportError: cannot import name '_identity'`.

- [ ] **Step 3: Implement**

In `sensitivity.py`:

(a) Change `from collections.abc import Sequence` to
`from collections.abc import Callable, Sequence`, and
`from agentic_data_contracts.validation.validator import Validator` to
`from agentic_data_contracts.validation.validator import Validator, _is_multi_statement`.

(b) Directly after `class _Refused`, add:

```python
#: A normalizer as the rewrite sees it: dialect text in, sqlglot-parseable text
#: out. The identity when the caller has no SqlNormalizer.
Normalize = Callable[[str], str]


def _identity(sql: str) -> str:
    return sql


def _normalized(sql: str, normalize: Normalize, *, what: str) -> str:
    """*sql* normalized, or a refusal naming what failed to normalize."""
    try:
        return normalize(sql)
    except Exception as e:  # noqa: BLE001 - any normalizer failure is one outcome
        raise _Refused(f"normalizer failed on {what}: {e}") from e
```

(c) In `_spans`, replace `toks = list(sqlglot.tokenize(sql, dialect=dialect))` with:

```text
    try:
        toks = list(sqlglot.tokenize(sql, dialect=dialect))
    except Exception as e:  # noqa: BLE001 - TokenError, whatever the version
        raise _Refused(f"original text could not be tokenized: {e}") from e
```

(d) Replace the whole `_rewrite` function with these four functions:

```python
def _prove_edit(
    body: str, target: str, *, dialect: str | None, normalize: Normalize
) -> None:
    """Refuse unless the edited body holds no real reference to *target*.

    Runs on the body BEFORE the CTE is injected: the shadow itself legitimately
    reads the target, so the final text always contains one. Count equality
    alone can be fooled -- a normalizer that drops one reference while the
    original holds a look-alike in table position gets the wrong span edited
    with the counts still agreeing -- so this checks the outcome, not the
    arithmetic.
    """
    try:
        remaining = _table_refs(
            _normalized(body, normalize, what="the edited query"),
            target,
            dialect=dialect,
        )
    except _Refused as e:
        raise _Refused(f"rewrite could not be proved: {e}") from e
    if remaining:
        raise _Refused(
            f"rewrite could not be proved: {remaining} reference(s) to "
            f"{target!r} remain after the edit"
        )


def _inject(body: str, alias: str, shadow: Shadow, *, dialect: str | None) -> str:
    """Put the shadow in front of *body* as a CTE, splicing into its own WITH."""
    try:
        toks = list(sqlglot.tokenize(body, dialect=dialect))
    except Exception as e:  # noqa: BLE001 - the original tokenized, so unexpected
        raise _Refused(f"rewrite could not be proved: {e}") from e
    # The newline ends any trailing line comment in the shadow before the
    # closing paren, which the comment would otherwise swallow.
    cte = f"{alias} AS ({shadow.sql}\n)"
    if toks and toks[0].token_type == sqlglot.TokenType.WITH:
        head = toks[1] if len(toks) > 1 else None
        cut = (
            head.end + 1
            if head is not None and head.token_type == sqlglot.TokenType.RECURSIVE
            else toks[0].end + 1
        )
        return f"{body[:cut]} {cte}, {body[cut:]}"
    return f"WITH {cte} {body}"


def _prove_parses(final: str, *, dialect: str | None, normalize: Normalize) -> None:
    """Refuse unless the injected text is exactly one statement that parses.

    "Parses" alone is not enough: at the sqlglot 28.6 floor ``parse_one``
    silently keeps only the first statement, so the count is checked the way
    Layer 1 checks it.
    """
    try:
        normalized = _normalized(final, normalize, what="the rewritten query")
        if _is_multi_statement(normalized, dialect):
            raise _Refused("it holds more than one statement")
        sqlglot.parse_one(normalized, dialect=dialect)
    except Exception as e:  # noqa: BLE001 - _Refused, ParseError, TokenError alike
        raise _Refused(f"rewrite could not be proved: {e}") from e


def _rewrite(
    sql: str,
    shadow: Shadow,
    *,
    dialect: str | None,
    normalize: Normalize = _identity,
) -> str:
    """The caller's own text, with *shadow.table* pointed at an injected CTE.

    References are COUNTED in the normalized text, which sqlglot can parse, and
    LOCATED in the original, which is what executes: a ``SqlNormalizer`` returns
    no offsets, and the protocol requires the original to run. The two must
    agree, and the edit is then proved rather than trusted. Raises only
    ``_Refused``.
    """
    refs = _table_refs(
        _normalized(sql, normalize, what="the query"), shadow.table, dialect=dialect
    )
    spans = _spans(sql, shadow.table, dialect=dialect)
    # Absent from BOTH texts is an outcome. Absent from only one is a
    # disagreement -- a normalizer that renamed the target reads as zero
    # references -- so it falls to the count guard instead of posing as
    # not_applicable.
    if refs == 0 and not spans:
        raise _Refused("not_applicable: query does not reference the target")
    if len(spans) != refs:
        raise _Refused(
            f"count guard: {len(spans)} spans in the original text vs {refs} "
            "table references after normalization; refusing to edit"
        )
    alias = _free_alias(sql)
    body = sql
    for start, end in sorted(spans, reverse=True):  # right-to-left
        body = body[:start] + alias + body[end:]
    _prove_edit(body, shadow.table, dialect=dialect, normalize=normalize)
    final = _inject(body, alias, shadow, dialect=dialect)
    _prove_parses(final, dialect=dialect, normalize=normalize)
    return final
```

- [ ] **Step 4: Run the tests to verify they pass, at both sqlglot versions**

```bash
uv run pytest tests/test_validation/test_sensitivity.py -q
uv run --resolution lowest-direct pytest tests/test_validation/test_sensitivity.py -q
git checkout uv.lock && uv sync --all-extras -q
```

Expected: PASS at both — the new classes, and every existing `TestRewrite` and
`TestCheckSensitivity` test unchanged (the identity normalizer is today's path
plus the proof).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/sensitivity.py tests/test_validation/test_sensitivity.py
git commit -m "feat(sensitivity): count references after normalization, edit the original, prove the edit"
```

---

### Task 3: Shadow governance after normalization

**Files:**
- Modify: `src/agentic_data_contracts/validation/sensitivity.py`
- Test: `tests/test_validation/test_sensitivity.py`

**Interfaces:**
- Consumes: `Normalize`, `_identity`, `_is_multi_statement` (Task 2).
- Produces:
  - `_normalizer_fn(sql_normalizer: SqlNormalizer | None) -> Normalize`;
  - `_shadow_tables(shadow: Shadow, *, dialect: str | None, normalize: Normalize = _identity) -> set[str] | None`;
  - `validate_sensitivity_tables(contract, metrics, *, dialect=None, sql_normalizer: SqlNormalizer | None = None) -> list[str]`.
  Task 4 calls `_normalizer_fn` and `_shadow_tables(..., normalize=...)`.

- [ ] **Step 1: Write the failing tests**

Add after `_strip_context` in the test file:

```python
class _VqlNormalizer:
    """A standalone SqlNormalizer -- the CI gate takes no adapter."""

    def normalize_sql(self, sql: str) -> str:
        return _strip_context(sql)
```

Add after the `FIRST_TOUCH_PROP = SensitivityProperty(...)` definition:

```python
VQL_SHADOW = Shadow(table="mkt.touchpoints", sql=MKT_SHADOW.sql + _CTX)

VQL_PROP = SensitivityProperty(
    name="attribution_is_first_touch",
    description="A touchpoint after qualification cannot change a first touch.",
    shadow=VQL_SHADOW,
    expect="unchanged",
)
```

Merge `_shadow_tables` into the existing `validation.sensitivity` import block,
then append:

```python
class TestNormalizedShadowGovernance:
    def test_a_shadow_is_read_after_normalization(self) -> None:
        assert _shadow_tables(VQL_SHADOW, dialect="duckdb") is None  # raw: unreadable
        assert _shadow_tables(
            VQL_SHADOW, dialect="duckdb", normalize=_strip_context
        ) == {"mkt.touchpoints", "mkt.lead_scores"}

    def test_a_normalizer_failure_leaves_the_tables_unknown(self) -> None:
        def boom(sql: str) -> str:
            raise RuntimeError("no")

        assert _shadow_tables(MKT_SHADOW, dialect="duckdb", normalize=boom) is None

    def test_a_multi_statement_shadow_leaves_the_tables_unknown(self) -> None:
        # Not exploitable -- the CTE's parentheses make the `;` a syntax error --
        # but governance saw only governed tables and waved it through
        # (DEVIATION 3).
        smuggle = Shadow(
            table="mkt.touchpoints",
            sql="SELECT * FROM mkt.touchpoints; DELETE FROM mkt.touchpoints",
        )
        assert _shadow_tables(smuggle, dialect="duckdb") is None

    def test_the_ci_gate_governs_a_normalized_shadow(self) -> None:
        governed = _contract("touchpoints", "lead_scores")
        assert (
            validate_sensitivity_tables(
                governed,
                [_metric(VQL_PROP)],
                dialect="duckdb",
                sql_normalizer=_VqlNormalizer(),
            )
            == []
        )

    def test_the_ci_gate_without_a_normalizer_cannot_read_it(self) -> None:
        governed = _contract("touchpoints", "lead_scores")
        (problem,) = validate_sensitivity_tables(
            governed, [_metric(VQL_PROP)], dialect="duckdb"
        )
        assert "cannot be parsed" in problem

    def test_the_ci_gate_still_refuses_an_ungoverned_normalized_shadow(self) -> None:
        (problem,) = validate_sensitivity_tables(
            _contract("touchpoints"),
            [_metric(VQL_PROP)],
            dialect="duckdb",
            sql_normalizer=_VqlNormalizer(),
        )
        assert "lead_scores" in problem
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_sensitivity.py::TestNormalizedShadowGovernance -q`
Expected: FAIL — `TypeError: _shadow_tables() got an unexpected keyword argument 'normalize'`
and `validate_sensitivity_tables() got an unexpected keyword argument 'sql_normalizer'`;
the multi-statement test fails because `_shadow_tables` returns `{'mkt.touchpoints'}`.

- [ ] **Step 3: Implement**

(a) Add to the module's top-of-file imports, beside the `validator` import:

```python
from agentic_data_contracts.adapters._normalizer import SqlNormalizer
```

`validator.py` already imports this module at runtime, so it opens no new import
cycle; `test_public_api.py::test_sensitivity_imports_without_a_cycle` guards that.

(b) Directly after `_normalized`, add:

```python
def _normalizer_fn(sql_normalizer: SqlNormalizer | None) -> Normalize:
    """The normalizer as a callable; the identity when there is none."""
    return sql_normalizer.normalize_sql if sql_normalizer is not None else _identity
```

(c) Replace the head of `_shadow_tables` — its signature, docstring and the
`try`/`parse_one` block — so it reads:

```python
def _shadow_tables(
    shadow: Shadow, *, dialect: str | None, normalize: Normalize = _identity
) -> set[str] | None:
    """Qualified names ``shadow.sql`` reads, or None when they cannot be known.

    The shadow is normalized first, so a shadow written in a dialect that parses
    only after normalization is governed like any other. None covers every way
    its tables cannot be read off it: the normalizer raised, the normalized text
    does not parse, or it holds more than one statement. The last is not
    exploitable -- the CTE's parentheses make a statement boundary a syntax
    error -- but refusing it here gives both gates a clear finding instead of
    an opaque engine error.
    """
    try:
        text = normalize(shadow.sql)
        if _is_multi_statement(text, dialect):
            return None
        parsed = sqlglot.parse_one(text, dialect=dialect)
    except Exception:  # noqa: BLE001 - every failure means "tables unknown"
        return None
```

Leave the rest of the function (the `if parsed is None`, the `cast`, the table
walk) unchanged.

(d) Give `validate_sensitivity_tables` the keyword, and pass the normalizer on:

```text
def validate_sensitivity_tables(
    contract: DataContract,
    metrics: list[MetricDefinition],
    *,
    dialect: str | None = None,
    sql_normalizer: SqlNormalizer | None = None,
) -> list[str]:
```

Append to its docstring:

```
    ``sql_normalizer`` should likewise match: a shadow in a dialect that parses
    only after normalization (Denodo VQL) is normalized before its tables are
    read. This gate takes no adapter, so -- unlike ``check_sensitivity`` -- it
    has no adapter to fall back on; pass the normalizer explicitly.
```

and change its loop's call to
`read = _shadow_tables(prop.shadow, dialect=dialect, normalize=normalize)`, with
`normalize = _normalizer_fn(sql_normalizer)` computed once before the loop.

- [ ] **Step 4: Run the tests to verify they pass, at both sqlglot versions**

```bash
uv run pytest tests/test_validation/test_sensitivity.py tests/test_public_api.py -q
uv run --resolution lowest-direct pytest tests/test_validation/test_sensitivity.py tests/test_public_api.py -q
git checkout uv.lock && uv sync --all-extras -q
```

Expected: PASS at both, including the import-cycle test.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/sensitivity.py tests/test_validation/test_sensitivity.py
git commit -m "feat(sensitivity): govern shadows after normalization; refuse multi-statement shadows"
```

---

### Task 4: `check_sensitivity` normalizes, with the adapter as fallback

**Files:**
- Modify: `src/agentic_data_contracts/validation/sensitivity.py`
- Test: `tests/test_validation/test_sensitivity.py`

**Interfaces:**
- Consumes: `Normalize`, `_identity`, `_normalizer_fn`, `_shadow_tables(...,
  normalize=)`, `_rewrite(..., normalize=)` (Tasks 2–3).
- Produces: `check_sensitivity(metric, sql, *, contract, adapter, properties=None,
  repeats=2, dialect=None, sql_normalizer: SqlNormalizer | None = None) -> SensitivityReport`
  and `_all_unchecked(selected, metric, reason) -> SensitivityReport`.

- [ ] **Step 1: Write the failing tests**

(a) Add `from agentic_data_contracts.adapters.base import QueryResult` to the test
file's top-of-file import block.

(b) Replace the `mkt_adapter` fixture with a shared loader and two fixtures:

```python
def _load_mkt(adapter: DuckDBAdapter) -> DuckDBAdapter:
    adapter.execute("CREATE SCHEMA mkt")
    adapter.execute(
        "CREATE TABLE mkt.lead_scores"
        "(lead_id INT, region TEXT, is_mql BOOL, qualified_date INT)"
    )
    adapter.execute(
        "CREATE TABLE mkt.touchpoints(lead_id INT, channel TEXT, touch_date INT)"
    )
    adapter.execute(
        "INSERT INTO mkt.lead_scores VALUES "
        "(1,'DACH',true,100),(2,'DACH',true,120),(3,'APAC',true,130),"
        "(4,'DACH',false,140)"
    )
    adapter.execute(
        "INSERT INTO mkt.touchpoints VALUES "
        "(1,'search',10),(1,'email',50),(2,'social',20),(2,'search',60),"
        "(3,'search',30),(4,'email',40)"
    )
    return adapter


class VqlDuckDBAdapter(DuckDBAdapter):
    """DuckDB that accepts VQL's CONTEXT clause, standing in for Denodo.

    sqlglot cannot parse ``CONTEXT (...)``, so the library must normalize before
    it parses; the engine accepts it, so the ORIGINAL text must be what runs.
    ``normalize_sql`` makes this a ``SqlNormalizer``; ``execute`` strips the
    clause the way Denodo would simply honour it.
    """

    def normalize_sql(self, sql: str) -> str:
        return _strip_context(sql)

    def execute(self, sql: str) -> QueryResult:
        return super().execute(_strip_context(sql))


@pytest.fixture
def mkt_adapter() -> DuckDBAdapter:
    return _load_mkt(DuckDBAdapter(":memory:"))


@pytest.fixture
def vql_adapter() -> VqlDuckDBAdapter:
    return _load_mkt(VqlDuckDBAdapter(":memory:"))  # type: ignore[return-value]
```

If `ty` rejects the `# type: ignore`, make `_load_mkt` generic instead
(`def _load_mkt[A: DuckDBAdapter](adapter: A) -> A:` — Python 3.12 syntax) and
drop the ignore.

(c) Append:

```python
def _spy(adapter, monkeypatch) -> list[str]:
    seen: list[str] = []
    original = adapter.execute

    def spy(sql: str):
        seen.append(sql)
        return original(sql)

    monkeypatch.setattr(adapter, "execute", spy)
    return seen


class _Renaming:
    """Syntax-only it is not: renames the target to another GOVERNED table, so
    Layer 1 and shadow governance both pass and only the count guard sees it."""

    def normalize_sql(self, sql: str) -> str:
        return sql.replace("mkt.touchpoints", "mkt.lead_scores")


class _Raising:
    def normalize_sql(self, sql: str) -> str:
        raise RuntimeError("the normalizer is down")


class TestNormalizedCheckSensitivity:
    def test_fan_out_violates_on_a_dialect_that_parses_only_after_normalization(
        self, vql_adapter, mkt_contract
    ) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT + _CTX,
            contract=mkt_contract,
            adapter=vql_adapter,  # a SqlNormalizer: used with no keyword
        )
        (result,) = report.results
        assert result.status == "violation"
        assert result.moved is True

    def test_first_touch_passes_on_a_dialect_that_parses_only_after_normalization(
        self, vql_adapter, mkt_contract
    ) -> None:
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FIRST_TOUCH + _CTX,
            contract=mkt_contract,
            adapter=vql_adapter,
        )
        (result,) = report.results
        assert result.status == "pass"
        assert report.ok is True

    def test_the_original_text_executes_never_the_normalized_one(
        self, vql_adapter, mkt_contract, monkeypatch
    ) -> None:
        seen = _spy(vql_adapter, monkeypatch)
        check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT + _CTX,
            contract=mkt_contract,
            adapter=vql_adapter,
        )
        assert seen, "something must have executed"
        assert all("CONTEXT" in s for s in seen)
        assert any("sens_shadow_0" in s for s in seen)

    def test_without_a_normalizer_the_same_query_is_unchecked(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        # Before and after: this is the capability the task adds.
        seen = _spy(mkt_adapter, monkeypatch)
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT + _CTX,
            contract=mkt_contract,
            adapter=mkt_adapter,
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert result.reason.startswith("unparseable")
        assert seen == []

    def test_a_renaming_normalizer_is_unchecked_and_runs_nothing(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        seen = _spy(mkt_adapter, monkeypatch)
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT,
            contract=mkt_contract,
            adapter=mkt_adapter,
            sql_normalizer=_Renaming(),
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert result.reason.startswith("count guard")
        assert seen == []

    def test_a_raising_normalizer_leaves_every_property_unchecked(
        self, mkt_adapter, mkt_contract, monkeypatch
    ) -> None:
        seen = _spy(mkt_adapter, monkeypatch)
        report = check_sensitivity(
            _metric(FIRST_TOUCH_PROP),
            FAN_OUT,
            contract=mkt_contract,
            adapter=mkt_adapter,
            sql_normalizer=_Raising(),
        )
        (result,) = report.results
        assert result.status == "unchecked"
        assert result.reason.startswith("normalizer failed")
        assert report.ok is False
        assert seen == []

    def test_a_governed_normalized_shadow_is_checked(
        self, vql_adapter, mkt_contract
    ) -> None:
        report = check_sensitivity(
            _metric(VQL_PROP),
            FIRST_TOUCH + _CTX,
            contract=mkt_contract,
            adapter=vql_adapter,
        )
        (result,) = report.results
        assert result.status == "pass"

    def test_an_ungoverned_normalized_shadow_raises(self, vql_adapter) -> None:
        # VQL_SHADOW also reads mkt.lead_scores, which this contract does not
        # govern -- visible only after normalization.
        with pytest.raises(ValueError, match="lead_scores"):
            check_sensitivity(
                _metric(VQL_PROP),
                f"SELECT count(*) FROM mkt.touchpoints{_CTX}",
                contract=_contract("touchpoints"),
                adapter=vql_adapter,
            )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_sensitivity.py::TestNormalizedCheckSensitivity -q`
Expected: FAIL — the two `vql_adapter` acceptance tests come back `unchecked`
(the query is parsed raw), the `sql_normalizer=` tests fail with `TypeError`
(unexpected keyword), and `test_without_a_normalizer_the_same_query_is_unchecked`
already PASSES (it is the before-state, kept as a regression guard).

- [ ] **Step 3: Implement**

(a) Add `SensitivityProperty` to the module's `TYPE_CHECKING` import from
`agentic_data_contracts.semantic.base`.

(b) Directly after `_normalizer_fn`, add:

```python
def _all_unchecked(
    selected: list[SensitivityProperty], metric: MetricDefinition, reason: str
) -> SensitivityReport:
    """Every selected property refused a verdict for the same whole-call reason."""
    return SensitivityReport(
        results=tuple(
            SensitivityResult(
                name=prop.name,
                metric=metric.name,
                status="unchecked",
                expected=prop.expect,
                reason=reason,
            )
            for prop in selected
        )
    )
```

`SensitivityReport` and `SensitivityResult` are defined later in the module; that
is fine, because the names resolve when the function is called, not when it is
defined.

(c) Add the keyword to `check_sensitivity`'s signature, after `dialect`:

```text
    sql_normalizer: SqlNormalizer | None = None,
```

(d) Replace everything from `if dialect is None:` down to the end of the existing
`if verdict.parse_error:` short-circuit with:

```text
    if dialect is None:
        dialect = adapter.dialect
    # An adapter that is itself a SqlNormalizer (the Denodo case) normalizes
    # unless the caller passes one -- the convention `create_tools` follows.
    if sql_normalizer is None and isinstance(adapter, SqlNormalizer):
        sql_normalizer = adapter
    normalize = _normalizer_fn(sql_normalizer)

    # Step 0 -- refuse to be the weak door. The query is normalized ONCE here
    # and Layer 1 sees the normalized text. That is equivalent to handing the
    # Validator the normalizer -- its only use of the raw text is an EXPLAIN,
    # and this Validator has no explain adapter -- and it keeps a normalizer
    # that raises something other than a parse error from escaping validate().
    # A normalizer failure, like an unparseable query, is "no verdict
    # possible", not a policy block: it is recorded, and nothing executes.
    no_verdict: str | None = None
    try:
        normalized_sql = normalize(sql)
    except Exception as e:  # noqa: BLE001 - any normalizer failure is one outcome
        no_verdict = f"normalizer failed: {e}"
    else:
        verdict = Validator(contract, dialect=dialect).validate(normalized_sql)
        if verdict.blocked and not verdict.parse_error:
            raise ValueError(
                f"query is blocked by the contract and will not be executed: "
                f"{'; '.join(verdict.reasons)}"
            )
        if verdict.parse_error:
            no_verdict = f"unparseable: {'; '.join(verdict.reasons)}"

    # A shadow that fails to parse cannot be checked against `allowed` here --
    # `None` is a refusal, never an empty set of tables read. Recorded now and
    # turned into an `unchecked` result (never an execution) in the loop below,
    # so a governance hole never opens just because sqlglot cannot read the
    # dialect a shadow is written in.
    allowed = {name.lower() for name in contract.allowed_table_names()}
    unparseable_shadows: set[str] = set()
    for prop in selected:
        read = _shadow_tables(prop.shadow, dialect=dialect, normalize=normalize)
        if read is None:
            unparseable_shadows.add(prop.name)
            continue
        for name in sorted(read):
            if name.lower() not in allowed:
                raise ValueError(
                    f"sensitivity property {prop.name!r} of metric "
                    f"{metric.name!r} has a shadow reading {name!r}, which the "
                    "contract does not allow"
                )

    # No verdict for the query -- unparseable, or its normalizer failed -- is
    # refused WITHOUT executing anything, not even the rewrite. That is what
    # makes skipping the policy raise safe: if the Validator's parse and
    # `_rewrite`'s ever disagreed, falling through would run SQL Layer 1 never
    # vouched for.
    if no_verdict is not None:
        return _all_unchecked(selected, metric, no_verdict)
```

(e) In the per-property loop, change the rewrite call to
`mutated_sql = _rewrite(sql, prop.shadow, dialect=dialect, normalize=normalize)`.
The base query still runs `_run(sql)` — the ORIGINAL text. Do not change that.

(f) In `check_sensitivity`'s docstring, replace
`Layer 1 could not even read (a dialect that needs a ``SqlNormalizer``
included), so this degrades` with `Layer 1 could not even read, so this degrades`,
and add this paragraph after the step-0 paragraphs:

```
    **Dialects that parse only after normalization.** ``sql_normalizer`` --
    defaulting to ``adapter`` when the adapter implements ``SqlNormalizer`` --
    is applied before anything is parsed: the query for Layer 1 and for
    locating the target, and each shadow for governance. The rewrite still
    edits the ORIGINAL text, which is what executes; the edit is proved by
    normalizing the result. The normalizer must change syntax only: one that
    renames the target makes the counts disagree, and every such query is
    ``unchecked``. A normalizer that raises is "no verdict possible" for the
    whole call.
```

- [ ] **Step 4: Run the tests to verify they pass, at both sqlglot versions**

```bash
uv run pytest tests/test_validation/test_sensitivity.py tests/test_public_api.py -q
uv run --resolution lowest-direct pytest tests/test_validation/test_sensitivity.py tests/test_public_api.py -q
git checkout uv.lock && uv sync --all-extras -q
uv run pytest -q
```

Expected: PASS everywhere, including every pre-existing `TestCheckSensitivity`
and `TestPolicy` test.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/sensitivity.py tests/test_validation/test_sensitivity.py
git commit -m "feat(sensitivity): check queries in dialects that parse only after normalization"
```

---

### Task 5: Documentation

**Files:**
- Modify: `src/agentic_data_contracts/validation/sensitivity.py` (module docstring)
- Modify: `README.md`, `CHANGELOG.md`

**Interfaces:** none.

- [ ] **Step 1: Module docstring**

Replace the paragraph beginning `NOT YET SUPPORTED: a dialect that parses only
after a ``SqlNormalizer``` (to the end of the docstring) with:

```
DIALECTS THAT PARSE ONLY AFTER NORMALIZATION (Denodo VQL, in this library).
References to the target are COUNTED in the normalized text, which sqlglot can
parse, and LOCATED in the original by tokenizing it -- tokenizing is far more
permissive than parsing -- because a ``SqlNormalizer`` returns no offsets and
the original is what executes. The two must agree, and the edit is proved by
normalizing the result: the edited body must hold no reference to the target,
and the final text must parse as one statement. The normalizer must change
syntax only; one that renames the target fails closed as ``unchecked``.
"""
```

- [ ] **Step 2: README**

Replace the paragraph beginning `**Not yet supported: a dialect that parses only
after a `SqlNormalizer` rewrite**` with:

```markdown
**Dialects that parse only after a `SqlNormalizer` rewrite** — Denodo VQL, in this library — are checked too. `check_sensitivity` takes a `sql_normalizer`, and defaults to the adapter when the adapter implements `SqlNormalizer`, the convention `create_tools` follows; `validate_sensitivity_tables` takes the same keyword, with no fallback because it takes no adapter. The normalized text is what gets *parsed*: the query for Layer 1 and to count the target's references, each shadow for governance. The *original* text is what gets edited and executed — the references are located in it by tokenizing, which accepts syntax (VQL's `CONTEXT` clause, for one) that parsing does not — and the edit is then proved by normalizing the result: the edited query must hold no reference to the target, and the final text must parse as one statement. The normalizer must change syntax only; one that renames or re-qualifies the target makes the counts disagree, and every such query comes back `unchecked`. On Denodo the rewrite relies on `WITH`, which the VQL grammar supports with several CTEs and no column list; the one detail it leaves unspecified — identifier rules for the `sens_shadow_0` alias — can be confirmed with `WITH sens_shadow_0 AS (SELECT * FROM <some_view>) SELECT COUNT(*) FROM sens_shadow_0`, and fails closed if Denodo refused it.
```

- [ ] **Step 3: CHANGELOG**

In the `## [0.52.0]` entry, replace the paragraph beginning
`**Dialects that parse only after a `SqlNormalizer` rewrite are not yet supported.**`
with:

```markdown
  **Dialects that parse only after a `SqlNormalizer` rewrite are checked by parsing the normalized text and editing the original.** A `SqlNormalizer` returns no offsets, and the original text is what must execute, so the target's references are *counted* in the normalized text and *located* in the original by tokenizing it — tokenizing accepts syntax, such as VQL's `CONTEXT` clause, that parsing rejects. The two counts must agree, and the edit is proved rather than trusted: normalized, the edited query must hold no reference to the target and the final text must parse as one statement. `check_sensitivity` and `validate_sensitivity_tables` take a `sql_normalizer`; `check_sensitivity` falls back to the adapter when it implements `SqlNormalizer`. A normalizer that renames the target, or that raises, fails closed as `unchecked`. The CTE alias is now the letter-leading `sens_shadow_N`, because some engines — Oracle among them — require unquoted identifiers to start with a letter.
```

- [ ] **Step 4: Verify the docs describe the code**

```bash
grep -rn -i "not yet supported" README.md CHANGELOG.md src/agentic_data_contracts/validation/sensitivity.py
grep -rn "__sens" README.md CHANGELOG.md src/ tests/ examples/
```

Expected: no output from either.

Then run the demo and CI's diff of it:

```bash
(cd examples/revenue_agent && uv run python setup_db.py >/dev/null 2>&1)
out=$(mktemp)
uv run python examples/revenue_agent/check_sensitivity.py > "$out"; echo "exit=$?"
diff -u examples/revenue_agent/expected_sensitivity_output.txt "$out"
```

Expected: `exit=0` and no diff. If the demo prints the alias anywhere, regenerate
`expected_sensitivity_output.txt` from this run and say so in the commit.

- [ ] **Step 5: Full suite, prek, commit**

```bash
uv run pytest -q
prek run --all-files
git add src/agentic_data_contracts/validation/sensitivity.py README.md CHANGELOG.md
git commit -m "docs: sensitivity checks support dialects that parse only after normalization"
```

---

## Self-Review

**Spec coverage.** The spec section's design steps 1–5 → Tasks 2 (locate,
rewrite, prove), 3 (shadows) and 4 (step 0, wiring). Where it lives: nothing to
build. Denodo `WITH`: the alias hardening → Task 1; the grammar-settled details
need no code. Guards table → every row has a test in Tasks 2–4. Public API →
Tasks 3–4. Testing without Denodo → `_strip_context`, `_VqlNormalizer`,
`VqlDuckDBAdapter`, and every row of the spec's test table: acceptance
(Task 4), original-executes (Task 4), plain-adapter before-state (Task 4),
renaming (Tasks 2 and 4), raising (Tasks 2 and 4), proof teeth (Task 2),
governed and ungoverned VQL shadow (Tasks 3 and 4), fallback (Task 4's
acceptance tests pass `adapter=` with no keyword), alias (Task 1). Floor runs →
every task. Docs → Task 5. Non-goals → Global Constraints.

**Deviations.** Three, flagged above, each with its reason; the spec is amended
in the same commit as this plan.

**Type consistency.** `Normalize` / `_identity` (Task 2) are used by Tasks 3–4.
`_rewrite(sql, shadow, *, dialect, normalize=_identity)` is defined in Task 2 and
called that way in Task 4. `_shadow_tables(shadow, *, dialect,
normalize=_identity)` is defined in Task 3 and called that way in Task 4.
`_normalizer_fn` (Task 3) is used in Task 4. `_all_unchecked(selected, metric,
reason)` is defined and used in Task 4. The test helpers `_strip_context` /
`_CTX` (Task 2), `_VqlNormalizer`, `VQL_SHADOW` and `VQL_PROP` (Task 3), and
`_load_mkt` / `VqlDuckDBAdapter` / `vql_adapter` / `_spy` (Task 4) are each
defined before the first task that uses them.
