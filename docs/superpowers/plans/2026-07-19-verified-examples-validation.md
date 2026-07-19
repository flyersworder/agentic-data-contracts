# Verified-Examples Contract Validation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `validate_examples` — a batch verb that runs an external corpus of `question → SQL` examples through the framework's existing two-layer `Validator`, with an engine-fallback dry run for SQL sqlglot cannot parse.

**Architecture:** One new module `validation/examples.py` (interchange type, report types, the verb) plus a one-field addition to `ValidationResult`. `validate_examples` reuses `Validator` verbatim; the only new logic is (a) per-principal validator caching, (b) mapping `ValidationResult` → `ExampleResult`, and (c) decision-B: on a parse failure, call the `ExplainAdapter` directly so the engine renders the verdict.

**Tech Stack:** Python 3.12+, dataclasses, sqlglot (already a dep), pytest. No new dependencies.

**Design source:** `docs/superpowers/specs/2026-07-19-verified-examples-validation-design.md`.

## Global Constraints

- Run everything with `uv run` (e.g. `uv run pytest tests/test_validation/test_examples.py -v`).
- Lint/type-check through **prek**, never bare tools: `prek run --all-files`. `ty` is authoritative and gates CI.
- Every new module starts with `from __future__ import annotations`.
- No new dependencies; nothing engine-specific may enter the framework — the three engine knobs (`dialect`, `sql_normalizer`, `explain_adapter`) are passed straight through.
- Follow existing dataclass + `Protocol` conventions in `validation/`.
- The **only** change to core `validator.py` is the `parse_error` field. All example logic lives in `examples.py`.
- TDD: write the failing test, watch it fail, implement minimally, watch it pass, commit.

---

### Task 1: `ValidationResult.parse_error` flag

**Files:**
- Modify: `src/agentic_data_contracts/validation/validator.py` (dataclass ~L55-64; parse-error return L266)
- Test: `tests/test_validation/test_validator.py`

**Interfaces:**
- Produces: `ValidationResult.parse_error: bool` — `True` only when sqlglot could not parse the SQL. Read by `validate_examples` (Task 4/5) to trigger the decision-B fallback.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_validation/test_validator.py`:

```python
def test_parse_error_sets_flag(validator: Validator) -> None:
    result = validator.validate("SELECT * FROM (")
    assert result.blocked
    assert result.parse_error


def test_valid_query_has_no_parse_error(validator: Validator) -> None:
    result = validator.validate(
        "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
    )
    assert not result.parse_error
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_validator.py -k parse_error -v`
Expected: FAIL — `AttributeError: 'ValidationResult' object has no attribute 'parse_error'`.

(First confirm the chosen string actually raises `sqlglot.errors.ParseError`: `uv run python -c "import sqlglot; sqlglot.parse_one('SELECT * FROM (')"` should raise. If it does not on the pinned sqlglot, substitute another guaranteed-unparseable string and reuse it in Task 5.)

- [ ] **Step 3: Add the field**

In the `ValidationResult` dataclass, append:

```python
    explain_errors: list[str] = field(default_factory=list)
    parse_error: bool = False
```

- [ ] **Step 4: Set it at the parse-error return**

Change validator.py:266 from:

```python
            return ValidationResult(blocked=True, reasons=[f"SQL parse error: {e}"])
```

to:

```python
            return ValidationResult(
                blocked=True,
                reasons=[f"SQL parse error: {e}"],
                parse_error=True,
            )
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_validation/test_validator.py -v`
Expected: PASS (all, including pre-existing).

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/validation/validator.py tests/test_validation/test_validator.py
git commit -m "feat: ValidationResult.parse_error flags sqlglot parse failures"
```

---

### Task 2: `VerifiedExample` interchange type

**Files:**
- Create: `src/agentic_data_contracts/validation/examples.py`
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Produces: `VerifiedExample(sql, question="", id=None, principal=None, metadata={})` and `VerifiedExample.from_dict(raw)`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_validation/test_examples.py`:

```python
from agentic_data_contracts.validation.examples import VerifiedExample


def test_from_dict_maps_known_fields() -> None:
    ex = VerifiedExample.from_dict(
        {"id": "wau", "question": "weekly active users", "sql": "SELECT 1"}
    )
    assert ex.id == "wau"
    assert ex.question == "weekly active users"
    assert ex.sql == "SELECT 1"


def test_from_dict_stashes_unknown_keys_in_metadata() -> None:
    ex = VerifiedExample.from_dict(
        {"sql": "SELECT 1", "verified_by": "jsmith", "type": "sql"}
    )
    assert ex.metadata["verified_by"] == "jsmith"
    assert ex.metadata["type"] == "sql"


def test_from_dict_merges_explicit_metadata() -> None:
    ex = VerifiedExample.from_dict(
        {"sql": "SELECT 1", "metadata": {"a": 1}, "b": 2}
    )
    assert ex.metadata == {"a": 1, "b": 2}


def test_from_dict_requires_sql() -> None:
    import pytest

    with pytest.raises(ValueError, match="sql"):
        VerifiedExample.from_dict({"question": "no sql here"})
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py -v`
Expected: FAIL — `ModuleNotFoundError: ... validation.examples`.

- [ ] **Step 3: Create the module with the dataclass**

Create `src/agentic_data_contracts/validation/examples.py`:

```python
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
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_validation/test_examples.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: VerifiedExample interchange type for example validation"
```

---

### Task 3: report types — `ExampleResult` and `ExampleValidationReport`

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py`
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: `VerifiedExample` (Task 2).
- Produces:
  - `ExampleResult(example, status, reasons, warnings, contract_checked, engine_checked)` — `status ∈ {"valid","violation","unchecked"}`.
  - `ExampleValidationReport(results)` with properties `valid`, `violations`, `unchecked`, `unverified_compliance`, `ok`, and method `summary()`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_examples.py`:

```python
from agentic_data_contracts.validation.examples import (
    ExampleResult,
    ExampleValidationReport,
)


def _res(status: str, *, contract_checked: bool = True) -> ExampleResult:
    return ExampleResult(
        example=VerifiedExample(sql="SELECT 1", id=status),
        status=status,
        reasons=[],
        warnings=[],
        contract_checked=contract_checked,
        engine_checked=False,
    )


def test_report_partitions_by_status() -> None:
    report = ExampleValidationReport(
        results=[_res("valid"), _res("violation"), _res("unchecked")]
    )
    assert [r.status for r in report.valid] == ["valid"]
    assert [r.status for r in report.violations] == ["violation"]
    assert [r.status for r in report.unchecked] == ["unchecked"]


def test_ok_is_false_only_when_violations_present() -> None:
    assert ExampleValidationReport(results=[_res("valid")]).ok
    assert ExampleValidationReport(results=[_res("unchecked")]).ok
    assert not ExampleValidationReport(results=[_res("violation")]).ok


def test_unverified_compliance_flags_engine_only_passes() -> None:
    report = ExampleValidationReport(
        results=[_res("valid"), _res("valid", contract_checked=False)]
    )
    assert len(report.unverified_compliance) == 1
    assert not report.unverified_compliance[0].contract_checked


def test_summary_mentions_counts_and_offenders() -> None:
    report = ExampleValidationReport(results=[_res("valid"), _res("violation")])
    text = report.summary()
    assert "violation" in text
    assert "1" in text  # a count appears
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py -k "report or ok or unverified or summary" -v`
Expected: FAIL — `ImportError: cannot import name 'ExampleResult'`.

- [ ] **Step 3: Implement the report types**

Append to `examples.py` (after `VerifiedExample`):

```python
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
                lines.append(
                    f"- unverified `{_label(r, i)}`: {'; '.join(r.warnings)}"
                )
        return "\n".join(lines)
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_validation/test_examples.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: ExampleResult and ExampleValidationReport"
```

---

### Task 4: `validate_examples` — parsed path + principal grouping

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py`
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: `VerifiedExample`, `ExampleResult`, `ExampleValidationReport`; `Validator` (constructor: `Validator(contract, dialect=None, explain_adapter=None, sql_normalizer=None, semantic_source=None, *, caller_principal=None)`); `ValidationResult` (has `blocked`, `reasons`, `warnings`, `schema_valid`, `parse_error`); `ExplainAdapter`, `ExplainResult`.
- Produces: `validate_examples(examples, contract, *, dialect=None, sql_normalizer=None, explain_adapter=None, semantic_source=None) -> ExampleValidationReport`.
- This task covers the parsed path and the parse-error-**without**-adapter (`unchecked`) case. The engine fallback for parse-error-**with**-adapter is Task 5.

**Contract-fixture facts (from `tests/fixtures/valid_contract.yml`, used across existing validator tests):**
- Allowed: `analytics.orders`, requires filter `tenant_id`. Forbidden table `raw.payments`. `SELECT *` blocked. `DELETE` blocked.
- A passing query: `"SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_examples.py` (add imports for `DataContract`, `Path`, `ExplainResult`, and a `FakeExplainAdapter` mirroring `test_explain.py`):

```python
from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.examples import validate_examples
from agentic_data_contracts.validation.explain import ExplainResult

_OK_SQL = "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"


class FakeExplainAdapter:
    def __init__(self, result: ExplainResult) -> None:
        self._result = result

    def explain(self, sql: str) -> ExplainResult:
        return self._result


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


def test_valid_example_passes(contract: DataContract) -> None:
    report = validate_examples([VerifiedExample(sql=_OK_SQL)], contract)
    assert report.ok
    r = report.results[0]
    assert r.status == "valid"
    assert r.contract_checked
    assert not r.engine_checked  # no adapter passed


def test_forbidden_table_is_violation(contract: DataContract) -> None:
    ex = VerifiedExample(sql="SELECT id FROM raw.payments WHERE tenant_id = 'x'")
    report = validate_examples([ex], contract)
    assert not report.ok
    assert report.results[0].status == "violation"
    assert any("raw.payments" in r for r in report.results[0].reasons)


def test_missing_filter_is_violation(contract: DataContract) -> None:
    report = validate_examples(
        [VerifiedExample(sql="SELECT id FROM analytics.orders")], contract
    )
    assert report.results[0].status == "violation"


def test_schema_drift_is_violation_via_explain(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(
            estimated_cost_usd=None,
            estimated_rows=None,
            schema_valid=False,
            errors=["Column 'amount' not found"],
        )
    )
    report = validate_examples(
        [VerifiedExample(sql=_OK_SQL)], contract, explain_adapter=adapter
    )
    r = report.results[0]
    assert r.status == "violation"
    assert r.contract_checked
    assert r.engine_checked


def test_valid_with_adapter_marks_engine_checked(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=None, estimated_rows=10, schema_valid=True)
    )
    report = validate_examples(
        [VerifiedExample(sql=_OK_SQL)], contract, explain_adapter=adapter
    )
    r = report.results[0]
    assert r.status == "valid"
    assert r.engine_checked


def test_parse_error_without_adapter_is_unchecked(contract: DataContract) -> None:
    report = validate_examples([VerifiedExample(sql="SELECT * FROM (")], contract)
    r = report.results[0]
    assert r.status == "unchecked"
    assert not r.contract_checked
    assert not r.engine_checked


def test_empty_input(contract: DataContract) -> None:
    report = validate_examples([], contract)
    assert report.results == []
    assert report.ok


def test_results_preserve_input_order(contract: DataContract) -> None:
    exs = [
        VerifiedExample(sql=_OK_SQL, id="a"),
        VerifiedExample(sql="SELECT id FROM analytics.orders", id="b"),
    ]
    report = validate_examples(exs, contract)
    assert [r.example.id for r in report.results] == ["a", "b"]


def test_forbidden_op_is_violation(contract: DataContract) -> None:
    report = validate_examples(
        [VerifiedExample(sql="DELETE FROM analytics.orders WHERE tenant_id = 'x'")],
        contract,
    )
    assert report.results[0].status == "violation"


def test_cost_block_marks_engine_checked(contract: DataContract) -> None:
    # valid_contract.yml sets cost_limit_usd = 5.00; 10.0 exceeds it. The block
    # comes from EXPLAIN (schema_valid stays True), so engine_checked must be
    # True even though it is not a schema rejection.
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=10.0, estimated_rows=None, schema_valid=True)
    )
    report = validate_examples(
        [VerifiedExample(sql=_OK_SQL)], contract, explain_adapter=adapter
    )
    r = report.results[0]
    assert r.status == "violation"
    assert r.engine_checked


def test_contract_policy_drift_valid_then_violation(contract: DataContract) -> None:
    # Same SQL: valid under contract A (analytics.orders allowed), a violation
    # under contract B (only analytics.customers allowed). Proves the verdict is
    # contract-relative — the drift sweep's core promise.
    from agentic_data_contracts.core.schema import DataContractSchema

    example = [VerifiedExample(sql=_OK_SQL)]
    assert validate_examples(example, contract).results[0].status == "valid"

    schema_b = DataContractSchema.model_validate(
        {
            "version": "1.0",
            "name": "drift-b",
            "semantic": {
                "allowed_tables": [{"schema": "analytics", "tables": ["customers"]}],
                "forbidden_operations": [],
                "rules": [],
            },
        }
    )
    contract_b = DataContract(schema=schema_b)
    assert validate_examples(example, contract_b).results[0].status == "violation"


def test_warn_rule_surfaces_warning_without_failing() -> None:
    # A warn-enforcement rule must land in ExampleResult.warnings and keep the
    # example valid (ok stays True). valid_contract's warn rule has no
    # query_check and never fires, so build a contract whose warn rule can.
    from agentic_data_contracts.core.schema import DataContractSchema

    schema = DataContractSchema.model_validate(
        {
            "version": "1.0",
            "name": "warn-test",
            "semantic": {
                "allowed_tables": [{"schema": "analytics", "tables": ["orders"]}],
                "forbidden_operations": [],
                "rules": [
                    {
                        "name": "prefer_explicit_columns",
                        "description": "avoid SELECT *",
                        "enforcement": "warn",
                        "query_check": {"no_select_star": True},
                    }
                ],
            },
        }
    )
    warn_contract = DataContract(schema=schema)
    report = validate_examples(
        [VerifiedExample(sql="SELECT * FROM analytics.orders")], warn_contract
    )
    r = report.results[0]
    assert r.status == "valid"
    assert report.ok
    assert any("SELECT *" in w for w in r.warnings)
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py -k "example or filter or drift or engine or unchecked or empty or order or forbidden or warn or cost" -v`
Expected: FAIL — `ImportError: cannot import name 'validate_examples'`.

- [ ] **Step 3: Implement `validate_examples` (parsed path + unchecked)**

Add imports at the top of `examples.py`:

```python
from collections.abc import Iterable

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.semantic.base import SemanticSource
from agentic_data_contracts.validation.explain import ExplainAdapter
from agentic_data_contracts.validation.validator import ValidationResult, Validator
from agentic_data_contracts.adapters._normalizer import SqlNormalizer
```

(`SqlNormalizer` is a `Protocol` used only as a type annotation here — the same import `validator.py` uses.)

Append the verb:

```python
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
    asked directly (decision B — added in the next task). Input order is
    preserved.
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

    # Parse failure — no AST, so no contract check. Engine fallback is Task 5.
    return ExampleResult(
        example=example,
        status="unchecked",
        reasons=list(vr.reasons),
        contract_checked=False,
        engine_checked=False,
    )
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_validation/test_examples.py -v`
Expected: PASS (all so far).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: validate_examples parsed path with per-principal validators"
```

---

### Task 5: decision-B engine fallback on parse failure

**Files:**
- Modify: `src/agentic_data_contracts/validation/examples.py` (`_to_result`)
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Consumes: `ExplainAdapter.explain(sql) -> ExplainResult` (has `schema_valid: bool`, `errors: list[str]`).
- Behavior change: when `vr.parse_error` and an `explain_adapter` is present, call `explain_adapter.explain(example.sql)`; `schema_valid` → `valid` (with caveat warning, `contract_checked=False`), else `violation`. No adapter still yields `unchecked`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_examples.py`:

```python
_UNPARSEABLE_SQL = "SELECT * FROM ("  # confirmed to raise ParseError in Task 1


def test_parse_fallback_engine_plans_is_valid_unverified(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=None, estimated_rows=1, schema_valid=True)
    )
    report = validate_examples(
        [VerifiedExample(sql=_UNPARSEABLE_SQL, id="vdp")],
        contract,
        explain_adapter=adapter,
    )
    r = report.results[0]
    assert r.status == "valid"
    assert not r.contract_checked
    assert r.engine_checked
    assert any("not statically verified" in w for w in r.warnings)
    assert report.unverified_compliance == [r]
    assert report.ok  # valid, so the gate is not failed


def test_parse_fallback_engine_rejects_is_violation(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(
            estimated_cost_usd=None,
            estimated_rows=None,
            schema_valid=False,
            errors=["syntax error near '('"],
        )
    )
    report = validate_examples(
        [VerifiedExample(sql=_UNPARSEABLE_SQL)], contract, explain_adapter=adapter
    )
    r = report.results[0]
    assert r.status == "violation"
    assert not r.contract_checked
    assert r.engine_checked
    assert any("syntax error" in reason for reason in r.reasons)
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/test_validation/test_examples.py -k fallback -v`
Expected: FAIL — both come back `status == "unchecked"` (Task 4 behavior).

- [ ] **Step 3: Implement the fallback branch**

In `_to_result`, replace the final `# Parse failure` block with:

```python
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
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_validation/test_examples.py -v`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/examples.py tests/test_validation/test_examples.py
git commit -m "feat: decision-B engine fallback for sqlglot-unparseable examples"
```

---

### Task 6: package exports + principal-scoped test

**Files:**
- Modify: `src/agentic_data_contracts/validation/__init__.py`
- Test: `tests/test_validation/test_examples.py`

**Interfaces:**
- Produces: `VerifiedExample`, `ExampleResult`, `ExampleValidationReport`, `validate_examples` importable from `agentic_data_contracts.validation`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_examples.py`:

```python
def test_public_exports() -> None:
    from agentic_data_contracts.validation import (
        ExampleResult,
        ExampleValidationReport,
        VerifiedExample,
        validate_examples,
    )

    assert VerifiedExample and ExampleResult
    assert ExampleValidationReport and validate_examples
```

Also add the principal-scoped test. `tests/fixtures/filter_values_contract.yml` gates
`sales.opps` per principal: `partner@co.com` may filter `account_id` in `[123, 456]`,
`vip@co.com` in `[999]`. The same SQL must therefore be `valid` for the partner and a
`violation` for the vip — proving per-principal validators are built and the batch is
checked under each example's own identity (this also exercises order preservation across
principals):

```python
def test_principal_scoped_validation(fixtures_dir: Path) -> None:
    contract = DataContract.from_yaml(fixtures_dir / "filter_values_contract.yml")
    sql = "SELECT id FROM sales.opps WHERE account_id = 123"
    report = validate_examples(
        [
            VerifiedExample(sql=sql, principal="partner@co.com", id="partner"),
            VerifiedExample(sql=sql, principal="vip@co.com", id="vip"),
        ],
        contract,
    )
    by_id = {r.example.id: r.status for r in report.results}
    assert by_id["partner"] == "valid"     # 123 in partner's allowlist
    assert by_id["vip"] == "violation"      # 123 not in vip's [999]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_validation/test_examples.py -k exports -v`
Expected: FAIL — `ImportError` from the package root.

- [ ] **Step 3: Add exports**

In `src/agentic_data_contracts/validation/__init__.py`, add the import and extend `__all__`:

```python
from agentic_data_contracts.validation.examples import (
    ExampleResult,
    ExampleValidationReport,
    VerifiedExample,
    validate_examples,
)
```

Add `"ExampleResult"`, `"ExampleValidationReport"`, `"VerifiedExample"`, `"validate_examples"` to `__all__` (keep it alphabetically sorted, matching the existing list).

- [ ] **Step 4: Run the full suite + linters**

Run: `uv run pytest tests/test_validation/ -v`
Expected: PASS.

Run: `prek run --all-files`
Expected: ruff + ty clean.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/validation/__init__.py tests/test_validation/test_examples.py
git commit -m "feat: export verified-example validation from validation package"
```

---

## Out of scope (do not build)

Per the spec: no YAML loader / file IO, no `find_examples` retrieval tool or prompt injection, no storage / write-back / embeddings, no query execution or result-correctness check, no prose-lesson handling, no taxonomy interpretation. A version bump / release is handled separately via the project's release flow, not this plan.

## Self-Review

- **Spec coverage:** interchange shape (Task 2), report + statuses (Task 3), the verb + two layers + principal grouping (Task 4), decision B (Task 5), exports + principal test (Task 6), the one core change `parse_error` (Task 1).
- **Type consistency:** `validate_examples` signature, `Validator` constructor args, `ValidationResult` fields (`blocked`/`reasons`/`warnings`/`schema_valid`/`estimated_cost_usd`/`estimated_rows`/`parse_error`), and `ExplainResult` fields (`schema_valid`/`errors`) match the source read during planning.
- **Applied after a three-lens plan review (2026-07-19):**
  - `engine_checked` in the violation branch now reports `True` for cost/row-limit
    blocks (EXPLAIN ran) — reconstructed from `schema_valid`/`estimated_*`, so the
    "only `parse_error` touches core" property holds; covered by
    `test_cost_block_marks_engine_checked`.
  - `summary()` iterates `enumerate(self.results)` for an `id → question → #index`
    label so two unnamed rows never collide.
  - Added tests the spec mandated but the first draft omitted: forbidden-op,
    contract-policy drift (A→B), warn-rule-surfaces-warning, and a concrete
    principal-scoped test (Task 6).
  - Fixed the test path in Tasks 4–6 (`tests/test_validation/test_examples.py`).
- **Known follow-up for the implementer:** confirm that `"SELECT * FROM ("` raises `sqlglot.errors.ParseError` on the pinned sqlglot before relying on it in Tasks 1 and 5; substitute another guaranteed-unparseable literal (reused in both tasks) if not.
