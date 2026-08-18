# Declared attribution convention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a contract declare where a decomposition's cross term goes, deliver that fact through both tool channels an attribution question already touches, and ship a pure kernel that computes the breakdown under the declared convention.

**Architecture:** Two optional fields on `Decomposition`, a source-level default resolved onto each decomposition at load time (so it survives `freeze_semantic_source` for free and leaves a frozen contract fully explicit), loud load-time validation, and propagation onto `IdentityEdge` so both `lookup_metric` and `trace_metric_impacts` carry it. A new `validation/attribution.py` holds pure arithmetic — values in, contributions out, no adapter — and is deliberately **not** wired into `create_tools()`.

**Tech Stack:** Python 3.12+, dataclasses, pydantic v2 (contract schema only), pytest, ruff + ty via prek, uv.

**Spec:** `docs/superpowers/specs/2026-08-18-decomposition-attribution-convention-design.md`

## Global Constraints

- Run every Python command through `uv run`. Run linters through `prek run --all-files`, never a bare `ruff` or `ty` — the hook `rev`s in `.pre-commit-config.yaml` are what CI uses.
- Follow TDD: write the failing test, watch it fail, implement, watch it pass, commit.
- Never write a `somefile.py:123` reference in a **Python** file — a pygrep hook rejects it. Name the symbol instead.
- Vocabulary is exactly `explicit`, `split_evenly`, `fold_into`. Cross-term operators are exactly `product`, `ratio`.
- Default tolerances match `reconcile_decomposition`: `rel_tol=1e-4`, `abs_tol=0.0`.
- Every new serialized key is **omitted when unset**. A contract that declares no convention must produce byte-identical `contract_canonical_bytes` before and after this change.
- Target version: **0.43.0**.
- Where this plan's tests use inline `raw` dicts rather than extending
  `tests/fixtures/decomposition_source.yml`, that is deliberate: the fixture is
  shared, and adding a convention to it changes assertions in unrelated tests.
  Task 6 is the one place the fixture *is* extended, because the tool tests are
  built on it — and that task calls out the assertion it breaks.

---

### Task 1: Vocabulary and validation on `Decomposition`

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py` — `Decomposition`, new constants beside `VALID_OPERATORS`, `validate_decompositions`
- Test: `tests/test_semantic/test_decomposition.py`

**Interfaces:**
- Consumes: nothing
- Produces: `Decomposition.convention: str | None`, `Decomposition.convention_operand: str | None`, `VALID_CONVENTIONS: frozenset[str]`, `_CROSS_TERM_OPERATORS: frozenset[str]`, and the validation rules every later task relies on.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_semantic/test_decomposition.py`:

```python
class TestConventionValidation:
    def _metrics(
        self,
        *,
        convention: str | None = None,
        convention_operand: str | None = None,
        operator: str = "product",
        operands: list[str] | None = None,
    ) -> list[MetricDefinition]:
        return [
            MetricDefinition(
                name="activations",
                description="",
                sql_expression="",
                decompositions=[
                    Decomposition(
                        operator=operator,
                        operands=operands if operands is not None else ["volume", "rate"],
                        convention=convention,
                        convention_operand=convention_operand,
                    )
                ],
            ),
            MetricDefinition(name="volume", description="", sql_expression=""),
            MetricDefinition(name="rate", description="", sql_expression=""),
        ]

    def test_undeclared_convention_is_valid(self) -> None:
        validate_decompositions(self._metrics())

    def test_each_vocabulary_value_is_accepted(self) -> None:
        validate_decompositions(self._metrics(convention="explicit"))
        validate_decompositions(self._metrics(convention="split_evenly"))
        validate_decompositions(
            self._metrics(convention="fold_into", convention_operand="rate")
        )

    def test_unknown_convention_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown attribution convention"):
            validate_decompositions(self._metrics(convention="laspeyres"))

    def test_convention_on_sum_raises(self) -> None:
        # sum is linear: there is no cross term to place, so declaring where it
        # goes is a misunderstanding worth surfacing at authoring time.
        with pytest.raises(ValueError, match="no cross term"):
            validate_decompositions(
                self._metrics(
                    operator="sum",
                    operands=["volume", "rate"],
                    convention="split_evenly",
                )
            )

    def test_convention_on_difference_raises(self) -> None:
        with pytest.raises(ValueError, match="no cross term"):
            validate_decompositions(
                self._metrics(
                    operator="difference",
                    operands=["volume", "rate"],
                    convention="explicit",
                )
            )

    def test_fold_into_without_operand_raises(self) -> None:
        with pytest.raises(ValueError, match="requires 'convention_operand'"):
            validate_decompositions(self._metrics(convention="fold_into"))

    def test_fold_into_unknown_operand_raises(self) -> None:
        with pytest.raises(ValueError, match="is not one of its operands"):
            validate_decompositions(
                self._metrics(convention="fold_into", convention_operand="margin")
            )

    def test_convention_operand_without_fold_into_raises(self) -> None:
        with pytest.raises(ValueError, match="only meaningful with"):
            validate_decompositions(
                self._metrics(convention="split_evenly", convention_operand="rate")
            )
```

Add `pytest` and `Decomposition` to that file's imports if they are not already there.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestConventionValidation -v`
Expected: FAIL — `TypeError: Decomposition.__init__() got an unexpected keyword argument 'convention'`

- [ ] **Step 3: Add the fields and constants**

In `semantic/base.py`, replace the `Decomposition` dataclass body:

```python
@dataclass
class Decomposition:
    """An arithmetic identity: how a metric is reconstructed from other metrics.

    Operands must be in units the declared operator composes — this is not
    validated, and cannot be without expression semantics the semantic layer
    does not carry. A rate declared as a rounded percentage makes a ``product``
    identity false by ~100x. See ``reconcile_decomposition`` for how an operand
    declared at limited precision interacts with its tolerance.

    ``convention`` names where the cross term goes when the identity's *change*
    is attributed to its factors. It is a non-inferable business fact: an agent
    told only the factors will pick a placement silently and present it as
    canonical, and two such reports are indistinguishable because both sum
    correctly. Only ``product`` and ``ratio`` have a cross term.
    """

    operator: str  # "sum" | "product" | "ratio" | "difference"
    operands: list[str] = field(default_factory=list)
    convention: str | None = None  # None = undeclared, agent picks (status quo)
    convention_operand: str | None = None  # required iff convention == "fold_into"
```

Beside `VALID_OPERATORS`:

```python
VALID_CONVENTIONS = frozenset({"explicit", "split_evenly", "fold_into"})
#: Only these have a ``ΔC·ΔP`` cross term to place. ``sum`` and ``difference``
#: are linear, so a convention on them states nothing.
_CROSS_TERM_OPERATORS = frozenset({"product", "ratio"})
```

- [ ] **Step 4: Add the validation rules**

In `validate_decompositions`, immediately after the arity checks and before the
`for operand in decomp.operands:` loop, insert:

```python
            _validate_convention(metric.name, decomp)
```

And add the helper directly below `validate_decompositions`:

```python
def _validate_convention(metric_name: str, decomp: Decomposition) -> None:
    """Validate a decomposition's attribution convention; raise on any fault.

    Undeclared is valid — it is the pre-0.43 state and means the agent picks.
    """
    if decomp.convention is None:
        if decomp.convention_operand is not None:
            raise ValueError(
                f"metric {metric_name!r} decomposition sets 'convention_operand' "
                f"but declares no convention; it is only meaningful with "
                f"convention 'fold_into'"
            )
        return
    if decomp.convention not in VALID_CONVENTIONS:
        raise ValueError(
            f"metric {metric_name!r} decomposition has unknown attribution "
            f"convention {decomp.convention!r}; expected one of "
            f"{sorted(VALID_CONVENTIONS)}"
        )
    if decomp.operator not in _CROSS_TERM_OPERATORS:
        raise ValueError(
            f"metric {metric_name!r} decomposition {decomp.operator!r} has no "
            f"cross term to place, so convention {decomp.convention!r} states "
            f"nothing; conventions apply to {sorted(_CROSS_TERM_OPERATORS)}"
        )
    if decomp.convention == "fold_into":
        if decomp.convention_operand is None:
            raise ValueError(
                f"metric {metric_name!r} decomposition convention 'fold_into' "
                f"requires 'convention_operand' naming which operand absorbs "
                f"the cross term"
            )
        if decomp.convention_operand not in decomp.operands:
            raise ValueError(
                f"metric {metric_name!r} decomposition convention_operand "
                f"{decomp.convention_operand!r} is not one of its operands "
                f"{list(decomp.operands)}"
            )
    elif decomp.convention_operand is not None:
        raise ValueError(
            f"metric {metric_name!r} decomposition sets 'convention_operand' "
            f"with convention {decomp.convention!r}; it is only meaningful "
            f"with 'fold_into'"
        )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_decomposition.py -v`
Expected: PASS, including every pre-existing test in the file.

- [ ] **Step 6: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/semantic/base.py tests/test_semantic/test_decomposition.py
git commit -m "feat: declare a cross-term attribution convention on Decomposition"
```

---

### Task 2: Source-level default in `YamlSource`

**Files:**
- Modify: `src/agentic_data_contracts/semantic/yaml_source.py` — `SEMANTIC_KEYS`, `_load_from_raw`
- Modify: `tests/test_public_api.py` — the `SEMANTIC_KEYS` assertion
- Test: `tests/test_semantic/test_yaml_source.py`

**Interfaces:**
- Consumes: Task 1's `VALID_CONVENTIONS`, `_CROSS_TERM_OPERATORS`, `Decomposition.convention`
- Produces: `SEMANTIC_KEYS` now contains `"decomposition_convention"`; a `YamlSource` whose parsed `Decomposition` objects carry the **resolved** convention.

**Note:** `SEMANTIC_KEYS` is re-exported at the top level and asserted in `tests/test_public_api.py` with a comment naming README and the v0.41.0 CHANGELOG. Changing it is a public API change; the test update belongs in this task.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_semantic/test_yaml_source.py`:

```python
class TestDecompositionConventionDefault:
    _RAW = {
        "decomposition_convention": {"convention": "split_evenly"},
        "metrics": [
            {
                "name": "activations",
                "decompositions": [
                    {"operator": "product", "operands": ["volume", "rate"]}
                ],
            },
            {
                "name": "signups",
                "decompositions": [
                    {
                        "operator": "product",
                        "operands": ["volume", "rate"],
                        "convention": "fold_into",
                        "convention_operand": "rate",
                    }
                ],
            },
            {
                "name": "net",
                "decompositions": [
                    {"operator": "sum", "operands": ["volume", "rate"]}
                ],
            },
            {"name": "volume"},
            {"name": "rate"},
        ],
    }

    def test_default_is_stamped_onto_undeclared_cross_term_decomposition(self) -> None:
        source = YamlSource.from_raw(self._RAW)
        metric = source.get_metric("activations")
        assert metric is not None
        assert metric.decompositions[0].convention == "split_evenly"

    def test_per_decomposition_declaration_wins(self) -> None:
        source = YamlSource.from_raw(self._RAW)
        metric = source.get_metric("signups")
        assert metric is not None
        assert metric.decompositions[0].convention == "fold_into"
        assert metric.decompositions[0].convention_operand == "rate"

    def test_default_is_not_stamped_onto_a_linear_operator(self) -> None:
        # Stamping a convention onto `sum` would trip its own validation.
        source = YamlSource.from_raw(self._RAW)
        metric = source.get_metric("net")
        assert metric is not None
        assert metric.decompositions[0].convention is None

    def test_default_key_is_vocabulary_not_an_extra(self) -> None:
        # expected_extras=[] is strict mode: an uninterpreted top-level key
        # raises. The default must be recognised, not warned about as a typo.
        source = YamlSource.from_raw(self._RAW, expected_extras=[])
        assert "decomposition_convention" not in source.get_extras()

    def test_fold_into_as_a_source_default_raises(self) -> None:
        with pytest.raises(ValueError, match="cannot be a source-level default"):
            YamlSource.from_raw(
                {
                    "decomposition_convention": {
                        "convention": "fold_into",
                        "convention_operand": "rate",
                    },
                    "metrics": [{"name": "volume"}],
                }
            )

    def test_unknown_default_convention_raises(self) -> None:
        with pytest.raises(ValueError, match="unknown attribution convention"):
            YamlSource.from_raw(
                {
                    "decomposition_convention": {"convention": "shapley"},
                    "metrics": [{"name": "volume"}],
                }
            )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_yaml_source.py::TestDecompositionConventionDefault -v`
Expected: FAIL — the first three fail on `convention is None`, and `test_default_key_is_vocabulary_not_an_extra` fails with `ValueError: unexpected top-level keys ['decomposition_convention']`.

- [ ] **Step 3: Recognise the key and resolve the default**

In `yaml_source.py`, extend the constant:

```python
SEMANTIC_KEYS = frozenset(
    {"metrics", "tables", "relationships", "metric_impacts", "decomposition_convention"}
)
```

Import the new names:

```python
from agentic_data_contracts.semantic.base import (
    _CROSS_TERM_OPERATORS,
    VALID_CONVENTIONS,
    # ... existing imports unchanged
)
```

In `_load_from_raw`, before the `for m in raw.get("metrics", []):` loop:

```python
        default_convention = _parse_convention_default(
            raw.get("decomposition_convention")
        )
```

Parse both fields on each decomposition, replacing the existing comprehension:

```python
                    decompositions=[
                        Decomposition(
                            operator=d["operator"],
                            operands=list(d.get("operands", [])),
                            convention=d.get("convention"),
                            convention_operand=d.get("convention_operand"),
                        )
                        for d in m.get("decompositions", [])
                    ],
```

Then, immediately before the existing `validate_decompositions(self._metrics)` call:

```python
        _apply_convention_default(self._metrics, default_convention)
```

Add both helpers at module level:

```python
def _parse_convention_default(raw: Any) -> str | None:
    """Read and validate the source-level ``decomposition_convention`` block.

    ``fold_into`` is rejected: it names an operand, and no operand name is
    meaningful across metrics. Declaring it source-wide is always a mistake.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError(
            "decomposition_convention must be a mapping with a 'convention' key,"
            f" got {type(raw).__name__}"
        )
    convention = raw.get("convention")
    if convention is None:
        raise ValueError("decomposition_convention must set 'convention'")
    if convention not in VALID_CONVENTIONS:
        raise ValueError(
            f"decomposition_convention has unknown attribution convention"
            f" {convention!r}; expected one of {sorted(VALID_CONVENTIONS)}"
        )
    if convention == "fold_into":
        raise ValueError(
            "convention 'fold_into' cannot be a source-level default: it names"
            " an operand, and no operand name is meaningful across metrics."
            " Declare it per decomposition."
        )
    return convention


def _apply_convention_default(
    metrics: list[MetricDefinition], default: str | None
) -> None:
    """Stamp *default* onto every cross-term decomposition that declares none.

    Resolved at load rather than carried, so the effective value survives
    ``freeze_semantic_source`` (which re-serializes from parsed objects) and a
    frozen contract states its convention outright instead of leaving a
    consumer to re-derive it. Linear operators are skipped: a convention on
    them fails ``validate_decompositions``.
    """
    if default is None:
        return
    for metric in metrics:
        for decomp in metric.decompositions:
            if (
                decomp.convention is None
                and decomp.operator in _CROSS_TERM_OPERATORS
            ):
                decomp.convention = default
```

- [ ] **Step 4: Update the public API assertion**

In `tests/test_public_api.py`, replace the `SEMANTIC_KEYS` assertion:

```python
    # README and the v0.41.0 CHANGELOG both promise this name at the top level.
    # v0.43.0 added decomposition_convention as interpreted vocabulary.
    assert SEMANTIC_KEYS == {
        "metrics",
        "tables",
        "relationships",
        "metric_impacts",
        "decomposition_convention",
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_yaml_source.py tests/test_public_api.py -v`
Expected: PASS.

- [ ] **Step 6: Run the whole suite to catch extras-policy fallout**

Run: `uv run pytest -q`
Expected: PASS. If `tests/test_semantic/test_extras_roundtrip.py` fails, a fixture was relying on `decomposition_convention` being an uninterpreted extra — update the fixture, not the constant.

- [ ] **Step 7: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/semantic/yaml_source.py tests/test_semantic/test_yaml_source.py tests/test_public_api.py
git commit -m "feat: source-level decomposition_convention default, resolved at load"
```

---

### Task 3: Round-trip through `dump_semantic_source`, with digest stability

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py` — `dump_semantic_source._dump_metric`
- Test: `tests/test_semantic/test_decomposition.py`, `tests/test_ard/test_catalog_entry.py`

**Interfaces:**
- Consumes: Task 1's fields, Task 2's `YamlSource.from_raw`
- Produces: a dump that emits `convention` / `convention_operand` only when set, so `YamlSource.from_raw(dump_semantic_source(src))` is a fixed point.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_semantic/test_decomposition.py`:

```python
class TestConventionRoundTrip:
    def test_declared_convention_survives_dump_and_reload(self) -> None:
        raw = {
            "metrics": [
                {
                    "name": "activations",
                    "decompositions": [
                        {
                            "operator": "product",
                            "operands": ["volume", "rate"],
                            "convention": "fold_into",
                            "convention_operand": "rate",
                        }
                    ],
                },
                {"name": "volume"},
                {"name": "rate"},
            ]
        }
        reloaded = YamlSource.from_raw(dump_semantic_source(YamlSource.from_raw(raw)))
        metric = reloaded.get_metric("activations")
        assert metric is not None
        assert metric.decompositions[0].convention == "fold_into"
        assert metric.decompositions[0].convention_operand == "rate"

    def test_resolved_default_survives_dump_and_reload(self) -> None:
        # The frozen artifact states the effective convention outright, rather
        # than carrying the source-level key for a consumer to re-apply.
        raw = {
            "decomposition_convention": {"convention": "split_evenly"},
            "metrics": [
                {
                    "name": "activations",
                    "decompositions": [
                        {"operator": "product", "operands": ["volume", "rate"]}
                    ],
                },
                {"name": "volume"},
                {"name": "rate"},
            ],
        }
        dumped = dump_semantic_source(YamlSource.from_raw(raw))
        assert dumped["metrics"][0]["decompositions"][0]["convention"] == "split_evenly"
        reloaded = YamlSource.from_raw(dumped)
        metric = reloaded.get_metric("activations")
        assert metric is not None
        assert metric.decompositions[0].convention == "split_evenly"

    def test_undeclared_convention_emits_no_keys(self) -> None:
        # Digest stability: a contract that declares no convention must dump
        # byte-identically to the pre-0.43 format.
        raw = {
            "metrics": [
                {
                    "name": "activations",
                    "decompositions": [
                        {"operator": "product", "operands": ["volume", "rate"]}
                    ],
                },
                {"name": "volume"},
                {"name": "rate"},
            ]
        }
        dumped = dump_semantic_source(YamlSource.from_raw(raw))
        assert dumped["metrics"][0]["decompositions"][0] == {
            "operator": "product",
            "operands": ["volume", "rate"],
        }
```

Add `dump_semantic_source` and `YamlSource` to the file's imports if absent.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestConventionRoundTrip -v`
Expected: FAIL — the first two assert on a `convention` key the dump does not emit.

- [ ] **Step 3: Emit the keys when set**

In `dump_semantic_source._dump_metric`, replace the decomposition block:

```python
        if m.decompositions:
            decompositions: list[dict[str, Any]] = []
            for d in m.decompositions:
                entry: dict[str, Any] = {
                    "operator": d.operator,
                    "operands": list(d.operands),
                }
                # Omitted when unset for the same reason ``decompositions``
                # itself is omitted when empty: a contract that declares no
                # convention must keep byte-identical canonical bytes, since
                # ``contract_canonical_bytes`` dumps with no ``exclude_none``
                # and any always-present key moves every published digest.
                if d.convention is not None:
                    entry["convention"] = d.convention
                if d.convention_operand is not None:
                    entry["convention_operand"] = d.convention_operand
                decompositions.append(entry)
            data["decompositions"] = decompositions
```

- [ ] **Step 4: Write the digest-stability test**

Append to `tests/test_ard/test_catalog_entry.py`:

```python
def test_contract_without_convention_keeps_stable_canonical_bytes(
    tmp_path: Path,
) -> None:
    # A metric declaring a decomposition but no convention must serialize
    # exactly as it did pre-0.43, or every published ARD attestation moves.
    semantic = tmp_path / "semantic.yml"
    semantic.write_text(
        "metrics:\n"
        "  - name: activations\n"
        "    decompositions:\n"
        "      - operator: product\n"
        "        operands: [volume, rate]\n"
        "  - name: volume\n"
        "  - name: rate\n"
    )
    contract_file = tmp_path / "contract.yml"
    contract_file.write_text(
        "name: t\n"
        "semantic:\n"
        "  source:\n"
        "    type: yaml\n"
        f"    path: {semantic}\n"
    )
    contract = DataContract.from_yaml(str(contract_file))
    payload = json.loads(contract_canonical_bytes(contract))
    decomp = payload["semantic"]["source"]["inline"]["metrics"][0]["decompositions"][0]
    assert decomp == {"operator": "product", "operands": ["volume", "rate"]}
```

Add `json`, `Path`, `DataContract`, and `contract_canonical_bytes` to that file's imports if absent.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_decomposition.py tests/test_ard -v`
Expected: PASS.

- [ ] **Step 6: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/semantic/base.py tests/test_semantic/test_decomposition.py tests/test_ard/test_catalog_entry.py
git commit -m "feat: convention round-trips through dump_semantic_source, digest stays stable"
```

---

### Task 4: `OssieSource` round-trip

**Files:**
- Modify: `src/agentic_data_contracts/semantic/ossie.py` — `_load_metrics`, `_load_model`
- Test: `tests/test_semantic/test_ossie.py`

**Interfaces:**
- Consumes: Task 1's fields, Task 2's `_parse_convention_default` and `_apply_convention_default`
- Produces: nothing new; parity with `YamlSource`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_semantic/test_ossie.py`:

```python
class TestOssieDecompositionConvention:
    def _model(self, extension: dict[str, object]) -> dict[str, object]:
        return {
            "semantic_models": [
                {
                    "name": "sales",
                    "metrics": [
                        {"name": "activations"},
                        {"name": "volume"},
                        {"name": "rate"},
                    ],
                    "custom_extensions": [
                        {
                            "vendor_name": "AGENTIC_DATA_CONTRACTS",
                            "data": json.dumps(extension),
                        }
                    ],
                }
            ]
        }

    def test_per_decomposition_convention_round_trips(self, tmp_path: Path) -> None:
        model = self._model(
            {
                "metrics": {
                    "activations": {
                        "decompositions": [
                            {
                                "operator": "product",
                                "operands": ["volume", "rate"],
                                "convention": "fold_into",
                                "convention_operand": "rate",
                            }
                        ]
                    }
                }
            }
        )
        path = tmp_path / "model.yml"
        path.write_text(yaml.safe_dump(model))
        metric = OssieSource(str(path)).get_metric("activations")
        assert metric is not None
        assert metric.decompositions[0].convention == "fold_into"
        assert metric.decompositions[0].convention_operand == "rate"

    def test_source_level_default_round_trips(self, tmp_path: Path) -> None:
        model = self._model(
            {
                "decomposition_convention": {"convention": "split_evenly"},
                "metrics": {
                    "activations": {
                        "decompositions": [
                            {"operator": "product", "operands": ["volume", "rate"]}
                        ]
                    }
                },
            }
        )
        path = tmp_path / "model.yml"
        path.write_text(yaml.safe_dump(model))
        metric = OssieSource(str(path)).get_metric("activations")
        assert metric is not None
        assert metric.decompositions[0].convention == "split_evenly"
```

Read the top of `tests/test_semantic/test_ossie.py` first: if it already has a helper that serializes a model dict to `tmp_path`, reuse it and delete `_model` above. Add `json`, `yaml`, `Path`, and `OssieSource` to the imports if absent.

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_semantic/test_ossie.py::TestOssieDecompositionConvention -v`
Expected: FAIL — `convention is None`.

- [ ] **Step 3: Parse both fields and the default**

In `ossie.py`, import the two helpers:

```python
from agentic_data_contracts.semantic.yaml_source import (
    _apply_convention_default,
    _parse_convention_default,
)
```

In `_load_metrics`, replace the decomposition comprehension:

```python
                    decompositions=[
                        Decomposition(
                            operator=d["operator"],
                            operands=list(d.get("operands", [])),
                            convention=d.get("convention"),
                            convention_operand=d.get("convention_operand"),
                        )
                        for d in extra.get("decompositions", [])
                    ],
```

In `_load_model`, where `overlay` is built and `_load_metrics` is called, apply the
default after metrics are loaded — insert immediately after the
`self._load_metrics(...)` call:

```python
        _apply_convention_default(
            self._metrics,
            _parse_convention_default(overlay.get("decomposition_convention")),
        )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_ossie.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/semantic/ossie.py tests/test_semantic/test_ossie.py
git commit -m "feat: OssieSource carries the attribution convention"
```

---

### Task 5: Propagate onto `IdentityEdge`

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py` — `IdentityEdge`, `identity_edges_from_metrics`
- Test: `tests/test_semantic/test_decomposition.py`

**Interfaces:**
- Consumes: Task 1's fields
- Produces: `IdentityEdge.convention: str | None`, `IdentityEdge.convention_operand: str | None` — consumed by Task 6's renderer.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_semantic/test_decomposition.py`:

```python
class TestIdentityEdgeConvention:
    def test_every_operand_edge_carries_the_convention(self) -> None:
        # The convention is a property of the identity, and an edge is one
        # operand of it, so all edges from one decomposition share the pair.
        metrics = [
            MetricDefinition(
                name="activations",
                description="",
                sql_expression="",
                decompositions=[
                    Decomposition(
                        operator="product",
                        operands=["volume", "rate"],
                        convention="fold_into",
                        convention_operand="rate",
                    )
                ],
            )
        ]
        edges = identity_edges_from_metrics(metrics)
        assert len(edges) == 2
        assert all(e.convention == "fold_into" for e in edges)
        assert all(e.convention_operand == "rate" for e in edges)

    def test_undeclared_convention_leaves_edges_unset(self) -> None:
        metrics = [
            MetricDefinition(
                name="net",
                description="",
                sql_expression="",
                decompositions=[
                    Decomposition(operator="sum", operands=["a", "b"])
                ],
            )
        ]
        edges = identity_edges_from_metrics(metrics)
        assert all(e.convention is None for e in edges)
        assert all(e.convention_operand is None for e in edges)
```

Add `identity_edges_from_metrics` to the file's imports if absent.

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_semantic/test_decomposition.py::TestIdentityEdgeConvention -v`
Expected: FAIL — `AttributeError: 'IdentityEdge' object has no attribute 'convention'`

- [ ] **Step 3: Add the fields and propagate them**

Replace `IdentityEdge`:

```python
@dataclass
class IdentityEdge:
    """A directed identity edge parent -> operand in the metric graph."""

    from_metric: str  # parent metric
    to_metric: str  # operand metric
    operator: str  # the decomposition operator that produced this edge
    # Carried from the producing decomposition so a root-cause walk that never
    # calls lookup_metric still learns where the cross term goes. Every edge
    # from one decomposition shares the pair.
    convention: str | None = None
    convention_operand: str | None = None

    @property
    def kind(self) -> str:
        return "identity"
```

In `identity_edges_from_metrics`, extend the constructor call:

```python
                edges.append(
                    IdentityEdge(
                        from_metric=metric.name,
                        to_metric=operand,
                        operator=decomp.operator,
                        convention=decomp.convention,
                        convention_operand=decomp.convention_operand,
                    )
                )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_semantic/ -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/semantic/base.py tests/test_semantic/test_decomposition.py
git commit -m "feat: identity edges carry the attribution convention"
```

---

### Task 6: Deliver through both tool channels

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` — the metric-details builder (`data["decompositions"]`) and the `trace_metric_impacts` edge renderer
- Modify: `tests/fixtures/decomposition_source.yml`
- Test: `tests/test_tools/test_decomposition_tools.py`

**Interfaces:**
- Consumes: Task 1's `Decomposition` fields, Task 5's `IdentityEdge` fields
- Produces: nothing new; this is the delivery surface.

**Heads-up — this task breaks a passing test on purpose.** `tests/test_tools/test_decomposition_tools.py::TestLookupMetricSurfacesDecomposition::test_includes_decompositions_and_drill_by` asserts an *exact dict*:

```python
assert {"operator": "product", "operands": ["paying_users", "arpu"]} in data["decompositions"]
```

Adding a convention to the shared fixture makes that dict no longer appear. Step 1 updates it deliberately. `tests/fixtures/decomposition_source.yml` is used by this file only, so nothing else is affected.

- [ ] **Step 1: Extend the shared fixture**

In `tests/fixtures/decomposition_source.yml`, add a convention to `revenue`'s
**product** decomposition and leave its `sum` decomposition untouched — one
metric then covers both the carries-it and omits-it cases:

```yaml
    decompositions:
      - operator: product
        operands: [paying_users, arpu]
        convention: fold_into
        convention_operand: arpu
      - operator: sum
        operands: [new_revenue, expansion_revenue]
```

- [ ] **Step 2: Update the now-stale exact-dict assertion**

In `tests/test_tools/test_decomposition_tools.py`, replace the body of
`test_includes_decompositions_and_drill_by`:

```python
    @pytest.mark.asyncio
    async def test_includes_decompositions_and_drill_by(self) -> None:
        data = await _call(_tools()["lookup_metric"], metric_name="revenue")
        assert {
            "operator": "product",
            "operands": ["paying_users", "arpu"],
            "convention": "fold_into",
            "convention_operand": "arpu",
        } in data["decompositions"]
        assert {
            "dimension": "region",
            "column": "analytics.dim_customer.region",
        } in data["drill_by"]
```

- [ ] **Step 3: Write the failing tests**

Append to the same file, reusing its existing `_tools()` and
`_call(fn, **args)` helpers:

```python
class TestConventionDelivery:
    @pytest.mark.asyncio
    async def test_lookup_metric_omits_an_undeclared_convention(self) -> None:
        # revenue's `sum` decomposition declares none: the keys must be absent,
        # not present-and-null. An always-present key would move every
        # published contract digest.
        data = await _call(_tools()["lookup_metric"], metric_name="revenue")
        sums = [d for d in data["decompositions"] if d["operator"] == "sum"]
        assert sums
        assert "convention" not in sums[0]
        assert "convention_operand" not in sums[0]

    @pytest.mark.asyncio
    async def test_trace_identity_edges_carry_the_convention(self) -> None:
        # trace_metric_impacts' own description tells the agent to walk
        # 'identity' first for root cause -- that is the attribution workflow.
        # It must not hand over the operator without the convention.
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="arpu",
            direction="upstream",
            kinds="identity",
        )
        product_edges = [e for e in data["edges"] if e["operator"] == "product"]
        assert product_edges
        for edge in product_edges:
            assert edge["convention"] == "fold_into"
            assert edge["convention_operand"] == "arpu"

    @pytest.mark.asyncio
    async def test_trace_omits_an_undeclared_convention(self) -> None:
        data = await _call(
            _tools()["trace_metric_impacts"],
            metric_name="new_revenue",
            direction="upstream",
            kinds="identity",
        )
        sum_edges = [e for e in data["edges"] if e["operator"] == "sum"]
        assert sum_edges
        assert all("convention" not in e for e in sum_edges)
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_decomposition_tools.py -v`
Expected: FAIL — `test_includes_decompositions_and_drill_by` and
`test_trace_identity_edges_carry_the_convention` both fail because the payload
carries no `convention` key. The two omit-when-unset tests already pass; keep
them as regression guards.

- [ ] **Step 5: Add the keys to the `lookup_metric` payload**

In `factory.py`, replace the decomposition block in the metric-details builder:

```python
    if metric.decompositions:
        decompositions: list[dict[str, Any]] = []
        for d in metric.decompositions:
            entry: dict[str, Any] = {
                "operator": d.operator,
                "operands": list(d.operands),
            }
            if d.convention is not None:
                entry["convention"] = d.convention
            if d.convention_operand is not None:
                entry["convention_operand"] = d.convention_operand
            decompositions.append(entry)
        data["decompositions"] = decompositions
```

- [ ] **Step 6: Add the keys to the identity-edge renderer**

In `trace_metric_impacts`, replace the `isinstance(edge, IdentityEdge)` branch:

```python
            if isinstance(edge, IdentityEdge):
                entry["operator"] = edge.operator
                # The convention rides here too: an agent following this tool's
                # own root-cause guidance ("walk 'identity' first") may never
                # call lookup_metric, and the operator alone does not say where
                # the cross term goes.
                if edge.convention is not None:
                    entry["convention"] = edge.convention
                if edge.convention_operand is not None:
                    entry["convention_operand"] = edge.convention_operand
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `uv run pytest tests/test_tools/ -v`
Expected: PASS.

- [ ] **Step 8: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_decomposition_tools.py tests/fixtures/decomposition_source.yml
git commit -m "feat: lookup_metric and trace_metric_impacts carry the convention"
```

---

### Task 7: The attribution kernel — `attribute_change`

**Files:**
- Create: `src/agentic_data_contracts/validation/attribution.py`
- Test: `tests/test_validation/test_attribution.py` (create)

**Interfaces:**
- Consumes: Task 1's `Decomposition` fields, `_CROSS_TERM_OPERATORS`; `_apply_operator` from `validation/reconciliation.py`
- Produces: `INTERACTION_KEY: str`, `AttributionResult`, `attribute_change(metric, *, before, after, decomposition=0) -> AttributionResult`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_validation/test_attribution.py`:

```python
import math

import pytest

from agentic_data_contracts.semantic.base import Decomposition, MetricDefinition
from agentic_data_contracts.validation.attribution import (
    INTERACTION_KEY,
    AttributionResult,
    attribute_change,
)

# The #67 worked example. parent = volume * rate.
BEFORE = {"volume": 10_000.0, "rate": 0.35}
AFTER = {"volume": 15_000.0, "rate": 0.45}


def _metric(
    convention: str | None,
    *,
    convention_operand: str | None = None,
    operator: str = "product",
    operands: list[str] | None = None,
) -> MetricDefinition:
    return MetricDefinition(
        name="activations",
        description="",
        sql_expression="",
        decompositions=[
            Decomposition(
                operator=operator,
                operands=operands if operands is not None else ["volume", "rate"],
                convention=convention,
                convention_operand=convention_operand,
            )
        ],
    )


class TestWorkedExample:
    """Reproduces every row of the #67 convention table."""

    def test_explicit_leaves_the_residual_unattributed(self) -> None:
        r = attribute_change(_metric("explicit"), before=BEFORE, after=AFTER)
        assert abs(r.delta_parent - 3250.0) < 1e-6
        assert abs(r.contributions["volume"] - 1750.0) < 1e-6
        assert abs(r.contributions["rate"] - 1000.0) < 1e-6
        assert abs(r.interaction - 500.0) < 1e-6

    def test_split_evenly_divides_the_residual(self) -> None:
        r = attribute_change(_metric("split_evenly"), before=BEFORE, after=AFTER)
        assert abs(r.contributions["volume"] - 2000.0) < 1e-6
        assert abs(r.contributions["rate"] - 1250.0) < 1e-6
        # The raw residual is still reported so the placement stays auditable.
        assert abs(r.interaction - 500.0) < 1e-6

    def test_fold_into_rate_is_laspeyres(self) -> None:
        r = attribute_change(
            _metric("fold_into", convention_operand="rate"),
            before=BEFORE,
            after=AFTER,
        )
        assert abs(r.contributions["volume"] - 1750.0) < 1e-6
        assert abs(r.contributions["rate"] - 1500.0) < 1e-6

    def test_fold_into_volume_is_paasche(self) -> None:
        r = attribute_change(
            _metric("fold_into", convention_operand="volume"),
            before=BEFORE,
            after=AFTER,
        )
        assert abs(r.contributions["volume"] - 2250.0) < 1e-6
        assert abs(r.contributions["rate"] - 1000.0) < 1e-6

    def test_every_convention_sums_to_delta(self) -> None:
        # The #67 finding: all four are arithmetically correct. What differs is
        # the split, which is exactly why it must be declared.
        for metric in (
            _metric("explicit"),
            _metric("split_evenly"),
            _metric("fold_into", convention_operand="rate"),
            _metric("fold_into", convention_operand="volume"),
        ):
            r = attribute_change(metric, before=BEFORE, after=AFTER)
            total = math.fsum(r.contributions.values())
            if r.convention == "explicit":
                total += r.interaction
            assert abs(total - r.delta_parent) < 1e-6

    def test_shares_are_relative_to_delta(self) -> None:
        r = attribute_change(_metric("split_evenly"), before=BEFORE, after=AFTER)
        assert r.shares is not None
        assert abs(r.shares["volume"] - 2000.0 / 3250.0) < 1e-9

    def test_shares_is_none_when_delta_is_zero(self) -> None:
        # None, not {} -- "undefined" must be distinguishable from "all zero".
        r = attribute_change(
            _metric("split_evenly"),
            before={"volume": 100.0, "rate": 0.5},
            after={"volume": 50.0, "rate": 1.0},
        )
        assert abs(r.delta_parent) < 1e-9
        assert r.shares is None


class TestOtherOperators:
    def test_ratio_residual_is_the_mix_effect(self) -> None:
        r = attribute_change(
            _metric("explicit", operands=["num", "den"], operator="ratio"),
            before={"num": 100.0, "den": 10.0},
            after={"num": 120.0, "den": 12.0},
        )
        assert abs(r.delta_parent) < 1e-9
        assert abs(r.contributions["num"] - 2.0) < 1e-9
        assert abs(r.contributions["den"] - (100.0 / 12.0 - 10.0)) < 1e-9
        assert abs(r.interaction - (0.0 - r.contributions["num"]
                                    - r.contributions["den"])) < 1e-9

    def test_sum_is_linear_and_needs_no_convention(self) -> None:
        r = attribute_change(
            _metric(None, operator="sum", operands=["a", "b"]),
            before={"a": 1.0, "b": 2.0},
            after={"a": 4.0, "b": 6.0},
        )
        assert abs(r.delta_parent - 7.0) < 1e-9
        assert abs(r.contributions["a"] - 3.0) < 1e-9
        assert abs(r.contributions["b"] - 4.0) < 1e-9
        assert abs(r.interaction) < 1e-9

    def test_difference_negates_the_subtrahend(self) -> None:
        r = attribute_change(
            _metric(None, operator="difference", operands=["a", "b"]),
            before={"a": 10.0, "b": 3.0},
            after={"a": 12.0, "b": 8.0},
        )
        assert abs(r.delta_parent - (-3.0)) < 1e-9
        assert abs(r.contributions["a"] - 2.0) < 1e-9
        assert abs(r.contributions["b"] - (-5.0)) < 1e-9


class TestPreconditions:
    def test_no_decomposition_raises(self) -> None:
        metric = MetricDefinition(name="m", description="", sql_expression="")
        with pytest.raises(ValueError, match="declares no decompositions"):
            attribute_change(metric, before={}, after={})

    def test_index_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError, match="out of range"):
            attribute_change(
                _metric("explicit"), before=BEFORE, after=AFTER, decomposition=3
            )

    def test_key_mismatch_raises(self) -> None:
        with pytest.raises(ValueError, match="do not match the declared operands"):
            attribute_change(
                _metric("explicit"), before={"volume": 1.0}, after=AFTER
            )

    def test_cross_term_operator_without_convention_raises(self) -> None:
        # The kernel only works on a governed metric. That is the point.
        with pytest.raises(ValueError, match="declares no attribution convention"):
            attribute_change(_metric(None), before=BEFORE, after=AFTER)

    def test_zero_ratio_denominator_raises(self) -> None:
        with pytest.raises(ValueError, match="denominator"):
            attribute_change(
                _metric("explicit", operands=["num", "den"], operator="ratio"),
                before={"num": 1.0, "den": 0.0},
                after={"num": 1.0, "den": 2.0},
            )


def test_result_is_frozen() -> None:
    # Matches the AttributeError + `# type: ignore` style TestResultType uses
    # in test_reconciliation.py.
    r = attribute_change(_metric("explicit"), before=BEFORE, after=AFTER)
    assert isinstance(r, AttributionResult)
    with pytest.raises(AttributeError):
        r.delta_parent = 0.0  # type: ignore


def test_interaction_key_is_exported() -> None:
    assert INTERACTION_KEY == "interaction"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_attribution.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'agentic_data_contracts.validation.attribution'`

- [ ] **Step 3: Write the module**

Create `src/agentic_data_contracts/validation/attribution.py`:

```python
"""Attribute a metric's change to its factors under the contract's convention.

Pure arithmetic: measured values in, contribution breakdown out. No adapter, no
SQL, and deliberately no time-window semantics — the caller owns *when* each
value was measured (calendar vs. cohort windows are analytics-domain logic, not
governance), and the contract owns where the cross term goes.

The cross-term placement is a non-inferable business fact. An agent told only
the factors picks a placement silently and presents it as canonical; two such
reports are indistinguishable because both sum correctly. That is what
``Decomposition.convention`` declares and what this module applies.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Deferred for the circular-import reason documented in
    # ``validation.reconciliation``: ``semantic.base`` imports ``TableSchema``
    # from ``adapters.base``, which initializes this package first. Safe at
    # runtime because ``from __future__ import annotations`` keeps annotations
    # unevaluated.
    from agentic_data_contracts.semantic.base import Decomposition, MetricDefinition

#: Key under which an ``explicit`` breakdown reports the unattributed residual.
#: A constant so callers need not hardcode the string.
INTERACTION_KEY = "interaction"


@dataclass(frozen=True)
class AttributionResult:
    metric: str
    operator: str
    convention: str | None
    convention_operand: str | None
    delta_parent: float
    contributions: dict[str, float]
    interaction: float
    shares: dict[str, float] | None
    # Populated only by ``check_attribution``.
    reported: dict[str, float] | None = None
    deviations: dict[str, float] | None = None
    matches: bool | None = None
    sums_to_delta: bool | None = None
    rel_tol: float | None = None
    abs_tol: float | None = None
    reason: str | None = None


def _resolve(metric: MetricDefinition, index: int) -> Decomposition:
    """Select the declared decomposition, raising before any arithmetic."""
    if not metric.decompositions:
        raise ValueError(f"metric {metric.name!r} declares no decompositions")
    if not 0 <= index < len(metric.decompositions):
        raise ValueError(
            f"decomposition index {index} out of range for metric "
            f"{metric.name!r} ({len(metric.decompositions)} declared)"
        )
    return metric.decompositions[index]


def _check_keys(
    values: Mapping[str, float], label: str, operands: list[str], metric_name: str
) -> None:
    if set(values) != set(operands):
        raise ValueError(
            f"{label} keys {sorted(values)} do not match the declared operands "
            f"{operands} of metric {metric_name!r}"
        )


def _main_effects(
    operator: str,
    operands: list[str],
    before: Mapping[str, float],
    after: Mapping[str, float],
) -> dict[str, float]:
    """Each operand's effect with every other operand held at its ``before``.

    The residual left over is the interaction, computed by the caller as
    ``delta - sum(effects)`` so it is a single lumped term at any arity rather
    than the 2**n - 1 expansion. That matches what an analyst reports as "the
    cross term" and is what the #67 explicit-cross runs actually produced.
    """
    if operator == "product":
        effects: dict[str, float] = {}
        for i, name in enumerate(operands):
            others = 1.0
            for j, other in enumerate(operands):
                if j != i:
                    others *= before[other]
            effects[name] = (after[name] - before[name]) * others
        return effects
    if operator == "ratio":
        num, den = operands
        return {
            num: (after[num] - before[num]) / before[den],
            den: before[num] / after[den] - before[num] / before[den],
        }
    if operator == "sum":
        return {name: after[name] - before[name] for name in operands}
    first, second = operands
    return {
        first: after[first] - before[first],
        second: -(after[second] - before[second]),
    }


def _place(
    convention: str | None,
    convention_operand: str | None,
    effects: dict[str, float],
    interaction: float,
) -> dict[str, float]:
    """Distribute the interaction residual as the declared convention says."""
    contributions = dict(effects)
    if convention == "split_evenly":
        share = interaction / len(contributions)
        for name in contributions:
            contributions[name] += share
    elif convention == "fold_into":
        assert convention_operand is not None  # validated at load and above
        contributions[convention_operand] += interaction
    # "explicit" (and the linear operators) leave the residual where it is.
    return contributions


def attribute_change(
    metric: MetricDefinition,
    *,
    before: Mapping[str, float],
    after: Mapping[str, float],
    decomposition: int = 0,
) -> AttributionResult:
    """Break a metric's change into per-factor contributions.

    ``before`` / ``after`` map each declared operand to its measured value at
    the two points being compared. The decomposition's declared ``convention``
    places the interaction residual; a ``product`` or ``ratio`` decomposition
    that declares none raises, because the answer would otherwise be one of
    several defensible numbers with no way to tell which was used.
    """
    from agentic_data_contracts.semantic.base import _CROSS_TERM_OPERATORS
    from agentic_data_contracts.validation.reconciliation import _apply_operator

    decomp = _resolve(metric, decomposition)
    operands = list(decomp.operands)
    _check_keys(before, "before", operands, metric.name)
    _check_keys(after, "after", operands, metric.name)

    if decomp.operator in _CROSS_TERM_OPERATORS and decomp.convention is None:
        raise ValueError(
            f"metric {metric.name!r} decomposition declares no attribution "
            f"convention; a {decomp.operator!r} identity has a cross term whose "
            f"placement changes the answer, so it must be declared"
        )
    if decomp.operator == "ratio":
        for label, values in (("before", before), ("after", after)):
            if values[operands[1]] == 0:
                raise ValueError(
                    f"ratio denominator (operand {operands[1]!r}) is zero at "
                    f"{label}"
                )

    delta = _apply_operator(decomp.operator, [after[n] for n in operands]) - (
        _apply_operator(decomp.operator, [before[n] for n in operands])
    )
    effects = _main_effects(decomp.operator, operands, before, after)
    interaction = delta - math.fsum(effects.values())
    contributions = _place(
        decomp.convention, decomp.convention_operand, effects, interaction
    )
    shares = (
        None
        if delta == 0
        else {name: value / delta for name, value in contributions.items()}
    )
    return AttributionResult(
        metric=metric.name,
        operator=decomp.operator,
        convention=decomp.convention,
        convention_operand=decomp.convention_operand,
        delta_parent=delta,
        contributions=contributions,
        interaction=interaction,
        shares=shares,
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_validation/test_attribution.py -v`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/validation/attribution.py tests/test_validation/test_attribution.py
git commit -m "feat: pure attribution kernel applies the declared convention"
```

---

### Task 8: `check_attribution` and package exports

**Files:**
- Modify: `src/agentic_data_contracts/validation/attribution.py`
- Modify: `src/agentic_data_contracts/validation/__init__.py`
- Test: `tests/test_validation/test_attribution.py`

**Interfaces:**
- Consumes: Task 7's `attribute_change`, `AttributionResult`, `INTERACTION_KEY`
- Produces: `check_attribution(metric, *, before, after, reported, rel_tol=1e-4, abs_tol=0.0, decomposition=0) -> AttributionResult`, exported from `agentic_data_contracts.validation`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_validation/test_attribution.py`:

```python
class TestCheckAttribution:
    def test_matching_breakdown_passes(self) -> None:
        r = check_attribution(
            _metric("split_evenly"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 2000.0, "rate": 1250.0},
        )
        assert r.matches is True
        assert r.sums_to_delta is True
        assert r.reason is None

    def test_wrong_convention_is_caught_even_though_it_sums(self) -> None:
        # This is the #67 failure exactly: correct arithmetic, wrong split.
        r = check_attribution(
            _metric("split_evenly"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 1750.0, "rate": 1500.0},
        )
        assert r.sums_to_delta is True
        assert r.matches is False
        assert r.reason is not None
        assert r.deviations is not None
        assert abs(r.deviations["volume"] - (-250.0)) < 1e-6

    def test_explicit_requires_the_interaction_key(self) -> None:
        r = check_attribution(
            _metric("explicit"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 1750.0, "rate": 1000.0, INTERACTION_KEY: 500.0},
        )
        assert r.matches is True

    def test_explicit_without_the_interaction_key_raises(self) -> None:
        with pytest.raises(ValueError, match="do not match"):
            check_attribution(
                _metric("explicit"),
                before=BEFORE,
                after=AFTER,
                reported={"volume": 1750.0, "rate": 1000.0},
            )

    def test_interaction_key_rejected_when_already_distributed(self) -> None:
        # Under split_evenly the residual is inside the contributions;
        # reporting it separately double-counts.
        with pytest.raises(ValueError, match="do not match"):
            check_attribution(
                _metric("split_evenly"),
                before=BEFORE,
                after=AFTER,
                reported={"volume": 2000.0, "rate": 1250.0, INTERACTION_KEY: 500.0},
            )

    def test_tolerance_is_scaled_to_delta_parent(self) -> None:
        # rel_tol * |delta| = 1e-3 * 3250 = 3.25, so a 3.0 deviation passes
        # and a 4.0 deviation does not.
        inside = check_attribution(
            _metric("split_evenly"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 2003.0, "rate": 1250.0},
            rel_tol=1e-3,
        )
        assert inside.matches is True
        outside = check_attribution(
            _metric("split_evenly"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 2004.0, "rate": 1250.0},
            rel_tol=1e-3,
        )
        assert outside.matches is False

    def test_expected_fields_are_still_populated(self) -> None:
        r = check_attribution(
            _metric("split_evenly"),
            before=BEFORE,
            after=AFTER,
            reported={"volume": 2000.0, "rate": 1250.0},
        )
        assert abs(r.contributions["volume"] - 2000.0) < 1e-6
        assert r.rel_tol == 1e-4
        assert r.abs_tol == 0.0


def test_attribution_is_exported_from_the_validation_package() -> None:
    from agentic_data_contracts.validation import (
        AttributionResult as Exported,
    )
    from agentic_data_contracts.validation import attribute_change, check_attribution

    assert Exported is AttributionResult
    assert attribute_change is not None
    assert check_attribution is not None
```

Add `check_attribution` to the module's import block at the top of the file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_validation/test_attribution.py -v`
Expected: FAIL — `ImportError: cannot import name 'check_attribution'`

- [ ] **Step 3: Implement `check_attribution`**

Append to `validation/attribution.py`:

```python
def check_attribution(
    metric: MetricDefinition,
    *,
    before: Mapping[str, float],
    after: Mapping[str, float],
    reported: Mapping[str, float],
    rel_tol: float = 1e-4,
    abs_tol: float = 0.0,
    decomposition: int = 0,
) -> AttributionResult:
    """Check a reported breakdown against the contract's declared convention.

    The intended caller is an **eval harness**, not CI and not production: a
    ``reported`` breakdown exists only inside an agent's written answer, and
    post-hoc checking is the wrong shape for a failure with an agent in the
    loop by construction. What it is good for is measuring whether declaring a
    convention changes what an agent reports.

    Under ``explicit``, *reported* must also carry the residual under
    ``INTERACTION_KEY``; under the other conventions that key is rejected,
    because the residual has already been distributed and reporting it again
    double-counts. Every deviation is judged against
    ``max(abs_tol, rel_tol * abs(delta_parent))`` — a contribution is
    meaningful as a share of the total change, and a per-contribution relative
    tolerance explodes when a contribution is near zero.
    """
    expected = attribute_change(
        metric, before=before, after=after, decomposition=decomposition
    )
    target = dict(expected.contributions)
    if expected.convention == "explicit":
        target[INTERACTION_KEY] = expected.interaction

    if set(reported) != set(target):
        raise ValueError(
            f"reported keys {sorted(reported)} do not match the expected "
            f"breakdown {sorted(target)} for metric {metric.name!r} under "
            f"convention {expected.convention!r}"
        )

    tolerance = max(abs_tol, rel_tol * abs(expected.delta_parent))
    deviations = {name: reported[name] - value for name, value in target.items()}
    matches = all(abs(d) <= tolerance for d in deviations.values())
    sums_to_delta = (
        abs(math.fsum(reported.values()) - expected.delta_parent) <= tolerance
    )
    return replace(
        expected,
        reported=dict(reported),
        deviations=deviations,
        matches=matches,
        sums_to_delta=sums_to_delta,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
        reason=(
            None
            if matches
            else "reported contributions do not match the declared convention "
            "within tolerance"
        ),
    )
```

- [ ] **Step 4: Export from the validation package**

In `validation/__init__.py`, add the import beside the reconciliation one:

```python
from agentic_data_contracts.validation.attribution import (
    INTERACTION_KEY,
    AttributionResult,
    attribute_change,
    check_attribution,
)
```

And add `"INTERACTION_KEY"`, `"AttributionResult"`, `"attribute_change"`, and
`"check_attribution"` to `__all__`, keeping its existing sort order.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_validation/ -v`
Expected: PASS.

- [ ] **Step 6: Run the whole suite**

Run: `uv run pytest -q`
Expected: PASS.

- [ ] **Step 7: Lint and commit**

```bash
prek run --all-files
git add src/agentic_data_contracts/validation/attribution.py src/agentic_data_contracts/validation/__init__.py tests/test_validation/test_attribution.py
git commit -m "feat: check_attribution scores a reported breakdown against the contract"
```

---

### Task 9: Documentation and release

**Files:**
- Modify: `README.md` — "Metric decomposition and drill dimensions"; the `trace_metric_impacts` example; the `reconcile_decomposition` sibling paragraph
- Modify: `docs/architecture.md` — a v0.43.0 section; the Future Extensions variance-diagnosis entry
- Modify: `CHANGELOG.md`, `pyproject.toml`
- Delete: `docs/superpowers/specs/2026-08-18-decomposition-attribution-convention-design.md`, `docs/superpowers/plans/2026-08-18-decomposition-attribution-convention.md`

**Interfaces:**
- Consumes: everything above
- Produces: the shipped release.

- [ ] **Step 1: Document the vocabulary in the README**

In "Metric decomposition and drill dimensions", after the paragraph ending "A
metric with no decomposition is a valid leaf." and before the operand-units
note, add:

````markdown
**Declaring where the cross term goes.** When an agent attributes a *change* in
`total_revenue` to its factors, the `ΔC·ΔP` cross term has to land somewhere —
and every placement sums correctly, so two reports built on different
placements are indistinguishable. A pilot across 16 sessions found three
distinct placements on identical data, with a 13.5% swing on the headline
contribution and the narrative conclusion flipping between them. That makes the
placement a non-inferable business fact, so declare it:

```yaml
decomposition_convention:
  convention: split_evenly       # source-wide default

metrics:
  - name: activations
    decompositions:
      - operator: product
        operands: [volume, rate]
        convention: fold_into    # explicit | split_evenly | fold_into
        convention_operand: rate # required for fold_into, overrides the default
```

| Convention | Cross term | Known as |
|---|---|---|
| `explicit` | reported on its own line, attributed to no factor | — |
| `split_evenly` | divided equally among the operands | Shapley, at two operands |
| `fold_into` | absorbed entirely by `convention_operand` | Laspeyres / Paasche, at two operands |

Only `product` and `ratio` have a cross term; declaring a convention on `sum`
or `difference` raises at load. The default is resolved onto each decomposition
when the source loads, so a frozen contract states its effective convention
outright. Both `lookup_metric` and `trace_metric_impacts` carry the declaration
to the agent.
````

- [ ] **Step 2: Update the `trace_metric_impacts` README example**

Replace the returned-edge comment in that section so it shows the pair:

```python
# Returns edges like: {"depth": 1, "from": "total_revenue", "to": "arpu",
#                      "kind": "identity", "operator": "product",
#                      "convention": "split_evenly"}
```

- [ ] **Step 3: Document the kernel in the README**

After the `reconcile_decomposition` sibling paragraph, add:

````markdown
`attribute_change(...)` applies a declared convention to measured values —
values in, contributions out, no database access, because *when* each value was
measured is the caller's business:

```python
from agentic_data_contracts.validation import attribute_change

result = attribute_change(
    contract.semantic_source.get_metric("activations"),
    before={"volume": 10_000, "rate": 0.35},
    after={"volume": 15_000, "rate": 0.45},
)
result.contributions   # {"volume": 2000.0, "rate": 1250.0} under split_evenly
result.interaction     # 500.0 — the raw residual, so the placement is auditable
```

Its sibling `check_attribution(...)` scores a *reported* breakdown against the
declared convention. Its intended caller is an **eval harness** measuring
whether an agent follows the contract — not CI and not production, since a
reported breakdown exists only inside a written answer.
````

- [ ] **Step 4: Update `docs/architecture.md`**

Add this paragraph after the v0.29.0 reconciliation section:

```markdown
**Declared attribution convention (v0.43.0+):** A `Decomposition` can declare `convention` (`explicit` | `split_evenly` | `fold_into`) and, for `fold_into`, a `convention_operand` naming which operand absorbs the cross term. This exists because attributing a metric's *change* to its factors has no single right answer: the `ΔC·ΔP` term can be left explicit, split evenly (Shapley), or folded into either factor, and every placement sums to the observed change. A 16-session pilot (#67) found three distinct placements on identical data, a 13.5% span on the headline contribution, and the narrative conclusion flipping between them — with 3 of 16 runs disclosing no convention at all. That is the same species of fact as "active users excludes staff": not derivable from the schema at any level of model intelligence, so it belongs in the contract. The operand is **named rather than positional** because `product` takes two or more operands, so `fold_into_last` would both make operand order load-bearing (it is semantically free for `product` today) and hand the agent an index to count instead of a metric name. `laspeyres` / `paasche` are documented as the analyst-facing names of the two-operand `fold_into` cases, never as schema values — they have no agreed meaning at higher arity, and adopting them would split the vocabulary by operand count. A source-level `decomposition_convention` key (in the YAML source, or the Ossie vendor block) sets a house default, restricted to the conventions that need no operand; it is **resolved onto each cross-term decomposition at load** rather than carried, so the effective value survives `freeze_semantic_source` — which re-serializes from parsed objects — and a frozen contract states its convention outright instead of leaving a consumer to re-derive it. It is deliberately *not* a `SemanticConfig` field: `contract_canonical_bytes` dumps with no `exclude_none`, so a contract-schema key would serialize as `null` for every contract that omits it and move every published ARD digest, and the `_CANONICAL_EXCLUDE` alternative would hide a governance fact from the attestation meant to describe the contract. Both keys are omitted from `dump_semantic_source` and from the tool payloads when unset, so a contract declaring no convention keeps byte-identical canonical bytes. Delivery is through **both** tool channels a decomposition already reaches the agent by — `lookup_metric` and, via `IdentityEdge`, `trace_metric_impacts`; the second is not optional, because that tool's own description tells the agent to "walk 'identity' first to localize the change" for root cause, which *is* the attribution workflow, and handing over the operator without the convention would omit the field governing the answer. This is grounding, not enforcement: an attribution report is prose and never passes a checker the way SQL does. `attribute_change` / `check_attribution` (in `validation/attribution.py`) apply and score a convention as pure arithmetic — values in, contributions out, no adapter, because calendar-vs-cohort window semantics are analytics-domain logic rather than governance. Neither is wired into `create_tools()`: such a tool would fire at the last step to do arithmetic the pilot shows the agent performs correctly (16/16), its only real content is the convention that both channels already deliver, and a tenth tool dilutes attention on the checkers that actually block queries.
```

Then rewrite the closing of the Future Extensions variance-diagnosis entry.
Replace the sentence "Neither outcome has been observed yet; variance
attribution has not been exercised in a pilot as of this revision." with:

```markdown
  **Resolved (2026-08-18, #67).** The probe was run: 16 sessions, two arms.
  Outcome #1 is **disconfirmed** — 16/16 runs were arithmetically correct, so
  the correctness kernel with a fixed default is not the artifact and that
  branch is closed rather than pending. Outcome #2 **fired** — three distinct
  cross-term placements in the undeclared arm, two in the declared arm, with a
  13.5% span on the headline contribution and the narrative conclusion flipping
  between them. Declaring `decompositions` did not stabilize the convention and
  structurally could not, since it names which factors participate and carries
  nothing about the cross term. v0.43.0 therefore ships the declared vocabulary
  (`convention` / `convention_operand`), delivered through both tool channels an
  attribution question already touches, plus the pure kernel as an importable
  helper. It is **not** wired into `create_tools()`: the tool would fire at the
  last step to do arithmetic the pilot shows the agent performs correctly, and
  its only real content is the convention, which both channels already deliver.
  The diagnosis tool itself stays out of scope, unchanged.
```

- [ ] **Step 5: Add the CHANGELOG entry and bump the version**

Add a `## [0.43.0] - 2026-08-18` section above `## [0.42.0]`, following that
file's established voice (a bolded lead sentence per bullet, then the reasoning
and the trap each decision avoids). It must cover, at minimum:

- **Added — declared attribution convention.** The pilot result in one line
  (16/16 arithmetically correct, three placements, 13.5% span), the three
  vocabulary values, the named-operand decision, and the YAML example.
- **Added — source-level `decomposition_convention` default.** Resolved at load;
  why that keeps a frozen contract explicit; why `fold_into` cannot be a default.
- **Added — `attribute_change` / `check_attribution`.** Pure, adapter-free, and
  scoped honestly: `check_attribution`'s caller is an eval harness, not CI.
  State plainly that neither is wired into `create_tools()`, and why.
- **Changed — `SEMANTIC_KEYS` gained `decomposition_convention`.** This is a
  **public API change**: the constant is re-exported at the top level, README
  and the v0.41.0 CHANGELOG both name it, and `tests/test_public_api.py` asserts
  its exact contents. A contract that listed `decomposition_convention` under
  `expected_extras` should drop it — it is interpreted vocabulary now.
- **Unchanged — digests.** A contract declaring no convention produces
  byte-identical `contract_canonical_bytes`; both keys are omitted when unset.

Set `version = "0.43.0"` in `pyproject.toml`.

- [ ] **Step 6: Verify the whole suite and the linters**

Run: `uv run pytest -q && prek run --all-files`
Expected: all tests pass, all hooks pass.

- [ ] **Step 7: Remove the process artifacts**

The spec and this plan are scaffolding, not shipped documentation — everything
durable now lives in `docs/architecture.md` and the README.

```bash
git rm docs/superpowers/specs/2026-08-18-decomposition-attribution-convention-design.md
git rm docs/superpowers/plans/2026-08-18-decomposition-attribution-convention.md
```

- [ ] **Step 8: Commit and open the PR**

```bash
git add README.md docs/architecture.md CHANGELOG.md pyproject.toml
git commit -m "docs: declared attribution convention (v0.43.0)"
git push -u origin feat/decomposition-attribution-convention
gh pr create --title "feat: declared attribution convention for decompositions (v0.43.0)" --body "Closes #67 ..."
```
