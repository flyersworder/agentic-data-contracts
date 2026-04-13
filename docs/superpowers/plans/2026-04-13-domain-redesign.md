# Domain Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Redesign `domains` from a flat `dict[str, list[str]]` to a first-class `Domain` model with summary, description, metrics, and optional tables — plus a new `lookup_domain` tool.

**Architecture:** Add `Domain` Pydantic model to `core/schema.py`, add `get_domain()` helper to `core/contract.py`, add `lookup_domain` tool to `tools/factory.py`, update prompt rendering to use compact domain index with summaries, update `list_metrics` internal lookup. All existing tests that construct domains in the old dict format must be migrated.

**Tech Stack:** Python 3.12+, Pydantic 2, thefuzz (for fuzzy domain matching), pytest + pytest-asyncio

**Spec:** `docs/superpowers/specs/2026-04-13-domain-redesign-design.md`

---

### Task 1: Add `Domain` Model and Update `SemanticConfig`

**Files:**
- Modify: `src/agentic_data_contracts/core/schema.py:72-77`
- Test: `tests/test_core/test_schema_validation.py` (create if needed, or use existing schema tests)

- [ ] **Step 1: Write the failing test for the new Domain model**

Create `tests/test_core/test_domain_model.py`:

```python
"""Tests for the Domain Pydantic model."""

from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)


def test_domain_model_basic():
    d = Domain(
        name="revenue",
        summary="Financial metrics",
        description="Revenue tracks recognized revenue from completed orders.",
        metrics=["total_revenue", "mrr"],
    )
    assert d.name == "revenue"
    assert d.summary == "Financial metrics"
    assert d.description == "Revenue tracks recognized revenue from completed orders."
    assert d.metrics == ["total_revenue", "mrr"]
    assert d.tables == []


def test_domain_model_with_tables():
    d = Domain(
        name="revenue",
        summary="Financial metrics",
        description="Revenue domain.",
        metrics=["total_revenue"],
        tables=["analytics.orders", "analytics.invoices"],
    )
    assert d.tables == ["analytics.orders", "analytics.invoices"]


def test_semantic_config_with_domains():
    config = SemanticConfig(
        allowed_tables=[
            AllowedTable.model_validate({"schema": "analytics", "tables": ["orders"]})
        ],
        domains=[
            Domain(
                name="revenue",
                summary="Financial metrics",
                description="Revenue domain.",
                metrics=["total_revenue"],
            ),
            Domain(
                name="engagement",
                summary="Customer activity",
                description="Engagement domain.",
                metrics=["active_customers"],
            ),
        ],
    )
    assert len(config.domains) == 2
    assert config.domains[0].name == "revenue"


def test_semantic_config_domains_default_empty():
    config = SemanticConfig(
        allowed_tables=[
            AllowedTable.model_validate({"schema": "analytics", "tables": ["orders"]})
        ],
    )
    assert config.domains == []


def test_domain_in_full_contract_schema():
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                )
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Financial metrics",
                    description="Revenue domain.",
                    metrics=["total_revenue"],
                ),
            ],
        ),
    )
    assert schema.semantic.domains[0].name == "revenue"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/test_domain_model.py -v`
Expected: ImportError — `Domain` not found in `schema.py`

- [ ] **Step 3: Implement the Domain model and update SemanticConfig**

In `src/agentic_data_contracts/core/schema.py`, add the `Domain` class before `SemanticConfig` and change the `domains` field:

```python
class Domain(BaseModel):
    name: str
    summary: str
    description: str
    metrics: list[str] = Field(default_factory=list)
    tables: list[str] = Field(default_factory=list)


class SemanticConfig(BaseModel):
    source: SemanticSource | None = None
    allowed_tables: list[AllowedTable] = Field(default_factory=list)
    forbidden_operations: list[str] = Field(default_factory=list)
    rules: list[SemanticRule] = Field(default_factory=list)
    domains: list[Domain] = Field(default_factory=list)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_core/test_domain_model.py -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/core/schema.py tests/test_core/test_domain_model.py
git commit -m "feat: add Domain model to schema, change domains from dict to list[Domain]"
```

---

### Task 2: Update YAML Fixture and `from_yaml` Parsing

**Files:**
- Modify: `tests/fixtures/valid_contract.yml:14-16`

- [ ] **Step 1: Write the failing test for YAML parsing**

Add to `tests/test_core/test_domain_model.py`:

```python
from pathlib import Path

from agentic_data_contracts.core.contract import DataContract


def test_domain_from_yaml(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    domains = dc.schema.semantic.domains
    assert len(domains) == 2

    revenue = domains[0]
    assert revenue.name == "revenue"
    assert revenue.summary != ""
    assert revenue.description != ""
    assert "total_revenue" in revenue.metrics

    engagement = domains[1]
    assert engagement.name == "engagement"
    assert "active_customers" in engagement.metrics
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/test_domain_model.py::test_domain_from_yaml -v`
Expected: FAIL — the current YAML has dict format which won't parse into `list[Domain]`

- [ ] **Step 3: Update the YAML fixture**

Replace the `domains` section in `tests/fixtures/valid_contract.yml`:

```yaml
  domains:
    - name: revenue
      summary: "Revenue and financial metrics from completed orders"
      description: >
        Revenue metrics track recognized revenue from completed orders.
        Revenue is recognized at fulfillment, not at booking.
      metrics: [total_revenue]
    - name: engagement
      summary: "Customer activity and retention patterns"
      description: >
        Customer engagement measures active usage patterns
        and retention over time.
      metrics: [active_customers]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_core/test_domain_model.py::test_domain_from_yaml -v`
Expected: PASS

- [ ] **Step 5: Run full test suite to see what broke**

Run: `uv run pytest -v`
Expected: Several test files will fail because they construct domains in the old `dict` format. This is expected — we fix them in the next tasks.

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/valid_contract.yml tests/test_core/test_domain_model.py
git commit -m "feat: update valid_contract.yml to new domain format"
```

---

### Task 3: Add `get_domain()` Helper to `DataContract`

**Files:**
- Modify: `src/agentic_data_contracts/core/contract.py`
- Test: `tests/test_core/test_domain_model.py` (append)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_core/test_domain_model.py`:

```python
def test_get_domain_exact_match():
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                )
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Financial metrics",
                    description="Revenue domain.",
                    metrics=["total_revenue"],
                ),
                Domain(
                    name="engagement",
                    summary="Customer activity",
                    description="Engagement domain.",
                    metrics=["active_customers"],
                ),
            ],
        ),
    )
    dc = DataContract(schema)

    result = dc.get_domain("revenue")
    assert result is not None
    assert result.name == "revenue"

    result = dc.get_domain("engagement")
    assert result is not None
    assert result.name == "engagement"

    result = dc.get_domain("nonexistent")
    assert result is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core/test_domain_model.py::test_get_domain_exact_match -v`
Expected: AttributeError — `DataContract` has no attribute `get_domain`

- [ ] **Step 3: Implement `get_domain` in contract.py**

Add this method to `DataContract` in `src/agentic_data_contracts/core/contract.py`:

```python
from agentic_data_contracts.core.schema import Domain  # add to imports

def get_domain(self, name: str) -> Domain | None:
    """Find a domain by exact name, or None."""
    for d in self.schema.semantic.domains:
        if d.name == name:
            return d
    return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_core/test_domain_model.py::test_get_domain_exact_match -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/core/contract.py tests/test_core/test_domain_model.py
git commit -m "feat: add get_domain() helper to DataContract"
```

---

### Task 4: Add `lookup_domain` Tool

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py`
- Test: `tests/test_tools/test_semantic_tools.py`

- [ ] **Step 1: Write the failing tests for lookup_domain**

Add to `tests/test_tools/test_semantic_tools.py`. First, update the `contract_with_domains` fixture to use the new `Domain` model, then add the new tests:

```python
# Update import at top of file — add Domain
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)


# Replace the contract_with_domains fixture:
@pytest.fixture
def contract_with_domains(fixtures_dir: Path) -> DataContract:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Financial metrics from completed orders",
                    description="Revenue metrics track recognized revenue. Revenue is recognized at fulfillment, not at booking.",
                    metrics=["total_revenue"],
                    tables=["analytics.orders"],
                ),
                Domain(
                    name="engagement",
                    summary="Customer activity and retention",
                    description="Customer engagement measures active usage patterns.",
                    metrics=["active_customers"],
                ),
            ],
        ),
    )
    return DataContract(schema)


# Add these new tests:

@pytest.mark.asyncio
async def test_lookup_domain_exact_match(
    contract_with_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_domain")
    result = await tool.callable({"name": "revenue"})
    text = result["content"][0]["text"]
    data = json.loads(text)
    assert data["name"] == "revenue"
    assert data["summary"] == "Financial metrics from completed orders"
    assert "fulfillment" in data["description"]
    assert len(data["metrics"]) == 1
    assert data["metrics"][0]["name"] == "total_revenue"
    assert data["metrics"][0]["description"] != ""  # should have description from source
    assert data["tables"] == ["analytics.orders"]


@pytest.mark.asyncio
async def test_lookup_domain_not_found(
    contract_with_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_domain")
    result = await tool.callable({"name": "nonexistent"})
    text = result["content"][0]["text"]
    assert "not found" in text.lower()


@pytest.mark.asyncio
async def test_lookup_domain_fuzzy_match(
    contract_with_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "lookup_domain")
    result = await tool.callable({"name": "rev"})
    text = result["content"][0]["text"]
    data = json.loads(text)
    # Should fuzzy-match to "revenue"
    assert data["exact_match"] is False
    assert len(data["candidates"]) >= 1
    assert data["candidates"][0]["name"] == "revenue"


@pytest.mark.asyncio
async def test_lookup_domain_no_semantic_source(
    contract_with_domains: DataContract,
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=None)
    tool = next(t for t in tools if t.name == "lookup_domain")
    result = await tool.callable({"name": "revenue"})
    text = result["content"][0]["text"]
    data = json.loads(text)
    assert data["name"] == "revenue"
    # Metrics should be names only (no descriptions) since no source
    assert data["metrics"] == ["total_revenue"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_semantic_tools.py::test_lookup_domain_exact_match -v`
Expected: StopIteration — no tool named `lookup_domain`

- [ ] **Step 3: Implement the `lookup_domain` tool in factory.py**

In `src/agentic_data_contracts/tools/factory.py`:

1. Update the docstring from "11 agent tools" to "12 agent tools".

2. Add the `lookup_domain` function after the `lookup_metric` tool (after line 231):

```python
# ── Tool 12: lookup_domain ──────────────────────────────────────────
async def lookup_domain(args: dict[str, Any]) -> dict[str, Any]:
    name = args.get("name", "")
    domain = contract.get_domain(name)

    if domain is not None:
        # Exact match — enrich metrics with descriptions from semantic source
        if semantic_source is not None:
            metric_data = []
            for metric_name in domain.metrics:
                m = semantic_source.get_metric(metric_name)
                if m is not None:
                    metric_data.append(
                        {"name": m.name, "description": m.description}
                    )
                else:
                    metric_data.append({"name": metric_name, "description": ""})
        else:
            metric_data = domain.metrics  # type: ignore[assignment]

        data: dict[str, Any] = {
            "name": domain.name,
            "summary": domain.summary,
            "description": domain.description,
            "metrics": metric_data,
        }
        if domain.tables:
            data["tables"] = domain.tables
        return _text_response(json.dumps(data))

    # Fuzzy fallback over domain names
    all_domains = contract.schema.semantic.domains
    if not all_domains:
        return _text_response(f"Domain '{name}' not found. No domains defined.")

    from thefuzz import fuzz, process

    choices = {d.name: d.name for d in all_domains}
    results = process.extractBests(
        name,
        choices,
        scorer=fuzz.token_set_ratio,
        score_cutoff=50,
        limit=3,
    )
    if not results:
        available = [d.name for d in all_domains]
        return _text_response(
            f"Domain '{name}' not found. Available domains: {available}"
        )

    candidates = []
    for _, _, key in results:
        d = contract.get_domain(key)
        if d is not None:
            candidates.append(
                {
                    "name": d.name,
                    "summary": d.summary,
                    "metric_count": len(d.metrics),
                }
            )
    return _text_response(
        json.dumps(
            {
                "query": name,
                "exact_match": False,
                "candidates": candidates,
            }
        )
    )
```

3. Add the `ToolDef` for `lookup_domain` in the return list (after `lookup_metric`):

```python
ToolDef(
    name="lookup_domain",
    description=(
        "Look up a business domain by name to get its full description,"
        " associated metrics, and tables. Use this to understand"
        " business context before querying."
    ),
    input_schema={
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": "Name of the domain to look up",
            }
        },
        "required": ["name"],
    },
    callable=lookup_domain,
),
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_tools/test_semantic_tools.py -v`
Expected: All tests PASS (including the updated `contract_with_domains` fixture and new lookup_domain tests)

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_semantic_tools.py
git commit -m "feat: add lookup_domain tool (tool 12) with fuzzy matching"
```

---

### Task 5: Update `list_metrics` Domain Lookup

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py:168-191`

The `list_metrics` tool currently does `domains.get(domain_filter, [])` which is dict-style. It needs to find a `Domain` object by name instead.

- [ ] **Step 1: Verify the existing `list_metrics` domain tests still pass**

Run: `uv run pytest tests/test_tools/test_semantic_tools.py::test_list_metrics_with_domain tests/test_tools/test_semantic_tools.py::test_list_metrics_unknown_domain -v`
Expected: These may already pass if the fixture was updated in Task 4. If they fail, proceed to Step 2.

- [ ] **Step 2: Update `list_metrics` domain lookup in factory.py**

Replace the domain filtering block in `list_metrics` (lines ~172-182):

```python
async def list_metrics(args: dict[str, Any]) -> dict[str, Any]:
    if semantic_source is None:
        return _text_response("No semantic source configured.")
    metrics = semantic_source.get_metrics()
    domain_filter = args.get("domain")
    if domain_filter:
        domain = contract.get_domain(domain_filter)
        if domain is None:
            available = [d.name for d in contract.schema.semantic.domains]
            return _text_response(
                f"Domain '{domain_filter}' not found."
                f" Available domains: {available}"
            )
        allowed_names = set(domain.metrics)
        metrics = [m for m in metrics if m.name in allowed_names]
    data = [
        {
            "name": m.name,
            "description": m.description,
            "source_model": m.source_model,
        }
        for m in metrics
    ]
    return _text_response(json.dumps({"metrics": data}))
```

- [ ] **Step 3: Run the list_metrics tests**

Run: `uv run pytest tests/test_tools/test_semantic_tools.py -v`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py
git commit -m "refactor: update list_metrics to use Domain model for domain lookup"
```

---

### Task 6: Update System Prompt Rendering

**Files:**
- Modify: `src/agentic_data_contracts/core/prompt.py:79-139`
- Modify: `tests/test_core/test_system_prompt_metrics.py`
- Modify: `tests/test_core/test_prompt_renderers.py`
- Modify: `tests/test_core/test_scalability.py`

This task updates the `_render_metrics` method to render a compact domain index when domains exist, and updates all tests that construct domains in the old dict format.

- [ ] **Step 1: Update test helpers in test_scalability.py**

Replace `_make_contract_with_domains` in `tests/test_core/test_scalability.py`:

```python
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)

def _make_contract_with_domains(
    metric_names: list[str],
) -> DataContract:
    half = len(metric_names) // 2
    domains = [
        Domain(
            name="domain_a",
            summary="Domain A metrics",
            description="Domain A full description.",
            metrics=metric_names[:half],
        ),
        Domain(
            name="domain_b",
            summary="Domain B metrics",
            description="Domain B full description.",
            metrics=metric_names[half:],
        ),
    ]
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]}),
            ],
            domains=domains,
        ),
    )
    return DataContract(schema)
```

- [ ] **Step 2: Update test assertions in test_scalability.py**

The `test_small_set_lists_all_metrics` test currently asserts `'name="metric_0"' in prompt`. After the redesign, when domains exist the prompt will show the compact domain index (domain name + summary + metric_count), not individual metrics. Update:

```python
class TestCompactMetricPrompt:
    def test_small_set_with_domains_shows_domain_index(self) -> None:
        source = FakeSemanticSource(5)
        dc = _make_contract_with_domains([f"metric_{i}" for i in range(5)])
        prompt = dc.to_system_prompt(semantic_source=source)
        # With domains defined, always show compact domain index
        assert "<available_domains>" in prompt
        assert 'name="domain_a"' in prompt
        assert 'summary="Domain A metrics"' in prompt
        assert "lookup_domain" in prompt

    def test_large_set_with_domains_shows_domain_index(self) -> None:
        source = FakeSemanticSource(30)
        dc = _make_contract_with_domains([f"metric_{i}" for i in range(30)])
        prompt = dc.to_system_prompt(semantic_source=source)
        assert "<available_domains>" in prompt
        assert 'name="domain_a"' in prompt
        assert 'metric_count="15"' in prompt
        assert "lookup_domain" in prompt
        # Should NOT list individual metrics
        assert 'name="metric_0"' not in prompt

    def test_large_set_no_domains_shows_count(self) -> None:
        # This test stays the same — no domains = old behavior
        source = FakeSemanticSource(30)
        schema = DataContractSchema(
            name="test",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate({"schema": "public", "tables": ["t"]}),
                ],
            ),
        )
        dc = DataContract(schema)
        prompt = dc.to_system_prompt(semantic_source=source)
        assert "30 metrics available" in prompt

    def test_threshold_boundary(self) -> None:
        # Without domains — threshold behavior unchanged
        source = FakeSemanticSource(20)
        schema = DataContractSchema(
            name="test",
            semantic=SemanticConfig(
                allowed_tables=[
                    AllowedTable.model_validate({"schema": "public", "tables": ["t"]}),
                ],
            ),
        )
        dc = DataContract(schema)
        prompt = dc.to_system_prompt(semantic_source=source)
        assert 'name="metric_0"' in prompt

        source = FakeSemanticSource(21)
        prompt = dc.to_system_prompt(semantic_source=source)
        assert 'name="metric_0"' not in prompt
        assert "21 metrics available" in prompt
```

- [ ] **Step 3: Update test_prompt_renderers.py**

Update the `_make_contract_with_domains` helper and tests 7/8 in `tests/test_core/test_prompt_renderers.py`:

```python
# Update import to add Domain
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)


def _make_contract_with_domains(metric_names: list[str]) -> DataContract:
    half = len(metric_names) // 2
    domains = [
        Domain(
            name="domain_a",
            summary="Domain A metrics",
            description="Domain A full description.",
            metrics=metric_names[:half],
        ),
        Domain(
            name="domain_b",
            summary="Domain B metrics",
            description="Domain B full description.",
            metrics=metric_names[half:],
        ),
    ]
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]}),
            ],
            domains=domains,
        ),
    )
    return DataContract(schema)


# Update test 7:
def test_claude_renderer_metrics_large_set_with_domains() -> None:
    contract = _make_contract_with_domains([f"metric_{i}" for i in range(30)])
    source = FakeSemanticSource(30)
    renderer = ClaudePromptRenderer()
    output = renderer.render(contract, semantic_source=source)

    assert "<available_domains>" in output
    assert "domain_a" in output
    assert "domain_b" in output
    # Should NOT list individual metric descriptions
    assert "metric_0 —" not in output
```

- [ ] **Step 4: Update test_system_prompt_metrics.py**

Update `test_system_prompt_with_domains` in `tests/test_core/test_system_prompt_metrics.py`:

```python
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Domain,
    SemanticConfig,
)


def test_system_prompt_with_domains(fixtures_dir: Path) -> None:
    source = YamlSource(fixtures_dir / "semantic_source.yml")
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]})
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Revenue and financial metrics",
                    description="Revenue domain.",
                    metrics=["total_revenue"],
                ),
                Domain(
                    name="engagement",
                    summary="Customer activity metrics",
                    description="Engagement domain.",
                    metrics=["active_customers"],
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    prompt = dc.to_system_prompt(semantic_source=source)
    assert 'name="revenue"' in prompt
    assert 'name="engagement"' in prompt
    assert "lookup_domain" in prompt
```

- [ ] **Step 5: Run all the updated tests to confirm they fail (prompt output doesn't match yet)**

Run: `uv run pytest tests/test_core/test_scalability.py tests/test_core/test_prompt_renderers.py tests/test_core/test_system_prompt_metrics.py -v`
Expected: Several FAIL — prompt still renders old format

- [ ] **Step 6: Implement the new prompt rendering**

In `src/agentic_data_contracts/core/prompt.py`, add a `_render_domains` method and update `render()` to call it:

Update the `render` method to call `_render_domains` before `_render_metrics`:

```python
def render(
    self,
    contract: DataContract,
    semantic_source: SemanticSource | None = None,
) -> str:
    lines: list[str] = []

    # Opening wrapper
    lines.append(f'<data_contract name="{contract.name}">')

    # 1. Allowed tables
    lines.extend(self._render_allowed_tables(contract))

    # 2. Domains (if defined) OR metrics OR semantic_source fallback
    domain_lines = self._render_domains(contract, semantic_source)
    if domain_lines:
        lines.extend(domain_lines)
    else:
        metrics_lines = self._render_metrics(contract, semantic_source)
        if metrics_lines:
            lines.extend(metrics_lines)
        elif contract.schema.semantic.source:
            lines.extend(self._render_semantic_source_fallback(contract))

    # 3. Table relationships
    rel_lines = self._render_relationships(semantic_source)
    if rel_lines:
        lines.extend(rel_lines)

    # 4. Resource limits (resources + temporal merged)
    resource_lines = self._render_resource_limits(contract)
    if resource_lines:
        lines.extend(resource_lines)

    # 5. Constraints (forbidden ops + rules)
    lines.extend(self._render_constraints(contract))

    # Closing wrapper
    lines.append("</data_contract>")

    return "\n".join(lines)
```

Add the `_render_domains` method:

```python
def _render_domains(
    self,
    contract: DataContract,
    semantic_source: SemanticSource | None,
) -> list[str]:
    domains = contract.schema.semantic.domains
    if not domains:
        return []

    lines = ["<available_domains>"]
    for domain in domains:
        metric_count = len(domain.metrics)
        lines.append(
            f'  <domain name="{domain.name}"'
            f' summary="{domain.summary}"'
            f' metric_count="{metric_count}" />'
        )
    lines.append(
        '  <hint>Use lookup_domain("...") for business context,'
        ' then lookup_metric("...") for SQL definitions.</hint>'
    )
    lines.append("</available_domains>")
    return lines
```

Update `_render_metrics` to remove the domain branches (it now only handles the no-domains case):

```python
def _render_metrics(
    self,
    contract: DataContract,
    semantic_source: SemanticSource | None,
) -> list[str]:
    if semantic_source is None:
        return []

    metrics = semantic_source.get_metrics()
    if not metrics:
        return []

    lines: list[str] = ["<available_metrics>"]
    compact = len(metrics) > self.METRIC_DETAIL_THRESHOLD

    if compact:
        lines.append(f"  <count>{len(metrics)} metrics available.</count>")
        lines.append(
            "  <hint>Use list_metrics() to browse,"
            ' lookup_metric("...") to get SQL definitions.</hint>'
        )
    else:
        for m in metrics:
            lines.append(f'  <metric name="{m.name}">{m.description}</metric>')
        lines.append(
            "  <hint>Use lookup_metric tool to get the SQL definition"
            " before computing any KPI.</hint>"
        )

    lines.append("</available_metrics>")
    return lines
```

- [ ] **Step 7: Run all prompt-related tests**

Run: `uv run pytest tests/test_core/test_scalability.py tests/test_core/test_prompt_renderers.py tests/test_core/test_system_prompt_metrics.py -v`
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add src/agentic_data_contracts/core/prompt.py tests/test_core/test_scalability.py tests/test_core/test_prompt_renderers.py tests/test_core/test_system_prompt_metrics.py
git commit -m "feat: render compact domain index in system prompt, simplify _render_metrics"
```

---

### Task 7: Run Full Test Suite and Fix Remaining Failures

**Files:**
- Potentially any file with old domain dict format

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest -v`
Expected: Check for any remaining failures from the old domain format

- [ ] **Step 2: Fix any remaining test failures**

Look for any test that constructs `domains={...}` as a dict and update to `domains=[Domain(...)]`. Common locations:
- Any test file not already updated in previous tasks
- Example files in `examples/` directory

- [ ] **Step 3: Run linting and type checking**

Run: `uv run ruff check src/ tests/ && uv run ruff format src/ tests/ && ty check`
Expected: All clean

- [ ] **Step 4: Run full suite one final time**

Run: `uv run pytest -v`
Expected: All PASS

- [ ] **Step 5: Commit any remaining fixes**

```bash
git add -A
git commit -m "fix: update remaining tests and examples for new Domain model"
```

---

### Task 8: Update `get_contract_info` to Include Domains

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` (in `get_contract_info`)
- Test: `tests/test_tools/test_semantic_tools.py` (append)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_tools/test_semantic_tools.py`:

```python
@pytest.mark.asyncio
async def test_get_contract_info_includes_domains(
    contract_with_domains: DataContract, semantic: YamlSource
) -> None:
    tools = create_tools(contract_with_domains, semantic_source=semantic)
    tool = next(t for t in tools if t.name == "get_contract_info")
    result = await tool.callable({})
    text = result["content"][0]["text"]
    data = json.loads(text)
    assert "domains" in data
    assert len(data["domains"]) == 2
    assert data["domains"][0]["name"] == "revenue"
    assert data["domains"][0]["summary"] == "Financial metrics from completed orders"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_tools/test_semantic_tools.py::test_get_contract_info_includes_domains -v`
Expected: FAIL — `domains` key not in response

- [ ] **Step 3: Update `get_contract_info` in factory.py**

In the `get_contract_info` function, add domain info after the rules section (around line 410):

```python
if contract.schema.semantic.domains:
    info["domains"] = [
        {"name": d.name, "summary": d.summary, "metric_count": len(d.metrics)}
        for d in contract.schema.semantic.domains
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_tools/test_semantic_tools.py::test_get_contract_info_includes_domains -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_semantic_tools.py
git commit -m "feat: include domain summaries in get_contract_info response"
```

---

### Task 9: Domain Validation Warnings in Tool Factory

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py`
- Test: `tests/test_tools/test_semantic_tools.py` (append)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_tools/test_semantic_tools.py`:

```python
import logging

@pytest.mark.asyncio
async def test_domain_validation_warns_unknown_metric(
    caplog: pytest.LogCaptureFixture,
) -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Financial metrics",
                    description="Revenue domain.",
                    metrics=["total_revenue", "nonexistent_metric"],
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    source = YamlSource(fixtures_dir_path / "semantic_source.yml")

    with caplog.at_level(logging.WARNING):
        create_tools(dc, semantic_source=source)

    assert any("nonexistent_metric" in msg for msg in caplog.messages)


@pytest.mark.asyncio
async def test_domain_validation_warns_unknown_table(
    caplog: pytest.LogCaptureFixture,
) -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Financial metrics",
                    description="Revenue domain.",
                    metrics=["total_revenue"],
                    tables=["analytics.orders", "analytics.nonexistent"],
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    source = YamlSource(fixtures_dir_path / "semantic_source.yml")

    with caplog.at_level(logging.WARNING):
        create_tools(dc, semantic_source=source)

    assert any("analytics.nonexistent" in msg for msg in caplog.messages)
```

Note: `fixtures_dir_path` needs to be defined. Either add a module-level constant `fixtures_dir_path = Path(__file__).parent.parent / "fixtures"` or use the `fixtures_dir` fixture. Adjust based on whether the test is inside a class or standalone. If using pytest fixtures, convert these to take `fixtures_dir: Path` as a parameter and construct the source inside:

```python
@pytest.mark.asyncio
async def test_domain_validation_warns_unknown_metric(
    fixtures_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Financial metrics",
                    description="Revenue domain.",
                    metrics=["total_revenue", "nonexistent_metric"],
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    source = YamlSource(fixtures_dir / "semantic_source.yml")

    with caplog.at_level(logging.WARNING):
        create_tools(dc, semantic_source=source)

    assert any("nonexistent_metric" in msg for msg in caplog.messages)


@pytest.mark.asyncio
async def test_domain_validation_warns_unknown_table(
    fixtures_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
            domains=[
                Domain(
                    name="revenue",
                    summary="Financial metrics",
                    description="Revenue domain.",
                    metrics=["total_revenue"],
                    tables=["analytics.orders", "analytics.nonexistent"],
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    source = YamlSource(fixtures_dir / "semantic_source.yml")

    with caplog.at_level(logging.WARNING):
        create_tools(dc, semantic_source=source)

    assert any("analytics.nonexistent" in msg for msg in caplog.messages)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_semantic_tools.py::test_domain_validation_warns_unknown_metric -v`
Expected: FAIL — no warning logged

- [ ] **Step 3: Add validation logic to `create_tools` in factory.py**

Add at the top of the file:

```python
import logging

logger = logging.getLogger(__name__)
```

Then after the `_rel_index` construction (around line 70), add:

```python
# Validate domain references
if contract.schema.semantic.domains:
    allowed_tables_set = set(contract.allowed_table_names())
    metric_names_set = (
        {m.name for m in semantic_source.get_metrics()}
        if semantic_source is not None
        else set()
    )
    for domain in contract.schema.semantic.domains:
        if semantic_source is not None:
            for metric_name in domain.metrics:
                if metric_name not in metric_names_set:
                    logger.warning(
                        "Domain '%s' references unknown metric '%s'",
                        domain.name,
                        metric_name,
                    )
        for table in domain.tables:
            if table not in allowed_tables_set:
                logger.warning(
                    "Domain '%s' references table '%s'"
                    " not in allowed_tables",
                    domain.name,
                    table,
                )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_tools/test_semantic_tools.py::test_domain_validation_warns_unknown_metric tests/test_tools/test_semantic_tools.py::test_domain_validation_warns_unknown_table -v`
Expected: Both PASS

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_semantic_tools.py
git commit -m "feat: validate domain metric/table references at tool creation time"
```

---

### Task 10: Final Validation

- [ ] **Step 1: Run full test suite**

Run: `uv run pytest -v`
Expected: All PASS

- [ ] **Step 2: Run linting and formatting**

Run: `uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/`
Expected: Clean

- [ ] **Step 3: Run type checking**

Run: `ty check`
Expected: Clean (or only pre-existing issues)

- [ ] **Step 4: Run pre-commit hooks**

Run: `prek run --all-files`
Expected: All checks pass

- [ ] **Step 5: Verify tool count is 12**

Run: `uv run python -c "from agentic_data_contracts.core.schema import *; from agentic_data_contracts.core.contract import *; from agentic_data_contracts.tools.factory import *; dc = DataContract.from_yaml('tests/fixtures/valid_contract.yml'); tools = create_tools(dc); print(f'Tool count: {len(tools)}'); print([t.name for t in tools])"`
Expected: `Tool count: 12` with `lookup_domain` in the list
