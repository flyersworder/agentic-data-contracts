# Prompt Renderer: XML-Optimized System Prompt + Custom Renderer Protocol

**Date:** 2026-03-30
**Status:** Draft

## Problem

The current `to_system_prompt()` generates flat Markdown output. Claude models are fine-tuned to recognize XML tags as structural boundaries, and Anthropic's prompt engineering guidance recommends XML for complex agentic system prompts. Additionally, users targeting non-Claude models need the ability to control prompt formatting entirely.

## Design

### PromptRenderer Protocol

A new `@runtime_checkable` protocol in `src/agentic_data_contracts/core/prompt.py`:

```python
@runtime_checkable
class PromptRenderer(Protocol):
    def render(
        self,
        contract: DataContract,
        semantic_source: SemanticSource | None = None,
    ) -> str: ...
```

Follows the project's existing protocol pattern (`DatabaseAdapter`, `SemanticSource`, `Checker`).

### ClaudePromptRenderer

The single built-in renderer. Generates XML-structured output optimized for Claude Sonnet 4.6+.

Design principles (from Anthropic's prompt engineering guidance):
- XML tags for unambiguous section boundaries
- Constraints consolidated at the end (improves instruction-following)
- Resource limits and temporal limits merged into one section
- Direct language without bold emphasis or ALL CAPS (Sonnet 4.6 follows normal language)
- Metrics section uses existing `METRIC_DETAIL_THRESHOLD` scaling logic

Output structure (refined during implementation — uses full XML elements for structured data, plain text for human-readable instructions):

```xml
<data_contract name="{name}">
<allowed_tables>
Only query these tables:
- schema.table1
- schema.table2
</allowed_tables>
<available_metrics>
  <metric name="metric_name">description</metric>
  <hint>Use lookup_metric tool to get the SQL definition before computing any KPI.</hint>
</available_metrics>
<table_relationships>
  <relationship type="many_to_one"><from>schema.table.col</from><to>schema.table.col</to></relationship>
</table_relationships>
<resource_limits>
  <cost_limit_usd>5.00</cost_limit_usd>
  <max_retries>3</max_retries>
  <token_budget>50000</token_budget>
  <max_query_time_seconds>30</max_query_time_seconds>
  <max_rows_scanned>1000000</max_rows_scanned>
  <max_duration_seconds>300</max_duration_seconds>
</resource_limits>
<constraints>
Forbidden operations: DELETE, DROP, TRUNCATE, UPDATE, INSERT

Rules (violations block execution):
- [rule_name] description

Rules (violations produce warnings):
- [rule_name] description
</constraints>
</data_contract>
```

Sections are omitted when empty (same behavior as today).

### to_system_prompt() Changes

```python
def to_system_prompt(
    self,
    semantic_source: SemanticSource | None = None,
    *,
    renderer: PromptRenderer | None = None,
) -> str:
```

When `renderer` is provided, delegates entirely: `return renderer.render(self, semantic_source)`. Otherwise instantiates `ClaudePromptRenderer` and calls it.

### File Layout

**New file:** `src/agentic_data_contracts/core/prompt.py`
- `PromptRenderer` protocol
- `ClaudePromptRenderer` class
- `_build_metrics_section()` helper (moved from contract.py)

**Modified files:**
- `core/contract.py` — `to_system_prompt()` becomes a thin delegate; inline prompt logic removed
- `__init__.py` — export `PromptRenderer` and `ClaudePromptRenderer`

**Tests:**
- Update `tests/test_core/test_contract.py` — assertions match XML output
- Update `tests/test_core/test_system_prompt_metrics.py` — assertions match XML output
- New `tests/test_core/test_prompt_renderers.py` — ClaudePromptRenderer output structure, custom renderer protocol compliance, metrics scaling in XML context

### Public API Additions

```python
from agentic_data_contracts import PromptRenderer, ClaudePromptRenderer
```

### What's NOT Changing

- `create_tools()` and tool descriptions — unchanged
- `DataContract` loading, validation, accessors — unchanged
- `_build_metrics_section()` scaling logic (METRIC_DETAIL_THRESHOLD) — unchanged, just moved
- `to_system_prompt()` signature is additive only (new optional `renderer` kwarg)
