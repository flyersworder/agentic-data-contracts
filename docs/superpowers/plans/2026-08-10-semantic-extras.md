# Semantic Extras, Renderer Rename, and Honest Degradation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close issue #60 by making the semantic layer say what it is doing — carry consumer-authored YAML sections instead of silently discarding them, render them only when named, and log when the prompt renderer degrades its own output.

**Architecture:** `YamlSource` keeps every top-level key outside `SEMANTIC_KEYS` as opaque "extras", normalizes them to JSON-safe values at load, and exposes them via a sibling `ExtensibleSemanticSource` protocol. `dump_semantic_source` carries extras into the frozen contract so they reach `contract_digest`. `XmlPromptRenderer` (renamed from `ClaudePromptRenderer`) renders extras only for sections the consumer names, and its two detail thresholds become constructor parameters that log when they degrade.

**Tech Stack:** Python 3.12, pydantic v2, PyYAML, sqlglot, thefuzz, pytest, uv, ruff + ty via prek.

## Global Constraints

- **Run everything through `uv run`.** `uv run pytest -v`, never a bare `pytest`.
- **Run linters through prek, never directly.** `prek run --all-files`. The hook `rev`s in `.pre-commit-config.yaml` pin ruff and ty and CI runs those same hooks; a bare `ty check` or `uv run ruff` uses whatever is on PATH and drifts.
- **No `file.py:NNN` references in Python files.** A `pygrep` hook (`no-file-line-refs`) fails the commit on the pattern `[A-Za-z_][A-Za-z0-9_]*\.py:[0-9]+`. Name the symbol instead. There is no inline escape.
- **TDD.** Write the failing test, run it, watch it fail for the right reason, then implement.
- **Extras are never interpreted.** No schema validation, no staleness checking, no tool serves them. The only processing applied is JSON-safety normalization, and that exists solely because extras reach `json.dumps` via `contract_canonical_bytes`.
- **Omit-when-empty is load-bearing.** A contract with no extras must produce byte-identical `contract_canonical_bytes`, so no published ARD digest moves.
- **Do not rewrite CHANGELOG history.** Existing `ClaudePromptRenderer` mentions in `CHANGELOG.md` stay as written.
- **Three releases:** v0.39.0 (Task 1), v0.40.0 (Tasks 2–3), v0.41.0 (Tasks 4–9). Bump `version` in `pyproject.toml` and add a CHANGELOG entry in the final task of each group.

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `src/agentic_data_contracts/core/prompt.py` | `PromptRenderer` protocol, `XmlPromptRenderer`, `_Unset` sentinel, `_default_extra_section` | 1, 2, 3, 8 |
| `src/agentic_data_contracts/semantic/yaml_source.py` | `SEMANTIC_KEYS`, extras capture, `_apply_extras_policy`, `_normalize_extras`, `get_extras` | 4, 5, 6 |
| `src/agentic_data_contracts/semantic/base.py` | `ExtensibleSemanticSource` protocol, module-level `_iso`, extras in `dump_semantic_source` | 7 |
| `src/agentic_data_contracts/__init__.py` | Public exports | 1, 7 |
| `tests/test_core/test_prompt_renderers.py` | Renderer behaviour | 1, 2, 3, 8 |
| `tests/test_semantic/test_yaml_source.py` | Extras carriage and policy | 4, 5, 6 |
| `tests/test_semantic/test_extras_roundtrip.py` (new) | Dump/rehydrate/digest stability | 7 |
| `tests/test_public_api.py` | Export surface | 1, 7 |

---

## PR 1 — v0.39.0: rename the renderer

### Task 1: Rename `ClaudePromptRenderer` to `XmlPromptRenderer`

**Files:**
- Modify: `src/agentic_data_contracts/core/prompt.py`
- Modify: `src/agentic_data_contracts/__init__.py`
- Modify: `src/agentic_data_contracts/core/contract.py`
- Modify: `src/agentic_data_contracts/tools/factory.py`
- Modify: `README.md`, `docs/architecture.md`, `examples/revenue_agent/agent.py`
- Modify: `pyproject.toml`, `CHANGELOG.md`
- Test: `tests/test_public_api.py`, `tests/test_core/test_prompt_renderers.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `XmlPromptRenderer` — the class every later task extends. `ClaudePromptRenderer` no longer exists anywhere in `src/`, `tests/`, `README.md`, `docs/`, or `examples/`.

- [ ] **Step 1: Write the failing test**

In `tests/test_public_api.py`, replace both `ClaudePromptRenderer` occurrences in `test_top_level_imports` and add a guard that the old name is gone:

```python
def test_top_level_imports() -> None:
    from agentic_data_contracts import (
        DataContract,
        MetricDefinition,
        MetricImpact,
        PromptRenderer,
        Relationship,
        SemanticSource,
        XmlPromptRenderer,
        contract_middleware,
        create_tools,
    )

    assert DataContract is not None
    assert create_tools is not None
    assert contract_middleware is not None
    assert PromptRenderer is not None
    assert XmlPromptRenderer is not None
    assert MetricDefinition is not None
    assert MetricImpact is not None
    assert Relationship is not None
    assert SemanticSource is not None


def test_claude_prompt_renderer_is_gone() -> None:
    """v0.39.0 renamed it with no alias — a stale import must fail loudly."""
    import agentic_data_contracts

    assert not hasattr(agentic_data_contracts, "ClaudePromptRenderer")
    assert "ClaudePromptRenderer" not in agentic_data_contracts.__all__
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_public_api.py -v`
Expected: FAIL — `ImportError: cannot import name 'XmlPromptRenderer'`.

- [ ] **Step 3: Perform the mechanical rename**

```bash
rg -l 'ClaudePromptRenderer' src tests README.md docs/architecture.md examples \
  | xargs sed -i '' 's/ClaudePromptRenderer/XmlPromptRenderer/g'
```

Two exclusions are deliberate, and both matter. `CHANGELOG.md` is history — released notes are not rewritten. And the path is `docs/architecture.md`, **not** `docs`: `docs/superpowers/` holds the spec and this plan, which both discuss the rename in prose. Renaming inside them produces sentences like "XmlPromptRenderer is renamed to XmlPromptRenderer" and destroys the record of why.

- [ ] **Step 4: Fix the three strings that need rewording, not just renaming**

The rename leaves behind copy that still claims Claude specificity. In `src/agentic_data_contracts/core/prompt.py`, the class docstring:

```python
class XmlPromptRenderer:
    """Renders a DataContract as XML-structured output for LLM agents.

    XML tags mark section boundaries. That convention originates in Anthropic's
    prompting guidance, but nothing here is Claude-specific — no SDK import, no
    model-specific handling — and every frontier model reads it. The name states
    the axis on which ``PromptRenderer`` implementations differ: output format,
    not vendor.
    """
```

In `README.md`, the sentence that currently reads "The default `XmlPromptRenderer` produces XML-structured output optimized for Claude models:" becomes:

```markdown
The system prompt is generated by a `PromptRenderer`. The default `XmlPromptRenderer` produces XML-structured output — XML tags mark section boundaries, and any frontier model reads them:
```

In `src/agentic_data_contracts/core/contract.py`, the `to_system_prompt` docstring arg line becomes:

```python
            renderer: Optional custom prompt renderer. Defaults to XmlPromptRenderer.
```

- [ ] **Step 5: Re-sort `__all__` and the import**

`sed` leaves `XmlPromptRenderer` in the old alphabetical slot. In `src/agentic_data_contracts/__init__.py`, move it from the first entry of `__all__` to directly after `"SqlNormalizer"`, and make the import read `from agentic_data_contracts.core.prompt import PromptRenderer, XmlPromptRenderer`.

- [ ] **Step 6: Verify no occurrence survives outside CHANGELOG**

Run: `rg -n 'ClaudePromptRenderer' --glob '!CHANGELOG.md' --glob '!docs/superpowers/**'`
Expected: no output, exit code 1. Both globs are excluded for the reasons in Step 3 — a hit inside them is correct, not a miss.

- [ ] **Step 7: Run the full suite and the linters**

Run: `uv run pytest -v && prek run --all-files`
Expected: all tests PASS, all hooks pass. `prek` may reformat the import ordering — that is fine, keep its result.

- [ ] **Step 8: Bump the version and write the CHANGELOG entry**

In `pyproject.toml`, set `version = "0.39.0"`. Prepend to `CHANGELOG.md` under the title block:

```markdown
## [0.39.0] - 2026-08-10

### Changed

- **`ClaudePromptRenderer` is renamed to `XmlPromptRenderer`.** The class contains no Claude- or Anthropic-specific logic: the only matches for `Claude|anthropic` in `core/prompt.py` were the class name and its docstring. It emits XML tags — an Anthropic-originated prompting convention, but the artifact is plain XML that any frontier model reads.

  The name had aged against the codebase. `tools/langchain.py` and `tools/pydantic_ai.py` both ship, so a Pydantic AI user's system prompt was being rendered by a class named after a runtime they had deliberately left.

  A protocol implementation should be named for the axis on which implementations differ. `PromptRenderer` exists so alternatives can exist, and a second implementation would differ by output *format* — Markdown sections instead of XML tags — not by vendor. Naming by vendor invites a `GptPromptRenderer` that emits the same tags and differs in nothing.

  Renamed with no deprecated alias, consistent with v0.38.0 removing `usage_limits_from_contract` outright. This is a `0.x` project at Beta status, where SemVer permits a breaking change in a minor bump.

  **Migration** — one line:

  ```diff
  - from agentic_data_contracts import ClaudePromptRenderer
  + from agentic_data_contracts import XmlPromptRenderer
  ```
```

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat!: rename ClaudePromptRenderer to XmlPromptRenderer (v0.39.0)"
```

---

## PR 2 — v0.40.0: honest degradation

### Task 2: Make both detail thresholds constructor parameters

**Files:**
- Modify: `src/agentic_data_contracts/core/prompt.py`
- Test: `tests/test_core/test_prompt_renderers.py`

**Interfaces:**
- Consumes: `XmlPromptRenderer` from Task 1.
- Produces: `XmlPromptRenderer.__init__(*, metric_detail_threshold: int | None | _Unset = _UNSET, relationship_detail_threshold: int | None | _Unset = _UNSET)`, storing `self.metric_detail_threshold: int | None` and `self.relationship_detail_threshold: int | None`. Module-level `_Unset` enum and `_UNSET` member. Task 8 adds `extra_sections` to this same `__init__`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_core/test_prompt_renderers.py`. `FakeSemanticSource` already exists at the top of that file; these tests build relationships directly instead.

```python
def _rel_source(count: int):  # noqa: ANN202
    """A source carrying `count` distinct relationships and nothing else."""

    class _Source(FakeSemanticSource):
        def __init__(self) -> None:
            super().__init__(0)
            self._rels = [
                Relationship(
                    from_=f"analytics.t{i}.id",
                    to=f"analytics.t{i + 1}.t{i}_id",
                    description=f"join {i}",
                )
                for i in range(count)
            ]

        def get_relationships(self) -> list[Relationship]:
            return list(self._rels)

    return _Source()


def test_relationship_threshold_none_never_degrades() -> None:
    contract = _make_minimal_contract()
    prompt = contract.to_system_prompt(
        _rel_source(52),
        renderer=XmlPromptRenderer(relationship_detail_threshold=None),
    )
    assert "<from>analytics.t0.id</from>" in prompt
    assert "join_count=" not in prompt


def test_relationship_threshold_int_degrades() -> None:
    contract = _make_minimal_contract()
    prompt = contract.to_system_prompt(
        _rel_source(52),
        renderer=XmlPromptRenderer(relationship_detail_threshold=30),
    )
    assert "<from>analytics.t0.id</from>" not in prompt
    assert "join_count=" in prompt


def test_omitted_threshold_falls_back_to_class_attribute() -> None:
    contract = _make_minimal_contract()
    prompt = contract.to_system_prompt(_rel_source(52), renderer=XmlPromptRenderer())
    assert "join_count=" in prompt  # class default of 30 still applies


def test_subclass_class_attribute_still_wins_over_default() -> None:
    class Loose(XmlPromptRenderer):
        RELATIONSHIP_DETAIL_THRESHOLD = 100

    contract = _make_minimal_contract()
    prompt = contract.to_system_prompt(_rel_source(52), renderer=Loose())
    assert "<from>analytics.t0.id</from>" in prompt


def test_metric_threshold_none_never_degrades() -> None:
    contract = _make_minimal_contract()
    prompt = contract.to_system_prompt(
        FakeSemanticSource(30),
        renderer=XmlPromptRenderer(metric_detail_threshold=None),
    )
    assert '<metric name="metric_0">' in prompt
    assert "metrics available" not in prompt
```

`_make_minimal_contract()` and `FakeSemanticSource` already exist near the top of
this file — reuse them, do not add a second contract helper. `FakeSemanticSource`
already defines `get_relationships` returning `[]`, which is what `_rel_source`
overrides.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_prompt_renderers.py -k threshold -v`
Expected: FAIL — `TypeError: XmlPromptRenderer() takes no arguments`.

- [ ] **Step 3: Add the sentinel and the constructor**

At module level in `src/agentic_data_contracts/core/prompt.py`, after the imports:

```python
import enum
import logging

logger = logging.getLogger(__name__)


class _Unset(enum.Enum):
    """Sentinel distinguishing "argument omitted" from an explicit ``None``.

    An enum rather than ``object()``: ty narrows enum members inside a union, so
    ``int | None | _Unset`` resolves to ``int | None`` after an ``is _UNSET``
    check without needing a cast at the use site.
    """

    TOKEN = 0


_UNSET = _Unset.TOKEN
```

Inside `XmlPromptRenderer`, keep both class attributes and add the constructor:

```python
    # Defaults for the constructor parameters below. Kept as class attributes so
    # an existing subclass that overrides them keeps working; an explicit
    # constructor argument takes precedence over the class attribute.
    METRIC_DETAIL_THRESHOLD = 20
    RELATIONSHIP_DETAIL_THRESHOLD = 30

    def __init__(
        self,
        *,
        metric_detail_threshold: int | None | _Unset = _UNSET,
        relationship_detail_threshold: int | None | _Unset = _UNSET,
    ) -> None:
        """``None`` for either threshold means "never degrade".

        These were class constants until v0.40.0, which made the only opt-out
        subclassing — so the consumer who knows their own prompt budget could not
        express it. The budget call belongs where the budget knowledge is.
        """
        self.metric_detail_threshold: int | None = (
            self.METRIC_DETAIL_THRESHOLD
            if metric_detail_threshold is _UNSET
            else metric_detail_threshold
        )
        self.relationship_detail_threshold: int | None = (
            self.RELATIONSHIP_DETAIL_THRESHOLD
            if relationship_detail_threshold is _UNSET
            else relationship_detail_threshold
        )
```

- [ ] **Step 4: Switch both render methods onto the instance attributes**

In `_render_metrics`, replace `compact = len(metrics) > self.METRIC_DETAIL_THRESHOLD` with:

```python
        threshold = self.metric_detail_threshold
        compact = threshold is not None and len(metrics) > threshold
```

In `_render_relationships`, replace `if len(rels) > self.RELATIONSHIP_DETAIL_THRESHOLD:` with:

```python
        threshold = self.relationship_detail_threshold
        if threshold is not None and len(rels) > threshold:
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/test_prompt_renderers.py -v && uv run pytest -v`
Expected: PASS, including the pre-existing `tests/test_core/test_scalability.py` assertions on `"30 metrics available"` and `"21 metrics available"`.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/core/prompt.py tests/test_core/test_prompt_renderers.py
git commit -m "feat: detail thresholds are constructor parameters, None disables degradation"
```

### Task 3: Log and disclose when the renderer degrades

**Files:**
- Modify: `src/agentic_data_contracts/core/prompt.py`
- Modify: `pyproject.toml`, `CHANGELOG.md`
- Test: `tests/test_core/test_prompt_renderers.py`

**Interfaces:**
- Consumes: `self.metric_detail_threshold` / `self.relationship_detail_threshold` from Task 2, and the module `logger`.
- Produces: no new API. The degraded `<hint>` and `<count>` strings gain a clause naming the threshold; both are substring-compatible with existing assertions.

- [ ] **Step 1: Write the failing tests**

```python
import logging

_PROMPT_LOGGER = "agentic_data_contracts.core.prompt"


def _prompt_logs(caplog) -> list[str]:  # noqa: ANN001
    """Fully-formatted messages from the prompt logger only.

    ``caplog.records`` collects records from *every* logger that propagates, so
    filtering by name is what keeps these assertions from breaking when an
    unrelated library logs during the same call.
    """
    return [r.message % r.args for r in caplog.records if r.name == _PROMPT_LOGGER]


def test_relationship_degradation_logs(caplog) -> None:  # noqa: ANN001
    contract = _make_minimal_contract()
    with caplog.at_level(logging.INFO, logger=_PROMPT_LOGGER):
        contract.to_system_prompt(
            _rel_source(52),
            renderer=XmlPromptRenderer(relationship_detail_threshold=30),
        )
    assert any(
        "52 relationships exceeds relationship_detail_threshold=30" in m
        for m in _prompt_logs(caplog)
    )


def test_relationship_degradation_hint_names_what_it_dropped() -> None:
    contract = _make_minimal_contract()
    prompt = contract.to_system_prompt(
        _rel_source(52),
        renderer=XmlPromptRenderer(relationship_detail_threshold=30),
    )
    assert "join keys omitted above threshold 30" in prompt


def test_metric_degradation_logs(caplog) -> None:  # noqa: ANN001
    contract = _make_minimal_contract()
    with caplog.at_level(logging.INFO, logger=_PROMPT_LOGGER):
        contract.to_system_prompt(
            FakeSemanticSource(30),
            renderer=XmlPromptRenderer(metric_detail_threshold=20),
        )
    assert any(
        "30 metrics exceeds metric_detail_threshold=20" in m
        for m in _prompt_logs(caplog)
    )


def test_no_log_when_not_degrading(caplog) -> None:  # noqa: ANN001
    contract = _make_minimal_contract()
    with caplog.at_level(logging.INFO, logger=_PROMPT_LOGGER):
        contract.to_system_prompt(_rel_source(5), renderer=XmlPromptRenderer())
    assert _prompt_logs(caplog) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_prompt_renderers.py -k "degradation or no_log" -v`
Expected: FAIL — the assertions find no matching log record and no threshold clause in the prompt.

- [ ] **Step 3: Add the log line and honest hint to `_render_relationships`**

Inside the degrading branch, immediately after the `if threshold is not None and len(rels) > threshold:` line:

```python
            logger.info(
                "XmlPromptRenderer: %d relationships exceeds"
                " relationship_detail_threshold=%d — per-relationship join keys"
                " omitted from the prompt; agents must call"
                " lookup_relationships to get them. Pass"
                " relationship_detail_threshold=None to render them inline.",
                len(rels),
                threshold,
            )
```

and replace the existing hint with one that discloses the omission:

```python
            lines.append(
                f"  <hint>{len(rels)} relationships defined; per-relationship"
                f" join keys omitted above threshold {threshold}."
                ' Use lookup_relationships(table="schema.table")'
                " to get join details and required filters.</hint>"
            )
```

- [ ] **Step 4: Add the symmetric treatment to `_render_metrics`**

`METRIC_DETAIL_THRESHOLD` degrades identically and just as silently; fixing only relationships would ship a corrected threshold beside an uncorrected one. Inside the `if compact:` branch:

```python
            logger.info(
                "XmlPromptRenderer: %d metrics exceeds"
                " metric_detail_threshold=%d — per-metric names and descriptions"
                " omitted from the prompt; agents must call list_metrics to"
                " browse. Pass metric_detail_threshold=None to render them"
                " inline.",
                len(metrics),
                threshold,
            )
            lines.append(
                f"  <count>{len(metrics)} metrics available;"
                f" per-metric descriptions omitted above threshold"
                f" {threshold}.</count>"
            )
```

This replaces the existing `<count>` line. The leading `"{n} metrics available"` text is preserved verbatim so the two `tests/test_core/test_scalability.py` assertions keep passing.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest -v`
Expected: PASS, all tests including `test_scalability.py`.

- [ ] **Step 6: Bump the version and write the CHANGELOG entry**

Set `version = "0.40.0"` in `pyproject.toml`. Prepend to `CHANGELOG.md`:

```markdown
## [0.40.0] - 2026-08-10

### Added

- **`XmlPromptRenderer(metric_detail_threshold=…, relationship_detail_threshold=…)`.** Both were class constants, so the only way to opt out of prompt degradation was to subclass the renderer — meaning the consumer who actually knows their prompt budget could not express it. `None` disables degradation entirely. Omitting an argument still reads the class attribute, so existing subclass overrides are unaffected.

### Changed

- **Both degrading branches now log at INFO and disclose what they dropped.** Above `relationship_detail_threshold`, `_render_relationships` drops every per-relationship `<from>`/`<to>` — the actual join keys — and substitutes per-table `join_count` summaries. On a 52-relationship contract that suppresses roughly 3.1× the whole fragment (9,658 rendered chars versus 29,852 detailed). Gating large content is reasonable; gating it silently means the only way to discover it is to render the prompt and diff it against expectations, which nobody thinks to do because the block looks populated.

  The rendered hint now names the omission and the threshold, and a log line names the count, the threshold, and the parameter that turns it off. `_render_metrics` gets the same treatment against `metric_detail_threshold`, which had the identical shape and the same gap.

  `logger.info`, not `warning`: degradation is intended behaviour under a configured budget, unlike an ignored contract key.

  Reported in #60.
```

- [ ] **Step 7: Run linters and commit**

```bash
prek run --all-files
git add -A
git commit -m "feat: log and disclose prompt-renderer degradation (v0.40.0)"
```

---

## PR 3 — v0.41.0: semantic extras

### Task 4: Capture unknown top-level keys as extras

**Files:**
- Modify: `src/agentic_data_contracts/semantic/yaml_source.py`
- Test: `tests/test_semantic/test_yaml_source.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `SEMANTIC_KEYS: frozenset[str]` at module level in `semantic/yaml_source.py`, and `YamlSource.get_extras() -> dict[str, Any]`. Tasks 5, 6, 7, and 8 all build on these two names.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_semantic/test_yaml_source.py`:

```python
from typing import Any

from agentic_data_contracts.semantic.yaml_source import SEMANTIC_KEYS, YamlSource


def test_unknown_top_level_keys_are_kept_as_extras() -> None:
    """Issue #60's reproduction: an unsupported section must not vanish."""
    src = YamlSource.from_raw(
        {
            "metrics": [],
            "widget_hints": [{"table": "t", "prefer": "a", "over": "b"}],
        }
    )
    assert src.get_extras() == {
        "widget_hints": [{"table": "t", "prefer": "a", "over": "b"}]
    }


def test_interpreted_keys_never_appear_in_extras() -> None:
    src = YamlSource.from_raw(
        {
            "metrics": [],
            "tables": [],
            "relationships": [],
            "metric_impacts": [],
            "column_hints": [{"table": "t", "prefer": "a"}],
        }
    )
    assert set(src.get_extras()) == {"column_hints"}


def test_no_extras_is_an_empty_dict() -> None:
    assert YamlSource.from_raw({"metrics": []}).get_extras() == {}


def test_semantic_keys_matches_what_the_parser_reads() -> None:
    """Guards the constant against drift when a new section is added."""
    assert SEMANTIC_KEYS == {"metrics", "tables", "relationships", "metric_impacts"}


def test_get_extras_returns_a_copy() -> None:
    src = YamlSource.from_raw({"metrics": [], "notes": ["a"]})
    src.get_extras()["notes"] = ["mutated"]
    assert src.get_extras() == {"notes": ["a"]}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_yaml_source.py -k extras -v`
Expected: FAIL — `ImportError: cannot import name 'SEMANTIC_KEYS'`.

- [ ] **Step 3: Add the constant and the capture**

At module level in `src/agentic_data_contracts/semantic/yaml_source.py`, after the imports:

```python
#: Top-level keys ``_load_from_raw`` interprets. Everything else in a semantic
#: YAML is carried verbatim as "extras" — see ``YamlSource.get_extras``. Exported
#: so a consumer can assert against it instead of hardcoding a list that drifts on
#: the next release.
SEMANTIC_KEYS = frozenset({"metrics", "tables", "relationships", "metric_impacts"})
```

At the top of `_load_from_raw`, before the `self._metrics = []` line:

```python
        self._extras: dict[str, Any] = {
            k: v for k, v in raw.items() if k not in SEMANTIC_KEYS
        }
```

And add the accessor at the end of the class:

```python
    def get_extras(self) -> dict[str, Any]:
        """Top-level keys this parser does not interpret, carried verbatim.

        The framework never interprets, validates, indexes, or computes over this
        content — it only carries it and, on request, places it in the prompt.
        Anything needing interpretation is a candidate for real vocabulary, not
        for extras.

        Shallow copy, consistent with ``get_metrics`` and ``get_relationships``.
        """
        return dict(self._extras)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_yaml_source.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/agentic_data_contracts/semantic/yaml_source.py tests/test_semantic/test_yaml_source.py
git commit -m "feat: YamlSource carries unrecognised top-level keys as extras"
```

### Task 5: `expected_extras` — warn by default, raise on request

**Files:**
- Modify: `src/agentic_data_contracts/semantic/yaml_source.py`
- Test: `tests/test_semantic/test_yaml_source.py`

**Interfaces:**
- Consumes: `SEMANTIC_KEYS`, `self._extras` from Task 4.
- Produces: keyword-only `expected_extras: Collection[str] | None = None` on `YamlSource.__init__`, `YamlSource.from_raw`, and `YamlSource._load_from_raw`; module-level `_apply_extras_policy`.

- [ ] **Step 1: Write the failing tests**

```python
import logging

import pytest


def test_extras_warn_by_default(caplog) -> None:  # noqa: ANN001
    logger_name = "agentic_data_contracts.semantic.yaml_source"
    with caplog.at_level(logging.WARNING, logger=logger_name):
        YamlSource.from_raw({"metrics": [], "widget_hints": []})
    joined = " ".join(r.message % r.args for r in caplog.records)
    assert "widget_hints" in joined
    assert "get_extras()" in joined


def test_no_warning_when_there_are_no_extras(caplog) -> None:  # noqa: ANN001
    logger_name = "agentic_data_contracts.semantic.yaml_source"
    with caplog.at_level(logging.WARNING, logger=logger_name):
        YamlSource.from_raw({"metrics": []})
    assert caplog.records == []


def test_expected_extras_silences_declared_sections(caplog) -> None:  # noqa: ANN001
    logger_name = "agentic_data_contracts.semantic.yaml_source"
    with caplog.at_level(logging.WARNING, logger=logger_name):
        src = YamlSource.from_raw(
            {"metrics": [], "column_hints": []},
            expected_extras={"column_hints"},
        )
    assert caplog.records == []
    assert set(src.get_extras()) == {"column_hints"}


def test_expected_extras_raises_on_an_undeclared_key() -> None:
    with pytest.raises(ValueError, match="widget_hints"):
        YamlSource.from_raw(
            {"metrics": [], "widget_hints": []},
            expected_extras={"column_hints"},
        )


def test_empty_expected_extras_is_strict_mode() -> None:
    with pytest.raises(ValueError, match="anything"):
        YamlSource.from_raw({"metrics": [], "anything": 1}, expected_extras=frozenset())


def test_typo_in_a_supported_key_is_caught_by_expected_extras() -> None:
    """`relationship:` for `relationships:` silently deleted a section before."""
    with pytest.raises(ValueError, match="relationship"):
        YamlSource.from_raw(
            {"metrics": [], "relationship": [{"from": "a.b.c", "to": "d.e.f"}]},
            expected_extras=frozenset(),
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_yaml_source.py -k "expected_extras or warn or typo" -v`
Expected: FAIL — `TypeError: from_raw() got an unexpected keyword argument 'expected_extras'` for the ones passing it, and no log record for the warn test.

- [ ] **Step 3: Add the logger and the policy function**

At the top of `src/agentic_data_contracts/semantic/yaml_source.py`:

```python
import logging
from collections.abc import Collection

logger = logging.getLogger(__name__)
```

At module level, after `SEMANTIC_KEYS`:

```python
def _apply_extras_policy(
    extras: dict[str, Any],
    expected_extras: Collection[str] | None,
) -> None:
    """Warn about, or reject, top-level keys the parser does not interpret.

    A deliberate custom section and a typo (``relationship:`` for
    ``relationships:``) are indistinguishable at this layer, so the default
    warning must serve both: it names the keys, the interpreted set, and the
    remedy. Passing *expected_extras* turns the typo case into a load-time error
    while staying silent for sections the consumer declared — which a bare
    ``strict=True`` flag could not do, since it would fire forever on a
    consumer's own deliberate extras.

    Deliberately not memoised, for the reason recorded on ``_warn_unenforceable``
    in ``validation/validator.py``: caching a logging side effect means a
    consumer that calls ``logging.basicConfig()`` after building its contracts
    loses the diagnostic permanently.
    """
    if not extras:
        return
    if expected_extras is None:
        logger.warning(
            "YamlSource: top-level keys not interpreted as semantic vocabulary:"
            " %s (interpreted keys: %s). They are carried and reachable via"
            " get_extras(), but reach a prompt only if named in"
            " XmlPromptRenderer(extra_sections=...). If one of these is a typo,"
            " that section is not being read at all.",
            sorted(extras),
            sorted(SEMANTIC_KEYS),
        )
        return
    unexpected = sorted(set(extras) - set(expected_extras))
    if unexpected:
        raise ValueError(
            f"YamlSource: unexpected top-level keys {unexpected}; declared"
            f" expected_extras={sorted(expected_extras)}, interpreted keys="
            f"{sorted(SEMANTIC_KEYS)}"
        )
```

- [ ] **Step 4: Thread the parameter through all three entry points**

```python
    def __init__(
        self,
        path: str | Path,
        *,
        expected_extras: Collection[str] | None = None,
    ) -> None:
        raw = yaml.safe_load(Path(path).read_text())
        self._load_from_raw(
            raw if raw is not None else {}, expected_extras=expected_extras
        )

    @classmethod
    def from_raw(
        cls,
        raw: dict[str, Any],
        *,
        expected_extras: Collection[str] | None = None,
    ) -> YamlSource:
        """Build a source from already-parsed semantic data — no file access.

        The inverse of :func:`dump_semantic_source`; lets a frozen contract carry
        its semantics inline and rebuild them on a consumer with no filesystem.
        """
        obj = cls.__new__(cls)
        obj._load_from_raw(raw, expected_extras=expected_extras)
        return obj

    def _load_from_raw(
        self,
        raw: dict[str, Any],
        *,
        expected_extras: Collection[str] | None = None,
    ) -> None:
```

and change the extras block added in Task 4 to run the policy first:

```python
        extras = {k: v for k, v in raw.items() if k not in SEMANTIC_KEYS}
        _apply_extras_policy(extras, expected_extras)
        self._extras: dict[str, Any] = extras
```

Key policy runs before the value normalization added in Task 6, so a mistyped key is reported as a mistyped key rather than as a serialization complaint about its contents.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_yaml_source.py -v && uv run pytest -v`
Expected: PASS. Watch for pre-existing tests that build a `YamlSource` from a fixture carrying stray keys — if any now warn, that is the feature working; only fix a test if it asserts on empty logs.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/semantic/yaml_source.py tests/test_semantic/test_yaml_source.py
git commit -m "feat: expected_extras — warn on uninterpreted keys, raise on undeclared ones"
```

### Task 6: Normalize extras to JSON-safe values at load

**Files:**
- Modify: `src/agentic_data_contracts/semantic/yaml_source.py`
- Test: `tests/test_semantic/test_yaml_source.py`

**Interfaces:**
- Consumes: the extras block from Tasks 4 and 5.
- Produces: module-level `_normalize_extras(extras: dict[str, Any]) -> dict[str, Any]`, `_jsonify(value, path)`, `_fmt_path(path)`. After this task `self._extras` is guaranteed `json.dumps`-safe, which Task 7 relies on.

- [ ] **Step 1: Write the failing tests**

```python
import json
from datetime import date, datetime


def test_yaml_native_date_in_extras_becomes_an_iso_string() -> None:
    src = YamlSource.from_raw(
        {
            "metrics": [],
            "column_hints": [{"table": "t", "verified": date(2026, 3, 14)}],
        }
    )
    assert src.get_extras()["column_hints"][0]["verified"] == "2026-03-14"


def test_datetime_in_extras_is_normalised_to_a_date_string() -> None:
    src = YamlSource.from_raw(
        {"metrics": [], "notes": {"at": datetime(2026, 3, 14, 12, 0, 0)}}
    )
    assert src.get_extras()["notes"]["at"] == "2026-03-14"


def test_normalised_extras_are_json_serialisable() -> None:
    src = YamlSource.from_raw(
        {"metrics": [], "column_hints": [{"verified": date(2026, 3, 14)}]}
    )
    assert json.dumps(src.get_extras())  # must not raise


def test_unserialisable_extras_value_raises_naming_the_path() -> None:
    with pytest.raises(ValueError, match=r"column_hints\[0\]\.owner"):
        YamlSource.from_raw(
            {"metrics": [], "column_hints": [{"owner": {1, 2, 3}}]}
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_yaml_source.py -k "json or date_in_extras or datetime_in_extras or unserialisable" -v`
Expected: FAIL — the date stays a `datetime.date` object and the set does not raise.

- [ ] **Step 3: Add the normalizer**

At module level in `src/agentic_data_contracts/semantic/yaml_source.py`:

```python
def _normalize_extras(extras: dict[str, Any]) -> dict[str, Any]:
    """Return *extras* with dates ISO-coerced and JSON-safety enforced.

    Extras ride inside ``SemanticSource.inline`` and therefore through
    ``contract_canonical_bytes``' ``json.dumps``. A YAML-native date is not JSON,
    and the guidance extras exist to carry explicitly wants one ("verified
    against the database on ..."). Checking at load means a bad value fails where
    it was authored, rather than months later inside an ARD publish.
    """
    return {k: _jsonify(v, (k,)) for k, v in extras.items()}


def _jsonify(value: Any, path: tuple[str | int, ...]) -> Any:
    """Recursively coerce *value* to JSON-safe types, or raise naming *path*.

    ``datetime`` is checked before ``date`` because it subclasses ``date`` — the
    same ordering trap ``_parse_date`` documents.
    """
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonify(v, (*path, str(k))) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonify(v, (*path, i)) for i, v in enumerate(value)]
    if value is None or isinstance(value, str | int | float):
        return value
    raise ValueError(
        f"YamlSource: extras value at {_fmt_path(path)} is not JSON-serializable"
        f" ({type(value).__name__}). Extras are carried into the frozen contract"
        f" and its digest, so they must be JSON-safe."
    )


def _fmt_path(path: tuple[str | int, ...]) -> str:
    """Render a key path as ``section[0].field`` for an actionable error."""
    rendered = str(path[0])
    for part in path[1:]:
        rendered += f"[{part}]" if isinstance(part, int) else f".{part}"
    return rendered
```

- [ ] **Step 4: Wire it into `_load_from_raw`**

```python
        extras = {k: v for k, v in raw.items() if k not in SEMANTIC_KEYS}
        _apply_extras_policy(extras, expected_extras)
        self._extras: dict[str, Any] = _normalize_extras(extras)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_semantic/test_yaml_source.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/semantic/yaml_source.py tests/test_semantic/test_yaml_source.py
git commit -m "feat: normalise extras to JSON-safe values at load, not at freeze"
```

### Task 7: `ExtensibleSemanticSource` and extras round-trip through freeze

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py`
- Modify: `src/agentic_data_contracts/__init__.py`
- Create: `tests/test_semantic/test_extras_roundtrip.py`
- Test: `tests/test_public_api.py`

**Interfaces:**
- Consumes: `YamlSource.get_extras` (Task 4), JSON-safe guarantee (Task 6).
- Produces: `ExtensibleSemanticSource` protocol and module-level `_iso` in `semantic/base.py`; `dump_semantic_source` now emits extras keys. Task 8 imports `ExtensibleSemanticSource`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_semantic/test_extras_roundtrip.py`:

```python
"""Extras survive freeze, rebuild identically, and move the contract digest."""

from __future__ import annotations

from agentic_data_contracts.semantic.base import (
    ExtensibleSemanticSource,
    dump_semantic_source,
)
from agentic_data_contracts.semantic.cube import CubeSource
from agentic_data_contracts.semantic.dbt import DbtSource
from agentic_data_contracts.semantic.yaml_source import YamlSource

_HINTS = [{"table": "analytics.orders", "prefer": "order_total", "over": "total"}]


def test_dump_carries_extras() -> None:
    src = YamlSource.from_raw({"metrics": [], "column_hints": _HINTS})
    assert dump_semantic_source(src)["column_hints"] == _HINTS


def test_dump_omits_extras_entirely_when_there_are_none() -> None:
    """Load-bearing: every existing frozen contract's digest must not move."""
    src = YamlSource.from_raw({"metrics": []})
    dumped = dump_semantic_source(src)
    assert set(dumped) == {"tables", "metrics", "relationships", "metric_impacts"}


def test_extras_round_trip_through_dump_and_rebuild() -> None:
    src = YamlSource.from_raw({"metrics": [], "column_hints": _HINTS})
    rebuilt = YamlSource.from_raw(dump_semantic_source(src))
    assert rebuilt.get_extras() == src.get_extras()


def test_yaml_source_is_extensible() -> None:
    assert isinstance(YamlSource.from_raw({"metrics": []}), ExtensibleSemanticSource)


def test_dbt_and_cube_are_not_extensible() -> None:
    """A sibling protocol, so sources without extras stay valid SemanticSources.

    ``__new__`` without ``__init__`` is enough: a runtime_checkable protocol
    checks method presence on the class, not instance state.
    """
    assert not isinstance(DbtSource.__new__(DbtSource), ExtensibleSemanticSource)
    assert not isinstance(CubeSource.__new__(CubeSource), ExtensibleSemanticSource)
```

Add to `tests/test_ard/test_catalog_entry.py`. Note the hints literal is spelled
out here rather than imported — `_HINTS` lives in `test_extras_roundtrip.py`, and
importing test-module private state across files is how fixtures rot:

```python
def test_extras_change_the_contract_digest() -> None:
    """Extras are resident prompt text, so they belong to the contract identity."""
    from agentic_data_contracts.ard import contract_digest

    hints = [{"table": "analytics.orders", "prefer": "order_total", "over": "total"}]
    base = _contract_with_inline_semantics({"metrics": []})
    hinted = _contract_with_inline_semantics({"metrics": [], "column_hints": hints})
    assert contract_digest(base) != contract_digest(hinted)
```

Before writing this test, read `tests/test_ard/test_catalog_entry.py` and build
`_contract_with_inline_semantics(inline)` from whatever contract-construction
style that file already uses — it must produce a `DataContract` whose
`schema.semantic.source` has `type="yaml"` and `inline=` the passed dict. Do not
introduce a second construction style in that file, and do not add a `tmp_path`
parameter: an inline contract needs no filesystem, which is the point of `inline`.

And extend `tests/test_public_api.py::test_semantic_imports`:

```python
def test_semantic_imports() -> None:
    from agentic_data_contracts.semantic.base import (  # noqa: F401, I001
        ExtensibleSemanticSource,
        MetricDefinition,
        SemanticSource,
    )
    from agentic_data_contracts.semantic.yaml_source import SEMANTIC_KEYS, YamlSource

    assert SemanticSource is not None
    assert ExtensibleSemanticSource is not None
    assert YamlSource is not None
    assert SEMANTIC_KEYS
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_extras_roundtrip.py -v`
Expected: FAIL — `ImportError: cannot import name 'ExtensibleSemanticSource'`.

- [ ] **Step 3: Add the sibling protocol**

In `src/agentic_data_contracts/semantic/base.py`, directly after the `SemanticSource` protocol:

```python
@runtime_checkable
class ExtensibleSemanticSource(Protocol):
    """A source carrying consumer-authored sections the framework never reads.

    Deliberately a *sibling* of :class:`SemanticSource` rather than an extension
    of it. ``runtime_checkable`` isinstance checks method *presence*, so folding
    ``get_extras`` into ``SemanticSource`` would make every external custom
    source fail ``isinstance(src, SemanticSource)`` on upgrade. A sibling breaks
    nobody and stays fully typed.

    The framework carries extras and, on request, places them in the prompt. It
    does not interpret, validate, index, or compute over their content — the
    boundary the ``verified-examples`` corpus already draws in
    ``validation/examples.py``.
    """

    def get_extras(self) -> dict[str, Any]: ...
```

- [ ] **Step 4: Lift `_iso` to module level and carry extras in the dump**

`_iso` is currently nested inside `dump_semantic_source`. Move it to module level so the extras normalizer and the metric dumper share one definition:

```python
def _iso(d: date | None) -> str | None:
    """ISO-format a date, passing ``None`` through."""
    return d.isoformat() if d is not None else None
```

Delete the nested `def _iso` from `dump_semantic_source`, then bind its return value to a local and merge extras:

```python
    payload: dict[str, Any] = {
        "tables": tables,
        "metrics": [_dump_metric(m) for m in source.get_metrics()],
        "relationships": [
            # ... unchanged ...
        ],
        "metric_impacts": [
            # ... unchanged ...
        ],
    }
    # Extras are the complement of SEMANTIC_KEYS by construction, so they cannot
    # collide with the four keys above. Empty extras update nothing, which is why
    # a contract without them still dumps byte-identically and no published ARD
    # digest moves — the same omit-when-empty reasoning as decompositions.
    if isinstance(source, ExtensibleSemanticSource):
        payload.update(source.get_extras())
    return payload
```

- [ ] **Step 5: Export the new names**

In `src/agentic_data_contracts/__init__.py`, add `ExtensibleSemanticSource` to the `semantic.base` import and to `__all__` (alphabetically, between `"DataContract"` and `"MetricDefinition"`).

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest -v`
Expected: PASS, with `test_dump_omits_extras_entirely_when_there_are_none` confirming digest stability for contracts without extras.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: ExtensibleSemanticSource carries extras through freeze into the digest"
```

### Task 8: Render extras the consumer names

**Files:**
- Modify: `src/agentic_data_contracts/core/prompt.py`
- Test: `tests/test_core/test_prompt_renderers.py`

**Interfaces:**
- Consumes: `XmlPromptRenderer.__init__` (Task 2), `ExtensibleSemanticSource` (Task 7), `get_extras` (Task 4).
- Produces: `ExtraSectionRenderer = Callable[[str, Any], list[str]]`, the `extra_sections` constructor parameter, `XmlPromptRenderer._render_extras`, and module-level `_default_extra_section`.

- [ ] **Step 1: Write the failing tests**

```python
class _ExtrasSource(FakeSemanticSource):
    """A FakeSemanticSource that also satisfies ExtensibleSemanticSource."""

    def __init__(self, extras: dict[str, Any]) -> None:
        super().__init__(0)
        self._extras = extras

    def get_extras(self) -> dict[str, Any]:
        return dict(self._extras)


_EXTRAS = {"column_hints": [{"table": "analytics.orders", "prefer": "order_total"}]}


def test_extras_do_not_render_unless_named() -> None:
    contract = _make_minimal_contract()
    prompt = contract.to_system_prompt(
        _ExtrasSource(_EXTRAS), renderer=XmlPromptRenderer()
    )
    assert "column_hints" not in prompt


def test_named_extra_renders_with_the_default_formatter() -> None:
    contract = _make_minimal_contract()
    prompt = contract.to_system_prompt(
        _ExtrasSource(_EXTRAS),
        renderer=XmlPromptRenderer(extra_sections=["column_hints"]),
    )
    assert "<column_hints>" in prompt
    assert "</column_hints>" in prompt
    assert "prefer: order_total" in prompt
    assert prompt.index("<column_hints>") < prompt.index("</data_contract>")


def test_custom_callable_receives_name_and_payload_and_is_used_verbatim() -> None:
    seen: list[tuple[str, Any]] = []

    def render(name: str, payload: Any) -> list[str]:
        seen.append((name, payload))
        return [f"<{name}>CUSTOM</{name}>"]

    contract = _make_minimal_contract()
    prompt = contract.to_system_prompt(
        _ExtrasSource(_EXTRAS),
        renderer=XmlPromptRenderer(extra_sections={"column_hints": render}),
    )
    assert "<column_hints>CUSTOM</column_hints>" in prompt
    assert seen == [("column_hints", _EXTRAS["column_hints"])]


def test_named_sections_render_in_the_order_given() -> None:
    contract = _make_minimal_contract()
    src = _ExtrasSource({"b_section": ["b"], "a_section": ["a"]})
    prompt = contract.to_system_prompt(
        src, renderer=XmlPromptRenderer(extra_sections=["b_section", "a_section"])
    )
    assert prompt.index("<b_section>") < prompt.index("<a_section>")


def test_naming_an_absent_section_warns_and_renders_nothing(caplog) -> None:  # noqa: ANN001
    contract = _make_minimal_contract()
    with caplog.at_level(logging.WARNING, logger=_PROMPT_LOGGER):
        prompt = contract.to_system_prompt(
            _ExtrasSource(_EXTRAS),
            renderer=XmlPromptRenderer(extra_sections=["colum_hints"]),
        )
    assert "colum_hints" not in prompt
    assert any("colum_hints" in m for m in _prompt_logs(caplog))


def test_a_raising_custom_renderer_propagates() -> None:
    def boom(name: str, payload: Any) -> list[str]:
        raise RuntimeError("bad renderer")

    contract = _make_minimal_contract()
    with pytest.raises(RuntimeError, match="bad renderer"):
        contract.to_system_prompt(
            _ExtrasSource(_EXTRAS),
            renderer=XmlPromptRenderer(extra_sections={"column_hints": boom}),
        )


def test_non_extensible_source_renders_no_extras() -> None:
    contract = _make_minimal_contract()
    prompt = contract.to_system_prompt(
        FakeSemanticSource(3),
        renderer=XmlPromptRenderer(extra_sections=["column_hints"]),
    )
    assert "column_hints" not in prompt
```

Add `import pytest` and `from typing import Any` to the test module's imports if not already present.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/test_prompt_renderers.py -k extra -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'extra_sections'`.

- [ ] **Step 3: Add the type alias and the default formatter**

At module level in `src/agentic_data_contracts/core/prompt.py` (add `import yaml` and `from collections.abc import Callable, Mapping, Sequence` to the imports):

```python
#: Renders one extras section. Receives ``(section_name, payload)`` and returns
#: the **complete** section, its own enclosing tags included — the renderer
#: contributes placement and ordering only, never wrapping. Taking the name lets
#: one function serve several sections.
ExtraSectionRenderer = Callable[[str, Any], list[str]]


def _default_extra_section(name: str, payload: Any) -> list[str]:
    """Wrap an extras payload in an XML tag, body dumped back as YAML.

    The payload has arbitrary nesting and no schema, so a hand-rolled XML walker
    would be guessing at its shape. Dumping the author's own structure back is
    faithful, handles nesting for free, stays compact, and models read YAML. The
    XML tag keeps the section boundary consistent with the rest of the prompt.

    Authored content is not escaped, so prose containing ``</name>`` would break
    the enclosing tag. That matches the existing ``description="..."``
    attributes, which do not escape quotes either: contract content is trusted
    at one level throughout.
    """
    body = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True).rstrip("\n")
    return [f"<{name}>", body, f"</{name}>"]
```

- [ ] **Step 4: Extend the constructor**

Add the parameter to the `__init__` written in Task 2, before the two thresholds:

```python
    def __init__(
        self,
        *,
        extra_sections: Sequence[str] | Mapping[str, ExtraSectionRenderer | None] = (),
        metric_detail_threshold: int | None | _Unset = _UNSET,
        relationship_detail_threshold: int | None | _Unset = _UNSET,
    ) -> None:
```

and store it as the first statement of the body:

```python
        # Opt-in by name in both directions: nothing renders unless named, so a
        # section authored as internal bookkeeping never leaks into a prompt.
        # A mapping value of None selects the default formatter.
        self.extra_sections: dict[str, ExtraSectionRenderer | None] = (
            dict(extra_sections)
            if isinstance(extra_sections, Mapping)
            else dict.fromkeys(extra_sections)
        )
```

- [ ] **Step 5: Add `_render_extras` and call it from `render`**

```python
    def _render_extras(self, semantic_source: SemanticSource | None) -> list[str]:
        """Render consumer-authored sections the caller explicitly named.

        Naming a section the source does not carry warns rather than rendering
        nothing quietly — it is the same typo class as a top-level key the parser
        ignores, and gets the same treatment.

        A consumer renderer that raises is allowed to propagate. Naming a section
        you cannot render is a bug, and swallowing it would silently drop content
        — the exact failure this whole feature removes.
        """
        if not self.extra_sections or semantic_source is None:
            return []
        # Imported locally to keep core decoupled from semantic at module scope,
        # matching _render_domains.
        from agentic_data_contracts.semantic.base import ExtensibleSemanticSource

        if not isinstance(semantic_source, ExtensibleSemanticSource):
            return []
        extras = semantic_source.get_extras()
        lines: list[str] = []
        for name, render in self.extra_sections.items():
            if name not in extras:
                logger.warning(
                    "XmlPromptRenderer: extra_sections names %r, which the"
                    " semantic source does not carry (available: %s) — nothing"
                    " rendered for it.",
                    name,
                    sorted(extras),
                )
                continue
            renderer = _default_extra_section if render is None else render
            lines.extend(renderer(name, extras[name]))
        return lines
```

In `render`, between the relationships block and the resource-limits block:

```python
        # 3b. Consumer-authored extras (opt-in by name, in the order given)
        lines.extend(self._render_extras(semantic_source))
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/agentic_data_contracts/core/prompt.py tests/test_core/test_prompt_renderers.py
git commit -m "feat: XmlPromptRenderer renders extras sections named by the consumer"
```

### Task 9: Documentation, version bump, and release notes

**Files:**
- Modify: `README.md`, `docs/architecture.md`, `pyproject.toml`, `CHANGELOG.md`

**Interfaces:**
- Consumes: everything from Tasks 4–8.
- Produces: no code. This is the task that makes the feature discoverable, which is the whole point of the issue.

- [ ] **Step 1: Add a README section**

Place it directly after the existing `PromptRenderer` section. Use `column_hints` and `join_paths` as the worked example — the two sections that motivated issue #60 become the illustration rather than framework vocabulary:

````markdown
### Consumer-authored sections (extras)

`YamlSource` interprets four top-level keys — `metrics`, `tables`,
`relationships`, `metric_impacts` (exported as `SEMANTIC_KEYS`). Every other
top-level key is carried verbatim as an *extra*: reachable, portable, and
renderable, but never interpreted.

```yaml
# semantic.yml
metrics: []

column_hints:
  - table: analytics.orders
    prefer: order_total
    over: total            # does not exist on this view
    reason: Verified against the warehouse on 2026-03-14.

join_paths:
  - name: leaf_to_root
    description: Traverse the product hierarchy from leaf to root.
    path:
      - {from: dim_leaf, to: dim_mid, on: mid_id}
      - {from: dim_mid, to: dim_root, on: root_id}
```

```python
source = YamlSource(
    "semantic.yml",
    expected_extras={"column_hints", "join_paths"},   # anything else raises
)
prompt = contract.to_system_prompt(
    source,
    renderer=XmlPromptRenderer(extra_sections=["column_hints", "join_paths"]),
)
```

Without `expected_extras`, uninterpreted keys are logged at WARNING and carried
anyway, so no existing contract stops loading. Pass a collection to turn a typo —
`relationship:` for `relationships:` — into a load-time error instead of a
silently deleted section.

Nothing renders unless you name it in `extra_sections`, so a section kept for
internal bookkeeping never reaches a prompt. Naming a section the source does not
carry warns rather than failing quietly. A mapping value replaces the default
YAML-in-a-tag formatter with your own:

```python
XmlPromptRenderer(extra_sections={"join_paths": my_renderer})
```

**Two things to know before you rely on this.**

Extras are part of the contract's identity. They travel through
`freeze_semantic_source()` into `contract_canonical_bytes`, so editing a hint's
prose changes `contract_digest` and invalidates any ARD attestation pinned to it.
That is deliberate — extras are resident prompt text, so they shape agent
behaviour, and a digest that ignored them would verify something different from
what the agent actually saw. It does mean hint prose lives under the same change
discipline as `forbidden_operations`.

Extras are never interpreted. There is no schema validation and no staleness
detection — the `last_reviewed` machinery that flags stale metrics does not apply
here, because applying it would mean reading the content. If your hints need
shape or freshness guarantees, that lint belongs on your side.
````

- [ ] **Step 2: Record both decisions in `docs/architecture.md`**

Add two rows to the Design Decisions table:

```markdown
| Consumer-authored semantics | Carried, never interpreted | Every first-class section is *computed on* — relationships are indexed and BFS-walked, decompositions are cycle-checked, drill_by is validated against schemas. A section that is only interpolated into a string is carriage, and carriage gets a generic mechanism (`get_extras`, `extra_sections`) rather than bespoke vocabulary. Generalises the boundary `validation/examples.py` already draws for verified examples |
| Prompt renderer naming | Named by output format | `XmlPromptRenderer`, not `ClaudePromptRenderer`: the class holds no Claude-specific logic, and `PromptRenderer` implementations differ by format (XML vs Markdown), not by vendor |
```

- [ ] **Step 3: Bump the version and write the CHANGELOG entry**

Set `version = "0.41.0"` in `pyproject.toml`. Prepend to `CHANGELOG.md`:

````markdown
## [0.41.0] - 2026-08-10

### Added

- **`YamlSource` now carries every top-level key it does not interpret**, reachable via `get_extras()` and exported as `SEMANTIC_KEYS`. Previously `_load_from_raw` read exactly four keys and discarded the rest with no error, no warning, and no log line — so adding an unsupported section was indistinguishable from success. A downstream consumer carried two custom sections in a production semantic YAML for roughly three weeks: well-formed, internally consistent, covered by a dedicated lint job and ~240 lines of tests, and contributing nothing to any rendered prompt or tool output the entire time. There was no symptom to investigate; it surfaced only when someone rendered `to_system_prompt()` in a scratch script and grepped the output for their own content.

  A one-character typo had the same consequence — `relationship:` for `relationships:` deleted a whole section while the library reported success — and that is the more common way this gets hit.

- **`YamlSource(..., expected_extras=...)`** and the same parameter on `from_raw`. Left as `None` (the default) uninterpreted keys are logged at WARNING and carried anyway, so every existing contract keeps loading. Pass a collection and any key outside it raises at load; `frozenset()` is therefore a strict mode. One parameter instead of a `strict=True` flag, which would have fired forever on a consumer's own deliberate sections.

- **`ExtensibleSemanticSource`**, a `runtime_checkable` protocol carrying only `get_extras()`. Deliberately a sibling of `SemanticSource` rather than an extension of it: `runtime_checkable` isinstance checks method *presence*, so folding `get_extras` into `SemanticSource` would have made every external custom source fail `isinstance(src, SemanticSource)` on upgrade.

- **`XmlPromptRenderer(extra_sections=...)`**, accepting a sequence of section names (default YAML-in-a-tag formatter) or a mapping of name → callable. Nothing renders unless named, so a section kept for internal bookkeeping never leaks into a prompt; naming a section the source does not carry warns rather than failing quietly.

### Changed

- **Extras travel through `freeze_semantic_source()` and into `contract_digest`.** They are resident prompt text, so they shape agent behaviour; a digest that ignored them would let the ARD publish→verify loop attest to something different from what the agent saw. Editing a hint's prose therefore moves the digest and invalidates a pinned attestation. Contracts *without* extras dump byte-identically to before, so no existing frozen contract or published digest moves.

- **Extras values are normalised to JSON-safe types at load.** Dates and datetimes become ISO strings — the shape this exists to carry explicitly wants a verification date — and anything else unserializable raises immediately, naming the key path. At load rather than at freeze, so a bad value fails where it was authored instead of months later inside an ARD publish.

### Compatibility

The framework carries extras and places them in the prompt on request. It does not interpret, validate, index, or compute over their content: no schema checking, no staleness detection, no tool serves them. That boundary is the same one `validation/examples.py` draws for verified examples, and it is why `column_hints` and `join_paths` — the two sections that prompted #60 — ship as the documented *example* of extras rather than as framework vocabulary.

Closes #60.
````

- [ ] **Step 4: Verify the docs match the code**

Run: `uv run pytest -v && prek run --all-files`
Then paste each README snippet into a scratch file under the session scratchpad and run it, confirming `expected_extras` and `extra_sections` behave exactly as documented. Documentation that drifts from the code is the failure mode this release exists to fix.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: semantic extras — carry, place, and disclose consumer sections (v0.41.0)"
```

---

## Self-Review Notes

Checked against `docs/superpowers/specs/2026-08-10-semantic-extras-design.md`:

| Spec item | Task |
|---|---|
| D1 extras as a general mechanism | 4, 8, 9 |
| D2 digest-bearing extras | 7 |
| D3 opt-in rendering | 8 |
| D4 rename, hard | 1 |
| Component 1 `SEMANTIC_KEYS` + carriage | 4 |
| Component 2 `expected_extras` | 5 |
| Component 3 JSON-safety at load | 6 |
| Component 4 `ExtensibleSemanticSource` | 7 |
| Component 5 dump carries extras, omit-when-empty | 7 |
| Component 6 constructor | 2 (thresholds), 8 (`extra_sections`) |
| Component 7 default formatter | 8 |
| Component 8 placement and ordering | 8 |
| Component 9 honest degradation | 3 |
| Non-goal: no staleness, no schema check, no tool | Enforced by Global Constraints; documented in 9 |
| Testing section | 2, 3, 4, 5, 6, 7, 8 |
| Release sequencing | 1, 3, 9 |
