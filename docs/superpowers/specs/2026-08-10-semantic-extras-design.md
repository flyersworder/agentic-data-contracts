# Semantic extras, renderer rename, and honest degradation — design

**Date:** 2026-08-10
**Status:** Approved, not yet implemented
**Author:** Qing Ye + Claude
**Issue:** #60
**Baseline:** v0.38.0

## Problem

Issue #60 reports three defects with one shape: *the library delivers less than
it appears to, with no signal to the consumer.*

1. `YamlSource` reads exactly four top-level keys and discards every other key
   with no error, warning, or log line. A downstream consumer carried two custom
   sections in a production semantic YAML for ~3 weeks — well-formed, linted,
   covered by ~240 lines of tests — that contributed nothing to any rendered
   prompt or tool output. Adding an unsupported section is indistinguishable
   from success. A one-character typo (`relationship:` for `relationships:`)
   deletes a whole section the same way.
2. Two of those custom sections — `column_hints` and `join_paths` — need a
   supported home rather than staying non-standard.
3. `ClaudePromptRenderer.RELATIONSHIP_DETAIL_THRESHOLD` is a class constant.
   Above it, per-relationship join keys are dropped from the prompt and replaced
   with a per-table `join_count`. On a 52-relationship contract this suppresses
   ~3.1x the whole fragment (9,658 chars rendered vs 29,852 detailed), with no
   log line and no way to opt out short of subclassing. `METRIC_DETAIL_THRESHOLD`
   has the identical shape and the same gap.

## Decisions

### D1 — Extras are a general mechanism, not two blessed sections

The framework grows a *carriage mechanism* for consumer-authored semantic
sections, not vocabulary for `column_hints` and `join_paths` specifically.

Rationale. Every section the semantic layer models today earns first-class
status by being *computed on*, not merely carried:

| Section | What the framework does with it |
|---|---|
| `relationships` | Indexed by `build_relationship_index`; walked by `find_join_path`; consumed by the join checkers |
| `metrics.decompositions` | Operator and arity checked, operands resolved, cycles rejected by `_assert_identity_acyclic`, flattened into identity edges for `trace_metric_impacts` |
| `metrics.drill_by` | Column references validated against declared table schemas by `validate_drill_by` |
| `metric_impacts` | Walked by `walk_metric_impacts` |

`column_hints` and `join_paths` as proposed are pure carriage: parsed, then
interpolated into a string. Nothing validates them, nothing computes with them,
nothing catches them going stale. Blessing pure carriage as vocabulary sets a
precedent that any consumer's private convention deserves a dataclass.

Two existing boundary calls point the same way. `docs/architecture.md` records
"Semantic governance: reference-based — point to external source of truth, don't
replicate it." The `validation/examples.py` module docstring is blunter: "The
framework never stores, loads, retrieves, or serves examples" — a deliberate
decision, on knowledge *more* agent-grounding than column hints, to validate an
externally-owned corpus rather than own it. `VerifiedExample.from_dict` already
handles unknown keys by preserving them under `metadata` and never interpreting
them. Extras generalise a pattern this codebase already chose.

`join_paths` specifically is also largely derivable today: `find_join_path` does
BFS multi-hop path-finding, `Relationship.preferred` disambiguates competing
paths, and `build_relationship_index` stable-sorts preferred edges first. What a
named path genuinely adds is a stable name and *path-level* notes — a much
smaller delta than a new first-class section justifies.

### D2 — Extras are digest-bearing

Extras survive `dump_semantic_source` → `SemanticSource.inline` →
`YamlSource.from_raw`, so they land in `contract_canonical_bytes` and change
`contract_digest`.

Rationale. Extras are resident prompt text, so they shape agent behaviour. The
ARD publish→verify loop tells a consumer to fetch the contract, recompute the
digest, and rebuild enforcement. A digest that ignores extras would verify
something different from what the agent actually saw.

Consequence, to be documented plainly: fixing a typo in a hint's prose changes
`contract_digest` and invalidates any attestation pinned to it. Hint prose lives
under the same change discipline as forbidden operations.

### D3 — Rendering is opt-in by name

Nothing renders unless the consumer names it. Rejected alternative: auto-render
every extra section. That mirrors the bug being fixed — instead of content
silently vanishing, arbitrary YAML silently inflates every request, and a
section authored as internal bookkeeping leaks into the prompt.

Rejected alternative: carriage only, consumers subclass the renderer. Subclassing
a concrete renderer couples consumers to its internals, and makes the common case
(put my list of hints in the prompt) require the most code.

### D4 — `ClaudePromptRenderer` is renamed to `XmlPromptRenderer`, hard

`core/prompt.py` contains no Claude- or Anthropic-specific logic: the only
matches for `Claude|anthropic` in the file are the class name and its docstring.
No SDK import, no model-specific handling. It emits XML tags — an
Anthropic-originated prompting convention, but the artifact is plain XML.

The name has aged against the codebase: `tools/langchain.py` and
`tools/pydantic_ai.py` both ship, so a Pydantic AI user's prompt is currently
rendered by a class named after a runtime they are not using.

Naming principle: name a protocol implementation after the axis along which
implementations differ. `PromptRenderer` exists so alternatives can exist, and a
second implementation would differ by *output format* (Markdown sections instead
of XML tags), not by vendor. `XmlPromptRenderer` names the real axis and leaves a
coherent slot for `MarkdownPromptRenderer`.

No deprecated alias. Consistent with v0.38.0 removing `usage_limits_from_contract`
outright; ships as `feat!`.

## Non-goals

- **Staleness detection over extras.** The framework models `last_reviewed`
  staleness for metrics and metric impacts. It will not do so for extras: that
  requires interpreting them, which collapses the D1 boundary. Documented as the
  consumer's lint to own.
- **Schema validation of extras.** No shape checking beyond JSON-safety (D2
  requires it). A malformed hint is the consumer's bug to catch.
- **Extras reaching the tools layer.** `get_extras()` is available to anyone
  holding the source, but no tool in `tools/factory.py` serves extras and none
  is added. Both `column_hints` and `join_paths` exist to prevent an error
  *before* SQL is written; behind an on-demand tool they cost a round-trip the
  model only pays if it already suspects it is wrong.
- **Making `column_hints` / `join_paths` first-class.** Explicitly out, per D1.
  They become the reference example for extras in the docs instead.
- **Warning on absent supported sections.** Issue #60 notes that an absent
  `tables:` is indistinguishable from an ignored one. Left alone: every optional
  section is legitimately absent in most contracts, so warning would fire almost
  everywhere and train readers to ignore the log — costing the signal this work
  exists to buy.

## Design

### Component 1 — `SEMANTIC_KEYS` and extras carriage (`semantic/yaml_source.py`)

A module-level constant becomes the single source of truth for what the parser
interprets, exported from the package so consumers assert against it rather than
hardcoding a list that drifts on the next bump:

```python
SEMANTIC_KEYS = frozenset({"metrics", "tables", "relationships", "metric_impacts"})
```

`_load_from_raw` keeps everything outside that set. Key policy (Component 2)
runs before value normalization (Component 3), so a mistyped key is reported as
a mistyped key rather than as a serialization complaint about its contents:

```python
extras = {k: v for k, v in raw.items() if k not in SEMANTIC_KEYS}
_apply_extras_policy(extras, expected_extras)   # warn | raise | silent
self._extras = _normalize_extras(extras)        # date -> ISO, json-safe assert
```

`get_extras()` returns a shallow copy, consistent with `get_metrics` and
`get_relationships` returning `list(...)`.

### Component 2 — `expected_extras`, replacing warn/strict

`YamlSource.__init__` and `YamlSource.from_raw` both gain one keyword-only
parameter:

```python
expected_extras: Collection[str] | None = None
```

| Value | Behaviour |
|---|---|
| `None` (default) | `logger.warning` naming every extras key. Every existing contract still loads — safe for a minor release. |
| A collection | Keys inside it are silent; any key outside it raises `ValueError` naming the key and the expected set. |
| `frozenset()` | Equivalent to a strict mode: any extras key raises. |

This single parameter covers all three remedies the issue asked for (warn by
default, opt-in strict, assert against a known set) and is strictly better than a
`strict=True` flag, which would fire on a consumer's *deliberate* extras forever.

Warning text names the remedy rather than just the symptom, since the same
message must serve both the deliberate-extras case and the typo case:

> `YamlSource: keys not interpreted as semantic vocabulary: ['widget_hints'] (interpreted keys: ['metric_impacts', 'metrics', 'relationships', 'tables']). Available via get_extras(); rendered only if named in XmlPromptRenderer(extra_sections=...). If one of these is a typo, the section is not being read.`

Not memoised, following the reasoning already recorded on
`validation/validator.py`'s unenforceable-operation warning: caching a logging
side effect means a consumer who calls `logging.basicConfig()` after building its
contracts loses the diagnostic permanently.

### Component 3 — JSON-safety normalization at load

`contract_canonical_bytes` runs `json.dumps` over the frozen payload, and extras
ride inside `SemanticSource.inline`, typed `dict[str, Any]`. A YAML-native date —
which the `column_hints` shape explicitly wants ("ideally with the date it was
verified against the database") — is not JSON. `dump_semantic_source` already hit
this and solved it by hand with `_iso()` for `last_reviewed`.

`_normalize_extras` walks the structure at load time and:

1. Coerces `date` / `datetime` to ISO strings, using the same ISO convention
   `dump_semantic_source` already applies to `last_reviewed`. (`_parse_date` in
   `semantic/yaml_source.py` handles the inbound direction; the ISO helper is
   currently nested inside `dump_semantic_source` in `semantic/base.py` and
   should be lifted to module level so both call sites share one definition.)
2. Asserts `json.dumps` succeeds on the result, raising `ValueError` naming the
   offending key path if anything remains unserializable.

At load time, not at freeze time: a bad hint then fails where it was authored,
not months later inside an ARD publish.

### Component 4 — `ExtensibleSemanticSource` (`semantic/base.py`)

```python
@runtime_checkable
class ExtensibleSemanticSource(Protocol):
    def get_extras(self) -> dict[str, Any]: ...
```

A sibling protocol, not an extension of `SemanticSource`. `runtime_checkable`
isinstance checks method *presence*, so adding `get_extras` to `SemanticSource`
would instantly make every external custom source fail
`isinstance(src, SemanticSource)`. A sibling breaks nobody, stays fully typed,
and matches the protocol-based extensibility decision in CLAUDE.md.

`DbtSource` and `CubeSource` do **not** implement it. Both `dump_semantic_source`
and the renderer branch on `isinstance(source, ExtensibleSemanticSource)` and
degrade cleanly when absent.

### Component 5 — `dump_semantic_source` carries extras

`dump_semantic_source` currently returns a dict literal directly; it binds that
literal to a local so extras can merge in, **omitted entirely when empty**:

```python
payload = { "tables": ..., "metrics": ..., "relationships": ..., "metric_impacts": ... }
if isinstance(source, ExtensibleSemanticSource):
    payload.update(source.get_extras())
return payload
```

Omit-when-empty is load-bearing, not cosmetic: it keeps
`contract_canonical_bytes` byte-identical for every contract without extras, so
existing frozen contracts and published ARD digests are unchanged. This follows
the precedent already recorded for `decompositions` and `drill_by`.

Key collision is impossible by construction: extras are defined as the complement
of `SEMANTIC_KEYS`, and `dump_semantic_source` writes only `SEMANTIC_KEYS`.

### Component 6 — `XmlPromptRenderer` constructor

```python
ExtraSectionRenderer = Callable[[str, Any], list[str]]

class _Unset(enum.Enum):
    TOKEN = 0

_UNSET = _Unset.TOKEN

def __init__(
    self,
    *,
    extra_sections: Sequence[str] | Mapping[str, ExtraSectionRenderer | None] = (),
    metric_detail_threshold: int | None | _Unset = _UNSET,
    relationship_detail_threshold: int | None | _Unset = _UNSET,
) -> None: ...
```

`extra_sections` accepts a sequence of names (built-in default formatter) or a
mapping of name → callable, where a `None` value selects the default formatter:

```python
XmlPromptRenderer(extra_sections=["column_hints"])
XmlPromptRenderer(extra_sections={"join_paths": render_join_paths})
```

The callable receives `(section_name, payload)` and returns the **complete**
section, its own enclosing tags included — the library contributes placement and
ordering only, never wrapping. That is what distinguishes it from the default
formatter, and it matches how `_render_relationships` and its siblings already
return whole blocks. One function can therefore serve several sections, which is
why it receives the name.

`_UNSET` is a single-member enum used as a sentinel, so an explicit `None` can
mean "never degrade" while an omitted argument falls back to the class attribute.
An enum rather than `object()` because ty narrows enum members in a union but
cannot narrow a bare sentinel instance, so `int | None | _Unset` type-checks at
the use site without a cast.
`METRIC_DETAIL_THRESHOLD = 20` and `RELATIONSHIP_DETAIL_THRESHOLD = 30` remain as
class attributes supplying those defaults, so existing subclass overrides keep
working; the constructor parameter takes precedence when given.

### Component 7 — the default extras formatter

Section boundary in XML, body in YAML:

```xml
<column_hints>
- table: schema.some_table
  prefer: real_column_name
  reason: Verified against the database on 2026-03-14.
</column_hints>
```

Produced with `yaml.safe_dump` — already a dependency. Rationale: the payload has
arbitrary nesting and no schema, so any hand-rolled XML walker would be guessing
at shape. Dumping the author's own structure back is faithful, compact, handles
nesting for free, and models read YAML fine. The XML tag keeps the section
boundary consistent with the rest of the prompt.

Known limitation, pre-existing and not introduced here: nothing escapes authored
content, so prose containing `</column_hints>` would break the enclosing tag.
This matches current behaviour for `description="..."` attributes, which do not
escape quotes either. Extras are authored contract content at the same trust
level as the rest of the contract.

### Component 8 — placement and ordering

Rendered extras go inside `<data_contract>`, after `<table_relationships>` and
before resource limits, in the order the consumer named them. Deterministic
ordering keeps a rendered-prompt diff reviewable.

### Component 9 — honest degradation (`core/prompt.py`)

Both degrading branches gain a log line and an honest hint.

`_render_relationships`, above threshold:

```
logger.info(
    "XmlPromptRenderer: %d relationships exceeds"
    " relationship_detail_threshold=%d — per-relationship join keys omitted"
    " from the prompt; agents must call lookup_relationships. Pass"
    " relationship_detail_threshold=None to render them.",
    len(rels), threshold,
)
```

and the rendered hint becomes:

```xml
<hint>52 relationships defined; per-relationship join keys omitted above
threshold 30. Use lookup_relationships(table="schema.table") to get join
details and required filters.</hint>
```

`_render_metrics` gets the symmetric treatment against
`metric_detail_threshold`. Fixing only relationships would ship a corrected
threshold beside an uncorrected one.

`logger.info`, not `warning`: degradation is intended behaviour under a
configured budget, unlike an ignored key.

## Data flow

```
data_contract.yml / semantic .yml
        │
        ▼  YamlSource._load_from_raw
   SEMANTIC_KEYS ──split──> interpreted sections   (metrics, tables, ...)
        │                   extras (complement)
        │                       │
        │                       ▼ expected_extras    (warn | raise | silent)
        │                       ▼ _normalize_extras  (date -> ISO, json.dumps assert)
        │                       ▼ self._extras
        ▼
   get_extras()  ◄── ExtensibleSemanticSource
        │
        ├──► dump_semantic_source ──► SemanticSource.inline ──► contract_canonical_bytes ──► contract_digest
        │                                     │
        │                                     ▼ YamlSource.from_raw  (round-trip)
        │
        └──► XmlPromptRenderer(extra_sections=...) ──► <data_contract> ... </data_contract>
```

## Error handling

| Condition | Behaviour |
|---|---|
| Extras present, `expected_extras=None` | `logger.warning` naming the keys and the remedy |
| Extras key outside `expected_extras` | `ValueError` naming the key and the expected set |
| Extras contain a `date` / `datetime` | Coerced to ISO string at load |
| Extras contain something else unserializable | `ValueError` at load naming the key path |
| `extra_sections` names a section not in extras | `logger.warning` naming it and listing available extras keys — same typo class as an ignored key, so it gets the same treatment |
| A consumer `ExtraSectionRenderer` raises | Propagates. Naming a section you cannot render is a bug, and swallowing it would silently drop content — the exact failure this work removes |
| Source does not implement `ExtensibleSemanticSource` | No extras rendered, no error; `dump_semantic_source` writes no extras keys |

## Testing

TDD per CLAUDE.md: tests first, each layer under its own `tests/test_<layer>/`.

**`tests/test_semantic/test_yaml_source.py`**
- Unknown top-level keys land in `get_extras()`, unchanged in content.
- Interpreted keys never appear in `get_extras()`.
- `SEMANTIC_KEYS` is importable from the package and matches the keys
  `_load_from_raw` actually reads.
- `expected_extras=None` warns and names every extras key (caplog).
- `expected_extras={"column_hints"}` is silent for that key, raises for another.
- `expected_extras=frozenset()` raises on any extras key.
- A YAML-native date inside extras is coerced to an ISO string.
- An unserializable value raises `ValueError` naming the key path.
- The reproduction from issue #60 (`widget_hints`) now surfaces.

**`tests/test_semantic/test_base.py`**
- `dump_semantic_source` carries extras for an extensible source.
- **Regression guard:** a source with no extras produces a dict byte-identical
  to the pre-change output, so `contract_digest` is unchanged for every existing
  frozen contract.
- Round-trip: `YamlSource.from_raw(dump_semantic_source(src)).get_extras()`
  equals `src.get_extras()`.
- `DbtSource` / `CubeSource` fail `isinstance(src, ExtensibleSemanticSource)` and
  dump without extras keys.

**`tests/test_core/test_prompt_renderers.py`**
- Extras present but unnamed render nothing.
- A named section renders inside `<data_contract>` with the default formatter.
- A custom callable's output is used verbatim; it receives `(name, payload)`.
- Multiple named sections render in the order given.
- Naming an absent section warns and renders nothing.
- A raising callable propagates.
- `relationship_detail_threshold=None` renders join keys for a 52-relationship
  source; an integer degrades and logs at INFO (caplog).
- Symmetric assertions for `metric_detail_threshold`.
- Omitted threshold arguments fall back to the class attributes; a subclass
  overriding the class attribute still wins over the default.

**`tests/test_ard.py`**
- `contract_digest` changes when extras change.
- `contract_digest` is unchanged for a contract with no extras.

**`tests/test_public_api.py`**
- `XmlPromptRenderer`, `SEMANTIC_KEYS`, and `ExtensibleSemanticSource` are
  exported; `ClaudePromptRenderer` is gone.

## Release sequencing

Three PRs. Each is independently releasable and independently revertible.

**PR 1 — v0.39.0, `feat!`: rename `ClaudePromptRenderer` to `XmlPromptRenderer`.**
Mechanical, public-API-breaking, no behaviour change. Sites: package
`__init__.py` (import + `__all__`), `core/prompt.py` (class + module docstring),
`core/contract.py` (3), `tools/factory.py` (1),
`tests/test_core/test_prompt_renderers.py` (17), `tests/test_public_api.py` (2),
`README.md`, `docs/architecture.md`, `examples/revenue_agent/agent.py`.
CHANGELOG history is not rewritten. Lands first so no documentation ever ships
`ClaudePromptRenderer(extra_sections=...)` under a name that then changes.

**PR 2 — v0.40.0: honest degradation.** Components 6 (threshold half) and 9.
Closes issue #60 part 3. Small, independent of extras.

**PR 3 — v0.41.0: extras.** Components 1–8. Closes issue #60 parts 1 and 2.

PR 2 and PR 3 are mutually independent and may swap if the extras mechanism is
wanted downstream sooner; both depend on PR 1 only to avoid renaming twice.

## Documentation

- README gains an extras section using `column_hints` and `join_paths` as the
  worked example — the sections that motivated this become the reference
  illustration rather than framework vocabulary.
- `docs/architecture.md` records the extras boundary alongside the existing
  reference-based semantic governance decision, and the renderer rename.
- The digest consequence from D2 and the staleness non-goal are both stated in
  the README extras section, not only here.

## Follow-up for the reporting consumer

After PR 3, the downstream YAML sets `expected_extras={"column_hints",
"join_paths"}` and constructs `XmlPromptRenderer(extra_sections=[...])`. Its
existing lint job keeps owning shape and staleness of the hint content, and the
typo class that motivated the issue now raises at load.
