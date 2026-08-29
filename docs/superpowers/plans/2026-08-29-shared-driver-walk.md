# Shared-Driver Walk Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `walk_metric_impacts` reports every declared edge between reached metrics, so a metric driving two others is no longer reported on only one branch.

**Architecture:** `visited` stops gating *reporting* and gates only *expansion*, which is the sole thing it was ever needed for. The walk becomes complete and uncapped; `trace_metric_impacts` gains a 200-edge serialization cap so an O(E) result cannot flood an agent's context.

**Tech Stack:** Python 3.12+, `uv run pytest`, `prek run --all-files`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-08-29-shared-driver-walk-design.md`

## Global Constraints

- Version `0.49.0` in `pyproject.toml` and as the `CHANGELOG.md` heading.
- No new dependencies. `networkx` was evaluated and rejected in the spec.
- `build_metric_impact_index`, `MetricEdge`, `IdentityEdge`, `MetricImpact` are **not** changed — no shape change, no new fields.
- The #81 value-equality dedupe is preserved exactly: two edges equal in every field report once; two edges differing in any field both report.
- `walk_metric_impacts` stays complete and uncapped. The cap lives only in `tools/factory.py`.
- TDD, red first. Run `uv run pytest` and `prek run --all-files` before every commit.
- Run linters through `prek`, never a bare `ruff`/`ty`.

---

### Task 1: `walk_metric_impacts` reports every declared edge

**Files:**
- Modify: `src/agentic_data_contracts/semantic/base.py` — `walk_metric_impacts`
- Test: `tests/test_semantic/test_metric_impacts.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `walk_metric_impacts(index, start, *, direction, max_depth=2) -> list[tuple[int, MetricEdge]]` — signature unchanged; it now returns strictly more edges. Task 2 consumes this return value.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_semantic/test_metric_impacts.py`, inside `class TestWalkMetricImpacts`. The file already imports `MetricImpact`, `build_metric_impact_index`, `walk_metric_impacts`. Add `Decomposition`, `MetricDefinition`, and `identity_edges_from_metrics` to the existing `from agentic_data_contracts.semantic.base import (...)` block.

```python
    def _shared_driver_metrics(self) -> list[MetricDefinition]:
        """`paying_users` is an operand of BOTH `revenue` and `new_revenue`."""

        def m(name: str, decs: list[Decomposition] | None = None) -> MetricDefinition:
            return MetricDefinition(
                name=name,
                description="",
                sql_expression="x",
                decompositions=decs or [],
            )

        return [
            m(
                "revenue",
                [Decomposition(operator="sum", operands=["paying_users", "new_revenue"])],
            ),
            m(
                "new_revenue",
                [
                    Decomposition(
                        operator="product",
                        operands=["paying_users", "conv"],
                        convention="fold_into",
                        convention_operand="conv",
                    )
                ],
            ),
            m("paying_users"),
            m("conv"),
        ]

    def test_a_shared_driver_reports_both_of_its_edges(self) -> None:
        """One metric driving two others must arrive on both branches.

        `paying_users` drives `revenue` directly and `new_revenue` as a
        `product` operand. Reporting only the first branch drops the
        `operator` and `convention` that only the second edge carries -- the
        facts a root-cause walk exists to deliver.
        """
        index = build_metric_impact_index(
            [e.as_driver_edge() for e in identity_edges_from_metrics(self._shared_driver_metrics())]
        )
        walk = walk_metric_impacts(index, "revenue", direction="upstream", max_depth=3)
        pairs = {(e.from_metric, e.to_metric) for _, e in walk}
        assert ("paying_users", "new_revenue") in pairs

    def test_a_shared_driver_carries_its_own_operator_and_convention(self) -> None:
        index = build_metric_impact_index(
            [e.as_driver_edge() for e in identity_edges_from_metrics(self._shared_driver_metrics())]
        )
        walk = walk_metric_impacts(index, "revenue", direction="upstream", max_depth=3)
        edge = next(
            e
            for _, e in walk
            if (e.from_metric, e.to_metric) == ("paying_users", "new_revenue")
        )
        assert edge.operator == "product"
        assert edge.convention == "fold_into"

    def test_a_shared_driver_is_expanded_once(self) -> None:
        """Reporting both edges must not expand the node twice."""
        index = build_metric_impact_index(
            [e.as_driver_edge() for e in identity_edges_from_metrics(self._shared_driver_metrics())]
        )
        walk = walk_metric_impacts(index, "revenue", direction="upstream", max_depth=3)
        depths = [d for d, e in walk if e.from_metric == "conv"]
        assert depths == [2]  # reached once, via new_revenue, not re-expanded
```

Now replace the existing `test_cycle_visited_tracking` **entirely** with the two tests below. Its assertions encode the old guarantee, not its intent; its intent is termination.

```python
    def test_a_cycle_terminates_and_reports_its_closing_edge(self) -> None:
        """`visited` exists to stop cycles, and still does.

        Replaces the former `test_cycle_visited_tracking`, whose assertions
        ("each target at most once", "start is never a target") encoded the
        node-visited guarantee rather than this test's actual intent, which is
        termination. A cycle-closing edge is declared, and an agent tracing
        root cause should be told the graph closes.
        """
        impacts = [
            MetricImpact(from_metric="a", to_metric="b"),
            MetricImpact(from_metric="b", to_metric="c"),
            MetricImpact(from_metric="c", to_metric="a"),
        ]
        index = build_metric_impact_index(impacts)
        walk = walk_metric_impacts(index, "a", direction="downstream", max_depth=10)
        assert [(e.from_metric, e.to_metric) for _, e in walk] == [
            ("a", "b"),
            ("b", "c"),
            ("c", "a"),
        ]

    def test_a_self_loop_terminates_and_reports_once(self) -> None:
        index = build_metric_impact_index([MetricImpact(from_metric="a", to_metric="a")])
        walk = walk_metric_impacts(index, "a", direction="downstream", max_depth=5)
        assert [(e.from_metric, e.to_metric) for _, e in walk] == [("a", "a")]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_semantic/test_metric_impacts.py -v`

Expected: the three `shared_driver` tests FAIL (`("paying_users", "new_revenue") in pairs` is False; `StopIteration` on the `next(...)`), and `test_a_cycle_terminates_and_reports_its_closing_edge` FAILS because `("c", "a")` is missing. `test_a_self_loop_terminates_and_reports_once` may already pass — that is expected and fine; it is a regression guard.

Every other test in the file must still pass at this point, including `test_parallel_edges_to_one_neighbor_are_all_reported` and `test_byte_identical_edges_are_reported_once`.

- [ ] **Step 3: Make the change**

In `src/agentic_data_contracts/semantic/base.py`, inside `walk_metric_impacts`'s adjacency loop, replace:

```python
            if neighbor in visited or any(edge == prior for prior in seen):
                continue
            result.append((depth + 1, edge))
            seen.append(edge)
            reached[neighbor] = None
```

with:

```python
            if any(edge == prior for prior in seen):
                continue
            result.append((depth + 1, edge))
            seen.append(edge)
            if neighbor not in visited:
                reached[neighbor] = None
```

- [ ] **Step 4: Rewrite the docstring's contract paragraphs**

In the same function, replace this paragraph:

```
    Returns ``(depth, edge)`` pairs in BFS order, where depth is the number
    of hops from ``start`` (direct neighbors at depth 1).  Visited tracking
    prevents cycles: each reachable metric is expanded at most once, and an
    edge is reported only when it reaches a metric no earlier edge has
    reached.
```

with:

```
    Returns ``(depth, edge)`` pairs in BFS order, where depth is the number
    of hops from ``start`` (direct neighbors at depth 1).  Visited tracking
    prevents cycles by gating *expansion* only: each reachable metric is
    expanded at most once, while **every declared edge between reached
    metrics is reported**, including one onto a metric another branch already
    reached. That is what lets a shared driver -- one metric that is an
    operand of two parents -- arrive on both branches, carrying the
    ``operator`` and ``convention`` that only its second edge holds.

    A consequence worth knowing: cycle-closing edges are reported. In
    ``a -> b -> c -> a`` walked downstream, ``c -> a`` appears. The edge is
    declared and an agent tracing root cause should be told the graph closes.
    Termination is unaffected -- it was never the reporting gate that
    guaranteed it.
```

Then **delete** this paragraph entirely — it is no longer true:

```
    A shared operand deeper in the graph is still lost — if ``a`` drives both
    ``b`` and ``c``, and ``b`` is reached first, the edge ``a -> c`` is not
    reported. That is the node-visited BFS this function has always been;
    widening it would report cycle-closing edges too, which is a different
    contract.
```

Leave the *parallel* edges paragraph exactly as it is — that behaviour is unchanged.

- [ ] **Step 5: Run the full suite and linters**

Run: `uv run pytest -q` then `prek run --all-files`

Expected: all tests pass, all hooks pass. If a test outside `tests/test_semantic/` fails, stop and report it — the walk is consumed by `trace_metric_impacts` and a break there is a real finding, not something to patch around.

- [ ] **Step 6: Commit**

```bash
git add src/agentic_data_contracts/semantic/base.py tests/test_semantic/test_metric_impacts.py
git commit -m "fix: report every declared edge between reached metrics (#83)"
```

---

### Task 2: `trace_metric_impacts` caps what it serializes

**Files:**
- Modify: `src/agentic_data_contracts/tools/factory.py` — the `trace_metric_impacts` tool body
- Test: `tests/test_tools/test_metric_impacts_tools.py`

**Interfaces:**
- Consumes: `walk_metric_impacts(...) -> list[tuple[int, MetricEdge]]` from Task 1, now returning O(E) edges.
- Produces: no new public names. `payload["note"]` may now carry two joined sentences.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_tools/test_metric_impacts_tools.py`. Match the existing file's style: `@pytest.mark.asyncio`, build tools via `create_tools(...)`, pick the tool by name, `json.loads(result["content"][0]["text"])`.

```python
class _ImpactsOnly:
    """A minimal `SemanticSource` carrying nothing but influence edges.

    `create_tools` has no `metric_impacts` parameter -- impacts reach the tool
    through `semantic_source.get_metric_impacts()`. A dense graph of hundreds
    of edges is impractical as a YAML fixture, so this stub supplies them
    directly. Every other protocol method returns empty, which is all
    `trace_metric_impacts` needs.
    """

    def __init__(self, impacts: list[MetricImpact]) -> None:
        self._impacts = impacts
        # `create_tools` validates every impact against `get_metrics()` and
        # raises on an unknown endpoint, so the stub must declare the metrics
        # its edges name. Derived rather than passed in, so a test only ever
        # states its edges.
        names = sorted({n for i in impacts for n in (i.from_metric, i.to_metric)})
        self._metrics = [
            MetricDefinition(name=n, description="", sql_expression="x") for n in names
        ]

    def get_metrics(self) -> list[MetricDefinition]:
        return self._metrics

    def get_metric(self, name: str) -> MetricDefinition | None:
        return next((m for m in self._metrics if m.name == name), None)

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return None

    def get_table_schemas(self) -> dict[str, TableSchema]:
        return {}

    def search_metrics(self, query: str) -> list[MetricDefinition]:
        return []

    def get_relationships(self) -> list[Relationship]:
        return []

    def get_relationships_for_table(self, table: str) -> list[Relationship]:
        return []

    def get_metric_impacts(self) -> list[MetricImpact]:
        return self._impacts


async def _trace(contract: DataContract, impacts: list[MetricImpact], **args) -> dict:
    tools = create_tools(contract, semantic_source=_ImpactsOnly(impacts))
    tool = next(t for t in tools if t.name == "trace_metric_impacts")
    result = await tool.callable(args)
    return json.loads(result["content"][0]["text"])


@pytest.mark.asyncio
async def test_trace_metric_impacts_caps_the_edges_it_serializes(
    contract_no_domains: DataContract,
) -> None:
    """A dense graph must not flood the agent's context.

    The walk itself stays complete; only what this tool serializes is capped.
    """
    names = [f"m{i}" for i in range(30)]
    impacts = [
        MetricImpact(from_metric=a, to_metric=b) for a in names for b in names if a != b
    ]
    data = await _trace(
        contract_no_domains, impacts, metric_name="m0", direction="downstream", max_depth=10
    )
    assert len(data["edges"]) == 200
    assert "870" in data["note"]  # 30 * 29 edges exist within the horizon
    assert "max_depth" in data["note"]


@pytest.mark.asyncio
async def test_trace_metric_impacts_under_the_cap_has_no_truncation_note(
    contract_no_domains: DataContract,
) -> None:
    impacts = [MetricImpact(from_metric="a", to_metric=f"b{i}") for i in range(10)]
    data = await _trace(
        contract_no_domains, impacts, metric_name="a", direction="downstream"
    )
    assert len(data["edges"]) == 10
    assert "showing" not in data.get("note", "")


@pytest.mark.asyncio
async def test_trace_metric_impacts_cap_boundary_is_exact(
    contract_no_domains: DataContract,
) -> None:
    """Pinned at 200 and 201 so an off-by-one cannot pass."""
    for total, expect_note in ((200, False), (201, True)):
        impacts = [
            MetricImpact(from_metric="hub", to_metric=f"leaf{i}") for i in range(total)
        ]
        data = await _trace(
            contract_no_domains, impacts, metric_name="hub", direction="downstream"
        )
        assert len(data["edges"]) == min(total, 200)
        assert ("showing" in data.get("note", "")) is expect_note
```

Add to the module's imports:

```python
from agentic_data_contracts.semantic.base import (
    MetricDefinition,
    MetricImpact,
    Relationship,
    TableSchema,
)
```

**Verified against v0.48.0:** this stub was run before the plan was written. It builds tools successfully, and a 30-metric complete graph returns **29 edges today** and `note: None` — which is the pre-fix behaviour these tests exist to change.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_tools/test_metric_impacts_tools.py -v`

Expected: the cap tests FAIL with `len(data["edges"]) == 870 != 200`. The under-the-cap test may already pass — it is a regression guard.

- [ ] **Step 3: Add the cap constant**

Near the other module-level constants at the top of `src/agentic_data_contracts/tools/factory.py`:

```python
# What `trace_metric_impacts` will serialize into an agent's context. The walk
# itself is complete and uncapped: a graph primitive that silently truncates is
# the same class of loss the walk was fixed to stop. Truncation happens here,
# announced in `note`, and drops the deepest edges because the walk is in BFS
# order. 200 clears any realistic metric graph -- the shipped semantic layers
# report at most 5 edges -- while bounding a dense one.
_MAX_TRACE_EDGES = 200
```

- [ ] **Step 4: Truncate, and compute the identity check against the FULL walk**

In the `trace_metric_impacts` body, replace:

```python
            walk = walk_metric_impacts(
                graph_index, metric_name, direction=direction, max_depth=max_depth
            )
            edges: list[dict[str, Any]] = []
            for depth, edge in walk:
```

with:

```python
            walk = walk_metric_impacts(
                graph_index, metric_name, direction=direction, max_depth=max_depth
            )
            total_edges = len(walk)
            # Read off the full walk, before truncation: a graph can hold
            # identity edges past the cap, and deciding "no identity edges came
            # back" from the truncated list would emit a direction note that is
            # simply wrong.
            walk_has_identity = any(e.kind == "identity" for _, e in walk)
            walk = walk[:_MAX_TRACE_EDGES]
            edges: list[dict[str, Any]] = []
            for depth, edge in walk:
```

- [ ] **Step 5: Join the two notes instead of one overwriting the other**

Replace:

```python
            if kinds in ("all", "identity") and not any(
                e["kind"] == "identity" for e in edges
            ):
                note = _identity_direction_note(
                    metric_name,
                    direction,
                    _oriented_identity,
                    # Nothing came back, so re-running the other way discards
                    # nothing.
                    suggest_rerun=not edges,
                )
                if note is not None:
                    payload["note"] = note
```

with:

```python
            # Both notes can apply at once -- a walk can return 200 edges none
            # of which are identity edges -- so they join rather than one
            # silently overwriting the other.
            notes: list[str] = []
            if total_edges > _MAX_TRACE_EDGES:
                notes.append(
                    f"The graph holds {total_edges} edges within depth "
                    f"{max_depth}; showing the {_MAX_TRACE_EDGES} nearest. "
                    f"Lower max_depth to see a complete result."
                )
            if kinds in ("all", "identity") and not walk_has_identity:
                note = _identity_direction_note(
                    metric_name,
                    direction,
                    _oriented_identity,
                    # Nothing came back, so re-running the other way discards
                    # nothing.
                    suggest_rerun=not edges,
                )
                if note is not None:
                    notes.append(note)
            if notes:
                payload["note"] = " ".join(notes)
```

- [ ] **Step 6: Run the full suite and linters**

Run: `uv run pytest -q` then `prek run --all-files`

Expected: all pass. Pay attention to any pre-existing `trace_metric_impacts` note test — if one asserts `payload["note"] == "<exact string>"`, it must still pass, because a lone identity note joins to itself unchanged.

- [ ] **Step 7: Commit**

```bash
git add src/agentic_data_contracts/tools/factory.py tests/test_tools/test_metric_impacts_tools.py
git commit -m "feat: cap what trace_metric_impacts serializes at 200 edges (#83)"
```

---

### Task 3: Documentation and version

**Files:**
- Modify: `docs/architecture.md:871`
- Modify: `CHANGELOG.md`
- Modify: `pyproject.toml`

**Interfaces:**
- Consumes: the behaviour shipped in Tasks 1 and 2.
- Produces: nothing code-level.

- [ ] **Step 1: Correct the architecture doc**

In `docs/architecture.md`, find the sentence inside the long v0.28.0+ metric-decomposition paragraph that reads:

```
It remains a node-visited BFS otherwise, so an edge onto a metric another branch already reached is not reported; widening that would also report cycle-closing edges, which is a separate contract decision.
```

Replace it with:

```
**Since v0.49.0 `visited` gates expansion only:** each reachable metric is expanded at most once, while every declared edge between reached metrics is reported — including one onto a metric another branch already reached. That is what lets a shared driver, a metric that is an operand of two different parents, arrive on both branches with the `operator` and `convention` only its second edge carries. Cycle-closing edges are reported as a consequence (`c -> a` in `a -> b -> c -> a`), which is correct: the edge is declared, and termination was never the reporting gate's job. Because the result is now O(E) rather than O(V), `trace_metric_impacts` serializes at most 200 edges and says so in `note`; the walk itself stays complete and uncapped.
```

- [ ] **Step 2: Add the CHANGELOG entry**

Insert directly above the `## [0.48.0]` heading in `CHANGELOG.md`:

```markdown
## [0.49.0] - 2026-08-29

### Fixed

- **`walk_metric_impacts` reports a shared driver's second edge** (#83). The walk marked a metric visited the first time any edge reached it and dropped every later edge onto it, so a metric driving two others was reported on whichever branch happened to be walked first — and the `operator` and `convention` carried only by its other edge never arrived. The loss was silent: no error, no warning, no `note`, and the result read as a complete walk.

  `visited` now gates *expansion* only, which is the sole thing it was ever needed for: each reachable metric is expanded at most once, while every declared edge between reached metrics is reported. This is not a new principle — `test_parallel_edges_to_one_neighbor_are_all_reported` already stated it in its own docstring; it had been applied within a single node's adjacency and is now applied across the walk.

  **Cycle-closing edges are now reported.** In `a -> b -> c -> a` walked downstream, `c -> a` appears. The edge is declared, and an agent tracing root cause should be told the graph closes; termination was never guaranteed by the reporting gate. The former `test_cycle_visited_tracking` asserted the old guarantee rather than its own intent and was rewritten to state the new one.

  The #81 value-equality rule is unchanged: two edges equal in every field carry one fact and report once; two edges differing in any field are two declarations and both report.

### Changed

- **`trace_metric_impacts` serializes at most 200 edges**, announced in `note`. Reporting grew from O(V) to O(E) within the depth horizon, and that output lands in an agent's context. The walk itself stays complete and uncapped — a graph primitive that silently truncates is the same class of loss this release fixes — so the cap sits in the tool, where the `max_depth` clamp already lives. This can truncate a response that was complete before: a star graph of one metric driving 100 others already returned 100 edges. No shipped semantic layer comes close; they report at most 5.
```

- [ ] **Step 3: Bump the version**

In `pyproject.toml`, change `version = "0.48.0"` to `version = "0.49.0"`.

- [ ] **Step 4: Verify**

Run: `uv run pytest -q` then `prek run --all-files`

Expected: all pass. Then confirm the doc claim is not stale:

```bash
grep -n "node-visited BFS otherwise" docs/architecture.md
```

Expected: no output — the old sentence is gone.

- [ ] **Step 5: Commit**

```bash
git add docs/architecture.md CHANGELOG.md pyproject.toml
git commit -m "docs: state the walk's new guarantee and bump to 0.49.0"
```
