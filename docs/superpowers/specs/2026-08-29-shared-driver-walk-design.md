# Reporting a shared driver's second edge

**Issue:** [#83](https://github.com/flyersworder/agentic-data-contracts/issues/83)
**Status:** design, awaiting review
**Date:** 2026-08-29

## The defect

`walk_metric_impacts` is a node-visited BFS. It marks a metric visited the
first time any edge reaches it, and every later edge onto that metric is
dropped before it can be reported. A **shared driver** — one metric that
drives two others — is therefore reported on whichever branch the walk
happened to reach first, and its other edge never arrives.

Reproduced against v0.48.0, no database or model required:

```python
metrics = [
    m("revenue",     [Decomposition(operator="sum", operands=["paying_users", "new_revenue"])]),
    m("new_revenue", [Decomposition(operator="product", operands=["paying_users", "conv"],
                                    convention="fold_into", convention_operand="conv")]),
    m("paying_users"), m("conv"),
]
index = build_metric_impact_index([e.as_driver_edge() for e in identity_edges_from_metrics(metrics)])
walk_metric_impacts(index, "revenue", direction="upstream", max_depth=3)
```

```
depth 1: paying_users -> revenue       op=sum      conv=None
depth 1: new_revenue  -> revenue       op=sum      conv=None
depth 2: conv         -> new_revenue   op=product  conv=fold_into
```

`paying_users -> new_revenue` is absent. It is declared, it carries
`operator="product"` and `convention="fold_into"`, and an agent tracing root
cause is never told it exists. `paying_users` was marked visited at depth 1 on
the `revenue` branch, so the edge from `new_revenue` to it is skipped before
reporting.

The loss is silent — no error, no warning, no `note`. The result reads as a
complete walk. This is the same failure shape as #81 and #60: content that
exists, is correctly declared, and does not arrive.

### Why it matters

A shared driver is not an edge case in a metric tree. "The same factor moves
both halves of the split" is precisely the finding a root-cause walk exists to
surface, and it is the one the walk cannot report. The agent sees a
decomposition tree where a real DAG was declared.

### Reachability today

No shipped semantic layer triggers this. `tests/fixtures/decomposition_source.yml`
does share an operand — `trial_conversions` appears twice — but both
occurrences are operands of the *same* parent (`new_revenue`), which is the
parallel-edge case #81 already fixed; both edges are verified to report. The
cross-branch case, where one metric is an operand of two *different* parents,
appears nowhere in `examples/` or `tests/fixtures/`.

The margin is one modelling decision wide. The moment an author declares
`paying_users` under two parents, this fires, silently.

## The change

`visited` stops gating *reporting* and gates only *expansion*, which is the
sole thing it was ever needed for.

```python
-  if neighbor in visited or any(edge == prior for prior in seen):
+  if any(edge == prior for prior in seen):
       continue
   result.append((depth + 1, edge))
   seen.append(edge)
-  reached[neighbor] = None
+  if neighbor not in visited:
+      reached[neighbor] = None
```

The value-equality dedupe from #81 stays exactly as it is: two edges equal in
every field carry one fact and report once; two edges differing in any field
are two declarations and both report.

This is not a new principle. `test_parallel_edges_to_one_neighbor_are_all_reported`
already states it in its own docstring:

> Visited tracking exists to stop cycles, so it must gate which nodes get
> *expanded*, not which edges get *reported*.

The codebase argued for this rule and then applied it only within a single
node's adjacency. This applies it across the walk.

### Verified consequences

Prototyped out of tree against v0.48.0:

| Case | Result |
|---|---|
| The #83 repro | `paying_users -> new_revenue` reported at depth 2 with `op=product conv=fold_into` |
| Cycle `a -> b -> c -> a`, depth 10 | terminates; reports `a->b`, `b->c`, `c->a`, each once |
| Self-loop `a -> a` | terminates; reported once |
| Any graph | no edge object reported twice |

## Decisions

### Cycle-closing edges are reported, not suppressed

In `a -> b -> c -> a` walked downstream, `c -> a` now appears. This is
correct: the edge is declared, and an agent tracing root cause should know the
graph closes a cycle. Termination is unaffected — it was never `visited`'s
reporting role that guaranteed it, only its expansion role.

`TestWalkMetricImpacts.test_cycle_visited_tracking` asserts today that each
target appears at most once and that the start is never a reported target.
Those two assertions encode the *old guarantee*, not the test's intent, which
is termination. The test is rewritten to state the new guarantee: the walk
terminates, and the cycle-closing edge is reported exactly once.

Inverting a live assertion is the change in this work most worth a reviewer's
attention.

### Duplicate suppression across endpoints is not needed

The issue raises this as an open question. It is moot.

`build_metric_impact_index` indexes each edge under both endpoints, but the
walk's direction filter binds each edge to exactly one expanding node: a
downstream walk examines `a -> b` only while expanding `a`, because expanding
`b` skips it on `edge.from_metric != current`. Since each node is expanded at
most once, each edge is examined at most once per walk. Verified by object
identity across the cycle and self-loop cases above.

The existing per-adjacency `seen` list therefore remains sufficient, and no
global reported-edge set is introduced.

### The payload cap lives in the tool, not the library

Reporting grows from O(V) to O(E) within the depth horizon. Measured on
complete graphs at `max_depth=10`:

| Metrics | Edges | Reported before | Reported after |
|---|---|---|---|
| 6 | 30 | 5 | 30 |
| 12 | 132 | 11 | 132 |
| 20 | 380 | 19 | 380 |

Real metric graphs are sparse DAGs. Measured across the shipped layers:
`growth_agent` 6 metrics / 5 edges, `revenue_agent` 4 / 3, `ops_agent` 4 / 2.
So the dense case is theoretical. But the growth is
unbounded relative to today, and `trace_metric_impacts` output is serialized
into an agent's context.

`walk_metric_impacts` stays complete and uncapped: a graph primitive that
silently truncates is the same class of silent loss this issue is about, and
every non-tool caller would inherit a limit it never asked for.

`trace_metric_impacts` caps what it serializes at **200 edges**. BFS order
means truncation drops the deepest edges, which is the right end to lose.

**The cap can truncate a response that is complete today, and that is a
behaviour change independent of this fix.** A star graph — one metric driving
100 others — already returns 100 edges at `max_depth=2` on v0.48.0; a
50-edge cap would have halved it. 200 clears any such shape by a wide margin
while still bounding the dense case the O(E) growth introduces (a complete
20-metric graph reports 380). Shipped layers report at most 5 edges, so no
example is affected.

This is where the boundary already sits: `max_depth` is clamped in the tool
(`max(1, min(int(args.get("max_depth", 2)), 10))`), not in the walk.

### Cost, and why the algorithm does not change

BFS is already the bound. The problem is single-source bounded-depth traversal
that reports edges, and no edge can be reported without being touched, so
O(V+E) is optimal. Measured, complete graphs at `max_depth=10`:

| Metrics | Edges | Reported before | after | before | after |
|---|---|---|---|---|---|
| 10 | 90 | 9 | 90 | 0.02ms | 0.04ms |
| 20 | 380 | 19 | 380 | 0.03ms | 0.27ms |
| 40 | 1560 | 39 | 1560 | 0.11ms | 1.88ms |

The growth is superlinear, and the cause is worth recording because it is a
second-order effect of this change rather than of the graph. Today's guard
reads `if neighbor in visited or any(edge == prior for prior in seen)`, and
Python short-circuits `or`: when a neighbour is already visited, the
value-dedupe scan never runs. Removing the `visited` term makes that scan
unconditional, so a node whose neighbours are mostly already visited goes from
O(k) to O(k²) dataclass comparisons across its adjacency.

**Not worth fixing.** The obvious remedy — replacing the list scan with a set
of hashable field tuples — was measured and is *slower* below roughly 40
metrics, because `astuple` costs more than scanning a short list. It only wins
at 80 metrics / 6320 edges, where it halves 14.8ms to 7.4ms. The crossover is
far past any realistic metric graph, the worst case constructible is
milliseconds, and the change would trade a rule that reads simply for a faster
one nobody needs.

A graph library does not help either. `networkx.bfs_edges` yields *tree edges
only*, which is precisely the defect this spec fixes; obtaining non-tree edges
means iterating `G.edges(nbunch)` and writing this same loop by hand, with a
heavy dependency added to a project that keeps its floors deliberately small.
`MultiDiGraph` also does not carry #81's parallel-edge semantics, where two
identical declarations are one fact and two differing ones are two.

A cleaner formulation does exist — compute reachability within the depth
horizon, then report every declared edge induced on that node set, which makes
"expand once, report all" structural instead of a subtlety inside one loop. It
is the same complexity, a substantially larger diff, and it changes edge
ordering, which the truncation cap depends on being BFS-nearest-first. Not
taken.

### Two notes can fire at once

`payload["note"]` is a single string, already used by the identity-direction
note added in v0.46.0. A truncation note can coexist with it — a walk can
return two hundred edges none of which are identity edges. The two join with a
space rather than one silently overwriting the other.

## Scope

Changed:

- `semantic/base.py` — `walk_metric_impacts` loop and docstring
- `tools/factory.py` — edge cap, truncation note, note joining
- `docs/architecture.md` — the walk guarantee
- `tests/test_semantic/test_metric_impacts.py` — cycle test rewritten, shared-driver tests added
- `tests/test_tools/` — cap and note coverage

Not changed:

- `build_metric_impact_index` — the index is already correct
- `MetricEdge` / `IdentityEdge` / `MetricImpact` — no shape change
- Edge kinds, tool arguments, or the response schema beyond `note` content

## Testing

TDD, red first.

1. The #83 repro as a test: the shared driver's second edge is reported with
   its operator and convention. Fails on v0.48.0.
2. Cycle test rewritten: terminates, and `c -> a` is reported exactly once.
3. Self-loop: reported once, terminates.
4. #81's parallel-edge behaviour unchanged: byte-identical declarations
   collapse to one; declarations differing in any field both report.
5. Tool: a graph exceeding the cap returns 200 edges and a note naming the
   total; a graph under it returns no truncation note. The boundary is pinned
   at exactly 200 and at 201, so an off-by-one cannot pass.
6. Tool: truncation and identity-direction notes both present when both apply.

## Versioning

Minor — `0.49.0`.

`walk_metric_impacts` reports strictly more than before; no caller of the
library function loses information.

`trace_metric_impacts` is a different matter and the spec should not soften
it: the 200-edge cap is a new limit that *can* truncate a response which is
complete on v0.48.0, on any graph reporting more than 200 edges within the
depth horizon. No shipped layer comes close, the truncation is announced in
`note` rather than silent, and the alternative — an unbounded serialization
into an agent's context — is the worse failure. Minor is the right label for a
0.x library on that trade, but it is a trade, not a no-op.
