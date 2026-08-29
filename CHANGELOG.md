# Changelog

All notable changes to this project will be documented in this file.

## [0.49.0] - 2026-08-29

### Fixed

- **`walk_metric_impacts` reports a shared driver's second edge** (#83). The walk marked a metric visited the first time any edge reached it and dropped every later edge onto it, so a metric driving two others was reported on whichever branch happened to be walked first — and the `operator` and `convention` carried only by its other edge never arrived. The loss was silent: no error, no warning, no `note`, and the result read as a complete walk.

  `visited` now gates *expansion* only, which is the sole thing it was ever needed for: each reachable metric is expanded at most once, while every declared edge between reached metrics within the depth horizon is reported. (A node reached at exactly `max_depth` is never expanded, so its outgoing edges are not reported even when both endpoints were reached.) This is not a new principle — `test_parallel_edges_to_one_neighbor_are_all_reported` already stated it in its own docstring; it had been applied within a single node's adjacency and is now applied across the walk.

  **Cycle-closing edges are now reported.** In `a -> b -> c -> a` walked downstream, `c -> a` appears. The edge is declared, and an agent tracing root cause should be told the graph closes; termination was never guaranteed by the reporting gate. The former `test_cycle_visited_tracking` asserted the old guarantee rather than its own intent and was rewritten to state the new one.

  The #81 value-equality rule is unchanged: two edges equal in every field carry one fact and report once; two edges differing in any field are two declarations and both report.

### Changed

- **`trace_metric_impacts` serializes at most 200 edges**, announced in `note`. Reporting grew from O(V) to O(E) within the depth horizon, and that output lands in an agent's context. The walk itself stays complete and uncapped — a graph primitive that silently truncates is the same class of loss this release fixes — so the cap sits in the tool, where the `max_depth` clamp already lives. This can truncate a response that was complete before: a star graph of one metric driving 100 others already returned 100 edges. No shipped semantic layer comes close; they report at most 5.

## [0.48.0] - 2026-08-29

### Added

- **`evaluate_conformance` can grade a certified breakdown** (#85). Pass 3 scores two orthogonal axes, and for a row certified with `expected_rows` only the protocol axis was ever checked: whether the agent followed the governed path was graded in full, while whether it got the right *numbers* reported `answer="skipped"`. The failure that slipped through is a contract that drifts without breaking — a metric description trimmed so it no longer says which column identifies a region — leaving an agent that followed every rule to return a right-shaped, wrong-grouped answer against a green gate.

  `Attempt` gains `final_rows` / `final_columns`, the breakdown counterpart to the existing `final_answer`. As with `final_answer`, the host declares what the agent answered rather than the library inferring it: `_select_answer` picks a scalar by clustering candidates and marks the guess when it is ambiguous, and there is no equally honest inference for a breakdown — choosing whichever query happened to match would let a lucky drill-down pass. Both fields are also accepted by `Attempt.from_session`.

  Grading delegates to the same `compare_rows` that pass 2 uses, so a breakdown is scored identically in both passes — with tolerance, and naming which group differed. `ConformanceResult` gains `row_differences` and `actual_row_count`, mirroring the pair `ExampleAnswerResult` gained in 0.47.0. A fault no pairing can resolve becomes `answer="error"` rather than propagating, since `evaluate_conformance` is pure and has no batch guard to convert it.

  **Not breaking.** A host that has not wired `final_rows` sees exactly the verdict it saw before: an undeclared breakdown still reports `skipped` and still passes. The guard order differs from the scalar path's for that reason — there, the relative-time check precedes the missing-scalar check, because a window that returned NULL is still a decaying window; here the undeclared case wins first, because with no declared answer there is nothing for a window to decay.

  A declared breakdown that reached no successful `run_query` reports `protocol="contaminated"`, the same verdict a declared scalar already earned: pass 3 asks whether the answer came *through the governed path*, and one that never queried did not. Declaring `final_rows` on a row certified with a scalar `expected` is ignored rather than honoured — a host may reasonably wire it uniformly, since an agent's result is a table for every question, and diverting a scalar row into the declared branch would bypass selection and report `error` for a good answer.

  `final_rows` without `final_columns` is refused at construction, and so is a row whose width disagrees with `final_columns`: the column names are what `compare_rows` uses to check the result's width and to name the column a difference is in, so rows alone are incomplete rather than merely inert. A row whose width disagrees with the certified answer is refused by `compare_rows` itself, beside its existing column-count check, so both passes convert it into a per-row verdict rather than letting an `IndexError` escape an `evaluate_conformance` that is total over its attempts. Declared rows are also copied at construction: a one-shot iterable would otherwise be read twice and report every certified group as missing on data that was correct.

## [0.47.0] - 2026-08-29

### Added

- **`expected_rows`: a certified answer for a breakdown, not just a scalar** (#82). `expected` is a single `float`, so a corpus row whose answer is a `GROUP BY` — revenue by region, a top-N list — had nowhere to put its answer and was silently unasserted by `check_example_answers`, contract-compliant or not. `expected_rows` is `expected`'s sibling for exactly that shape: a `list[list[Any]]`, one inner list per certified row, and a row declares one assertion or the other, never both.

  Row identity is *inferred* from the cells themselves, not declared: a numeric cell is a value (the measurement) and so is a `null` — a group whose measurement is absent is still a measurement, and a row cannot identify itself by a cell holding nothing — while every other cell is a key (the group). This is the opposite of `convention`, which must be declared because which factor absorbs a cross term isn't derivable from the schema, whereas a row's key is derivable at runtime from what the query actually returned.

  Comparison is **unordered by default** — a `GROUP BY` without an `ORDER BY` has no guaranteed row order, so a correct query never fails on the order it happens to come back in — and pairs rows by key. Set `ordered: true` when order *is* the answer (a top-N list); comparison then pairs rows by position instead, and a row-count difference is reported as a count rather than as named missing groups, since position (not key) is identity there.

  A comparison reports three named difference kinds rather than a bare pass/fail: **missing group** (a certified key absent from the result), **unexpected group** (a result key absent from `expected_rows`), and **value mismatch** (the key matched, the number didn't, reported with its diff). Value mismatches are listed first, because a caller that renders only the first few differences still reports the group and row counts beside them — so a group-set difference is signalled even when it goes unnamed, while a wrong number has no other signal. It refuses outright, raising `ValueError`, on three faults rather than guessing at them: a column-count mismatch between `expected_rows` and the result, a duplicated group in the result (no pairing can resolve which one is certified), and a non-numeric cell at a value position (the certified answer says the column is a measurement and the query returned something else). `check_example_answers`' batch guard converts that raise into a result, so a caller never sees the `ValueError` itself — the row is reported with `status="error"`, distinct from a `mismatch`, naming the fault in `reason`. A fourth case — every cell numeric, so no key exists and every row collides — is caught at load time, before any adapter is touched, naming `ordered: true` as the fix.

  `expected_rows: []` is rejected: "this query returns no rows" is a real certified answer, but it's already expressible as `SELECT COUNT(*) ... expected: 0`, which fails more usefully (naming *how many* rows came back, not just that some did) and leaves the empty list as unambiguous evidence of a mistake — a truncated file, a templating bug, a key never filled in.

  `evaluate_conformance` is unchanged and still reports `answer="skipped"` for a breakdown row; grading a breakdown there needs the recorder to carry more than the scalar/row_count/relative_time it holds today, and is deferred to #85 as its own design rather than a side effect of closing this gap.

## [0.46.0] - 2026-08-29

### Fixed

- **`trace_metric_impacts` sent a root-cause walk to the empty side of the graph** (#81). Identity edges are canonically `parent -> operand`; influence edges are `driver -> affected`. Those are opposite orientations for the same real relationship, so the one graph holding both could not answer "what drives this metric" by topology — and the tool's own `kinds` description ("walk 'identity' first to localize the change") composed with its `upstream` default into a call that returned `{"edges": []}`. No error, no warning, and the `convention` an attribution needs never arrived. Identity edges now enter the walk re-pointed operand -> parent via the new `IdentityEdge.as_driver_edge()`, so `direction` means the same thing for both kinds: `upstream` is drivers, `downstream` is what the metric feeds, and every returned edge points from a driver to what it affects. The canonical orientation is unchanged — `lookup_metric` and `reconcile_decomposition` read it as before.

  **Breaking, for callers of this tool only.** A walk that asked `direction="downstream", kinds="identity"` for a metric's operands must now ask `"upstream"`. An identity walk that comes back empty while edges exist on the other side now carries a `note` naming that direction and how many metrics are there, so the migration reports itself instead of returning a bare `[]`. The note only suggests re-running the other way when the walk returned nothing at all — a `kinds="all"` walk that found influence edges but no identity ones is told where the identity edges are, not to go and lose what it has.

- **A pair declared twice reached the agent once.** `walk_metric_impacts` marked a neighbour visited on the first edge that reached it, so with both kinds now pointing the same way, a metric declared as both an impact edge and a decomposition operand (as `examples/revenue_agent/` declares `active_customers`) surfaced only whichever edge was indexed first — dropping the `operator` and `convention` that only the identity edge carries. Visited tracking now gates which nodes are *expanded*, not which edges are *reported*: each node is still walked once, cycles are still safe, and *parallel* edges — several found while expanding one node onto the same neighbour — are all returned. Edges equal in every field are the exception and report once, since identical declarations carry one fact.

### Known limitation

- **A shared driver across branches is still dropped** (#83). If `a` drives both `b` and `c` and `b` is reached first, the edge `a -> c` is never reported, taking its `operator` and `convention` with it — the same silence this release fixes, one hop deeper. The fix above covers parallel edges within a single node's adjacency, which is what the mixed influence + identity graph needed; the cross-branch case is the node-visited BFS `walk_metric_impacts` has always been, and closing it changes a contract rather than fixing a bug (cycle-closing edges would start being reported, and the walk's "each reachable metric appears at most once" guarantee would go). Tracked in #83 rather than folded in here. The docstring and `docs/architecture.md` state the limit rather than implying otherwise.

### Internal

- **The `examples` CI job now diffs each example's whole stdout against a committed golden file** (`examples/*/expected_output.txt`, regenerated by `scripts/regen_examples.sh`) instead of grepping for markers. The old gate asserted 5 markers across three examples that print 27 sections, and it stayed green while this release left `growth_agent`'s identity section printing `{"edges": []}` under a heading promising a decomposition. Per-section markers do not scale — one judgment call per section, one forgotten marker per section added — whereas a snapshot covers a new section the moment it is regenerated, catches the empty-payload case a heading grep sails past, and puts the change in front of a PR reviewer rather than only in a CI log.

- **The three example demo queries gained an `ORDER BY`.** All of them relied on `GROUP BY` alone for row order, and `revenue_agent`'s rows genuinely reordered between runs — latent flakiness the marker greps never surfaced, and a blocker for diffing output. Fixed at the source: a `GROUP BY` whose rows an agent reads should be ordered anyway.

## [0.45.0] - 2026-08-24

### Added

- **A third pass over the verified-examples corpus: does the contract still *teach*?** `validate_examples` asks whether certified SQL is still allowed and plannable; `check_example_answers` whether it still returns the right number. Both check a query a human already got right, and neither involves an agent — so neither can see a contract that stays enforceable and accurate while quietly ceasing to be usable. Rename a metric, trim a domain description, drop the sentence that said which order status counts as revenue: enforcement is untouched, the certified SQL still returns the same number, both passes stay green, and an agent reading that contract can no longer find its way to the query. `evaluate_conformance(attempts)` closes that gap by asking whether an agent can reproduce the certified answer *from the contract alone, through the governed path*. New public names, all exported from `agentic_data_contracts.validation`: `ToolCall`, `ToolRecorder`, `Attempt`, `ConformanceResult`, `ConformanceReport`, `evaluate_conformance`.

  ```python
  attempts = []
  for example in corpus:                      # rows carrying a `question`
      session = ContractSession(contract, recorder=ToolRecorder())
      tools = {t.name: t.callable for t in create_tools(contract, adapter=adapter,
                                                        semantic_source=semantic,
                                                        session=session,
                                                        caller_principal=example.principal)}
      final_text = await your_agent_loop(example.question, tools)
      attempts.append(Attempt.from_session(example, session, final_text=final_text))

  conformance = evaluate_conformance(attempts)
  if not (report.ok and answers.ok and conformance.ok):
      sys.exit(1)
  ```

- **The library never runs the agent, and the evaluator is pure.** Everything expensive, credentialed and nondeterministic stays in the consumer's own loop, above the call; `evaluate_conformance` scores recorded `Attempt`s with no network, no database and no model. That is what makes every verdict rule testable without an API key and a past run re-scorable from what was recorded. It also draws the boundary the corpus features already draw: the corpus stays yours, the agent loop stays yours, and the library contributes one verb.

- **Two orthogonal axes, and a `ToolRecorder` that refuses reuse.** Each attempt lands on an `answer` status (`match` / `mismatch` / `unassertable` / `skipped` / `error`) and, independently, a `protocol` status (`followed` / `violated` / `contaminated` / `not_applicable` / `unchecked`), with `answer_source` (`declared` / `sole_scalar` / `last_scalar` / `none`) recording *how* the answered number was selected rather than fusing that into the verdict — a row that matched on an ambiguous `last_scalar` still reports `answer="match"` and is still excluded from `ok`. `ToolRecorder.consume()` hands over its call log exactly once and raises on a second read: a recorder reused across two questions would merge their call logs and produce confidently wrong protocol verdicts, and a loud error beats a quiet wrong answer.

- **Nothing-to-judge passes; couldn't-judge fails.** `skipped` (the row certified no `expected`) and `not_applicable` (the row activated no protocol rule) pass the gate, because no assertion was made and nothing can be held against the contract. `error` and `unchecked` fail, because something was meant to be judged and the evaluation could not do it. Conflating the two is the classic evaluation bug — a suite reporting green because every case was quietly skipped — so `ConformanceReport.ok` also refuses an **empty** report for the same reason `ExampleAnswerReport.ok` does: a harness that stopped producing attempts fails rather than passes. One consequence that reads as a bug and is not: a row whose SQL the contract *blocked* is counted among the passes, because it carries no certified answer and activates no protocol rule, so pass 3 has nothing to judge. Legality is pass 1's job, and that row already failed there; the blocked attempt still surfaces in `reasons` as friction, which is diagnostic signal about how hard the contract was to obey, not a verdict.

- **`VerifiedExample.expects_metrics`** — an optional list of metric names that should have been consulted before the answering query. It is what *activates* the protocol rule: `lookup_metric` must have resolved each name, successfully, before the `run_query` that produced the answer. Rows naming nothing land on `not_applicable` and pass, deliberately. The output of this pass is evidence about the contract's *prose* — a `violated` row is an argument for rewriting a definition — so a guessed violation would become wrongly-rewritten contract text, which is worse than the finding never existing. The rule fires only where a human said in the corpus what the right path was. `expects_metrics` joins `expected` / `rel_tol` / `abs_tol` / `time_scoped` as a *recognised* corpus key; a corpus already using that name for something else should rename it before upgrading, since `from_dict` no longer carries it through to `.metadata` untouched.

- **The closed world is enforced by derived evidence, never by assertion.** An agent in production may hold tools this library never created — a generic SQL tool, a warehouse MCP server, a shell, a retriever — which the recorder cannot see, making "never called `lookup_metric`" ambiguous between a real prose gap and the agent going around the contract. The evaluator assumes the eval runs the agent with only the contract toolset, but does not ask the consumer to *promise* that: an answer declared with zero successful `run_query` calls proves by construction that the number came from outside the governed path and is marked `contaminated`. Consumers holding full framework logs may populate `Attempt.foreign_tool_calls`, which is used *only* to mark a row `contaminated` and never as input to any other verdict, keeping arbitrary trace formats out of the reasoning path. Documented limit, stated as such: an agent that used `run_query` but drew its business context from a foreign retriever leaves no detectable trace, so findings are trustworthy in proportion to how closed the eval world actually was.

- **The recorder never retains result rows.** A `ToolCall` carries `scalar` and `row_count`, not the result set: a conformance report is the kind of artifact that gets committed or posted as a PR comment, and one containing warehouse rows would leak data straight out of the boundary this library exists to defend. Query *arguments* are retained, since SQL is schema rather than data, but `summary()` never prints raw SQL — it stays available programmatically on the record.

- **`ConformanceReport.by_example()` and `pass_rate()`, for a nondeterministic pass.** Pass 3 differs in kind from the first two: it runs an agent, so it costs money per invocation, needs a credential, takes minutes, and can pass and fail on consecutive runs of the same contract from sampling alone. Wired as a hard all-must-match per-PR gate it will flake, and a flaky gate gets disabled — which costs the signal entirely. The README recommends passes 1 and 2 hard on every PR, and pass 3 path-filtered on contract / semantic YAML changes plus nightly, gated on `pass_rate()` over repeats or advisory with `summary()` posted as a comment. `by_example()` groups repeats of the same question, keyed on `id` falling back to `question` and never on the positional `label`, so a verdict can be a measurement rather than a single sample.

- **Known limitation on the SDK and middleware paths.** `create_sdk_mcp_server` and the `contract_middleware` decorator both check `session.check_limits()` and return the canonical blocked envelope *without* calling the inner tool closure — which is where the recorder lives. So a session budget exhausted on either of those two paths records **no tool call at all**: the attempt's call log is short for a reason nothing in it explains, and if the consumer also declares a `final_answer`, the row is attributed `contaminated`, implying the agent went around the contract when it was in fact cut off by its own budget. On those paths, pass `error=` to `Attempt.from_session` when a budget block is observed; the attempt then scores `error` / `unchecked` and fails honestly instead of being mis-diagnosed. The direct `create_tools` path is unaffected.

### Changed

- **`ContractSession` takes a keyword-only `recorder`.** `ContractSession(contract, *, recorder: ToolRecorder | None = None)`, defaulting to `None`, so every existing call site is unchanged and no session records anything unless a recorder is handed to it — an eval-time facility that costs production nothing. `Attempt.from_session` raises a `ValueError` naming the fix when handed a session built without one, rather than silently producing an attempt with an empty call log that would score as `contaminated`. One qualification: this is neutral for *recording*, not byte-for-byte for *every* payload — `_error_response` now always emits a `_kind` key, recorder or not, and it rides into the MCP payload on the `create_sdk_mcp_server` path. It is additive and harmless (the LangChain and Pydantic AI wrappers read only `content`, following the existing `is_error` precedent), but a strict "nothing changed" reading would be wrong.

- **`ValidationResult` gains `relative_time`.** The validator already parses every query it gates and the relative-time scan already exists for pass 2, so the node name (`CurrentDate`, `Now`, ...) is now carried out on the result instead of being recomputed. The recorder passes it through to `ToolCall.relative_time`, which is what lets pass 3 mark a row `unassertable` when the *agent's own* answering query used a decaying window — the same refusal `check_example_answers` applies to certified SQL, applied to SQL the agent wrote. Purely additive: the field defaults to `None` and no existing consumer reads it.

- **Corpus readiness is the first step of adoption.** `VerifiedExample.question` has always been optional and documented as non-load-bearing, so most existing corpora leave it empty. Pass 3 can only evaluate a row that carries a question, and the protocol rule only judges one that carries `expects_metrics`. Rows without a question are skipped and counted, never errors — an unevaluatable corpus stays visible instead of silently shrinking the run.

### Internal

- **Two extractions, both non-breaking.** The scalar rule — empty result, NULL, non-finite, non-scalar — moved into `validation/_scalar.py` and the pure half was split out as `_scalar_value(columns, rows, label)`, so the `run_query` recorder can apply *identical* semantics to a result it already holds without re-executing the query; `reconcile_decomposition` and `check_example_answers` keep calling the executing `_scalar` on top of it. The relative-time scan moved into `validation/_timewindow.py` so the `Validator` can reach it without importing `validation.examples`, which would close an import cycle through the adapters. Both are private module moves with no public-name or behaviour change; the `sqlglot>=28.6` floor comment in `pyproject.toml` now names the module the tuple actually lives in.

## [0.44.1] - 2026-08-22

### Changed

- **The `[agent-sdk]` extra now resolves `mcp` 2.0, and the MCP annotation is built in the one form valid across the 1.x/2.0 rename.** claude-agent-sdk 0.2.144 widened its own constraint from `mcp<2.0.0,>=1.23.0` to `mcp<3.0.0,>=1.23.0`, and this extra's `mcp>=1.23.0` is unbounded above, so a fresh install picks 2.0 up with no edit here — which is exactly what declining to duplicate someone else's ceiling was for. mcp 2.0 moves `mcp.types` into a separate `mcp-types` distribution and renames the model attributes to snake_case (`readOnlyHint` -> `read_only_hint`) while keeping the camelCase alias the protocol actually sends. `_annotations_for` therefore stops passing a camelCase *kwarg*: that spelling still works on both versions, but only by way of alias validation, so it reads — and to a type checker *is* — an argument silently discarded. It now builds from the wire representation instead, `ToolAnnotations.model_validate({"readOnlyHint": True})`, which resolves to the field name on 1.x and to the validation alias on 2.0. The emitted annotation is byte-identical either way; the point is that both ends of the declared range stay honest, since `test-lowest-floors` resolves 1.23 while the lockfile now carries 2.0. No floor moves: mcp 2.0 requires `pydantic>=2.12`, but the `pydantic>=2.11` floor stays true because the lowest-direct resolve takes mcp 1.23 with it.

### Internal

- **Dependency refresh across both tracked lockfiles.** The root lock takes 24 updates, the notable majors being `anthropic` 0.122.0 -> 1.0.0 and `mcp` 1.29.0 -> 2.0.0, alongside `pydantic-ai-slim` 2.33.0, `langchain-core` 1.6.0, `claude-agent-sdk` 0.2.144 and `websockets` 16.1.1. `experiments/mermaid-joinpath-eval` keeps its own lockfile and is upgraded separately: `openai` 2.41.0 -> 3.3.1, whose v3 surface still carries everything `ModelClient` uses (`chat.completions.create` with `max_tokens`, `.choices[0].message.content`, `.usage.prompt_tokens` / `.completion_tokens`).

  Worth recording because the name invites the opposite conclusion: `anthropic` 1.0 and `openai` 3.0 both moved to **`httpx2`, which is a separate distribution and top-level module, not httpx 2.x**. It installs alongside the `httpx<1.0.0` that `langchain-core` still requires rather than competing with it, so the migration is additive at the resolver and invisible here — `src/` imports neither `httpx` nor any vendor client, and `mcp.types` is its only third-party runtime surface outside sqlglot, pydantic and pyyaml. The experiments env is the clean case: with `openai` 3.x, `httpx`, `httpcore`, `distro` and `tqdm` leave that lock entirely.

- **Pre-commit hooks bumped**: ruff `v0.16.3` -> `v0.16.4`, ty `v0.0.72` -> `v0.0.73`. The ty bump is what surfaced the discarded-kwarg diagnostic above.

## [0.44.0] - 2026-08-22

### Added

- **Expected-value assertions for the verified-examples corpus.** `validate_examples` re-checks each example's SQL against the same `Validator` that gates live agent queries — allowed tables, tenant filter, no `SELECT *`, and (with an `ExplainAdapter`) a live schema dry-run — and reports whether the SQL *complies*. It cannot report whether the SQL is *right*. A query that satisfies every one of those rules and still sums the wrong rows passes today with `status: "valid"`; nothing downstream can tell it apart from a correct one. That is the gap this closes: `VerifiedExample` gains four optional fields (`expected`, `rel_tol`, `abs_tol`, `time_scoped`), and a new second pass, `check_example_answers(report, *, adapter, ...)`, executes the compliant, asserted rows and compares the live result against the certified answer within tolerance, yielding `match` / `mismatch` / `unassertable` / `error` per row.

  ```python
  report = validate_examples(examples, contract, explain_adapter=adapter)
  answers = check_example_answers(report, adapter=adapter)
  if not (report.ok and answers.ok):
      sys.exit(1)
  ```

- **The checker consumes the validation *report*, not raw examples — the load-bearing choice in the design.** An example that failed contract validation must never be executed: a row that violates the tenant-filter rule is precisely the query that must not be run against a warehouse to see what it returns. Taking `ExampleValidationReport` as the input makes that ordering a property of the function signature rather than a rule stated in a docstring — no ordinary call shape hands it unvalidated SQL. A row is executed only when it is `status == "valid"` **and** declares an `expected`; everything else produces no result. This also keeps `validate_examples`'s own invariant intact: it still only plans (via `ExplainAdapter`) and never executes. The execute-capable `DatabaseAdapter` enters the pipeline only at this second, already-filtered stage.

- **A relative time window refuses the row rather than running it.** `WHERE created_at >= CURRENT_DATE - 30` is correct today and wrong in a month, for no reason the corpus author did anything about — an expected value pinned against that SQL decays on its own. The checker scans the parsed SQL before executing anything and marks a hit `unassertable`, never running the query. The scan needs two arms: sqlglot only normalises a spelling to a typed AST node (`exp.CurrentDate`, `exp.CurrentTimestamp`, ...) in the dialects that own it. `NOW()` becomes `CurrentTimestamp` under postgres but stays an untyped `exp.Anonymous` call under duckdb, mysql, snowflake, bigquery, tsql, and oracle; `GETDATE()` is typed only under tsql and snowflake; `TODAY()` only under duckdb. A typed-node-only scan would therefore miss `NOW()` — the single most common relative spelling — under the dialects most likely to be running it. The second arm matches `exp.Anonymous` calls (`now`, `getdate`, `sysdate`, `today`, `curdate`, and others) by name, when the arguments look like a clock read: none (`NOW()`), or a single integer literal, which is a fractional-seconds precision spec (`NOW(3)`, `SYSDATE(6)`). Both halves of that are load-bearing: matching a *call* means a column named `now_flag` is never flagged, and keying on the argument's *kind* rather than its mere presence means the deterministic `UNIX_TIMESTAMP(created_at)` conversion — whose argument is a column — is not refused alongside the clock reads that share its name, while `NOW(3)` still is refused. Arity alone would get one of those two wrong, and `NOW` / `SYSDATE` are precisely the names with no typed-node arm to fall back on. Setting `time_scoped: true` on the example tells the checker the window is pinned some other way and clears the refusal.

- **The comparison tolerance is anchored on `expected`, not on the larger magnitude the way `math.isclose` anchors on `max(|a|, |b|)`.** An assertion has a privileged side: the certified answer is the fixed point and the query result is what varies against it, unlike `reconcile_decomposition`'s two-measurements case which has no privileged side and anchors on the parent it measured instead. Anchoring on `expected` keeps "within 0.1% of the certified number" meaning the same thing regardless of how far the query result has drifted. The default (`rel_tol=1e-9`, `abs_tol=0.0`) is deliberately far tighter than `reconcile_decomposition`'s `1e-4`: a decomposition identity is approximate by construction (operands carried at limited precision leave a real residual), but a certified answer is meant to be *the* number — the default tolerates floating-point representation noise and nothing else. An answer certified off a dashboard that rounds to whole dollars will not match a full-precision `SUM` at this default; the author sets an explicit per-example `rel_tol` / `abs_tol` matching the answer's actual precision. `rel_diff` is guarded against a zero `expected` the same three-branch way `reconcile_decomposition` guards a zero parent, so `expected: 0.0` never raises — but the guard has a visible consequence: the relative term vanishes at zero, so a zero-valued assertion matches only an exact zero unless the author also sets an `abs_tol`.

### Changed

- **The `sqlglot` floor rises from `>=28.0` to `>=28.6`.** The relative-time scan names `exp.Localtime`, `exp.Localtimestamp` and `exp.Systimestamp` in a module-level tuple, and those three node types first exist in 28.6 — so on 28.0 through 28.5 this release does not import at all (`AttributeError` at `import agentic_data_contracts`, the same failure mode the `sqlglot>=23.0` floor once had for `exp.Revoke`). The tuple is deliberately not built with `getattr(exp, ..., None)` filtering: below 28.6 a bare `LOCALTIMESTAMP` parses as an `exp.Column`, and the name arm matches only function *calls* — so a filtered tuple would not fall back to the other arm, it would silently execute a row whose certified answer is pinned to a decaying window. A floor that fails loudly beats a scan that quietly checks less in one environment than another.

- **Four corpus keys stop being free-form.** `VerifiedExample.from_dict` preserves any key it does not recognise under `.metadata`, untouched — that is a documented feature of the corpus format, and consumers rely on it for `type`, `verified_by`, `last_verified` and whatever else their review flow records. `expected`, `rel_tol`, `abs_tol` and `time_scoped` are now *recognised*, so a corpus already using one of those names for its own purpose changes behaviour on upgrade rather than carrying through inertly. Two shapes to check before bumping: a **non-numeric** value (`expected: "one row per region"` is a plausible note in a question-to-SQL corpus) now raises `ValueError` out of `from_dict`, which will abort the load of the whole corpus rather than that one row; and a **numeric** value that meant something else now reads as a certified answer, so `check_example_answers` will execute that row's SQL against the warehouse and report a `mismatch` against a number that was never an answer. Neither is destructive — the second pass only ever reads — but both are visible, and renaming the offending key (to `expected_shape`, say) before upgrading avoids them. A corpus using none of the four names is unaffected.

## [0.43.1] - 2026-08-18

### Fixed

- **`attribute_change` validates operator and arity, like the sibling it mirrors.** It is public, accepts an arbitrary `MetricDefinition`, and its own comments state it cannot assume the object came through `validate_decompositions` at load — but it re-validated only the *convention*. `reconcile_decomposition` validates operator **and** arity up front for exactly that reason, before running a single query. Without the same block, malformed decompositions surfaced as interpreter errors from inside the arithmetic rather than the `ValueError` the module commits to: a 1-operand `ratio` raised `IndexError` from the zero-denominator guard's `operands[1]`, a 3-operand `ratio` or `difference` raised `too many values to unpack` naming nothing about the contract, and an operand-less `product` raised `ZeroDivisionError` from inside `_place`. Unreachable for any contract-loaded metric, since `validate_decompositions` rejects all four shapes at load — but the asymmetry between two kernels with the same stated precondition posture is the kind that gets copied. ([#72](https://github.com/flyersworder/agentic-data-contracts/issues/72))

### Internal

- **The convention-default helpers moved to `semantic/base.py`.** `OssieSource` had been importing `_parse_convention_default` / `_apply_convention_default` — two *private* functions — across from `yaml_source.py`, while every other helper shared between sources (`validate_decompositions`, `validate_drill_by`, `parse_review_date`, `jsonify_extras`, `build_relationship_index`) lives in `base.py`. The giveaway was that both had to import `VALID_CONVENTIONS` and `_CROSS_TERM_OPERATORS` back out of `base.py` to do their work, and that those two constants had no other consumer in `yaml_source.py` — the vocabulary they enforce already lived where the functions belonged. `parse_review_date`'s own docstring states the principle: a second copy is a second chance to get the rule wrong, and both sources resolve a default onto the same `Decomposition.convention` field. No behavior change; both source paths keep their existing tests. ([#74](https://github.com/flyersworder/agentic-data-contracts/issues/74))
- **The pinned round-trip digest now covers metric serialization.** `_PINNED_ROUNDTRIP_DIGEST` is the repo's broadest guard against canonical-bytes drift, but it was computed over a semantic source with no `metrics:` key at all, so `_dump_metric` was never called and it could not see any change to metric serialization. That is worse than a missing test, because the assertion passed either way: every optional field added to `_dump_metric` since the pin was written could have been made unconditional — moving the digest of every real contract and invalidating every published ARD attestation — with the test still green. The fixture now carries a metric exercising every optional field the dump writes plus two leaves for the omit-when-empty branches, verified by mutation (removing the `if m.decompositions:` guard now fails the pin), and a companion test asserts the pinned contract's canonical bytes still carry those fields so the coverage cannot be silently narrowed again. No shipped code changed. ([#73](https://github.com/flyersworder/agentic-data-contracts/issues/73))

## [0.43.0] - 2026-08-18

### Added

- **Declared attribution convention for decompositions.** A 16-session pilot (#67) found the arithmetic was never the problem — 16/16 runs summed correctly — but the *placement* of the `ΔC·ΔP` cross term was: three distinct placements appeared on identical data, a 13.5% span on the headline contribution, and the narrative conclusion flipped between them. Every placement sums correctly, so nothing downstream of the number can tell which one an agent used; that makes it a non-inferable business fact rather than an implementation detail, the same species as "active users excludes staff." `Decomposition` now accepts `convention` (`explicit` | `split_evenly` | `fold_into`) and, for `fold_into`, `convention_operand` naming which operand absorbs the term:

  ```yaml
  metrics:
    - name: activations
      decompositions:
        - operator: product
          operands: [volume, rate]
          convention: fold_into
          convention_operand: rate
  ```

  The operand is named, not positional (no `fold_into_last`), because `product` takes two or more operands and order is otherwise semantically free — a positional scheme would make operand order load-bearing and hand the agent an index to count instead of a name.

- **Source-level `decomposition_convention` default.** A YAML source (or an Ossie vendor block) can set a house default, restricted to the conventions that need no operand — `fold_into` cannot be a default because there is no metric-agnostic operand to fold into. The default is **resolved onto each cross-term decomposition at load**, not carried by reference, so the resolved value survives `freeze_semantic_source` (which re-serializes from parsed objects) and a frozen contract states its effective convention outright instead of leaving a consumer to re-derive it from a default that may since have changed.

- **`attribute_change` / `check_attribution` (`validation/attribution.py`).** `attribute_change` applies a declared convention to measured before/after values — pure arithmetic, no adapter, because *when* each value was measured is the caller's business, not the library's. `check_attribution` scores a *reported* breakdown against the same convention; its intended caller is an **eval harness** measuring whether an agent follows the contract, not CI and not production — a reported breakdown exists only inside a written answer, the same species of grounding as `drill_by`, and it is never wired into anything that blocks a query. Neither function is registered in `create_tools()`: the pilot shows the arithmetic itself is not where agents fail (16/16 correct), the tool's only real content would be the convention, and both `lookup_metric` and `trace_metric_impacts` already deliver that. A tenth tool here would dilute attention on the checkers that actually gate queries.

### Changed

- **`SEMANTIC_KEYS` gained `decomposition_convention`.** This is a **public API change** — the constant is re-exported at the package top level, both the README and the v0.41.0 CHANGELOG entry name it exactly, and `tests/test_public_api.py` asserts its precise contents. A contract that previously listed `decomposition_convention` under `expected_extras` should drop it: it is interpreted vocabulary now, resolved and validated at load rather than carried through `get_extras()` as an opaque extra.

### Unchanged

- **Digests.** A contract that declares no convention produces byte-identical `contract_canonical_bytes` — `convention` and `convention_operand` are omitted from `dump_semantic_source`, the tool payloads, and the frozen inline snapshot alike whenever they are unset, so adopting this feature costs nothing for a contract that doesn't use it. `decomposition_convention` is a different case: it is never emitted by `dump_semantic_source`, set or unset, because its effect is resolved onto each decomposition's `convention` at load — so it does not survive a freeze, by design, rather than being merely omitted when empty.

## [0.42.0] - 2026-08-16

### Added

- **`OssieSource` — read semantics from an Apache Ossie (incubating) model.** [Apache Ossie](https://ossie.apache.org/), formerly Open Semantic Interchange, is the vendor-neutral spec for exchanging semantic models across analytics, AI, and BI platforms; it entered the Apache Incubator with 50+ participating organizations behind it. Declare it like any other source:

  ```yaml
  semantic:
    source:
      type: ossie
      path: "./semantic/model.yml"
  ```

  The strategic read is that Ossie standardises what a metric *is*, while this library enforces what an agent may *do* with it. Nothing in the spec or its roadmap validates a generated query against a policy — their `validation/` checks models against the JSON Schema, and the planned reference engine *compiles* semantic queries into SQL. So Ossie is a fourth input to `SemanticSource`, not a competitor to the enforcement layer, and the protocol was built for exactly this.

  Four decisions define the adapter, each one a place where the easy choice is a silent correctness bug:

  - **Table keys drop the database qualifier.** Ossie sources are `database.schema.table`; our keys are two-part because `Relationship` endpoints are `schema.table.column` and `build_relationship_index` recovers the table with a single `rsplit`. A three-part key would make every endpoint four deep. Collisions are logged; query-backed datasets register no table.
  - **Cardinality is derived from both sides.** Ossie never writes the join type down — it is implicit in the keys. The spec documents `to` as the one side, but nothing validates it, and trusting it is not free: `RelationshipChecker._check_fan_out` fires only on `one_to_many`, so reading a backwards-declared join as `one_to_one` silently disables the row-multiplication warning on an aggregate. The type is read off `(from_columns are a key, to_columns are a key)`. Keys are optional in Ossie and their absence is not evidence of fan-out, so when the `to` dataset declares no keys at all the spec's declaration stands — otherwise every key-less model would flood the checker with false positives.
  - **Composite joins are skipped, not split.** Ossie carries parallel `from_columns`/`to_columns` lists; our `Relationship` has single-column endpoints. One edge per column pair would assert two joins that are each individually wrong, and the join planner walks those edges. The skip is logged with the relationship's name, matching the precedent `CubeSource` set for Cube's `AND`-chained joins.
  - **Dialect is an opaque string.** Resolution is deterministic as the spec requires — caller's choice, then `ANSI_SQL`, then `Ossie_SQL_2026`, then the first declared entry — but never validated against the enum. The accepted expression-language proposal adds `Ossie_SQL_2026` and makes it the default, so a closed check would break on the next spec bump for no benefit.

- **Governance vocabulary round-trips through Ossie's `custom_extensions`.** Ownership, review dates, tiers, domains, `decompositions`, `drill_by`, and the metric-impact graph have no home in the Ossie spec — every `$def` in `osi-schema.json` sets `additionalProperties: false`, so they cannot be smuggled in as extra keys. They ride in the spec's own escape hatch under the `AGENTIC_DATA_CONTRACTS` vendor name, whose `data` is a JSON *string* per the spec. Restored `decompositions` / `drill_by` pass through the same `validate_decompositions` / `validate_drill_by` as `YamlSource`, so a bad identity still fails loudly at load.

  This makes `OssieSource` the second source after `YamlSource` to populate `business_owner` / `operational_owner` / `last_reviewed` and `decompositions` / `drill_by`; dbt and Cube still leave them unset.

- **`ai_context` and foreign vendor blocks are carried, not interpreted.** Every `ai_context` in the model (Ossie's AI-grounding channel — instructions, synonyms, example questions, in either its string or object form) reaches `get_extras()["ossie_ai_context"]`, and every non-`AGENTIC_DATA_CONTRACTS` vendor block reaches `get_extras()["ossie_custom_extensions"]`. This is the boundary `YamlSource.get_extras` already draws: the framework carries extras and, on request, places them in the prompt, but never interprets, validates, indexes, or computes over them. In particular `search_metrics` still matches on name and description only — Ossie synonyms do not silently change retrieval.

  A foreign vendor's malformed JSON payload is carried verbatim and logged rather than raised on. Another vendor's typo must not stop this library from enforcing a contract.

  Both sections are keyed by **semantic-model name first**. Ossie's top level is a list and it namespaces entity names per model, so a file with two models — each declaring a `customer` dataset, or each carrying a block from the same vendor — would otherwise silently lose the first. Both are omitted entirely when empty rather than emitted as `{}`: extras ride into the contract's inline snapshot and its canonical bytes, so an always-present empty dict would add a noise key to every Ossie contract's digest and would trip a contract declaring `expected_extras` on rehydrate.

- **`expected_extras` on a non-`yaml` source now warns instead of being silently dropped.** The policy is threaded only into `YamlSource`. An Ossie source *does* produce extras, so declaring it there looks like it should work; without a warning the author believes strict mode is on, and the mismatch surfaces much later as a load failure on a frozen contract.

### Changed

- **Shared parser helpers promoted to `semantic/base.py`.** `_normalize_extras` / `_jsonify` became `jsonify_extras`. Extras ride into `contract_canonical_bytes` through `json.dumps`, so every source that carries them needs the same date coercion and JSON-safety check — a YAML-native date inside an Ossie `ai_context` would otherwise reach a published ARD attestation unconverted. The error now names the parser that raised it.

- **`_parse_date` became `parse_review_date`.** Two sources now parse review dates, and the helper's whole reason for existing is a trap worth stating once: `datetime` must be checked before `date` because it subclasses it, or downstream `date - datetime` staleness arithmetic raises `TypeError`. A second copy would have been a second chance to get that ordering wrong. It sits alongside the other shared helpers (`build_relationship_index`, `fuzzy_search_metrics`, the validators) rather than being reached into privately from a sibling module.

- **All locked dependencies upgraded.** Notably `sqlglot` 30.14.0 → 30.17.0 (Layer 1 static analysis), `pydantic-ai-slim` 2.22.0 → 2.31.0, `starlette` 1.3.1 → 1.6.0, and `xxhash` 3.8.1 → 4.0.0. The pinned `contract_digest` guard in `tests/test_ard/test_catalog_entry.py` still holds, so published contract bytes and every existing ARD attestation are unaffected.

## [0.41.1] - 2026-08-11

### Fixed

- **`semantic.source.expected_extras` no longer disappears from an ordinary `model_dump()`.** v0.41.0 kept the field out of `contract_digest` with `Field(exclude=True)`, which was too blunt: a field-level exclusion applies to *every* serialization, so a tool that round-tripped a contract through `model_dump()` locally and revalidated it silently lost the declaration — and got back the warning it had just silenced. The empty-list form (strict mode) collapsed to `None` on the same path, quietly downgrading strict to warn-and-carry.

  The exclusion now lives where it belongs, at the one call site that computes the content address (`_CANONICAL_EXCLUDE` in `contract_canonical_bytes`), rather than on the field. Published bytes are unchanged in every respect: `contract_digest` for a contract with no extras is still `sha256:898c842e…`, and `expected_extras` still never appears in `contract_canonical_bytes` in any form. The rule the field-level exclusion was reaching for — a lint policy about *reading* the source should not move the digest — is intact; it just no longer costs correctness everywhere else.

  Found by the whole-branch review of #62 and documented as a known limitation at the time.

## [0.41.0] - 2026-08-11

Closes #60, whose three defects share one shape: **the library delivered less
than it appeared to, with no signal to the consumer.**

This release bundles three units of work, developed as 0.39.0, 0.40.0, and
0.41.0. Neither intermediate version was published, so the upgrade path is
**0.38.0 → 0.41.0** and the breaking rename below applies to every upgrader.

### Breaking

- **`ClaudePromptRenderer` is renamed to `XmlPromptRenderer`.** The class contains no Claude- or Anthropic-specific logic: the only matches for `Claude|anthropic` in `core/prompt.py` were the class name and its docstring. It emits XML tags — an Anthropic-originated prompting convention, but the artifact is plain XML that any frontier model reads.

  The name had aged against the codebase. `tools/langchain.py` and `tools/pydantic_ai.py` both ship, so a Pydantic AI user's system prompt was being rendered by a class named after a runtime they had deliberately left.

  A protocol implementation should be named for the axis on which implementations differ. `PromptRenderer` exists so alternatives can exist, and a second implementation would differ by output *format* — Markdown sections instead of XML tags — not by vendor. Naming by vendor invites a `GptPromptRenderer` that emits the same tags and differs in nothing.

  Renamed with no deprecated alias, consistent with v0.38.0 removing `usage_limits_from_contract` outright. This is a `0.x` project at Beta status, where SemVer permits a breaking change in a minor bump.

  **Migration** — one line:

  ```diff
  - from agentic_data_contracts import ClaudePromptRenderer
  + from agentic_data_contracts import XmlPromptRenderer
  ```

### Added

- **`YamlSource` now carries every top-level key it does not interpret**, reachable via `get_extras()` and exported as `SEMANTIC_KEYS`. Previously `_load_from_raw` read exactly four keys and discarded the rest with no error, no warning, and no log line — so adding an unsupported section was indistinguishable from success. A downstream consumer carried two custom sections in a production semantic YAML for roughly three weeks: well-formed, internally consistent, covered by a dedicated lint job and ~240 lines of tests, and contributing nothing to any rendered prompt or tool output the entire time. There was no symptom to investigate; it surfaced only when someone rendered `to_system_prompt()` in a scratch script and grepped the output for their own content.

  A one-character typo had the same consequence — `relationship:` for `relationships:` deleted a whole section while the library reported success — and that is the more common way this gets hit.

- **`YamlSource(..., expected_extras=...)`** and the same parameter on `from_raw`. Left as `None` (the default) uninterpreted keys are logged at WARNING and carried anyway, so every existing contract keeps loading. Pass a collection and any key outside it raises at load; `frozenset()` is therefore a strict mode. One parameter instead of a `strict=True` flag, which would have fired forever on a consumer's own deliberate sections.

- **`semantic.source.expected_extras` in the contract YAML**, threading the same policy through `DataContract.from_yaml` → `load_semantic_source()`. Without it the parameter was unreachable on every path the framework itself uses: the library constructs `YamlSource` internally, so a consumer with a deliberate extras section got the "if one of these is a typo, that section is not being read at all" warning on every load and could not silence it — the "train readers to ignore the log" failure that is the whole reason not to warn on absent sections.

  ```yaml
  semantic:
    source:
      type: yaml
      path: ./semantic.yml
      expected_extras: [column_hints, join_paths]
  ```

  Applies on both load paths — the external `path` and the frozen `inline` snapshot — so declaring your sections does not stop working the moment the contract is frozen. `[]` is strict mode; an omitted key keeps warn-and-carry. Ignored for `dbt` and `cube` sources.

  Deliberately **not digest-bearing**: it is a policy about reading the source, not part of what the agent sees, so it is excluded from `contract_canonical_bytes` and never moves `contract_digest`. A naively added pydantic field would have serialized as `"expected_extras": null` into every contract's canonical bytes and silently invalidated every published ARD attestation. The trade is that the declaration does not travel with a published contract: a consumer rehydrating the frozen bytes falls back to warn-and-carry.

- **`ExtensibleSemanticSource`**, a `runtime_checkable` protocol carrying only `get_extras()`. Deliberately a sibling of `SemanticSource` rather than an extension of it: `runtime_checkable` isinstance checks method *presence*, so folding `get_extras` into `SemanticSource` would have made every external custom source fail `isinstance(src, SemanticSource)` on upgrade.

  Because the protocol is public, `dump_semantic_source` no longer assumes extras cannot collide with the vocabulary it dumps. A third-party implementation returning `{"metrics": [...]}` would have silently replaced the dumped metrics and left `contract_digest` attesting to the corrupted payload; it now raises, naming the clashing keys.

- **`XmlPromptRenderer(extra_sections=...)`**, accepting a sequence of section names (default YAML-in-a-tag formatter) or a mapping of name → callable. Nothing renders unless named, so a section kept for internal bookkeeping never leaks into a prompt; naming a section the source does not carry warns rather than failing quietly.

  A bare string raises `TypeError` naming the fix. `str` satisfies `Sequence[str]` structurally, so `extra_sections="column_hints"` would have iterated characters — and against a non-extensible source that produced no warning at all, which is precisely the silence this release exists to remove.

### Changed

- **Extras travel through `freeze_semantic_source()` and into `contract_digest`.** They are resident prompt text, so they shape agent behaviour; a digest that ignored them would let the ARD publish→verify loop attest to something different from what the agent saw. Editing a hint's prose therefore moves the digest and invalidates a pinned attestation. Contracts *without* extras dump byte-identically to before, so no existing frozen contract or published digest moves.

- **Extras values are normalised to JSON-safe types at load.** Dates and datetimes become ISO strings — the shape this exists to carry explicitly wants a verification date — and anything else unserializable raises immediately, naming the key path. At load rather than at freeze, so a bad value fails where it was authored instead of months later inside an ARD publish.

### Compatibility

The framework carries extras and places them in the prompt on request. It does not interpret, validate, index, or compute over their content: no schema checking, no staleness detection, no tool serves them. That boundary is the same one `validation/examples.py` draws for verified examples, and it is why `column_hints` and `join_paths` — the two sections that prompted #60 — ship as the documented *example* of extras rather than as framework vocabulary.

Closes #60.

### Also in this release: honest prompt degradation

- **`XmlPromptRenderer(metric_detail_threshold=…, relationship_detail_threshold=…)`.** Both were class constants, so the only way to opt out of prompt degradation was to subclass the renderer — meaning the consumer who actually knows their prompt budget could not express it. `None` disables degradation entirely. Omitting an argument still reads the class attribute, so existing subclass overrides are unaffected.

- **Both degrading branches now log at INFO and disclose what they dropped.** Above `relationship_detail_threshold`, `_render_relationships` drops every per-relationship `<from>`/`<to>` — the actual join keys — and substitutes per-table `join_count` summaries. On a 52-relationship contract that suppresses roughly 3.1× the whole fragment (9,658 rendered chars versus 29,852 detailed). Gating large content is reasonable; gating it silently means the only way to discover it is to render the prompt and diff it against expectations, which nobody thinks to do because the block looks populated.

  The rendered hint now names the omission and the threshold, and a log line names the count, the threshold, and the parameter that turns it off. `_render_metrics` gets the same treatment against `metric_detail_threshold`, which had the identical shape and the same gap.

  `logger.info`, not `warning`: degradation is intended behaviour under a configured budget, unlike an ignored contract key.

  Reported in #60.

## [0.38.0] - 2026-08-03

### Removed

- **`usage_limits_from_contract()` is deleted.** Added in v0.36.0 and superseded by `contract_run_kwargs()` in v0.37.0 — **28 minutes later**, by release timestamp. It has no remaining use case, and keeping it was actively harmful rather than merely untidy.

  The justification for keeping it was that it "suits a caller who cannot carry a counter across turns". That describes an empty set: every Pydantic AI run API accepting `usage_limits=` also accepts `usage=` — all six of `run`, `run_sync`, `run_stream`, `run_stream_sync`, `run_stream_events`, `iter`. There is no call path where the limit can be passed but the counter cannot, so there was never a caller the older helper served and the newer one does not.

  Meanwhile the two behave differently in a way a reader cannot see from the signatures: `usage_limits_from_contract` derives each run's allowance from `session.tokens_used`, which misses every model request after a run's last tool call, so spend grows **linearly with turns** — 2.2× the declared budget at 6 turns, 3.4× at 12, 5.0× at 20. `contract_run_kwargs` is flat at 1.2× regardless. Offering both meant the README presented two ways to do one thing where the simpler-looking one silently under-enforces — the exact failure class v0.32.0–v0.37.0 have been removing, reintroduced as a menu choice.

  Removed outright rather than deprecated: a deprecation window's value scales with adoption, and a symbol that was the latest release for 28 minutes has none to speak of. This is a `0.x` project at Beta status, where SemVer permits a breaking change in a minor bump.

  **Migration** — one line, and it strictly improves enforcement:

  ```diff
  - usage_limits=usage_limits_from_contract(dc, session),
  + **contract_run_kwargs(dc, session),
  ```

  One caveat when migrating a **live** session: the carried counter starts at zero, so spend that session already recorded under the old wiring is not deducted from the flat ceiling. In-tool `check_limits()` still sees the full tally and stops the next tool call, so this degrades rather than escapes — but a fresh session per user is the clean way to adopt it.

### Changed

- **The `max_retries` → `request_limit` caveat moved to `contract_run_kwargs`,** along with the `cost_limit_usd` / `max_duration_seconds` note, the zero-versus-absent budget distinction, and the `dataclasses.replace` composition path. These lived only in the deleted docstring. The caveat is a property of translating contracts into `UsageLimits` at all, not of whichever function does it — `max_retries` counts *blocked query attempts* here while `request_limit` counts *model requests*, and conflating them would silently redefine existing contracts. Its guarding test moved too.

### Compatibility

- **Breaking**, for anyone who adopted `usage_limits_from_contract` during the 28 minutes it was current. `from agentic_data_contracts import usage_limits_from_contract` now raises `ImportError`. The replacement is a one-line change and enforces the budget more tightly.

## [0.37.0] - 2026-08-03

### Added

- **`contract_run_kwargs(contract, session)`** (Pydantic AI) — makes `token_budget` accounting exact, where v0.36.0's `usage_limits_from_contract` lets spend grow *linearly with the number of turns*. Returns both halves of the wiring, because they are only correct together:

  ```python
  await agent.run(prompt, **contract_run_kwargs(dc, session))
  # -> {"usage": <carried RunUsage>, "usage_limits": <flat ceiling>}
  ```

  One `RunUsage` is carried across every `agent.run()` for the session, so Pydantic AI's own counter sees **every** request — including the answer generation after a run's last tool call, which the tool wrapper can never observe and whose omission is what made the previous helper approximate. Nothing is missed, so nothing is estimated. Measured on an agent spending 100 tokens per request against a 500-token budget: **true spend settles at 600 and stays flat however many turns run**, where the superseded helper is linear — 1100 at 6 turns, 1700 at 12, 2500 at 20. Flat-versus-linear is the real difference; any single pair of numbers understates it. The *accounting* is exact; spend still overshoots the ceiling by the requests in flight when it is crossed (600 against 500 here), which is a bounded overshoot rather than a growing one. And a refused turn now **costs nothing** — with the whole history in the counter, `check_before_request` refuses before issuing a request, where the bounded helper's per-run counter started at zero and so bought one billed request on every refused turn, forever.

### Design notes

- **The limit is flat, not the remainder.** The carried counter already holds the spend; subtracting it again would take it off twice and lock the run out below its own budget — the same double-subtraction `usage_limits_from_contract` warns about when combined with `agent.run(usage=...)`. Guarded by a test that spends first, because a fresh session cannot tell the two apart (`budget - 0 == budget`); found by mutation, which passed every other test in the file.
- **The counter lives in a module-private `WeakKeyDictionary` keyed on the session**, not on `ContractSession`: `RunUsage` is a Pydantic AI type and `core/` stays framework-agnostic. Weak keys because the caller owns session lifetime.
- **The tool wrapper chooses its usage scope by identity, not a flag.** If `ctx.usage` *is* the counter registered for that session it accrues under one constant key; otherwise it falls back to `run_id`. So ignoring the helper, or passing its `usage_limits` without its `usage`, degrades to the older bounded behaviour instead of corrupting the tally — and no boolean exists to get wrong. This matters in both directions: treating a carried counter as per-run re-accrues the running total on every run, measured at **900 recorded against 600 truly spent**, and the inflated tally then blocked a run that was still within budget.
- `session.tokens_used` still lags mid-run, since the wrapper only observes while a tool executes. `contract_run_kwargs` reconciles it at the start of each run, so between runs — when a caller would read it — it is the true spend.
- **Adopt it from the start of a session.** The carried counter begins at zero and knows nothing of spend recorded before the first call — from a plain `agent.run` without these kwargs, from another adapter sharing the session, or from a direct `observe_tokens()`. The flat ceiling is measured against the counter alone, so that earlier spend is not deducted from it. In-tool `check_limits()` still sees the session's full tally and stops the next tool call, so this degrades rather than escapes; but mixing wirings mid-session gives up the exactness that is the point.

### Superseded

- **`usage_limits_from_contract` is superseded but still supported**, with no deprecation warning: it is one release old, remains correct for what it claims, and needs nothing but the limit — so it still suits a caller who cannot carry a counter across turns. Its docstring, the README and `docs/architecture.md` now point at `contract_run_kwargs` and state plainly that it bounds rather than enforces.

### Compatibility

- Additive and opt-in. Existing wiring is unchanged, including `usage_limits_from_contract`. Callers should still catch `ContractSessionLimitError` alongside `pydantic_ai.exceptions.UsageLimitExceeded`: the session continues to enforce `max_retries`, `cost_limit_usd` and `max_duration_seconds`, and still fires when fed from outside the run.
- Sequential turns only. One carried counter per session is shared mutable state, so concurrent runs for the same user race on it — the same shape `ContractDeps` already describes for sessions.

## [0.36.0] - 2026-08-03

### Added

- **`usage_limits_from_contract(contract, session)`** (Pydantic AI) — narrows the pre-tool-call window that v0.34.0 shipped as a known limitation. `ContractSession.check_limits()` only runs when a tool is about to be invoked, so an agent that exhausts its budget and then stops calling tools was never interrupted. Pydantic AI checks `UsageLimits` on every *model request*, which needs no tool call at all, closing that hole **within a run**:

  ```python
  result = await agent.run(
      prompt,
      deps=ContractDeps(session=session),
      usage_limits=usage_limits_from_contract(dc, session),  # per run, not once
  )
  ```

- **The `session` argument is required, and is the substance of the change.** `total_tokens_limit` is checked against `RunUsage` — usage for *one* `agent.run()` — while a contract's `token_budget` is a ceiling for the user across every turn (`ContractDeps` instructs callers to reuse one session so limits accumulate). The obvious mapping, `total_tokens_limit = token_budget`, is therefore **wrong in the unsafe direction**: it re-grants the full budget on each turn, so a 50,000-token contract authorises 500,000 across ten turns — declared-but-unenforced, the failure this library exists to prevent. Limiting each run to `max(0, token_budget - session.tokens_used)` bounds the total instead. Call it per run; the value is a snapshot, and a stale one re-grants spend that already happened.

### Known limitations

- **This bounds the budget; it does not enforce it exactly.** The subtracted figure is `session.tokens_used`, and the session is fed only from inside the tool wrapper — so every model request *after a run's last tool call*, including the final answer generation (typically the largest context), goes unobserved. Each run is therefore granted more than the true remainder, and the shortfall compounds per turn. Measured on a two-request-per-turn agent with a 500-token budget, real spend reached **~2× the ceiling by the first refusal** — and that factor is a property of the tool-call-to-request ratio, not a constant: ~1.4× measured for a three-tool-call turn, and see the next point for the case with no ratio at all. It is a large improvement on the unbounded behaviour without the helper, but not the identity a quick read might assume. Tracked as #56, which carries the design that would make it exact.

- **An agent that calls no tools at all is still unbounded across turns.** Within a run it is now stopped, which is the hole this release closes. But `session.tokens_used` never leaves 0 if no tool ever executes, so every subsequent run is granted the full budget again. Measured: 12 turns, 500-token budget, 1200 tokens spent, zero refusals. The v0.34.0 limitation is narrowed to "across runs", not removed.

- **Catch `pydantic_ai.exceptions.UsageLimitExceeded` *as well as* `ContractSessionLimitError`.** Once the helper is wired the framework usually stops the run first, because a tool can only execute on a response that already passed `check_tokens` — so a budget breach normally surfaces as the framework exception rather than the contract one. It is **not** a replacement, though: `ContractSessionLimitError` still fires whenever the session is fed from outside the run, which is exactly what a session shared with another adapter or a direct `observe_tokens()` call does (verified on both `apply_middleware` values). Keep the existing handler and add the new one. The session's token tally also freezes once runs start being refused, since no further tools execute to feed it, while each refused turn still costs one billed model request — so cumulative spend keeps climbing past the ~2× first-refusal figure.

- **Do not combine with `agent.run(usage=...)`.** A carried counter is subtracted twice — once by this helper, once inside the counter — and the run locks out permanently below the declared budget. Concurrent runs sharing one session each snapshot the same `tokens_used` and are each granted the full remainder; correct for the sequential turns `ContractDeps` describes, worth knowing if you fan out.

### Not mapped, deliberately

- **`max_retries` is not mapped onto `request_limit`.** They count different things — `max_retries` counts *blocked query attempts* here (`record_retry()` fires on a validation block), while `request_limit` counts *model requests*. A contract saying `max_retries: 3` means "three bad queries and you are done", not "three LLM calls"; conflating them would silently change what an existing contract means. `request_limit` is left at Pydantic AI's own default, which is its runaway guard and not ours to reinterpret.
- **`cost_limit_usd` and `max_duration_seconds`** have no `UsageLimits` equivalent and stay session-side.
- **A contract with no `token_budget`** gets `UsageLimits()` with no token ceiling. Inventing a limit nothing declared is the mirror image of failing to enforce one that was.

### Scope

- Pydantic AI only. LangChain has no per-request ceiling to map onto, and the Claude Agent SDK path cannot observe usage at all — so this lives in a module that imports only under the `[pydantic-ai]` extra, rather than anywhere that would imply cross-framework coverage it does not have.

### Compatibility

- Additive and entirely opt-in at the API level: nothing changes unless you pass the helper's output to `agent.run()`. The *code* for in-tool enforcement is untouched — but wiring the helper changes which exception a budget breach raises, per the second known limitation above. Adjust your `except` clause when you adopt it.

## [0.35.0] - 2026-08-03

### Added

- **`create_langchain_tools` now feeds `token_budget` itself.** v0.34.0 gave the budget a producer on two of four paths; this closes the third. The `StructuredTool` coroutine declares a `runtime: ToolRuntime | None` parameter, which LangGraph's `ToolNode` injects with the run's message history and thread id — the same material `ContractMiddleware` already read. A declared budget therefore binds on a plain `create_langchain_tools(...)` wiring, with no middleware and no manual `observe_tokens()` call. Verified end-to-end through a real `create_agent`, and at the declared floors (`langchain` 1.2.17 / `langchain-core` 1.3.3 / `langgraph` 1.1.10) — no floor bump was needed.

  Three properties worth knowing, each of which had a way of going wrong:

  - **The model never sees the parameter.** `infer_schema=False` means the advertised schema is the tool's JSON Schema dict verbatim, not the coroutine signature; `tool.args` is unchanged.
  - **It is optional, defaulting to `None`.** Injection happens inside a graph; `await tool.ainvoke({"sql": ...})` is a supported call shape that passes no runtime. A required parameter would break every direct caller in order to catch a mis-wiring whose worst case is the *previous* behaviour — usage simply unobserved.
  - **Observation precedes the limit check**, matching the Pydantic AI adapter, so a budget the current run has already blown stops that call rather than the next.

- **The usage accounting is now shared** between `ContractMiddleware` and the tool path (`_observe_usage` / `_usage_scope_for`, keyed on `(state, config)` — the one shape both callers can produce) rather than duplicated. Fed the same run, both derive the same per-conversation scope key and observe the same cumulative total, so **wiring both does not double-count**: whichever runs second is a no-op. That property is what makes the tool-path observation safe to leave unconditional instead of gating it on `apply_middleware`, which governs *enforcement*, not observation. Sharing was the point — a duplicated envelope in `middleware.py` is what let a bug through in v0.32.0, and two copies of a scope derivation could drift into disagreeing about one session's total.

### Removed

- **The wiring-time `token_budget` warning on `create_langchain_tools`**, and with it the `apply_middleware` special case that suppressed it. Both existed only to describe this gap. `contract_middleware` and the Claude Agent SDK path still warn; they receive an `args` dict and nothing else, and remain genuinely blind. The warning's *"does not observe"* wording — chosen over *"cannot"* in v0.34.0 — earned its keep: closing this gap took one parameter declaration, not a new capability.

### Known limitation

- **A nested run under-counts.** A sub-agent or subgraph inherits its parent's `thread_id`, so it collides on the scope key while carrying its own, shorter history — and because `observe_tokens` keeps the larger total per scope, its usage is dropped whole rather than added: a 5000-token parent turn followed by an 800-token sub-agent turn accrues 5000, not 5800. This is not a regression (the tools observed nothing at all before), and it under-enforces rather than over-enforces, but v0.35.0 makes it reachable automatically in `create_deep_agent(tools=tools)`, which is the README's own headline wiring. The obvious fix does not work: the config's `checkpoint_ns` is `tools:<task-id>` and changes on *every* tool call, so keying on it would re-accrue the full total each time and multiply rather than separate. Pinned by `test_nested_run_sharing_a_thread_id_under_counts` so it cannot drift unnoticed.

### Compatibility

- Additive. Existing calls are unchanged, direct `tool.ainvoke({...})` still works, and the advertised tool arguments are unchanged (asserted by test, not assumed). **But a `token_budget` declared alongside `create_langchain_tools` now binds where it previously did not** — a contract carrying an aspirational budget will start raising `ToolException` once it is passed. Raise or remove the value if it was never meant to bind; the v0.34.0 warning was the notice for this.
- One caveat for the direct-invocation path: outside a graph nothing injects a runtime, so usage still goes unobserved there. This is unchanged behaviour, not a new gap, and is why the parameter is optional.

## [0.34.0] - 2026-08-02

### Fixed

- **`resources.token_budget` is now actually enforced.** It has always been declared in the schema and checked by `ContractSession.check_limits()`, but **nothing ever fed it** — `record_tokens()` was called only from tests. A contract could declare a 50,000-token ceiling and a run could burn 500,000 without the check firing, because the model's own consumption between tool calls never reached the session.

  It was worse than an inert limit. `ContractSession.remaining()` reports `tokens_remaining = token_budget - tokens_used`, and `tokens_used` was permanently 0 — so every `BLOCKED —` envelope and every `run_query` response told the model *"tokens_remaining: 50000"* regardless of what it had spent. The library was not merely failing to enforce; it was asserting a false number to the agent.

### Added

- **`ContractSession.observe_tokens(cumulative, *, scope="")`** — records a *cumulative* total from an external counter, accruing only the per-scope delta. Neither obvious implementation is correct here: framework counters report a running total for **their** scope (a Pydantic AI run, a LangGraph thread) while a `ContractSession` deliberately spans several of them — `ContractDeps` instructs callers to keep one session per user across every turn so limits accumulate. Adding the total on each tool call multiplies it; assigning it resets the tally at each new run and silently discards earlier turns. A total lower than the last seen for a scope adds nothing rather than subtracting, so a counter restarting under a reused key cannot hand back spent budget. `record_tokens()` is unchanged and remains the delta-based path for framework-free callers.

- **Token usage is fed on the two paths that can observe it.** The Pydantic AI wrapper now registers `takes_ctx=True` and reads `ctx.usage.total_tokens`, scoped by `ctx.run_id`, *before* the limit check — so a budget already exhausted by the model is caught on that call rather than the next. LangChain's `ContractMiddleware` sums `usage_metadata` across `request.state["messages"]`, scoped **per conversation** — one middleware instance serves every conversation an agent handles, so a constant scope silently drops the second conversation's usage whenever its running sum sits below the first's peak, under-enforcing *and* reporting a false `tokens_remaining`. The key is the runtime's `thread_id` when a checkpointer is configured, otherwise the id LangGraph stamps on the first message in state (which covers the no-checkpointer wiring the README shows). A bare constant remains the last resort, and is the one case that still under-counts.

  **With LangChain, share one `ContractSession` between the tools and the middleware.** Each defaults to building its own, and a split pair enforces against the middleware's session while `run_query` reports `remaining()` from the tools' — so the model is told it has its full budget regardless of spend. The README example now shows the shared wiring.

- **A warning on the paths that do not.** `create_sdk_mcp_server`, `contract_middleware` (both receive an `args` dict alone) and `create_langchain_tools` now warn at wiring time when a contract declares a `token_budget`, naming the adapters that do enforce it. The wording is *does not* rather than *cannot*, deliberately: a `StructuredTool` coroutine can be handed a `ToolRuntime` and with it the message list, so `create_langchain_tools` is a wiring gap rather than a capability limit — asserting otherwise in a permanent runtime warning would be the same declared-but-false failure in doc form. `create_langchain_tools` stays quiet when `apply_middleware=False`, which is exactly how a caller signals it is delegating to `ContractMiddleware`; a warning that fires on a correct configuration is how the real ones get ignored. Same shape and reasoning as the `ENFORCEABLE_OPERATIONS` warning: a declared-but-unenforced limit is worse than an absent one, because the contract reads as protective while permitting the very thing it names.

### Known limitation

- `check_limits()` runs **pre-tool-call**, so a budget breach is caught at the *next* tool call. An agent that exhausts its budget and then stops calling tools will not trip it. This is inherent to enforcing from inside tools and matches how `max_retries` and `cost_limit_usd` already behave. The mitigation is soft but real: now that usage is fed, `remaining()` reports honest `tokens_remaining` in every `run_query` response, so the model can self-regulate. Pydantic AI's `UsageLimits` (passed to `agent.run()`) is what would close the window; it is not wired here.

### Compatibility

- Purely additive at the API level — `observe_tokens` is new, `record_tokens` is unchanged. **But `token_budget` now actually stops runs.** A contract that has been carrying an aspirational budget will begin raising `ContractSessionLimitError` (Pydantic AI) or returning a blocked `ToolMessage` (LangChain middleware) once that budget is passed. Raise or remove the value if it was never meant to bind.
- The Pydantic AI wrapper's internal function now takes a `RunContext` first argument (`takes_ctx=True`). This is invisible through `Agent(tools=...)`, which dispatches it; only code calling `Tool.function` directly is affected.

## [0.33.0] - 2026-08-02

### Fixed

- **Two optional-extra floors were untrue, and `mcp` was imported without being declared.** 0.32.0 added the `test-lowest-floors` job but scoped it to core dependencies, because widening it revealed pre-existing breaks that would have left the job permanently red. Those are fixed here, so the job now runs the **full suite** at every floor uv actually resolves to, matrixed over both supported Pythons — plus an explicit wheel check for the floors it cannot reach (see below).

  - **`agent-sdk` now declares `mcp>=1.23.0` directly, and requires `claude-agent-sdk>=0.2.96`.** `tools/sdk.py` imports `mcp.types.ToolAnnotations` at runtime, so `mcp` was always a direct dependency — it simply was not declared, which is how this extra inherited an upstream constraint bug it could have owned. `claude-agent-sdk` 0.1.x declared a bare `mcp>=0.1.0`, so resolving the extra at its floor pulled `mcp` 2.0.0, which removed `Server.list_tools` and broke the SDK's own `create_sdk_mcp_server`. The **upper** bound is deliberately left with `claude-agent-sdk` (0.2.96 is the first release declaring `mcp<2.0.0,>=1.23.0`): mcp 2.0 broke *its* call, not ours, and duplicating another project's ceiling here would go stale.
  - **`duckdb`: `>=1.0.0` → `>=1.1.1`.** DuckDB 1.0.0 renders the EXPLAIN cardinality as `EC: N`; 1.1.0 changed it to `~N Rows`, which is what `DuckDBAdapter._parse_row_estimate` matches. This was **not** merely lost telemetry: `Validator` gates the `max_rows_scanned` check on `estimated_rows is not None`, so on duckdb 1.0.0 a contract's row-scan limit was **silently unenforced** — the limit read as active and did nothing. 1.1.1 rather than 1.1.0 because 1.1.0 ships no cp313 wheel and this package supports Python 3.13; at that floor a 3.13 install would fall back to a from-source C++ build.

  - **`pydantic`: `>=2.0` → `>=2.11`.** Untrue twice over. pydantic 2.0 pins `pydantic-core==2.0.1`, whose wheels stop at cp311 — so the declared floor meant a PyO3 source build on *both* supported interpreters — and anything below 2.11 fails against `pydantic-ai-slim` 2.0 with a `GenerateSchema.__init__()` signature mismatch. It survived because uv resolves the project **universally**: transitive requirements float pydantic to 2.12 even with no extras installed, so `--resolution lowest-direct` never exercised its floor.
  - **Wheel-only floors for the driver extras:** `google-cloud-bigquery` `>=3.0.0` → `>=3.7.0` (3.0.0 drags in a `pyarrow` with no wheel), `snowflake-connector-python` `>=3.6.0` → `>=3.14.1` (3.6.0 has no cp312 *or* cp313 wheel, so it was unusable on every Python this package supports), `psycopg2-binary` `>=2.9.9` → `>=2.9.10` (no cp313 wheel). These extras install a driver for a `DatabaseAdapter` you write, so a floor that cannot install defeats their only purpose.

  Verified rather than assumed: `langchain`, `pydantic-ai`, `agent-contracts`, `sqlglot`, `pyyaml`, and `thefuzz` all hold at their existing floors. The `bigquery`, `snowflake`, and `postgres` extras stay out of the job by design: this package contains **no adapter code** for them (no import of those drivers exists in `src/`), so they are install conveniences for users writing their own `DatabaseAdapter` and their floors cannot affect it.

### Changed

- **`test-lowest-floors` is matrixed over Python 3.12 and 3.13**, and gained a wheel check. A floor can be satisfiable on one interpreter and not the other — the duckdb cp313 gap above is exactly that, and a 3.12-only job could not see it. The job also now uses the `dev` extra instead of unbounded `--with pytest` flags, which additionally floor-tests `dev`.

- **New `scripts/check_floor_wheels.py`, run by that job on both Pythons.** Resolution alone is not sufficient: uv resolves universally, so a transitive requirement can float a package above its own floor and the floor is never tested — which is precisely how `pydantic>=2.0` survived. The script reads every `>=` floor out of `pyproject.toml` (so a new dependency is covered without editing it), pins each to `==`, and asserts it resolves with wheels alone. It found the `google-cloud-bigquery` floor above while being written.

### Compatibility

- **The `agent-sdk` extra now requires `claude-agent-sdk>=0.2.96`, dropping support for the 0.1.x line.** To be clear, 0.1.x is not broken *per se* — it works when `mcp` resolves below 2.0. What it cannot do is guarantee that on its own, and the fix is to depend on a release that bounds it. If you are pinned to `claude-agent-sdk` 0.1.x you will get a resolution conflict and must upgrade.
- **The `duckdb` extra now requires `>=1.1.1`.** Anything below that either mis-parses the row estimate (silently disabling `max_rows_scanned`) or, on Python 3.13, has no wheel.
- Minor version rather than patch, deliberately: raising a floor can break a consumer's resolve, and this project puts changes that can do that in the minor slot.

## [0.32.0] - 2026-08-02

### Changed

- **`run_query` and `inspect_query` now carry the query protocol in their tool descriptions.** Two clauses: an **ordering** rule on both tools — *"When computing a metric, you MUST call lookup_metric first and use its governed definition — never invent or adapt a metric formula"* — and, on `run_query` only, a **precedence** claim — *"Prefer this tool over any other SQL or data-access path."* Previously this guidance existed solely in `ClaudePromptRenderer` output, which is **opt-in**: none of the ecosystem wrappers inject the rendered contract, so a host calling `create_langchain_tools(...)` or `create_pydantic_ai_toolset(...)` with its own system prompt got the governed tools with none of the workflow contract attached. Descriptions travel with the tools on every path; a rule the host can drop by writing its own prompt is not a rule. The precedence clause additionally addresses tool competition — an agent holding both `run_query` and a generic SQL tool, warehouse MCP server, or shell previously had nothing telling it which path was governed. The overlap with the renderer's `<hint>` is intentional and the two surfaces do different jobs: the hint aids *discovery* (the tool exists), the description enforces at *call time*.

  Both clauses obey one rule: **a clause appears only when the capability it names exists.** The ordering clause resolves to `""` unless the semantic source actually yields metrics, so a schema-only contract never points the agent at an empty tool; the precedence clause resolves to `""` unless an adapter is configured, since without one `run_query` returns *"No database adapter configured — cannot execute query"* and claiming precedence would steer the agent off a path that works and onto one that cannot run. Both follow the conditional shape `row_format` established with `_COMPACT_ROWS_NOTE`.

  Two further limits. The **trigger is narrow** — "when computing a metric" rather than a broader "before any query" — because the guarded failure is a KPI computed from an invented formula (SQL that is *authorized* and merely wrong, the one class no checker catches), and a broad trigger would tax plain exploratory SQL with a lookup turn that finds nothing. And precedence lands on **`run_query` only**, never `inspect_query`, which executes nothing and already argues its own precedence against spending retry budget.

- **Blocked tool results now carry MCP's `isError`.** `claude_agent_sdk` maps `is_error` off the returned envelope onto MCP's `isError`, the channel the spec designates for *tool execution errors* a model can self-correct from — and we never set it, so every governance decision (forbidden operation, missing tenant filter, restricted table, exhausted budget) reached the model as a **successful** tool result, discoverable only by reading the prose. A new `_error_response` helper in `tools/factory.py` sets it, and the session-limit envelopes that `tools/sdk.py` and `tools/middleware.py` build themselves set it too.

  The rule is **"the tool did not perform the action it advertises"**: governance blocks, access denials (`Table X is not in the allowed tables list`, `Table X is restricted`), misconfiguration (no adapter, no semantic source), and invalid arguments. This covers `contract_middleware`'s envelopes too — the BYO-tool-wrapper path — not only `create_tools`. Deliberately **excluded** are lookups that legitimately found nothing — `Metric 'x' not found.` answered the question it was asked, and flagging it would tell the model its call failed and distort fuzzy-search recovery — and `inspect_query` reporting violations, since reporting violations is precisely what that tool is *for*. All three modules that build envelopes — `factory.py`, `middleware.py`, `sdk.py` — now construct them through the single `_error_response` helper, so the flag cannot be forgotten at a new block site. Tests exercise each block path individually (per-path, with a fresh session each, plus `preview_table`'s gated site). The key is additive on every path — the LangChain and Pydantic AI wrappers read only `content` and ignore it, and they already signalled errors natively (`ToolException`, `ModelRetry`), so this closes a gap specific to the SDK path.

- **MCP tool annotations on the SDK path.** `create_sdk_mcp_server` now passes `ToolAnnotations(readOnlyHint=True)` for the eight tools that only read. `run_query` is deliberately left **unannotated** rather than marked `False`: whether it can write depends on the contract's `forbidden_operations`, and even with the blocklist gap fixed below, `CALL` and vendor-specific DDL parse as a bare `exp.Command` and pass unseen — an omitted hint means "unknown" in MCP, which is honest, where claiming read-only would invite a client to skip a confirmation prompt it should have shown. Annotations are an `mcp.types` concept, so they live in `tools/sdk.py` behind `_annotations_for(name)` rather than on the framework-agnostic `ToolDef`; `mcp.types` is imported lazily so the module stays importable without the `[agent-sdk]` extra. Per the MCP spec, clients must treat annotations as untrusted, so this improves client UX and is not itself enforcement.

### Fixed

- **`forbidden_operations` silently ignored most of what it could be asked to forbid.** `OperationBlocklistChecker` recognised only DELETE / DROP / INSERT / UPDATE / TRUNCATE, so a contract declaring `forbidden_operations: [CREATE]` parsed, validated, and stored cleanly while permitting every `CREATE` — the contract *read* as protective and enforced nothing. That is the failure class this library exists to prevent, applied to itself. The map now also covers **CREATE, ALTER, MERGE, GRANT, REVOKE, and COPY** (all parse to distinct sqlglot nodes; `CREATE TABLE ... AS SELECT` and `COPY ... FROM` are real writes that previously passed unseen).

  Extending the map alone would only move the boundary, so the *class* of bug is closed too: `ENFORCEABLE_OPERATIONS` is **derived from the map** (never retyped — a hand-maintained second list would drift, which is the bug itself), and `Validator.__init__` now warns when a contract names an operation outside it: `forbidden_operations names ['CALL', 'VACUUM'], which the operation blocklist cannot detect — declared but NOT enforced`. Same shape as the existing warnings for unknown domain and metric-impact references, and it fires on the standalone `validate_examples` path as well as `create_tools`. Tests cover both directions per operation — blocked when listed, and **not** blocked when unlisted, since over-blocking a statement a contract never forbade would be worse than the original gap.

  The warning is deliberately **not** memoised: caching a logging side effect would lose it permanently for a consumer that calls `logging.basicConfig()` after building its contracts, and the five sibling warnings in `create_tools` are uncached on the identical call path.

  **The recommended contracts were extended to match** — the README quick start, the architecture doc's schema example, and all three `examples/*/contract.yml` now forbid the full set. Widening what *can* be forbidden achieves nothing if every contract a user copies still permits `CREATE TABLE scratch AS SELECT * FROM sensitive` or `COPY … TO 's3://…'`.

  `run_query`'s `readOnlyHint` deliberately stays unset even now: `CALL` and vendor-specific DDL still parse as a bare `exp.Command`, and `SELECT … INTO newtbl` parses as a plain `exp.Select` that the operation blocklist never sees (the table allowlist catches it instead). "Unknown" remains the honest annotation.

### Compatibility

- **`sqlglot`'s floor moved from `>=23.0` to `>=28.0`.** The operation blocklist maps AST nodes (`exp.Alter`, `exp.Grant`, `exp.Revoke`, `exp.Copy`) that do not exist below 27.10, and because the map is a class-body dict evaluated at import, an older sqlglot raised `AttributeError` on `import agentic_data_contracts` rather than degrading a feature. 28.0 rather than 27.10 because `WHERE x IS x` only parses from 28.0 and the tautology-bypass checks depend on it — 27.10 would have declared support for a version failing the project's own governance tests. **If you pin `sqlglot` below 28.0 you will now get a resolution conflict at install** instead of a working install. A new `test-lowest-floors` CI job resolves core dependencies to their declared minimums so this cannot silently recur; the optional extras' floors are known to be wrong and are tracked separately.

- **Behavior change for contracts already naming the newly-covered operations.** A contract listing CREATE / ALTER / MERGE / GRANT / REVOKE / COPY in `forbidden_operations` starts blocking those statements, where before it silently allowed them. This is the declared intent finally taking effect rather than a new restriction — but a caller depending on the no-op will see queries begin to fail, and the fix is to remove the operation from `forbidden_operations` if it was never meant to be enforced. Contracts that do not name these operations are unaffected: the checker only ever blocks what the contract explicitly lists.

- **Additive throughout; no API or schema change.** No signature changed, no new parameter, no new dependency, and the other seven tool descriptions are untouched (asserted by a scope-guard test). In `compact` mode the row-shape note remains the description **suffix**, so the `compact == records + _COMPACT_ROWS_NOTE` identity still holds. `ClaudePromptRenderer` output changed in one place: the compact metric hint (used above `METRIC_DETAIL_THRESHOLD`) now ends `... to get SQL definitions before computing any KPI.` A prompt-snapshot test will see it. Two further things to know if you parse tool output directly: callers asserting on the exact text of `run_query`'s or `inspect_query`'s `description` need to update, and error envelopes now carry an extra `is_error: True` key alongside `content` (absent, as before, on success — read it with `.get("is_error")`).

### Dependencies

- **All locked dependencies upgraded** (`uv lock --upgrade`), notably `ai-agent-contracts` 0.3.2 → **0.4.0** (the optional formal-governance bridge), `pydantic-ai-slim` / `pydantic-graph` 2.17.0 → **2.22.0**, `sqlglot` 30.13.0 → **30.14.0** (the Layer-1 analysis engine), `anthropic` 0.119.0 → 0.120.2, `claude-agent-sdk` 0.2.126 → 0.2.128, `mcp` 1.28.1 → 1.29.0, `langchain-core` 1.5.1 → 1.5.3, and `cryptography` 49.0.0 → 50.0.0. Full suite green on the upgraded lock (863 tests at time of merge), including the 18 bridge/ARD tests against `ai-agent-contracts` 0.4.0 and the `test-pydantic-ai-latest` early-warning job (26 tests) resolved against the newest release the `>=2.0` floor allows.

## [0.31.0] - 2026-07-24

### Changed

- **`run_query` and `preview_table` return compact rows by default.** Both tools previously rendered each result row as a JSON object repeating every column name, which roughly **doubles to triples** the token cost of a result for identical information — measured at 198,003 characters versus 84,503 for a 20-column x 500-row result. Because tool results persist in the message history and are re-sent on every subsequent model request, that overhead is paid repeatedly rather than once. `rows` is now a list of **positional arrays** aligned to the `columns` key that `run_query` already returned. New `create_tools(..., row_format=...)` selects the rendering: `"compact"` (default) or `"records"` (the previous dict-per-row shape). The knob is deliberately **operator-facing rather than a model-facing tool argument** — unlike Anthropic's `concise`/`detailed` pattern this drops no information, so the agent has no basis on which to choose, and an `input_schema` field would cost tokens on every request. An unrecognised value raises `ValueError` at `create_tools()` time. In `compact` mode both tool descriptions gain one clause stating that rows are positionally aligned to `columns`.

### Added

- **`preview_table` now returns a `columns` key** in both renderings, so the two result tools share one `{columns, rows}` envelope and a consumer writes a single parser. This also closes a real gap: a zero-row preview previously returned `{"rows": []}` and told the agent nothing about the table's shape.
- **`RowFormat`** (`Literal["compact", "records"]`) is exported from the package root, alongside `Principal`, for callers typing their own wiring.
- All four ecosystem wrappers — `create_langchain_tools`, `create_sdk_mcp_server`, `create_pydantic_ai_tools`, `create_pydantic_ai_toolset` — accept and forward `row_format`. `create_pydantic_ai_toolset` validates it in its own body rather than deferring to the per-run factory, so the fail-at-wiring-time guarantee holds on every path. A pre-built `tools=` list continues to take precedence, as it already does over `adapter` and `semantic_source`.

### Compatibility

- **The default output shape of `run_query` and `preview_table` changed.** Anything parsing `rows` as a list of dicts must either read positionally (`row[columns.index("col")]`) or pass `row_format="records"` to restore the previous `rows` rendering — with one caveat: `records`' `dict(zip(columns, row))` is last-value-wins, so a query with duplicate column labels (e.g. `SELECT t.id, u.id FROM t, t u`) silently drops one column under `records`, where `compact`'s positional arrays keep both — one more reason `compact` is the better default. Values are unaffected — `json.dumps(..., default=str)` is unchanged, so `Decimal` and `date` still serialize identically. `preview_table`'s new `columns` key is additive and present in both modes. Every other tool is untouched, and no new dependencies are added.

### Internal

- New `_render_rows` helper in `tools/factory.py` owns the branch for both tools. Its `list(row)` coercion is load-bearing: `DatabaseAdapter` is a `@runtime_checkable` Protocol, so an adapter may return its driver's row type, which `dict(zip(...))` tolerated (it needs only iteration) but `json.dumps` would have routed through `default=str` and serialized as a string. Full suite green; `ruff` / `ruff format` / `ty` clean.

## [0.30.0] - 2026-07-19

### Added

- **Verified-examples contract validation.** New `validate_examples(...)` (in `agentic_data_contracts.validation`) re-validates an external corpus of `question → SQL` examples against a `DataContract` using the **same two-layer `Validator`** that gates live agent queries — Layer 1 (sqlglot static analysis: allowed tables, forbidden ops, required filters, `SELECT *`) always, plus Layer 2 (a live `EXPLAIN` dry-run) when a database adapter is supplied. The examples database stays **entirely external** — your repo, your YAML, your human-reviewed MR flow; the framework never stores, loads, retrieves, or executes the corpus, contributing exactly one verb: *validate*. The interchange is a plain `VerifiedExample` dataclass (`sql` is the only load-bearing field; `VerifiedExample.from_dict(...)` is a shape adapter that preserves unknown keys under `.metadata` and never interprets them). The result is an `ExampleValidationReport` of `ExampleResult`s, each with exactly one `status` — `valid` (statically contract-checked *and* passed), `violation` (a check rejected it), `unverified` (**decision B**, below), or `unchecked` (no verdict possible) — and two flags, `contract_checked` and `engine_checked`, recording *what* was verified, plus `unverified_compliance`, a markdown `summary()`, and `ok`. `report.ok` is a **safe CI gate**: True only when *every* example is `valid`, so `if not report.ok: sys.exit(1)` fails on violations, unchecked, *and* unverified rows alike (test `report.violations` directly for a laxer gate). Two uses of the same call: an **MR gate** (validate the corpus in CI before a human reviews it) and a **drift sweep** (re-run against a changed contract; `report.violations` are the examples the change — or a dropped/renamed column caught by the live EXPLAIN — just broke). The verdict is honest about its own reach: it confirms an example is still *allowed, well-formed, and plannable against the current schema* — never that it still returns the right answer, because it **never executes** the SQL (result correctness stays with the human review). For SQL an engine parses but sqlglot cannot (e.g. Denodo/VDP), a parse failure falls back to the engine's own planner (decision B): the engine vouches for plannability but contract policy is never statically checked, so the example is `unverified`, not `valid`. A per-example guard degrades any example whose adapter raises to `unchecked` rather than aborting the batch.

### Compatibility

- **Backward compatible.** One new field on `ValidationResult` — `parse_error: bool` (defaults `False`, appended after existing fields) — is the only change to core validation; it lets callers distinguish a parse failure from a policy block and drives the decision-B fallback. Otherwise a net-new module plus four new exports (`VerifiedExample`, `ExampleResult`, `ExampleValidationReport`, `validate_examples`) from `agentic_data_contracts.validation`; nothing existing changes behavior, and no new dependencies are added.

### Docs

- New runnable demo `examples/revenue_agent/verify_examples.py` + `verified_examples.yml`: validates an external corpus against the revenue contract with a **live DuckDB EXPLAIN**, showing a valid example, static violations, a **schema-drift catch only the dry-run finds**, and the same SQL diverging `valid` / `violation` by principal. New README section "Validating a verified-examples corpus", and the previously-undocumented `reconcile_decomposition` (0.29.0) is now cross-referenced there.

### Internal

- New `validation/examples.py`, built across 6 TDD tasks (red-first) with a per-task spec+quality review, a three-lens plan review *before* execution, and a final whole-branch review. `engine_checked` is reconstructed from `schema_valid` / `estimated_*` rather than a second core field, so the only `validator.py` change stays `parse_error`. The `SemanticSource` type import is guarded under `TYPE_CHECKING` (annotation-only, cycle-safe, matching the existing `validator.py` pattern). Full suite green (785 tests); `ruff` / `ruff format` / `ty` clean.

## [0.29.0] - 2026-07-19

### Added

- **Metric decomposition reconciliation check.** New `reconcile_decomposition(...)` (in `agentic_data_contracts.validation`) executes a metric's declared `decompositions` against a live database and asserts the arithmetic identity holds within tolerance — the *reconciliation half of Spec B*. It catches an identity that has gone false in the data (ETL drift, a child metric SQL that diverged, a join that skews a population) which the per-query validators (sqlglot, EXPLAIN) never see because the SQL is still authorized. The check is **keyed off the declared decomposition** — the contract owns *what* the identity is (operator + operand names), while the caller supplies scalar SQL for the parent and each declared operand, owning *how* to measure each over its chosen slice; no metric executor is assumed. Malformed input raises `ValueError` (no decomposition, out-of-range index, operand-key mismatch, unknown operator, wrong operand arity); data conditions are *findings* (`reconciles=False` with a mechanical `reason`) — a NULL / empty / non-finite (`NaN` / `inf`) measurement, or a `ratio` zero denominator. `ReconciliationResult.reason` reports only the mechanical condition and **never infers the cause** — diagnosis stays agent-owned, the same governance/agent boundary the decomposition feature drew. Default `rel_tol=1e-4` is tight because decompositions are exact identities. Intended primary home is CI: a hermetic per-PR regression guard plus a live-warehouse nightly drift detector.

### Compatibility

- **Fully backward compatible.** A net-new module plus two new exports (`reconcile_decomposition`, `ReconciliationResult`) from `agentic_data_contracts.validation`; nothing existing changes behavior, and no new dependencies are added.

### Internal

- New `validation/reconciliation.py`, built across 3 TDD tasks (red-first) with a per-task spec+quality review, a final whole-branch review, and a workflow-backed code review whose findings were folded in (NaN/±inf and empty-result measurements routed to the missing-value path with accurate reasons; operator/arity validated before any query; three result constructions collapsed into one helper). The operator vocabulary (`VALID_OPERATORS` / `_BINARY_OPERATORS`) is reused from `semantic.base` as the single source of truth via a cycle-safe function-local import. Includes a table-backed population-mismatch integration test. Full suite green (756 tests); `ruff` / `ruff format` / `ty` clean.

## [0.28.1] - 2026-07-19

### Fixed

- **`dump_semantic_source` now omits empty `decompositions` / `drill_by`.** 0.28.0 emitted both keys on *every* metric even when empty, which (a) diverged from the omit-when-empty convention the tools layer already uses (`_metric_details`) and (b) changed a frozen contract's `contract_digest` for a source that declares no decompositions — re-freezing a pre-0.28 contract produced a different content address than it had under 0.27.x. A leaf metric now dumps byte-identically to the pre-0.28 format, so `contract_digest` is stable across the upgrade for contracts that don't use the new fields. Metrics that *do* declare decompositions/drill_by are unaffected (the keys are still emitted and round-trip through `from_raw`).

### Docs

- The `revenue_agent` and `growth_agent` examples now demonstrate the 0.28.0 feature. `revenue_agent` declares a `product` identity (`total_revenue = active_customers × revenue_per_customer`) plus a `region` drill dimension; `growth_agent` declares a `ratio` identity on `conversion_rate` which — combined with its existing causal impact edge — exercises a *mixed* identity + influence graph, the case `trace_metric_impacts`'s `kinds` filter is built for.
- Filled decomposition gaps left in `docs/architecture.md` by the 0.28.0 doc pass: the 9-tools list now shows `trace_metric_impacts`'s `kinds` argument and its dual-edge-kind walk, `lookup_metric` notes the surfaced `decompositions` / `drill_by`, and the `MetricDefinition` field enumeration includes both new fields.

## [0.28.0] - 2026-07-19

### Added

- **Metric identity decomposition.** A metric can now declare `decompositions` — arithmetic identities describing how its value is exactly reconstructed from other metrics via `sum`, `product`, `ratio`, or `difference` (e.g. `total_revenue = product(paying_customers, arpu)`). Unlike the *causal* `metric_impacts` graph (evidential and non-exhaustive), an identity decomposition is *exact and exhaustive* — the operands fully reconstruct the parent — so an agent doing root-cause analysis can walk the arithmetic skeleton deterministically before reaching for speculative drivers. Decompositions are validated loudly at load time, on both file load and frozen-contract `from_raw` rehydration: unknown operator, wrong operand arity (`ratio`/`difference` are binary; `sum`/`product` take two or more operands), an operand that doesn't resolve to a declared metric, and any cycle — identity edges must form a DAG, so a metric cannot transitively decompose into itself. A metric with no decomposition is a valid leaf.
- **Dimensional drill hints via `drill_by`.** A metric can declare `drill_by`: a priority-ordered list of dimensional slice hints (`dimension` name + `schema.table.column`) naming the exhaustive cuts (`revenue GROUP BY region`) that dominate weekly-review diagnosis. Columns are *soft*-validated — a malformed `schema.table.column` shape always raises, but a reference to an as-yet-undeclared table is skipped, since table schemas are optional in a contract.
- **`trace_metric_impacts` walks both edge kinds.** Decomposition operands become `IdentityEdge`s that share the metric graph with the existing `MetricImpact` (influence) edges. `trace_metric_impacts` now traverses both, tags each edge with its `kind` (`identity` carries the producing `operator`; `influence` keeps `direction`/`confidence`/`evidence`), and takes a new `kinds` argument (`all` | `identity` | `influence`, default `all`) so an agent can walk the deterministic identity skeleton first, then the causal drivers. `lookup_metric` surfaces a metric's `decompositions` and `drill_by` directly (omitted when empty).

### Compatibility

- **Fully backward compatible.** `decompositions` and `drill_by` are new optional fields that default to empty; existing contracts, and dbt/Cube-sourced metrics (which don't populate them), behave identically. Extraction from dbt/Cube, an execution-based reconciliation check (parent vs. children), and a variance-diagnosis tool are deliberately deferred — today the two fields are `YamlSource`-only, declared directly in contract YAML.

### Internal

- New `Decomposition` / `DrillDimension` / `IdentityEdge` dataclasses and the `MetricEdge` union in `semantic/base.py`; `build_metric_impact_index` / `walk_metric_impacts` generalized to the union (BFS logic unchanged, type annotations widened); new pure helpers `identity_edges_from_metrics`, `validate_decompositions`, `validate_drill_by`; both fields round-trip through `dump_semantic_source` / `from_raw` for frozen contracts. Built across 10 TDD tasks (red-first) with a per-task spec+quality review and a final whole-branch review; full suite green (722 tests), `ruff` / `ruff format` / `ty` clean.

## [0.27.0] - 2026-06-28

### Added

- **Portable, self-contained contracts via `DataContract.freeze_semantic_source()`.** Snapshots the contract's semantic source *inline* (metrics, relationships, metric-impacts, and table column-schemas) so a serialized contract carries its own semantics and enforces identically on any machine — with no filesystem access to the original dbt/Cube/YAML source. Freezing clears the machine-specific external `path` and normalizes `type` to `yaml`, so the frozen artifact's content address (`contract_digest`) is reproducible across machines and leaks no local filesystem paths into a published catalog. `load_semantic_source()` prefers the inline snapshot over `path`. New `YamlSource.from_raw(dict)` and `semantic.base.dump_semantic_source(source)` are an inverse pair (the latter backed by a new `SemanticSource.get_table_schemas()`), so frozen snapshots are *source-type-agnostic* — a contract authored against dbt or Cube freezes into the same canonical YAML-source shape.
- **ARD publish path — new `agentic_data_contracts.ard` module.** `build_catalog_entry(contract, ...)` emits a spec-valid [Agentic Resource Discovery](https://agenticresourcediscovery.org/) `ai-catalog.json` entry for a contract-governed MCP server, with the frozen contract pinned as a digest-addressed `data-contract` attestation in the trust manifest (`identity` equals the entry `identifier`, per the AI Catalog spec). `build_ai_catalog(entries, ...)` assembles the top-level `/.well-known/ai-catalog.json` document. `contract_canonical_bytes()` / `contract_digest()` produce the portable, content-addressable artifact a consumer independently recomputes — so the publish→verify loop closes with no trust in the publisher's assertion. ARD does publisher *authentication*; the `data-contract` attestation is the hook for the per-operation *authorization* ARD itself leaves open. (`data-contract` is a custom attestation type — the spec's `attestations[].type` is an open string — not yet a registered well-known value.)

### Compatibility

- **Breaking (fail-loud): a declared-but-unavailable semantic source now raises `SemanticSourceUnavailableError`, not a bare `FileNotFoundError`.** When a contract declares a semantic source that cannot be loaded — missing file, a directory path, a permission error, or a malformed YAML/Cube or dbt-manifest source — `load_semantic_source()` (and therefore `create_tools` and every adapter) fails closed with a new governance-specific error. It is deliberately **not** a `FileNotFoundError` subclass, so an application's generic file-error handling cannot swallow it and fall through to silent under-enforcement (dropped relationship/metric enforcement and dark discovery tools). **Migration:** code that catches `FileNotFoundError` around contract/tool construction should catch `SemanticSourceUnavailableError` (exported at the top level).
- **`SemanticSource.path` is now optional**, but a source must declare **either** `path` **or** `inline` — a model validator rejects one with neither at load time. A frozen contract carries only an `inline` snapshot (no `path`).
- Contracts without a semantic source, or with a loadable one, are unaffected. `freeze_semantic_source()` is opt-in: a no-op until called.

### Internal

- New top-level exports: `SemanticSourceUnavailableError`, `build_catalog_entry`, `build_ai_catalog`, `contract_digest`, `contract_canonical_bytes`.
- New test suites, built TDD red-first: `tests/test_portability/` (a frozen contract's enforcement survives serialize→rehydrate; an unfrozen one fails closed), `tests/test_ard/` (entry shape, trust-manifest identity binding, the attestation digest closing the publish→verify loop, path-independent digests, and the full publish → consumer-verify → enforce flow), `tests/test_tools/test_fail_closed_semantic_source.py`, plus `freeze`/fail-closed/table-capture cases in `tests/test_core/`.
- Hardened via two independent code-review passes of the branch (a high-effort multi-agent review and a final senior-reviewer pass): freeze now clears the machine-specific path and normalizes type (reproducible, leak-free content addresses), captures table column-schemas (`describe_table` keeps authored descriptions off-box), no longer crashes under `force=True` on inline-only contracts, and fails closed on every unloadable-source error (OSError, malformed YAML, malformed dbt JSON); `path` becoming optional is guarded in the prompt renderer and the `ai-agent-contracts` bridge; a `SemanticSource` model validator requires path-or-inline; and the published content-address bytes use the documented YAML aliases (`schema`, not the `schema_` field name). Full suite green (688 tests); `ruff`, `ruff format`, and `ty` clean.

## [0.26.0] - 2026-06-27

### Changed

- **Domain membership is now metric-first — `Domain.metrics` removed.** A metric declares the domains it belongs to via `MetricDefinition.domains` (read from `meta.domains` by `YamlSource`, `DbtSource`, and `CubeSource` alike); the contract's `Domain` model now carries only *catalog metadata* — `name`, `summary`, `description`, `tables`, `business_owner`, `operational_owner`, `last_reviewed` — and no longer lists its metrics. The metric is the single source of truth for membership, so the contract and the semantic source can no longer disagree (the old shape let them drift). A new pure helper `metrics_in_domain(metrics, domain_name)` in `semantic/base.py` is the canonical reverse-lookup, used by `lookup_domain` and the `list_metrics(domain=...)` filter. The previous `_effective_domains` union shim — which reconciled the contract-side `Domain.metrics` with `metric.domains` — is gone.
- **The domain catalog is authoritative for which domains exist.** Metrics declare *membership* in catalog domains; a domain a metric references but the contract does not catalog is not navigable. `list_metrics(domain=...)`, `lookup_domain`, and the system-prompt `<available_domains>` index now share one notion of "known domain" (the catalog), so the agent never gets contradictory guidance.

### Compatibility

- **Breaking (fail-loud): `Domain.metrics` is no longer a field, and `Domain` now sets `extra="forbid"`.** A pre-0.26 contract that still lists `metrics:` under a domain raises a `ValidationError` at load time (rather than silently dropping the key and leaving the domain with no discoverable members). Migration is mechanical: delete the `metrics:` lines from your contract's `domains:` block and ensure each metric declares its domain in the semantic source via `domains: [...]` (all built-in adapters already read it; metric-first contracts already work unchanged).
- **Behavior changes in the agent-facing tools.** `lookup_domain(name)` now returns members reverse-looked-up from `metric.domains`, so it surfaces *every* metric that declares the domain (previously only those hand-listed in `Domain.metrics`, which could drift out of sync). With no semantic source configured, `lookup_domain` returns an empty member list — membership lives on the metric, which lives in the source. The startup "Domain references unknown metric" warning is *replaced* by its metric-first mirror: a metric self-declaring a domain absent from the contract's catalog (a typo or rename) now logs a warning. The "references unknown table" warning is unchanged.
- **Prompt index `metric_count` is now omitted when zero** (a cataloged domain with no declaring metric, or no semantic source) rather than rendered as `metric_count="0"` — the `<available_domains>` entry shows name + summary and the agent uses `lookup_domain` to drill in.
- **The `[pydantic-ai]` extra now requires `pydantic-ai-slim[anthropic]>=2.0.0`** (was `>=1.107.0`). The adapter is verified against 2.x and the lockfile resolves 2.0.0, so the declared floor now matches what's actually tested. Installs that pinned `pydantic-ai-slim<2` must upgrade to use this extra.

### Internal

- Removed `_effective_domains` and all `Domain.metrics` plumbing from `tools/factory.py`; `_metric_details` no longer takes a `contract_domains` argument.
- Per-domain `metric_count` (system prompt and `lookup_domain` fuzzy fallback) is now tallied in a single `Counter` pass via the `domain_metric_counts(metrics)` helper instead of an O(domains × metrics) reverse-lookup — `to_system_prompt` runs on every agent invocation, and this library targets large catalogs. The helper de-duplicates a metric's domain tags so the displayed count always equals `len(metrics_in_domain(...))`. `core/prompt.py` keeps `core` decoupled from `semantic/` at module scope.
- Docs rewritten to teach metric-first throughout: `README.md` ("How It Works" two-file split, "Defining domains", bidirectional metric↔domain navigation), `docs/architecture.md` (Design Decisions row + a metric-first note in the Semantic Layer section). All three `examples/*/contract.yml` drop the redundant `metrics:` from their domains (membership already lives in each `examples/*/semantic.yml`).
- New `tests/test_semantic/test_domain_membership.py` covers `metrics_in_domain` and `domain_metric_counts` (including the de-dupe invariant); new tests for `extra="forbid"`, catalog-authoritative `list_metrics`, and the metric→uncataloged-domain warning. The obsolete "warns on unknown domain metric" and "merges contract Domain.metrics" union-shim tests are removed. Full suite green (663 tests); `ruff`, `ruff format`, and `ty` clean.
- Hardening above was driven by two code-review passes over the branch (a multi-agent automated review and a follow-up reviewer subagent): correctness/migration items (`extra="forbid"`, catalog-authoritative consistency, the metric-first validation warning) and efficiency cleanups were applied; a `lookup_domain` "standalone fallback" finding was correctly refuted in verification.
- `prek autoupdate`: pinned `ruff-pre-commit` `v0.15.15 → v0.15.20` (aligned with the `uv`-resolved ruff).
- The CI early-warning job (renamed `test-pydantic-ai-v2` → `test-pydantic-ai-latest`) now resolves the newest release allowed by the `>=2.0` floor (the next 2.x, and the next major once it ships) instead of a `<3`-capped 2.x, so it keeps catching upstream breaks ahead of users. Still not a release gate.
- **Dependencies upgraded via `uv lock --upgrade`.** Notably `pydantic-ai-slim` / `pydantic-graph` `1.107.0 → 2.0.0` (a major bump) — the Pydantic AI adapter and its 26 tests pass unchanged against v2.x, confirming what the v0.24.0 early-warning CI job was added to watch. Also refreshed: `anthropic` `0.111.0 → 0.112.0`, `claude-agent-sdk` `0.2.105 → 0.2.110`, `langchain` `1.3.10 → 1.3.11`, `sqlglot` `30.11.0 → 30.12.0`, `ruff` `0.15.18 → 0.15.20`, and assorted transitive bumps.

## [0.25.0] - 2026-06-27

### Added

- **Dual-role ownership on metrics and domains.** `MetricDefinition` and `Domain` gain optional `business_owner` and `operational_owner` fields — the business owner owns the *definition* and its review cadence; the operational owner owns *data health* (DQ, backfills). Owners are teams, not individuals (a convention, not validated — owners must outlive any one person). Read from the YAML semantic source and from the contract's `domains:`; `DbtSource` / `CubeSource` leave them unset (graceful defaults, deferred). Inspired by Lyft's Metric Semantic Layer governance model.
- **Per-metric review timestamps + metric staleness.** `MetricDefinition` gains an optional `last_reviewed: date` (joining the existing field on `Domain` and `MetricImpact`). `find_stale_reviews()` / `DataContract.find_stale()` now evaluate metrics as a third artefact kind, so findings come back as `domain` / `metric` / `metric_impact`. Every domain and metric finding carries its owners in `context` (`business_owner` / `operational_owner`) so the audit report says *who to nag*. A new pure helper `review_age_days(last_reviewed, as_of)` centralises the age arithmetic shared by the detector and the tool layer.
- **Owners + freshness surfaced in the agent-facing tools.** `lookup_metric` and `lookup_domain` now include `business_owner` / `operational_owner` and a `last_reviewed` + `stale` pair; `list_metrics` carries a lean `stale: true` flag (only when stale). `create_tools(...)` gains a `staleness_threshold_days: int = 90` knob. **Two audiences, two policies:** `find_stale()` is the strict audit path (missing `last_reviewed` = stale), while the tools are lenient — they emit `last_reviewed`/`stale` only when a review date is actually set, so contracts that never adopted the field get no false "stale" noise at query time.
- **17 tests** across `tests/test_core/test_domain_model.py` (domain owners), `tests/test_semantic/test_yaml_source.py` (parsing the three new fields + None defaults), `tests/test_core/test_staleness.py` (metric staleness: missing / fresh / beyond-threshold, owner context for metric and domain findings, `find_stale` pulling metrics from a source), and `tests/test_tools/test_factory.py` (owners + freshness on `lookup_metric` / `lookup_domain`, the lenient omit-when-unset path, the `list_metrics` stale flag, and `staleness_threshold_days` control).

### Compatibility

- **Additive API.** All new fields are optional with `None` defaults; `find_stale_reviews` gains an optional keyword-only `metrics=` parameter (defaults to `None`), and `create_tools` gains an optional `staleness_threshold_days` keyword. Existing contracts, semantic sources (including dbt/Cube), and call sites compile and run unchanged. The full pre-existing test suite passes.
- **Behavior change in `DataContract.find_stale()`.** It now also audits metrics, so any metric whose source does not set `last_reviewed` is reported as a new `metric`-kind finding (`age_days=None`) on first run — the same "missing = stale" forcing function already applied to domains and metric-impacts. This is intentional, but a consumer that gates on a non-empty result (e.g. a CI check that fails when `find_stale()` returns anything) will newly fire for every un-reviewed metric. To grandfather existing catalogs during rollout, either back-fill `last_reviewed` or filter with `f.age_days is not None`. dbt/Cube sources do not yet parse `last_reviewed` on metrics, so all their metrics report as un-reviewed until that lands.

### Internal

- Documentation updated: `README.md` (semantic-source YAML schema + ownership/cadence note + tools table), `docs/architecture.md` (Governance Staleness section, `MetricDefinition` field list, the two-audiences policy). All three `examples/*/semantic.yml` now declare owners + `last_reviewed` on their metrics, showcasing the feature and keeping the `find_stale` demos focused.

## [0.24.0] - 2026-06-20

### Added

- **Deps-aware Pydantic AI toolset — one shared `Agent` for many users.** New `create_pydantic_ai_toolset(contract, ...)` returns a Pydantic AI `ToolsetFunc` you register on a *single* shared agent via the public `agent.toolset(...)` API. On each run it reads a per-user `ContractDeps` (new dataclass: `session` + `caller_principal`) from `RunContext.deps` and rebuilds the contract's 9 tools bound to that user's `ContractSession` and principal. This delivers the multi-user memory profile the v0.23.0 baked-in path could not: build the `Agent` **once**, and each user is just a `message_history` + a small `ContractDeps`, instead of a separate per-user tools list (and the Claude Agent SDK's per-session subprocess is gone entirely). The **caller owns** each user's `ContractSession` (created once per user, passed on every turn so cumulative limits accumulate); the toolset never creates sessions. Enforcement is unchanged from `create_pydantic_ai_tools` — a validation block becomes `ModelRetry`, a session-budget breach the terminal `ContractSessionLimitError` — and now applies the *correct* per-user principal on each run.
- **`caller_principal` passthrough on `create_pydantic_ai_tools`.** The v0.23.0 function now accepts a `caller_principal` keyword and threads it to `create_tools`, so per-principal table/rule gating applies in the baked-in path too (previously it was silently dropped).
- **Tests** covering the `caller_principal` passthrough, that `create_pydantic_ai_toolset` returns a registrable factory, multi-user **session isolation** — both directly and **end-to-end through one shared `Agent` via `agent.run()`** (user A exhausting their budget does not affect user B on the same agent), **per-principal gating** via `deps`, blocked-SQL → `ModelRetry` through the toolset, and clear errors on both mis-wiring branches (non-`ContractDeps`, and `ContractDeps` with `session=None`). `ContractDeps` and `create_pydantic_ai_toolset` are re-exported from the package root behind the `[pydantic-ai]` import guard.

### Compatibility

- **Purely additive.** `tools/factory.py` and the v0.23.0 `create_pydantic_ai_tools` behaviour are unchanged except the new optional `caller_principal` keyword (defaults to `None` = prior behaviour). No new dependency — the deps-aware path reuses `pydantic-ai-slim`. The full pre-existing test suite passes unchanged.

### Internal

- Uses Pydantic AI's built-in dynamic-toolset mechanism via a `ToolsetFunc` registered with the public `agent.toolset(...)` — deliberately **not** a hand-rolled `AbstractToolset` subclass, which would need fragile run-scoped caching to keep `get_tools()` and `call_tool()` consistent. Register with `agent.toolset(per_run_step=False)(create_pydantic_ai_toolset(...))`: the deps (session, principal) are stable within a run, so the factory need only run once per run rather than once per model step (the docstring and README show this form). The 9 tools + a `Validator` are rebuilt per run (no I/O); the shared config — adapter connection pool, semantic source — stays shared across all users.

## [0.23.0] - 2026-06-20

### Added

- **Pydantic AI adapter — `create_pydantic_ai_tools(contract, ...)`.** A third agent-framework adapter (alongside `create_sdk_mcp_server` and `create_langchain_tools`) that wraps a contract's 9 governed tools as a list of `pydantic_ai.Tool`, ready to drop into `pydantic_ai.Agent(tools=...)`. It reuses the framework-neutral `ToolDef` output of `create_tools` via `pydantic_ai.Tool.from_schema`, so the contract's JSON Schemas reach the model verbatim with no Pydantic re-synthesis. Enforcement is applied in-tool: each wrapped tool pre-checks `ContractSession` limits (unless `apply_middleware=False`) and the underlying callables self-validate SQL. Install with the new `[pydantic-ai]` extra (`pip install "agentic-data-contracts[pydantic-ai]"`, which pulls `pydantic-ai-slim[anthropic]`).
- **Recoverable-vs-terminal enforcement mapped onto Pydantic AI's error contract.** A validation block (bad SQL, forbidden operation, missing required filter, failed result-check, or a per-caller permission gate) surfaces as `pydantic_ai.ModelRetry`, so the model can rewrite its arguments or switch tools and try again. A *session-budget* breach (`max_retries` / `max_duration` / cost) surfaces as the new terminal `ContractSessionLimitError` instead — retrying cannot recover an exhausted cap, so the run ends rather than burning further model retries. This is finer-grained than the SDK / LangChain adapters, which collapse both signals into one transport error. The distinction holds even under `apply_middleware=False`, where `run_query`'s own limit check self-emits the `BLOCKED — Session limit exceeded` envelope.
- **`ContractSessionLimitError` in `core.session`.** A new terminal-budget error type defined alongside `LimitExceededError`. `LimitExceededError` remains the *internal* signal raised by `ContractSession.check_limits()`; `ContractSessionLimitError` is the *terminal* error adapters raise to propagate a breach out of an agent run. It lives in the core layer so the terminal-error vocabulary is shared across adapters rather than transport-specific.
- **18 tests in `tests/test_tools/test_pydantic_ai.py`** covering tool shape, schema-verbatim registration, the `BLOCKED — → ModelRetry` mapping, the terminal `ContractSessionLimitError` path (including the `apply_middleware=False` regression guard), `inspect_query` reporting violations without raising, the `_unwrap_mcp_text` helper, and an end-to-end agent run through Pydantic AI's real invocation machinery via `TestModel`. `create_pydantic_ai_tools` is also re-exported from the package root behind a lazy import guard, and `tests/test_public_api.py` now covers both the new export and `ContractSessionLimitError`'s core-layer location.

### Compatibility

- **Purely additive — no API change.** No existing source file's behaviour changed: `tools/sdk.py`, `tools/langchain.py`, `tools/factory.py`, and `tools/middleware.py` are untouched. The only edits to shipped modules are a new export guard in `__init__.py` and the additive `ContractSessionLimitError` definition in `core/session.py`. The full pre-existing test suite passes unchanged.
- **The `[pydantic-ai]` extra is optional.** Base installs gain no new required dependency; `from agentic_data_contracts import create_pydantic_ai_tools` resolves to `None` when the extra is absent — exactly like `create_langchain_tools` — so importing the package on a base install keeps working.

### Internal

- New `pydantic-ai` extra wired into `pyproject.toml` (added to `all` and `dev`). Documentation updated: `README.md` (integration section + Optional Dependencies table), `docs/architecture.md` (module tree + dependencies block), and `CLAUDE.md` (adapter list).

## [0.22.0] - 2026-06-02

### Fixed

- **Async enforcement paths no longer block the event loop on synchronous adapter I/O.** The `async def` handlers across the tools layer called **synchronous**, blocking `DatabaseAdapter` / `ExplainAdapter` methods directly on the event-loop thread. A single slow query (tens of seconds, common for analytical warehouses) therefore stalled the **entire asyncio event loop of the host process** — freezing every other concurrent coroutine: other sessions' tool calls, health-check probes, and anything else sharing the loop. This is invisible at low concurrency but becomes the dominant latency multiplier once multiple sessions share one host process (e.g. a shared in-process MCP server backing a multi-user bot). Each blocking round-trip is now offloaded via `asyncio.to_thread(...)` in every async path:
  - **`tools/factory.py`** — `adapter.execute` (in `run_query` and `preview_table`), `adapter.describe_table` (in `describe_table`), and `validator.validate`'s EXPLAIN dry-run (`explain_adapter.explain`, used by `run_query` and `inspect_query`).
  - **`tools/middleware.py`** — `contract_middleware`'s async wrapper now offloads `validator.validate`.
  - **`tools/langchain.py`** — `ContractMiddleware.awrap_tool_call` now offloads its `_check` (which runs `validator.validate`); the synchronous `wrap_tool_call` path is unchanged.
- **`DuckDBAdapter` is now safe under the concurrency the offloading enables.** A single DuckDB connection is not safe for concurrent queries across threads, and the new `asyncio.to_thread` offloading lets multiple sessions land in `execute` / `explain` / `describe_table` on different worker threads at once. The adapter now guards every access to its shared connection with a `threading.Lock`, so concurrent calls serialize on the connection instead of interleaving and corrupting each other's result state. DuckDB still parallelizes the work of an individual query internally; the lock only prevents two queries from interleaving on the same connection.

### Compatibility

- **No API change.** The synchronous `DatabaseAdapter` / `ExplainAdapter` protocols are unchanged — this is a purely internal change to how the async handlers invoke them, so existing consumers (and custom adapters) need no changes.
- **Connection pool, not thread pool, remains the concurrency gate.** `asyncio.to_thread` uses the event loop's default `ThreadPoolExecutor`; concurrent DB work stays bounded by the adapter's own connection pool. Consumers should size that pool to the concurrency they want to support (now documented in `docs/architecture.md`).

### Added

- 4 tests in `tests/test_tools/test_event_loop.py` asserting that `run_query`, `inspect_query`, `describe_table`, and `preview_table` execute their blocking adapter calls on a worker thread rather than the event-loop thread.
- Matching off-loop tests for the other two async enforcement paths: `test_middleware_offloads_validate_off_event_loop` (`contract_middleware`) and `test_contract_middleware_offloads_validate_off_event_loop` (LangChain `ContractMiddleware.awrap_tool_call`).
- `test_execute_serializes_concurrent_connection_access` in `tests/test_adapters/test_duckdb.py`, which fires 8 concurrent `execute` calls through `asyncio.to_thread` and asserts the adapter lock holds peak connection concurrency at 1.

## [0.21.1] - 2026-05-30

### Added

- **Reference template for layering Anthropic's [`data` knowledge-work plugin](https://github.com/anthropics/knowledge-work-plugins/tree/main/data) on top of contract-governed tools.** All three example agents (`revenue_agent`, `growth_agent`, `ops_agent`) now carry an opt-in overlay, off by default and enabled by pointing `DATA_PLUGIN_PATH` at a local plugin checkout. The agent gains the plugin's tool-agnostic analyst *skills* (`validate-data`, `statistical-analysis`, `explore-data`, `sql-queries`) while every query it runs is still enforced by the contract — the plugin's skills drive "whatever warehouse tool is connected," which in-process is the governed `create_sdk_mcp_server` server. `growth_agent/agent.py` is the canonical, fully-commented template; the README gains a "Layer Anthropic's `data` plugin on top" subsection under the Agent SDK usage docs.
- **The security-critical piece is `strict_mcp_config=True`.** Loading a plugin would otherwise also activate the warehouse servers in its bundled `.mcp.json`, giving the agent an ungoverned path around the contract. `strict_mcp_config` restricts the session to *only* the servers passed via `mcp_servers`, so the plugin's skills load but its `.mcp.json` warehouse servers stay inert. The curated skill list deliberately omits `data-context-extractor` (it generates a parallel semantic skill that competes with the contract as the source of metric truth) and the viz/dashboard skills (they require code-execution tools the governed agents do not grant). Each agent's system prompt also gained a metric-precedence line so the plugin's "just write a query" instinct does not undercut the governed semantic layer.

### Compatibility

- **No library API change.** This release touches only `examples/`, `README.md`, the dependency lockfile, and tooling config — nothing under `src/`. Consumers of the importable library see no behavioural change; hence a patch bump.
- **Opt-in and degrades gracefully.** The overlay is inert unless `DATA_PLUGIN_PATH` is set to a valid plugin directory, and the example feature-detects `plugins` / `skills` / `strict_mcp_config` on `ClaudeAgentOptions` before using them — so the examples still run unchanged on older `claude-agent-sdk` versions and with zero external setup.

### Internal

- `uv lock --upgrade` refreshed all dependencies. Headline bump: **`claude-agent-sdk 0.1.81 → 0.2.87`** (a 0.1→0.2 minor jump); the overlay-relevant `ClaudeAgentOptions` surface (`plugins`, `skills`, `strict_mcp_config`, the `claude_code` system-prompt preset) and `create_sdk_mcp_server` were verified intact across the jump. Other notable bumps: `langchain 1.3.0 → 1.3.2`, `langgraph 1.2.0 → 1.2.2`, `duckdb 1.5.2 → 1.5.3`, `snowflake-connector-python 4.5.0 → 4.6.0`, `mcp 1.27.1 → 1.27.2`, `ruff 0.15.13 → 0.15.15`, `cryptography`/`boto3`/`starlette`/`uvicorn` patch/minor refreshes. Full 602-test suite + ruff + ty all green against the new versions.
- `.pre-commit-config.yaml`: `ruff-pre-commit` rev bumped `v0.15.13 → v0.15.15` (via `prek autoupdate`) to match the lockfile-pinned `ruff` binary, preserving the local-vs-hook alignment.

## [0.21.0] - 2026-05-17

### Fixed

- **`describe_table` now emits column descriptions to the agent.** Since the tool factory's first commit (`0296613`), the tool serialised columns as `{name, type, nullable}` only — `Column.description` was silently dropped on the way out, even when populated by the adapter (e.g., a Denodo deployment carrying authored catalog comments) or available in the contract's semantic source. This is the single largest *context* improvement a data-contract library can make: per the [Datacult "boring work" benchmark](https://www.datacult.com/post/the-boring-work-that-makes-ai-analytics-actually-work-why-winning-with-ai-in-analytics-is-an-investment-in-a-rich-data-context-not-better-llm-models), adding column descriptions moved an agent's SQL accuracy from 0% to 15% and SQL generation from 38.5% to 100% — the largest jump in their six-layer experiment. The fix overlays descriptions onto the tool response with this precedence: (1) semantic source via `SemanticSource.get_table_schema(schema, table)`, which is the canonical agent-facing authority; (2) `Column.description` from the adapter, which captures warehouse catalog comments; (3) field omitted entirely when both are empty, keeping responses tight.
- **The `SemanticSource.get_table_schema` protocol method is no longer dead code from the tool layer's perspective.** All three built-in semantic sources (`YamlSource`, `DbtSource`, `CubeSource`) already populated `TableSchema.columns[*].description` from their respective inputs; the tool just never consulted them. Now it does.

### Added

- 3 new tests in `tests/test_tools/test_factory.py` covering the merge behaviour: `test_describe_table_includes_semantic_descriptions` (semantic-source descriptions reach the agent), `test_describe_table_falls_back_to_adapter_description` (adapter-supplied descriptions surface when the semantic source has no entry, and the field is omitted when both are empty), and `test_describe_table_semantic_overrides_adapter_description` (semantic source wins when both have descriptions for the same column).

### Compatibility

- **Backward-compatible response shape.** The new `description` field is *additive only* — consumers that ignore unknown keys see no behaviour change. The field is omitted (not set to `""`) when no description exists, so JSON payload size is unchanged for description-less columns.
- **No new failure modes.** The merge guards `semantic_source is None`, `get_table_schema(...)` returning `None`, columns appearing in one source but not the other, and empty-string descriptions. A column described in the semantic source but absent from the warehouse is silently dropped — the adapter's column list is the source of truth for *which* columns exist; the semantic source only adorns them.
- **No new dependencies.** The fix uses interfaces that already existed in the codebase.

### Internal

- `uv lock --upgrade` refreshed transitive dependencies (notable bumps: `sqlglot 30.6.0 → 30.8.0`, `langchain 1.2.17 → 1.3.0`, `langgraph 1.1.10 → 1.2.0`, `pydantic 2.13.3 → 2.13.4`, `cryptography 47.0.0 → 48.0.0`). Full 602-test suite + ruff + ty all green against the new versions.
- `.pre-commit-config.yaml`: `ruff-pre-commit` rev bumped to `v0.15.13` to match the lockfile-pinned `ruff` binary, preventing the silent local-vs-hook drift where the same file passes `uv run ruff` but a stale hook env flags it.

## [0.20.0] - 2026-05-10

### Changed

- **`create_sdk_mcp_server` now auto-applies session-limit enforcement to all 9 tools by default**, matching the v0.19.0 behavior of `create_langchain_tools`. Pre-v0.20.0, only `run_query` self-checked `ContractSession` limits — lookup tools (`describe_table`, `list_metrics`, etc.) bypassed. The two adapters now behave identically: a single contract YAML enforces the same way under SDK and LangChain.
- **Practical effect**: `max_duration_seconds` now measures wall-clock from the *first tool call* (any tool), not just from the first `run_query`. For most contracts this is invisible — lookups complete in milliseconds. The narrow population that sees a behavior change: agents with tight `max_duration_seconds` AND lookup-heavy prompts that browse extensively before querying. The new behavior matches the YAML's documented intent ("the agent has N seconds total"), and closes the runaway-loop gap where an agent stuck on lookup tools previously bypassed the duration cap.
- **Escape hatch**: pass `apply_middleware=False` to `create_sdk_mcp_server` to restore pre-0.20.0 behavior.

### Added

- New `_wrap_with_session_check(inner, session)` helper in `tools/sdk.py` — exported as a private symbol so tests can verify the enforcement wrapper directly without going through the SDK's `@tool` decorator. Mirrors the in-tool enforcement pattern in `tools/langchain.py:_to_structured_tool`.

### Fixed

- Wrapper-emitted BLOCKED envelopes now include the canonical `Remaining: {budget}` suffix that `run_query`'s self-emitted blocks have always carried (per `factory.py:627-628`). Pre-0.20.0 the LangChain wrapper (introduced in v0.19.0) and the new SDK wrapper both omitted this suffix, so agents whose retry-planning logic depended on the suffix would lose context once they hit the wrapper layer instead of `run_query`'s own block. Applies to both `_wrap_with_session_check` (SDK) and `_to_structured_tool` / `ContractMiddleware._check` (LangChain).

### Compatibility

- Public API unchanged — `create_sdk_mcp_server` gains one optional kwarg with a sensible default. Pre-built `ToolDef` lists, custom sessions, and all other call shapes continue to work.
- 6 new tests in `tests/test_tools/test_sdk.py` cover the wrapper behavior and the new kwarg.

## [0.19.0] - 2026-05-10

### Added

- **LangChain / deepagents integration**: new optional `[langchain]` extra exposes `create_langchain_tools()` returning a `list[BaseTool]` consumable by `deepagents.create_deep_agent(tools=...)` or any `langchain.agents.create_agent(tools=...)`. Each tool wraps the existing `ToolDef` via `StructuredTool.from_function(args_schema=<JSON Schema dict>, response_format="content_and_artifact")` — the JSON Schema reaches the agent verbatim and the original MCP envelope is preserved on `ToolMessage.artifact`. `BLOCKED —` envelopes from the underlying tools (`tools/factory.py`, `tools/middleware.py`) are surfaced as `ToolException`, which the agent loop renders as `ToolMessage(status="error")`.
- **`ContractMiddleware(AgentMiddleware)`** for graph-level enforcement. Pair with `create_langchain_tools(..., apply_middleware=False)` to avoid duplicate work when the middleware is installed at the deepagents/LangGraph boundary. Runs `Validator.validate(sql)` + `ContractSession.check_limits()` and short-circuits with `ToolMessage(status="error")`.
- **Top-level lazy re-exports** of `create_langchain_tools` and `ContractMiddleware` from `agentic_data_contracts`. The base install (without the `[langchain]` extra) keeps both names defined as `None`, so `from agentic_data_contracts import …` continues to work unchanged for existing Claude Agent SDK users.

### Notes

- Auto-applied in-tool enforcement intentionally **does not** validate SQL on every call (unlike `contract_middleware`); doing so would block `inspect_query`'s purpose of *reporting* violations as JSON. SQL validation is left to `run_query`'s self-checks at `tools/factory.py:632-672`, which still surface as `ToolException` via the canonical `BLOCKED —` prefix sniff.
- Backward-compatible: zero changes to `tools/factory.py`, `tools/sdk.py`, `tools/middleware.py`, or any existing top-level import. Pinned to current stable: `langchain-core>=1.3.3`, `langchain>=1.2.17`.

## [0.18.0] - 2026-05-02

### Added

- **Cube schema relationship parsing**: `CubeSource.get_relationships()` and `get_relationships_for_table()` no longer return empty stubs. Each cube's `joins:` block is parsed and projected into `Relationship` instances, completing the trifecta with `YamlSource` (v0.16.0) and `DbtSource` (v0.17.0) — every built-in semantic source now carries the same `preferred`, `required_filter`, and `relationship_type` semantics end-to-end.
- **`from`/`to` normalisation**: The Relationship's `from` is always the column on the cube declaring the join, regardless of which side `{CUBE}` appears on in the SQL. Authors who write `{CUBE}.fk = {Other}.pk` and authors who write `{Other}.pk = {CUBE}.fk` get identical Relationship instances. The `type` carries the cardinality (`many_to_one` / `one_to_one` / `one_to_many`), so a `hasMany` join on cube A produces `A.pk -> B.fk` with `type=one_to_many`, matching how `YamlSource` authors hand-write the same pattern.
- **Cube enum mapping**: `belongsTo` / `hasOne` / `hasMany` (camelCase v1) and `many_to_one` / `one_to_one` / `one_to_many` (snake_case v2 aliases) all collapse to the canonical `Relationship.type` strings via a small lookup table. `meta.relationship_type` overrides for unusual cases.
- **`meta:` block convention** matches `DbtSource`'s v0.17.0 contract: `meta.preferred`, `meta.required_filter`, `meta.relationship_type` on each join entry.
- **Index reuse via `build_relationship_index`** — `CubeSource` inherits the v0.16.0 preferred-first stable-sort guarantee, same as `YamlSource` and `DbtSource`.

### Documentation

- 11 new tests in `tests/test_semantic/test_cube.py` (`TestCubeRelationships`) covering: load count with one phantom-cube reference filtered, canonical edge round-trip, non-preferred default, reversed-SQL parsing (`{Other}.pk = {CUBE}.fk` form), `hasMany` -> `one_to_many` alias mapping, self-FK with `meta.relationship_type` overriding `hasOne`, preferred-first index ordering, referenced-side indexing, self-FK appearing once not twice, the original empty-joins fixture continuing to return `[]`, and unresolvable cube names being silently skipped.
- New fixture `tests/fixtures/sample_cube_schema_with_joins.yml` — Orders cube with two role-playing joins into Users (`customer_id` preferred, `sales_rep_id` not), a Customers cube with a `hasMany` join into Orders, a self-referencing Employees cube, and one deliberately broken join pointing at a non-existent cube to exercise the silent-skip path. Existing `sample_cube_schema.yml` left untouched as the empty-joins baseline.
- README's *Semantic Sources / Cube* section documents the parser convention with a `joins:` example showing all three `meta:` keys; architecture doc gains a paragraph mirroring the v0.17.0 dbt note.

### Limitations

- Composite-key joins (`{CUBE}.tenant_id = {Other}.tenant_id AND {CUBE}.id = {Other}.parent_id`) are not parsed — declare those via `YamlSource` instead. The TODO surfaces as a "skipped silently" outcome rather than an error so existing Cube schemas don't break.

## [0.17.0] - 2026-05-02

### Added

- **dbt manifest relationship parsing**: `DbtSource.get_relationships()` and `get_relationships_for_table()` no longer return empty stubs. Each `relationships` schema test in the manifest (`resource_type == "test"`, `test_metadata.name == "relationships"`) projects into a `Relationship` instance. The owner model is resolved via `attached_node` (manifest v12+); the referenced model is taken from `depends_on.nodes` minus the owner (with self-FK fallback when there's only one entry). Tests that don't carry `relationships` semantics (`not_null`, `unique`, custom test types) are silently filtered out, as are tests whose dependencies can't be resolved to a model — manifests routinely carry tests on seeds/sources we don't track.
- **`meta:`-block convention** matches the existing `_parse_metrics` pattern (`meta.tier`, `meta.domains`): three optional keys on each `relationships` test surface end-to-end through the same code paths that already serve `YamlSource`-loaded relationships:
  - `meta.preferred: bool` — canonical-join hint, propagates to the index sort, prompt rendering, and `lookup_relationships` JSON.
  - `meta.required_filter: str` — SQL predicate enforced by `RelationshipChecker`.
  - `meta.relationship_type: str` — overrides the default `many_to_one` (accepts `one_to_one`, `many_to_many`).
- **Index reuse**: `DbtSource` now builds its own `_rel_index` via `build_relationship_index`, inheriting the same preferred-first stable-sort guarantee `YamlSource` got in v0.16.0. No new helper code; the parser is the only addition.

### Documentation

- 9 new tests in `tests/test_semantic/test_dbt.py` (`TestDbtRelationships`) covering the round-trip for canonical and non-preferred edges, self-referencing FK, `meta.relationship_type` override, preferred-first index ordering, the referenced-side appearing in the index, self-FK appearing once not twice, non-`relationships` tests being filtered, and the original empty-relationships fixture continuing to return `[]`.
- New fixture `tests/fixtures/sample_dbt_manifest_with_relationships.json` — a realistic manifest v12 shape with a multi-FK `orders` model (preferred `customer_id` + secondary `sales_rep_id`), a self-referencing `employees.manager_id`, and decoy `not_null` / `unique` tests to confirm filtering. Existing `sample_dbt_manifest.json` left untouched so it stays the empty-relationships baseline.
- README's *Semantic Sources / dbt* section documents the manifest convention with a `schema.yml` example showing all three `meta:` keys; architecture doc gains a paragraph describing the resolution logic and skip semantics.

### Notes

- `CubeSource.get_relationships()` still returns `[]` — Cube's `joins:` block carries a SQL expression that needs parsing to extract column references, which is a separate piece of work. The TODO at `semantic/cube.py:73` is preserved.

## [0.16.0] - 2026-05-02

### Added

- **`preferred: bool` flag on `Relationship`**: Marks the canonical join when multiple parallel join paths exist between the same pair of tables — the role-playing-dimension and multi-role-FK case (`fact_sales → dim_date` on `order_date` / `ship_date` / `deliver_date`; `orders → users` on `customer_id` / `sales_rep_id` / `approver_id`). Default `False`. Surfaces as a per-edge `preferred="true"` attribute in the rendered prompt and as `"preferred": true` in `lookup_relationships` JSON output (omitted when false, mirroring the `required_filter` shape).
- **Index-time stable sort**: `build_relationship_index` now stable-sorts each adjacency list with preferred edges first (`edges.sort(key=lambda r: not r.preferred)`). One line of code propagates the invariant to two consumers automatically: `find_join_path` (BFS) picks the preferred edge when alternatives exist at the same hop depth, and `get_relationships_for_table` (the `lookup_relationships` direct-lookup path) returns preferred-first ordering. The flat list returned by `SemanticSource.get_relationships()` deliberately keeps declaration order — that list feeds the prompt renderer, which uses the per-edge `preferred="true"` attribute instead of reordering. Forward-compat: a future `preference: int | None` rank field can be added as a non-breaking superset, treating `preferred=True` as `preference=0`.
- **YAML loader threading**: `YamlSource` reads `preferred` from each relationship entry, defaulting to `False` when absent. The `dbt` and `cube` loaders are unchanged (they still return `[]` as TODO stubs); when those parsers are filled in, `preferred` will read from their respective `meta:` blocks.
- **`growth_agent` example demonstrates the feature end-to-end**: `analytics.events` gains a `referrer_user_id` column populated only for users with `acquisition_source = 'referral'`, creating two parallel edges into `analytics.users`. The canonical actor join (`events.user_id → users.id`) carries `preferred: true`; the referrer join is unmarked and the description steers the agent toward it only for referral-mechanics questions. Both referral users (charlie, henry) carry their referrer FK across every event row in their history, so referral attribution is observable at event grain.

### Changed

- **Authoring guidance**: When alternatives are *semantic peers* (e.g. role-playing date dimensions where `order_date` and `ship_date` are equally valid), authors should leave all edges unmarked and rely on `description` for disambiguation. The boolean form is intentional — it captures "canonical vs secondary" without inviting authors to invent false hierarchies among genuine peers. No uniqueness validator is enforced at load time (matches `AllowedTable.preferred`'s lenient handling).

### Documentation

- 11 new tests across three suites: 8 in `tests/test_semantic/test_relationships.py` (`TestPreferredRelationship` class — default value, YAML round-trip, index sort stability, `find_join_path` preference, declaration-order preservation across `get_relationships()` vs `get_relationships_for_table()`); 2 in `tests/test_tools/test_relationship_tools.py` covering JSON shape and preferred-first ordering for both direct-lookup and `join_path` modes; 1 in `tests/test_core/test_prompt_renderers.py` mirroring the `AllowedTable.preferred` rendering test pattern.
- New fixture `tests/fixtures/relationships_preferred.yml` — three parallel edges between `analytics.orders` and `analytics.users` with the preferred edge deliberately at position 2 of 3, decoupling preferred-first behaviour from declaration order so a no-op sort would fail the tests.
- README's *Table Relationships* section gains a parallel-edge YAML snippet and a `preferred` row in the field reference table covering prompt rendering, direct-lookup ordering, and BFS path-finding bias. Architecture doc's `Relationship` field listing extended with the same semantics.

## [0.15.1] - 2026-04-28

### Fixed

- **`preview_table` honoured table-level access only — bypassing per-rule data-visibility gates** ([#20](https://github.com/qye-inf/agentic-data-contracts/issues/20)). The tool ran a synthesised `SELECT * FROM <table> LIMIT N` directly through `adapter.execute()` after checking `allowed_table_names_for(principal)`, so v0.14's per-rule `blocked_columns` (with `allowed_principals` / `blocked_principals` scoping) and v0.15's `required_filter_values` per-principal value allowlist were silently skipped. Any caller allowed at the table level could read every column via preview, even columns that a `blocked_columns` rule restricted to a whitelist — and could see every row, even when a `required_filter_values` rule meant they should only see a value-bound subset.
- **Fix scope**: `preview_table` now consults the same per-rule, per-principal gates that the validator applies, classified by enforcement: `block` rules refuse the preview with a structured BLOCKED message naming the rule; `warn` / `log` rules surface `WARNINGS:` / `LOG:` preambles before the JSON body, mirroring `run_query`'s convention. Query-shape rules (`required_filter`, `no_select_star`, `require_limit`, `max_joins`) remain bypassed by design — those guard user-supplied SQL in `run_query`, not preview's auto-built discovery query. `result_check` rules are also skipped (preview executes no result-check pipeline).
- **Behavioural contract**: preview honours rules that gate which **data** an in-scope caller may see (`blocked_columns`, `required_filter_values`); it bypasses rules that gate **query shape** the caller writes. The matching predicate mirrors `Validator._is_table_in_scope` + `_rule_applies_to_principal` (validator.py:233-247) — including the `principal_in_scope` skip semantics for unidentified callers against principal-scoped rules.

### Changed

- **New module-level helper `_caller_label(principal)`** in `tools.factory`: collapses the `principal if principal else "<no caller identified>"` idiom shared by `describe_table` and `preview_table` into one place. Pure refactor; output messages are byte-identical.

### Documentation

- 9 new edge-case tests for `preview_table` covering: wildcard `table: "*"` rule, omitted `table:` (None) rule, unidentified caller against unscoped vs principal-scoped rules, `enforcement: warn` and `enforcement: log` preamble surfacing, `result_check` rules being correctly skipped, `required_filter_values` blocking when the principal is in `values_by_principal`, and falling through when the principal is unmapped. Built via small inline contracts so the shared `principals_contract.yml` fixture (used by 8 other test modules) is untouched.
- 3 new regression tests on the issue's exact alice / intern / bob scenarios.

## [0.15.0] - 2026-04-28

### Added

- **Per-principal value allowlist on WHERE filters**: New `required_filter_values` field on `QueryCheck` carries a `column` plus a `values_by_principal: dict[str, list[str | int | float]]` map. The new `RequiredFilterValuesChecker` walks the WHERE clause as a boolean tree and enforces that every literal predicate value pinning the column is in the calling principal's allowlist. Composes with the existing `allowed_principals` / `blocked_principals` rule scoping; principals not in the value map fall through (rule is a no-op for them — pair with `allowed_principals` for hard deny on unknown callers). This closes the value-bound counterpart to `required_filter`: column-presence is no longer enough, the *values* must also match.
- **Two-layer static analysis**: A literal-set guard collects every literal value referenced against the target column anywhere in the AST and rejects values outside the allowlist regardless of AND/OR structure (catches cross-alias smuggling like `t1.account_id = 123 AND t2.account_id = 999` and contradictions). A coverage analysis (`_Coverage` state machine) then enforces that the column is actually pinned at all (catches `account_id = 123 OR amount > 0`). Non-literal predicates on the target column — subqueries, function calls, BETWEEN, range comparisons (`<`, `>`, `!=`, `LIKE`), NOT-wrapped EQ/IN — are rejected as unprovable. `IS NULL` / `IS NOT NULL` are treated as presence predicates (UNBOUND) so defensive `IS NOT NULL AND col = 123` patterns pass cleanly.
- **Numeric form normalisation** via `_canon`: YAML int `123`, YAML float `123.0`, SQL literal `123`, SQL literal `123.0`, and string `"123"` all collapse to the same canonical key. Scientific notation (`1e3`/`1000`), integer-valued floats, and bool literals round-trip stably.
- **Principal-aware system-prompt rendering**: `to_system_prompt(principal=...)` and `ClaudePromptRenderer.render(principal=...)` now filter `required_filter_values` to expose only the calling principal's allowlist. Other principals' value lists never appear in the prompt.
- **`partner_customer_scope` rule in the `revenue_agent` example** demonstrates the feature end-to-end with two external partners scoped to different account allowlists. Deliberately *not* table-scoped — per-value rules apply globally so a partner can't bypass them via a join to a sibling table that exposes the same column.

### Changed

- **Breaking**: `PromptRenderer.render()` protocol signature gained a third optional argument `principal: str | None = None`. Custom renderers written against the prior 2-arg signature (`def render(self, contract, semantic_source=None)`) will raise `TypeError` when called via `to_system_prompt`, which now always passes `principal=` as a keyword argument. **Migration**: add `principal=None` to the renderer's signature; if you don't need per-principal filtering, ignore the value.
- **`Validator` query-checker `check_ast` calls now thread `resolved_principal=` as a keyword argument** uniformly. `RequiredFilterChecker`, `NoSelectStarChecker`, `BlockedColumnsChecker`, `RequireLimitChecker`, and `MaxJoinsChecker` all gained `**_` to accept and ignore the new kwarg — purely additive, no behaviour change.
- **Mutual-exclusion validator on `QueryCheck`**: A single check may not set both `required_filter` and `required_filter_values` — they target the same column conceptually; pick one.

### Documentation

- 23 new unit tests for `RequiredFilterValuesChecker` covering subset matches, smuggled values, OR-bypass, AND-narrowing, contradictions, self-join smuggling, BETWEEN/range rejection, NOT-wrapped predicates, `IS NULL` / `IS NOT NULL`, numeric and string normalisation, and qualified column refs.
- 10 integration tests covering the full validator wiring (resolved-principal threading, ContextVar-switched late binding for the Webex pattern, per-principal value gating).

## [0.14.0] - 2026-04-25

### Added

- **Per-rule principal access control**: New optional `allowed_principals` / `blocked_principals` fields on `SemanticRule` (mutually exclusive at load time) gate individual rules by caller identity, mirroring the v0.13.0 `AllowedTable` semantics. A rule whose principal scope excludes the current caller is skipped at validate-time. This generalises across every rule kind — `blocked_columns`, `required_filter`, `no_select_star`, `max_joins`, and `result_check` — letting contracts express things like "Alice may not select `ssn` from `pii.users`, but Bob may" directly in YAML, without having to split tables into per-principal views.
- **`principal_in_scope()` helper** in `agentic_data_contracts.core.principal`: Single source of truth for the allow/block-list policy used by both `DataContract.allowed_table_names_for` and per-rule scoping. Encapsulates the two-layer empty-string invariant so unauthenticated callers (`None` or `""`) fail closed against any restricted resource.
- **`ops_agent` example demonstrates per-rule principal gating end-to-end**: Adds a block-level rule that lets `compliance@co.com` select PII columns from `sre.incidents` while every other identified caller is denied — composes with the existing per-table gate on `sre.deploys` to show table-level and rule-level controls side by side.

### Changed

- **`Validator` query and result rule lists are now small frozen dataclasses** (`_QueryRuleEntry`, `_ResultRuleEntry`) rather than plain tuples, carrying an extra `principal_scope` snapshot. Internal change — public `Validator` API is unchanged.
- **`pending_result_check_names()` documents the superset contract**: When rules carry `allowed_principals` / `blocked_principals`, the actual run-set for a given caller is `<= pending`. The method intentionally does not resolve a callable principal at call time (TOCTOU avoidance); the only consumer is `run_query` telemetry.
- **Resolved the v0.13.0 known limitation around rule-level scoping** — see the new per-rule principal access control feature above. The system-prompt-rendering limitation is unchanged.

## [0.13.0] - 2026-04-24

### Added

- **Per-table principal access control**: New optional `allowed_principals` / `blocked_principals` fields on `AllowedTable` (mutually exclusive at load time) gate individual tables by caller identity. Principals are opaque strings compared by exact equality — works equally for emails, Webex IDs, employee numbers, or JWT subject claims. Fail-closed: any `*_principals` field on a table requires identification.
- **`caller_principal` parameter on `Validator` and `create_tools`**: New keyword-only argument accepting `str | Callable[[], str | None] | None`. Static string for one-user-per-session (Chainlit); zero-arg callable for multi-user-per-bot scenarios (Webex rooms with `contextvars.ContextVar`-backed identity per message). The resolver is called per-query, not cached, so one long-lived `Validator` can serve different callers sequentially.
- **`DataContract.allowed_table_names_for(principal)`**: Returns the subset of declared tables accessible to the given principal. Centralizes the per-caller allowlist computation.
- **`Principal` type alias and `resolve_principal()` helper**: Re-exported from the package root (`from agentic_data_contracts import Principal, resolve_principal`) for integrators typing their own middleware.
- **Two-tier `TableAllowlistChecker` error messages**: Blocked queries now distinguish "Tables not in allowlist: X" (undeclared) from "Tables restricted to other principals (caller: 'Y'): X" (declared but not accessible to the current caller). The same idiom extends to `describe_table` / `preview_table` tool responses.

### Changed

- **`TableAllowlistChecker` signature gained optional `principal_resolver: Callable[[], str | None] | None = None`**: Backwards compatible — `TableAllowlistChecker()` with no args still works (resolver defaults to returning `None`, so restricted tables fail closed).
- **`describe_table` and `preview_table` are now principal-aware**: Both tools check `allowed_table_names_for(principal)` before serving a response. Restricted tables return `"Table X is restricted (caller: 'Y')."`. The remaining 7 tools (`list_metrics`, `lookup_metric`, `lookup_domain`, `lookup_relationships`, `trace_metric_impacts`, `inspect_query`, `run_query`) are unchanged as far as the discovery surface — `inspect_query` / `run_query` inherit principal gating through the underlying Validator.

### Known Limitation

- **System prompt does not filter by principal**: `DataContract.to_system_prompt()` currently renders the unscoped table list. An LLM serving a user who can't access a restricted table may still be told the table exists. Query-time gating remains authoritative (the spy-adapter integration test confirms denied queries never reach the database), but this can cause the agent to waste retries on queries that would be blocked. Principal-aware prompt rendering is a candidate future feature — file an issue if your deployment needs it.

## [0.12.0] - 2026-04-18

### Added

- **`last_reviewed: date | None` field on `Domain` and `MetricImpact`**: Optional review timestamp for governance artefacts. YAML loader accepts both YAML-native dates (`last_reviewed: 2026-04-18`) and ISO strings (`last_reviewed: "2026-04-18"`); other types raise `TypeError` at load time. Pydantic coerces ISO strings on `Domain` natively.
- **`find_stale_reviews()` detector** (`agentic_data_contracts.core.staleness`): Pure function returning `list[StaleFinding]` for domains and metric-impact edges whose `last_reviewed` is missing or older than `threshold_days` (default 90). Accepts `today: date | None` for deterministic testing. Missing timestamp is reported as stale (`age_days=None`) — otherwise adoption is optional and defeats the forcing function. Inclusive boundary: `age == threshold` is fresh.
- **`StaleFinding` dataclass**: Frozen value object with `kind`, `name`, `last_reviewed`, `age_days`, `threshold_days`, and `context: dict[str, Any]`. Metric-impact findings carry `{from_metric, to_metric, confidence, direction}` in `context` so callers can filter (e.g. "only fail CI on `verified` edges") or format messages.
- **`DataContract.find_stale()` convenience method**: Discoverable entry point that pulls impacts from an optional `SemanticSource` and delegates to `find_stale_reviews`. Mirrors the signature style of `DataContract.to_system_prompt(semantic_source=...)`.
- **Module-level `extract_where_columns()` and `extract_bound_columns()` helpers** in `validation.checkers`: Reusable AST utilities for checker authors. `extract_bound_columns` returns the set of columns that appear in at least one non-tautological predicate (comparison, `IN`, `BETWEEN`, or `IS (NOT) NULL` where the other side doesn't reference the same column).

### Changed

- **`RequiredFilterChecker` now rejects trivially-satisfied predicates**: Previously performed column-presence-only matching, so `WHERE tenant_id = tenant_id` would satisfy a blocking `required_filter: tenant_id` rule — the exact bypass governance rules exist to prevent. The checker now requires the filter column to appear in a non-tautological predicate (comparison, `IN`, `BETWEEN`, or `IS (NOT) NULL`). Covers `=`, `!=`, `<`, `<=`, `>`, `>=`, `LIKE`, `ILIKE`, `IS`, `IN`, `BETWEEN` variants. Column matching is by name only (not table-qualified), so cross-table self-comparisons like `a.tenant_id = b.tenant_id` are also flagged — deliberate, since such predicates don't pin the column to a specific value. Does not attempt SAT-level reasoning (e.g. tautology-inside-OR).
- **`RelationshipChecker` required-filter warning** (advisory path): Same tightening, surfaced as a warning with the message "predicate on ... is trivially satisfied (e.g. `col = col`); add a non-trivial condition".

### Migration

- Review any queries that use self-referential predicates like `col = col`, `col != col`, `col IS col`, `col IN (col)`, or `col BETWEEN col AND col` — these will now be rejected by blocking `required_filter` rules (previously silently passed). Replace with a literal or parameter: `tenant_id = $session_tenant`, `status IS NOT NULL`, `status IN ('active', 'pending')`.
- Adopting the new `last_reviewed` field is optional. If you add the field to any `Domain` or `metric_impacts` entry and run `find_stale`, be aware that entries *without* the field are reported as stale. To grandfather in existing artefacts during rollout, filter findings by `f.age_days is not None`, or add `last_reviewed: <today>` to each entry as a baseline.

### Documentation

- **Two new end-to-end example apps** covering governance archetypes orthogonal to the existing `revenue_agent`:
  - [`examples/growth_agent/`](examples/growth_agent/) — experimentation / leading-indicator archetype. Demonstrates all three impact confidence levels (`verified` / `correlated` / `hypothesized`) with realistic A/B evidence strings, a time-bounded events block rule, a `log`-level PII audit invisible to the agent, and an un-reviewed impact edge that `find_stale_reviews` flags.
  - [`examples/ops_agent/`](examples/ops_agent/) — SRE reliability / real-time-dashboard archetype. Demonstrates `blocked_columns` for PII on incident triage data, **two** `log`-level audit rules (governance trail), `require_limit` and `max_joins` caps, a rare **negative-direction** metric impact (DORA pattern: higher deploy frequency → lower incident count), and tight resource limits (`max_duration=30s`).
- Both examples run cleanly in demo mode without the Claude Agent SDK and exercise ~6–7 tools each.
- **`per-file-ignores` for `examples/**`** added to the ruff config: fixture SQL `INSERT` blocks benefit from aligned-column readability, so `E501` line-length is waived only for example files.

## [0.11.0] - 2026-04-17

### Breaking

- **Tool surface consolidated from 13 to 9 tools**: Five tools dropped and two merged into one. The full contract is already injected into the system prompt by `ClaudePromptRenderer`, so the dropped tools were redundant from an analytics-agent perspective.
- **`list_schemas` removed**: The allowed-schemas set is implicit in the allowed-tables list that the prompt renderer already injects.
- **`list_tables` removed**: The prompt renderer already injects the full allowed-tables list. Per-table column details remain available via `describe_table`.
- **`get_contract_info` removed**: Contract name, allowed tables, rules, and limits are all in the prompt. The one dynamic field the tool exposed — remaining session budget — is now embedded in every `run_query` response under `session.remaining`.
- **`validate_query` + `query_cost_estimate` merged into `inspect_query`**: Both tools wrapped the same underlying `Validator.validate()` call (which internally runs Layer 1 + EXPLAIN). The merge removes a "which tool do I call?" decision. Response is structured JSON with `valid`, `violations`, `warnings`, `log_messages`, `schema_valid`, `explain_errors`, `pending_result_checks`, and — when an adapter is configured — `estimated_cost_usd` and `estimated_rows`.

### Changed

- **`run_query` response**: Success responses now include a `session.remaining` block mirroring `ContractSession.remaining()` (elapsed seconds, retries remaining, token budget remaining, cost remaining). Blocked responses append a one-line `Remaining: {...}` suffix with the same data.
- **`ValidationResult` dataclass**: Gains three additive fields — `estimated_rows: int | None`, `schema_valid: bool = True`, and `explain_errors: list[str] = []`. Populated in `Validator.validate()` when an `ExplainAdapter` is configured. Defaults are safe for existing callers.

### Migration

- Replace `validate_query(sql)` calls with `inspect_query(sql)`. The response is JSON rather than a status string; parse `valid`, `violations`, and `warnings`. Cost and row estimates live under the same response.
- Replace `query_cost_estimate(sql)` calls with `inspect_query(sql)`. Cost and row fields are now nested alongside validation fields.
- If an agent previously called `get_contract_info`, read remaining budget from `run_query` responses (`data["session"]["remaining"]`) instead. Static contract metadata is already in the system prompt.
- `list_schemas` and `list_tables` have no replacements — the prompt already contains this information.

## [0.10.0] - 2026-04-17

### Added

- **Metric role metadata**: `MetricDefinition` gains three optional fields — `domains` (list), `tier` (list, e.g. `north_star` / `department_kpi` / `team_kpi`), and `indicator_kind` (`leading` / `lagging`). Lets the agent prioritize north-stars and verified leading indicators, and filter metrics by organizational role. All fields default to empty, so existing fixtures parse unchanged.
- **Metric-impact graph**: New `MetricImpact` dataclass captures directed, annotated edges between metrics — `from_metric`, `to_metric`, `direction` (`positive` / `negative`), `confidence` (`verified` / `correlated` / `hypothesized`), and free-text `evidence` the agent can cite verbatim. Declared via a top-level `metric_impacts:` block in the semantic YAML.
- **`trace_metric_impacts` tool**: New tool (13th) that walks the metric-impact graph via BFS from a starting metric. `direction="upstream"` returns drivers (for root-cause analyses like "why did revenue drop?"); `direction="downstream"` returns affected metrics (for "what does this KPI move?"). Each edge in the response carries direction, confidence, and evidence for grounded reasoning. `max_depth` is clamped to `[1, 10]` to prevent runaway walks.
- **`build_metric_impact_index()` and `walk_metric_impacts()` helpers**: Standalone functions in `semantic.base` mirroring the `build_relationship_index` / `find_join_path` pattern. Dual-keyed index (each edge stored under both endpoints); walker disambiguates direction at traversal time. Cycle-safe via visited tracking; self-loops are deduplicated by the index builder.
- **`get_metric_impacts()` on `SemanticSource` protocol**: New method returning `list[MetricImpact]`. `YamlSource` parses from the `metric_impacts:` block; `DbtSource` / `CubeSource` return `[]` — neither system has a native causal-graph concept, so impacts are declared in the contract YAML regardless of where metrics themselves come from.
- **Metric role metadata from dbt / Cube `meta`**: `DbtSource` and `CubeSource` now read `tier`, `indicator_kind`, and `domains` from each metric's `meta` dict. String values for `tier` / `domains` are coerced to single-element lists consistently across all three sources (YAML, dbt, Cube), so writing `tier: north_star` works the same as `tier: [north_star]`.
- **Metric-impact validation warnings**: `create_tools()` emits `logger.warning` at tool-creation time if any `metric_impacts` edge references an unknown metric name. Mirrors the existing domain-reference validation.

### Changed

- **Tool count**: Factory now produces 13 tools (was 12), adding `trace_metric_impacts`.
- **`lookup_metric` response shape**: Enriched with `domains`, `tier`, `indicator_kind`, `impacts` (outgoing edges), and `impacted_by` (incoming edges). Each edge is rendered as a one-line citation string (e.g. `"positive impact on total_revenue (verified): A/B test exp-042, +3.2% lift, p<0.01"`) the agent can quote verbatim. Fields are only included when non-empty, keeping responses compact for metrics with no impact data.
- **`list_metrics` filters**: Gains optional `tier` and `indicator_kind` arguments alongside the existing `domain` filter. Entries include `tier` and `indicator_kind` when set.
- **`list_metrics` domain semantics**: Domain filtering now uses a union of contract-side `Domain.metrics` and metric-side self-declared `metric.domains`. A metric that self-declares a domain is discoverable via the filter even if the contract's `Domain.metrics` list doesn't include it.
- **Factory tool descriptions**: `lookup_metric` and `list_metrics` descriptions now advertise the new fields and filters so the agent knows when to use them.

### Breaking

- **`SemanticSource` protocol extension**: The `@runtime_checkable` Protocol gains a required `get_metric_impacts()` method. Custom third-party `SemanticSource` implementations must add this method (returning `[]` is fine); without it, `isinstance(source, SemanticSource)` returns `False`. Built-in `YamlSource`, `DbtSource`, and `CubeSource` all implement it — no migration required for users who only use the built-in sources.

## [0.9.2] - 2026-04-15

### Fixed

- **Lazy session timer**: `ContractSession` no longer starts its wall-clock timer at construction. The timer now starts on the first `check_limits()` call, so idle time before the user's first interaction does not count against `temporal.max_duration_seconds`. This fixes premature "session expired" errors in long-lived agent setups (Chainlit, Webex bots) where the session object is created well before the first user message. (#16)

### Added

- **`ContractSession.reset_timer()`**: New method that resets the duration timer so it restarts on the next `check_limits()` call. Useful for frameworks with their own idle-timeout mechanisms that want to restart the clock on user activity.

## [0.9.1] - 2026-04-13

### Added

- **Schema `description` field**: Optional description on `AllowedTable` entries, surfaced via `list_schemas` to help agents understand what each schema contains and when to use it.
- **Schema `preferred` flag**: Optional boolean on `AllowedTable` (default `false`), surfaced via `list_schemas` to signal which schema the agent should prefer when similar tables exist across schemas.
- **Example improvements**: Revenue agent example updated with `lookup_domain` and `lookup_metric` demo steps, schema description/preferred in contract, and fixed pre-existing missing `query_check` blocks on `tenant_isolation` and `no_select_star` rules.
- **Domain-driven README**: README reframed around the domain-driven approach — agents understand business domains before writing SQL.

## [0.9.0] - 2026-04-13

### Added

- **First-class business domains**: `domains` redesigned from a flat `dict[str, list[str]]` to a list of `Domain` objects with `name`, `summary`, `description`, `metrics`, and optional `tables`. Domains now carry business context that helps agents understand what a domain means before querying.
- **`lookup_domain` tool**: New tool (12th) that returns full domain context — description, associated metrics with descriptions (enriched from semantic source), and tables. Supports fuzzy matching for domain names, consistent with `lookup_metric`.
- **Compact domain index in system prompt**: When domains are defined, the system prompt renders `<available_domains>` with domain name, summary, and metric count — progressive disclosure that keeps context compact while giving the agent enough to decide which domain to explore.
- **Domain validation warnings**: `create_tools()` now warns at tool creation time if a domain references metrics not found in the semantic source or tables not in `allowed_tables`.
- **Domain summaries in `get_contract_info`**: The `get_contract_info` tool now includes domain names, summaries, and metric counts in its response.
- **`get_domain()` helper**: New method on `DataContract` for exact-match domain lookup by name.

### Changed

- **Tool count**: Factory now produces 12 tools (was 11), adding `lookup_domain`.
- **`list_metrics` domain lookup**: Now uses `DataContract.get_domain()` internally instead of dict lookup.
- **System prompt rendering**: `_render_metrics` simplified to only handle the no-domains case. When domains exist, the new `_render_domains` method takes over with compact domain index rendering.

### Breaking

- **Domain YAML format**: `domains` changed from `dict[str, list[str]]` to `list[Domain]`. Existing contracts must migrate from `domains: { revenue: [metric1] }` to `domains: [{ name: revenue, summary: "...", description: "...", metrics: [metric1] }]`.

## [0.8.0] - 2026-04-12

### Added

- **Lazy-loading relationships**: When a semantic source has more than 30 relationships, the system prompt switches to a compact per-table join-count summary instead of listing every relationship. The agent uses the new `lookup_relationships` tool to fetch details on demand — same progressive-disclosure pattern used for metrics since v0.2.6.
- **`lookup_relationships` tool**: New tool (11th) that returns all relationships involving a given table. When `target_table` is provided, finds the shortest multi-hop join path via BFS (up to 3 hops) — useful when tables are connected through intermediate tables.
- **`get_relationships_for_table()` protocol method**: Added to `SemanticSource` for filtered relationship lookup by table name. Implemented in `YamlSource` with an O(1) index; `DbtSource` and `CubeSource` return empty (ready for future implementation).
- **`build_relationship_index()` helper**: Standalone function in `semantic.base` that builds a `dict[str, list[Relationship]]` index from a relationship list, keyed by table name. Reusable by any `SemanticSource` implementation.
- **`find_join_path()` helper**: BFS shortest-path function that finds a chain of relationships connecting two tables, bounded by `max_hops` (default 3). Returns `None` if no path exists.

### Changed

- **Tool count**: Factory now produces 11 tools (was 10), adding `lookup_relationships`.

## [0.7.1] - 2026-04-11

### Fixed

- **Tools factory now passes `semantic_source` to the Validator**: `create_tools()` was creating the `Validator` without the `semantic_source` parameter, so `RelationshipChecker` never ran through `validate_query` or `run_query`. Relationship warnings now surface correctly in the tools layer.
- **Example SDK fallback**: `agent.py` now catches `AttributeError` alongside `ImportError` when the installed `claude-agent-sdk` version is incompatible, falling back to demo mode instead of crashing.

### Changed

- **Example demo step**: Added a relationship warning demonstration — validates a JOIN query missing the declared `required_filter` to showcase the advisory warning.

## [0.7.0] - 2026-04-11

### Added

- **`RelationshipChecker`**: Advisory validation of SQL JOINs against declared semantic relationships. Produces warnings only — never blocks queries. Silent on undeclared joins. Three detection modes:
  - **Join-key correctness**: Warns when an agent joins two tables that have a declared relationship but uses different columns than specified (e.g., joining on `email` instead of declared `customer_id → id`). Supports both `ON` and `USING` clause syntax.
  - **Required-filter enforcement**: Warns when a join's declared `required_filter` condition is missing from the query's WHERE clause. Checks column presence only (not exact expression), so `status = 'active'` satisfies `required_filter: "status != 'cancelled'"`.
  - **Fan-out risk detection**: Warns when the query aggregates (SUM, COUNT, AVG, etc.) across a `one_to_many` join, which may inflate results due to row multiplication. Only checks top-level SELECT aggregations — subquery aggregations are ignored.
- **`Validator` accepts `semantic_source`**: Optional `SemanticSource` parameter on `Validator.__init__()` enables relationship checking when provided. Fully backward-compatible — omitting it preserves existing behavior.
- **Relationship warnings skip blocked queries**: When a query is already blocked by structural checkers, relationship warnings are suppressed to reduce noise.

## [0.6.0] - 2026-04-09

### Added

- **Relationship `description` field**: Optional free-text description on `Relationship` for communicating join conditions, data quality caveats, or usage guidance to the agent. Rendered as an XML attribute in the system prompt when present.
- **Relationship `required_filter` field**: Optional structured filter condition (e.g., `"attribution_model = 'last_touch'"`) that must be applied when using a relationship. Rendered as a `<required_filter>` element in the system prompt, giving agents a clear, unambiguous signal about mandatory join conditions — especially useful for bridge/junction tables.
- **Contract-relative path resolution**: `DataContract.from_yaml()` now resolves `source.path` relative to the contract file's directory, not the process CWD. This means `path: "./semantic.yml"` in `contracts/contract.yml` correctly loads `contracts/semantic.yml` regardless of where the process runs. Absolute paths and `from_yaml_string()` are unaffected.

### Fixed

- **Example contract**: Removed invalid `filter_column` field from `examples/revenue_agent/contract.yml` (the field was removed in v0.4.0 in favor of `query_check.required_filter`).

## [0.5.0] - 2026-04-04

### Added

- **`SqlNormalizer` protocol**: Optional pre-processing hook for adapters serving non-standard SQL dialects (e.g., Denodo VQL, Teradata). Adapters implement `normalize_sql(sql) -> str` to rewrite proprietary syntax into a form sqlglot can parse, while the original SQL is preserved for `execute()` and `explain()`.
- **Auto-detection in factory and middleware**: When an adapter implements both `DatabaseAdapter` and `SqlNormalizer`, the factory and middleware automatically wire normalization into the `Validator` — no API changes needed.
- **Normalization in `validate_results()`**: Table-scoped result checks now also benefit from SQL normalization, ensuring scoped checks fire correctly for non-standard dialects.
- **Adapter package exports**: `adapters/__init__.py` now re-exports `Column`, `DatabaseAdapter`, `QueryResult`, `SqlNormalizer`, and `TableSchema`.
- **Root export**: `SqlNormalizer` is available via `from agentic_data_contracts import SqlNormalizer`.

## [0.4.0] - 2026-03-31

### Added

- **Unified rule engine**: Rules now support `query_check` (pre-execution) and `result_check` (post-execution) blocks, replacing the old `filter_column` shorthand. All rules live in one `rules` list; the engine determines execution phase automatically.
- **Table scoping**: Every rule can be scoped to a specific table (`table: "schema.table"`) or apply globally (omitted or `"*"`). Pre-execution and post-execution rules both support scoping.
- **5 built-in query checks**: `required_filter`, `no_select_star`, `blocked_columns`, `require_limit`, `max_joins` — all declarative in YAML, no Python needed.
- **6 built-in result checks**: `min_value`/`max_value` (numeric column bounds), `not_null`, `min_rows`/`max_rows` — validated against actual query output post-execution.
- **Advisory rules**: Rules with neither `query_check` nor `result_check` appear in the system prompt as guidance but don't enforce anything.
- **Session cost enforcement**: `run_query` now records estimated cost from EXPLAIN and enforces cumulative `cost_limit_usd` across the session.
- **`validate_results()` on Validator**: New method for post-execution result validation, used transparently inside `run_query`.
- **`validate_query` result check notes**: Output now lists pending result checks that will run at execution time.
- **New checker classes**: `BlockedColumnsChecker`, `RequireLimitChecker`, `MaxJoinsChecker`, `ResultCheckRunner` — all exported from `validation` module.

### Changed

- **Checker protocol**: All checkers now use `check_ast(ast)` instead of `check_sql(sql)`. SQL is parsed once by the Validator and the AST is passed to all checkers.
- **`extract_tables()` utility**: Extracted from `TableAllowlistChecker` into a standalone function for shared use by the Validator's table scoping logic.
- **`ValidationResult`**: Gains `estimated_cost_usd: float | None` field for session cost passthrough from EXPLAIN.
- **Three-phase validation**: Validator now runs query checks (Phase 1) → EXPLAIN (Phase 2) → result checks (Phase 3), up from the previous two-phase pipeline.

### Removed

- **`SemanticRule.filter_column`**: Replaced by `query_check: { required_filter: <column> }`. No backward compatibility — the old field is removed entirely.
- **Heuristic filter detection**: The regex-based `_extract_filter_column()` method that guessed filter columns from rule descriptions is gone. Filters are now explicit in `query_check`.

## [0.3.0] - 2026-03-30

### Added

- **`PromptRenderer` protocol**: New `@runtime_checkable` protocol for custom system prompt formatting. Users can implement `render(contract, semantic_source) -> str` to control how contracts are presented to their model of choice.
- **`ClaudePromptRenderer`**: Built-in XML-structured renderer optimized for Claude models (Sonnet 4.6+). Uses XML tags for structural boundaries, places constraints at the end for better instruction-following, and merges resource/temporal limits into a single section.
- **Custom renderer support**: `to_system_prompt(renderer=MyRenderer())` delegates entirely to a user-provided renderer.
- **Top-level exports**: `from agentic_data_contracts import PromptRenderer, ClaudePromptRenderer`

### Changed

- **Default system prompt format**: `to_system_prompt()` now generates XML output (was Markdown). Pass a custom renderer if you need a different format.
- **`contract.py` simplified**: `to_system_prompt()` is now a thin delegate (~7 lines). All prompt-building logic moved to `core/prompt.py`.

## [0.2.6] - 2026-03-29

### Changed

- **Compact system prompt at scale**: When metrics exceed 20, the system prompt shows domain names with counts (e.g., "acquisition (45)") instead of listing every metric. Reduces prompt from ~6K to ~100 tokens for large metric sets.
- **Paginated `list_tables`**: Added `limit` (default 50) and `offset` parameters for handling schemas with many tables. Response includes `total` count and `next_offset` for pagination.
- **Cached wildcard resolution**: `resolve_tables()` is now idempotent — subsequent calls are no-ops, avoiding redundant database queries.

## [0.2.5] - 2026-03-29

### Added

- **Table relationship metadata**: `Relationship` dataclass and `get_relationships()` on `SemanticSource` protocol for declaring join paths between tables (from/to column + relationship type)
- **Relationships in system prompt**: `to_system_prompt()` includes join paths so the agent knows how to combine tables correctly
- **YamlSource relationships**: Parsed from `relationships` section in semantic YAML files
- DbtSource and CubeSource return empty relationships (ready for future parsing of native join metadata)

## [0.2.4] - 2026-03-29

### Added

- **Wildcard table support**: Use `tables: ["*"]` in `allowed_tables` to allow all tables in a schema, discovered from the database at runtime via `adapter.list_tables()`
- **`DataContract.resolve_tables(adapter)`**: Expands wildcard entries using the database adapter; called automatically by `create_tools()` when an adapter is provided
- **`DatabaseAdapter.list_tables(schema)`**: New protocol method for listing tables in a schema; implemented in `DuckDBAdapter` via `information_schema.tables`

## [0.2.3] - 2026-03-29

### Added

- **SDK MCP server convenience method**: `create_sdk_mcp_server(contract, adapter=...)` wraps all 10 tools with the SDK's `@tool` decorator and bundles them into a ready-to-use MCP server for `ClaudeAgentOptions.mcp_servers`
- **Top-level export**: `from agentic_data_contracts import create_sdk_mcp_server`

### Changed

- **SDK dependency**: Bumped `claude-agent-sdk` minimum to `>=0.1.52`

## [0.2.2] - 2026-03-28

### Added

- **SDK config generation**: `DataContract.to_sdk_config()` maps contract limits to Claude Agent SDK options (`token_budget` → `task_budget`, `max_retries` → `max_turns`)

## [0.2.1] - 2026-03-28

### Added

- **Auto-load semantic source**: `DataContract.load_semantic_source()` reads `source.type` and `source.path` from the contract YAML and instantiates the correct `SemanticSource` (YamlSource, DbtSource, or CubeSource)
- **Zero-config tools**: `create_tools()` auto-loads the semantic source from contract config when none is explicitly passed

## [0.2.0] - 2026-03-28

### Added

- **Scalable semantic discovery**: `domains` field in contract YAML for grouping metrics by business domain (e.g., acquisition, retention, attribution)
- **Fuzzy metric search**: `lookup_metric` now falls back to fuzzy matching via `thefuzz` when no exact match is found, returning ranked candidates
- **Domain-filtered list_metrics**: `list_metrics` tool accepts optional `domain` parameter to filter metrics by domain
- **Metrics in system prompt**: `to_system_prompt()` accepts an optional `SemanticSource` and renders a compact metric index (names + descriptions, grouped by domain)
- **`search_metrics()` protocol method**: Added to `SemanticSource` with shared `fuzzy_search_metrics()` helper using `thefuzz` `token_set_ratio` scorer
- **`thefuzz`** added as core dependency (backed by `rapidfuzz` C++ for performance)

### Fixed

- **EXPLAIN integration**: Validator pipeline now enforces `cost_limit_usd` and `max_rows_scanned` via Layer 2 EXPLAIN dry-run
- **`describe_table` allowlist check**: Tool now rejects tables not in the contract's allowed list
- **`filter_column` field**: Explicit column specification on `SemanticRule` for deterministic required filter detection
- **DuckDB row estimates**: EXPLAIN output parsed for `~N` cardinality estimates
- **TRUNCATE detection**: Fixed sqlglot `TruncateTable` type handling in `OperationBlocklistChecker`
- **Code quality**: CTE extraction O(n²)→O(n), `NoSelectStar` idiom, `preview_table` limit validation, public `Checker` protocol

## [0.1.0] - 2026-03-27

### Added

- **Core layer**: YAML-first data contract schema with Pydantic validation, `DataContract` class with YAML loading and system prompt generation, `ContractSession` for lightweight resource enforcement (retries, tokens, cost, duration)
- **Validation layer**: Four built-in SQL checkers via sqlglot (table allowlist, operation blocklist, required filters, no SELECT *), `Validator` orchestrator with two-layer pipeline (static checkers + optional EXPLAIN dry-run for cost/row enforcement)
- **Tools layer**: `create_tools()` factory producing 10 agent tools (list_schemas, list_tables, describe_table, preview_table, list_metrics, lookup_metric, validate_query, query_cost_estimate, run_query, get_contract_info), `contract_middleware` decorator for wrapping existing tools
- **Semantic layer**: `SemanticSource` protocol with three implementations — `YamlSource`, `DbtSource` (manifest.json), `CubeSource` (Cube schema YAML)
- **Database adapters**: `DatabaseAdapter` protocol with `DuckDB` implementation (execute, explain with row estimate parsing, describe_table)
- **Bridge layer**: Optional `ai-agent-contracts` integration via `compile_to_contract()` mapping YAML contracts to the formal 7-tuple Contract model
- **Example**: Revenue analysis agent with DuckDB, YAML semantic source, and Claude Agent SDK fallback demo mode
- **Developer tooling**: uv for dependency management, prek pre-commit hooks (ruff + ty), 124 tests
