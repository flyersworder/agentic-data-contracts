# Declared attribution convention for decompositions

**Issue:** #67 · **Target release:** v0.43.0 · **Date:** 2026-08-18

## Context

`docs/architecture.md` (Future Extensions → variance-diagnosis tool) gated a
variance-attribution kernel on an observable trigger, revised 2026-08-16 because
the original condition ("proving to be a real error source") could not fire —
attribution errors return numbers that are plausible, internally consistent, and
confidently presented.

The probe that trigger describes has now been run (#67): 16 fresh sessions, two
arms, scored against the same `rel_tol=1e-4` that `reconcile_decomposition`
uses.

- **Outcome #1 — contributions do not sum to the change — is disconfirmed.**
  16/16 runs were arithmetically correct. This is a finding, not an absence: the
  branch of the trigger that would have justified a correctness kernel with a
  fixed default is closed, not left pending.
- **Outcome #2 — they sum, but the convention drifts — fired.** Three distinct
  cross-term placements in the undeclared arm, two in the declared arm, with a
  13.5% span on the headline contribution. Declaring `decompositions` did not
  stabilize it, and structurally could not: `decompositions` names *which
  factors* participate, and carries nothing about where the `ΔC·ΔP` cross term
  goes.

The artifact outcome #2 implies is declared vocabulary. This spec commits to it.

## What ships

1. A `convention` / `convention_operand` pair on `Decomposition`.
2. A source-level default, resolved onto each decomposition at load.
3. Loud load-time validation of both.
4. Round-trip through `YamlSource`, `OssieSource`, and `dump_semantic_source`.
5. The pair in the `lookup_metric` payload.
6. A pure attribution kernel in `validation/attribution.py`, exported from
   `agentic_data_contracts.validation`, **not** wired into `create_tools()`.

## 1. Vocabulary and data model

```python
VALID_CONVENTIONS = frozenset({"explicit", "split_evenly", "fold_into"})
_CROSS_TERM_OPERATORS = frozenset({"product", "ratio"})


@dataclass
class Decomposition:
    operator: str
    operands: list[str] = field(default_factory=list)
    convention: str | None = None          # None = undeclared, today's behavior
    convention_operand: str | None = None  # required iff convention == "fold_into"
```

| Value | Meaning |
|---|---|
| `explicit` | The interaction residual is reported as its own line, attributed to no factor. |
| `split_evenly` | The residual is divided equally among the operands. At two operands this is the Shapley value. |
| `fold_into` | The whole residual is absorbed by the operand named in `convention_operand`. |

**Why a named operand rather than a positional enum.** `product` takes two or
more operands, so "fold into one factor" must say which. Naming it is
reorder-safe (operand order is semantically free for `product` today, and this
keeps it that way), generalizes to any arity, and gives the agent reading
`lookup_metric` a metric name instead of an index to count.

**Why not `laspeyres` / `paasche`.** They are exactly the two-operand cases of
`fold_into`, and have no single agreed meaning at higher arity — adopting them
would split the vocabulary by operand count. They appear in the docs as the
names an analyst will recognise, never in the schema.

## 2. Source-level default

A top-level key in the semantic source, beside `metrics:`:

```yaml
decomposition_convention:
  convention: split_evenly

metrics:
  - name: activations
    decompositions:
      - operator: product
        operands: [volume, rate]
        convention: fold_into      # per-decomposition declaration wins
        convention_operand: rate
```

- Added to `SEMANTIC_KEYS` in `yaml_source.py`. Without this, a strict contract
  (`expected_extras: []`) rejects the key as an undeclared extra, and a
  default-policy contract logs it as a possible typo.
- `OssieSource` reads the same key from the `AGENTIC_DATA_CONTRACTS` vendor
  block, beside the `decompositions` it already restores.
- Only `explicit` and `split_evenly` are legal as a default. `fold_into` names an
  operand, and no operand name is meaningful across metrics — declaring it
  source-wide raises.

**Resolved at load, not carried.** `YamlSource` stamps the default onto every
decomposition whose operator is in `_CROSS_TERM_OPERATORS` and which declares no
convention of its own. Two consequences, both wanted:

- `dump_semantic_source` re-serializes from parsed objects rather than copying
  raw YAML, so a resolved default survives `freeze_semantic_source` with no
  extra dump plumbing.
- A frozen contract is fully explicit. The published, content-addressed artifact
  states the effective convention per decomposition, with no inherited state a
  consumer has to re-derive.

Stamping is restricted to cross-term operators so the default cannot push a
convention onto a `sum` and trip its own validation.

## 3. Validation

Extends `validate_decompositions()`, raising `ValueError` at load — the posture
every existing decomposition check already takes.

| Condition | Result |
|---|---|
| `convention` not in `VALID_CONVENTIONS` | raise, listing the vocabulary |
| `convention` on `sum` / `difference` | raise — the identity is linear, there is no cross term to place |
| `convention == "fold_into"` with no `convention_operand` | raise |
| `convention_operand` not among the declared operands | raise |
| `convention_operand` set without `fold_into` | raise — meaningless |
| source-level default is `fold_into` | raise |
| source-level default not in `VALID_CONVENTIONS` | raise |

## 4. Round-trip and digest stability

`dump_semantic_source` emits `convention` / `convention_operand` **only when
set**, matching the existing omit-when-empty treatment of `decompositions` —
which exists precisely so a leaf metric's dump stayed byte-identical across the
0.28 upgrade.

This is load-bearing. `contract_canonical_bytes` calls
`model_dump(mode="json")` with no `exclude_none`, so any always-present key moves
every digest and invalidates every published ARD attestation. A contract that
declares no convention must produce byte-identical canonical bytes before and
after this change, and a test asserts exactly that.

For the same reason the source-level default is **not** a field on
`SemanticConfig`: a contract-schema field serializes as `null` for every contract
that omits it. The alternative — a `_CANONICAL_EXCLUDE` entry — would hide a
governance fact from the attestation that is supposed to describe the contract.

## 5. Delivery to the agent

`tools/factory.py` adds both keys to the `lookup_metric` payload, beside
`operator` and `operands`, omitted when unset:

```python
{"operator": "product", "operands": ["volume", "rate"],
 "convention": "fold_into", "convention_operand": "rate"}
```

Pull-only, via the channel decompositions already use. No prompt change: the
`XmlPromptRenderer` does not carry decompositions today, and pushing them would
cost prompt real estate on every contract to serve a question most sessions never
ask.

**This is grounding, not enforcement.** An attribution report is prose; it never
passes a checker the way SQL does. The honest claim is the one `drill_by` makes —
the contract states a non-inferable fact and makes departures from it visible.

## 6. The attribution kernel

`validation/attribution.py`, exported from `agentic_data_contracts.validation`.

```python
attribute_change(metric, *, before, after, decomposition=0) -> AttributionResult
check_attribution(metric, *, before, after, reported,
                  rel_tol=1e-4, abs_tol=0.0, decomposition=0) -> AttributionResult
```

`before` / `after` map each declared operand to its measured value.

**No adapter, no SQL — deliberately unlike `reconcile_decomposition`.** The
architecture doc rules out time-series execution semantics (calendar vs. cohort
windows, seasonality) as analytics-domain logic rather than governance. Taking
values rather than queries is what keeps that boundary: the caller owns *when*,
the contract owns how the cross term is placed.

### Arithmetic

One definition of a main effect, holding every other operand at its `before`
value, and a single lumped residual:

```
product:      c_i         = (x_i_after - x_i_before) * prod_{j != i} x_j_before
              delta       = prod(after) - prod(before)

ratio:        c_num       = (N_after - N_before) / D_before
              c_den       = N_before / D_after - N_before / D_before
              delta       = N_after / D_after - N_before / D_before

sum:          c_i         = x_i_after - x_i_before
difference:   c_a = a_after - a_before ; c_b = -(b_after - b_before)

all:          interaction = delta - sum(c_i)
```

The declared convention then places `interaction`: `explicit` reports it as its
own line; `split_evenly` adds `interaction / n` to each contribution;
`fold_into` adds all of it to the named operand. `sum` and `difference` are
linear, so `interaction` is zero and no convention is required.

A lumped residual rather than the full 2ⁿ−1 interaction expansion: it is
well-defined at any arity, and it is what the pilot's explicit-cross runs
actually reported. `split_evenly` is implemented as literally that — at two
operands it coincides with the Shapley value, which the docs note and the code
does not claim at higher arity.

### Worked example (reproduces #67's table exactly)

`parent = volume × rate`, T0 = (10,000, 0.35), T1 = (15,000, 0.45).
Δparent = 3,250; `c_volume` = 1,750; `c_rate` = 1,000; `interaction` = 500.

| convention | volume | rate | interaction |
|---|---|---|---|
| `explicit` | 1,750 | 1,000 | 500 |
| `split_evenly` | 2,000 | 1,250 | — |
| `fold_into: rate` (Laspeyres) | 1,750 | 1,500 | — |
| `fold_into: volume` (Paasche) | 2,250 | 1,000 | — |

### Result type

`AttributionResult`, frozen, mirroring `ReconciliationResult`: `metric`,
`operator`, `convention`, `convention_operand`, `delta_parent`, `contributions`,
`interaction`, `shares`, and — populated only by `check_attribution` —
`reported`, `deviations`, `matches`, `sums_to_delta`, `rel_tol`, `abs_tol`,
`reason`.

`shares` is each contribution over `delta_parent`, omitted when `delta_parent`
is zero.

### Verdict scale

Each deviation is judged against `max(abs_tol, rel_tol * abs(delta_parent))`. A
contribution is meaningful as a share of the total change, and a per-contribution
relative tolerance explodes when a contribution is near zero.
`sums_to_delta` is reported separately, on the same scale — cheap to keep even
though the pilot disconfirmed the failure it detects.

### Errors

`ValueError`, before any arithmetic, for: no decomposition declared;
out-of-range `decomposition` index; `before` / `after` keys not matching the
declared operands; a cross-term operator with no declared convention; a `ratio`
with a zero denominator at either point. The last diverges from
`reconcile_decomposition`, which treats a zero denominator as a *finding*
because it discovers the value itself mid-run; here the caller measured it and
can see it.

### Who calls it

Stated honestly, because the answer shaped the design:

- **`attribute_change`** — a consuming agent's own toolbox, wrapping it as a tool
  so the agent asks for the breakdown instead of computing one in its head. Not
  wired into `create_tools()` here: the architecture doc puts variance diagnosis
  on the agent-owned side of the Spec B split, and says the kernel "could live
  here or in the agent's own toolbox". Shipping the arithmetic here unwired
  resolves that sentence — the arithmetic is governance-adjacent, the
  orchestration is not.
- **`check_attribution`** — an eval harness. The #67 pilot hand-wrote this
  scoring pass; any follow-up arm needs it again. It is **not** a CI or
  production path: it needs a `reported` breakdown, which exists only inside an
  agent's prose answer, and post-hoc checking is the wrong shape for a failure
  with an agent in the loop by construction.

## 7. Deliberately not built

- **No tool wiring.** See above.
- **No prompt push.** `XmlPromptRenderer` is untouched.
- **No change to `reconcile_decomposition`.** It checks a *level* identity at one
  point in time; a convention governs attributing a *change between two*. The
  two never interact.
- **No enforcement.** Nothing validates an agent's reported breakdown in
  production, and this spec does not pretend otherwise.
- **No correctness kernel with a fixed default.** Disconfirmed by the pilot.

## 8. Testing

| Suite | Covers |
|---|---|
| `tests/test_semantic/test_decomposition.py` | Every validation row in §3; default resolution and per-decomposition override; default not stamped onto `sum` / `difference` |
| `tests/test_semantic/test_yaml_source.py` | `decomposition_convention` accepted as vocabulary, not flagged as an extra under `expected_extras=[]` |
| `tests/test_semantic/test_ossie.py` | Both fields and the default round-trip through `custom_extensions` |
| `tests/test_ard.py` | A contract declaring no convention produces byte-identical `contract_canonical_bytes`; one that declares it round-trips through freeze → rehydrate |
| `tests/test_tools/test_decomposition_tools.py` | `lookup_metric` payload carries both keys, omits them when unset |
| `tests/test_validation/test_attribution.py` | All four rows of the worked example; `ratio`, `sum`, `difference`; `check_attribution` verdicts at and across the tolerance boundary; every `ValueError` in §6 |

Fixtures extend `tests/fixtures/decomposition_source.yml` and
`tests/fixtures/sample_ossie_model.yml`.

## 9. Docs and release

- **README** — the convention block in "Metric decomposition and drill
  dimensions", with the Laspeyres / Paasche / Shapley names given as the
  analyst-facing gloss; a short `attribute_change` example beside the
  `reconcile_decomposition` paragraph.
- **`docs/architecture.md`** — a v0.43.0 section; and the Future Extensions
  variance-diagnosis entry updated to close the open question: outcome #2 fired,
  outcome #1 disconfirmed, the kernel ships as a pure unwired helper, the
  diagnosis tool stays out of scope.
- **CHANGELOG** + version bump to **0.43.0**.
