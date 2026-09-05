# Paper 1 — plan

**Status:** in preparation. Focus is Paper 1 and its arXiv preprint. A second
paper carrying the enterprise-deployment lessons is deferred until quantitative
disclosure is cleared — see [Paper 2](#paper-2-deferred).

## Target

| | |
|---|---|
| **Preprint** | arXiv, primary **cs.DB**, cross-list cs.AI (optionally cs.CL) |
| **Venue** | **PVLDB "Experiment, Analysis & Benchmark" (EA&B)** research track |
| **Fallback** | EDBT Industrial & Applications (2027 Lille, rolling cycles) |
| **Not** | VLDB Industrial — it expects a production system at scale; our warehouse is 138k rows at a venue named for very large databases |

`cs.DB` and not `cs.AI` is deliberate: the primary category decides which
mailing list announces the paper, the audience we want is the data-management
community, and cs.AI is high-volume enough to bury an applied evaluation.

EA&B is the right track because it asks for exactly what we have, and not for
a new method. Its stated criteria: *"fundamentally new insights into the
strengths and weaknesses of existing methods"*, *"new ways to evaluate
existing methods and systems"*, and an Experimental Survey subcategory that
accepts *comparison of existing (including open-source) solutions*. That
dissolves the "no novel method" objection an industrial track would raise.

## The claim

> Holding the tool surface, the procedural instruction, the table allow-list
> and the operation rules fixed, emptying only a contract's *prose* drops
> hard-task accuracy to the bare-schema baseline — 19.3% vs 13.9% on glm
> (p=0.058) and 22.6% vs 22.6% on deepseek (p=0.61). Restoring the prose
> reaches 55.1% and 56.6%.

The contribution is the **decomposition**, not the accuracy: **the gain is
semantic content, and the scaffolding term is not distinguishable from zero on
either model.** Published context-layer results (MotherDuck Guides, +72pp; and
the two systems atop DABStep's validated leaderboard, NVIDIA KGMON at 89.95
hard and OceanBase DataPilot at 87.57) report two accuracy numbers each, from
which no mechanism can be recovered — was it the content, the fetch loop, or
the pre-built views? This is, as far as we know, the only controlled answer.

**Corrected 2026-09-01.** This plan previously claimed "content 87%,
scaffolding 13%" from run A's +5.4pp point estimate. That estimate does not
survive a significance test (p=0.058), run B puts it at +0.0pp (p=0.61), and
the two runs disagree on sign. The null result on scaffolding is the stronger
claim — it forecloses the deflationary reading of arm C instead of conceding
part of it — but the paper must state it as a null, with the pooled p=0.29,
and must not re-import the percentage split anywhere.

Lead with mechanism. Never imply we compete on score: we do not, and a
reviewer who reads it that way is right to reject it.

### Second contribution: governance

Not in the earlier draft of this plan, and strong enough to carry its own
section. Ungoverned arms submitted **166 mutating SQL statements across 26
tasks**; both governed arms submitted **zero**, on both models (Fisher
p=6e-08 pooled). The governed arms did not attempt and get blocked — they
never attempted, so the mechanism is deterrence by declared rule, not
interception. One ungoverned run escalated from `TEMP` to persistent tables
and actually mutated the warehouse copy, narrating its reasoning for the
escalation as it went.

This matters for an EA&B audience independently of accuracy: it is a measured
safety property of the same artifact, on the same runs, at no extra cost.

## Positioning: anticipated metrics vs unanticipated ones

The paper's most likely misreading is "they score 55% where MotherDuck scores
99.8% and the DABStep leaderboard's top entry scores 89.95, so this is worse."
§8 must close that off early and explicitly, because a reviewer who forms that
impression in §1 will not revise it in §8.

The leaderboard version of the objection has a sharper answer than the
MotherDuck version, and the paper should use it: both leading systems build
their artifact **against the benchmark's gold answers** — NVIDIA's learning
loop "validat[es] them against ground truth answers" and its reflection phase
compiles pitfalls "from the test data" into the inference prompt; DataPilot
benchmarks competing branches against golden answers and promotes the winner.
Our contract was authored from the vendor manual alone and frozen, digest-
pinned, before any question was read. Their number bounds what a labelled,
enumerable question space can be compiled down to; ours does not. State this
factually — nothing in the benchmark forbids gold supervision — and do not
imply misconduct.

**The two approaches compile the semantics at different times.**

| | pre-built views / macros | declarative contract |
|---|---|---|
| the metric is | compiled ahead of time, by a human | prose the model compiles per query |
| covers | metrics someone anticipated | any question over the described domain |
| authoring cost | one macro per metric, tested and maintained | one description per domain |
| fails when | the question was not anticipated | the model reimplements the rule wrongly |
| DABStep score | 88% retrieval -> **93%** warehouse-baked macros/views -> **100%** hierarchical semantic layer (MotherDuck's own progression); **89.95** hard, gold-supervised `helper.py` (NVIDIA KGMON, leaderboard 1st) | 55.1% / 56.6% |
| artifact built against golds | yes, iteratively | no, frozen before any question was read |

DABStep is 450 questions from **26 templates**, and 294 of 332 hard tasks here
are fee questions. That is a question space enumerable in advance, which is
the best case for the pre-built approach and a case real warehouses do not
offer. A macro contributes nothing to a question nobody wrote it for; the
declarative layer is the one that has to cover the unanticipated question.
**So 55.1% is a measurement of a harder task, not a worse attempt at the same
one** — and the gap to 99.8% is substantially work relocated from the agent to
a human author, not reasoning improved.

Verified concretely in `FINDINGS.md`: tasks 1500 and 1502, both of which the
contract arm got wrong, are answered exactly by a five-line `wild()` macro
encoding the NULL-wildcard rule the contract states in prose.

Two coupling costs are quoted from the sources rather than speculated about:
the 100% author writes that "the tuned artifact is coupled" to the serving
model, and third-party commentary warns against hardcoding answers and urges
measuring generalisation to unseen questions. Attribute both; accuse nobody.

**Be generous about it, because the generous reading is the correct one.**
These are complementary layers, not competitors. Macros optimise the head of
the distribution; a contract covers the tail — and in a system with both, the
contract is what tells the agent which macro exists and when to call it. Say
this. A paper that frames a well-engineered competing result as a rival it
beats is a paper a reviewer distrusts.

It also sets up the roadmap honestly: this library's own deferred
`compose_metric_query` **is** the executable layer, and it was deliberately
not built before this measurement so that the declarative contribution could
be isolated first. That is the same argument as the "deliberate non-response"
section in `FINDINGS.md`, and the two must not contradict each other.

Scalability is the open question to state rather than answer: one macro per
metric per dialect is a maintenance burden that grows with the metric
catalogue, and we have no data on where that crosses over. Do not claim the
contract approach scales better. Claim only that it degrades differently —
gracefully on unanticipated questions, where a missing macro degrades to
nothing.

## Structure, and what exists today

| § | content | status |
|---|---|---|
| 1 Introduction | context layers work; nobody has said *which part* works | to write |
| 2 Background | DABStep; the semantic-layer / context-layer landscape | to write |
| 3 Experimental design | four arms, what is held fixed, the hollow control | **`FINDINGS.md`** |
| 4 Protocol | frozen digest-pinned artifact, official vendored scorer, gold reconstruction + independent verification | **`FINDINGS.md`**, `dce/golds.py` |
| 5 Results | four arms x 2 models, every pairwise McNemar | **both runs done**; pro sweep outstanding |
| 6 Mechanism | task 1278 traced end to end; both baselines produce the identical wrong number | **`FINDINGS.md`** |
| 6b Governance | 166 mutating statements vs 0, deterrence not interception, the task-68 escalation | **`FINDINGS.md`** |
| 7 Threats to validity | one benchmark / 26 templates, reconstructed golds, k=1, run B's 29% truncation, self-attested provenance | **`FINDINGS.md`** |
| 8 Related work | MotherDuck Guides; the validated leaderboard's top two (NVIDIA KGMON, OceanBase DataPilot) and their gold-supervised learning loops; the negative result that their *unsupervised* group-consistency half yields ~1.1x here and catches 0 of the 19 official overturns; why their 88–100% and our 55.1% measure different things; Xiaomi retail's Text-to-Metrics deployment as the one source that ran the ungoverned arm in production first (corroborates direction and mechanism, not effect size) | **`FINDINGS.md`** |
| 9 Artifacts | harness, frozen contract, 3,208 transcripts, results JSONL | **in repo** |

Most of sections 3–8 already exist in `experiments/dabstep-contract-eval/FINDINGS.md`.
The paper is substantially a rewrite of that document for an academic
audience, not new analysis.

## Gaps to close before submission

Ordered by what a reviewer would reject over. Only 1 and 2 need compute;
the rest are writing.

1. **The pre-registered primary comparison has never been run.** `dce.stats`
   still prints `PRIMARY (pre-registered) ... (no rows for
   deepseek-v4-pro-0813)`. Publishing exploratory arms while the
   pre-registered one sits unrun is the single most attackable thing in the
   paper — a reviewer who notices reads it as reporting the runs that worked.
   Either run it or de-register it in print with a stated reason; running it
   is far better. **Design in the next section.**
2. **The capability interaction needs a third point.** `contract` −
   `manual_prompt` is +32.2pp on glm and +13.7pp on deepseek, and `contract`
   itself barely moves between them (55.1% -> 56.6%). Two readings — context
   substitutes for capability, or both arms are near a benchmark ceiling — and
   two flash models cannot separate them. A reviewer *will* ask whether the
   effect survives on frontier models. Gap 1's run answers this too, which is
   why it is worth the money. A cross-family arm (not another Chinese
   open-weight family) would additionally kill "it's an open-model quirk".
3. **k>1.** No variance estimate on either run, and one task (1480) was
   observed flipping verdict between identical runs. The two runs are
   different models, not replicates, so nothing so far estimates
   within-condition variance. ~20 tasks x 3 repeats on the contract arm; a few
   dollars.
4. ~~**Gold reconstruction needs its own subsection**~~ **done, and the
   submission was made (2026-09-04).** The measurement replaced the argument
   and it did not come back clean: 95.3% agreement on 401 tasks, but the 19
   disagreements are **one-sided** — lenient here, wrong officially, all hard
   split, **+5.7 pp**, and all of them wrong *values* rather than the
   formatting slips they resemble: the scorer is the same vendored file on
   both sides and calls all 19 correct against our reconstructed golds
   (`analysis/gold_disagreement.py`), so the golds differ, not the rules.
   The exclusion of 49 tasks
   *is* unbiased (52.4% kept vs 47.8% dropped, official). A proxy
   (`analysis/leniency.py`) shows the contract arm is the least *exposed* on
   all four models but has the most correct answers to lose, so correcting it
   moves the twelve arm contrasts by −2.8 to +1.4 pp — not conservative for
   the headline, but small. Written up in `FINDINGS.md` and in the paper
   (§protocol *External validation*, §threats *Reconstructed golds are
   lenient*).
4a. **A SECOND arm was submitted (2026-09-05), and it changes §protocol.**
   `manual_prompt` on the same model, commit and provider went up as
   `agentic-data-contracts-manual-baseline`. Adyen-graded both sides:
   **51.9% vs 18.8% hard, +33.1 pp, paired McNemar p = 4.9e-28** on 378 tasks.
   The headline contrast no longer needs our golds at all — drop the hedging
   that 51.9% and 22.9% "are not measured the same way", and drop the
   stuffing objection with it: validation closed 2026-07-22, so a second
   entry cannot be a ranking claim, and it is labelled a baseline control.
   Bonus: applying the leniency proxy blind to the new arm predicted its bias
   at 3.1 pp against a measured 3.0 pp, so the transfer assumption is tested
   and holds — keep the estimator, it still bounds the 14 unsubmitted cells.
4b. **Related work closes on publishability, not on accuracy.** All four
   comparable artifacts are unpublished, for two reasons: MotherDuck's,
   NVIDIA's and OceanBase's are fused to the test set, Xiaomi's to its own
   revenue definitions. Ours is in the repo with a digest because a contract
   written from a vendor manual has neither coupling. Make this the closing
   move of §8 — it is the sense in which a data contract is a different object
   than a semantic layer, and it costs us the accuracy gap rather than
   excusing it. Verified, not assumed: the NeMo Agent Toolkit and its examples
   repo have no `dabstep`/`helper.py`; DataPilot's leaderboard "Repo URL" is a
   blog post; none of the 81 `github.com/XiaoMi` repos covers this work.
5. **Run B's 29% truncation.** Report in full: near-uniform across arms
   (28-30%), 114 of 122 lost tasks lost all four arms together, complete-case
   and per-arm figures agree within a point. It costs power, not validity.
   The 122 tasks are re-runnable but only on a different provider than the
   other 279, which is its own confound; leaving them out is cleaner.
6. **Single benchmark.** Cannot be fixed with compute. State it plainly.
7. **The `COUNT(*)` validator defect** (fixed in `cde8b20`) handicapped run
   A's contract arms and neither baseline; run B has the fix and its
   inspect-rejection count falls 218 -> 29. Report run A as a floor. The
   defect's direction is confirmed, its magnitude is tangled with the model
   change.

## The pro sweep: design

### Which model

**Corrected 2026-09-01.** The figures below replace an earlier table built from
OpenRouter *list* prices. `dce/pricing.py` pins `deepseek-v4-pro-0813` to the
`alibaba` endpoint at $0.5808/$1.7424/$0.0581 per 1M (in/out/cached), roughly
half list — so the pre-registered sweep costs about **half** what this plan
previously said.

Projected from run B's measured token profile across the three sweep arms
(77k fresh input, 472k cached, 39k output per task; 86% cache hit) at each
model's **pinned endpoint** price. Upper bounds: a stronger model needs fewer
turns, and run B's contract arm already used 40% fewer than its baselines.

| model (pinned endpoint) | per task | 240 tasks | **all 401** |
|---|---:|---:|---:|
| **`deepseek-v4-pro-0813`** [alibaba] — pre-registered | $0.141 | $34 | **$56** |
| `openai/gpt-5.6-sol` [openai] — cross-family | $0.641 | $154 | $257 |
| `z-ai/glm-5.3-flash` [z-ai] — already run | $0.023 | $5 | $9 |

**At $56 there is no reason to subsample.** The power analysis below was written
when the sweep looked like a $94 decision; it now argues only about what the run
can *detect*, not about how many tasks to buy. Run all 401, matching run A's
task set exactly.

The pre-registered model is also the only affordable one, which resolves the
tension between methodological duty and budget. Its weakness is family: run B is
already DeepSeek, so pro does not answer "is this a Chinese-open-weight-model
quirk". `gpt-5.6-sol` is the pinned cross-family option at $257 for a full
sweep, or ~$51 for an 80-task partial arm — worth considering *only* if the
capability probe shows it materially stronger, since a model we cannot afford to
sweep is a model the probe cannot inform a decision about.

**Do not pick the third model by reputation — measure it.** What gap 2 needs is
a point further along the *capability axis*, and that axis is operationally
`schema_only` accuracy (13.9% glm -> 22.6% deepseek-flash). A one-arm,
50-task `schema_only` probe costs ~$3 on pro and ~$13 on sol.

**A constraint the runner enforces:** the spend cap must admit one worst-case
task-group reserve, computed from the per-request token cap (3.29M) times the
model's output price. For `gpt-5.6-sol` that is ~$33 per group *before any
observation tightens it*, so a probe of that model cannot run under a cap below
~$35 however little it will actually spend. Probe models one at a time.

### Probe result: pro is not a third capability point

**Run 2026-09-01.** `schema_only`, 50 stratified tasks, $2.40. Paired on the 40
hard tasks all three models have scored:

| model | hard | 95% CI |
|---|---:|---|
| `glm-5.3-flash` | 6/40 = 15.0% | [7, 29] |
| `deepseek-v4-flash` | 10/40 = 25.0% | [14, 40] |
| **`deepseek-v4-pro`** | **11/40 = 27.5%** | [16, 43] |

Pro vs flash: 3 discordant pairs (1 vs 2), **p=1.0**. Pro spends 19.4 turns per
hard task against flash's 18.6 to land in the same place.

**What this does not say.** `schema_only` measures how much *unstated domain
convention* a model recovers from a bare schema. That a `NULL` in `fees` means
"applies to every value" is a fact about Adyen's business, not something
reasoning recovers — so this arm is closer to a floor on missing information
than a capability test. It is not evidence that pro is a weak model, and the
plan should stop calling `schema_only` accuracy "the capability axis" without
that qualification.

**What it does say.** Bare-schema hard accuracy sits in a 15--27% band across
four models and three families, including the DABStep paper's own o4-mini at
14.6%, while the contract arm sits at 55--57%. If that survives the `gpt-5.6-sol`
probe, **the reportable finding is that bare-schema accuracy is largely
capability-insensitive on this benchmark** — which argues the paper's thesis
directly (the bottleneck is information, not reasoning) and is stronger than the
interaction plot it replaces. Section 5.5 would change from "two readings we
cannot separate" into a result.

**The pro sweep still has a live rationale, and it is not the one this plan
started with.** The probe measured only the bare-schema arm; how pro behaves
*with* a contract is unmeasured. If pro reaches materially above flash's 56.6%
on the contract arm, capability and context are **complementary rather than
substitutes** — which reframes the interaction rather than closing it, and is a
result neither reading in Section 5.5 predicts. That question needs the contract
and manual_prompt arms, so it needs the sweep.

### Probe result: sol supplies the third point, pro does not

**Both probes run 2026-09-01**, `schema_only`, 50 stratified tasks each,
$2.40 and $2.11. On the 40 hard tasks all four models have scored:

| model | hard | 95% CI | vs sol (McNemar) |
|---|---:|---|---|
| `glm-5.3-flash` | 6/40 = 15.0% | [7, 29] | 0 / 11, **p=0.0010** |
| `deepseek-v4-flash` | 10/40 = 25.0% | [14, 40] | 0 / 7, **p=0.0156** |
| `deepseek-v4-pro` | 11/40 = 27.5% | [16, 43] | 1 / 7, p=0.0703 |
| **`gpt-5.6-sol`** | **17/40 = 42.5%** | [29, 58] | --- |

Sol essentially dominates: it takes 7--11 tasks the others miss and gives up
one. It is also the cheapest and fastest of the three ($0.042/task, 8.3
turns/row against pro's $0.048 and 17.4).

**This falsified an interim conclusion recorded in this plan**, that
bare-schema accuracy was "capability-insensitive on this benchmark". The
15--27% band was a property of the model tier sampled, not of the task. The
lesson is narrow and worth keeping: three models that cluster are not a
ceiling, and the reframing was written after three points and would have been
wrong.

### The decision, and the criterion pre-committed before results

**Running the four-arm 401-task sweep on `gpt-5.6-sol` ($28, flex tier).** It
supplies the capability point pro cannot, it is cross-family (killing "an
open-weight quirk"), and it is the cheapest of the candidates.

**`deepseek-v4-pro` is deferred, not cancelled.** It is the pre-registered
primary, so the substitution has to be argued rather than assumed:

> We pre-specified `deepseek-v4-pro-0813` as the primary comparison. A
> capability probe (n=50, published) showed it does not differ from the flash
> tier on the bare-schema arm --- 27.5% vs 25.0%, 3 discordant pairs, p=1.0 ---
> so it would not supply the capability contrast the primary was chosen to
> provide. We substituted `gpt-5.6-sol`, selected by the same pre-specified
> criterion, and publish the probe that drove the substitution.

What makes that legitimate rather than a researcher degree of freedom: **the
probe measured the covariate, not the effect.** Model selection used
bare-schema accuracy while blind to the `contract` vs `manual_prompt` contrast
the paper is about. Selecting on a covariate measured independently of the
treatment comparison does not bias the comparison.

To keep it that way, the criterion for running pro after all is fixed **now**,
before the sol sweep reports:

1. Sol's sweep is compromised the way run B was --- a truncation, or a harness
   failure rate above 5%.
2. Sol's result is ambiguous in a way a fourth capability point at 27.5% would
   actually resolve.

Not "if we dislike the answer". If neither condition fires, pro stays unrun and
the substitution paragraph above goes in the paper.

### Which arms

**Three, not four: `schema_only`, `manual_prompt`, `contract`.** Drop
`contract_hollow`. Its null is established on two models and pooled, and the
pro run's job is the interaction, not the decomposition. `schema_only` must
stay — it *is* the capability axis the interaction is measured against.
Saves 25%.

### How many tasks

Not all of them. But the reason is not cost, and this must be understood
before the run is designed:

Power to detect `contract` > `manual_prompt` at alpha=0.05, discordance rate
0.30, by task count and psi (the share of discordant pairs favouring
`contract`; glm 0.82, deepseek 0.65):

| n | psi=0.80 | psi=0.70 | psi=0.65 | psi=0.60 | psi=0.575 |
|---:|---:|---:|---:|---:|---:|
| 100 | 0.91 | 0.52 | 0.31 | 0.14 | 0.09 |
| 200 | 1.00 | 0.86 | 0.61 | 0.29 | 0.18 |
| 279 | 1.00 | 0.95 | 0.75 | 0.42 | 0.24 |
| **401** | 1.00 | 0.99 | **0.90** | **0.55** | **0.34** |

**If the trend continues and psi falls to 0.60 on pro, even all 401 tasks
gives 55% power.** Sample size cannot rescue a shrinking effect. So the pro
run must be designed and reported as an **estimation** run — a confidence
interval on the paired difference — and not as a significance test it may be
unable to pass. Reporting "p>0.05, no effect" from an underpowered arm would
be the worst outcome available, and it is avoidable only by saying so in
advance.

Precision, by contrast, is affordable. 95% CI half-width on the paired
accuracy difference at discordance 0.30, and near-independent of the effect
size:

| n | 100 | 150 | 200 | 250 | 332 | 401 |
|---|---:|---:|---:|---:|---:|---:|
| half-width | ±10.5 | ±8.6 | **±7.5** | ±6.7 | ±5.8 | ±5.3 |

**n=200 gives ±7.5pp**, which separates deepseek's +13.7pp from zero and is
the point where the curve flattens. Recommended design: **200 hard + 40 easy,
stratified random, seed fixed and recorded in the plan before the run.** The
easy stratum is the null control — the contract should do nothing there, and
it does on both runs.

Cost at 3 arms x 240 tasks on the pre-registered model: **~$55**.

Pre-specify the subsample and the estimation framing *before* running, so
that neither looks chosen after seeing the result.

## The leaderboard submission

Not a ranking play — we would land mid-table and the paper explicitly
disclaims competing on score. The reason to submit is **gap 4**.

Every submission publishes a per-task file (`data/task_scores/<id>.jsonl`)
carrying `{"task_id", "score": true|false, "agent_answer"}` for all 450 tasks,
graded by DABStep's own withheld golds. Submitting the contract arm's answers
therefore returns an official per-task verdict on the exact answers we already
scored against reconstructed golds — turning "our golds are probably right"
into a measured agreement rate on 401 tasks. That is the one caveat a reader
cannot otherwise check.

Conditions, since it is public and effectively one-shot per agent name:

* Run all 450 first — we currently run the 401 with reconstructed golds.
* Submit the **contract arm only**. Submitting several arms under different
  names to validate both ends of the contrast is scientifically better and
  reads as leaderboard-stuffing; not worth it.
* Accept that our answers become public and enter other people's gold
  reconstruction, exactly as we used theirs.
* Accept the downside symmetrically: if official and reconstructed verdicts
  disagree materially, that is discovered in public. It would be discovered
  eventually anyway, and better before the paper is cited than after.
* Requires the user's explicit go-ahead. Time it with the preprint.

## Sequencing

1. ~~deepseek-flash sweep~~ **done** — two model families, "it's a glm quirk"
   is dead
2. **Start drafting now.** Sections 1-4 and 6-9 do not depend on any pending
   run; every number they need is in `FINDINGS.md`.
3. Capability probe (~$20-47) -> pick the third model on measured
   `schema_only` accuracy, not reputation
4. Pre-registered sweep, 3 arms x 240 stratified tasks (~$55) — the one run
   that must happen before submission
5. k>1 probe (gap 3)
6. Fold both into section 5; arXiv preprint
7. ~~All-450 contract-arm run + leaderboard submission (gap 4)~~ **done
   2026-09-04** — submitted as `agentic-data-contracts`/`flyersworder`,
   scored 51.9% hard / 70.8% easy / 54.9% overall
8. Submit to PVLDB EA&B

Drafting and the pro sweep are independent and should run concurrently. Do
not post the preprint before the pro sweep lands: a v1 that omits its own
pre-registered primary invites exactly the objection a v2 would then look
like it was patching.

## Run D: Claude Sonnet 5, and how it enters the paper (2026-09-03)

**Status: folded into the paper on 2026-09-03** — every section, all three
figures, `make check` clean at 20 pages. The list below is what was done.

A fourth full sweep landed via PR #92: `claudesonnet5`, 401 tasks x 4 arms,
over an enterprise LiteLLM gateway fronting Bedrock EU, $164.75, zero harness
failures. `FINDINGS.md` carries it in full as run D. This section records the
decision on the paper.

### Decision: a full fourth run in the main text, not an appendix

The paper already names this experiment as the one it most needs. The
conclusion's future work: *"A second frontier model would test every
capability claim in this paper, all of which now rest on one, which is the
exact condition under which a claim here has already broken once."* The
threats section is titled *"One frontier point, and a trend that already
broke once."* The governance section ends: *"Whether such rules deter a model
that was not going to try is untested."* Run D speaks to all three, and a
reader who finds it in an appendix will ask why.

### What run D does to each claim

Replicates, and should be reported as replication:

- **Scaffolding step is not zero.** +15.1 pp hard, p=9e-09 (14/64), on a
  third model family. "Undetectable on two, +14.2 on the third" becomes
  "undetectable on the two flash models, +14.2 and +15.1 on the two stronger
  ones". Content step still dominates: 2.0x on Sonnet 5 (1.8x on sol).
- **Margin over a prompt is not monotone.** +44.6, the largest of the four,
  on a model whose bare-schema score (22.9%) ties deepseek's (22.6%).
- **Delivery over possession, behaviourally.** Contract arm writes all six
  clauses 55% of the time, manual_prompt 4% (Fisher 9e-16). Fourth model on
  which the prompt arm sits at 3-9% regardless of capability.
- **Contract arm contributes no diagnosable error.** 0/11 on run D; pooled
  0/46 vs 103/766, Fisher p=0.0025.
- **Work claim.** Fewest turns (7.0), tool calls (3,791), reasoning (824k),
  zero forced answers (vs 43/32/27). Cost: second-cheapest outright by 3%,
  cheapest per correct answer by ~2x. The withdrawn price claim stays
  withdrawn, but the paper can say "cheapest on two, within 3% on a third,
  and cheapest per correct answer on three of four".
- **Leaderboard-band check on a second family.** manual_prompt 23.8% hard
  vs the leaderboard's 19.8% for Claude 4 Sonnet under the same approach.

Extends, with one new sharp result:

- **The manual in the prompt does nothing on Sonnet 5's hard tasks.**
  79/332 vs 76/332 for a bare schema, McNemar p=0.72. Same 24k characters
  that lift glm +9 and deepseek +20 lift Sonnet 5 +0.9; the contract carrying
  the same knowledge lifts it +45.5. This is the strongest instance of the
  paper's central claim and deserves its own paragraph in Section 5. It also
  means empty governed tools (37.9%) beat the whole manual (23.8%) on this
  model, p=1e-04 -- the first model where scaffolding alone outperforms
  possession as prose.
- **Governance gets its frontier-class hazard.** 22 mutating statements on 2
  manual_prompt tasks, none in schema_only, none governed; task 2546
  escalated to persistent `CREATE OR REPLACE TABLE` + `ALTER` + `UPDATE` and
  corrupted the warehouse -- the second corruption in 6,416 rows, the first
  from a strong model, with the same narrated diagnosis as deepseek's ("temp
  tables might be getting dropped between separate execute_sql calls").
  Totals become 166 statements / 26 tasks / 0 governed, pooled Fisher 3e-08.
  n=2 attempting tasks is disclosed as such.

Softens, and the prose must change rather than grow:

- **"Capability arrives unevenly, nearly all of the frontier's gain lands on
  macro."** Sonnet 5's derivation gap is 26.1 pp (between ~38 and 5.1, and
  the macro-gap series 39.2 / 37.3 / 26.1 / 5.1 is monotone in bare-schema
  order), but Sonnet 5 is the *best* of the four models on the derived bucket
  (60.1% vs sol's 56.1%) while trailing sol by 21 on macro. Its gain over
  deepseek is +11.2 macro, +13.6 derived. Keep: the macro gap closes with
  capability, the cost of deriving is transient. Drop: "the derived bucket
  barely moves" and "what survives a frontier model is only the reasoning on
  top". The abstract's "capability arrives unevenly" sentence needs rewriting
  to say the macro gap is what closes, not that derived stands still.
- **"Capability changed the idiom" (governance).** Sonnet 5 uses CTEs on
  32%/37% of ungoverned traces -- glm's rate, not sol's 59%/55% -- and
  attempts CREATE TEMP 8 times. Idiom is model-family, not capability.
- **"Error legibility is a function of capability."** Lesions diagnose 10%
  of Sonnet 5's errors (sol 46%, glm 8%, deepseek 3%). Legibility tracks the
  bare-schema score (where Sonnet 5 ties deepseek), not the contract-arm
  score. Reword to "tracks what the model does unaided".
- **"Ordered by capability."** The paper uses bare-schema accuracy as the
  capability axis. Sonnet 5 and deepseek are 0.3 pp apart on that axis and
  11.8 pp apart with the contract. Ranks agree under either ordering, spacing
  does not; say so once in threats and keep the bare-schema axis (it is the
  only pre-declared one).

### Section-by-section edit list

- **Abstract**: four families; add the 22.9 -> 68.4 pair; scaffolding
  sentence; four-number recovery series with the derived series beside it;
  6,416 transcripts.
- **Intro**: the +5.4 / +0.0 / +14.2 sentence and the 39.2 / 37.3 / 5.1
  sentence each gain a number; nothing structural.
- **Protocol**: model table gains a row; a paragraph on the gateway route --
  Anthropic Messages API (the OpenAI-compatible route bills every input token
  fresh at an arm-dependent multiple, 1.86x vs 3.40x, a caching confound),
  `anthropic_effort=medium` as a translation of the same scale, and **neither
  temperature nor seed is accepted** (HTTP 400), uniform across arms.
- **Results 5.2** ("what the third model changed"): add one paragraph --
  the fourth model replicated both reversals and softened one new claim.
- **Results tables**: headline (run D block), McNemar (column), ablation
  table (row) and figure (series), efficiency (run D block + cost/correct),
  buckets (two columns; 9 columns will overflow -- switch to one row per
  model or drop the oracle row into the caption), interaction (row).
- **Results prose**: leaderboard check on a second family; the
  manual-does-nothing paragraph; derivation-gap rewrite per above; "Run D
  lands between" after the pre-registered prediction, flagged as not
  pre-registered.
- **Mechanism**: clauses table column; counterfactual rows; legibility
  reword; Fisher update.
- **Governance**: table row; 166/26; second corruption with the quoted
  reasoning; idiom paragraph reworded; "nothing to deter" paragraph now
  answered at n=2.
- **Threats**: retitle "One frontier point" to "Two frontier-class points
  that disagree on size"; temperature unpinned on *both* strong runs;
  contamination collinearity now includes Sonnet 5; run D stamps the hollow
  digest correctly (the arm-D digest defect is runs A-C only); **verify
  before claiming** whether run D's commit `b55c932` includes the
  preview_table cap fix, and count preview calls >50 rows either way.
- **Conclusion**: 1.8x / 6.6x gains 2.0x; recovery series to four; "144 to
  zero on two models, third never tried" becomes "166 to zero on three of
  four, including one frontier-class model that did try"; future work:
  second frontier model is done -- what remains is repeats, the executable
  metric layer, and a capability axis that is not the bare-schema arm.
- **Artifact availability**: 6,416 transcripts. Run D's 1,604 traces
  (32 MB) are on disk under `traces/sonnet5-full/` and git-ignored like the
  others; the release bundle must include them.
- **Figures**: `make_figures.py` RUNS/MARKER/LINESTYLE/HARD_EXPECTED gain
  a fourth entry (diamond, dash-dot); the interaction figure's x-axis puts
  Sonnet 5 at 22.9 beside deepseek at 22.6, so the two labels need an offset.
  The script asserts every drawn value against the paper's numbers, so the
  tables and the script move together.

### Sequencing

If the arXiv v1 has not been posted, fold run D in before posting: a v1 with
"one frontier point" in its threats section followed weeks later by a v2 that
adds the point the v1 asked for is the shape of a paper that ran the
experiment because a reviewer asked. If v1 is already up, this is the v2, and
the PVLDB submission uses it. Either way the FINDINGS integration is done and
the paper edit is one focused pass over ten sections plus the figure script,
with `make check` gating the wider tables.

## Paper 2 (deferred)

An experience report on deploying governed agentic SQL against a dialect no
parser models: the parse/emit asymmetry, template assembly instead of SQL
generation, honest degradation to `unverified_compliance`, and a framework
migration forced by operations.

**It becomes a paper only if quantitative disclosure is cleared** (contracts
in use, tables covered, error-rate change). Without numbers it is an
architecture description and a reviewer will say so — in which case the
material belongs in a practitioner talk or a written case study, which reaches
the adopting audience better anyway and clears far more easily.

Keep the two questions disjoint so neither can be called redundant:
**Paper 1 asks whether it works and which part does the work; Paper 2 asks
what it takes to run it.** Paper 2 cites Paper 1 for the mechanism and spends
its pages on the deployment.
