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
either model.** Published context-layer results (MotherDuck Guides, +72pp)
report two accuracy numbers, from which no mechanism can be recovered — was it
the content, the fetch loop, or the pre-built views? This is, as far as we
know, the only controlled answer.

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
section. Ungoverned arms submitted **144 mutating SQL statements across 24
tasks**; both governed arms submitted **zero**, on both models (Fisher
p=6e-08 pooled). The governed arms did not attempt and get blocked — they
never attempted, so the mechanism is deterrence by declared rule, not
interception. One ungoverned run escalated from `TEMP` to persistent tables
and actually mutated the warehouse copy, narrating its reasoning for the
escalation as it went.

This matters for an EA&B audience independently of accuracy: it is a measured
safety property of the same artifact, on the same runs, at no extra cost.

## Structure, and what exists today

| § | content | status |
|---|---|---|
| 1 Introduction | context layers work; nobody has said *which part* works | to write |
| 2 Background | DABStep; the semantic-layer / context-layer landscape | to write |
| 3 Experimental design | four arms, what is held fixed, the hollow control | **`FINDINGS.md`** |
| 4 Protocol | frozen digest-pinned artifact, official vendored scorer, gold reconstruction + independent verification | **`FINDINGS.md`**, `dce/golds.py` |
| 5 Results | four arms x 2 models, every pairwise McNemar | **both runs done**; pro sweep outstanding |
| 6 Mechanism | task 1278 traced end to end; both baselines produce the identical wrong number | **`FINDINGS.md`** |
| 6b Governance | 144 mutating statements vs 0, deterrence not interception, the task-68 escalation | **`FINDINGS.md`** |
| 7 Threats to validity | one benchmark / 26 templates, reconstructed golds, k=1, run B's 29% truncation, self-attested provenance | **`FINDINGS.md`** |
| 8 Related work | MotherDuck Guides; why their 99.8% and our 55.1% measure different things | **`FINDINGS.md`** |
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
   is far better. Full 401x4 is roughly **$60-90** (pro is ~9x flash on input,
   ~10x on output, against run B's $6.32); a stratified subsample is
   proportionally less.
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
4. **Gold reconstruction needs its own subsection**, not a footnote. The
   defence is strong — only answers DABStep's own grader marked correct enter
   the vote, and 59/59 of the largest template family reproduce from the
   database independently — but it has to be argued, since golds are the one
   thing a reader cannot check for themselves. **A leaderboard submission
   would convert this from an argument into a measurement** — see below.
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
3. Pre-registered `deepseek-v4-pro` sweep (gaps 1+2) — the one run that must
   happen before submission
4. k>1 probe (gap 3)
5. Fold both into section 5; arXiv preprint
6. All-450 contract-arm run + leaderboard submission (gap 4), on the user's
   go-ahead
7. Submit to PVLDB EA&B

Drafting and the pro sweep are independent and should run concurrently. Do
not post the preprint before the pro sweep lands: a v1 that omits its own
pre-registered primary invites exactly the objection a v2 would then look
like it was patching.

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
