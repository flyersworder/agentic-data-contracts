# Paper v3: one thesis, scoped claims

Two independent reviews of the PVLDB version, a fresh Opus 5.5 reader and
gpt-6.1-sol through Copilot CLI with the artifact, both returned major
revision at confidence 4 of 5. gpt-6.1-sol recomputed 24 headline numbers
from the committed rows: 21 match, 2 need private data, and 1 was a
wording error, since fixed in 9316226. The science holds. What the
reviewers object to is how much is claimed and how much is packed into 12
pages. This design restructures the PVLDB version around one thesis.
Nothing is deleted from the extended version.

## The thesis

> On DABStep, a context layer's gain is its knowledge, and most of the
> contract's lead over the manual is two facts the manual leaves out. Once
> the facts are equal, delivery's effect on accuracy is an order smaller
> than knowledge's and depends on the model: from about 0 to 10 points,
> against 26 to 50. What delivery reliably buys is cost at scale and a
> record of which definition each answer used.

Determinability becomes the scoped secondary finding that explains why
LiveSQLBench behaves differently. It is no longer a general bound.

## The storyline

**The problem.** Teams build context layers for analytics agents:
semantic layers, knowledge bases, documentation behind tools. Vendors
report gains as large as 72 points. Building one means two investments:
writing the knowledge down, and building the machinery that delivers it
(lookup tools, governed access, retrieval, progressive disclosure). A
with/without comparison can't say which produced the gain, and
practitioners need to know where to put the effort.

**The instrument.** A data contract can be taken apart:
- emptied of its knowledge while keeping its tools and rules byte for byte
  (the hollow contract);
- pasted into the prompt instead of looked up;
- given extra facts as notes;
- written by different authors.

It runs on DABStep and LiveSQLBench across seven models, with predictions
committed before the runs.

**Act 1: knowledge does the work.** Emptying the contract costs 26 to 50
points on DABStep's hard tasks, on every model that ran it. The empty
machinery is worth -9 to +15.

**Act 2: what looked like a delivery win was knowledge.** The contract
beats the pasted manual by 20 to 23 points, which looks like the case for
structured lookup over a long prompt. Two facts the manual leaves out
decide most of the gap:
- an empty list means "applies to all";
- overlapping fee rules add up.

Stated in words in the manual, they recover most of the gap, including on
165 tasks nobody inspected. With the facts equal, format and tools move
accuracy by 0 to 10 points depending on the model, mostly in a few fee
families:
- form: +3.7 on luna, +5.5 on Qwen;
- tools against the same text: -0.7 on luna, +10.1 on Qwen, on the
  rule-matching tasks.

**Act 3: where the knowledge comes from, and where it runs out.** An
author or compiler that checks definitions against the data recovers what
the data settles, but not a convention the data can't reveal. On
LiveSQLBench the documentation leaves a result-changing choice open on
over four tasks in ten, and no author we tried gained. This is a scoped
hypothesis: a layer can add only what the documentation and data
determine.

**Act 4: what delivery buys.** Compared with pasting the whole prompt,
looking definitions up:
- makes cost scale better: 1.7 to 2.4 times cheaper at four times the
  size, on the GPT models;
- keeps working past the context window;
- leaves a record of which definition each answer used.

**Takeaway.** For accuracy, complete the knowledge: state every
convention a reader could misread, and check it against the data. Choose
the delivery machinery for cost, scale and auditability.

**Contributions.**
1. A way to take a context layer apart: the hollow contract and the
   text-versus-tools and facts-as-notes arms.
2. The main finding: on DABStep the gain is knowledge, and the apparent
   delivery advantage is two omitted facts, confirmed on held-out tasks.
   This is a caution for reading context-layer and leaderboard gains.
3. Delivery measured with the knowledge held equal: its accuracy effects
   are small and depend on the model; its cost, scale and audit benefits
   are real.
4. A scoped account of authoring: what a layer can add is limited by what
   the documentation and data determine (LiveSQLBench, as a hypothesis).
5. The artifact: harness, contracts, committed predictions (20 of 53
   failed, reported), rows, and transcripts on Zenodo.

**Not claimed:**
- that contracts beat documentation retrieval;
- that the findings hold beyond fee-rule procedures;
- that the scale result tests context rot.

## Claims, before and after

| Now | After |
|---|---|
| Contribution (3): "determinability as the bound on what any layer or author can add" | "On LiveSQLBench's tested databases, the knowledge a contract can add is limited by what the documentation and data determine; we state this as a hypothesis, with the evidence and its size." |
| "the tools alone are worth -9 to +15" | "the empty scaffolding, tools whose lookups return nothing, is worth -9 to +15"; the knowledge-equal comparison (Section 6) measures delivery |
| Representation "adds about 4 to 6 points" | "a package effect (format, SQL definitions, repetition) of 3.7 and 5.5 points; significant under family clustering, not under question-shape clustering on Qwen; provisional" |
| Lookups "add a few points on some fee families"; the Qwen +10 | Labelled as against the original wording, not the clarified prose |
| "Do not expect a format, a retrieval loop or a stronger model to supply..." | "Do not expect a format or a delivery mechanism to supply..."; no retrieval loop or stronger model was tested there |
| "past about twice that, look definitions up" | The crossover in tokens (about 26k base, 92k at 4x) and pricing conditions; it reverses on Qwen's billing |
| "Runs that state the rules do not write" | "No observed persistent mutations where the rules were stated", confounded with knowledge and workflow, mostly Qwen's |
| Lookup log "traces a wrong answer to the definition" | "syntactic inclusion audit": the consulted expression appears in the submitted SQL |
| "The natural reading, and the one an earlier version of this paper gave" | State the delivery hypothesis and refute it; no preprint history in either build |
| Provenance threat | Add plainly: the summing convention is the one fact the independent compile did not recover; the robust claim is "the manual omits these facts and they matter", not "an author would find them" |
| Ties counted as held predictions | Say which are failures to reject, not equivalence |

## PVLDB outline (12 pages)

| § | Content | Pages, now -> target |
|---|---|---|
| 1 Intro | The question, the fee example, the thesis, three findings, scoped contributions, practitioner bullets (reworded as above) | 1.3 -> 1.2 |
| 2 Background | Trimmed; positions retrieval: the paper attributes content and compares two delivery implementations, it does not rank contracts against documentation retrieval; CHESS discussed | 1.0 -> 0.8 |
| 3 Instrument | Contract, tools, hollow contract. The three authoring paths move to section 7 | 1.2 -> 0.9 |
| 4 Design | Benchmarks, the gold reconstruction and contract timeline (now self-contained), core arms only, models, statistics and prediction registration in one paragraph. The 13-row experiment table shrinks to the experiments the PVLDB version keeps | 2.7 -> 1.8 |
| 5 Knowledge does the work | The hollow ablation on six models | 0.6 -> 0.6 |
| 6 What looked like delivery was knowledge | One fact; the second fact with the 165 held-out tasks; the facts stated alike (package effect, provisional); governed tools against the same text | 1.6 -> 1.8 |
| 7 Where the knowledge comes from, and where it runs out | Compile and better compile (short); the gaps experiment with the 9 of 15 ruling; LiveSQLBench determinacy as the scoped finding (moved from 5.2); the LSB author result in two sentences | 1.7 -> 1.6 |
| 8 What delivery buys | Cost and scale as structured lookup against the whole prompt, in tokens; the inclusion audit; governance in one paragraph | 1.0 -> 0.9 |
| 9 Threats | Adds the provenance sentence, the retrieval scope and the ties | 0.8 -> 0.8 |
| 10 Conclusion | | 0.3 -> 0.3 |

Freed: about 1 page, which pays for the self-contained gold and timeline
paragraph and the added caveats.

To the extended version only:
- 5.3, the pre-computed layer (a coverage diagnostic, not a deployment comparison);
- 6.5, complete prose carries a procedure;
- the LiveSQLBench oracle experiment;
- the scale details;
- most of the run-by-run numbers.

The PVLDB version keeps one sentence each and a pointer.

## Abstract (draft, about 150 words)

> Context layers, curated documentation an analytics agent consults at
> query time, report large accuracy gains on text-to-SQL benchmarks, but a
> with/without comparison cannot say which part of the layer does the work.
> Using a data contract as the instrument, we separate the knowledge a layer
> carries from the tools and format that deliver it, on DABStep and
> LiveSQLBench across seven models, with predictions committed before the
> runs. The knowledge does the work: emptying the contract while keeping its
> tools costs 26 to 50 points on DABStep's hard tasks. Most of the
> contract's lead over the pasted manual is two facts the manual leaves out.
> Stated in words, they recover most of it, also on 165 tasks nobody
> inspected. Once the facts are equal, delivery moves accuracy by 0 to 10
> points depending on the model; what it reliably buys is cost at scale
> and a record of which definition each answer used. Where the documentation leaves the answer open, as on
> much of LiveSQLBench, no contract author we tried helped.

## Open decisions

1. **No new run (decided 2026-10-23).** We considered completing the
   knowledge x delivery 2x2 on gpt-6-sol: `contract_text`, plus
   `manual_summed` and `contract_text_noted`, k=3, with a drift rerun of
   `contract`. It would cost about $170-210 at the panel's recorded per-task
   cost. It is skipped, because:
   - the likely result is a tie, since luna, of the same family, gave -0.7;
   - it touches none of the weaknesses that drove either verdict;
   - generality would need a new benchmark, not another model.

   Instead:
   - state the knowledge-equal delivery result as measured on luna and Qwen;
   - rebuild the decomposition table so each model's row shows only arms
     that ran together.

   The run is held as a ready answer if the revision round asks for the
   strongest model.
2. **The preprint (decided 2026-10-23).** The paper stands on its own:
   no correction note, no "an earlier version", and no reliance on the
   preprint for content. The author will release a new preprint version
   separately. Both builds drop the history, and any remaining citation
   of the preprint is checked.

## Order of work

1. Claims table edits across sections 1, 5, 6 and 8, and the threats.
2. Moves: 5.2 to 7, 5.3 and 6.5 to the extended version, the authoring
   paths to 7.
3. Section 4 rewrite: the gold reconstruction and timeline, the slimmer
   experiment table.
4. Abstract and intro last, since they summarise the rest.
5. Both builds checked, the PVLDB content ending on page 12 or earlier.
   Every number touched is checked against FINDINGS.
6. One review round, then copy edits; the AI disclosure goes in then.
