# Paper v3: missing data semantics, not delivery

**Status.** Revision 2, after a review round on revision 1 (822d7cf). The
earlier review rounds:
- **Round 1, on the PVLDB version.** A fresh Opus 5.5 reader and gpt-6.1-sol,
  through Copilot CLI with the artifact, both returned major revision at
  confidence 4 of 5. gpt-6.1-sol recomputed 24 numbers from the committed
  rows: 20 match, 1 matches in part, 2 need private data, and 1 was a
  wording error, fixed in 9316226.
- **Round 2, on revision 1 of this spec.** gpt-6.1-sol and gemini-4-argon
  through Copilot CLI, and a fresh Opus 5.5 reader, each with the artifact.
  The verdicts were "needs rework", "ready with changes" and "ready with
  changes". All three found the same fault: the thesis and the draft
  abstract re-introduced the over-claims the claims table removed. The
  reviews are kept outside the repository, in
  `~/.cache/review-sandbox/review5/`.

This revision restructures the PVLDB version around one finding, states
every number with the scope it was measured on, and adds one small run,
experiment 5d. Nothing is deleted from the extended version.

## The thesis

> On DABStep, most of the context layer's apparent advantage over the
> documentation is two pieces of data semantics the documentation omits:
> - a wildcard stored as an empty list;
> - how the fees of several matching rules aggregate.
>
> Supplied in prose, they recover most of the contract's lead on luna and
> Qwen, also on 165 tasks outside the transcript-inspection set. With the
> facts held equal, the remaining effects of representation and route are
> small, model- and task-dependent, and of either sign, and LiveSQLBench's
> prompt-versus-tools comparison ties. Lookup's case is operational: it
> keeps working past the context window, and it logs which definitions
> were consulted.

The exp5d result is pending. Once it is in, the delivery sentence states
it, whichever way it falls.

## The storyline

1. **The problem.** Context-layer gains are reported with/without. That
   comparison confounds *what is documented* with *how it is delivered*,
   so it can't tell a team whether to invest in writing the semantics down
   or in the delivery machinery: lookup tools, governed access, retrieval,
   progressive disclosure.
2. **The protocol.** A data contract makes the semantics explicit and
   separable from the tools, so each can be varied with the other held
   fixed:
   - the hollow contract keeps the tools and empties the knowledge;
   - facts can be added as notes in identical words;
   - the same contract can be pasted into the prompt or put behind the
     tools.

   This is why the instrument is a contract and not any structured
   document (review B, W8).
3. **The finding.** On DABStep the contract beats the pasted manual by 20
   to 23 points on the two repeated panels, which looks like a delivery win.
   It is mostly two omitted data semantics:
   - **a sentinel encoding.** The manual says null means "applies to all";
     the data stores that as an empty list in 720 of 1,000 rules for
     `account_type`.
   - **join multiplicity and aggregation.** Several fee rules match one
     payment, and their fees sum. The manual defines one rule's fee and is
     silent on overlap.

   Stated in prose, they recover most of the gap on luna and Qwen,
   including on the 165 rule-matching tasks outside the
   transcript-inspection set (+19.2 and +33.9 points). On gpt-6-sol only
   the first was tested; it recovers 77%.
4. **Delivery with the knowledge held equal.** It is measured on luna and
   Qwen, mostly on the rule-matching subset.
   - **Form** (the contract's text against prose, both with the notes):
     +3.7 and +5.5 on the rule-matching tasks, a package of format, SQL
     definitions and repetition. It is provisional: it does not clear zero
     under question-shape clustering on the held-out tasks, and on luna it
     rests largely on the 34 inspected tasks. On the other 202 tasks it is
     +9.5 on luna and -5.3 on Qwen.
   - **Route** (tools against the same text): experiment 5d replaces the
     confounded +10.1 with the facts stated alike. Experiment 5 gave -0.7
     on luna and +10.1 on Qwen, both against the original wording; on the
     other tasks +5.1 and -6.9.
   - **A second benchmark.** On LiveSQLBench, the same compiled knowledge in
     the prompt or behind the tools ties: 36.8 against 35.9 on luna, 37.7
     against 37.7 on Qwen.
   - The summary is "of either sign, by model and task family"; no range
     is offered as a bound.
5. **What the contract's knowledge is worth, and where it comes from
   (secondary).**
   - **The ablation.** Emptying the contract costs 26 to 50 points on
     DABStep's hard tasks, on the six models that ran it (luna did not),
     nearly all of it on the fee procedure. It is the corroborating control,
     not the headline. The empty scaffolding is worth -9 to +15, and at most
     +0.2 on the two repeated panels.
   - **Data inspection.** It grounds instance-verifiable encodings:
     - the stronger compile found the empty-list wildcard;
     - the gaps experiment recovered 6 of 6 settled gaps and 6 of 9 split
       ones (12 of 15 under the recorded ruling, 9 of 15 otherwise).

     It did not recover a convention the data cannot reveal: 0 of 3, and
     every session chose the lowest rule id for precedence. The summing
     convention in the original contract cannot be credited to data
     inspection (provenance).
   - **LiveSQLBench as a benchmark property.** By two gold-aware model
     raters (kappa 0.79, cross-family checked), the KB leaves a
     result-changing choice open on 132 of 302 primary tasks. On 55 tasks of
     three databases, one model and the authors we tried, there was no
     detectable gain from a better author. This is an observation, not a
     bound.
6. **Operational trade-offs (short).** These come from 60 constructed
   questions that name their metric, near ceiling, with padding from other
   databases:
   - **Accuracy:** pasting up to 4x (about 92k tokens) costs no accuracy.
   - **Cost against the contract's YAML pasted in:** at 4x, lookup is 1.7
     times cheaper on luna and 2.4 on sol.
   - **Cost against the prose KB pasted in at 4x:** about 1.1 times
     cheaper; on Qwen's output-metered billing the prose prompt costs more.
   - **Beyond the window:** lookup keeps working where the prompt doesn't
     fit, as on Qwen at 8x and at all 22 databases.
   - **The consultation log:** a syntactic inclusion check of the consulted
     expression in the submitted SQL, not provenance.
   - **Governance:** no observed persistent mutation where the rules were
     stated, confounded and mostly Qwen's.

**Takeaway for practitioners.** Before crediting a context layer's
machinery, check that the comparison holds the semantics equal. Write down
the semantics a reader could misread:
- wildcard encodings;
- what happens when several rules match;
- units;
- `NULL`s.

Check them against the data. Choose the delivery for the operational
reasons above, not for accuracy.

## Contributions

1. **The finding.** On DABStep, an apparent delivery advantage of a context
   layer is mostly two omitted data semantics, confirmed outside the
   inspection set. It cautions anyone comparing context layers with
   documentation, including DABStep leaderboard claims.
2. **A knowledge-equal protocol.** The hollow control, facts as identical
   notes, and the same text by two routes, read as conditional contrasts
   and not an additive decomposition. It is applied on two benchmarks; on
   LiveSQLBench, the route ties.
3. **Benchmark findings.**
   - DABStep's manual omits both semantics.
   - Its scorer ignores sign.
   - The reconstructed golds, against the official grader, run 3.0 to 5.7
     points lenient.
   - LiveSQLBench's grader strips `DISTINCT`.
   - LiveSQLBench's KB, by gold-aware raters, leaves 132 of 302 tasks open.
4. **The artifact.** Harness, contracts, predictions, rows, and the
   transcripts on Zenodo. Its limits are stated: the LiveSQLBench
   transcripts are not released, and 208 DABStep rows have no transcript.

Authoring, scale and cost, and governance are secondary analyses, not
contributions.

**Not claimed.** The paper does not claim:
- that contracts beat documentation retrieval;
- that the findings hold beyond the fee-rule procedure;
- that the scale result tests context rot;
- that any delivery effect is bounded;
- that what a layer can add is bounded by what the documentation and data
  determine.

## Experiment 5d (added; decided 2026-10-23)

- **What it adds.** `contract_noted` is the governed contract plus the two
  notes, in identical words, after its prompt. It runs together with
  `contract` and `contract_text_noted` on luna and Qwen, k=3, at about $15.
- **Registered predictions.** (41) and (41Q) are a route tie against
  `contract_text_noted`; (42Q) is the notes lifting Qwen's governed arm;
  (42) is a luna tie with `contract`. They are fixed in FINDINGS and in
  `analysis/governed_noted.py`, at run commit 9cb3643 (tag `exp5d-run`).
- **What it answers.** Review A's Q4: whether Qwen's +10.1 lookup advantage
  survives once the facts are stated alike. It is the paper's only
  comparison that holds the facts equal and varies only the route.

## Decisions

1. **No sol co-run.** Completing the 2x2 on gpt-6-sol, about $170-210, is
   not run. The reason is budget and scope: the paper's claims stop at the
   models the comparisons ran on. That was not the reason given in
   revision 1. Its arguments ("a tie is likely"; "it touches no
   verdict-driving weakness") were wrong, as all three round-2 reviewers
   said.
   - Every knowledge-equal delivery claim says "on luna and Qwen" up to
     the abstract.
   - Sol's evidence is the panel, the hollow ablation and the one-fact
     result, in its own rows.
   - Table 5 (the ladder) shows co-run blocks per model. The route
     comparison gets its own small table.
2. **The preprint.** The paper stands on its own:
   - no correction note, and no "an earlier version";
   - the extended version is the new arXiv version built from `main.tex`;
   - the PVLDB argument must not need it;
   - appendix J is reworded as the single-run models' analysis, with no
     history.
3. **Small effects.** The form and route effects are reported with a
   sensitivity analysis under exact-match scoring (`analysis/leniency.py`'s
   flag): the effect recomputed with every lenient-only "correct" counted
   wrong. Appendix C's transfer estimator is no longer used as a bound on
   later experiments.

## Claims, before and after

| Now | After |
|---|---|
| "determinability as the bound on what any layer or author can add" (contribution 3, and section 7's bold claims, its "has nothing to hold there" and "no author can recover the gold's choice", and the conclusion) | Removed as a contribution. A rated benchmark property (132 of 302, gold-aware raters), plus "no detectable gain from the authors we tried on 55 tasks". No bound wording anywhere. |
| "the tools alone are worth -9 to +15" | "the empty scaffolding is worth -9 to +15 (at most +0.2 on the repeated panels)" |
| "The contract beats the manual by 20 to 23" | "on the two repeated panels" (13.7 to 44.6 on the single runs; 42.9 on luna's hard tasks) |
| "recover most of the gap" | "on luna and Qwen"; on sol, one fact, 77% |
| "165 tasks nobody inspected" | "165 rule-matching tasks outside the transcript-inspection set" |
| Representation "about 4 to 6 points" | A package effect, provisional; the luna held-out caveat; the other 202 tasks' values |
| Lookups "help on some fee families"; Qwen +10.1 | Experiment 5d's facts-alike route test; the +10.1 labelled as against the original wording |
| "a few points", "0 to 10", "an order smaller" | "of either sign, by model and task family"; no bound |
| "with 53 predictions committed before the results they predict" | "later experiments fixed 53 numbered predictions before their runs; the repeated panels fixed none; one subgroup plan followed aggregate results" |
| 29 held | "23 directional, 6 non-rejected ties, 1 equivalence, 3 partial, 20 failed"; (32) also fails under the 9-of-15 ruling |
| "end-to-end" view | "official plus final-paragraph recovery", defined where it is first used |
| "Do not expect a format, a retrieval loop or a stronger model to supply..." | "Delivery does not establish which undocumented convention is intended" |
| "past about twice that, look definitions up" | The sampled levels (1x cheaper as a prompt, 2x and 4x cheaper as lookup on the GPT models), the pricing, and the prose-arm comparison |
| "Runs that state the rules do not write" | "No observed persistent mutation where the rules were stated"; confounded; 44 of the 50 are Qwen's |
| "records which knowledge each answer used", "traces a wrong answer to the entry behind it" | "a consultation log and a syntactic inclusion check" (sections 1, 8 and 10) |
| "a convention only when the gold uses the default idiom" (abstract, section 7.4, conclusion) | Removed. Data inspection grounds encodings; no session recovered a pure convention; the original contract's summing is not credited to inspection |
| "a contract four times a database's size costs the prompt no accuracy" | "pasting a KB padded to four times its size, about 92k tokens, cost no accuracy on 60 near-ceiling questions" |
| "90% of the way from the first compile" | Defined in place as (Opus - Sonnet) / (original - Sonnet), in the same luna run: 47.3 to 76.6 against 80.0 |
| "The natural reading, and the one an earlier version gave" | State the delivery hypothesis and test it |
| Provenance threat | The independent compile emitted one row per matching pair but left the aggregation open; the robust claim is "the manual omits these semantics and they matter" |
| "A compiler that checks its definitions against the data adds what the data settles"; "large and certain" | Scoped to one compiler on DABStep; "large and consistent across the six models" |
| "selection does not inflate it overall"; the held-out set "understates" it | Report the exclusion rule and the third-repeat check, with no guarantee language |
| Title "...in Text-to-SQL Agents" | Open decision 1 |

## PVLDB outline (12 pages)

The page estimates are rounded. Now 12.2 (the build ends on page 12); the
target is 11.9, which leaves 0.1 for slack. The added material is counted.

| § | Content | Now -> target |
|---|---|---|
| 1 Intro | The with/without problem; the fee example as data semantics; the finding; the protocol; secondary results in one paragraph; contributions as above; the practitioner bullets reworded. The three-part bundle (what bought the 72 points) becomes two parts, knowledge and delivery | 1.3 -> 1.2 |
| 2 Background | Trimmed. Retrieval positioned: we attribute content and compare two routes, we do not rank against documentation retrieval. CHESS's component split; Buneman et al. on provenance versus a consultation log | 1.0 -> 0.8 |
| 3 Instrument | Contract, tools, hollow contract. The authoring paths go to section 7, in 3 sentences | 1.2 -> 0.8 |
| 4 Design | Benchmarks. The gold reconstruction, its official-grader check and the contract timeline (09:56 / 10:34 / 11:37). The 199-task rule-matching subset: the raters, and that it was chosen after (18) failed. One merged arm-and-experiment table, in place of tables 1 and 2. Models. Registration facts as in the claims table. Scoring views defined. Clustering primary over question shapes | 2.7 -> 2.2 |
| 5 The finding | The panel gap; fact one; fact two with the held-out set (from section 6) | 0.6 -> 1.3 |
| 6 Delivery at equal knowledge | Form (provisional, with the sensitivity); route (experiment 5d); the LiveSQLBench prompt-versus-tools tie; the co-run ladder table per model | 1.6 -> 1.5 |
| 7 The knowledge, and where it comes from | The hollow ablation (from section 5); data inspection: compile and gaps; LiveSQLBench determinacy as a benchmark property; the author result in 2 sentences, the oracle in 1 | 1.7 -> 1.8 |
| 8 Operational trade-offs | Cost and scale with the prose arm and the regime; the consultation log; governance in one paragraph | 1.0 -> 0.9 |
| 9 Threats | Adds, from extended-only: selection, ceilings, provenance, ties, scoring views, model raters | 0.8 -> 1.1 |
| 10 Conclusion | | 0.3 -> 0.3 |

To the extended version only, with one sentence each in PVLDB:
- **5.3, the pre-computed layer.** It is a coverage diagnostic, so PVLDB
  makes no comparative claim about executable semantic layers.
- **6.5, the constructed procedures.** One sentence: near ceiling, so they
  test neither form nor procedure type. The subset definition moves to
  section 4.
- **The LiveSQLBench oracle.** One sentence, with no conditional claim.
- **The scale engineering.** The regime conditions stay in section 8.

## Abstract (draft, to be finished after experiment 5d)

> Context layers, curated documentation an analytics agent consults at
> query time, are credited with large accuracy gains, but a with/without
> comparison confounds what the layer documents with how it delivers it.
> We hold one fixed while varying the other, using a data contract that can
> be emptied, pasted or looked up. On DABStep, most of the contract's
> 20-point lead over the pasted manual is two pieces of data semantics the
> manual omits: an empty list that means "all", and fees that add up when
> several rules match. Stated in prose, they recover most of the lead on
> two models, also on tasks outside the inspected set. With the facts held
> equal, representation and route move accuracy by a few points either way,
> depending on model and task, and on LiveSQLBench the route ties. Looking
> definitions up keeps working past the context window and logs what was
> consulted. Knowing the semantics, not delivering them, is what the
> measured gain bought.

The draft is about 150 words. The route clause changes with experiment 5d.

## Open decisions

1. **The title.** The current title is "Which Part of the Context Layer
   Does the Work?", and the "...in Text-to-SQL Agents" part generalises
   (review B). Options:
   - keep it and scope it in the abstract;
   - "Missing Semantics or Better Delivery? Attributing Context-Layer Gains
     in a Text-to-SQL Benchmark" (gpt-6.1-sol's suggestion).

   This is the author's call.

## Order of work

1. Experiment 5d runs; the analysis and FINDINGS follow.
2. The exact-match sensitivity for the form and route effects.
3. The claims table, applied across sections 1 and 5 to 10, appendices C, I
   and J, the captions and the figure labels.
4. The restructure: section 4 (gold, timeline, subset, registration,
   merged table), the new section 5, sections 6 and 7, and the moves.
5. Abstract and intro last.
6. Both builds, the PVLDB content ending by page 12. Every number touched is
   checked against FINDINGS or the rows, and every remaining pointer to the
   extended version is checked for a missing premise.
7. A review round, then copy edits; the AI disclosure goes in then.
