# Paper v3: missing data semantics, not delivery

**Status.** This is revision 3. The earlier rounds:
- **Round 1** reviewed the PVLDB version. Two reviews, both major revision.
- **Round 2** reviewed revision 1 of this spec. Three reviews: "needs
  rework", "ready with changes" and "ready with changes". All three found
  that the thesis and the abstract re-introduced the over-claims the claims
  table removed.
- **Round 3** was a calibration review of revision 2. gpt-6.1-sol and
  gemini-4-argon ran through Copilot CLI and a fresh Opus 5.5 reader ran on
  its own, each with the artifact. All three said "nearly calibrated":
  - the abstract and thesis still dismissed delivery ahead of experiment 5d;
  - the framing of 5d was wrong;
  - the hollow ablation and Qwen's two-fact result were under-claimed;
  - two statements were factually wrong.

  Revision 3 makes those changes. Numbers that only one reviewer reported
  were re-checked against the rows before use: sol's subset shares, Qwen's
  prompt fit and the range of rater pairings.

The reviews are kept outside the repository, in
`~/.cache/review-sandbox/review{5,6}/`.

## Calibration rules

These apply to every sentence of both builds.
1. **Scope.** Every number carries its scope: models, task subset,
   scoring view, and which arms ran together.
2. **Prefer "no detectable difference" to "tie".** A registered tie is a
   point estimate within 5 points plus non-rejection, not equivalence. The
   one exception is a registered equivalence result: the scale experiment's
   (37) met its 5-point margin.
3. **Delivery claims stay conditional.** No sentence dismisses delivery or
   bounds it. The plan and the abstract keep the delivery clause open until
   experiment 5d is analysed, and then state what it shows.
4. **Strong results are stated plainly.** The hollow ablation, the held-out
   lift, Qwen's two-fact result, the cross-family robustness and the
   governance counts survive their checks, and are not hedged as if they
   had not.
5. **Each Qwen claim names its view.** The views are official, or
   official plus final-paragraph recovery; the arms reorder between them.

## The thesis

> On DABStep, most of the contract's apparent advantage over the pasted
> documentation is two pieces of data semantics the documentation omits:
> a wildcard stored as an empty list, and how the fees of several matching
> rules aggregate. Removing the contract's knowledge while keeping its tools
> costs 26 to 50 hard-task points on six models. Stating the two semantics
> in prose recovers most of the contract's lead on GPT-6 luna, and all of it
> on Qwen 3.8 with final-paragraph recovery (on the official view the
> contract keeps a 6-point lead on the rule-matching tasks). A
> six-model control and a held-out check support it. With the facts held
> equal, the contract's representation still leads on the rule-matching
> tasks. Once both facts are pasted into both arms (experiment 5d), the
> lookup route shows no advantage end-to-end on luna or Qwen; on Qwen's
> official view it leads by 5 points. The notes lift the governed contract
> on both models. On LiveSQLBench the route makes no detectable
> difference.

## The storyline

1. **The problem.** Context-layer gains are reported with/without. That
   comparison confounds what a layer documents with how it delivers it.
2. **The protocol.** The protocol needs knowledge that can be emptied,
   given extra facts in identical words, and delivered by two routes. A
   data contract is a convenient instrument for that, because it keeps
   explicit semantics separate from the tools that enforce and serve them.
   It is not the only possible instrument. Which parts ran on which test
   bed:
   - **DABStep:** all three parts.
   - **The constructed questions:** notes and routes (experiment 4b).
   - **LiveSQLBench:** routes only.
3. **The control.** Emptying the contract's descriptions and SQL while
   keeping its tools, instruction and rules costs 26 to 50 points on
   DABStep's hard tasks.
   - **Scope:** six models; the knowledge step exceeds the scaffolding step
     on every one. On the repeated panels it survives clustering over
     question shapes: sol +50.0 [+32.1, +66.2].
   - **Where it sits:** nearly all of it is in the fee procedure. On sol it
     is +57.6 on the fee tasks and -8.8 on the 38 others.
   - **The empty scaffolding:** -9 to +15, and at most +0.2 on the two
     repeated panels.
   - **What it measures:** knowledge given the scaffolding, not "all
     knowledge versus none". Names, types and rules remain.
4. **The finding.** The contract leads the pasted manual on the hard tasks:
   by 19.9 on sol and 22.7 (official) or 23.2 on Qwen, on the two repeated
   panels, and by 42.9 on luna. This looks like a delivery win. It is mostly
   two omitted data semantics:
   - **a sentinel encoding.** The manual says null means "applies to all";
     the data stores that as an empty list in 720 of 1,000 rules for
     `account_type`.
   - **join multiplicity and aggregation.** Several fee rules match one
     payment, and their fees sum. The manual defines one rule's fee and is
     silent on overlap.

   Stated in prose:
   - **On luna** they recover most of the lead.
   - **On Qwen** the prose with both facts *outscores* the governed
     contract with final-paragraph recovery: 86.1 against 79.6 on the
     rule-matching tasks, and 80.0 against 67.7 over all 401. On the
     official view the contract keeps the lead on the rule-matching tasks,
     51.6 against 45.6, and trails over all 401, 51.0 against 54.4. These
     are co-run, in experiment 5.
   - **Plain beats unplain.** The prose with both facts beats the contract's
     own text by 16.6 points on Qwen ((29Q)), robust to clustering and to
     every rater family. Experiment 5c then lifts the contract's text by
     18.9 when the notes are added. How plainly the semantics are stated
     matters, not only whether they are present.
   - **The held-out lift.** The second fact, `SUM_NOTE`, was written after
     reading 34 tasks' transcripts. On the 165 rule-matching tasks outside
     that set it lifts the one-fact prose by +19.2 points on luna and +33.9
     on Qwen, with shape intervals [+8.5, +31.9] and [+17.7, +53.6]. It
     holds under every rater family. Excluding the 34 removes tasks the
     comparator failed twice, so the selection biases the lift down; the
     third repeat alone gives +20.6. Exploratory and cross-run, the
     recovery of the whole lead on those 165 is about 104% on luna and
     126% (end-to-end) or 76% (official) on Qwen.
   - **Sol.** Only the first fact was tested. It recovers 77% of the gap
     over all 401 tasks (shape interval 0.16 to 1.01), but 112% on the
     tasks outside rule matching and **8% on the 199 rule-matching tasks**.
     Sol's rule-matching gap, where the second fact acts, is untested.
5. **Delivery with the facts held equal.** This was measured on luna and
   Qwen on DABStep. Sol's knowledge-equal evidence is on the constructed
   questions and the scale experiment.
   - **Form** (the contract's text against prose, both with the notes):
     - **Rule-matching tasks:** the contract's text leads, +3.7 on luna
       ([+0.6, +7.9] over shapes, robust to every rater family) and +5.5
       on Qwen ([-0.4, +10.4]; it excludes zero over families only). This
       is the direction the run fixed in advance.
     - **Why provisional:**
       - it is a package of format, SQL definitions and repetition;
       - neither model's effect clears shape clustering on the 165 held-out
         tasks;
       - on luna it rests largely on the 34 inspected tasks;
       - it is close to the 2.8-point drift between identical arms;
       - under exact-match scoring luna's +3.7 falls to about +1.8
         (p about 0.08). The sensitivity analysis reports this.
     - **The other 202 tasks:** +9.6 on luna (51 tasks to 14); -5.3 on
       Qwen, not significant.
   - **Route** (governed delivery against the same text). Experiment 5's
     contrast is knowledge-equal by construction: the same contract, both
     arms with its original wording.
     - **Experiment 5:** -0.7 on luna and +10.1 on Qwen on the
       rule-matching tasks. Qwen's is registered prediction (27); its shape
       interval excludes zero, its shape sign test does not, and it crosses
       zero on Gemini's subset. On the rest the route gives +5.1 on luna
       and -6.9 on Qwen, neither surviving clustering.
     - **Experiment 5d** (co-run, k=3, results `ddddb75`) puts the two notes
       in both prompts. It tests governed delivery of the rest of the
       contract, with the deciding facts pasted in both arms. It does not
       test whether the facts themselves reach the agent better by lookup.
       On the rule-matching tasks, end-to-end:
       - **luna:** governed 86.3 against text 88.1, -1.8 (16 tasks to 23,
         p = 0.34). No detectable difference; (41) holds.
       - **Qwen:** 86.8 against 88.9, -2.2 (10 to 21, p = 0.071). This
         fails the registered tie test (p >= 0.10), with the text ahead.
         Its shape interval is [-6.9, +1.9] (p = 0.39); over fee families
         it excludes zero.
       - **Qwen, official view:** the governed arm leads, 57.8 against
         52.4, 75 tasks to 49, p = 0.024.
       - **Over all 401, end-to-end:** -2.3 on luna, +1.5 on Qwen (58 to
         53, p = 0.7).
       - **The notes lift the governed contract:** +4.0 on luna (22 to 6,
         p = 0.004; (42) predicted no difference and failed) and +7.0 on
         Qwen ((42Q) holds), +15.0 on Qwen over all 401.

       Experiment 5's +10.1 for the route on Qwen, under the original
       wording, does not survive the plain wording end-to-end. The route's
       effect on Qwen depends on the wording and on the scoring view.
   - **Other route contrasts point the same way:**
     - **4b's constructed questions:** authored contract by two routes,
       +0.5 (2 tasks to 0); `contract_text` against `manual_noted` -1.4;
       knowledge share 1.03 [0.98, 1.35]. Five gaps, so the clusters are
       few.
     - **LiveSQLBench:** compiled knowledge in the prompt or behind the
       tools, 36.8 against 35.9 on luna (22 tasks to 25) and 37.7 against
       37.7 on Qwen (22 to 24). There is no detectable difference;
       prediction (3) had the tools ahead and failed. Both arms sit at a
       knowledge-limited level of about 37%.
   - **Summary:** form estimates are positive on the rule-matching tasks of
     both models. Route estimates with the facts pasted alike are near zero
     end-to-end and change sign on Qwen's official view. The other
     contrasts are unresolved and vary by model, task family and view. No
     range is offered as a bound.
6. **Where the knowledge comes from (secondary).**
   - **Data inspection.** It grounds instance-verifiable encodings. The
     stronger compile found the empty-list wildcard. Its gain over the
     first compile, 47.3 to 76.6 against the original's 80.0, is the Opus
     compiler together with join-capable SQL; without joins it scored
     47.4. In the gaps experiment the sessions with data recovered:
     - 6 of 6 draws on the two settled gaps;
     - 6 of 9 draws on the three split gaps (3 of 9 under the alternative
       ruling; 12 of 15 or 9 of 15 in all);
     - 0 of 3 draws on the one convention gap. Every session chose the
       lowest rule id where the gold uses the most specific.

     The no-data control recovered none of five. What the author recovered,
     and what they got wrong, travelled through prose notes and through the
     contract alike.
   - **Provenance.** The independent compile emitted one row per matching
     pair, but left the aggregation open. The original contract's summing
     matches the gold's default join-and-sum, and is not credited to data
     inspection.
   - **LiveSQLBench determinacy.** Two gold-aware model raters (kappa
     0.79) find that the KB leaves a result-changing choice open on 132 of
     302 primary tasks; the count ranges from 112 to 146 across the four
     rater pairings. No human checked them.
     - **Accuracy split:** knowledge arms solve about 60% of the tasks the
       KB settles and 14% of those it leaves open, under every rater
       family. Schema-only accuracy differs as well (38% against 4%), so
       this locates where accuracy is, not a differential effect of
       knowledge.
     - **Knowledge still helps there:** +15.6 on luna and +14.1 on Qwen,
       with database intervals excluding zero.
   - **Better authors.** On 55 tasks of three databases, on luna, there was
     no detectable gain: the Opus compile +1.8 [-3.0, +7.3], the whole
     author -1.8 [-9.1, +5.5].
   - **The oracle.** Task-specific corrections lift the training tasks by
     about 21 points and do not transfer to the held-out tasks; the
     intervals exclude a transfer above about 10 points. The rows are
     private.
   - **The constructed procedures.** Complete prose suffices there: every
     knowledge arm scores 95.6 to 100%. That ceiling means they cannot rank
     representations or procedure types.
7. **Operational trade-offs.** These come from 60 constructed questions that
   name their metric, near ceiling, padded with other databases:
   - **Accuracy:** pasting the contract's YAML at 4x (about 92k tokens) met
     the registered 5-point equivalence margin, losing 3 of 540 runs.
   - **Cost at 4x against the YAML pasted in:** lookup is 1.7 times cheaper
     on luna and 2.4 on sol.
   - **Cost at 4x against the prose KB pasted in:** about 1.1 times
     cheaper. On Qwen's output-metered billing the YAML prompt is cheaper
     than lookup and the prose prompt dearer.
   - **At 1x:** the prompt is cheaper on the GPT models.
   - **Beyond the window:** lookup keeps working where *pasting the YAML*
     does not fit. Qwen's YAML stops fitting from 8x; the prose KB fits
     even at all 22 databases (about 99k tokens), but its accuracy there
     was not run.
   - **The consultation log:** a log of consulted definitions and a
     syntactic check that the consulted expression appears in the
     submitted SQL. It is not provenance.
   - **Governance**, from the DABStep runs, not the 60 questions: no
     persistent mutation in any of 29,356 runs whose prompt stated the
     rules, against 50 of 21,790 that did not. On Qwen, 0 of 3,609 text-arm
     runs that stated the rules without enforcement, against 44 of 8,421,
     p = 2e-7. This is an association, confounded with knowledge and
     workflow; the validator never blocked a write.

**Takeaway for practitioners.**
1. Before crediting a context layer's machinery, check that the comparison
   holds the semantics equal.
2. Write down the semantics a reader could misread: wildcard encodings,
   what happens when several rules match, units, `NULL`s. Check them
   against the data.
3. Where accuracy is comparable on your models and tasks, choose the
   delivery for cost, capacity and the consultation log. Lookup's capacity
   advantage is over pasting a verbose YAML; a prose KB is several times
   smaller.

## Contributions

1. **The finding.** On DABStep, an apparent delivery advantage of a context
   layer is mostly two omitted data semantics. It is supported by a
   six-model knowledge control (26 to 50 points) and a held-out lift, on
   luna and Qwen. A DABStep comparison of context layers should check
   whether each layer states these two semantics.
2. **A knowledge-equal protocol.** It combines the hollow control, facts
   as identical notes, and the same knowledge by two routes, read as
   conditional contrasts and not an additive decomposition. On DABStep all
   three parts were applied; on the constructed questions, notes and
   routes; on LiveSQLBench, routes.
3. **Benchmark properties.**
   - DABStep's manual omits both semantics.
   - Its scorer ignores sign.
   - By gold-aware model raters, LiveSQLBench's KB leaves 132 of 302
     primary tasks open (112 to 146 across rater pairings).
   - LiveSQLBench's grader strips `DISTINCT`. Each arm moves by under 2
     points and no outcome changes; it is documented as a property.
4. **The artifact:**
   - the harness, the contracts and the result rows;
   - 53 earlier predictions plus 4 for experiment 5d, each fixed in a
     commit before its run, with one timing exception. 20 of the 53
     failed, and 2 of the 4 ((41Q) and (42));
   - the transcripts on Zenodo.

   Its limits: the LiveSQLBench transcripts are not released, and 208 rows
   of the constructed-question runs (`exp4-sol-r3` and three `exp6` runs)
   have no transcript.

The gold reconstruction's validation is method, in section 4, not a
benchmark finding. Its leniency is 3.0 to 5.7 points on GLM's two submitted
arms on the hard split. The official grader confirms the contract-versus-
manual contrast for GLM: +33.1 points, 137 tasks to 12. Authoring, scale
and cost, and governance are secondary analyses.

## Not claimed

The paper does not claim:
- that contracts beat documentation retrieval: no retrieval arm was run;
- that DABStep's two-fact attribution and its delivery effects generalise
  to other procedures. The LiveSQLBench knowledge gains and the
  constructed-question results are separate evidence;
- that the scale result tests context rot;
- that any delivery effect is bounded, or that delivery never affects
  accuracy: (27) held, and in 5d the route reverses between Qwen's two
  scoring views;
- that what a layer can add is bounded by what the documentation and data
  determine;
- that the summing convention is recoverable from the documentation or the
  data;
- that the two-fact and knowledge-equal results hold on sol or on the
  single-run models;
- that any reported tie is an equivalence, except (37);
- that the model-rated labels (rule-matching, determinacy, load) match human
  judgement;
- that the absolute accuracies compare with the official leaderboard: the
  golds are reconstructed;
- that stated rules cause the absence of mutations, or that enforcement was
  tested;
- that the consultation log certifies provenance or correctness;
- that another system's reported gain, such as the 72 points, is
  attributable to these two semantics;
- that retrieval is needed whenever a contract does not fit: a prose KB of
  the same knowledge can;
- that the hollow step shows tools are worthless: empty tools can mislead.

## Experiment 5d (done 2026-10-24; results `ddddb75`, read 2026-11-01)

Results are in storyline item 5 and in FINDINGS, *Tools with the facts
stated alike*. (41) and (42Q) hold; (41Q) and (42) fail. The design, as
fixed before the run:

- **What it adds.** `contract_noted` is the governed contract plus the two
  notes, in identical words, after its prompt. It runs together with
  `contract` and `contract_text_noted` on luna and Qwen, k=3, at about $15.
- **Registered predictions** (41), (41Q), (42) and (42Q). They are fixed in
  FINDINGS and in `analysis/governed_noted.py`, at run commit 9cb3643 (tag
  `exp5d-run`).
- **What it measures.** Whether governed delivery of the rest of the
  contract adds to, or subtracts from, the same contract pasted as text,
  once the deciding facts are stated plainly in both prompts.
- **How the result reads:**
  - a "tie" is non-rejection;
  - failing with the governed arm ahead is a route effect under clarified
    wording;
  - neither outcome by itself proves that the earlier +10.1 was "the
    wording".
- **Analysis to add before reading the rows:** shape-clustered intervals
  beside the sign tests.

## Decisions

1. **No sol co-run.** Completing the 2x2 on gpt-6-sol, about $170-210, is
   not run, for budget and scope. Revision 1's reasons were wrong, as round
   2 said.
   - Every claim about DABStep's two-fact attribution and its form and route
     contrasts names "luna and Qwen", up to the abstract.
   - Sol's evidence stays in its own rows: the panel, the hollow ablation,
     the one-fact result with its subset split, and the constructed and
     scale experiments.
   - Table 5 (the ladder) shows co-run blocks per model; the route
     comparisons get their own small table.
2. **The preprint.** The paper stands on its own:
   - no correction note, and no "an earlier version";
   - the extended version is the new arXiv version built from `main.tex`;
   - the PVLDB argument does not need it;
   - appendix J is reworded as the single-run models' analysis.

   The arXiv comments field of the new version states that it supersedes
   v1's claims. That is metadata, not paper text.
3. **Small effects.** The form and route effects are reported with an
   exact-match sensitivity. For Qwen it is reported separately for the
   full message and for the recovered final paragraph, so that leniency is
   not mixed with final-paragraph recovery. Appendix C's transfer estimator
   is no longer a bound on later experiments.

## Claims, before and after

| Now | After |
|---|---|
| "determinability as the bound on what any layer or author can add" (contribution 3, section 7's bold claims, "has nothing to hold there", "no author can recover the gold's choice", the conclusion) | Removed. A rated benchmark property with its range, the 60/14 split with its confound, and "no detectable gain from the authors we tried on 55 tasks" |
| "the tools alone are worth -9 to +15" | "the empty scaffolding is worth -9 to +15 (at most +0.2 on the repeated panels)" |
| "The contract beats the manual by 20 to 23" | "by 19.9 and 22.7/23.2 hard-task points on the two repeated panels, and 42.9 on luna" |
| "recover most of the gap" | "most on luna; on Qwen all of it with final-paragraph recovery, while on the official view the contract keeps 6 points on the rule-matching tasks; on sol only one fact, 77% over all tasks and 8% on the rule-matching ones" |
| "165 tasks nobody inspected" | "the second fact's lift on the 165 rule-matching tasks outside the transcript-inspection set" |
| Representation "about 4 to 6 points" | "the contract's text leads on the rule-matching tasks, +3.7 and +5.5; a package; provisional" with the caveats of storyline item 5 |
| Lookups "help on some fee families"; Qwen +10.1 | "a registered, knowledge-equal route effect under the original wording, which does not survive the plain wording end-to-end (5d: -2.2), and reverses on the official view (+5.4)" |
| "a few points", "0 to 10", "an order smaller", "small", "of either sign" | "form positive on the rule-matching tasks; the other contrasts unresolved and model- and task-dependent" |
| "ties" (LiveSQLBench, (29), 5d) | "no detectable difference" with the counts; "equivalent" only for (37) |
| "with 53 predictions committed before the results they predict" | "53 earlier predictions and 4 for experiment 5d were fixed in commits before their runs; one, (6), followed two repeats' totals; the repeated panels fixed none" |
| 29 held | "23 directional, 6 non-rejected ties, 1 equivalence, 3 partial, 20 failed"; (32) also fails under the 9-of-15 ruling |
| "end-to-end" view | "official plus final-paragraph recovery", defined where it is first used |
| "Do not expect a format, a retrieval loop or a stronger model to supply..." | "Delivery does not establish which undocumented convention is intended" |
| "past about twice that, look definitions up" | The sampled levels, the pricing, and the YAML-versus-prose comparators |
| "and runs where the prompt no longer fits" | "where pasting the YAML no longer fits; the prose KB of the same knowledge still fits" |
| "Runs that state the rules do not write" | "No observed persistent mutation where the rules were stated", with the counts; confounded |
| "records which knowledge each answer used", "traces a wrong answer to the entry behind it" | "a consultation log and a syntactic inclusion check" (sections 1, 8 and 10) |
| "a convention only when the gold uses the default idiom" | Removed; see storyline item 6 |
| "a contract four times a database's size costs the prompt no accuracy" | "pasting the contract's YAML at 4x, about 92k tokens, met the registered 5-point equivalence margin (3 of 540 runs lost)" |
| "90% of the way from the first compile" | "(Opus - Sonnet) / (original - Sonnet) = 90%, in the same luna run: 47.3 to 76.6 against 80.0; compiler and joins together" |
| "The natural reading, and the one an earlier version gave" | State the delivery hypothesis and test it |
| Provenance threat | As in storyline item 6 |
| "A compiler that checks its definitions against the data adds what the data settles"; "large and certain" | Scoped to one compiler on DABStep; "large, on all six models, and robust to clustering" |
| "selection does not inflate it overall"; the held-out set "understates" it | The exclusion rule and its mechanism (it removes tasks the comparator failed twice), plus the third-repeat check (+20.6) |
| "6 of 6 settled gaps, 6 of 9 split" | "draws", with the gap counts (two settled, three split, one convention) |
| "the reconstructed golds run 3.0-5.7 points lenient" as a benchmark finding | Method validation in section 4, scoped to GLM's two submitted arms; the +33.1 official contrast beside it |

## PVLDB outline (12 pages)

Pages are rounded. The build now ends on page 12 (12.2 estimated); the
target is 11.9.

| § | Content | Now -> target |
|---|---|---|
| 1 Intro | The with/without problem; the fee example as data semantics; the control and the finding; the protocol; one paragraph of secondary results; the contributions; the practitioner bullets | 1.3 -> 1.2 |
| 2 Background | Trimmed. Retrieval positioned: we attribute content and compare routes, we do not rank against documentation retrieval. CHESS's component split. Buneman et al. on provenance versus a consultation log | 1.0 -> 0.8 |
| 3 Instrument | Contract, tools, hollow contract. The authoring paths move to section 7, in 3 sentences | 1.2 -> 0.8 |
| 4 Design | Benchmarks. The gold reconstruction with its official-grader check (+33.1 for GLM) and leniency (3.0 to 5.7). The contract timeline (09:56 / 10:34 / 11:37). The 199-task rule-matching subset: its raters, and that it was chosen after (18) failed. One merged arm-and-experiment table. Models. Registration. Scoring views defined. Clustering over question shapes is primary | 2.7 -> 2.2 |
| 5 Knowledge and the two semantics | The hollow control; the panel gaps; fact one, with sol's subset split; fact two; (29Q); the held-out lift | 0.6 -> 1.4 |
| 6 Delivery at equal knowledge | Form with its caveats and sensitivity; route (experiment 5, experiment 5d, 4b, LiveSQLBench); the co-run ladder per model | 1.6 -> 1.5 |
| 7 Where the knowledge comes from | Data inspection (compile, gaps); provenance; LiveSQLBench determinacy with its range and split; better authors; oracle; constructed-procedure sufficiency in 1 sentence | 1.7 -> 1.7 |
| 8 Operational trade-offs | Cost and capacity against both comparators, the regime, the consultation log, the governance counts | 1.0 -> 0.9 |
| 9 Threats | Adds, from extended-only: selection, ceilings, provenance, ties, scoring views, model raters | 0.8 -> 1.1 |
| 10 Conclusion | | 0.3 -> 0.3 |

To the extended version only, with one sentence each in PVLDB:
- **5.3, the pre-computed layer.** A coverage diagnostic; PVLDB makes no
  comparative claim about executable semantic layers.
- **The details of 6.5.** The subset definition moves to section 4, and
  the sufficiency sentence to section 7.
- **The oracle details.** Section 7 keeps the result and its interval.
- **The scale engineering.** Section 8 keeps the regime conditions.

## Abstract (draft; the 5d clause filled 2026-11-01)

> Context-layer evaluations compare an analytics agent with and without
> curated documentation, which confounds what the layer documents with how
> it delivers it. We vary one with the other held fixed, using a data
> contract that can be emptied, pasted into the prompt or looked up. On
> DABStep, emptying the contract while keeping its tools costs 26 to 50
> hard-task points on six models. Its lead over the pasted manual, 20 to 43
> points, is mostly two data semantics the manual omits: an empty list that
> means "all", and fees that add up when several rules match. Stated in
> prose, they recover most of the lead on GPT-6 luna and all of it on Qwen
> 3.8; the second lifts accuracy by 19 and 34 points on tasks outside the
> inspected set. With the facts equal, the contract's representation still
> leads on rule-matching tasks (+3.7 and +5.5, provisional). Looking the
> contract up shows no advantage end-to-end once both arms state the two
> facts, though on Qwen's official scoring it leads; on LiveSQLBench the
> route makes no detectable difference.
> Lookup also works where pasting the contract would not fit, and logs what
> was consulted.

About 160 words. "All of it on Qwen" is with final-paragraph recovery;
section 5 gives the official view, where the contract keeps 6 points on
the rule-matching tasks. If that reads as too strong for an abstract, use
"most of it on both".

## Decided: the title (2026-10-23)

The title is narrowed slightly: "Which Part of the Context Layer Does the
Work? Separating Data Semantics from Delivery on Two Text-to-SQL
Benchmarks". The question stays, since it is also the preprint's title.
The subtitle names the paper's framing (data semantics) and its scope (two
benchmarks, not text-to-SQL agents in general). Both builds use it. The
`extended` entry in refs.bib keeps the v1 title until the new arXiv version
is posted.

## Order of work

1. Add the shape-clustered intervals to `analysis/governed_noted.py`
   before reading the 5d rows.
2. ~~Experiment 5d completes; copy down, verify, commit; analysis and
   FINDINGS. Fill the abstract's and thesis's 5d clause from what it
   shows.~~ Done 2026-11-01.
3. The exact-match sensitivity for form and route, both Qwen variants.
4. The claims table applied across sections 1 and 5 to 10, appendices C,
   I, J and K, the captions and the figure labels.
5. The restructure:
   - section 4: gold, timeline, subset, registration, the merged table;
   - section 5, new;
   - sections 6 and 7;
   - the moves.
6. Abstract and intro last.
7. Both builds, the PVLDB content ending by page 12.
   - Check every number touched against FINDINGS or the rows.
   - Check every remaining pointer to the extended version for a missing
     premise.
8. A review round on the paper, then copy edits; the AI disclosure goes in
   then.
