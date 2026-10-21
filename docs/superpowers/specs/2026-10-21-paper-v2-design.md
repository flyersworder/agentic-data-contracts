# Paper v2: which part of the context layer does the work, across two benchmarks

## Intent

The preprint (arXiv:2609.22259, the `docs/paper/` sources at `842e6d6`)
answers its title question on one benchmark. Content dominates scaffolding.
The contract beats the manual pasted into the prompt "because the rule it
needs is one lookup away rather than buried in a long prompt".

Since then there have been eleven more experiments, with about 40 predictions
fixed in run commits:
- DABStep: knowledge or delivery, the compiler, experiments 5-5c;
- LiveSQLBench: experiments 1-4, 4b and 6.

They keep the first half of the answer and revise the second. Version 2
gives the current answer to the same question, on two benchmarks:

- **Knowledge does the work.** What matters is complete knowledge, written
  down by someone who could see the data.
- **What looked like delivery was mostly knowledge.** The DABStep manual
  lacked two facts that the contract states.
- **Where the knowledge comes from decides what it is worth.**
- **Delivery buys cost and scale, a record of what each answer consulted,
  and governance; not accuracy.**

Readers are PVLDB EA&B reviewers. This is a new submission, not a
revision: v1 is on arXiv only. The target is the 2026-12-01 cycle, with
the abstract due 2026-11-25. The paper stays a mechanism paper, not a
leaderboard paper.

## Decided with the author (2026-10-21)

- **Organised by question.** Each results section draws on both
  benchmarks. It is not one section per benchmark.
- **The correction is stated in the text.** One plain sentence says the
  preprint's "buried in a long prompt" explanation was tested and does not
  hold, and what replaced it. A revision note goes in the arXiv comments.
- **Both builds come from the shared section files**, as now: the PVLDB
  build in acmart sigconf with 12 content pages and no appendix, and the
  arXiv build with the appendix.
- **The title stays:** "Which Part of the Context Layer Does the Work?".
  The subtitle changes to name both benchmarks, for example "Separating
  Knowledge from Delivery in Text-to-SQL Agents, on Two Benchmarks". That
  wording is my call.

## Thesis, one paragraph (abstract seed)

Context layers raise text-to-SQL accuracy, but a with/without comparison
cannot say which part does the work. We use a data contract as the
instrument, and run ablations on DABStep and LiveSQLBench across seven
models. Most experiments fixed their predictions in a run commit before
running.

The prose content does the work. The tools and instruction without content
add little, and the k=3 panels set the range (step 0). On DABStep's hard
tasks the content adds 26-36 points (runs A-D).

What looked like an advantage of delivery on DABStep was mostly knowledge
the manual lacked:
- one fact (the empty-list wildcard) recovers 38-77% of the gap between
  the manual and the contract;
- a second fact (overlapping rules each apply) lifts the prose by a further
  28-43 points on the rule-matching tasks;
- once the facts are worded alike, the contract's form adds 4-6 points;
- the governed tools add retrieval of a rule just before the query that
  needs it.

What the knowledge is worth depends on its authoring:
- a compiled contract is worth what the manual is, on both benchmarks;
- on DABStep, a stronger compiler model with a format that holds
  multi-table procedures closes 90% of the gap to the hand-written contract,
  still from the documentation alone;
- on LiveSQLBench, knowledge helps far more where the documentation
  determines the gold (+21 points against +8);
- an author who can query the data recovers what the data settles, recovers
  what it only suggests two times in three, and misses conventions it
  cannot reveal.

Delivering the contract on demand rather than in the prompt costs no
accuracy up to 4x the base contract (tested), and luna's 98.3% at 22
databases (551k tokens) fits that, though it was not tested. It is cheaper
from 2x (1.7-2.4 times at 4x, 6.4 on luna at 22 databases) and runs past
the context window. The lookup log names the definition behind each
answer. Governed arms issued no mutating statements, against 166.

## Page budget (PVLDB, 12 content pages)

| section | pages |
|---|---:|
| 1 Introduction | 1.25 |
| 2 Background and related work | 0.9 |
| 3 The instrument: contracts, three ways to author one | 0.9 |
| 4 Experimental design | 1.2 |
| 5 Knowledge does the work | 1.5 |
| 6 What looked like delivery was knowledge | 1.6 |
| 7 Where the knowledge comes from | 1.9 |
| 8 What delivery buys | 1.5 |
| 9 Threats to validity | 0.75 |
| 10 Conclusion | 0.5 |
| total | 12.0 |

`make check` enforces it. Each section is drafted to its budget; a section
over by more than 0.2 pages is cut before the next one starts.

## Sections

Each item names its source, which is where every number comes from:
- `F:` is a heading in `experiments/dabstep-contract-eval/FINDINGS.md`;
- a script name means its printed output.

No number enters the paper that is not in one of them.

### 1 Introduction

Keep the opening, from the failure mode to context layers to the three
parts (`01-intro.tex` lines 1-47). It works, and v1's readers will
recognise it.

Then the new arc in four findings, as above. One paragraph on method:
- predictions fixed in run commits before each run;
- sign tests over tasks;
- k=3;
- two benchmarks;
- the models.

**Contributions:**
1. the decomposition, now on two benchmarks;
2. the revised account of delivery, with the experiments that revised it;
3. the authoring result, including when knowledge is determinable;
4. what delivery buys, measured: cost and scale, the lookup log,
   governance;
5. an artifact: the harness, contracts, predictions and results, public.

The correction sentence goes here, or in section 6, where the evidence is.
I recommend section 6, with a forward pointer from here.

### 2 Background and related work

Compress `02-background.tex` from about 1,400 words to about 700:
- DABStep, briefly;
- LiveSQLBench, new: 22 databases, a knowledge base per database, Soft-EX
  grading;
- the three kinds of layer;
- what has been measured.

Add:
- context rot and long-context retrieval, briefly, as the belief that
  section 8 tests;
- progressive disclosure: agent skills, tool search, "load on demand",
  argued for and rarely measured.

Keep the related-work closing move from paper-plan gap 4b: the artifacts
from four vendors are unpublished; ours is public.

### 3 The instrument: contracts, three ways to author one

From `03-system.tex`, compressed:
- the contract as YAML: allowed tables, domains, rules, plus a semantic
  file of metrics with SQL;
- the governed tools: lookups, `inspect_query`, `run_query`;
- the hollow contract.

New, the **three authoring paths**, which section 7 compares:
- **by hand**: the DABStep contract;
- **compiled from documentation alone**: `dce/compile`, with a Sonnet 5 or
  Opus 5.5 compiler, one-table or with declared joins;
- **authored with the data open**: `dce.author`, which reads the whole
  knowledge base, explores the data read-only and writes the contract.

Also new, the **delivery forms**, which sections 6 and 8 compare:
- prose in the prompt (`manual_*`);
- the contract's files in the prompt over plain tools (`contract_text*`);
- the contract behind the governed tools (`contract*`).

Provenance and freeze: one paragraph, details to the appendix.

### 4 Experimental design

- **Two benchmarks, and what each tests.**
  - DABStep: 401 tasks, one domain, one procedure (fee matching) that many
    tasks share.
  - LiveSQLBench: 302 primary tasks over 22 databases, knowledge that is
    mostly composite.
  - Experiments 4, 4b and 6: 60 constructed questions on three
    LiveSQLBench databases, where the knowledge is complete by
    construction.
- **The arm family, in one table.** For each arm: what it knows and how it
  is delivered. Show only the arms the main text uses. The table is the
  reader's map; every later section refers to it.
- **Models.**
  - DABStep's four-model ablation, runs A-D, k=1: glm-5.3-flash,
    deepseek-v4-flash, Claude Sonnet 5, gpt-5.6-sol.
  - The k=3 runs:
    - gpt-6-sol and qwen3.8-27b in the four-arm panels;
    - gpt-6-sol, gpt-6-luna and qwen3.8-27b in the knowledge-or-delivery
      series;
    - gpt-6-luna and qwen3.8-27b on the 302 LiveSQLBench tasks;
    - gpt-6-sol joins them on the constructed questions of experiments 4,
      4b and 6.
  - Use the model names exactly as FINDINGS gives them. gpt-5.6-sol (run C)
    and gpt-6-sol are different models.
- **Pre-registration.** From the knowledge-or-delivery series on, plans
  and predictions were committed before each run, with outcome readings
  fixed in advance. Runs A-D had a pre-registered design and primary model,
  but no numbered predictions. The paper says this plainly.
- **The prediction index** (my call). It counts every numbered prediction
  in FINDINGS, Q variants as separate entries.
  - Outcomes are graded as FINDINGS grades them: holds, holds in part,
    fails, equivalent.
  - Results labelled exploratory are not predictions and are counted
    apart.
  - The numbering repeats across experiments, so the index keys each
    prediction by experiment.
  - Build the index first, from FINDINGS, as a table in the appendix
    source; the counts in section 4 come from it.
- **Statistics.**
  - Task-level sign tests over repeats, ties dropped.
  - One-sided where predicted.
  - The direction must hold on each model.
  - Bootstrap equivalence within ±5 points where registered.
  - McNemar for the k=1 runs.
- **Grading:**
  - DABStep: reconstructed golds and the official scorer, with end-to-end
    strict alongside;
  - LiveSQLBench: official Soft-EX, reproduced exactly.

### 0 Before drafting: two analyses the paper needs

1. **The k=3 panels' four-arm decomposition** (`results/gpt6-panel-r*`,
   `results/qwen38-panel-r*`).
   - FINDINGS reports these panels only as the knowledge-or-delivery series.
     No section reports schema, hollow, manual and contract at k=3 with sign
     tests.
   - Add `analysis/panel_decomposition.py`, with tests: per model, accuracy
     on all and on hard tasks, and task-level sign tests for:
     - hollow against schema (scaffolding alone);
     - contract against hollow (content);
     - manual against schema.
   - Add its FINDINGS section.
   - Known so far: gpt-6-sol's hollow arm is below the bare schema (37.4%
     against 44.5%). Qwen 3.8's two are level, end-to-end (28.9% against
     28.8%).
   - These panels, not runs A-D, become section 5's primary decomposition.
2. **The scorer ignores sign.**
   - The vendored DABStep scorer's `extract_numeric` drops a minus sign, so
     `5.24` matches `-5.24` in both directions (verified 2026-10-21).
   - A rough count finds 114 correct verdicts across the DABStep result
     files where the answer's sign differs from the gold's. They fall mostly
     on Qwen and mostly on baseline arms: on the Qwen panel, 16 for
     schema-only against 5 for hollow.
   - Some may be right in words ("a decrease of 5.24"), so the count
     overstates.
   - Add `analysis/sign_regrade.py`, with tests. It re-grades with the sign
     enforced and lists the affected answers for reading.
   - Add a FINDINGS note.
   - Report whether any prediction's outcome or any paper number changes.
     Official scores stay the headline, as fixed.

### 5 Knowledge does the work

- **DABStep, k=3** (step 0, the gpt-6-sol and Qwen 3.8 panels): the
  primary decomposition table.
- **DABStep, runs A-D** (`F: Headline`, `F: The ablation is the result`):
  breadth across four more models at k=1.
  - The content step on hard tasks is +26 to +36 points.
  - Scaffolding alone is not distinguishable from zero on deepseek
    (p = 0.61). On glm it is p = 0.058 overall and p = 0.015 on the hard
    tasks.
  - It is large on Sonnet 5 and gpt-5.6-sol, at k=1, and the k=3 gpt-6-sol
    panel does not reproduce it.

  Say this plainly: scaffolding's sign and size vary by model, and content
  dominates on every model.
- **LiveSQLBench** (`F: LiveSQLBench: compiled knowledge`, `F: Qwen 3.8: the
  tie replicates`). Knowledge beats the bare schema on both models, 71 tasks
  to 8 and 68 to 13. That is +15.5 and +14.1 points.
- **The knowledge must determine the gold**
  (`F: Does the KB determine the gold?`, all 302 tasks).
  - Blind raters label each task determined or not; kappa is 0.79.
  - Determined tasks: 60% right; undetermined tasks: 14%.
  - The knowledge gain over the bare schema is +21 points on determined
    tasks and +8 on undetermined ones (p = 0.002).
  - Determined tasks are also easier without knowledge (schema_only 39%
    against 6%). So the low ceiling is partly difficulty and partly
    undetermined golds. The confound check (+21 against +8) separates them.
    This sets up section 7.
- **The compiled ceiling and the derivation gap** (`F: The contract
  compiles`): one paragraph, the rest to the appendix.
- **The gain stays inside the contract's domain**: one sentence, details to
  the appendix.

### 6 What looked like delivery was knowledge

This is where the preprint's mechanism is revised. Tell it as the sequence
of tests, briefly, because the sequence is the evidence.

1. **One fact** (`F: Knowledge or delivery? ... gpt-6-sol`, `F: ... Qwen
   3.8`, `F: ... gpt-6-luna`).
   - The manual says null means "all". The data stores "all" as an empty
     list.
   - Adding that fact as a note (`manual_resolved`) recovers 77%, 41% and
     38% of the manual-to-contract gap on gpt-6-sol, Qwen and luna.
2. **A second fact** (`F: The prose lacked a second fact`, `F: The second
   fact, measured`).
   - The manual never says that overlapping fee rules each apply.
   - A note stating it lifts the prose by 28.5 points (luna) and 42.7 (Qwen)
     on the 199 rule-matching tasks.
   - On luna the prose then ties the contract's text (+0.8 points).
3. **The facts stated alike** (`F: The notes were plainer than the
   contract`, `F: The facts stated alike`).
   - With the same two notes in both, the contract's text is ahead by 3.7
     points (luna) and 5.5 (Qwen) on the rule-matching tasks.
   - Caveat: the noted text states each fact twice, so part of that lead may
     be repetition.
4. **What the tools add** (`F: Text or tools`, `F: Where the tools help`).
   - On rule-matching tasks the governed arm and the contract's text tie
     (-0.7 points).
   - Elsewhere the governed arm leads by 5 points (luna), in three
     average-fee families. In all 50 winning runs the wildcard rule came
     from a governed call made before the first fee query: a metric lookup
     in 34, `lookup_domain` or the governed `describe_table` in 16.
     `inspect_query` played no part.
   - On Qwen the tools add 10.1 points on rule-matching tasks, and trail on
     the rest.
5. **Complete prose carries even a rule-matching procedure** (`F: Knowledge
   type, controlled`). Experiment 4 builds R and C pieces whose knowledge is
   stated in full, in clean and in business-manual style:
   - every knowledge arm is at 97-100% on luna and sol; Qwen was added after
     the ceiling was seen, is reported apart, and sits at 97-100% as well;
   - neither the type of knowledge nor the style of prose decides it.
   - Procedural load, as rated (`F: Is the gain where the knowledge carries
     procedure?`), explains DABStep's gain and not LiveSQLBench's null.

The correction sentence, in substance:

> The preprint attributed the contract's lead to delivery: the rule one
> lookup away, not buried in a long prompt. That explanation does not
> survive these tests.
> - Most of the lead was two facts the manual did not state.
> - With the facts worded alike, the contract's form adds a few points.
> - The tools add focus at the point of use on some task families.
> - Section 8 tests burial directly, on constructed questions with
>   dissimilar padding: no accuracy is lost up to 4x, and the untested 22
>   databases fit that. It is not a test on DABStep's manual.

Figure: a ladder on the rule-matching tasks, for luna and Qwen.
- **The arms:**
  - `manual_summed`, `contract_text` and `contract_text_noted` from
    experiment 5c, where they ran together in each repeat;
  - `manual_resolved` and `contract` from experiment 5 (luna) and 5b
    (Qwen), drawn with a separator and labelled as from another run.
- **The caption** explains Qwen's dip at `contract_text`: the contract's
  sentence "the fee rule" half-contradicts the summing convention
  (`F: The notes were plainer than the contract`).
- **Numbers** come from `analysis/contract_text.py` and
  `analysis/contract_noted.py`; the figure script asserts them.

### 7 Where the knowledge comes from

1. **Compiled from documentation alone, the contract is worth the manual**
   (`F: The compiler on DABStep`, `F: LiveSQLBench: compiled knowledge`).
   - DABStep: compiled 45.6% against the manual's 45.3%.
   - LiveSQLBench: 35.9% against 35.5% on luna, 37.7% against 37.3% on Qwen.
   - On LiveSQLBench, prompt, prompt plus SQL and governed tie three ways.
2. **On DABStep a better compile closes most of the gap**
   (`F: A stronger compiler on DABStep`). The Sonnet compile scores 47.3%,
   Opus with joins 76.6%, the hand-written contract 80.0%. It is still
   compiled from documentation alone. What closes the gap is the stronger
   compiler model together with a format that can hold a multi-table
   procedure, and DABStep's procedure is determinable from the manual and
   the data.
3. **On LiveSQLBench it does not** (`F: A better-made contract on
   LiveSQLBench`, `F: Knowledge reuse`, `F: Completing the knowledge`).
   - Opus compile and Opus author tie with the Sonnet compile.
   - Corrections learned from training golds lift the training tasks by
     about 21 points and the held-out tasks by none. LiveSQLBench settles
     what its knowledge base leaves open task by task, in the gold, so a
     contract has nothing consistent to hold there.
4. **An author with the data recovers what the data settles** (`F: Gaps the
   documentation leaves open`).
   - Recovery: 12 of 15 data gap-draws were recovered; settled 6 of 6,
     split 6 of 9, convention-only 0 of 3.
   - The no-data control recovered none of five.
   - The contract's gain over the gapped KB, 26.8 points, is the author's
     knowledge: the share is 1.03.
   - The author's misses travel with the contract.
5. **The account across both benchmarks, as the tests left it.** Three
   candidate conditions were tested:
   - reuse across tasks (`F: Knowledge reuse`) does not separate the
     benchmarks;
   - the type or procedural load of the knowledge (`F: Is the gain where
     the knowledge carries procedure?`, `F: Knowledge type, controlled`)
     does not either: complete prose carries it;
   - what survives is completeness and determinability: a contract pays
     where it states something true that the prose leaves out, and an
     author can only state what the documentation or the data determines
     (`F: Completing the knowledge`, `F: Gaps the documentation leaves
     open`).

   State the earlier conditions as tested and dropped.

### 8 What delivery buys

1. **Cost and scale** (`F: Disclosure at scale`, experiment 6).
   - Accuracy:
     - (37) is equivalent: the drop interval is [+0.0, +1.3] points;
     - luna with 551k tokens in the prompt still scores 98.3%.
   - Cost:
     - the crossover is at 2x;
     - the governed arm is 1.7 times cheaper at 4x on luna and 2.4 on sol,
       and 6.4 times cheaper at "all" on luna, 22 databases;
     - the same holds under a fixed price, cached or uncached.
   - Qwen's window: the governed arm scores 100% where the text arm does not
     fit.
   - Mechanism: no lookup names padding.
   - Experiment 4b's small-scale observation, that disclosure saves input
     but not money for one database, is the left end of this curve.
   - Scope the context-rot claim: the padding was namespaced, dissimilar
     databases; neighbouring domains would be the harder test.
   - Figure: cost per run against contract size, both arms, luna and sol,
     with a mark where the text arm stops fitting Qwen.
2. **Accountability** (`F: What the lookup log tells an auditor`;
   exploratory).
   - The final SQL contains a looked-up metric verbatim in 87-98% of runs.
   - A departure does not flag a wrong answer.
   - The log does locate an error's source: in 4b, 90 of 175 wrong answers
     applied a wrong definition verbatim.
   - So trust here means accountability, not detection.
3. **Governance** (`07-governance.tex`, `F: Governance`). Ungoverned arms
   submitted 166 mutating statements; governed arms none. That is
   deterrence by declared rule, not interception. Compress to about half a
   page.
4. **Efficiency at small scale**: one paragraph. Cost per correct answer,
   from `06-why.tex`'s efficiency subsection and 4b's efficiency table.

### 9 Threats to validity

One paragraph each, short:
- **Benchmarks.** Two benchmarks, but the rule-matching procedure that
  carries DABStep's form gain is one procedure.
- **Raters.** The blind raters and the procedural-load raters are model
  instances of one family. Their agreement overstates human reliability.
- **Ceilings.** Experiments 4 and 6 sit at the ceiling. Experiment 6 can
  detect a drop but not a gain.
- **5c repetition.** The noted text states each fact twice.
- **Qwen's scoring views.** Official and end-to-end verdicts disagree on
  Qwen, so we report both. The order of the arms changes under the official
  view.
- **Graders.**
  - The vendored DABStep scorer ignores sign. Report the step 0 re-grade's
    effect and keep the official verdicts.
  - LiveSQLBench's official DISTINCT stripping leans against the contract
    arms; re-grading moves no outcome.
- **Reconstructed golds are lenient** (keep).
- **Contamination** (keep).
- **k=1 for runs A-D** (keep, shortened). The k=3 series answer it.
- **Run health.** Retries, the deepseek truncation, and traces lost to a pod
  restart in experiment 6.

### 10 Conclusion

Practitioner guidance:
- Write down the knowledge, completely, with the data open.
- State conventions explicitly.
- For a single database's contract, the prompt is fine and, with caching,
  as cheap or cheaper. From about twice that size, looking things up as
  needed is cheaper, and without caching it is cheaper even at one
  database. State the threshold in contract sizes as measured (1x, 2x,
  4x), not in tokens, unless `fit.json`'s sizes are cited.
- Use the lookup log to trace an error to its source.
- Pre-compute only where agents demonstrably fail to derive.

Future work:
- a second independent rule-matching procedure;
- padding with neighbouring domains;
- human raters.

Artifact availability: keep, and update the commits and tags.

## Appendix (arXiv build only)

- **Keep** the current appendices A-H, trimmed where the main text now
  covers them.
- **New:**
  - **I, the prediction index:** every prediction, with its experiment,
    run commit, outcome and the reading fixed in advance;
  - **J, LiveSQLBench details:** the compile audit, the root-cause groups,
    the failure audit, grading defects and corrections;
  - **K, the constructed procedures and gaps:** the design of experiments 4
    and 4b, the recovery scoring and the R06 note;
  - **L, the scale materials and mechanism counts.**
- **Material moved out of the main text:**
  - the details of the k=1 ablation;
  - the bucket analysis;
  - the counterfactual macros;
  - the transcript case studies.

## Constraints

- **Sources.** Every number comes from FINDINGS.md or an analysis script's
  output. Numbers already in the paper and in FINDINGS are checked for
  agreement; where they differ, FINDINGS is the source and the difference
  is noted.
- **No private material**: no LiveSQLBench golds, test cases, task-to-KB
  mapping content or ratings packets. Aggregates only.
- **No internal names** in the paper, captions or comments; see the
  scrub-internal-names memory.
- **No AI attribution** anywhere.
- **Style.** Plain declarative sentences, as in the current paper. Spell
  numbers in the paper's existing style, with the macros in
  `preamble.tex`.
- **Builds.** Both builds compile with tectonic. `make check` passes:
  12 content pages, no overfull boxes, no "??".
- **Figures** are generated by `figures/make_figures.py` from result rows,
  asserting the paper's numbers.

## Left to the writer

- Exact wording, the order of paragraphs within a section, and table
  layout.
- Which secondary numbers to cite.
- The new subtitle's exact wording.
- Whether section 2's context-rot paragraph cites specific papers. Cite
  only what can be verified; no invented references.

## Process

0. Step 0's two analyses, test-first, with their FINDINGS sections.
1. Draft sections 5-8 first: the evidence.
2. Then 4, then 1-3.
3. Then 9-10 and the abstract.
4. Then the appendix.

Build after each section. When the draft is complete, one fresh reviewer
checks every number against its source, and one reads it as a PVLDB
reviewer would. Commit section by section on `compiler-from-sql`.
