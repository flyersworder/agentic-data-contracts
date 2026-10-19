# Experiment 4b: does an author recover what the documentation leaves open?

Status: draft for review, revised after two review rounds (2026-10-19).
Builds on experiment 4 (`prep/procedures/`, run commit `05015db`) and
experiments 5-5c (`f499053`, `c39026f`, `a960495`).

## Why

The account the paper will make, from experiments 1-5c:

1. DABStep's contract gain is mostly knowledge the manual leaves open: two
   decisions (an empty list is the wildcard; the fees of every matching
   rule add up). Stated plainly in prose, they recover most of the gain
   (luna 54% to 82-84%, Qwen 43% to 84-86% on the rule-matching tasks).
2. With the facts stated alike, the contract's form adds a modest gain
   (+3.7 points luna, +5.5 Qwen; experiment 5c).
3. Where the documentation is complete, prose reaches the ceiling and a
   contract adds nothing (experiment 4, three models). Where it does not
   determine the gold and the gold is settled task by task (LiveSQLBench),
   a contract has nothing to hold (experiments 2, 3; the blind clarity
   test).

The positive case rests on one benchmark and one procedure, and a critic
can say that filling a gap we built is bound to help. 4b answers the
questions that construction does not settle:

- **Q1, recovery.** Given documentation with a decision left open and the
  database, does an author (`dce.author`, no questions shown) settle that
  decision the way the reference does, and is data access what lets it?
  Answered offline, record by record against the reference, before any
  agent runs.
- **Q2, does the author's knowledge carry the gain?** Over the gapped
  documentation, does the authored contract pay, and how much of that gain
  do the author's resolutions recover when written as prose? Measured as a
  registered share.
- **Q3, does an agent need the author at all?** Every arm can query the
  data. If an agent recovers data-settled decisions at query time, the
  contract adds nothing on them. This is the outcome that most threatens
  the account, and it gets a registered reading.

DABStep's two facts are of two kinds, and 4b keeps them apart:

- **Data-settled.** The documentation says one thing and the data shows
  which reading is right (DABStep: the manual says null means "all"; the
  fee table never uses null for list fields and stores "all" as an empty
  list).
- **Convention-only.** The documentation is silent and the data cannot
  settle it (DABStep: how overlapping rules combine). Any author has to
  choose. On DABStep the contract chose summing, the gold convention, which
  is also what a SQL join followed by `SUM` gives by default.

**Framing.** G1 and G3 below transplant DABStep's two facts into new
domains under control; they are not independent evidence of the same
mechanism. G2 is the gap of a different kind, and carries the
generalisation.

## Goals

- Measure recovery per gap as a rate over author draws, with a no-data
  control, before any agent runs.
- Register the knowledge share with an interval and a threshold that
  falsifies "mainly knowledge".
- Register what it means if agents recover data-settled gaps by
  themselves.
- Keep the distinction "data-settled vs convention-only" a property others
  can check, not our assertion.

## Non-goals

- A new benchmark. 4b reuses experiment 4's three databases, its questions
  and its reference implementations.
- Testing the contract's form. Experiment 4's materials put prose and
  contract at 97-100%; DABStep's form effect (+3.7 to +5.5 points) is two
  or three tasks here. 4b reports `contract_text` against `manual_noted`
  as an estimate, marked underpowered; the paper's form claim rests on
  experiment 5c. *(Decided 2026-10-19.)*
- Writing style. 4b uses the clean KB style so the gaps are the only
  difference.
- Tuning the author. `dce.author` runs as in experiments 2 and 4, its
  prompt unchanged; if it fails to recover a gap, that is a result.

## Anti-goals (a failure even if it runs)

- Gaps whose data-settled status is only our assertion.
- Gaps that do not change any gold, so the arms cannot differ.
- The gap proposer, the author, the note writer, the raters or the
  contract checker seeing a question, a gold or a result.
- Notes for the prose arm that carry anything other than what the author
  settled, right or wrong.
- Changing materials after the smoke.

## Materials

### Databases, knowledge and questions

Experiment 4's three databases (`reverse_logistics`,
`labor_certification_applications`, `solar_panel`), their pieces R
(rule-matching procedure) and C (composite), the 60 questions (ten per
piece per database) and the reference SQL. Golds are recomputed on the 4b
databases. Where a gap changes only how the data stores a fact, not its
meaning, the recomputed golds must equal experiment 4's byte for byte.

### The gaps

G1 and G2 in every database; G3 in `reverse_logistics` only (decided
2026-10-19: with G3 open, almost every R question would depend on both G1
and G3, and a G3 loss would contaminate the G1 test; `reverse_logistics`
has the most multi-match records, 1,213 of 1,500).

- **G1, data-settled, in R: a wildcard encoding.** The rule table stops
  using NULL for "matches anything" in its **text** criteria and uses a
  sentinel instead, a different one per database. Numeric band bounds
  (`reverse_logistics`' `min_monthly_returns` / `max_monthly_returns`,
  where NULL means "no limit on this side") keep NULL. The gapped KB keeps
  the original sentences, including "a non-NULL criterion never matches a
  record whose value is NULL". The reference treats the sentinel exactly
  as the KB's NULL criterion, so it also matches records whose value is
  NULL (655 returns, 273+ cases and 378+ snapshots depend on this). Full
  recovery of G1 means the same; reading the sentinel as a wildcard but
  not for NULL record values is scored as partial and reported apart. The
  full KB states the sentinel and that it matches any value, NULL
  included.
- **G2, data-settled, in C: a silent encoding.** One quantity C reads is
  stored differently from what the KB says, in a way the values reveal and
  that yields a plausible but wrong number when read literally (a
  percentage stored as a fraction, a scale or unit shift). Label or case
  changes are excluded: a literal filter returning nothing alerts the
  agent (a "loud" gap). At least one of the three G2s must be a type that
  `dce.author`'s prompt does not name (it names how values are stored,
  units, NULLs and formats); for example a shifted code mapping. No G2 may
  touch a value R matching or a question text uses. The gapped KB keeps
  the original wording, including the original entries that describe the
  column; the full KB states the stored form.
- **G3, convention-only, in R of `reverse_logistics`: how matching rules
  combine.** The gapped KB drops the precedence sentences of KB 33 (most
  specific wins, ties to the lowest `rule_id`) and says only that a return
  is charged under the schedule. The no-match default stays, moved to an
  entry of its own; singular wording elsewhere ("the applying rule") is
  rewritten neutrally in the gapped KB only. The reference, and so the
  gold, stays "most specific wins". `reverse_logistics`' R06 ("which rule
  applies") becomes "which rules match" a described return (gold: the set
  of matching rule ids) and counts toward G1 only.

Each gap is classified **loud** or **silent** before authoring (does a
literal reading return empty or error, or a plausible wrong value?), and
results are reported per class. By design G1-G3 are silent.

**Shared prompt surfaces.** Every arm and the author (except the no-data
control) see the DDL, column meanings and the first rows of each table.
Column meanings follow the gapped documentation in every arm, so the
documentation is consistently wrong, as DABStep's is. Sample rows are
regenerated from the 4b databases as they come; whether they display each
gap is recorded, not arranged. `manual_full`'s KB states the stored form
as a correction that overrides the column meaning.

### Who chooses the gaps, and what they see

1. **The proposer** is a session of a different model family from the
   author (`gpt-6-sol`; the author is Claude). It sees the original KB,
   the schema and read-only data, never `questions.json`, golds, the
   reference SQL or results. Per database it writes a ranked menu of G1
   sentinels and G2 candidates meeting the criteria above. The menu is
   committed.
2. **The bite check** is run by the builder, who sees the questions. It
   returns only pass or fail per candidate. A failed candidate is replaced
   by the next on the committed menu; no other feedback goes back.
3. **The blind raters** (two, as in the LiveSQLBench clarity test) see the
   gapped KB and read-only data, no questions, golds or reference. For
   each gap location they say how the documentation should be read, or
   that the data cannot tell. A gap counts as data-settled when both reach
   the reference reading, and convention-only when they split or say the
   data cannot tell. The analysis plan for the ratings is fixed before
   they are read.

### Checks per gap (in the materials commit)

1. **It bites.** A literal implementation of the gapped KB (the original
   reference SQL on the 4b database for G1 and G2; a sum over all matching
   rules, and separately the lowest `rule_id`, for G3) differs from the
   gold on at least half of the questions that use the gapped entry. The
   share is reported per gap.
2. **A stated data property.** G1: the sentinel never occurs as a record
   value in the matched column; across the database's text criteria at
   least 20% of rules use it, and at least 15% for each criterion (solar's
   `vol_tier` is at 19.6%, so a per-criterion 20% would force new golds);
   no text criterion of any rule is NULL. G2: every stored value lies in a
   range or code set that contradicts the KB's stated form and agrees with
   the full KB's.
3. **G3 is not settled by the data.** Both candidate conventions give
   valid, non-degenerate answers on every question that uses it.
4. **The blind raters** classify each gap as above. A G1 or G2 the raters
   do not settle is replaced from the menu.

### Question subsets (tagged from the bite check, before any run)

- **Data-settled subset:** the R questions of `labor_certification_applications`
  and `solar_panel` (G1), `reverse_logistics`' rewritten R06 (G1), and the
  C questions of all three databases whose gold depends on G2. About 50
  questions, but about five gap clusters: G1 in two databases (plus one
  question) and G2 in three.
- **G3 subset:** `reverse_logistics`' other nine R questions (G1 and G3
  together), read descriptively.

## Authoring

- **Three draws with data per database**: `dce.author`, the gapped KB,
  the schema, sample rows and read-only `run_sql`, as in experiment 4. The
  first draw run feeds the agent arms (fixed now, not chosen after
  seeing the draws).
- **One no-data control per database**: the same author, gapped KB and
  schema, with no `run_sql` and no sample rows. Recovery only; it feeds no
  arm.
- **Recovery rubric**, committed before the first draw. For each gap and
  each draw, the contract is checked on every record against the
  reference, separately for its text (what it says the reading is) and its
  SQL (what it computes):
  - *recovered*: text and SQL agree with the reference on every record
    the gap touches;
  - *partial*: right on some criteria or records and not others (for
    example the sentinel as a wildcard but not for NULL record values; the
    stored form handled in one metric and not another);
  - *stated, not built*: the text gives the reference reading and the SQL
    does not;
  - *missed*: the literal reading, or another one.
  For G3 the rubric records which convention the draw chose.
- **Correctable errors** are disagreements on records no gap touches (a
  JSON path, the monthly count, a join). A question-blind reviewer
  corrects them in the draw that feeds the arms, records each, and the fix
  reaches both `contract_gap` and the notes. Anything on a record a gap
  touches, including a slip in gap-handling SQL, is the measurement and is
  never corrected.

## Arms

All arms have the database's DDL and column meanings and the plain or
governed tools as in experiment 4, and run together in each repeat.

| arm | what it gets | role |
|---|---|---|
| `schema_only` | nothing more | manipulation check |
| `manual_gap` | the KB with G1-G3 open | the documentation as it stands |
| `manual_full` | the KB with G1-G3 stated | knowledge ceiling |
| `contract_gap` | the authored contract (first draw) | the contract as an author made it |
| `manual_noted` | `manual_gap` plus the author's resolutions as notes | the author's knowledge, in prose |
| `contract_text` | `contract_gap`'s two files in the prompt, plain tools | the author's knowledge in the contract's form, no tools |

`manual_noted`'s notes carry every way the authored contract departs from
or adds to the gapped KB, in words, under one heading ("## Data notes"):
the author's own text where it states a resolution, a plain paraphrase
without SQL where only the SQL does. A separate model writes them, blind
to questions, golds and results; a second checks that every resolution is
there and nothing else. Each resolution is stated once in each arm, which
avoids experiment 5c's repetition caveat.

## Models, runs, measure

`gpt-6-luna`, `qwen3.8-27b` and `gpt-6-sol`, k=3, the six arms together in
each repeat, `--retry-pass`, on the cluster. 60 questions x 6 arms x 3
repeats = 1,080 runs per model. Graded as LiveSQLBench is (Soft-EX,
official rules), end-to-end beside it.

**Statistics.** The independent unit is closer to the gap than the
question: one author choice moves every question of its cluster together.
So each test is reported three ways: a task-level sign test over the three
repeats; the direction per gap cluster; and the direction per model. A
directional prediction holds only if the sign test passes, the direction
holds on each model, and it holds on a majority of gap clusters. Shares
get a 95% bootstrap interval resampling gap clusters.

## Predictions (fixed in the run commit; the smoke may not change them)

Offline, before any agent runs:

- **(32)** Authors with data recover the data-settled gaps: at least two
  thirds of the gap-draws (G1 and G2, three databases, three draws) are
  *recovered*.
- **(32c)** The no-data control recovers fewer data-settled gaps than the
  data draws, and at most one of six.
- **(33)** On G3 no data draw chooses "most specific wins"; the account
  expects summing over matches. The distribution of choices is reported.

On the data-settled subset:

- **(34)** `manual_full` beats `manual_gap` (the gaps bite agents).
- **(35)** `contract_gap` beats `manual_gap`.
- **(36)** The knowledge share, (`manual_noted` - `manual_gap`) /
  (`contract_gap` - `manual_gap`), is at least 0.5. "Mainly knowledge" is
  falsified if the interval's upper bound is below 0.5; an interval
  spanning 0.5 with a point estimate below it is reported as
  inconclusive.

Reported, not predicted: `contract_text` against `manual_noted` (form,
underpowered) and `contract_gap` against `contract_text` (tools).

On the G3 subset, descriptive: `contract_gap` against `manual_gap` and
`manual_full`, read against the convention the first draw chose.

**Readings fixed with the predictions.**

- (34) fails, `manual_gap` within 5 points of `manual_full`: agents
  recover these data-settled decisions at query time; on such gaps a
  contract adds nothing, and its value lies in decisions the data cannot
  settle and in gaps an agent does not think to check. (35) and (36) are
  then reported but not read as support.
- (32) fails: an author with the data open does not reliably recover
  data-settled decisions; a contract's value then depends on review
  against certified answers (`analysis/certify_metrics.py`), not on
  authoring alone.
- (32c) fails: recovery does not come from the data, and the gaps were
  guessable from the documentation; Q1 is then not tested.
- (35) holds and (36) is falsified: the contract's gain comes mainly from
  its form or tools, not the author's knowledge, contrary to DABStep.
- (33) fails: the author did not fall to the SQL default; report what it
  chose and whether it matched the gold.

## Order of work

1. Gap menu (proposer); bite check; data-property checks; blind raters;
   4b databases, gapped and full KBs, golds recomputed, the R06 rewrite,
   loud/silent classes, question subsets. **Materials commit**, before any
   authoring.
2. Recovery rubric committed; three data draws and one no-data control per
   database; recovery scored; correctable errors fixed in draw 1.
   **Recovery commit**, before any agent runs.
3. Notes written and checked; `contract_text` built.
4. Smoke (12 questions, gpt-6-luna, all arms). It checks that the
   arms run; it does not change materials or predictions except to
   record their final form.
5. **Run commit** with (32)-(36), the readings, and the recovery results
   already recorded. Runs on the cluster. Analysis script
   `analysis/procedures_gap.py`, built test-first like
   `analysis/contract_noted.py`.

## Constraints

- No question, gold or result reaches the proposer, the author, the note
  writer, the raters or the contract checker.
- LiveSQLBench databases stay uncommitted (as in experiment 4); the 4b
  materials (KBs, gap menus, ratings, rubric, recovery scores, questions,
  reference SQL, checks) are ours and are committed.
- Every row stamps the run commit; the runner refuses a dirty tree.
- Spend: agent runs about $15 a repeat across the three models;
  authoring 12 sessions, roughly $20-60.

## Left to the builder

- The G1 sentinels and G2 quantities, taken in order from the committed
  menus.
- How the 4b databases are built from experiment 4's build scripts (a flag
  on each `build.py` is the expected route).
- The proposer's and raters' prompts, within the visibility rules above.
- The note heading and extraction, within the rule that notes carry the
  author's resolutions and nothing else.

## Amendments while building the materials (2026-10-19, before any authoring)

Recorded as they happened; nothing below was decided after seeing an author
draw or an agent run.

1. **Proposer prompt, three rounds.** Round 1 (reverse_logistics only)
   judged "the data reveals it" against the data before the change and
   proposed G2s on `unit_value`, which R also reads. Round 2 (all three)
   proposed G2s whose evidence shows only that something is off. The prompt
   was sharpened twice, generically: the evidence must be visible in the
   changed data alone and must pin down the exact transformation; nothing R
   reads may change; candidates on at least two quantities. Rounds 1 and 2
   are kept under `prep/procedures4b/<db>/superseded-attempt{1,2}/`.
2. **Sentinels** are taken in menu order per database, alphabetically by
   database, skipping one already used: labor `*`, reverse_logistics `ANY`,
   solar `ALL`.
3. **No G2 for labor.** Its menu (wage amounts) has no bound the
   documentation states, and the wage fields are text; the menu is
   exhausted and labor's C questions become a no-gap control.
4. **Missing-value and specificity wording.** Round 1 of the ratings showed
   that the gapped KB's sentence "a non-NULL criterion never matches a NULL
   record value" made G1 partly a convention (does the sentinel match a
   missing value?). In both documentations the missing-value and
   specificity sentences now speak of a criterion that "places no
   restriction" (`gaps.json`, `doc_edits`); the sentences that say the
   wildcard is NULL are G1 and stay.
5. **reverse_logistics' G2** moved from menu rank 0 (scores stored 0-9) to
   rank 2 (stored 2-11) after round 1 of the ratings split on rank 0; rank
   1 is not read by C.
6. **Rater labels replace the pass/fail gate** (decided 2026-10-19). Round
   2 of the ratings: both raters settle solar's G1 and G2; they split on
   labor G1, reverse_logistics G1 and G2 (gpt-6-sol identifies every
   sentinel but will not extend it to missing values, nor infer the exact
   shift from 2-11 against a documented 1-10; claudeopus5.5 does both); G3
   is convention-only. Every gap is kept and labelled: **settled** (solar
   G1, G2; 20 questions), **split** (labor G1, reverse_logistics G1 via R06,
   reverse_logistics G2; 21), **convention-only** (reverse_logistics G3; 9),
   **no gap** (labor C; 10). The data-informed subset is settled + split
   (41 questions, five clusters).
7. **Predictions, re-specified before authoring.** (32) is over the five
   data-informed gaps (15 gap-draws). Added **(32g)**: recovery is highest
   on settled gaps, then split, then convention-only (no draw choosing
   "most specific wins"). (32c) becomes at most one of five. (34)-(36) are
   on the data-informed subset, reported per label; the no-gap control is
   reported beside them (all knowledge arms should tie there).
