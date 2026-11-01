# Experiment 7: does a contract drawn from the data recover what DataAgentBench's hints state?

Status: draft (2026-11-01), revised after a builder check; the calls it
prompted are marked *(Our call.)*. Phase 1 is a research run on our harness; phase 2
is a public leaderboard submission, after the PVLDB deadline and after the
benchmark's maintainers answer the questions in "Phase 2". Builds on
experiment 4b (`docs/superpowers/specs/2026-10-19-experiment-4b-design.md`,
results `e1467c2`) and on paper v3's thesis
(`docs/superpowers/specs/2026-10-23-paper-v3-restructure-design.md`).

## Why

The paper's thesis is that a context layer's measured gain is mostly
knowledge, and that the knowledge an agent lacks is data semantics the
documentation leaves out. Its evidence has a ceiling: one natural procedure
(DABStep's fee rules), plus constructed gaps on LiveSQLBench (experiment 4b).
Every reviewer so far has named generality as the weakest point.

DataAgentBench (DAB; Ma et al., arXiv 2603.20576, UC Berkeley EPIC;
`github.com/ucbepic/DataAgentBench`) is an external benchmark that has the
same structure, built by someone else:

- Its builders corrupted the data on purpose: join keys with stray prefixes
  and whitespace, identifiers in non-standard encodings, structured values
  stored as strings, numbers written as free text.
- Each dataset has a plain description (`db_description.txt`) and a short
  hints file (`db_description_withhint.txt`, 360 bytes to about 1 KB) that
  states the corruptions and some code meanings. The hints are the
  builders' knowledge of the data, kept apart from the description. The
  benchmark's leaderboard marks every entry by whether it used them.
- Its leaderboard shows the confound the paper isolates. The top entry,
  "DataGallery + semantic layer" (0.9917 pass@1), gives the agent "one note
  per benchmark query ... distilled from the team's earlier runs on the
  same questions"; the same team and model without the notes score 0.7932.
  Context-layer gains on the leaderboard arrive together with new
  knowledge, some of it written from the test questions.

**The question (phase 1).** An author who sees the description and the data
but no hints and no questions draws a contract. Does that contract state
what the builders' hints state, and how much of the hints' accuracy gain
does it recover?

**The question (phase 2).** Under the leaderboard's own rules (all 54
queries, 5 trials, traces, leakage audit), what does a question-blind
contract drawn from the data add to the same agent?

**What it does not ask.** Delivery. The hints are 1 KB at most; experiment 6
shows text and tools tie at one contract's size. Delivery is reported
(`dab_contract` against `dab_contract_text`), not predicted beyond a tie.

## Goals

- A second, externally built benchmark for the "missing semantics" finding:
  the knowledge a builder documents, against the knowledge an author
  recovers from the data.
- Recovery measured offline, item by item, before any agent run, with the
  rubric of experiment 4b.
- Predictions, tests and readings fixed in the run commit.
- A harness adapter that phase 2 extends rather than rewrites.

## Non-goals

- A leaderboard entry in phase 1. Phase 1 uses SQL-only tools and a subset.
- Python execution or MongoDB in phase 1 (phase 2 adds both).
- Changing the library. The governed arm is experiment 4b's.
- Tuning the prompt to DAB's conventions. The base prompt is DAB's own,
  adapted only for our tool names.

## Anti-goals (a failure even if it runs)

- Any question, gold, validator or result reaching the author, the note
  writer, the raters or the contract reviewer. This is also the leaderboard's
  leakage rule (rubric section 2.2), and the reason the comparison is clean.
- A floor effect: arms that all fail because SQL-only tools cannot do DAB's
  text extraction. The pilot gate below exists for this.
- A contrast that is really the validator's leniency: DAB's validators
  check that the gold appears in the answer, so a long answer listing many
  candidates can pass.
- A difference made by the harness rather than the knowledge: every arm
  runs on the same tools, prompt and database session, differing only in
  the knowledge (and, for `dab_contract`, the governed tools).

## Materials

### Benchmark, pinned

- DAB at commit `bcc2e1b` (2026-10-31). Data files from its LFS store and its
  Hugging Face mirror, checked against `dataset_manifest.tsv` sha256s.
- Phase 1 datasets: the leaderboard's 54-query set without MongoDB and
  without PATENTS (5.4 GB): bookreview (3), crmarenapro (13), DEPS_DEV_V1
  (2), GITHUB_REPOS (4), googlelocal (4), music_brainz_20k (3),
  PANCANCER_ATLAS (3), stockindex (3), stockmarket (5). **40 queries, 9
  datasets.**
- Gold answers and validators are public in DAB's repository. We do not
  commit them; we commit what we make (hint items, ratings, rubric,
  recovery scores, contracts, author logs).
- DAB's repository has no license file (the paper is CC BY 4.0); the issue
  in phase 2 asks. Until answered, nothing derived from DAB data is
  published beyond our contracts and short quotes of the hints.

### One database session per dataset

DAB spreads a dataset over two to six databases in SQLite, DuckDB and
PostgreSQL. Our tools take one DuckDB database. Per dataset, a build step
makes one DuckDB file: SQLite and DuckDB files are attached and copied,
and PostgreSQL dumps (plain `pg_dump` with `COPY ... FROM stdin` blocks)
are parsed and loaded. Each logical database becomes a schema named as in
`db_description.txt` (for example `books_database.books_info`), so the
description's names stay right. Types are kept; values are loaded byte for
byte, corruptions included.

The prompt adds one fixed sentence to the description: the databases are
schemas in one DuckDB session and are queried with DuckDB SQL. This differs
from DAB's harness, where the agent joins across systems in Python. It is
the same for every arm and is stated as a deviation.

**stockmarket** keeps DAB's structure: one table per ticker (2,753), byte
for byte, no union view. The author's input lists `stockinfo`, says the
trade schema holds 2,753 tables of one shape, and shows the DDL and sample
rows of three, chosen by a fixed seed. If the contract library needs every
allowed table listed, the build lists them programmatically. *(Our call:
a view would help every arm, but it is not DAB's task.)*

Checks in the materials commit: per table, row counts against the source;
per column, a hash of the values against the source; the built files'
sha256s recorded.

### Tools

Phase 1: the harness's tools as in experiment 4b (`list_tables`,
`describe_table`, `execute_sql`; governed variants for the contract arm),
plus the harness's answer step. Text extraction is done in DuckDB SQL
(`regexp_extract`, JSON functions). No Python.

### Hint items

Each hints file is split into items, one per stated fact (usually one per
bullet). A short-list of items per dataset is committed. Example
(bookreview): "`description`, `categories` and `features` look like lists
or dictionaries but are stored as strings"; "`book_id` and `purchase_id`
name the same books and need a fuzzy join"; "some queries need
`categories` or `details`".

Two blind raters, of different model families, see the description, the
items and read-only data, never the questions, golds or validators. Each
labels each item with one class, checked in this order:
- *described*: `db_description.txt` already states it (bookreview's fields
  "stored as string representation of list/dict", music_brainz's
  "potential duplicates"). Excluded from (43)'s denominator and reported
  apart: an author who copies the description gets it for free.
- *definition*: a formula or business definition (intraday volatility, up
  and down days, "financially troubled"). The data cannot show a
  definition; it is scored but read as convention.
- *data-discoverable*: the data shows it (a prefix, an encoding, a type);
- *world*: general knowledge settles it (what an exchange code means);
- *guidance*: it points at where to look rather than stating a property
  ("some queries need `categories`").

An item takes a class when both raters give it; a split goes to the later
class in the list (the conservative side for (43)). The ratings and this
rule are committed before they are read, as in 4b.

## Authoring

**The author's prompt.** `dce.author`'s prompt is built for LiveSQLBench
(numbered KB entries, a `kb_coverage` gate, one schema `main`). Its DAB
version may change only mechanics: the description as free text in place
of KB entries, no coverage gate, several schemas, and the stockmarket
schema summarised (below). The content guidance stays 4b's, word for word
("how values are stored, units, NULLs, JSON paths, formats"). Nothing may
name a kind of corruption: whoever edits the prompt has read the hints, so
any such wording would carry them into draw A. A reviewer who sees the
4b prompt and the diff, but not the hints, checks that the diff is
mechanical. The prompt is frozen in the materials commit. *(Our call.)*

- **Draw A, three per dataset, no hints.** `dce.author` with the
  description, the schema, sample rows and read-only `run_sql` on the built
  DuckDB file. No hints, no questions. Draw A1 feeds `dab_contract` and
  `dab_contract_text` (fixed now, not chosen after seeing the draws).
- **Draw B, one per dataset, with hints.** The same, plus the hints file.
  It feeds `dab_hints_contract`: the deployment case, an author who has the
  documentation and the data.
- **No-data control, one per dataset.** The same author, description and
  schema, with no `run_sql` and no sample rows. Recovery only; it feeds no
  arm. DAB and its hints have been public since March 2026, so the author
  may remember them. Contamination is not symmetric for the author the way
  it is across agent arms. The control separates recovery from the data
  from recovery from memory or from the description. *(Our call; 9 more
  sessions.)*
- **Recovery rubric** (4b's, committed before the first draw). For each
  hint item and each draw: *recovered*, *partial*, *stated, not built*,
  *missed*. Scored by a checker who sees the contract, the items and the
  data, never the questions. Items with records to check
  (data-discoverable) are scored on every record they touch, as in 4b.
  Items without records (definition, world, guidance) are *recovered* when
  the contract's text states an equivalent and, where the item implies a
  computation, a metric computes it on the built data; *stated, not built*
  when only the text does.
- **Additions.** What a draw states that no hint does (an undocumented
  corruption, a join, a unit) is listed, not scored.
- **Correctable errors** follow 4b: a question-blind reviewer fixes errors
  on records no hint item touches, records each, and the fix reaches every
  arm that uses the draw. Anything a hint item touches is the measurement.

## Arms

All arms have the description, DAB's system prompt adapted to our tools,
and the one-session sentence. They run together in each repeat.

| arm | what it gets | role |
|---|---|---|
| `dab_desc` | description only | DAB's no-hints condition |
| `dab_hints` | description + hints, appended as DAB's `run_agent.py` does | the builders' knowledge |
| `dab_contract` | description + governed contract, draw A1 | an author's knowledge, from the data |
| `dab_contract_text` | description + draw A1's files in the prompt, plain tools | the same knowledge, no tools |
| `dab_hints_contract` | description + hints + governed contract, draw B | builders' and author's knowledge together |

## Models, runs, measure

- `gpt-6-luna` and `qwen3.8-27b`, k=5 (the leaderboard's trial count; the
  runs are cheap), five arms together in each repeat, `--retry-pass`, on
  the cluster. 40 queries x 5 arms x 5 repeats = 1,000 runs per model.
- The job fetches DAB at the pinned commit, checks sha256s and builds the
  DuckDB files in the pod's emptyDir (about 3 GB of source; the namespace's
  ephemeral quota is 5 GiB). The PVC (1 GiB) holds rows and transcripts
  only.
- **Primary measure:** DAB's own `validate.py` for each query, on the
  harness's final answer only, run in a subprocess. This is the measure the
  leaderboard uses.
- **Leniency check:** an *overlong pass* is a pass whose answer is more than
  200 characters longer than the gold string. Reported per arm. If two arms
  differ by more than 5 points in overlong passes, every comparison between
  them is also reported without those rows.

**Statistics.** 40 queries in 9 datasets: only large effects are
detectable (about 15 points at this n). Accuracy is pooled over queries;
the tests are task-level, so the measure matches them. The leaderboard's
form (the mean over datasets of each dataset's pass rate) is reported
beside it, never tested. crmarenapro is 13 of the 40 queries, so every
test is also reported leaving out one dataset at a time. *(Our call.)*
Each test is a task-level sign test over the five repeats, with the
direction per dataset reported beside it. A directional prediction holds
when the sign test passes and the direction holds on a majority of the
datasets where the arms differ. The share in (46) gets a 95% bootstrap
interval resampling datasets.

## Pilot gate (before the run commit; its rows enter no test)

One repeat of `dab_desc` and `dab_hints` on luna over the 40 queries
(80 runs, under $1). It checks that the harness, the builds and the
validators work. Two floor rules:
- **Overall:** if `dab_hints` passes fewer than 25% of the queries,
  SQL-only tools are a floor. The Python tool is built first and the pilot
  is repeated.
- **Per dataset:** a dataset where `dab_hints` passes none of its queries is
  recorded before the run commit. It still runs, and it is reported, but it
  is left out of (44)-(48). stockmarket is the likely case. *(Our call.)*

The Python tool, if the floor rule fires, is DAB's design: it reads only
the stored results of the agent's earlier tool calls, never the data
files. All data access stays in SQL, so the governed arm stays governed.
*(Our call.)* The pilot cannot change the predictions or the arms.

## Predictions (fixed in the run commit)

Offline, before any agent runs:

- **(43)** Draw A recovers at least half of the data-discoverable hint
  items (*recovered*, over items x three draws).
- **(43c)** The no-data control recovers fewer data-discoverable items
  than draw A, and at most a third of them.
- **(43w)** Draw A recovers fewer *definition*, *world* and *guidance*
  items than data-discoverable ones. Reported per class; *described* items
  are reported apart.

On the 40 queries, primary measure, per model:

- **(44)** `dab_hints` beats `dab_desc` (the builders' hints bite in our
  harness; manipulation check). One-sided p < 0.05.
- **(45)** `dab_contract` beats `dab_desc`. One-sided p < 0.05.
- **(46)** The recovered share, (`dab_contract` - `dab_desc`) /
  (`dab_hints` - `dab_desc`), is at least 0.5. Falsified if the interval's
  upper bound is below 0.5; an interval spanning 0.5 with a point estimate
  below it is inconclusive.
- **(47)** `dab_contract` and `dab_contract_text` show no detectable
  difference: within 5 points and two-sided p >= 0.10.
- **(48)** `dab_hints_contract` beats `dab_hints`. One-sided p < 0.05.

**Readings fixed with the predictions.**

- (44) fails: the hints do not bite in this harness, on this subset. (45)
  to (48) are reported but not read as support; the floor rule and the
  leniency check are the first suspects.
- (43) holds and (45) fails: the author writes down what the data shows,
  and agents with the description alone recover it at query time anyway.
  On these datasets a contract's knowledge adds nothing an agent would not
  find.
- (43c) fails: recovery does not come from the data. The author remembered
  DAB or read it off the description, and (43) is not read as recovery
  from the data.
- (43) fails: an author with the data open does not reliably recover
  planted, data-visible semantics; contracts then need review against
  certified answers, as 4b concluded.
- (45) holds and (46) falsified: the author recovers some of the builders'
  knowledge, but most of the hints' gain comes from items the data does
  not show (world, guidance). This is read with (43w).
- (47) fails: delivery matters here; reported with its direction and read
  against experiment 6 (contract size) and experiment 5d.
- (48) holds: an author with the documentation and the data adds to the
  documentation, as DABStep's contract did. (48) fails: on DAB, the hints
  already carry what the data adds.

## Phase 2: the leaderboard (after Dec 1)

Two entries, identical except for the contract, both with hints and DAB's
untuned prompt:
1. `agentic-data-contracts harness (gpt-6-luna)`, hints;
2. the same plus the draw-B contract, governed.

Their difference is the leaderboard's own measure of what a question-blind
contract drawn from the data adds, with the leakage audit applied to both.

What phase 2 needs beyond phase 1:
- All 12 datasets, 54 queries, 5 trials each (270 per entry), with traces.
- **MongoDB** (agnews, yelp): a read-only Mongo query tool, or the
  collections loaded unchanged into DuckDB, as the maintainers allow.
- **Python execution:** a sandboxed `execute_python` (no network,
  read-only data, 600 s timeout, pandas and pyarrow, as DAB's).
- **PATENTS** (5.4 GB): build within the pod's ephemeral quota.
- The submission JSON (`dataset`, `query`, `run`, `answer`) and traces in
  the PR; host names scrubbed; the gateway sign-off for publishing traces
  made through it.
- The maintainers' answers to the issue below: whether a question-blind
  contract counts as a legitimate prompt input or "tuned prompt", whether
  Mongo collections may be queried through DuckDB, whether one DuckDB
  session for a dataset's databases is acceptable, and the repository's
  license.

### Issue to the maintainers (draft, posted by the author)

> **Title:** Submission question: a question-blind semantic layer built from
> the sanctioned data
>
> Hi, and thanks for DAB and for the detailed submission rubric.
>
> We are planning a leaderboard submission that compares two
> configurations of the same agent, identical except for one thing: a
> "data contract" (a small semantic layer: table and column semantics,
> join rules, metric SQL, and the forbidden operations) for each dataset.
> The aim is to measure what such a layer adds under your leakage rules.
> Before we run, we'd like to check four points against the rubric.
>
> 1. **The contract as a prompt input.** Each contract is written ahead of
>    time by a separate model session that sees only `db_description.txt`
>    (and, for one configuration, `db_description_withhint.txt`), the
>    schema, and read-only SQL access to the sanctioned stores. It never
>    sees `query.json`, `ground_truth.csv`, `validate.py`, or any run
>    output. We would publish the contracts and the authoring logs with the
>    traces. Is that a legitimate input under section 2.2 ("discovered
>    through exploration of the sanctioned data"), and would you mark it as
>    "tuned prompt"?
> 2. **One SQL session per dataset.** Our tools query a single DuckDB
>    session. We would load each dataset's SQLite, DuckDB and PostgreSQL
>    stores unchanged into one DuckDB file, one schema per logical database,
>    so cross-database joins are possible in SQL. Is that acceptable, or
>    should each store stay behind its own connection?
> 3. **MongoDB.** For agnews and yelp, may the collections be loaded
>    unchanged from the provided dumps into DuckDB and queried with SQL, or
>    should the agent query MongoDB itself?
> 4. **License.** We couldn't find a license file in the repository. Under
>    what terms may we publish material derived from the datasets (the
>    contracts quote column names and a few values)?
>
> We'd also be glad to know whether the leaderboard will move from the
> 54-query set to the paper's 104 queries, so we run the right set.
>
> Thanks!

## Order of work

1. Adapter `dce/benchmarks/dataagentbench.py` (benchmark name
   `dataagentbench`, not `dab`, to avoid confusion with DABStep), the build
   step and its checks, validator wrapper with the overlong flag; tests
   first. **Materials commit.**
2. Hint items split; raters; ratings read by the committed rule.
   **Ratings commit.**
3. Recovery rubric committed; draws A (3 per dataset), B (1 per dataset)
   and the no-data control (1 per dataset); recovery scored; correctable errors fixed in A1 and B.
   **Recovery commit**, before any agent run.
4. Pilot gate.
5. **Run commit** with (43)-(48), the readings and the recovery results
   recorded; analysis script `analysis/dataagentbench.py`, test-first.
   Runs on the cluster.
6. FINDINGS; one paragraph and one table in the paper if it lands in
   time, else the revision.

## Constraints

- No question, gold, validator or result reaches the raters, the author,
  the checker or the reviewer. Authoring sessions see only the
  description, the schema, sample rows, read-only data and (draw B) the
  hints.
- Every row stamps the run commit; the runner refuses a dirty tree.
  Transcripts go to the PVC, and trace coverage is checked before archiving
  (as in experiment 5d).
- Spend: agent runs a few dollars per model; authoring 9 x 5 sessions,
  roughly $25-60; raters and checker a few dollars.
- Contamination: DAB and its golds have been public since March 2026, so
  the models may have seen them. It affects every arm alike and is stated
  as a threat; the contrasts are within model.

## Left to the builder

- How items are split where a hint bullet states two facts.
- The DuckDB types for PostgreSQL types without a direct match.
- The exact text of the one-session sentence and the tool-name adaptation
  of DAB's system prompt (committed in the materials commit).
