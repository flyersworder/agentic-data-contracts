# Experiment 7: recovery rubric (draft)

Fixed before the first author draw (design: "Authoring"), on the model of
experiment 4b's (`prep/procedures4b/RUBRIC.md`). Applied to every draw, per
dataset: three without hints (`contracts-dataagentbench/draw-a{1,2,3}/`), one
with them (`draw-b/`) and one without the data (`no-data/`), all written by
`dce.benchmarks.dab_author`. Draw A1 feeds `dab_contract` and
`dab_contract_text`, draw B feeds `dab_hints_contract`; both were fixed before
any draw ran. The no-data control feeds no arm.

## The items

The unit is a hint item: `prep/dataagentbench/hint-items.json`, one id
`<dataset>-<n>` per item, with the class it was given before any draw
(described, definition, data-discoverable, world, guidance). The checker
scores the class as given and does not reclassify; a doubt about a class is
recorded as a note beside the score.

Described items (the description already gives them) are scored like the
others but left out of the main recovery measure and reported apart.

## Who checks, and what they see

One checker per dataset, who sees no question, gold, validator or agent
result: never a query folder's `query.json`, `ground_truth.csv` or
`validate.py`, nor any run under `results/`. The checker sees:

- the draw's `contract.yml` and `semantic.yml`, and its author log (the
  queries the author ran and what it wrote between them);
- the dataset's hint items and their classes;
- DAB's `db_description.txt`;
- the built file (`DAB_DATA/duckdb/<dataset>.duckdb`), read-only.

The checker runs the contract's SQL on the built file, and queries of its own
to find what an item touches.

## Before any draw is read: the references

Per item, written once and used for every draw of the dataset, before the
checker opens a draw:

- **data-discoverable**: the item's record set, a query on the built file that
  returns every record the item touches, and the reading the item gives each
  of them (the reference). Recorded with its count.
- **definition, world, guidance and described**: what the contract must state
  to be equivalent, and whether the item implies a computation. When it does,
  the checker's own SQL for it on the built file is the reference.

## Scoring

Two readings are recorded for each item and draw, as in 4b: the **text**
(does the contract say what the item says, with a quote and where) and the
**SQL** (what the contract's metrics compute). Numbers agree within 1e-6,
relative or absolute; text and ids exactly.

**Data-discoverable items** are scored on every record they touch. The
checker finds every metric, `from_sql` and description that reads the
records, runs the metrics' SQL over them, and compares each record with the
reference.

- **recovered**: the SQL agrees with the reference on every record the item
  touches.
- **partial**: the SQL agrees on some records and not others, or one metric
  reads them as the reference does and another that reads the same records
  does not. Which part is right is recorded.
- **stated, not built**: the text gives the reference reading and the SQL
  does not, or no metric reads the records.
- **missed**: neither the text nor the SQL gives the reference reading.

**Definition, world, guidance and described items** have no records to check.

- **recovered**: the contract's text states an equivalent (any wording, the
  same meaning) and, where the item implies a computation, a metric computes
  it: run on the built file, it agrees with the reference.
- **partial**: the text states part of the item, or the metric computes part
  of it. Which part is recorded.
- **stated, not built**: the text states an equivalent and the item implies a
  computation that no metric performs, or that a metric performs and gets
  wrong.
- **missed**: the text states no equivalent.

A draw whose session ended with no accepted contract scores every item
*missed*, noted "no contract".

## Additions

What a draw states that no hint item does is listed, not scored: a property of
the stored data the description does not give, a join, a unit, a convention.
Each with its quote, where it is, and whether the checker confirmed it on the
built file (yes, no, or cannot be checked). Additions enter no measure.

## Correctable errors

As in 4b. A disagreement whose cause lies outside every hint item is an author
error, not the measurement: a wrong join key or column for records no item
touches, a wrong table, a slip in a metric that reads no item's records. The
checker names the cause. In draws A1 and B only, a reviewer who sees no
question, gold, validator or result makes the smallest edit that removes it,
checks that the contract still passes the author's acceptance check, and
records each edit in `contracts-dataagentbench/<draw>/CORRECTIONS.md`; the fix
reaches every arm that uses the draw. Anything a hint item touches (how a
touched record is read, what a definition computes), including a slip in the
SQL that implements it, is the measurement and is never corrected. Draws A2,
A3 and no-data are not corrected. Recovery is scored on each draw as written;
a correction never changes a score.

The build's listing of a schema allowed with `tables: ["*"]`, table by table,
is not an edit.

## Recorded

`contracts-dataagentbench/RECOVERY.md` and `recovery.json`, per dataset, draw
and item: the class, the category, the text reading with its quote and where,
the SQL reading with where, the comparison counts (agree / total over the
touched records) or the computation check, the right part of a *partial*, any
correctable error with its cause and fix, and any note on the class. Per
dataset and draw, the additions. Per dataset, the references. The summary
counts each category per class and per variant (draw A as each draw and as
the mean of three, draw B, no-data), described items apart; every figure
reported is computed from `recovery.json` alone.
