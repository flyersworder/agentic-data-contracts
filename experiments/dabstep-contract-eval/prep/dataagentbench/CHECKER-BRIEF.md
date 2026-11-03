# Experiment 7: the recovery checker's brief

Given to each dataset's checker. The rubric it applies is
`prep/dataagentbench/RECOVERY-RUBRIC.md`; read it first and in full. One
checker per dataset, in two phases with a commit between them: phase 1
writes the references without opening any draw; the references are
committed; phase 2 scores the five draws against them.

Paths below are relative to `experiments/dabstep-contract-eval/` unless
absolute. `<D>` is the dataset name.

## What you may read

- The rubric, this brief, and `contracts-dataagentbench/recovery-work/<D>.items.json`
  (the dataset's hint items, each with the class it was given: `cls`).
- `~/data/dataagentbench/src/query_<D>/db_description.txt` and nothing else
  in that folder.
- The built file `~/data/dataagentbench/duckdb/<D>.duckdb`, read-only.
- Phase 2 only: `contracts-dataagentbench/{draw-a1,draw-a2,draw-a3,draw-b,no-data}/<D>/`
  (`contract.yml`, `semantic.yml`, `author-log.jsonl.gz`).

## What you must never open

Any `query*/` folder (its `query.json`, `ground_truth.csv`, `validate.py`),
`db_description_withhint.txt`, anything under `results/`, any other
dataset's draws, and any file not listed above. Do not search the web for
DataAgentBench. If a file you need is not listed, stop and say so in your
report instead of opening it.

## Querying the data

From `experiments/dabstep-contract-eval/`:

```
UV_FROZEN=1 uv run python - <<'PY'
import duckdb
con = duckdb.connect("/home/yeqi/data/dataagentbench/duckdb/<D>.duckdb", read_only=True)
con.execute("SET memory_limit='1GB'; SET threads=2")
print(con.sql("...").fetchall()[:20])
PY
```

The machine has 7 GB shared with other checkers: keep the memory limit,
never `fetchall()` a large table, aggregate in SQL.

## What is committed, and what is not

DataAgentBench data is not ours to publish. Committed files hold queries,
counts, readings in words, and short quotes of the hints and of our
contracts; never rows, values lists or record dumps from the data beyond a
few illustrative values. Record-level outputs (a reference's record set, a
per-record comparison) go to `~/data/dce-held-results/exp7/recovery/<D>/`,
which is not committed.

## Phase 1: the references

Write `contracts-dataagentbench/recovery-work/<D>.references.json`:

```json
{"dataset": "<D>",
 "items": [
  {"id": "<D>-1", "cls": "data-discoverable", "text": "...",
   "kind": "records",
   "record_query": "SQL returning every record the item touches, with its key",
   "count": 123,
   "reading": "how the item reads each touched record, in words",
   "reference_sql": "SQL computing the item's reading per record (the reference)",
   "held_file": "~/data/dce-held-results/exp7/recovery/<D>/<D>-1.csv or null",
   "notes": ""},
  {"id": "<D>-2", "cls": "definition", "text": "...",
   "kind": "statement",
   "equivalent": "what a contract must state to be equivalent",
   "computation": true,
   "reference_sql": "the checker's SQL for the computation, or null",
   "reference_value": "its result in summary, or null",
   "notes": ""}
 ]}
```

`kind` is `records` for data-discoverable items and `statement` for the
other classes. Run every SQL you record and check the counts. Use the class
as given; a doubt about it goes in `notes`. Then stop and report: one line
per item, and anything you could not settle.

## Phase 2: scoring

Only after you are told the references are committed. Do not change the
references file; if one turns out wrong, say so in your report and in the
item's `notes` in the scores file, and score against it as committed.

Write `contracts-dataagentbench/recovery-work/<D>.scores.json`:

```json
{"dataset": "<D>",
 "draws": {
  "draw-a1": {
   "accepted_contract": true,
   "items": [
    {"id": "<D>-1", "cls": "data-discoverable",
     "category": "recovered | partial | stated, not built | missed",
     "text_reading": {"quote": "...", "where": "semantic.yml metrics.x.description"},
     "sql_reading": {"where": "semantic.yml metrics.x.sql", "summary": "..."},
     "agree": 120, "total": 123,
     "computation_check": null,
     "right_part": null,
     "class_note": null,
     "notes": ""}],
   "additions": [
    {"quote": "...", "where": "...", "confirmed": "yes | no | cannot be checked"}],
   "correctable_errors": [
    {"cause": "...", "where": "...", "smallest_fix": "..."}]
  },
  "draw-a2": {}, "draw-a3": {}, "draw-b": {}, "no-data": {}
 }}
```

`text_reading` is null when the contract says nothing on the item; `agree`
and `total` are null for statement items, which use `computation_check`.
Score every draw as written; a correctable error never changes a score.
The no-data control has no data access and is scored the same way. Name
correctable errors in every draw (the rubric's definition: a cause outside
every hint item); fixes are made later, by someone else, in A1 and B only.

End with a report: per draw, the category of each item in one line, the
correctable errors, and anything the rubric did not settle and how you read
it.
