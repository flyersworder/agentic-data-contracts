# LiveSQLBench data preparation

These scripts build the private data directory `LSB_DATA` (default
`~/data/livesqlbench`) that `dce.benchmarks.livesqlbench` reads. Nothing they
read or write is committed: the gold SQL, the gold results, the test cases
and the DuckDB files stay under `LSB_DATA`.

Run them from the experiment directory, in this order:

| script | needs | reads | writes |
|---|---|---|---|
| `convert.py` | `LSB_PG_DSN` | the official PostgreSQL databases | `duckdb/<db>.duckdb` |
| `gold.py <public tasks .jsonl>` | `LSB_PG_DSN` | the gold SQL (`livesqlbench_base_full_v1_gt_kg_testcases_20260613.jsonl`) | `gold_results_full_v1.json` |
| `freeze.py` | sqlglot 30.19.0 | gold SQL and results, `prep/pg_only_gold.txt`, `prep/translations.json` | `prep/freeze_verdicts.json`, `tasks_frozen_full_v1.json.rebuilt` |
| `verify_translations.py` | | `prep/translations.json`, gold results | nothing |
| `check_grader.py` | sqlglot 30.19.0 | everything above | nothing |

`LSB_PG_DSN` is a libpq connection string for the benchmark's PostgreSQL
container (`host=... port=... user=... password=...`), without `dbname`.
It is read from the environment and never written down.

`freeze.py` runs every gold in DuckDB 20 times, under the grader's own
connection settings. A gold reproduced every time is `primary`. One that
reproduces every time only as a set is `order_only`. Anything else is
`excluded`. So is any gold that reads the clock (`CURRENT_DATE`, `NOW()`,
...), since its frozen result stops being right the next day. The 22
hand-translated golds (`translations.json`) are `primary`, and the rest of
`pg_only_gold.txt` is excluded. It writes `.rebuilt` beside the frozen file
and never over it; adopting a rebuild is a deliberate copy.

Expected:
- `freeze.py`: 302 primary, 73 order-only and 36 excluded, identical to `tasks_frozen_full_v1.json`.
- `verify_translations.py`: 22/22 ok entries verified.
- `check_grader.py`: `0 of 375 gold answers not graded correct`. This is the gate before any agent run.

Run lines are in each script's docstring, for example
`LSB_DATA=... uv run --with sqlglot==30.19.0 python prep/livesqlbench/check_grader.py`.
