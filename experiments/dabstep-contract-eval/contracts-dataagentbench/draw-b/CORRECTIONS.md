# Experiment 7: corrections to draw B

One correctable error, found by the recovery checkers (`../RECOVERY.md`) and
fixed by a reviewer who saw no question, gold, validator or result
(`../../prep/dataagentbench/RECOVERY-RUBRIC.md`, "Correctable errors"). The
fix reaches every arm that uses draw B (`dab_hints_contract`). Recovery is
scored on draw B as written; this correction changes no score.

## GITHUB_REPOS: metric `commit_author_name`

**Cause.** The metric reads the author's name from the `author` JSON of
`artifacts_database.commits`, with a regex fallback for the 2 rows whose
`author` is not valid JSON (their names hold over-escaped quotes, `\\"`). The
fallback's pattern treats `\\` as one escaped backslash and the quote after it
as the end of the string, so on both rows it stops part-way through the name
(a 6- or 8-character prefix in place of the full name).

**Fix.** The greedy pattern draws A1 and A2 use, which takes everything up to
the last `",` on the name's line (`.` does not cross the newline). Only the
`ELSE` branch changes; valid rows are untouched.

Before:

```sql
CASE WHEN json_valid(author) THEN json_extract_string(author, '$.name') ELSE regexp_extract(author, '"name": "((?:[^"\\]|\\.)*)"', 1) END
```

After:

```sql
CASE WHEN json_valid(author) THEN json_extract_string(author, '$.name') ELSE regexp_extract(author, '"name": "(.*)",', 1) END
```

As with draw A2's fallback, the backslashes stored in these 2 values stay in
the result; the fix only stops the cut.

**Check** (on `GITHUB_REPOS.duckdb`, read-only, aggregated in SQL). Reference:
the name field's whole text on its line, `regexp_extract(author, '\n  "name":
"([^\n]*)",\n', 1)`.

| rows | expression | equals the full name field | captured length vs field |
|---|---|---|---|
| 2 malformed | before | 0 of 2 | 6 vs 25, 8 vs 22 (cut short) |
| 2 malformed | after | 2 of 2 | 25 vs 25, 22 vs 22 |
| 17,974 valid | after | 17,974 of 17,974 equal `json_extract_string(author, '$.name')` | unchanged |

The greedy form does not overshoot: on neither row does the capture reach
the following `"time_sec"` key (or any other key).

**Acceptance.** `dce.author.check_submission` on draw B's GITHUB_REPOS
`contract.yml` and the edited `semantic.yml` against the built file, with the
schemas `dce.benchmarks.dab_author` gives it (every non-`main` schema of the
file, each table to allow), returns no reasons.
