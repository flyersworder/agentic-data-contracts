# Traces

One gzipped transcript per `(task, arm, model)` — every model turn, every tool
call **with its arguments**, and every tool return.

## Why they are here

A result row records the tool-call *names* and counts. That answers "did it
fail" and "how much did it explore", and nothing else. Diagnosis needs the SQL
the arm actually wrote, the metric it actually looked up, and what the contract
said when it blocked something. See `dce/trace.py` for the worked example
(task 1480) where three arms returned the same wrong answer and the rows could
not distinguish "never retrieved the rule" from "retrieved it and ignored it".

## Why they are git-ignored

Traces are diagnostic evidence, not results. The accuracy and money claims are
made from `results/*.jsonl`, which stays small, readable, and in git — that is
what the tamper-evidence argument rests on. Transcripts run to hundreds of MB
across a full sweep and would drown it.

Commit one deliberately when it is the evidence for a claim (`git add -f`), and
say in the text why that trace is being shown.

## Where the full sets are

All four sweeps (runs A–D, 1,604 files each) are attached to the GitHub
release tagged `dabstep-eval-2026-09`, one `traces-<run>-full.tar.gz` per run
with SHA-256 digests in the release notes. Unpack them here to reproduce the
post-hoc analyses. Run D's `provider_url` field reads `litellm-gateway`; it
originally named the enterprise gateway host and nothing else was altered.

The later runs are attached to the release tagged `dabstep-eval-2026-10`, one archive per run, with the same digests. These are the panels repeated three times, the arms added to them, the compiled contracts and the constructed-question experiments. The release notes cover the details.

- **Coverage:** 57,626 of those runs' 57,834 rows have a transcript.
- **Missing:** the 208 rows without one are the first rows of four runs: `exp4-sol-r3`, `exp6-qwen-r1`, `exp6-sol-r1` and `exp6-luna-r1`. Those cluster jobs kept transcripts in the pod's temporary storage and rows in persistent storage, so a pod restart kept the rows and lost the transcripts written before it. The release's `MISSING.tsv` lists the 208 rows. Cluster jobs should write `--traces` to persistent storage.
- **Retries:** where a unit was retried after a harness error, the transcript is of the final attempt, which is the row the analyses keep.
- **LiveSQLBench:** transcripts are not released; see the experiment README, *What is released, and what is not*.

## Reading one

```python
from dce.trace import read_trace

for message in read_trace("traces/smoke12/1480__contract__z-ai-glm-5.3-flash.json.gz"):
    for part in message["parts"]:
        if part["part_kind"] == "tool-call":
            print(part["tool_name"], part["args"])
        elif part["part_kind"] == "tool-return":
            print("  ->", str(part["content"])[:200])
```
