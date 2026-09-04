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
