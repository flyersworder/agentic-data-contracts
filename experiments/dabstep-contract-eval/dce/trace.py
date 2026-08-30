"""Full per-run transcripts — the record that makes a failure diagnosable.

WHY THIS EXISTS. A result row preserves the tool-call *names* and counts and
nothing else. That is enough to see THAT a run failed and roughly how much it
explored, and not remotely enough to see WHY. Worked example from the first
clean smoke run, task 1480, arm `contract`:

    tool_calls: ['lookup_domain','preview_table','lookup_metric',
                 'run_query','run_query']
    answer: 156, 159, 298, 424, ...     gold: 5, 9, 20, 30, ...
    inspect_rejections: 0   enforcement_blocks: 0

All three arms returned that same wrong answer. The plausible cause was
DABStep's null-wildcard fee rule — but the frozen contract STATES that rule
("if a field of a fee rule is set to null it means the rule applies to all
possible values of that field"), and arm C failed anyway with nothing blocked.
So either the agent never retrieved the rule, or it retrieved it and ignored
it. Those have completely different fixes — one is a retrieval problem in the
library, the other a reasoning failure in the model — and the recorded row
cannot tell them apart. Neither can a re-run: sampling is not deterministic
here (see `dce.agent`'s note on `temperature`).

A trace holds the tool ARGUMENTS and the tool RETURNS, so both questions are
answerable from disk: which metric was looked up, what SQL was written, what
came back, what the contract said when it blocked something.

WHAT THIS IS NOT. Traces are diagnostic evidence, not results. The accuracy
and money claims are made from `results/*.jsonl`, which stays small and in git;
traces are large and git-ignored by default (see `traces/README.md`). Nothing
in the analysis path reads them.

FAILURE POLICY. Writing a trace must never cost a paid row. Every entry point
here swallows its own exceptions and returns `None`: a full disk, an
unserialisable part, a permission error — all of them lose the trace and keep
the row. That asymmetry is deliberate and is the same reasoning as
`dce.agent`'s guarded tail: the model call is already paid for.
"""

from __future__ import annotations

import gzip
import json
import re
from pathlib import Path

#: Cap on one serialised transcript before gzip. `dce.arms.MAX_ROWS` (50)
#: already bounds the biggest single tool return, so this is a backstop
#: against a pathological run rather than a routine truncation — it fires,
#: if ever, on the runs least worth reading in full anyway. A truncated trace
#: is marked, never silently short.
MAX_TRACE_BYTES: int = 8_000_000

_SLUG = re.compile(r"[^A-Za-z0-9._-]+")


def _slug(value: str) -> str:
    """Filesystem-safe token. Model ids carry `/`, which would otherwise
    silently create directories (`z-ai/glm-5.3-flash` -> a `z-ai` dir)."""
    return _SLUG.sub("-", str(value)).strip("-") or "unknown"


def trace_name(task_id: str, arm: str, model: str) -> str:
    """The (task, arm, model) triple is the same key `dce.runner` resumes on,
    so a trace is findable from a result row without an index."""
    return f"{_slug(task_id)}__{_slug(arm)}__{_slug(model)}.json.gz"


def write_trace(
    trace_dir: Path | str | None,
    *,
    task_id: str,
    arm: str,
    model: str,
    messages: list,
) -> str | None:
    """Persist one run's transcript. Returns a path relative to `trace_dir`'s
    parent for stamping on the result row, or `None` if anything at all went
    wrong (including `trace_dir=None`, meaning traces are switched off).
    """
    if trace_dir is None:
        return None
    try:
        from pydantic_ai.messages import ModelMessagesTypeAdapter

        directory = Path(trace_dir)
        directory.mkdir(parents=True, exist_ok=True)
        try:
            payload = ModelMessagesTypeAdapter.dump_json(messages)
        except Exception:
            # A transcript pydantic-ai cannot serialise is still worth
            # something: fall back to a readable shape rather than no trace.
            payload = json.dumps(
                {
                    "unserializable": True,
                    "repr": [repr(m)[:20_000] for m in messages],
                },
                ensure_ascii=False,
            ).encode()
        truncated = len(payload) > MAX_TRACE_BYTES
        if truncated:
            payload = payload[:MAX_TRACE_BYTES] + b'\n{"__truncated__": true}'
        path = directory / trace_name(task_id, arm, model)
        with gzip.open(path, "wb") as handle:
            handle.write(payload)
        return str(Path(directory.name) / path.name)
    except Exception:
        # See FAILURE POLICY above: losing a trace is acceptable, losing a
        # paid result row is not.
        return None


def read_trace(path: Path | str) -> list[dict]:
    """Load a trace back as plain JSON, for diagnosis.

    Deliberately returns dicts rather than pydantic-ai message objects: the
    point is to read what happened — the SQL an arm wrote, what a tool
    returned, what a block said — not to replay it.
    """
    with gzip.open(Path(path), "rb") as handle:
        return json.loads(handle.read())
