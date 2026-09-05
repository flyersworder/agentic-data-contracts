"""Did the agent report what its own query returned?

Every accuracy in `FINDINGS.md` is answer-level, and every diagnostic built on
top of it is too: `analysis/counterfactuals.py` matches a wrong ANSWER against
the numbers a lesioned macro produces, and `analysis/clauses.py` reads the SQL
but only checks which clauses appear in it. Nothing so far executes the
agent's own query and compares the result to what the agent said.

That leaves one failure mode unmeasured and, in this document, silently assumed
to be zero: the query was right and the answer was mis-read off it — wrong
cell, dropped row, a value transcribed from the wrong column. It matters
because the counterfactual table leaves **444 wrong answers of which 417 are
undiagnosed**, read there as agents "failing idiosyncratically". That reading is
an inference from a lesion catalogue finding no match. A row whose SQL replays
to the reported answer is a genuine derivation error and confirms it; a row
whose SQL does NOT is something else, and the lesion method is structurally
blind to the difference.

THE MEASUREMENT. For each row: take the statements the agent submitted, replay
the last one that executes, render the result the way an answer is written, and
score it against the answer the agent actually reported — using the vendored
official scorer, so "agrees" means what it means everywhere else here.

WHY CORRECT ROWS ARE THE CONTROL, AND NOT AN ASIDE. A final query need not
contain the whole answer: an agent may run an aggregate and then do arithmetic
in its head, in which case a non-replay is normal reasoning rather than a
misreport. There is no way to tell those apart row by row, so the baseline is
the replay rate among rows that were CORRECT — those, by construction, reported
the right value, so whatever share of them fails to replay is the method's
false-alarm rate. Only the gap between the wrong-row rate and that baseline
carries information, and the script prints both. Read a single arm's
`wrong` column alone and you will over-read it.

SCOPE. Only the four ablation runs have transcripts. `results/glm-all450.jsonl`
— the leaderboard submission, and the only file with official verdicts — was
run without them, so none of this reaches the 19 grader disagreements.

Run:  uv run python analysis/replay.py [results/*.jsonl ...]

No API calls. Attaches `data/dabstep.duckdb` READ_ONLY; agent SQL is executed
against a throwaway in-memory database with views over it, so a stray DDL in a
transcript cannot touch the frozen file.
"""

from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from clauses import trace_for  # noqa: E402
from coverage import BASE_TABLES, DB  # noqa: E402
from dce.arms import MAX_ROWS  # noqa: E402
from dce.stats import ANSWER_VERDICTS, load  # noqa: E402
from vendor.dabstep_scorer import question_scorer  # noqa: E402

ARMS = ["contract", "contract_hollow", "manual_prompt", "schema_only"]

#: Tools whose RESULT SET the agent actually receives. Deliberately NOT
#: `clauses.SQL_TOOLS`, which also contains `inspect_query`: that tool returns
#: a validation verdict (`{"valid": false, "violations": [...]}`) and never
#: rows, so a value found by replaying its SQL was never in front of the model.
#: Counting it would also be one-sided — only the governed arms have the tool,
#: so it would inflate `contract` and `contract_hollow` against the two arms
#: that cannot produce such a call at all.
ANSWERING_TOOLS = frozenset({"run_query", "execute_sql"})

#: A transcript can hold dozens of statements; only the tail is plausibly the
#: one that produced the answer, and replaying all of them is slow for nothing.
TAIL = 8


def render(rows: list) -> str:
    """A result set, written the way an answer is written.

    Flattened row-major: a single scalar is itself, anything wider or longer is
    the comma-separated list the benchmark's guidelines ask for. `None` becomes
    an empty cell rather than the string "None", which would otherwise match a
    gold of "Not Applicable" by accident.
    """
    cells = [
        "" if value is None else str(value) for row in rows for value in tuple(row)
    ]
    return ", ".join(cells)


def cells(rows: list) -> list[str]:
    """The individual values in a result set.

    A reported answer is almost always a PROJECTION of what the query returned,
    not the whole of it — a query answering "which scheme is cheapest" returns
    `('GlobalCard', 0.329)` and the answer is `GlobalCard`; one answering
    "what is the fee" returns three intermediates and the answer is the last.
    Whole-result equality scores all of those as non-replays, which is what put
    the control at 30-76% and made the measurement useless. Membership is the
    honest test of "did the agent report a value its own query produced".
    """
    return [str(v) for row in rows for v in tuple(row) if v is not None]


def sql_statements(path: Path) -> list[str]:
    """Every SQL string the agent submitted, in order.

    `clauses.sql_of` concatenates these for text matching; replaying needs them
    separable, and needs the malformed ones dropped rather than kept — an
    argument blob that would not parse as JSON will not parse as SQL either.
    """
    out: list[str] = []
    for message in json.load(gzip.open(path)):
        for part in message.get("parts", []):
            if part.get("part_kind") != "tool-call":
                continue
            if part.get("tool_name") not in ANSWERING_TOOLS:
                continue
            args = part.get("args")
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except json.JSONDecodeError:
                    continue
            if isinstance(args, dict) and str(args.get("sql", "")).strip():
                out.append(str(args["sql"]))
    return out


def connect() -> duckdb.DuckDBPyConnection:
    """Views over the frozen database, in a throwaway in-memory catalogue."""
    con = duckdb.connect()
    con.execute(f"ATTACH '{DB}' AS src (READ_ONLY)")
    for table in BASE_TABLES:
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM src.{table}")
    return con


def replay(con: duckdb.DuckDBPyConnection, statements: list[str]) -> list[str]:
    """Candidate answers the last few statements produced, newest first.

    Each statement contributes its whole rendered result AND each individual
    cell, because a reported answer may be either.

    Truncated to `dce.arms.MAX_ROWS`, because that is all the harness ever
    showed the agent: rows past the cap were never in front of the model, so
    counting them is the same error as replaying `inspect_query`. It also
    keeps the candidate pool from growing with the query, which matters
    because pool size is not independent of the verdict --- wrong rows tend to
    run wider queries, and an uncapped pool would give them more chances to
    match by accident than the control rows it is compared against.

    Not just the final statement. Agents routinely compute the answer and then
    run a sanity check after it, so "the last query that executes" is the wrong
    unit — measured on correct rows, taking only the last one put the control
    at 17-65%, which is too weak a baseline for a non-match to mean anything.
    Asking whether ANY recent query returned what was reported is both the
    fairer question and the one with a usable control.
    """
    out: list[str] = []
    for sql in reversed(statements[-TAIL:]):
        try:
            rows = con.execute(sql).fetchall()[:MAX_ROWS]
        except Exception:
            continue
        out.append(render(rows))
        out.extend(cells(rows))
    return out


def main(paths: list[str]) -> None:
    for path in paths:
        rows = [r for r in load(Path(path)) if r.get("verdict") in ANSWER_VERDICTS]
        if not rows:
            continue
        run = ROOT / "traces" / Path(path).stem
        if not run.is_dir():
            continue
        print(f"\n{path}")
        print(
            f"  {'arm':<17}{'correct':>9}{'replays':>9}"
            f"{'wrong':>9}{'replays':>9}{'gap':>8}"
        )
        for arm in ARMS:
            tallies = {"correct": [0, 0], "incorrect": [0, 0]}
            for row in rows:
                if row["arm"] != arm:
                    continue
                trace = trace_for(run, str(row["task_id"]), arm)
                if trace is None:
                    continue
                # A fresh catalogue per row. Transcripts contain DDL
                # (`CREATE TEMP TABLE feats`, `DROP TABLE ... ; CREATE TABLE m1`)
                # and object names repeat across tasks, so a shared connection
                # lets one task's leftover table satisfy another task's query
                # whose own CREATE fell outside TAIL.
                con = connect()
                try:
                    results = replay(con, sql_statements(trace))
                finally:
                    con.close()
                if not results:
                    continue
                reported = row.get("answer") or ""
                tally = tallies[row["verdict"]]
                tally[0] += 1
                tally[1] += any(question_scorer(r, reported) for r in results)
            (ok_n, ok_hit), (bad_n, bad_hit) = tallies["correct"], tallies["incorrect"]
            if not ok_n and not bad_n:
                continue
            ok_rate = ok_hit / ok_n if ok_n else 0.0
            bad_rate = bad_hit / bad_n if bad_n else 0.0
            print(
                f"  {arm:<17}{ok_n:>9}{ok_rate:>8.0%}"
                f"{bad_n:>9}{bad_rate:>8.0%}{ok_rate - bad_rate:>+8.0%}"
            )
    print(
        "\n  correct/replays is the control: correct answers reported the right "
        "value,\n  so a non-replay there is the method's own false alarm. Only "
        "the gap counts."
    )


if __name__ == "__main__":
    args = sys.argv[1:] or [
        str(p) for p in sorted((ROOT / "results").glob("*-full.jsonl"))
    ]
    main(args)
