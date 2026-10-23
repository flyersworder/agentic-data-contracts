"""Does the governed arm's SQL implement the metrics it looked up?

Exploratory, not predicted. The lookup log records which metric definitions
a run read. This asks whether the run's final SQL implements one of them,
and whether the answers that do not are the wrong ones.

Per run, against each metric `lookup_metric` returned:
- exact: the metric's `sql_expression` is a subtree of the final SQL, after
  both are parsed (DuckDB) and stripped of table aliases;
- columns: every column the expression reads appears in the final SQL, but
  the formula does not;
- none: neither.
A run takes its best status. Runs without a metric lookup, or without SQL,
are counted apart. The final SQL is the last `run_query` call.

Traces are not in the repository; they are released with the transcripts.

Run:  uv run python analysis/lookup_fidelity.py <dir of *-traces.tgz and results>
"""

from __future__ import annotations

import gzip
import json
import re
import sys
import tarfile
from collections import Counter
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dce.trace import _slug  # noqa: E402

GOVERNED = re.compile(r"^(contract_scaled_\w+|contract_gap)$")
SQL_TOOLS = ("run_query", "execute_sql")
RANK = ("exact", "columns", "none")


def _strip(tree: exp.Expression) -> exp.Expression:
    def drop(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column) and node.table:
            return exp.column(node.name)
        return node

    return tree.transform(drop)


def _parse(sql: str) -> exp.Expression | None:
    try:
        tree = sqlglot.parse_one(sql, dialect="duckdb")
    except ParseError:
        return None
    return _strip(tree) if tree is not None else None


def _flat(sql: str) -> str:
    return " ".join(sql.lower().split())


def uses(expression: str, sql: str) -> str:
    """How `sql` uses a metric's `expression`: exact, columns or none."""
    e, s = _parse(expression), _parse(sql)
    if e is None or s is None:
        return "exact" if _flat(expression) in _flat(sql) else "none"
    if any(node == e for node in s.walk()):
        return "exact"
    need = {c.name for c in e.find_all(exp.Column)}
    have = {c.name for c in s.find_all(exp.Column)}
    return "columns" if need and need <= have else "none"


def _parts(trace: list[dict]):
    for message in trace:
        yield from message.get("parts", [])


def _json(value) -> dict | None:
    if isinstance(value, dict):
        return value
    try:
        out = json.loads(value)
    except (TypeError, ValueError):
        return None
    return out if isinstance(out, dict) else None


def looked_up(trace: list[dict]) -> dict[str, str]:
    """name -> sql_expression for every metric a lookup returned."""
    out = {}
    for part in _parts(trace):
        if part.get("part_kind") != "tool-return":
            continue
        if part.get("tool_name") != "lookup_metric":
            continue
        body = _json(part.get("content"))
        if body and body.get("name") and body.get("sql_expression"):
            out[body["name"]] = body["sql_expression"]
    return out


def final_sql(trace: list[dict]) -> str | None:
    """The last SQL the run executed."""
    last = None
    for part in _parts(trace):
        if part.get("part_kind") == "tool-call" and part.get("tool_name") in SQL_TOOLS:
            args = _json(part.get("args"))
            if args and args.get("sql"):
                last = args["sql"]
    return last


def run_status(metrics: dict[str, str], sql: str | None) -> str:
    if not metrics:
        return "no lookup"
    if sql is None:
        return "no sql"
    found = {uses(e, sql) for e in metrics.values()}
    return next(r for r in RANK if r in found)


def _rows(results: Path) -> dict[tuple[str, str], dict]:
    """(task slug, arm) -> the latest row, as dce.stats.load keeps it; the
    slug is how a trace file names its task."""
    out = {}
    for line in results.read_text(encoding="utf-8").splitlines():
        r = json.loads(line)
        out[(_slug(str(r["task_id"])), r["arm"])] = r
    return out


def runs(archive: Path) -> list[dict]:
    """One record per governed run in a repeat's archive, with its verdict."""
    rows = _rows(archive.with_name(archive.name.replace("-traces.tgz", ".jsonl")))
    out = []
    with tarfile.open(archive) as tar:
        for member in tar:
            if not member.isfile() or not member.name.endswith(".json.gz"):
                continue
            task, arm, _ = Path(member.name).name.split("__")
            if not GOVERNED.match(arm):
                continue
            row = rows.get((task, arm))
            if row is None:
                continue
            handle = tar.extractfile(member)
            assert handle is not None
            trace = json.loads(gzip.decompress(handle.read()))
            metrics = looked_up(trace)
            out.append(
                {
                    "task": str(row["task_id"]),
                    "arm": arm,
                    "model": row["model"],
                    "correct": row["verdict"] == "correct",
                    "status": run_status(metrics, final_sql(trace)),
                    "metrics": sorted(metrics),
                }
            )
    return out


def main(directory: Path) -> None:
    order = ("exact", "columns", "none", "no sql", "no lookup")
    for experiment in ("exp4b", "exp6"):
        archives = sorted(directory.glob(f"**/{experiment}-*-r*-traces.tgz"))
        if not archives:
            continue
        records = [r for a in archives for r in runs(a)]
        print(f"== {experiment}: {len(records)} governed runs")
        for model in sorted({r["model"] for r in records}):
            rs = [r for r in records if r["model"] == model]
            n = Counter(r["status"] for r in rs)
            right = Counter(r["status"] for r in rs if r["correct"])
            cells = "  ".join(
                f"{s} {n[s]} ({right[s] / n[s]:.0%} right)" for s in order if n[s]
            )
            print(f"   {model:12s} {len(rs):4d}  {cells}")
        wrong = [r for r in records if not r["correct"]]
        print(
            f"   wrong answers by status: {dict(Counter(r['status'] for r in wrong))}"
        )
        for r in sorted(wrong, key=lambda r: (r["status"], r["task"])):
            print(
                f"      {r['status']:9s} {r['model']:12s} {r['task']:45s} "
                f"{r['metrics']}"
            )


if __name__ == "__main__":
    main(Path(sys.argv[1]))
