"""Experiment 4b's blind raters (design, "Who chooses the gaps", step 3).

Two raters of different model families, each alone, see one database's
gapped documentation, its schema with sample rows, the column meanings and
read-only SQL on the 4b database: no question, gold, reference or result.
For each gap location they give a structured reading. A data-settled gap
(G1, G2) counts as such when both reach the reference reading and call it
settled; G3 counts as convention-only when the raters split or call it
unsettled.

    set -a; . ./.env; set +a
    UV_FROZEN=1 uv run python prep/procedures4b/rate.py --max-spend 20
"""

from __future__ import annotations

import argparse
import functools
import gzip
import json
import math
import sys
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))

from propose import RULE_TABLES, evaluate  # noqa: E402

RATERS = ("gpt-6-sol", "claudeopus5.5")
REQUEST_LIMIT = 60
MAX_SUBMISSIONS = 3
G3_CONVENTIONS = ("most_specific", "lowest_rule_id", "sum_all", "other")

SYSTEM_PROMPT = """\
You are a careful data engineer reviewing one database: its documentation, \
its schema with the first rows of each table, the column meanings, and \
read-only SQL with run_sql. Documentation and data can disagree. For each \
item below, work out how the data should be read, from the documentation and \
from what the data itself shows, and say whether the two together settle it. \
Answer only from what you can see here.

Submit with submit_readings(readings): an object keyed by item id, each value \
an object with exactly the fields the item asks for. A submission that fails \
a check comes back with the reasons; you have {submissions}. When one returns \
"Accepted.", reply "done".
"""

USER_PROMPT = """\
Database `{db}`.

## Items

{items}

## Schema

{ddl}

## Column meanings

{meanings}

## Documentation ({n} entries)

{kb}
"""


def items(db: str, gaps: dict) -> list[dict]:
    table, criteria = RULE_TABLES.get(db, ("the rule table", ()))
    out = []
    if gaps.get("g1"):
        out.append(
            {
                "id": "g1",
                "gap": "g1",
                "ask": (
                    f"In {table}, the criteria columns ({', '.join(criteria)}) hold "
                    "the "
                    "conditions a rule places on a record. Which stored value, if "
                    "any, means "
                    "that a rule places no restriction on that field, and does such a "
                    "criterion match a record whose own value for that field is "
                    "missing "
                    "(NULL)? Fields: sentinel (the stored value, or null for SQL "
                    "NULL), "
                    "matches_missing (true, false or null), settled (true when the "
                    "documentation and data together settle both answers), evidence."
                ),
            }
        )
    if gaps.get("g2"):
        g2 = gaps["g2"]
        where = f"{g2['table']}.{g2['column']}" + (
            f" at {g2['json_path']}" if g2.get("json_path") else ""
        )
        out.append(
            {
                "id": "g2",
                "gap": "g2",
                "ask": (
                    f"Documentation entry [{g2['entry_id']}] uses the quantity stored "
                    "in "
                    f"{where}. Give the arithmetic expression in v, the stored value, "
                    "that "
                    'yields the quantity as the documentation defines it ("v" if it is '
                    "stored as documented), or null if the data cannot tell. Fields: "
                    "recover, settled, evidence."
                ),
            }
        )
    if gaps.get("g3_entries"):
        out.append(
            {
                "id": "g3",
                "gap": "g3",
                "ask": (
                    f"When a record matches several rules of {table}, which rule or "
                    "rules "
                    "set its charge? Fields: convention (one of "
                    f"{', '.join(G3_CONVENTIONS)}), "
                    "detail, settled (true when the documentation and data settle it), "
                    "evidence."
                ),
            }
        )
    return out


def matches_reference(gap: str, gaps: dict, reading: dict) -> bool:
    if not reading.get("settled"):
        return False
    if gap == "g1":
        return (
            reading.get("sentinel") == gaps["g1"]["sentinel"]
            and reading.get("matches_missing") is True
        )
    if gap == "g2":
        expr = reading.get("recover")
        if not isinstance(expr, str):
            return False
        try:
            return all(
                math.isclose(
                    evaluate(expr, x),
                    evaluate(gaps["g2"]["recover"], x),
                    rel_tol=1e-9,
                    abs_tol=1e-9,
                )
                for x in (0.0, 0.5, 1.0, 3.0, 9.0, 97.5)
            )
        except (ValueError, SyntaxError, ZeroDivisionError):
            return False
    raise ValueError(gap)


def classify(gap: str, gaps: dict, readings: list[dict]) -> str:
    if gap == "g3":
        conventions = {r.get("convention") for r in readings}
        if len(conventions) > 1 or not all(r.get("settled") for r in readings):
            return "convention-only"
        return "settled (not convention-only)"
    if all(matches_reference(gap, gaps, r) for r in readings):
        return "data-settled"
    return "not settled"


def check_readings(readings, wanted: list[dict]) -> list[str]:
    if not isinstance(readings, dict):
        return ["readings must be an object keyed by item id"]
    problems = []
    for item in wanted:
        r = readings.get(item["id"])
        if not isinstance(r, dict):
            problems.append(f"no reading for item {item['id']}")
            continue
        fields = {
            "g1": ("sentinel", "matches_missing", "settled", "evidence"),
            "g2": ("recover", "settled", "evidence"),
            "g3": ("convention", "detail", "settled", "evidence"),
        }[item["gap"]]
        missing = [f for f in fields if f not in r]
        if missing:
            problems.append(f"item {item['id']} lacks {', '.join(missing)}")
        if item["gap"] == "g3" and r.get("convention") not in G3_CONVENTIONS:
            problems.append(f"item g3: convention must be one of {G3_CONVENTIONS}")
    return problems


def rate_one(
    db: str, gaps: dict, model: str, driver, data: Path
) -> tuple[dict | None, dict]:
    from dce.benchmarks.livesqlbench import compile_inputs
    from dce.compile.db import CompileDB

    inputs = compile_inputs(data, db)
    wanted = items(db, gaps)
    state: dict = {"readings": None, "submissions": 0, "queries": [], "errors": []}
    lock = threading.Lock()
    con = CompileDB(inputs.db_path, init_sql=inputs.init_sql)

    def run_sql(sql: str) -> str:
        """Run a read-only SQL query on the database; at most 50 rows come back."""
        out = con.run_sql(sql)
        state["queries"].append({"sql": sql, "error": out.startswith("ERROR:")})
        return out

    def submit_readings(readings: dict) -> str:
        """Submit one reading per item, keyed by item id."""
        with lock:
            if state["readings"] is not None:
                return "Already accepted."
            if state["submissions"] >= MAX_SUBMISSIONS:
                return "No submissions left. Stop."
            state["submissions"] += 1
            problems = check_readings(readings, wanted)
            if not problems:
                state["readings"] = readings
                return "Accepted."
            text = "\n".join(f"- {p}" for p in problems)
            state["errors"].append(text)
            return f"ERROR:\n{text}"

    try:
        result = driver(
            system_prompt=SYSTEM_PROMPT.format(submissions=MAX_SUBMISSIONS),
            user_prompt=USER_PROMPT.format(
                db=db,
                items="\n\n".join(f"[{i['id']}] {i['ask']}" for i in wanted),
                ddl=inputs.ddl,
                meanings=inputs.column_meanings or "(none)",
                n=len(inputs.entries),
                kb="\n".join(
                    f"[{e.id}] {e.name}: {' '.join(e.text.split())}"
                    for e in inputs.entries
                ),
            ),
            tools=[run_sql, submit_readings],
        )
    finally:
        con.close()
    log = {
        **{k: v for k, v in state.items() if k != "readings"},
        "model": model,
        "usage": result.usage,
        "messages": result.messages,
    }
    return state["readings"], log


def main() -> None:
    from build import GAPS, PROC4B_DATA
    from dce.compile import run as run_module

    parser = argparse.ArgumentParser()
    parser.add_argument("--max-spend", type=float, required=True)
    args = parser.parse_args()
    gaps_all = json.loads(GAPS.read_text(encoding="utf-8"))
    out_path = HERE / "ratings.json"
    ratings = (
        json.loads(out_path.read_text(encoding="utf-8")) if out_path.exists() else {}
    )
    spent = 0.0
    for db, gaps in gaps_all.items():
        for model in RATERS:
            key = f"{db}|{model}"
            if key in ratings:
                continue
            if spent >= args.max_spend:
                raise SystemExit(f"stopped at the ${args.max_spend:.2f} cap")
            driver = functools.partial(
                run_module.claude_driver, model=model, request_limit=REQUEST_LIMIT
            )
            readings, log = rate_one(db, gaps, model, driver, PROC4B_DATA)
            spent += log["usage"].get("usd") or 0.0
            ratings[key] = readings
            logdir = HERE / db
            logdir.mkdir(exist_ok=True)
            (logdir / f"rate-log-{model}.jsonl.gz").write_bytes(
                gzip.compress(
                    (json.dumps(log, ensure_ascii=False) + "\n").encode("utf-8"),
                    mtime=0,
                )
            )
            out_path.write_text(
                json.dumps(ratings, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            print(f"{key}: {'readings' if readings else 'NONE'} (spent ${spent:.2f})")
    lines = ["# Experiment 4b: blind ratings", ""]
    for db, gaps in gaps_all.items():
        for item in items(db, gaps):
            readings = [ratings.get(f"{db}|{m}") or {} for m in RATERS]
            per = [r.get(item["id"]) or {} for r in readings]
            lines.append(
                f"- {db} {item['gap']}: {classify(item['gap'], gaps, per)}; "
                + "; ".join(
                    f"{m}: "
                    f"{json.dumps({k: v for k, v in p.items() if k != 'evidence'})}"
                    for m, p in zip(RATERS, per, strict=True)
                )
            )
    (HERE / "RATINGS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
