"""Experiment 4b's gap proposer (design: docs/superpowers/specs/
2026-10-19-experiment-4b-design.md, "Who chooses the gaps").

One session per database, of a different model family from the contract
author, sees what a data engineer would: the documentation (experiment 4's KB,
with R and C marked), the schema with its first rows, the column meanings and
read-only SQL. It never sees a question, a gold, the reference SQL or a
result. It proposes, ranked best first:

* G1: sentinels to replace the NULL that means "matches any value" in the
  rule table's text criteria;
* G2: quantities C reads, each to be stored in another form than the
  documentation states, silently, in a way the data reveals.

The menu is committed as proposed. The builder takes candidates in order and
replaces one that fails a check by the next; nothing else goes back.

    set -a; . ./.env; set +a
    UV_FROZEN=1 uv run python prep/procedures4b/propose.py --max-spend 20
"""

from __future__ import annotations

import argparse
import ast
import functools
import gzip
import json
import math
import os
import sys
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))

from dce.compile.inputs import CompileInputs  # noqa: E402

PROPOSER_MODEL = "gpt-6-sol"
REQUEST_LIMIT = 60
MAX_SUBMISSIONS = 4
LOG = "propose-log.jsonl.gz"
MENU = "menu.json"
PROC_DATA = Path(os.environ.get("PROC_DATA", Path.home() / "data" / "procedures"))

#: database -> (rule table, its text criteria). The numeric band bounds of
#: reverse_logistics are not criteria here: NULL there means "no limit".
RULE_TABLES: dict[str, tuple[str, tuple[str, ...]]] = {
    "reverse_logistics": (
        "return_charge_rules",
        (
            "customer_segment",
            "return_channel",
            "item_category",
            "primary_reason",
            "item_condition",
        ),
    ),
    "labor_certification_applications": (
        "review_schedule",
        (
            "visa_class",
            "wage_level",
            "h1b_dependent",
            "soc_major_group",
            "worksite_state",
            "monthly_volume_band",
        ),
    ),
    "solar_panel": (
        "warranty_settle_rules",
        ("maker_tag", "pnl_kind", "alrt_state", "warr_state", "cap_band", "vol_tier"),
    ),
}

#: What `dce.author`'s prompt tells the author to check in the data. A G2 of
#: one of these kinds is one the author was pointed at.
NAMED_BY_AUTHOR_PROMPT = (
    "how values are stored",
    "units",
    "NULLs",
    "JSON paths",
    "formats",
)

G2_KEYS = (
    "table",
    "column",
    "json_path",
    "stored",
    "recover",
    "kind",
    "named_by_author_prompt",
    "silent_because",
    "data_shows",
)

SYSTEM_PROMPT = """\
You are helping build a controlled test of what happens when a database's \
documentation and its data disagree. You see one database: its schema (with \
the first rows of each table), the column meanings and its documentation. \
Two parts of the documentation are marked: R, a procedure that matches \
records against the rule table `{table}`, and C, a set of formulas computed \
from existing columns. You can run read-only SQL with run_sql.

We will change how two things are stored, leave the documentation as it is, \
and see whether people and programs that read the documentation and look at \
the data can tell what changed. Propose candidates for both, ranked best \
first.

G1, a wildcard sentinel. In `{table}`, the text criteria {criteria} use NULL \
to mean "this rule matches any value of that field". We will replace every \
such NULL by one sentinel string. Propose at least four candidate sentinels. \
Each must never occur as a value of the record field its criterion is \
matched against, and should be something a real system might store.

G2, a silent encoding. Pick a quantity that C's formulas read from an \
existing column (or a field inside a JSON column), to be stored in a \
different form from the one the documentation and column meanings state. \
Each candidate must:
- be silent: read as documented, it gives a plausible but wrong number, not \
an error, an empty result or an obviously absurd value;
- be revealed by the changed data alone: the person who checks sees only \
the data after the change and the unchanged documentation, never the data \
as it is now. Something they can see must contradict the documented form: \
a range, unit or scale the documentation or column meanings state (for \
example a score documented as 1 to 10 whose stored values run 0 to 9), or a \
relation to another column that the change breaks. The evidence must pin \
down the exact transformation, so that a careful reader can recover every \
original value; data that only looks odd is not enough. Quote that \
evidence, from the documentation and from the data, in data_shows;
- be exactly invertible, given as two arithmetic expressions in v: `stored` \
computes the new stored value from the current one, `recover` computes the \
current value back from the stored one (numbers, v, + - * / and brackets \
only);
- not touch any column or JSON field that R's entries read, and not touch \
category labels or names.
Propose at least four, on at least two different quantities. At least two \
must be of a kind other than these, \
which another tool is told to check: {named}. For example an offset or a \
reversed scale.

Submit with submit_menu(menu): {{"g1": [{{"sentinel", "why"}}, ...], \
"g2": [{{{g2_keys}}}, ...]}}, where json_path is null for a plain column, \
kind names the kind of change, and named_by_author_prompt says whether that \
kind is one of the listed ones. A submission that fails a check comes back \
with the reasons; you have {submissions}. When one returns "Accepted.", \
reply "done".
"""

USER_PROMPT = """\
Database `{db}`. Rule table: `{table}`.

## Schema

{ddl}

## Column meanings

{meanings}

## Documentation ({n} entries; (R) and (C) mark the two parts)

{kb}
"""


def system_prompt(db: str) -> str:
    table, criteria = RULE_TABLES[db]
    return SYSTEM_PROMPT.format(
        table=table,
        criteria=", ".join(criteria),
        named=", ".join(NAMED_BY_AUTHOR_PROMPT),
        g2_keys=", ".join(G2_KEYS),
        submissions=MAX_SUBMISSIONS,
    )


def user_prompt(inputs: CompileInputs, *, r_ids: set[int], c_ids: set[int]) -> str:
    table, _ = RULE_TABLES.get(inputs.db_name, ("return_charge_rules", ()))

    def tag(i: int) -> str:
        return " (R)" if i in r_ids else " (C)" if i in c_ids else ""

    return USER_PROMPT.format(
        db=inputs.db_name,
        table=table,
        ddl=inputs.ddl,
        meanings=inputs.column_meanings or "(none)",
        n=len(inputs.entries),
        kb="\n".join(
            f"[{e.id}]{tag(e.id)} {e.name}: {' '.join(e.text.split())}"
            for e in inputs.entries
        ),
    )


_ALLOWED = (ast.Expression, ast.BinOp, ast.UnaryOp, ast.Constant, ast.Name, ast.Load)
_OPS = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.USub, ast.UAdd)


def _check_expr(expr: str) -> ast.Expression:
    tree = ast.parse(str(expr), mode="eval")
    for node in ast.walk(tree):
        if isinstance(node, ast.operator | ast.unaryop):
            if not isinstance(node, _OPS):
                raise ValueError(expr)
        elif not isinstance(node, _ALLOWED):
            raise ValueError(expr)
        elif isinstance(node, ast.Name) and node.id != "v":
            raise ValueError(expr)
        elif isinstance(node, ast.Constant) and not isinstance(node.value, int | float):
            raise ValueError(expr)
    return tree


def evaluate(expr: str, v: float) -> float:
    """`expr`, arithmetic in v only, at v."""

    def run(node: ast.AST) -> float:
        if isinstance(node, ast.Expression):
            return run(node.body)
        if isinstance(node, ast.Constant):
            return float(node.value)  # type: ignore[arg-type]
        if isinstance(node, ast.Name):
            return v
        if isinstance(node, ast.UnaryOp):
            x = run(node.operand)
            return -x if isinstance(node.op, ast.USub) else x
        assert isinstance(node, ast.BinOp)
        a, b = run(node.left), run(node.right)
        if isinstance(node.op, ast.Add):
            return a + b
        if isinstance(node.op, ast.Sub):
            return a - b
        if isinstance(node.op, ast.Mult):
            return a * b
        return a / b if b else math.inf

    return run(_check_expr(expr))


def validate_menu(menu) -> list[str]:
    """Why `menu` cannot be accepted; empty when it can."""
    if not isinstance(menu, dict):
        return ["the menu must be an object with g1 and g2"]
    problems: list[str] = []
    g1 = menu.get("g1") or []
    sentinels = [str(c.get("sentinel")) for c in g1 if isinstance(c, dict)]
    if len(sentinels) < 4:
        problems.append("g1 needs at least four candidate sentinels")
    elif len(set(sentinels)) != len(sentinels):
        problems.append("g1 sentinels must be distinct")
    g2 = menu.get("g2") or []
    if len(g2) < 4:
        problems.append("g2 needs at least four candidates")
    unnamed = 0
    for i, cand in enumerate(g2):
        if not isinstance(cand, dict):
            problems.append(f"g2[{i}] must be an object")
            continue
        missing = [k for k in G2_KEYS if k not in cand]
        if missing:
            problems.append(f"g2[{i}] lacks {', '.join(missing)}")
            continue
        unnamed += cand["named_by_author_prompt"] is False
        try:
            for x in (1.0, 3.0, 7.5, 0.3, 100.0):
                back = evaluate(cand["recover"], evaluate(cand["stored"], x))
                if not math.isclose(back, x, rel_tol=1e-12, abs_tol=1e-12):
                    problems.append(f"g2[{i}]: recover does not invert stored")
                    break
        except (ValueError, SyntaxError):
            problems.append(
                f"g2[{i}]: stored and recover must be arithmetic in v "
                "(numbers, v, + - * / and brackets only)"
            )
    if g2 and unnamed < 2:
        problems.append(
            "g2 needs at least two candidates of a kind the author prompt does not name"
        )
    return problems


class _Session:
    def __init__(self, db) -> None:
        self.db = db
        self.menu: dict | None = None
        self.submissions = 0
        self.errors: list[str] = []
        self.queries: list[dict] = []
        self._lock = threading.Lock()

    def tools(self):
        def run_sql(sql: str) -> str:
            """Run a read-only SQL query on the database; at most 50 rows come back."""
            out = self.db.run_sql(sql)
            self.queries.append({"sql": sql, "error": out.startswith("ERROR:")})
            return out

        def submit_menu(menu: dict) -> str:
            """Submit the ranked menu: {"g1": [...], "g2": [...]}."""
            with self._lock:
                if self.menu is not None:
                    return "Already accepted."
                if self.submissions >= MAX_SUBMISSIONS:
                    return "No submissions left. Stop."
                self.submissions += 1
                problems = validate_menu(menu)
                if not problems:
                    self.menu = menu
                    return "Accepted."
                text = "\n".join(f"- {p}" for p in problems)
                self.errors.append(text)
                left = MAX_SUBMISSIONS - self.submissions
                return f"ERROR:\n{text}\nFix these and submit again ({left} left)."

        return [run_sql, submit_menu]


def pieces(db: str) -> tuple[set[int], set[int]]:
    entries = json.loads(
        (ROOT / "prep" / "procedures" / db / "knowledge.json").read_text(
            encoding="utf-8"
        )
    )
    return (
        {e["id"] for e in entries if e["piece"] == "R"},
        {e["id"] for e in entries if e["piece"] == "C"},
    )


def propose(db: str, driver, out_dir: Path) -> dict:
    from dce.agent import _commit_sha
    from dce.benchmarks.livesqlbench import compile_inputs
    from dce.compile.db import CompileDB

    inputs = compile_inputs(PROC_DATA, db)
    r_ids, c_ids = pieces(db)
    con = CompileDB(inputs.db_path, init_sql=inputs.init_sql)
    try:
        session = _Session(con)
        result = driver(
            system_prompt=system_prompt(db),
            user_prompt=user_prompt(inputs, r_ids=r_ids, c_ids=c_ids),
            tools=session.tools(),
        )
    finally:
        con.close()
    out_dir.mkdir(parents=True, exist_ok=True)
    if session.menu is not None:
        (out_dir / MENU).write_text(
            json.dumps(session.menu, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    lines = [
        {
            "type": "header",
            "db": db,
            "model": PROPOSER_MODEL,
            "commit_sha": _commit_sha(),
        },
        {
            "type": "session",
            "accepted": session.menu is not None,
            "submissions": session.submissions,
            "errors": session.errors,
            "queries": session.queries,
            "usage": result.usage,
            "messages": result.messages,
        },
    ]
    (out_dir / LOG).write_bytes(
        gzip.compress(
            "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in lines).encode(
                "utf-8"
            ),
            mtime=0,
        )
    )
    return result.usage


def main() -> None:
    from dce.compile import run as run_module

    parser = argparse.ArgumentParser()
    parser.add_argument("--db", nargs="+", default=sorted(RULE_TABLES))
    parser.add_argument("--max-spend", type=float, required=True)
    args = parser.parse_args()
    driver = functools.partial(
        run_module.claude_driver,
        model=PROPOSER_MODEL,
        request_limit=REQUEST_LIMIT,
    )
    spent = 0.0
    for db in args.db:
        out = HERE / db
        if (out / LOG).exists():
            print(f"{db}: already proposed; delete {out / LOG} to propose again")
            continue
        if spent >= args.max_spend:
            raise SystemExit(f"stopped at the ${args.max_spend:.2f} cap")
        usage = propose(db, driver, out)
        spent += usage.get("usd") or 0.0
        print(
            f"{db}: {'menu' if (out / MENU).exists() else 'NO MENU'} "
            f"(spent ${spent:.2f})"
        )


if __name__ == "__main__":
    main()
