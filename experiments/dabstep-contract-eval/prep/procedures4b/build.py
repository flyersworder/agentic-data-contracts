"""Experiment 4b's materials (design: docs/superpowers/specs/
2026-10-19-experiment-4b-design.md).

Starts from experiment 4's built data (PROC_DATA, default ~/data/procedures)
and the gaps chosen in gaps.json, and writes PROC4B_DATA (default
~/data/procedures-4b):

* duckdb/<db>.duckdb -- experiment 4's database with G1 (the rule table's
  wildcard NULLs in its text criteria replaced by a sentinel) and G2 (one
  quantity C reads stored in another form) applied;
* hf-full-v1/<db>/ -- the gapped documentation: experiment 4's KB (with G3's
  edits on reverse_logistics), the schema text with the changed tables'
  sample rows as they now read, the column meanings unchanged;
* full/kb/<db>_kb.jsonl -- the full documentation: experiment 4's KB plus a
  correction for each gap, for `manual_full`;
* the task files, experiment 4's, with reverse_logistics' R06 rewritten.

Every gap only changes how the data stores a fact, so decoding a 4b database
with the inverse transformation must give back experiment 4's content, and
every gold query run on the decoded copy must give experiment 4's gold:
`decode_check`.

    UV_FROZEN=1 uv run python prep/procedures4b/build.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))

from propose import RULE_TABLES, evaluate  # noqa: E402

PROC_DATA = Path(os.environ.get("PROC_DATA", Path.home() / "data" / "procedures"))
PROC4B_DATA = Path(
    os.environ.get("PROC4B_DATA", Path.home() / "data" / "procedures-4b")
)
GAPS = HERE / "gaps.json"
QUESTIONS = ROOT / "prep" / "procedures"


# -- G1 ---------------------------------------------------------------------


def rule_aliases(sql: str, table: str) -> set[str]:
    pattern = rf"(?:FROM|JOIN)\s+{re.escape(table)}\s+(?:AS\s+)?(\w+)"
    return set(re.findall(pattern, sql, flags=re.IGNORECASE))


def adapt_g1(sql: str, table: str, criteria: tuple[str, ...], sentinel: str) -> str:
    """`sql` with every wildcard test on the rule table's criteria reading the
    sentinel as the NULL it replaced."""
    s = sentinel.replace("'", "''")
    for alias in rule_aliases(sql, table):
        for col in criteria:
            ref = f"{alias}.{col}"
            sql = re.sub(
                rf"\b{re.escape(ref)} IS NOT NULL\b",
                f"({ref} IS NOT NULL AND {ref} <> '{s}')",
                sql,
            )
            sql = re.sub(
                rf"\b{re.escape(ref)} IS NULL\b",
                f"({ref} IS NULL OR {ref} = '{s}')",
                sql,
            )
    return sql


def apply_g1(con, table: str, criteria: tuple[str, ...], sentinel: str) -> None:
    for col in criteria:
        con.execute(
            f'UPDATE "{table}" SET "{col}" = ? WHERE "{col}" IS NULL', [sentinel]
        )


def decode_g1(con, table: str, criteria: tuple[str, ...], sentinel: str) -> None:
    for col in criteria:
        con.execute(
            f'UPDATE "{table}" SET "{col}" = NULL WHERE "{col}" = ?', [sentinel]
        )


# -- G2 ---------------------------------------------------------------------


def number(value: float, *, was_int: bool):
    if was_int and float(value).is_integer():
        return int(value)
    return round(value, 12)


def _leaf_path(json_path: str) -> list[str]:
    return [p for p in json_path.removeprefix("$.").split(".") if p]


def apply_g2(con, cand: dict, direction: str) -> None:
    """Rewrite one quantity: `direction` is "stored" (encode) or "recover"."""
    expr = cand[direction]
    table, column = cand["table"], cand["column"]
    if not cand.get("json_path"):
        con.execute(
            f'UPDATE "{table}" SET "{column}" = '
            + expr.replace("v", f'"{column}"')
            + f' WHERE "{column}" IS NOT NULL'
        )
        return
    path = _leaf_path(cand["json_path"])

    def rewrite(text: str | None) -> str | None:
        if text is None:
            return None
        doc = json.loads(text)
        node = doc
        for key in path[:-1]:
            node = node.get(key) if isinstance(node, dict) else None
            if node is None:
                return text
        leaf = node.get(path[-1]) if isinstance(node, dict) else None
        if not isinstance(leaf, int | float) or isinstance(leaf, bool):
            return text
        node[path[-1]] = number(
            evaluate(expr, float(leaf)), was_int=isinstance(leaf, int)
        )
        return json.dumps(doc, ensure_ascii=False)

    name = f"g2_{direction}"
    con.create_function(name, rewrite, ["VARCHAR"], "VARCHAR", null_handling="special")
    con.execute(
        f'UPDATE "{table}" SET "{column}" = {name}(CAST("{column}" AS VARCHAR))'
    )
    con.remove_function(name)


def transform_sample_text(text: str, cand: dict) -> str:
    """A sample row as the schema file shows it (a Python-dict repr), with the
    G2 leaf transformed as the data now stores it."""
    leaf = _leaf_path(cand["json_path"])[-1]

    def sub(m: re.Match) -> str:
        raw = m.group(2)
        value = evaluate(cand["stored"], float(raw))
        return m.group(1) + str(number(value, was_int="." not in raw))

    return re.sub(rf"('{re.escape(leaf)}': )(-?\d+(?:\.\d+)?)", sub, text)


# -- G3 ---------------------------------------------------------------------

_QUALIFY = re.compile(
    r"QUALIFY ROW_NUMBER\(\) OVER \(PARTITION BY (\w+) ORDER BY specificity DESC, "
    r"rule_id(?: ASC)?\) = 1"
)
_BEST = re.compile(
    r"best AS \(\s*SELECT \*\s*FROM matches\s*" + _QUALIFY.pattern + r"\s*\)"
)


def g3_variant(sql: str, convention: str) -> str:
    """`sql` with the precedence rule replaced: "lowest_id" (the lowest
    matching rule_id applies) or "sum" (every matching rule applies)."""
    if convention == "lowest_id":
        return _QUALIFY.sub(
            r"QUALIFY ROW_NUMBER() OVER (PARTITION BY \1 ORDER BY rule_id ASC) = 1", sql
        )
    if convention == "sum":
        return _BEST.sub(
            lambda m: (
                f"best AS (SELECT {m.group(1)}, MIN(rule_id) AS rule_id, "
                "MAX(specificity) AS specificity, SUM(flat_charge) AS flat_charge, "
                f"SUM(charge_pct) AS charge_pct FROM matches GROUP BY {m.group(1)})"
            ),
            sql,
        )
    raise ValueError(convention)


def rewrite_r06(question: dict) -> dict:
    """reverse_logistics' R06 asked which single rule applies; with G3 open it
    asks which rules match, a set that does not depend on precedence."""
    case = re.search(r"RL\d+", question["query"]).group(0)  # type: ignore[union-attr]
    head = question["gold_sql"].split("best AS (")[0].rstrip().rstrip(",")
    return {
        **question,
        "query": (
            f"Which rules of the Return Handling Charge Schedule match return {case}? "
            "Return one column, rule_id, in ascending order."
        ),
        "gold_sql": f"{head}\nSELECT rule_id FROM matches WHERE casenum = '{case}' "
        "ORDER BY rule_id",
        "order": "True",
        "note": "4b: the set of matching rules, independent of precedence (G3).",
    }


# -- sample rows of the rule table -------------------------------------------


def g1_sample_block(block: str, criteria: tuple[str, ...], sentinel: str) -> str:
    """The rule table's "First 3 rows" lines (header, dashes, rows) with the
    blank criterion cells holding the sentinel, re-aligned."""
    lines = block.splitlines()
    header, dashes, rows = lines[0], lines[1], lines[2:]
    spans = [(m.start(), m.end()) for m in re.finditer(r"-+", dashes)]
    names = [
        header[a : b + 2].strip() if i + 1 < len(spans) else header[a:].strip()
        for i, (a, b) in enumerate(spans)
    ]

    def cells(line: str) -> list[str]:
        out = []
        for i, (a, b) in enumerate(spans):
            end = spans[i + 1][0] if i + 1 < len(spans) else len(line)
            out.append(line[a:end].strip())
        return out

    table = [cells(r) for r in rows]
    for row in table:
        for i, name in enumerate(names):
            if name in criteria and row[i] == "":
                row[i] = sentinel
    numeric = [
        all(re.fullmatch(r"-?\d+(\.\d+)?", r[i]) for r in table if r[i])
        and any(r[i] for r in table)
        for i in range(len(names))
    ]
    widths = [
        max(len(names[i]), *(len(r[i]) for r in table)) for i in range(len(names))
    ]

    def line(values: list[str]) -> str:
        return "  ".join(
            v.rjust(widths[i]) if numeric[i] else v.ljust(widths[i])
            for i, v in enumerate(values)
        ).rstrip()

    return "\n".join(
        [line(names), "  ".join("-" * w for w in widths)] + [line(r) for r in table]
    )


def gapped_schema(schema: str, db: str, gaps: dict) -> str:
    table, criteria = RULE_TABLES[db]
    head, _, rest = schema.partition(f'CREATE TABLE "{table}"')
    block_start = rest.index("First 3 rows:\n") + len("First 3 rows:\n")
    block_end = rest.index("\n...", block_start)
    rest = (
        rest[:block_start]
        + g1_sample_block(rest[block_start:block_end], criteria, gaps["g1"]["sentinel"])
        + rest[block_end:]
    )
    out = head + f'CREATE TABLE "{table}"' + rest
    g2 = gaps.get("g2")
    if g2 and g2.get("json_path"):
        a, _, b = out.partition(f'CREATE TABLE "{g2["table"]}"')
        start = b.index("First 3 rows:\n")
        end = b.index("\n...", start)
        b = b[:start] + transform_sample_text(b[start:end], g2) + b[end:]
        out = a + f'CREATE TABLE "{g2["table"]}"' + b
    return out


# -- documentation -------------------------------------------------------------


def read_kb(db: str) -> list[dict]:
    path = PROC_DATA / "hf-full-v1" / db / f"{db}_kb.jsonl"
    return [
        json.loads(x)
        for x in path.read_text(encoding="utf-8").splitlines()
        if x.strip()
    ]


def write_kb(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8"
    )


def doc_edits(rows: list[dict], gaps: dict) -> list[dict]:
    """The wording both documentations share: the missing-value and
    specificity sentences stated in terms of a criterion that places no
    restriction, so that G1 asks only which stored value means that. Each
    old sentence must occur exactly once in its entry."""
    rows = [dict(r) for r in rows]
    by_id = {r["id"]: r for r in rows}
    for e in gaps.get("doc_edits") or []:
        row = by_id[int(e["entry_id"])]
        if row["definition"].count(e["old"]) != 1:
            raise SystemExit(
                f"entry {e['entry_id']}: edit does not match once: {e['old']!r}"
            )
        row["definition"] = row["definition"].replace(e["old"], e["new"])
    return rows


def gapped_kb(rows: list[dict], gaps: dict) -> list[dict]:
    """G1 and G2 leave the documentation as it is; G3 replaces entries."""
    rows = doc_edits(rows, gaps)
    edits = {int(k): v for k, v in (gaps.get("g3_entries") or {}).items()}
    return [{**r, **edits[r["id"]]} if r["id"] in edits else r for r in rows]


def full_kb(rows: list[dict], gaps: dict) -> list[dict]:
    """Experiment 4's KB, each gap's correction appended to its entry."""
    rows = doc_edits(rows, gaps)
    notes: dict[int, list[str]] = {}
    for key in ("g1", "g2"):
        gap = gaps.get(key)
        if gap:
            notes.setdefault(int(gap["entry_id"]), []).append(gap["full_note"])
    return [
        {**r, "definition": r["definition"] + " " + " ".join(notes[r["id"]])}
        if r["id"] in notes
        else r
        for r in rows
    ]


# -- building and the decode check -----------------------------------------------


def build_db(
    db: str, gaps: dict, out: Path, *, g1: bool = True, g2: bool = True
) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(PROC_DATA / "duckdb" / f"{db}.duckdb", out)
    con = duckdb.connect(str(out))
    try:
        table, criteria = RULE_TABLES[db]
        if g1 and gaps.get("g1"):
            apply_g1(con, table, criteria, gaps["g1"]["sentinel"])
        if g2 and gaps.get("g2"):
            apply_g2(con, gaps["g2"], "stored")
    finally:
        con.close()
    return out


def questions(db: str) -> list[dict]:
    return json.loads((QUESTIONS / db / "questions.json").read_text(encoding="utf-8"))


def run_gold(db_path: Path, sql: str) -> list[list]:
    sys.path.insert(0, str(QUESTIONS))
    from build_tasks import gold_rows

    return gold_rows(db_path, sql)


def same_answer(a, b, *, ordered: bool) -> bool:
    """Equal as the grader compares them: row order counts only when the
    question asks for it."""
    if ordered or isinstance(a, str) or isinstance(b, str):
        return a == b
    return sorted(map(repr, a)) == sorted(map(repr, b))


def decode_check(db: str, gaps: dict, built: Path) -> list[str]:
    """Problems: a decoded 4b database that does not give experiment 4's
    gold on every question."""
    problems = []
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp) / f"{db}.duckdb"
        shutil.copyfile(built, copy)
        con = duckdb.connect(str(copy))
        try:
            table, criteria = RULE_TABLES[db]
            if gaps.get("g1"):
                decode_g1(con, table, criteria, gaps["g1"]["sentinel"])
            if gaps.get("g2"):
                apply_g2(con, gaps["g2"], "recover")
        finally:
            con.close()
        for q in questions(db):
            want = run_gold(PROC_DATA / "duckdb" / f"{db}.duckdb", q["gold_sql"])
            got = run_gold(copy, q["gold_sql"])
            if not same_answer(got, want, ordered=q["order"] == "True"):
                problems.append(f"{q['id']}: decoded gold differs")
    return problems


def write_tasks() -> None:
    """Experiment 4's task files, with reverse_logistics' R06 rewritten and
    its gold recomputed (on experiment 4's database: the gold SQL carries the
    reference's meaning)."""
    public_path = PROC_DATA / "hf-full-v1" / "livesqlbench_data.jsonl"
    public = [
        json.loads(x) for x in public_path.read_text(encoding="utf-8").splitlines() if x
    ]
    frozen = json.loads(
        (PROC_DATA / "tasks_frozen_full_v1.json").read_text(encoding="utf-8")
    )
    gold = json.loads(
        (PROC_DATA / "gold_results_full_v1.json").read_text(encoding="utf-8")
    )
    old = next(
        q for q in questions("reverse_logistics") if q["id"] == "reverse_logistics_R06"
    )
    new = rewrite_r06(old)
    rows = run_gold(PROC_DATA / "duckdb" / "reverse_logistics.duckdb", new["gold_sql"])
    if len(rows) < 2 or any(v is None for r in rows for v in r):
        raise SystemExit(f"R06's new gold is degenerate: {rows}")
    for item in public:
        if item["instance_id"] == new["id"]:
            item["query"] = new["query"]
            item["conditions"] = {"order": True}
    frozen[new["id"]]["order"] = True
    gold[new["id"]] = {"db": "reverse_logistics", "order": True, "rows": rows}
    out = PROC4B_DATA / "hf-full-v1" / "livesqlbench_data.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        "".join(json.dumps(x, ensure_ascii=False) + "\n" for x in public),
        encoding="utf-8",
    )
    for name, data in (
        ("tasks_frozen_full_v1.json", frozen),
        ("gold_results_full_v1.json", gold),
    ):
        (PROC4B_DATA / name).write_text(
            json.dumps(data, indent=1) + "\n", encoding="utf-8"
        )
    print(f"tasks: {len(public)}; R06 now has {len(rows)} matching rules")


def main() -> None:
    write_tasks()
    gaps_all = json.loads(GAPS.read_text(encoding="utf-8"))
    for db, gaps in gaps_all.items():
        built = build_db(db, gaps, PROC4B_DATA / "duckdb" / f"{db}.duckdb")
        problems = decode_check(db, gaps, built)
        src = PROC_DATA / "hf-full-v1" / db
        dst = PROC4B_DATA / "hf-full-v1" / db
        dst.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(
            src / f"{db}_column_meaning_base.json",
            dst / f"{db}_column_meaning_base.json",
        )
        schema = (src / f"{db}_schema.txt").read_text(encoding="utf-8")
        (dst / f"{db}_schema.txt").write_text(
            gapped_schema(schema, db, gaps), encoding="utf-8"
        )
        rows = read_kb(db)
        write_kb(dst / f"{db}_kb.jsonl", gapped_kb(rows, gaps))
        write_kb(PROC4B_DATA / "full" / "kb" / f"{db}_kb.jsonl", full_kb(rows, gaps))
        print(f"{db}: built; decode check {'OK' if not problems else problems}")


if __name__ == "__main__":
    main()
