"""Experiment 4b's gap checks (design, "Checks per gap"): each database's
menu taken in order, the first candidate that passes kept, nothing but pass
or fail going back. The only step of the materials that reads the questions.

    UV_FROZEN=1 uv run python prep/procedures4b/checks.py

Writes CHECKS.md (every candidate, every check) and selected.json (the
first passing G1 sentinel and G2 candidate per database).
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import build as b4  # noqa: E402
from propose import RULE_TABLES  # noqa: E402

DBS = ("labor_certification_applications", "reverse_logistics", "solar_panel")
#: G1: each criterion's wildcard share at least this, pooled at least POOLED.
PER_CRITERION, POOLED = 0.15, 0.20
BITE = 0.5
#: Candidates the blind raters did not settle (RATINGS.md), replaced by the
#: next on the menu, as the design says.
RATER_REJECTED = {
    ("reverse_logistics", "g2", 0): "blind raters split (round 1): gpt-6-sol said the "
    "data cannot tell, claudeopus5.5 read v + 1",
}
#: Bounds the documentation states for a quantity C reads, by (table, column,
#: JSON path): (low, high, "percentage" when it is documented as a percentage
#: on 0-100). From the KB entries named; a candidate on a quantity with no
#: stated bound fails the data-property check.
DOC_BOUNDS = {
    ("quality_assessment", "assessment_summary", "$.results.aesthetic_score"): (
        1,
        10,
        None,
        "KB 36: 'a number from 1 to 10'",
    ),
    (
        "electrical_performance",
        "elec_perf_snapshot",
        "$.energy_yield.availability_pct",
    ): (0, 100, "percentage", "KB 56: 'a percentage on a 0 to 100 scale'"),
    ("electrical_performance", "elec_perf_snapshot", "$.efficiency.soil_loss_pct"): (
        0,
        100,
        "percentage",
        "KB 56: 'a percentage on a 0 to 100 scale'",
    ),
}


def leaf(cand: dict) -> str:
    return (cand.get("json_path") or "").split(".")[-1] or cand["column"]


def questions(db: str) -> list[dict]:
    qs = b4.questions(db)
    return [b4.rewrite_r06(q) if q["id"] == "reverse_logistics_R06" else q for q in qs]


def answers(db_path: Path, qs: list[dict], transform=None) -> dict[str, object]:
    out = {}
    for q in qs:
        sql = transform(q["gold_sql"]) if transform else q["gold_sql"]
        try:
            out[q["id"]] = b4.run_gold(db_path, sql)
        except Exception as exc:  # noqa: BLE001 - an error is a (loud) answer
            out[q["id"]] = f"ERROR {type(exc).__name__}"
    return out


def bite(gold: dict, got: dict, ids: list[str], ordered: set[str]) -> tuple[float, int]:
    """(share of `ids` whose answer differs, how many of them are loud:
    empty or an error)."""
    differ = [
        i for i in ids if not b4.same_answer(got[i], gold[i], ordered=i in ordered)
    ]
    loud = [i for i in differ if not got[i] or isinstance(got[i], str)]
    return (len(differ) / len(ids) if ids else 0.0), len(loud)


def g1_checks(db: str, con, sentinel: str) -> list[str]:
    table, criteria = RULE_TABLES[db]
    problems = []
    texts = con.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE data_type = 'VARCHAR' AND table_schema = 'main' AND table_name <> ?",
        [table],
    ).fetchall()
    for t, c in texts:
        n = con.execute(
            f'SELECT count(*) FROM "{t}" WHERE "{c}" = ? OR "{c}" LIKE ?',
            [sentinel, f'%"{sentinel}"%'],
        ).fetchone()[0]
        if n:
            problems.append(
                f"the sentinel occurs as a record value ({t}.{c}, {n} rows)"
            )
    total = con.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
    shares = {
        c: con.execute(
            f'SELECT count(*) FROM "{table}" WHERE "{c}" IS NULL'
        ).fetchone()[0]
        / total
        for c in criteria
    }
    low = [c for c, s in shares.items() if s < PER_CRITERION]
    if low:
        problems.append(f"criteria below {PER_CRITERION:.0%}: {low}")
    if sum(shares.values()) / len(shares) < POOLED:
        problems.append(f"pooled wildcard share below {POOLED:.0%}")
    return problems


def g2_property(db_path: Path, cand: dict) -> tuple[bool, str]:
    key = (cand["table"], cand["column"], cand.get("json_path"))
    if key not in DOC_BOUNDS:
        return False, "no bound stated in the documentation for this quantity"
    lo, hi, kind, source = DOC_BOUNDS[key]
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        expr = (
            f"CAST(\"{cand['column']}\" ->> '{cand['json_path']}' AS DOUBLE)"
            if cand.get("json_path")
            else f'CAST("{cand["column"]}" AS DOUBLE)'
        )
        smin, smax = con.execute(
            f'SELECT min({expr}), max({expr}) FROM "{cand["table"]}"'
        ).fetchone()
    finally:
        con.close()
    if smin is None:
        return False, "no stored values"
    if smin < lo or smax > hi:
        return True, f"stored {smin:g}..{smax:g} breaks {source}"
    if kind == "percentage" and smax <= 1:
        return True, f"stored {smin:g}..{smax:g}, all within [0, 1], against {source}"
    return False, f"stored {smin:g}..{smax:g} is consistent with {source}"


def main() -> None:
    report = [
        "# Experiment 4b: gap checks",
        "",
        "Each database's menu in order; the first candidate that passes is kept.",
        "",
    ]
    selected: dict[str, dict] = {}
    used: set[str] = set()
    for db in DBS:
        menu = json.loads((HERE / db / "menu.json").read_text(encoding="utf-8"))
        qs = questions(db)
        ordered = {q["id"] for q in qs if q["order"] == "True"}
        r_ids = [q["id"] for q in qs if q["piece"] == "R"]
        c_qs = [q for q in qs if q["piece"] == "C"]
        exp4 = b4.PROC_DATA / "duckdb" / f"{db}.duckdb"
        gold = answers(exp4, qs)
        report += [f"## {db}", ""]
        sel: dict = {}
        with tempfile.TemporaryDirectory() as tmp:
            for i, cand in enumerate(menu["g1"]):
                s = cand["sentinel"]
                problems = []
                if s in used:
                    problems.append("already used by another database")
                con = duckdb.connect(str(exp4), read_only=True)
                try:
                    problems += g1_checks(db, con, s)
                finally:
                    con.close()
                share, loud = 0.0, 0
                if not problems:
                    path = b4.build_db(
                        db, {"g1": {"sentinel": s}}, Path(tmp) / f"g1-{i}.duckdb"
                    )
                    got = answers(path, qs)
                    share, loud = bite(gold, got, r_ids, ordered)
                    if share < BITE:
                        problems.append(
                            f"bites {share:.0%} of R questions, below {BITE:.0%}"
                        )
                verdict = "PASS" if not problems else "FAIL: " + "; ".join(problems)
                report.append(
                    f"- G1[{i}] `{s}`: bite {share:.0%} of R ({loud} loud). {verdict}"
                )
                if not problems and "g1" not in sel:
                    sel["g1"] = {
                        "sentinel": s,
                        "menu_rank": i,
                        "bite": share,
                        "loud": loud,
                    }
                    used.add(s)
                    break
            for i, cand in enumerate(menu["g2"]):
                problems = []
                if (db, "g2", i) in RATER_REJECTED:
                    problems.append(RATER_REJECTED[(db, "g2", i)])
                name = leaf(cand)
                reads = [q["id"] for q in c_qs if name in q["gold_sql"]]
                if not reads:
                    problems.append("no C question reads it")
                if any(name in q["gold_sql"] for q in qs if q["piece"] == "R"):
                    problems.append("R reads it")
                if any(name in q["query"] for q in qs):
                    problems.append("a question text names it")
                share, loud, prop = 0.0, 0, ""
                if not problems:
                    path = b4.build_db(db, {"g2": cand}, Path(tmp) / f"g2-{i}.duckdb")
                    ok, prop = g2_property(path, cand)
                    if not ok:
                        problems.append(prop)
                    got = answers(path, qs)
                    share, loud = bite(gold, got, reads, ordered)
                    if share < BITE:
                        problems.append(
                            f"bites {share:.0%} of the C questions reading it"
                        )
                verdict = "PASS" if not problems else "FAIL: " + "; ".join(problems)
                report.append(
                    f"- G2[{i}] {cand['table']}.{cand['column']} "
                    f"{cand.get('json_path') or ''} "
                    f"`{cand['stored']}` ({cand['kind']}): bite {share:.0%} of "
                    f"{len(reads)} C "
                    f"questions reading it ({loud} loud). {prop} {verdict}"
                )
                if not problems:
                    sel["g2"] = {
                        **cand,
                        "menu_rank": i,
                        "reads": reads,
                        "bite": share,
                        "loud": loud,
                    }
                    break
            if "g2" not in sel:
                report.append("- G2: menu exhausted; no G2 for this database.")
        if db == "reverse_logistics":
            g3_ids = [i for i in r_ids if i != "reverse_logistics_R06"]
            g3_qs = [q for q in qs if q["id"] in g3_ids]
            for conv in ("sum", "lowest_id"):
                got = answers(exp4, g3_qs, lambda sql, c=conv: b4.g3_variant(sql, c))
                share, loud = bite(gold, got, g3_ids, ordered)
                degenerate = [
                    i for i in g3_ids if not got[i] or isinstance(got[i], str)
                ]
                report.append(
                    f"- G3 `{conv}`: differs from the gold on {share:.0%} of the "
                    f"{len(g3_ids)} G3 questions; degenerate answers: {len(degenerate)}"
                )
                sel.setdefault("g3", {})[conv] = {
                    "bite": share,
                    "degenerate": len(degenerate),
                }
        report.append("")
        selected[db] = sel
    (HERE / "CHECKS.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    (HERE / "selected.json").write_text(
        json.dumps(selected, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print("\n".join(report))


if __name__ == "__main__":
    main()
