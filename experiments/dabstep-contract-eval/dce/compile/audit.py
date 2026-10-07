"""The compile audit (spec, Part 3), drafted from the compile logs.

Per database: entries by kind, metrics translated and not, prose, the
execution rate (translated over calculation and predicate entries), the
compiler's queries and cost. Then a seeded sample of translated metrics,
each with its KB text and SQL and a blank verdict line for the hand check.
The verdicts are filled in by hand and committed with the contracts.

    uv run python -m dce.compile.audit --benchmark livesqlbench \
        > contracts/livesqlbench/AUDIT.md
"""

from __future__ import annotations

import argparse
import random
from collections import Counter
from pathlib import Path

from dce.compile.run import FINAL_LOG, _gz_records, _usd


def _databases(bench_dir: Path) -> list[tuple[str, list[dict], float]]:
    """(database, entry records, spend). The spend counts failed and redone
    entries too, which have no record."""
    out = []
    for log in sorted(bench_dir.rglob(FINAL_LOG)):
        lines = _gz_records(log)
        records = [r for r in lines if r.get("type") == "entry"]
        out.append((log.parent.name, records, _usd(lines)))
    return out


def render(bench_dir: Path, *, n: int = 30, seed: int = 2026) -> str:
    dbs = _databases(bench_dir)
    kinds = [
        k
        for k in ("calculation", "predicate", "illustration", "section")
        if any(r["kind"] == k for _, rs, _ in dbs for r in rs)
    ]
    head = [
        "database",
        "entries",
        *kinds,
        "metrics",
        "untranslated",
        "prose",
        "execution",
        "queries",
        "cost",
    ]
    lines = [
        f"# Compile audit: {bench_dir.name}",
        "",
        "| " + " | ".join(head) + " |",
        "|" + "---|" * len(head),
    ]
    totals: Counter = Counter()
    usd_total = 0.0
    sample_pool = []
    for db, records, usd in dbs:
        by_kind = Counter(r["kind"] for r in records)
        status = Counter(r["status"] for r in records)
        metrics = sum(1 for r in records for m in r["metrics"] if m["sql_expression"])
        untranslated = status["untranslated"]
        prose = status["prose"]
        translatable = by_kind["calculation"] + by_kind["predicate"]
        rate = (
            f"{(translatable - untranslated) / translatable:.0%}"
            if translatable
            else "-"
        )
        queries = sum(len(r["queries"]) for r in records)
        usd_total += usd
        row = [
            db,
            str(len(records)),
            *[str(by_kind[k]) for k in kinds],
            str(metrics),
            str(untranslated),
            str(prose),
            rate,
            str(queries),
            f"${usd:.2f}",
        ]
        lines.append("| " + " | ".join(row) + " |")
        totals.update(by_kind)
        totals.update(
            {
                "entries": len(records),
                "metrics": metrics,
                "untranslated": untranslated,
                "prose": prose,
                "queries": queries,
            }
        )
        for r in records:
            for m in r["metrics"]:
                if m["sql_expression"]:
                    sample_pool.append((db, r, m))
    total_translatable = totals["calculation"] + totals["predicate"]
    rate = (
        f"{(total_translatable - totals['untranslated']) / total_translatable:.0%}"
        if total_translatable
        else "-"
    )
    lines.append(
        "| **total** | "
        + " | ".join(
            [
                str(totals["entries"]),
                *[str(totals[k]) for k in kinds],
                str(totals["metrics"]),
                str(totals["untranslated"]),
                str(totals["prose"]),
                rate,
                str(totals["queries"]),
                f"${usd_total:.2f}",
            ]
        )
        + " |"
    )
    pool = sorted(sample_pool, key=lambda x: (x[0], x[2]["name"]))
    picked = pool if n == 0 else random.Random(seed).sample(pool, min(n, len(pool)))
    lines += ["", f"## Sample: {len(picked)} metric(s), seed {seed}", ""]
    for db, record, metric in picked:
        lines += [
            f"### {db} / {metric['name']}",
            "",
            f"Entry: {record['name']} ({record['kind']})",
            "",
            "```text",
            metric["description"],
            "```",
            "",
            f"Over `{metric['source_model']}`:",
            "",
            "```sql",
            metric["sql_expression"],
            "```",
            "",
            "Verdict: agrees / disagrees / unclear -- note:",
            "",
        ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dce.compile.audit")
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--out", type=Path, default=Path("contracts"))
    parser.add_argument("--n", type=int, default=30)
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args(argv)
    print(render(args.out / args.benchmark, n=args.n, seed=args.seed))


if __name__ == "__main__":
    main()
