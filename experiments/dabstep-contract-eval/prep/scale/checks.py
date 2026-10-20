"""Experiment 6's materials checks (design, Materials: Checks). Writes
contracts-scale/CHECKS.md and contracts-scale/prompt-sizes.json.

    UV_FROZEN=1 uv run python prep/scale/checks.py [--data ~/data/scale]

* every merged contract (both placements, every level) loads, and its table,
  domain, metric and rule names are unique;
* every table it allows exists in the base's working database;
* the base's entries are those of experiment 4's contract, unchanged and in
  order, at every level;
* the governed prompt at each level holds the 1x prompt's lines in order, and
  every line it adds names a padding database;
* describe_table on every base table, and the validator's decision on a fixed
  probe set of base queries, are the same at every level;
* each working database's base tables equal experiment 4's, row for row;
* each arm-level's system prompt, in characters (prompt-sizes.json).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dce.benchmarks import livesqlbench as lsb  # noqa: E402
from dce.frozen import load_contract_at  # noqa: E402
from dce.tools import _BoundedDuckDBAdapter, governed_arm  # noqa: E402
from prep.scale.build import BASE_CONTRACTS, BASES, LEVELS, OUT  # noqa: E402

from agentic_data_contracts.core.session import ContractSession  # noqa: E402
from agentic_data_contracts.tools.factory import create_tools  # noqa: E402

PROC = Path.home() / "data" / "procedures"


def _load(folder: Path) -> tuple[dict, dict]:
    return (
        yaml.safe_load((folder / "contract.yml").read_text(encoding="utf-8")),
        yaml.safe_load((folder / "semantic.yml").read_text(encoding="utf-8")),
    )


def _names(contract: dict, semantic: dict) -> dict[str, list[str]]:
    sem = contract["semantic"]
    return {
        "tables": [
            f"{a['schema']}.{t}" for a in sem["allowed_tables"] for t in a["tables"]
        ],
        "domains": [d["name"] for d in sem.get("domains") or []],
        "rules": [r["name"] for r in sem.get("rules") or []],
        "metrics": [m["name"] for m in semantic.get("metrics") or []],
    }


def _base_entries(contract: dict, semantic: dict, prefixes: set[str]) -> dict:
    def own(name: str) -> bool:
        return name.split(".", 1)[0] not in prefixes

    sem = contract["semantic"]
    return {
        "allowed_tables": [a for a in sem["allowed_tables"] if a["schema"] == "main"],
        "domains": [d for d in sem.get("domains") or [] if own(d["name"])],
        "rules": [r for r in sem.get("rules") or [] if own(r["name"])],
        "metrics": [m for m in semantic.get("metrics") or [] if own(m["name"])],
        "tables": [t for t in semantic.get("tables") or [] if t["schema"] == "main"],
    }


async def _tool_answers(contract, db: Path, tables: list[str]) -> dict:
    tools = {
        t.name: t
        for t in create_tools(
            contract,
            adapter=_BoundedDuckDBAdapter(db),
            session=ContractSession(contract),
            max_result_rows=50,
        )
    }
    out = {}
    for t in tables:
        r = await tools["describe_table"].callable({"schema": "main", "table": t})
        out[f"describe {t}"] = r
        for sql in (
            f"SELECT * FROM main.{t} LIMIT 1",
            f"SELECT count(*) AS n FROM main.{t}",
            f"DELETE FROM main.{t}",
        ):
            r = await tools["inspect_query"].callable({"sql": sql})
            body = json.loads(r["content"][0]["text"])
            out[sql] = (body.get("valid"), sorted(body.get("violations") or []))
    return out


def _subsequence(small: list[str], big: list[str]) -> bool:
    it = iter(big)
    return all(line in it for line in small)


def check(data: Path) -> list[str]:
    levels = json.loads((OUT / "levels.json").read_text(encoding="utf-8"))
    lines, failures = [], []
    for base in BASES:
        working = data / "duckdb" / f"{base}.duckdb"
        con = duckdb.connect(str(working), read_only=True)
        existing = {
            f"{s}.{t}"
            for s, t in con.sql(
                "SELECT schema_name, table_name FROM duckdb_tables()"
            ).fetchall()
        }
        same_rows = True
        con.execute(f"ATTACH '{PROC / 'duckdb' / f'{base}.duckdb'}' AS e4 (READ_ONLY)")
        for (t,) in con.sql(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = 'e4' AND schema_name = 'main'"
        ).fetchall():
            diff = con.sql(
                f'SELECT count(*) FROM (SELECT * FROM e4.main."{t}" '
                f'EXCEPT ALL SELECT * FROM main."{t}")'
            ).fetchone()
            n = con.sql(f'SELECT count(*) FROM main."{t}"').fetchone()
            m = con.sql(f'SELECT count(*) FROM e4.main."{t}"').fetchone()
            same_rows &= diff == (0,) and n == m
        con.close()
        lines.append(f"- {base}: base tables equal experiment 4's: {same_rows}")
        failures += [] if same_rows else [f"{base}: base tables differ"]

        own = _load(BASE_CONTRACTS / base)
        own_entries = _base_entries(*own, set())
        prompts, answers = {}, {}
        base_tables = own[0]["semantic"]["allowed_tables"][0]["tables"]
        for placement in ("merged", "first"):
            for level in LEVELS:
                folder = OUT / placement / level / base
                merged = _load(folder)
                pads = set(levels[base][level])
                tag = f"{base} {placement} {level}"
                try:
                    contract = load_contract_at(folder / "contract.yml")
                except Exception as exc:  # noqa: BLE001
                    failures.append(f"{tag}: does not load: {exc}")
                    continue
                for kind, names in _names(*merged).items():
                    dup = [n for n, c in Counter(names).items() if c > 1]
                    if dup:
                        failures.append(f"{tag}: duplicate {kind}: {dup[:5]}")
                missing = sorted(set(_names(*merged)["tables"]) - existing)
                if missing:
                    failures.append(
                        f"{tag}: tables not in the working db: {missing[:5]}"
                    )
                if _base_entries(*merged, pads) != own_entries:
                    failures.append(f"{tag}: the base's entries changed")
                if placement == "merged":
                    prompts[level] = governed_arm("", working, contract).system_prompt
                    answers[level] = asyncio.run(
                        _tool_answers(contract, working, base_tables)
                    )
        one = prompts["1x"].splitlines()
        for level, prompt in prompts.items():
            added = prompt.splitlines()
            if not _subsequence(one, added):
                failures.append(f"{base} {level}: governed prompt drops 1x lines")
            pads = levels[base][level]
            extra = [ln for ln in added if ln not in set(one)]
            stray = [ln for ln in extra if not any(f"{p}." in ln for p in pads)]
            lines.append(
                f"- {base} {level}: governed prompt {len(prompt):,} chars, "
                f"{len(extra)} added lines, {len(stray)} not naming a padding "
                "database"
            )
            if answers[level] != answers["1x"]:
                failures.append(f"{base} {level}: base tool answers changed")
    return (
        lines
        + [f"- FAIL {f}" for f in failures]
        + (["- every check passed"] if not failures else [])
    )


def prompt_sizes(data: Path) -> dict:
    bench = lsb.LiveSQLBench.from_data(data, contracts=BASE_CONTRACTS, dbs=BASES)
    out: dict[str, dict[str, int]] = {}
    for base in BASES:
        task = next(t for t in bench.tasks() if t.meta["db"] == base)
        db = bench.pristine_db(task)
        out[base] = {}
        for arm in lsb.SCALE_ARMS:
            setup = bench.build_arm(arm, task, db)
            out[base][arm] = len(setup.system_prompt)
            setup.close()
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path.home() / "data" / "scale")
    args = parser.parse_args()
    lines = check(args.data)
    sizes = prompt_sizes(args.data)
    (OUT / "prompt-sizes.json").write_text(json.dumps(sizes, indent=1) + "\n", "utf-8")
    text = (
        "# Experiment 6: materials checks\n\n"
        "Written by `prep/scale/checks.py`; see its docstring for what each "
        "line checks.\n\n" + "\n".join(lines) + "\n"
    )
    (OUT / "CHECKS.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
