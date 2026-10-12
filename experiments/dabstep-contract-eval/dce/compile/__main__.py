"""python -m dce.compile -- compile a benchmark's KB into frozen contracts.

    uv run python -m dce.compile --benchmark livesqlbench --db households --max-spend 5
    uv run python -m dce.compile --benchmark livesqlbench --all --max-spend 60
    uv run python -m dce.compile --benchmark dabstep --max-spend 10
    uv run python -m dce.compile --benchmark dabstep --joins --max-spend 10
    uv run python -m dce.compile --benchmark livesqlbench --db households \
        --recompile 12 31 --max-spend 60

Output goes to `<out>/<benchmark>/[<db>/]`; `--joins` lets a metric declare
the FROM it is written against, and writes to `contracts-joins` by default.
`--max-spend` caps the spend recorded across every log under
`<out>/<benchmark>/`, resumes included.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
from pathlib import Path

from dce.compile import run as run_module


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dce.compile")
    parser.add_argument(
        "--benchmark", choices=("dabstep", "livesqlbench"), required=True
    )
    parser.add_argument(
        "--db", nargs="+", default=None, help="(livesqlbench) databases"
    )
    parser.add_argument(
        "--all", action="store_true", help="(livesqlbench) every database"
    )
    parser.add_argument(
        "--lsb-data",
        type=Path,
        default=Path(os.environ.get("LSB_DATA", Path.home() / "data" / "livesqlbench")),
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="default: contracts, or contracts-joins with --joins",
    )
    parser.add_argument(
        "--joins",
        action="store_true",
        help="let a metric declare the FROM it is written against (from_sql and grain)",
    )
    parser.add_argument("--max-spend", type=float, required=True)
    parser.add_argument(
        "--recompile",
        type=int,
        nargs="+",
        default=None,
        help="entry ids of one finished database to compile again, with "
        "every entry built on them",
    )
    args = parser.parse_args(argv)
    if args.out is None:
        args.out = Path("contracts-joins" if args.joins else "contracts")

    if args.benchmark == "livesqlbench":
        from dce.benchmarks.livesqlbench import compile_inputs

        if args.all == bool(args.db):
            raise SystemExit("name databases with --db, or pass --all")
        names = args.db or sorted(
            p.name
            for p in (args.lsb_data / "hf-full-v1").iterdir()
            if (args.lsb_data / "duckdb" / f"{p.name}.duckdb").is_file()
        )
        work = [compile_inputs(args.lsb_data, name) for name in names]
    else:
        from dce.benchmarks.dabstep import compile_inputs as dabstep_inputs

        work = [dabstep_inputs()]

    if args.joins:
        work = [dataclasses.replace(inputs, joins=True) for inputs in work]

    if args.recompile:
        if args.all or len(work) != 1:
            raise SystemExit("--recompile takes one database (--db NAME)")
        ids = run_module.reopen(args.out / work[0].out_subdir, work[0], args.recompile)
        print(f"{work[0].db_name}: recompiling entries {ids}")
    spent = run_module.log_spend(args.out / args.benchmark)
    for inputs in work:
        status, spent = run_module.compile_database(
            inputs,
            args.out / inputs.out_subdir,
            run_module.claude_driver,
            spend_cap=args.max_spend,
            spent=spent,
        )
        print(
            f"{inputs.db_name}: {status} (spent ${spent:.2f} of ${args.max_spend:.2f})"
        )
        if status == "capped":
            raise SystemExit(2)


if __name__ == "__main__":
    main()
