"""python -m dce.benchmarks.dab_author -- experiment 7's author sessions.

    uv run python -m dce.benchmarks.dab_author --variant draw-a1 \
        --dataset bookreview stockmarket --max-spend 40

`dce.author` on a DataAgentBench dataset (spec, "Authoring"): one session
writes a dataset's whole contract from DAB's description (with the adapter's
`SESSION_NOTE`), the schema text of the built DuckDB file
(`dab_schema.schema_text`: each table's DDL and first rows, stockmarket's
ticker tables summarised) and read-only `run_sql` on that file. No question,
gold or validator is read. Five variants, each its own directory under
`--out` (default `contracts-dataagentbench/`, the adapter's):

- `draw-a1`, `draw-a2`, `draw-a3`: no hints. Draw A1 feeds `dab_contract`
  and `dab_contract_text`, fixed before any draw ran;
- `draw-b`: the hints appended to the description as the adapter's
  `dab_hints` arm appends them (`dataagentbench.with_hints`). It feeds
  `dab_hints_contract`;
- `no-data`: no hints, no `run_sql` and no sample rows. Recovery only.

A session with a log is finished and skipped. `--max-spend` caps everything
logged under `--out`, across variants, this run included.
"""

from __future__ import annotations

import argparse
import functools
import os
from pathlib import Path

from dce import author
from dce.benchmarks import dab_data, dab_fetch, dab_schema, dataagentbench
from dce.compile.inputs import CompileInputs

#: Per variant, whether the author reads the hints and whether it has the data.
VARIANTS: dict[str, dict[str, bool]] = {
    "draw-a1": {"hints": False, "no_data": False},
    "draw-a2": {"hints": False, "no_data": False},
    "draw-a3": {"hints": False, "no_data": False},
    "draw-b": {"hints": True, "no_data": False},
    "no-data": {"hints": False, "no_data": True},
}

#: As DABStep's: the built files are plain DuckDB.
DIALECT_NOTE = "DuckDB's own semantics apply."


def author_inputs(
    data: Path, dataset: str, *, hints: bool = False, no_data: bool = False
) -> CompileInputs:
    """What one session reads: the description (and, for draw B, the hints),
    the schema text (without sample rows for the no-data control) and the
    built file."""
    folder = data / "src" / dab_fetch.dataset_dir(dataset)
    db_path = data / "duckdb" / f"{dataset}.duckdb"
    description = dataagentbench.dataset_description(folder)
    sources = [folder / "db_description.txt"]
    if hints:
        description = dataagentbench.with_hints(description, folder)
        sources.append(folder / dataagentbench.HINTS_FILE)
    return CompileInputs(
        benchmark="dataagentbench",
        db_name=dataset,
        entries=(),
        ddl=dab_schema.schema_text(db_path, rows=0 if no_data else 3),
        column_meanings="",
        dialect_note=DIALECT_NOTE,
        db_path=db_path,
        init_sql=(),
        sources=(*sources, db_path),
        out_subdir=dataset,
        description=description,
        schemas=dab_schema.schemas(db_path),
    )


def main(argv: list[str] | None = None) -> None:
    from dce.compile import run as run_module

    parser = argparse.ArgumentParser(prog="dce.benchmarks.dab_author")
    parser.add_argument(
        "--dab-data",
        type=Path,
        default=Path(os.environ.get("DAB_DATA", dab_data.DEFAULT_DATA)),
    )
    parser.add_argument("--out", type=Path, default=dataagentbench.DEFAULT_CONTRACTS)
    parser.add_argument(
        "--variant", nargs="+", choices=list(VARIANTS), default=list(VARIANTS)
    )
    parser.add_argument("--dataset", nargs="+", default=None, help="default: all")
    parser.add_argument("--model", default=author.AUTHOR_MODEL)
    parser.add_argument("--max-spend", type=float, required=True)
    args = parser.parse_args(argv)

    spent = sum(
        author._logged_usd(p)
        for variant in VARIANTS
        for p in (args.out / variant).glob("*")
        if p.is_dir()
    )
    driver = functools.partial(
        run_module.claude_driver,
        model=args.model,
        request_limit=author.AUTHOR_REQUEST_LIMIT,
        timeout=author.AUTHOR_TIMEOUT_S,
    )
    datasets = args.dataset or list(dab_fetch.DATASETS)
    for variant in (v for v in VARIANTS if v in args.variant):
        flags = VARIANTS[variant]
        for dataset in datasets:
            status, spent = author.author_database(
                author_inputs(args.dab_data, dataset, **flags),
                args.out / variant / dataset,
                driver,
                spend_cap=args.max_spend,
                spent=spent,
                model=args.model,
                no_data=flags["no_data"],
            )
            print(
                f"{variant}/{dataset}: {status} "
                f"(spent ${spent:.2f} of ${args.max_spend:.2f})"
            )
            if status == "capped":
                raise SystemExit(2)


if __name__ == "__main__":
    main()
