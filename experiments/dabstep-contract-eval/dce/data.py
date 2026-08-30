"""Fetch DABStep context files and build the local DuckDB the agents query."""

from __future__ import annotations

from pathlib import Path

import duckdb

DATASET = "adyen/DABstep"

# table name -> path within the HF dataset repo
CONTEXT_FILES: dict[str, str] = {
    "payments": "data/context/payments.csv",
    "fees": "data/context/fees.json",
    "merchant_data": "data/context/merchant_data.json",
    "acquirer_countries": "data/context/acquirer_countries.csv",
    "merchant_category_codes": "data/context/merchant_category_codes.csv",
}

# Prompt/contract source material — never loaded into the database.
DOC_FILES: dict[str, str] = {
    "manual": "data/context/manual.md",
    "payments_readme": "data/context/payments-readme.md",
}


def download_context(dest: Path) -> dict[str, Path]:
    """Download context files + docs. Returns {name: local path}."""
    from huggingface_hub import hf_hub_download

    dest.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}
    for name, repo_path in {**CONTEXT_FILES, **DOC_FILES}.items():
        out[name] = Path(
            hf_hub_download(
                repo_id=DATASET,
                filename=repo_path,
                repo_type="dataset",
                local_dir=dest,
            )
        )
    return out


def build_duckdb(files: dict[str, Path], db_path: Path) -> None:
    """CREATE OR REPLACE one table per file. Idempotent by construction."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        for table, path in files.items():
            reader = (
                f"read_json_auto('{path}')"
                if path.suffix == ".json"
                else f"read_csv_auto('{path}')"
            )
            con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {reader}")
    finally:
        con.close()


# DABStep's public split names ("default"/"dev") don't match the repo's actual
# task-file names (data/tasks/all.jsonl, data/tasks/dev.jsonl) — verified by hand
# via huggingface_hub.list_repo_files(DATASET, repo_type="dataset").
_TASK_FILES: dict[str, str] = {"default": "all", "dev": "dev"}


def load_tasks(split: str) -> list[dict]:
    """Load the DABStep task rows. split is 'default' (450) or 'dev' (10)."""
    import json

    from huggingface_hub import hf_hub_download

    filename = _TASK_FILES.get(split, split)
    path = hf_hub_download(
        repo_id=DATASET,
        filename=f"data/tasks/{filename}.jsonl",
        repo_type="dataset",
    )
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]
