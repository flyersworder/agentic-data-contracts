"""Experiment 7's blind hint raters (design, "Hint items"; order of work,
step 2).

Two raters of different model families, each alone, see one dataset's
description (`db_description.txt`, without hints), its databases' schemas
with first rows (`dab_schema`), the hint items of `hint-items.json` and
read-only SQL on the built DuckDB file: no question, gold, validator or
hints file. Each labels each item with the first class of `CLASSES` that
applies. An item takes a class when both raters give it; a split goes to the
later class in `CLASSES` (`resolve`). This rule is committed before any
rating is read.

    set -a; . ./.env; set +a
    UV_FROZEN=1 uv run python prep/dataagentbench/rate_hints.py --max-spend 20

Writes `hint-ratings.json` and `HINT-RATINGS.md` here; each session's full
log (its messages hold DAB data) goes to `LOGS`, outside the repository.
"""

from __future__ import annotations

import argparse
import functools
import gzip
import json
import sys
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

from dce.benchmarks import dab_fetch  # noqa: E402
from dce.benchmarks.dab_data import DEFAULT_DATA  # noqa: E402
from dce.benchmarks.dab_schema import schema_text  # noqa: E402
from dce.benchmarks.dataagentbench import SESSION_NOTE  # noqa: E402
from dce.compile.db import CompileDB  # noqa: E402

ITEMS = HERE / "hint-items.json"
RATINGS = HERE / "hint-ratings.json"
REPORT = HERE / "HINT-RATINGS.md"
LOGS = Path.home() / "data" / "dce-held-results" / "exp7" / "hint-ratings"

#: As in experiment 4b: two families, through the gateway.
RATERS = ("gpt-6-sol", "claudeopus5.5")
#: Checked in this order; a split goes to the later one.
CLASSES = ("described", "definition", "data-discoverable", "world", "guidance")
REQUEST_LIMIT = 60
MAX_SUBMISSIONS = 3

SYSTEM_PROMPT = """\
You are a careful data engineer reviewing one dataset: its description, its \
databases' schemas with the first rows of each table, and read-only SQL with \
run_sql. Below are statements about this dataset, called items. For each \
item, give the FIRST class in this list that applies to it:

1. described: the description already states it. Quote the description's \
words in the evidence.
2. definition: it is a formula or a business definition (how a quantity is \
computed, or what a term counts). Data can show values, not a definition.
3. data-discoverable: the data can show it (a prefix, an encoding, a format, \
a type, duplicates, which values join). Before choosing this class, run a \
query that checks it, and give the query, what it returned and whether it \
bears the item out in the evidence.
4. world: general knowledge settles it, not this description or data (what a \
code, an abbreviation or a place means).
5. guidance: it points at where to look or what a query will need, rather \
than stating a property of the data or a definition.

Rate each item on its own. Answer only from what you can see here.

Submit with submit_ratings(ratings): an object keyed by item id, each value \
an object with exactly the fields "class" (one of described, definition, \
data-discoverable, world, guidance) and "evidence" (one to three sentences). \
A submission that fails a check comes back with the reasons; you have \
{submissions}. When one returns "Accepted.", reply "done".
"""

USER_PROMPT = """\
Dataset `{dataset}`.

## Items

{items}

## Description

{description}

## Schemas

{schemas}"""


def load_items() -> dict[str, list[dict]]:
    return json.loads(ITEMS.read_text(encoding="utf-8"))


def resolve(a: str, b: str) -> str:
    return max(a, b, key=CLASSES.index)


def check_ratings(ratings, ids: list[str]) -> list[str]:
    if not isinstance(ratings, dict):
        return ["ratings must be an object keyed by item id"]
    problems = [f"unknown item id {k}" for k in ratings if k not in ids]
    for i in ids:
        r = ratings.get(i)
        if not isinstance(r, dict):
            problems.append(f"no rating for item {i}")
            continue
        if r.get("class") not in CLASSES:
            problems.append(f"item {i}: class must be one of {', '.join(CLASSES)}")
        if not isinstance(r.get("evidence"), str) or not r["evidence"].strip():
            problems.append(f"item {i}: evidence is missing")
    return problems


def rate_one(
    dataset: str, items: list[dict], model: str, driver, data: Path
) -> tuple[dict | None, dict]:
    folder = data / "src" / dab_fetch.dataset_dir(dataset)
    description = (folder / "db_description.txt").read_text(encoding="utf-8")
    db_path = data / "duckdb" / f"{dataset}.duckdb"
    ids = [i["id"] for i in items]
    state: dict = {"ratings": None, "submissions": 0, "queries": [], "errors": []}
    lock = threading.Lock()
    con = CompileDB(db_path)

    def run_sql(sql: str) -> str:
        """Run a read-only SQL query on the dataset; at most 50 rows come back."""
        out = con.run_sql(sql)
        state["queries"].append({"sql": sql, "error": out.startswith("ERROR:")})
        return out

    def submit_ratings(ratings: dict) -> str:
        """Submit one rating per item, keyed by item id."""
        with lock:
            if state["ratings"] is not None:
                return "Already accepted."
            if state["submissions"] >= MAX_SUBMISSIONS:
                return "No submissions left. Stop."
            state["submissions"] += 1
            problems = check_ratings(ratings, ids)
            if not problems:
                state["ratings"] = ratings
                return "Accepted."
            text = "\n".join(f"- {p}" for p in problems)
            state["errors"].append(text)
            return f"ERROR:\n{text}"

    try:
        result = driver(
            system_prompt=SYSTEM_PROMPT.format(submissions=MAX_SUBMISSIONS),
            user_prompt=USER_PROMPT.format(
                dataset=dataset,
                items="\n".join(f"[{i['id']}] {i['text']}" for i in items),
                description=description.strip() + "\n\n" + SESSION_NOTE,
                schemas=schema_text(db_path),
            ),
            tools=[run_sql, submit_ratings],
        )
    finally:
        con.close()
    log = {
        **{k: v for k, v in state.items() if k != "ratings"},
        "model": model,
        "usage": result.usage,
        "messages": result.messages,
    }
    return state["ratings"], log


def resolved(
    items: dict[str, list[dict]], ratings: dict, raters=RATERS
) -> dict[str, str]:
    out = {}
    for dataset, its in items.items():
        a, b = (ratings[f"{dataset}|{m}"] for m in raters)
        for i in its:
            out[i["id"]] = resolve(a[i["id"]]["class"], b[i["id"]]["class"])
    return out


def report(items: dict[str, list[dict]], ratings: dict, raters=RATERS) -> str:
    final = resolved(items, ratings, raters)
    lines = [
        "# Experiment 7: blind hint ratings",
        "",
        f"Raters {raters[0]} and {raters[1]}; a split goes to the later class "
        f"of {', '.join(CLASSES)}.",
        "",
    ]
    for dataset, its in items.items():
        lines.append(f"## {dataset}")
        lines.append("")
        for i in its:
            per = "; ".join(
                f"{m}: {ratings[f'{dataset}|{m}'][i['id']]['class']}" for m in raters
            )
            lines.append(f"- {i['id']}: {final[i['id']]} ({per})")
        lines.append("")
    counts = {c: sum(v == c for v in final.values()) for c in CLASSES}
    agree = sum(
        ratings[f"{d}|{raters[0]}"][i["id"]]["class"]
        == ratings[f"{d}|{raters[1]}"][i["id"]]["class"]
        for d, its in items.items()
        for i in its
    )
    lines.append(
        f"Totals: {', '.join(f'{c} {n}' for c, n in counts.items())}; the raters "
        f"agree on {agree} of {len(final)} items."
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    from dce.compile import run as run_module

    parser = argparse.ArgumentParser()
    parser.add_argument("--max-spend", type=float, required=True)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    args = parser.parse_args()
    items = load_items()
    ratings = (
        json.loads(RATINGS.read_text(encoding="utf-8")) if RATINGS.exists() else {}
    )
    spent = 0.0
    for dataset, its in items.items():
        for model in RATERS:
            key = f"{dataset}|{model}"
            if ratings.get(key):
                continue
            if spent >= args.max_spend:
                raise SystemExit(f"stopped at the ${args.max_spend:.2f} cap")
            driver = functools.partial(
                run_module.claude_driver, model=model, request_limit=REQUEST_LIMIT
            )
            got, log = rate_one(dataset, its, model, driver, args.data)
            spent += log["usage"].get("usd") or 0.0
            ratings[key] = got
            logdir = LOGS / dataset
            logdir.mkdir(parents=True, exist_ok=True)
            (logdir / f"rate-log-{model}.jsonl.gz").write_bytes(
                gzip.compress(
                    (json.dumps(log, ensure_ascii=False) + "\n").encode("utf-8"),
                    mtime=0,
                )
            )
            RATINGS.write_text(
                json.dumps(ratings, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            print(f"{key}: {'ratings' if got else 'NONE'} (spent ${spent:.2f})")
    if all(ratings.get(f"{d}|{m}") for d in items for m in RATERS):
        REPORT.write_text(report(items, ratings), encoding="utf-8")
        print(f"wrote {REPORT.name}")
    else:
        print("some sessions gave no ratings; rerun to retry them")


if __name__ == "__main__":
    main()
