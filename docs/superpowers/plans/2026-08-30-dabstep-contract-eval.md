# DABStep Contract-Context Evaluation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a three-arm ablation on the DABStep benchmark that measures whether contract-delivered context beats a manual-in-prompt baseline and a schema-only floor, producing the library's first end-to-end accuracy number.

**Architecture:** A standalone `uv` project under `experiments/dabstep-contract-eval/`, mirroring the layout of `experiments/mermaid-joinpath-eval`. It depends on `agentic-data-contracts` as a path dependency and drives it through `create_pydantic_ai_tools`. Arms differ only in what context reaches the agent; the model, loop, caps, and scorer are shared code. All model calls go through OpenRouter via Pydantic AI. Results append to JSONL, one row per `(task, arm, model)`, and statistics are computed offline from those files.

**Tech Stack:** Python 3.12+, `uv`, `pydantic-ai-slim[openai]` >=2.36, `duckdb`, `huggingface-hub`, `scipy` (McNemar/Wilson), `pytest`.

**Spec:** `docs/superpowers/specs/2026-08-30-dabstep-contract-eval-design.md` — read it before starting. The plan implements it; where they disagree, the spec wins and the plan is wrong.

## Global Constraints

- **Python:** `requires-python = ">=3.12"`.
- **Run everything through `uv run`** from inside `experiments/dabstep-contract-eval/`.
- **Model ids are pinned snapshots, verbatim:** `deepseek/deepseek-v4-flash-0731`, `deepseek/deepseek-v4-pro-0813`, `z-ai/glm-5.3-flash`, `openai/gpt-5.6-sol`. Never substitute an unpinned alias.
- **Pricing, USD per 1M tokens, verbatim:** flash-0731 `0.065 / 0.18`; pro-0813 `0.66 / 1.98`; glm-5.3-flash `0.075 / 0.25`; gpt-5.6-sol `2.00 / 10.00`.
- **Three arm names, verbatim:** `schema_only`, `manual_prompt`, `contract`.
- **`OPENROUTER_API_KEY`** comes from a `.env` **outside** this repo, sourced via `LENS_ENV_FILE`. Never commit a key; never write one into a file under the repo.
- **Nothing under `data/` is committed.** It holds the DuckDB file, the HF cache, and reconstructed golds.
- **No network in tests.** Every test in `tests/` runs offline and deterministically.
- **The contract is frozen before any scored run.** Once Task 5 commits it, editing `contract/` invalidates all downstream results.
- **Tests precede implementation** (repo TDD convention). Steps below are ordered accordingly; do not reorder them.
- **`uv.lock` is committed** for this experiment, as it is for `mermaid-joinpath-eval`. The lock is what makes a re-run months later reproducible; the `>=` constraints only set the floor.
- **This experiment's `pydantic-ai-slim>=2.36` floor is local to `experiments/dabstep-contract-eval/pyproject.toml`.** Do **not** raise the library's own `pydantic-ai-slim[anthropic]>=2.0.0` floor in the root `pyproject.toml` to match — the library supports that floor deliberately, and the experiment is a separate `uv` project with its own resolution, so nothing forces them to agree. The API surface used here (`UsageLimits(cost_limit=, tool_calls_limit=)`, `RunUsage.cache_read_tokens`, `OpenRouterProvider`, `ModelSettings(temperature/seed/timeout)`) was verified present in 2.36.0 on 2026-08-30.

---

### Task 1: Project scaffold and cost accounting

**Files:**
- Create: `experiments/dabstep-contract-eval/pyproject.toml`
- Create: `experiments/dabstep-contract-eval/.gitignore`
- Create: `experiments/dabstep-contract-eval/dce/__init__.py`
- Create: `experiments/dabstep-contract-eval/dce/pricing.py`
- Test: `experiments/dabstep-contract-eval/tests/test_pricing.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `MODELS: dict[str, ModelSpec]`, `cost(model: str, in_tok: int, out_tok: int) -> float`, `ModelSpec` dataclass with fields `id: str`, `price_in: float`, `price_out: float`, `role: str`.

- [ ] **Step 1: Create the project files**

`experiments/dabstep-contract-eval/pyproject.toml`:

```toml
[project]
name = "dce"
version = "0.1.0"
description = "DABStep contract-context evaluation: schema-only vs manual-in-prompt vs contract"
requires-python = ">=3.12"
dependencies = [
    "agentic-data-contracts",
    "pydantic-ai-slim[openai]>=2.36",
    "duckdb>=1.0",
    "huggingface-hub>=0.25",
    "scipy>=1.11",
]

[dependency-groups]
dev = ["pytest>=8"]

[tool.uv.sources]
agentic-data-contracts = { path = "../..", editable = true }

[tool.pytest.ini_options]
testpaths = ["tests"]
```

`experiments/dabstep-contract-eval/.gitignore`:

```
data/
.venv/
__pycache__/
.pytest_cache/
```

`experiments/dabstep-contract-eval/dce/__init__.py`: empty file.

- [ ] **Step 2: Write the failing test**

`experiments/dabstep-contract-eval/tests/test_pricing.py`:

```python
import pytest

from dce.pricing import MODELS, cost


def test_all_four_models_are_pinned_snapshots():
    assert set(MODELS) == {
        "deepseek/deepseek-v4-flash-0731",
        "deepseek/deepseek-v4-pro-0813",
        "z-ai/glm-5.3-flash",
        "openai/gpt-5.6-sol",
    }


def test_cost_is_per_million_tokens():
    # pro-0813 is 0.66 in / 1.98 out per 1M tokens.
    assert cost("deepseek/deepseek-v4-pro-0813", 1_000_000, 0) == pytest.approx(0.66)
    assert cost("deepseek/deepseek-v4-pro-0813", 0, 1_000_000) == pytest.approx(1.98)
    assert cost("deepseek/deepseek-v4-pro-0813", 30_000, 2_000) == pytest.approx(
        0.0198 + 0.00396
    )


def test_unknown_model_raises_rather_than_guessing():
    # A silent 0.0 would let an unbudgeted model run to completion.
    with pytest.raises(KeyError):
        cost("deepseek/deepseek-v4-pro", 100, 100)
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd experiments/dabstep-contract-eval && uv run pytest tests/test_pricing.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.pricing'`

- [ ] **Step 4: Write the implementation**

`experiments/dabstep-contract-eval/dce/pricing.py`:

```python
"""Pinned model snapshots and their OpenRouter prices.

Prices are USD per 1M tokens, recorded 2026-08-30. An unpinned model id would
let OpenRouter silently re-point to a new snapshot mid-sweep, putting two
different models in one results file with no column recording it.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelSpec:
    id: str
    price_in: float   # USD per 1M input tokens
    price_out: float  # USD per 1M output tokens
    role: str


MODELS: dict[str, ModelSpec] = {
    m.id: m
    for m in (
        ModelSpec("deepseek/deepseek-v4-flash-0731", 0.065, 0.18, "weak"),
        ModelSpec("deepseek/deepseek-v4-pro-0813", 0.66, 1.98, "strong"),
        ModelSpec("z-ai/glm-5.3-flash", 0.075, 0.25, "cross_family_control"),
        ModelSpec("openai/gpt-5.6-sol", 2.00, 10.00, "frontier_subset"),
    )
}


def cost(model: str, in_tok: int, out_tok: int) -> float:
    """USD for a call. Raises KeyError on an unpinned or unknown model id."""
    spec = MODELS[model]
    return (in_tok * spec.price_in + out_tok * spec.price_out) / 1_000_000
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd experiments/dabstep-contract-eval && uv sync && uv run pytest -v`
Expected: 3 passed

- [ ] **Step 6: Confirm the resolved pydantic-ai version and commit the lock**

```bash
cd experiments/dabstep-contract-eval
uv run python -c "
import importlib.metadata as md
print('pydantic-ai-slim', md.version('pydantic-ai-slim'))
"
```

Expected: 2.36.0 or newer. If `uv` resolved something older, the root package's
`pydantic-ai-slim[anthropic]>=2.0.0` extra is not the cause (a floor cannot cap a
resolution) — check for an unexpected upper bound before proceeding, because the
plan's `UsageLimits(cost_limit=...)` budget guard does not exist below 2.33.

- [ ] **Step 7: Commit**

```bash
git add experiments/dabstep-contract-eval/
git commit -m "experiment: scaffold DABStep eval project with pinned model pricing"
```

---

### Task 2: DABStep data fetch into DuckDB

**Files:**
- Create: `experiments/dabstep-contract-eval/dce/data.py`
- Test: `experiments/dabstep-contract-eval/tests/test_data.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `CONTEXT_FILES: dict[str, str]` (table name -> repo-relative source path), `download_context(dest: Path) -> dict[str, Path]`, `build_duckdb(files: dict[str, Path], db_path: Path) -> None`, `load_tasks(split: str) -> list[dict]`.

Only `build_duckdb` and `load_tasks`' parsing are unit-tested; the two network functions are exercised by hand in Step 6.

- [ ] **Step 1: Write the failing test**

`experiments/dabstep-contract-eval/tests/test_data.py`:

```python
import json
from pathlib import Path

import duckdb

from dce.data import build_duckdb


def test_build_duckdb_creates_one_table_per_source(tmp_path: Path):
    csv = tmp_path / "acquirer_countries.csv"
    csv.write_text("acquirer,country\nbank_a,NL\nbank_b,BE\n")
    js = tmp_path / "merchant_data.json"
    js.write_text(json.dumps([{"merchant": "Crossfit_Hanna", "mcc": 7997}]))

    db = tmp_path / "dabstep.duckdb"
    build_duckdb({"acquirer_countries": csv, "merchant_data": js}, db)

    con = duckdb.connect(str(db))
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert tables == {"acquirer_countries", "merchant_data"}
    assert con.execute("SELECT count(*) FROM acquirer_countries").fetchone()[0] == 2
    assert con.execute("SELECT merchant FROM merchant_data").fetchone()[0] == (
        "Crossfit_Hanna"
    )


def test_build_duckdb_is_idempotent(tmp_path: Path):
    # Re-running the loader must not double the rows; the sweep may restart.
    csv = tmp_path / "acquirer_countries.csv"
    csv.write_text("acquirer,country\nbank_a,NL\n")
    db = tmp_path / "dabstep.duckdb"

    build_duckdb({"acquirer_countries": csv}, db)
    build_duckdb({"acquirer_countries": csv}, db)

    con = duckdb.connect(str(db))
    assert con.execute("SELECT count(*) FROM acquirer_countries").fetchone()[0] == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd experiments/dabstep-contract-eval && uv run pytest tests/test_data.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.data'`

- [ ] **Step 3: Write the implementation**

`experiments/dabstep-contract-eval/dce/data.py`:

```python
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


def load_tasks(split: str) -> list[dict]:
    """Load the DABStep task rows. split is 'default' (450) or 'dev' (10)."""
    from huggingface_hub import hf_hub_download
    import json

    path = hf_hub_download(
        repo_id=DATASET,
        filename=f"data/tasks/{split}.jsonl",
        repo_type="dataset",
    )
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd experiments/dabstep-contract-eval && uv run pytest -v`
Expected: 5 passed

- [ ] **Step 5: Verify the real download by hand**

```bash
cd experiments/dabstep-contract-eval
uv run python -c "
from pathlib import Path
from dce.data import download_context, build_duckdb, CONTEXT_FILES
files = download_context(Path('data/hf'))
print({k: f'{v.stat().st_size/1e6:.1f} MB' for k, v in files.items()})
build_duckdb({k: files[k] for k in CONTEXT_FILES}, Path('data/dabstep.duckdb'))
"
```

**Record `payments.csv`'s actual size** — the spec flags the HF listing as implausible. If it is genuinely tens of GB, stop and report; the fallback is to project only the columns the benchmark uses during the `read_csv_auto` step. If `data/tasks/{split}.jsonl` 404s, list the repo tree with `huggingface_hub.list_repo_files(DATASET, repo_type='dataset')` and correct the path before continuing.

- [ ] **Step 6: Commit**

```bash
git add experiments/dabstep-contract-eval/dce/data.py experiments/dabstep-contract-eval/tests/test_data.py
git commit -m "experiment: fetch DABStep context into a local DuckDB"
```

---

### Task 3: Gold reconstruction and the feasibility gate

**This task is the project's go/no-go. It spends no money and must complete before any model runs.**

**Files:**
- Create: `experiments/dabstep-contract-eval/dce/golds.py`
- Test: `experiments/dabstep-contract-eval/tests/test_golds.py`

**Interfaces:**
- Consumes: `dce.grade.normalize` is *not* available yet, so this task carries its own minimal `_norm` helper; Task 4 does not replace it (the two normalizations serve different purposes — consensus vs scoring).
- Produces: `reconstruct(submissions: dict[str, dict[str, str]], scores: dict[str, dict[str, bool]], min_agreement: int = 2) -> tuple[dict[str, str], dict[str, str]]` returning `(golds, exclusions)` where `exclusions` maps task_id -> reason; `check_dev_gate(golds: dict[str, str], dev_tasks: list[dict]) -> tuple[bool, list[str]]`.

- [ ] **Step 1: Write the failing test**

`experiments/dabstep-contract-eval/tests/test_golds.py`:

```python
from dce.golds import check_dev_gate, reconstruct


def test_two_agreeing_correct_submissions_make_a_gold():
    submissions = {
        "agent_a": {"1": "0.12"},
        "agent_b": {"1": "0.12"},
    }
    scores = {"agent_a": {"1": True}, "agent_b": {"1": True}}
    golds, exclusions = reconstruct(submissions, scores)
    assert golds == {"1": "0.12"}
    assert exclusions == {}


def test_one_correct_submission_is_not_enough():
    submissions = {"agent_a": {"1": "0.12"}}
    scores = {"agent_a": {"1": True}}
    golds, exclusions = reconstruct(submissions, scores)
    assert golds == {}
    assert exclusions["1"] == "insufficient_agreement"


def test_incorrect_submissions_are_ignored_even_when_they_agree():
    submissions = {
        "agent_a": {"1": "wrong"},
        "agent_b": {"1": "wrong"},
        "agent_c": {"1": "0.12"},
        "agent_d": {"1": "0.12"},
    }
    scores = {
        "agent_a": {"1": False},
        "agent_b": {"1": False},
        "agent_c": {"1": True},
        "agent_d": {"1": True},
    }
    golds, _ = reconstruct(submissions, scores)
    assert golds == {"1": "0.12"}


def test_correct_submissions_that_disagree_are_excluded_not_guessed():
    # Both scored correct but their text differs beyond normalization: the
    # leaderboard scorer accepted something our normalizer cannot reconcile,
    # so we must not pick a winner.
    submissions = {"a": {"1": "0.12"}, "b": {"1": "totally other"}}
    scores = {"a": {"1": True}, "b": {"1": True}}
    golds, exclusions = reconstruct(submissions, scores)
    assert golds == {}
    assert exclusions["1"] == "conflicting_golds"


def test_agreement_is_normalization_insensitive():
    submissions = {"a": {"1": "['C']"}, "b": {"1": "C"}}
    scores = {"a": {"1": True}, "b": {"1": True}}
    golds, _ = reconstruct(submissions, scores)
    assert "1" in golds


def test_tasks_absent_from_every_submission_are_excluded():
    golds, exclusions = reconstruct({"a": {"1": "x"}}, {"a": {"1": False}})
    assert golds == {}
    assert exclusions["1"] == "no_correct_submission"


def test_dev_gate_passes_when_every_dev_answer_is_reproduced():
    dev = [{"task_id": "1", "answer": "0.12"}, {"task_id": "2", "answer": "NL"}]
    ok, mismatches = check_dev_gate({"1": "0.12", "2": "NL"}, dev)
    assert ok is True
    assert mismatches == []


def test_dev_gate_fails_loudly_on_one_mismatch():
    dev = [{"task_id": "1", "answer": "0.12"}, {"task_id": "2", "answer": "NL"}]
    ok, mismatches = check_dev_gate({"1": "0.12", "2": "BE"}, dev)
    assert ok is False
    assert mismatches == ["2"]


def test_dev_gate_fails_when_a_dev_task_was_never_reconstructed():
    # The spec's unverified assumption: dev tasks may not appear in submissions
    # at all. Missing must fail the gate, not pass it vacuously.
    dev = [{"task_id": "1", "answer": "0.12"}]
    ok, mismatches = check_dev_gate({}, dev)
    assert ok is False
    assert mismatches == ["1"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd experiments/dabstep-contract-eval && uv run pytest tests/test_golds.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.golds'`

- [ ] **Step 3: Write the implementation**

`experiments/dabstep-contract-eval/dce/golds.py`:

```python
"""Reconstruct DABStep gold answers from published leaderboard artifacts.

DABStep withholds golds for the 450 test tasks. Where at least two independently
submitted agents were *scored correct* on a task and their answers agree, that
answer is the gold. One correct submission is not enough: a normalized
comparison can pass by luck.

This yields golds only for tasks some agent already solved, so the scorable
subset is easier than the full benchmark. See the spec's "Selection bias"
section — the bias is disclosed, not corrected.
"""

from __future__ import annotations

import re
from collections import defaultdict

_BRACKETS = re.compile(r"^[\[\('\"]+|[\]\)'\"]+$")


def _norm(value: str) -> str:
    """Loose normalization for *agreement*, not for scoring."""
    text = _BRACKETS.sub("", str(value).strip()).strip().lower()
    try:
        return f"{float(text):.6f}"
    except ValueError:
        return re.sub(r"\s+", " ", text)


def reconstruct(
    submissions: dict[str, dict[str, str]],
    scores: dict[str, dict[str, bool]],
    min_agreement: int = 2,
) -> tuple[dict[str, str], dict[str, str]]:
    """Return (golds, exclusions). exclusions maps task_id -> reason."""
    by_task: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    seen: set[str] = set()

    for agent, answers in submissions.items():
        for task_id, answer in answers.items():
            seen.add(task_id)
            if scores.get(agent, {}).get(task_id):
                by_task[task_id][_norm(answer)].append(answer)

    golds: dict[str, str] = {}
    exclusions: dict[str, str] = {}

    for task_id in sorted(seen):
        groups = by_task.get(task_id, {})
        if not groups:
            exclusions[task_id] = "no_correct_submission"
            continue
        if len(groups) > 1:
            exclusions[task_id] = "conflicting_golds"
            continue
        (raws,) = groups.values()
        if len(raws) < min_agreement:
            exclusions[task_id] = "insufficient_agreement"
            continue
        golds[task_id] = raws[0]

    return golds, exclusions


def check_dev_gate(
    golds: dict[str, str], dev_tasks: list[dict]
) -> tuple[bool, list[str]]:
    """Reconstruction must reproduce every published dev answer exactly.

    A dev task missing from `golds` fails the gate. Treating absence as a pass
    would make the gate vacuous — which is precisely the unverified assumption
    the spec flags.
    """
    mismatches = [
        task["task_id"]
        for task in dev_tasks
        if _norm(golds.get(task["task_id"], "\x00missing")) != _norm(task["answer"])
    ]
    return (not mismatches), mismatches
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd experiments/dabstep-contract-eval && uv run pytest -v`
Expected: 14 passed

- [ ] **Step 5: Run the real gate and STOP for review**

```bash
cd experiments/dabstep-contract-eval
uv run python -c "
from huggingface_hub import list_repo_files
files = list_repo_files('adyen/DABstep', repo_type='dataset')
print('submissions:', len([f for f in files if f.startswith('data/submissions/')]))
print('task_scores:', [f for f in files if f.startswith('data/task_scores/')][:5])
"
```

Then load those files, run `reconstruct`, run `check_dev_gate`, and report:
1. how many of the 450 tasks got a gold,
2. the exclusion counts by reason,
3. gold coverage split by `level` and by template,
4. the dev-gate verdict.

**Do not proceed past this step without a human decision.** Three outcomes:
- **Gate passes** → continue to Task 4.
- **Dev tasks are absent from the submission files** → the gate cannot run as designed. Substitute the spec's hand-verified sample: write SQL by hand for 10 reconstructed golds stratified across templates, confirm each. Weaker evidence; label it as such in `FINDINGS.md`.
- **Gate fails on a mismatch** → the method is unsound. Fall back to the 10 `dev` tasks only plus a leaderboard submission, per the spec.

- [ ] **Step 6: Write the one-shot preparation entrypoint**

The README tells a reader to run `python -m dce.prepare`; this is that module. It
wires Tasks 2 and 3 together so preparation is one reproducible command rather
than a sequence of pasted snippets.

`experiments/dabstep-contract-eval/dce/prepare.py`:

```python
"""One-shot preparation: download, build the DuckDB, reconstruct golds, gate."""

from __future__ import annotations

import json
from pathlib import Path

from dce.data import CONTEXT_FILES, build_duckdb, download_context, load_tasks
from dce.golds import check_dev_gate, reconstruct


def load_leaderboard_artifacts() -> tuple[dict, dict]:
    """Return (submissions, scores) keyed by agent name then task id.

    Fill in the exact filenames after inspecting the repo tree in Task 3 Step 5;
    the shapes are {agent: {task_id: answer}} and {agent: {task_id: bool}}.
    """
    raise NotImplementedError("wire to the real submission/score file layout")


def main() -> None:
    data = Path("data")
    files = download_context(data / "hf")
    build_duckdb({k: files[k] for k in CONTEXT_FILES}, data / "dabstep.duckdb")

    tasks = load_tasks("default")
    (data / "tasks.json").write_text(json.dumps(tasks, indent=2))

    submissions, scores = load_leaderboard_artifacts()
    golds, exclusions = reconstruct(submissions, scores)
    ok, mismatches = check_dev_gate(golds, load_tasks("dev"))

    (data / "golds.json").write_text(json.dumps(golds, indent=2))
    (data / "exclusions.json").write_text(json.dumps(exclusions, indent=2))

    print(f"golds: {len(golds)} / {len(tasks)} tasks")
    for reason in sorted(set(exclusions.values())):
        print(f"  excluded {sum(v == reason for v in exclusions.values())}: {reason}")
    print(f"dev gate: {'PASS' if ok else 'FAIL — ' + ', '.join(mismatches)}")
    if not ok:
        raise SystemExit("dev gate failed; see the spec's fallback before spending")


if __name__ == "__main__":
    main()
```

`load_leaderboard_artifacts` is the one function this plan cannot write blind —
its file layout is discovered in Step 5. Implement it there, against the real
tree, before running `main()`.

- [ ] **Step 7: Commit**

```bash
git add experiments/dabstep-contract-eval/dce/golds.py experiments/dabstep-contract-eval/dce/prepare.py experiments/dabstep-contract-eval/tests/test_golds.py
git commit -m "experiment: reconstruct DABStep golds from leaderboard artifacts with a dev-set gate"
```

---

### Task 4: Scoring

**Files:**
- Create: `experiments/dabstep-contract-eval/dce/grade.py`
- Test: `experiments/dabstep-contract-eval/tests/test_grade.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `score(predicted: str, gold: str) -> bool`.

- [ ] **Step 1: Write the failing test**

`experiments/dabstep-contract-eval/tests/test_grade.py`:

```python
from dce.grade import score


def test_exact_and_case_insensitive_match():
    assert score("NL", "NL")
    assert score("nl", "NL")


def test_bracket_and_quote_normalization():
    assert score("['C']", "C")
    assert score('"C"', "C")


def test_float_tolerance_is_relative_and_tight():
    assert score("0.120132", "0.1201320000001")
    assert not score("0.120132", "0.120133")


def test_na_equivalence():
    assert score("", "N/A")
    assert score("none", "N/A")
    assert score("null", "N/A")


def test_comma_lists_compare_order_insensitively():
    assert score("fee_3, fee_1", "fee_1,fee_3")


def test_a_wrong_answer_is_wrong():
    assert not score("BE", "NL")
    assert not score("fee_1", "fee_1, fee_2")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd experiments/dabstep-contract-eval && uv run pytest tests/test_grade.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.grade'`

- [ ] **Step 3: Write the implementation**

`experiments/dabstep-contract-eval/dce/grade.py`:

```python
"""DABStep answer scoring.

Prefers the official scorer when installable so results stay comparable with
the published leaderboard; the fallback mirrors its documented behaviour.
"""

from __future__ import annotations

import re

_NA = {"", "n/a", "na", "none", "null", "not applicable"}
_BRACKETS = re.compile(r"^[\[\('\"]+|[\]\)'\"]+$")


def _official(predicted: str, gold: str) -> bool | None:
    try:
        from dabstep_benchmark.evaluation.scorer import question_scorer
    except ImportError:
        return None
    return bool(question_scorer(predicted, gold))


def _clean(value: str) -> str:
    return _BRACKETS.sub("", str(value).strip()).strip()


def _as_float(value: str) -> float | None:
    try:
        return float(value.replace(",", "") if value.count(",") == 1 else value)
    except ValueError:
        return None


def _scalar_match(predicted: str, gold: str) -> bool:
    p, g = _clean(predicted), _clean(gold)
    if p.lower() in _NA and g.lower() in _NA:
        return True
    pf, gf = _as_float(p), _as_float(g)
    if pf is not None and gf is not None:
        scale = max(abs(pf), abs(gf), 1e-12)
        return abs(pf - gf) / scale < 1e-9
    return p.lower() == g.lower()


def score(predicted: str, gold: str) -> bool:
    """True when `predicted` matches `gold` under DABStep's rules."""
    official = _official(predicted, gold)
    if official is not None:
        return official

    p, g = _clean(predicted), _clean(gold)
    if "," in g:
        pl = sorted(_clean(x).lower() for x in p.split(",") if _clean(x))
        gl = sorted(_clean(x).lower() for x in g.split(",") if _clean(x))
        return pl == gl
    return _scalar_match(p, g)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd experiments/dabstep-contract-eval && uv run pytest -v`
Expected: 20 passed

- [ ] **Step 5: Commit**

```bash
git add experiments/dabstep-contract-eval/dce/grade.py experiments/dabstep-contract-eval/tests/test_grade.py
git commit -m "experiment: DABStep answer scorer with official-scorer passthrough"
```

---

### Task 5: Author and freeze the contract

**This task creates the artifact under test. Read the spec's "Freeze discipline" section before starting.**

**Files:**
- Create: `experiments/dabstep-contract-eval/contract/contract.yml`
- Create: `experiments/dabstep-contract-eval/contract/semantic.yml`
- Create: `experiments/dabstep-contract-eval/dce/frozen.py`
- Test: `experiments/dabstep-contract-eval/tests/test_frozen.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `CONTRACT_PATH: Path`, `load_contract() -> DataContract`, `digest() -> str`.

**Authoring rules — non-negotiable:**
- Source material is **only** `manual.md` and `payments-readme.md` (downloaded in Task 2).
- **Do not read any DABStep question, gold answer, or score while authoring.** Arm B gets the same two documents verbatim; if the contract encodes anything they do not contain, the arms are no longer information-equivalent and the comparison is void.
- Model the manual's fee rules as metrics with `sql_expression`, its merchant/account-type vocabulary as domains, and the five tables as `allowed_tables`.
- Copy the patterns in `examples/revenue_agent/contract.yml` and `examples/revenue_agent/semantic.yml`. Domain membership is metric-first: metrics declare `domains: [...]`, and the contract's domain blocks never list their metrics.

- [ ] **Step 1: Write the contract skeleton**

`experiments/dabstep-contract-eval/contract/contract.yml`:

```yaml
version: "1.0"
name: dabstep-payments

semantic:
  source:
    type: yaml
    path: "./semantic.yml"
    expected_extras: []
  allowed_tables:
    - schema: main
      description: "DABStep payments context tables"
      preferred: true
      tables:
        [payments, fees, merchant_data, acquirer_countries, merchant_category_codes]
  forbidden_operations:
    [DELETE, DROP, TRUNCATE, UPDATE, INSERT, CREATE, ALTER, MERGE, GRANT, REVOKE, COPY]
  domains:
    - name: fees
      summary: "Scheme fee rules and how a transaction is matched to a fee"
      description: >
        Replace this text with what manual.md states about fee matching: which
        fields participate, how a null field in a fee rule behaves, and the
        order rules are applied in. Write it from the file open in front of you,
        never from memory — this text is the contract's half of an
        information-equivalence claim against arm B.
      tables: [main.fees, main.payments]
  rules:
    - name: no_select_star
      description: "Queries must name their columns"
      enforcement: block
      query_check:
        forbid_select_star: true
  limits:
    max_rows: 10000
```

`experiments/dabstep-contract-eval/contract/semantic.yml` follows this shape — one entry per rule the manual states, with the placeholder text replaced by what `manual.md` actually says:

```yaml
metrics:
  - name: transaction_fee
    description: >
      One sentence, in the manual's own terms, for what this computes and when
      it applies.
    sql_expression: "<the arithmetic manual.md specifies, as SQL>"
    source_model: main.fees
    domains: [fees]
  # ... one metric per distinct computation the manual defines
```

**Authoring checklist — work through `manual.md` top to bottom and, for each rule it states, decide where it lands:**

1. **A computation** (a fee formula, a rate, an aggregate) → a metric with a real `sql_expression`. This is the bulk of the contract and the reason arm C can exist.
2. **A matching or applicability rule** ("a null field in a fee rule matches any value") → the `description` of the metric it governs, or the domain description. Prose here is fine; it is *structured* prose attached to the thing it governs.
3. **A vocabulary** (account types, merchant categories, capture delays) → a domain `description`, or a `column_hints` extra if you add one to `expected_extras`.
4. **A constraint on querying** → a contract `rule` with a `query_check`.

Nothing in `manual.md` may be dropped silently. If a statement fits none of the four, note it in a YAML comment saying so — an unencodable rule is itself a finding worth reporting in `FINDINGS.md`, because it marks a limit of what a contract can carry.

- [ ] **Step 2: Write the failing test**

`experiments/dabstep-contract-eval/tests/test_frozen.py`:

```python
from agentic_data_contracts import DataContract

from dce.frozen import digest, load_contract


def test_contract_loads():
    contract = load_contract()
    assert isinstance(contract, DataContract)


def test_digest_is_stable_across_calls():
    assert digest() == digest()


def test_contract_declares_the_five_dabstep_tables():
    contract = load_contract()
    rendered = contract.to_system_prompt()
    for table in (
        "payments",
        "fees",
        "merchant_data",
        "acquirer_countries",
        "merchant_category_codes",
    ):
        assert table in rendered


def test_contract_defines_at_least_one_metric():
    # A contract with no metrics would make arm C a schema-only arm wearing a
    # contract's clothes, and the whole comparison meaningless.
    contract = load_contract()
    assert contract.semantic_source is not None
    assert len(contract.semantic_source.list_metrics()) >= 1
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd experiments/dabstep-contract-eval && uv run pytest tests/test_frozen.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.frozen'`

- [ ] **Step 4: Write the implementation**

`experiments/dabstep-contract-eval/dce/frozen.py`:

```python
"""The frozen contract under test.

Its digest is stamped into every result row so a post-hoc edit is detectable
against git history rather than merely promised in prose.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from agentic_data_contracts import DataContract, contract_digest

CONTRACT_PATH = Path(__file__).parent.parent / "contract" / "contract.yml"


@lru_cache(maxsize=1)
def load_contract() -> DataContract:
    return DataContract.from_yaml(CONTRACT_PATH)


def digest() -> str:
    return contract_digest(load_contract())
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd experiments/dabstep-contract-eval && uv run pytest -v`
Expected: 24 passed

If `contract.semantic_source` or `list_metrics()` is not the accessor this library version exposes, find the real one with `uv run python -c "from dce.frozen import load_contract; print([a for a in dir(load_contract()) if not a.startswith('_')])"` and fix the test to match the library — do not weaken the assertion.

- [ ] **Step 6: Commit the freeze**

```bash
git add experiments/dabstep-contract-eval/contract/ experiments/dabstep-contract-eval/dce/frozen.py experiments/dabstep-contract-eval/tests/test_frozen.py
git commit -m "experiment: freeze the DABStep contract, authored from manual.md only"
cd experiments/dabstep-contract-eval && uv run python -c "from dce.frozen import digest; print('FROZEN DIGEST:', digest())"
```

**Record the printed digest in the commit message body or in `FINDINGS.md` immediately.** From this point, any edit under `contract/` invalidates every scored result.

---

### Task 6: Arm assembly

**Files:**
- Create: `experiments/dabstep-contract-eval/dce/arms.py`
- Test: `experiments/dabstep-contract-eval/tests/test_arms.py`

**Interfaces:**
- Consumes: `dce.frozen.load_contract`.
- Produces: `ARMS: tuple[str, ...]`, `build_arm(arm: str, db_path: Path, docs: dict[str, str]) -> ArmSetup` where `ArmSetup` is a dataclass with `system_prompt: str`, `tools: list`, `session` (a `ContractSession` for arm `contract`, else `None`).

- [ ] **Step 1: Write the failing test**

`experiments/dabstep-contract-eval/tests/test_arms.py`:

```python
from pathlib import Path

import pytest

from dce.arms import ARMS, build_arm

DOCS = {"manual": "FEE RULE ALPHA: match on card_scheme.", "payments_readme": "cols"}


@pytest.fixture
def db(tmp_path: Path) -> Path:
    import duckdb

    path = tmp_path / "t.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE payments AS SELECT 1 AS psp_reference")
    con.close()
    return path


def test_three_arms_with_the_spec_names():
    assert ARMS == ("schema_only", "manual_prompt", "contract")


def test_schema_only_prompt_contains_no_manual_text(db):
    setup = build_arm("schema_only", db, DOCS)
    assert "FEE RULE ALPHA" not in setup.system_prompt


def test_manual_prompt_contains_the_manual_verbatim(db):
    setup = build_arm("manual_prompt", db, DOCS)
    assert "FEE RULE ALPHA: match on card_scheme." in setup.system_prompt


def test_contract_arm_prompt_contains_no_verbatim_manual_text(db):
    # Arm C must carry the manual's knowledge as structure, not as prose.
    setup = build_arm("contract", db, DOCS)
    assert "FEE RULE ALPHA" not in setup.system_prompt


def test_only_the_contract_arm_gets_the_governed_tools(db):
    names = {t.name for t in build_arm("contract", db, DOCS).tools}
    assert {"inspect_query", "run_query", "lookup_metric"} <= names

    for arm in ("schema_only", "manual_prompt"):
        ungoverned = {t.name for t in build_arm(arm, db, DOCS).tools}
        assert "inspect_query" not in ungoverned
        assert "lookup_metric" not in ungoverned


def test_ungoverned_arms_still_execute_sql(db):
    setup = build_arm("schema_only", db, DOCS)
    assert "execute_sql" in {t.name for t in setup.tools}


def test_only_the_contract_arm_carries_a_session(db):
    assert build_arm("contract", db, DOCS).session is not None
    assert build_arm("schema_only", db, DOCS).session is None


def test_unknown_arm_raises(db):
    with pytest.raises(ValueError):
        build_arm("some_other_arm", db, DOCS)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd experiments/dabstep-contract-eval && uv run pytest tests/test_arms.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.arms'`

- [ ] **Step 3: Write the implementation**

`experiments/dabstep-contract-eval/dce/arms.py`:

```python
"""The three context configurations under comparison.

Arms differ only in what context reaches the agent. Arms `schema_only` and
`manual_prompt` deliberately bypass the library entirely — if they used its
`run_query`, they would inherit contract enforcement and arm `contract` would no
longer be the only governed arm.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
from pydantic_ai import Tool

from dce.frozen import load_contract

ARMS: tuple[str, ...] = ("schema_only", "manual_prompt", "contract")

MAX_ROWS = 10_000  # harness property, mirrored from the contract's limits

BASE_PROMPT = (
    "You are a data analyst answering questions over a DuckDB database.\n"
    "Explore the schema, write SQL, and verify your result before answering.\n"
    "Your final message must be the answer alone, formatted exactly as the "
    "question's guidelines require. Do not show working in the final message."
)


@dataclass
class ArmSetup:
    system_prompt: str
    tools: list[Tool]
    session: object | None


def _ungoverned_tools(db_path: Path) -> list[Tool]:
    """Plain schema access + query execution. No contract, no validation."""

    def list_tables() -> str:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            return "\n".join(r[0] for r in con.execute("SHOW TABLES").fetchall())
        finally:
            con.close()

    def describe_table(table: str) -> str:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            rows = con.execute(f"DESCRIBE {table}").fetchall()
            return "\n".join(f"{r[0]}: {r[1]}" for r in rows)
        finally:
            con.close()

    def execute_sql(sql: str) -> str:
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(MAX_ROWS)
            return "\n".join([",".join(cols), *(",".join(map(str, r)) for r in rows)])
        except Exception as exc:  # surfaced to the model, same as arm C's errors
            return f"ERROR: {exc}"
        finally:
            con.close()

    return [Tool(list_tables), Tool(describe_table), Tool(execute_sql)]


def _governed_tools(db_path: Path):
    from agentic_data_contracts import create_pydantic_ai_tools
    from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
    from agentic_data_contracts.core.session import ContractSession

    contract = load_contract()
    session = ContractSession(contract)
    tools = create_pydantic_ai_tools(
        contract,
        adapter=DuckDBAdapter(database=str(db_path)),
        session=session,
    )
    return tools, session


def build_arm(arm: str, db_path: Path, docs: dict[str, str]) -> ArmSetup:
    if arm == "schema_only":
        return ArmSetup(BASE_PROMPT, _ungoverned_tools(db_path), None)

    if arm == "manual_prompt":
        prompt = (
            f"{BASE_PROMPT}\n\n## Domain manual\n\n{docs['manual']}\n\n"
            f"## Payments table reference\n\n{docs['payments_readme']}"
        )
        return ArmSetup(prompt, _ungoverned_tools(db_path), None)

    if arm == "contract":
        tools, session = _governed_tools(db_path)
        prompt = (
            f"{BASE_PROMPT}\n\n"
            "A data contract governs this database. Look up the domain and the "
            "metrics that apply before writing SQL, and validate every query "
            "with inspect_query before running it.\n\n"
            f"{load_contract().to_system_prompt()}"
        )
        return ArmSetup(prompt, tools, session)

    raise ValueError(f"unknown arm: {arm!r}; expected one of {ARMS}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd experiments/dabstep-contract-eval && uv run pytest -v`
Expected: 32 passed

If `to_system_prompt()` emits the manual's exact wording (because the contract quoted it), `test_contract_arm_prompt_contains_no_verbatim_manual_text` fails. That is a **real finding, not a test bug**: reword the contract's descriptions so arm C carries structure rather than prose, then re-freeze and re-record the digest.

- [ ] **Step 5: Commit**

```bash
git add experiments/dabstep-contract-eval/dce/arms.py experiments/dabstep-contract-eval/tests/test_arms.py
git commit -m "experiment: three arm configurations, only the contract arm governed"
```

---

### Task 7: Agent loop with caps and cost accounting

**Files:**
- Create: `experiments/dabstep-contract-eval/dce/agent.py`
- Test: `experiments/dabstep-contract-eval/tests/test_agent.py`

**Interfaces:**
- Consumes: `dce.arms.build_arm`, `dce.pricing.cost`, `dce.grade.score`, `dce.frozen.digest`.
- Produces: `run_task(task: dict, arm: str, model: str, db_path: Path, docs: dict[str, str], gold: str, *, max_tool_calls: int = 25, per_task_usd: float = 0.25, agent_factory=None) -> dict` returning a result row.

- [ ] **Step 1: Write the failing test**

`experiments/dabstep-contract-eval/tests/test_agent.py`:

```python
from pathlib import Path

import pytest

from dce.agent import build_result_row, run_task

TASK = {"task_id": "7", "question": "What is X?", "guidelines": "Answer with a number.", "level": "hard"}


def test_result_row_carries_full_provenance():
    row = build_result_row(
        task=TASK,
        arm="contract",
        model="deepseek/deepseek-v4-pro-0813",
        answer="0.12",
        gold="0.12",
        verdict="correct",
        in_tok=30_000,
        out_tok=2_000,
        cached_tok=0,
        tool_calls=["lookup_metric", "inspect_query", "run_query"],
        inspect_rejections=1,
    )
    for field in (
        "task_id", "level", "arm", "model", "answer", "gold", "verdict",
        "input_tokens", "output_tokens", "cached_tokens", "usd",
        "tool_calls", "inspect_rejections", "contract_digest", "commit_sha",
        "adc_version",
    ):
        assert field in row, field


def test_result_row_prices_from_the_pinned_table():
    row = build_result_row(
        task=TASK, arm="contract", model="deepseek/deepseek-v4-pro-0813",
        answer="x", gold="y", verdict="incorrect",
        in_tok=1_000_000, out_tok=0, cached_tok=0, tool_calls=[], inspect_rejections=0,
    )
    assert row["usd"] == pytest.approx(0.66)


def test_run_task_records_a_cap_trip_as_hit_limit_not_incorrect(tmp_path: Path):
    class Exploded:
        def run_sync(self, *a, **k):
            from pydantic_ai.exceptions import UsageLimitExceeded

            raise UsageLimitExceeded("tool call limit")

    row = run_task(
        TASK, "schema_only", "z-ai/glm-5.3-flash", tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"}, gold="0.12",
        agent_factory=lambda **_: Exploded(),
    )
    # A cap trip is a harness artifact. Scoring it as a wrong answer would let
    # a too-tight cap masquerade as an arm being worse at reasoning.
    assert row["verdict"] == "hit_limit"


def _fake_result(output: str, tool_names: list[str] | None = None):
    class Part:
        def __init__(self, name):
            self.part_kind = "tool-call"
            self.tool_name = name

    class Msg:
        def __init__(self, names):
            self.parts = [Part(n) for n in names]

    class R:
        def __init__(self):
            self.output = output

        def usage(self):
            class U:
                input_tokens = 100
                output_tokens = 10
                cache_read_tokens = 0

            return U()

        def all_messages(self):
            return [Msg(tool_names or [])]

    return R()


def test_run_task_scores_the_final_message(tmp_path: Path):
    class Fake:
        def run_sync(self, *a, **k):
            return _fake_result("0.12")

    row = run_task(
        TASK, "schema_only", "z-ai/glm-5.3-flash", tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"}, gold="0.12",
        agent_factory=lambda **_: Fake(),
    )
    assert row["verdict"] == "correct"
    assert row["answer"] == "0.12"


def test_run_task_records_the_tool_call_sequence(tmp_path: Path):
    # The sequence is how we tell whether arm C actually used progressive
    # disclosure or ignored the lookup tools — MotherDuck's central finding.
    class Fake:
        def run_sync(self, *a, **k):
            return _fake_result(
                "0.12", ["lookup_domain", "lookup_metric", "inspect_query", "run_query"]
            )

    row = run_task(
        TASK, "contract", "z-ai/glm-5.3-flash", tmp_path / "x.duckdb",
        {"manual": "m", "payments_readme": "r"}, gold="0.12",
        agent_factory=lambda **_: Fake(),
    )
    assert row["tool_calls"] == [
        "lookup_domain", "lookup_metric", "inspect_query", "run_query"
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd experiments/dabstep-contract-eval && uv run pytest tests/test_agent.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.agent'`

- [ ] **Step 3: Write the implementation**

`experiments/dabstep-contract-eval/dce/agent.py`:

```python
"""Run one (task, arm, model) and return a fully provenanced result row."""

from __future__ import annotations

import os
import subprocess
from importlib.metadata import version
from pathlib import Path

from dce.arms import build_arm
from dce.frozen import digest
from dce.grade import score
from dce.pricing import cost


def _tool_call_names(result) -> list[str]:
    """Ordered tool names from a run's message history.

    Whether arm C actually called the lookup tools, or ignored them and went
    straight to SQL, is the behavioural half of the result — an arm C that never
    calls `lookup_metric` is not testing what we think it is.
    """
    names: list[str] = []
    try:
        for message in result.all_messages():
            for part in getattr(message, "parts", []):
                if getattr(part, "part_kind", "") == "tool-call":
                    names.append(part.tool_name)
    except Exception:
        pass
    return names


def _commit_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except Exception:
        return "unknown"


def build_result_row(
    *, task, arm, model, answer, gold, verdict, in_tok, out_tok, cached_tok,
    tool_calls, inspect_rejections,
) -> dict:
    return {
        "task_id": task["task_id"],
        "level": task.get("level", "unknown"),
        "arm": arm,
        "model": model,
        "answer": answer,
        "gold": gold,
        "verdict": verdict,
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "cached_tokens": cached_tok,
        "usd": cost(model, in_tok, out_tok),
        "tool_calls": tool_calls,
        "inspect_rejections": inspect_rejections,
        "contract_digest": digest(),
        "commit_sha": _commit_sha(),
        "adc_version": version("agentic-data-contracts"),
    }


def _default_agent_factory(*, model: str, system_prompt: str, tools: list):
    from pydantic_ai import Agent
    from pydantic_ai.models.openai import OpenAIChatModel
    from pydantic_ai.providers.openrouter import OpenRouterProvider
    from pydantic_ai.settings import ModelSettings

    return Agent(
        OpenAIChatModel(
            model, provider=OpenRouterProvider(api_key=os.environ["OPENROUTER_API_KEY"])
        ),
        system_prompt=system_prompt,
        tools=tools,
        model_settings=ModelSettings(temperature=0.0, seed=0, timeout=300),
    )


def run_task(
    task: dict,
    arm: str,
    model: str,
    db_path: Path,
    docs: dict[str, str],
    gold: str,
    *,
    max_tool_calls: int = 25,
    per_task_usd: float = 0.25,
    agent_factory=None,
) -> dict:
    from decimal import Decimal

    from pydantic_ai.exceptions import UsageLimitExceeded
    from pydantic_ai.usage import UsageLimits

    setup = build_arm(arm, db_path, docs)
    factory = agent_factory or _default_agent_factory
    agent = factory(model=model, system_prompt=setup.system_prompt, tools=setup.tools)

    prompt = f"{task['question']}\n\nAnswer guidelines: {task.get('guidelines', '')}"
    limits = UsageLimits(
        tool_calls_limit=max_tool_calls, cost_limit=Decimal(str(per_task_usd))
    )

    answer, verdict, in_tok, out_tok, cached = "", "hit_limit", 0, 0, 0
    tool_calls: list[str] = []
    try:
        result = agent.run_sync(prompt, usage_limits=limits)
        usage = result.usage()
        in_tok = usage.input_tokens
        out_tok = usage.output_tokens
        cached = getattr(usage, "cache_read_tokens", 0) or 0
        answer = str(result.output).strip()
        tool_calls = _tool_call_names(result)
        verdict = "correct" if score(answer, gold) else "incorrect"
    except UsageLimitExceeded:
        verdict = "hit_limit"
    except Exception as exc:
        verdict = "error"
        answer = f"{type(exc).__name__}: {exc}"

    rejections = 0
    if setup.session is not None:
        rejections = getattr(setup.session, "rejected_queries", 0) or 0

    return build_result_row(
        task=task, arm=arm, model=model, answer=answer, gold=gold, verdict=verdict,
        in_tok=in_tok, out_tok=out_tok, cached_tok=cached, tool_calls=tool_calls,
        inspect_rejections=rejections,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd experiments/dabstep-contract-eval && uv run pytest -v`
Expected: 37 passed

`ContractSession` may not expose `rejected_queries`. Find the real accessor with `uv run python -c "from agentic_data_contracts.core.session import ContractSession; print([a for a in dir(ContractSession) if not a.startswith('_')])"` and use it — the `inspect_query` rejection count is a required metric, so a permanent `0` here is a plan failure, not an acceptable default.

- [ ] **Step 5: Commit**

```bash
git add experiments/dabstep-contract-eval/dce/agent.py experiments/dabstep-contract-eval/tests/test_agent.py
git commit -m "experiment: per-task agent loop with uniform caps and provenanced rows"
```

---

### Task 8: Runner CLI with resume and spend cap

**Files:**
- Create: `experiments/dabstep-contract-eval/dce/runner.py`
- Test: `experiments/dabstep-contract-eval/tests/test_runner.py`

**Interfaces:**
- Consumes: `dce.agent.run_task`, `dce.arms.ARMS`, `dce.pricing.MODELS`.
- Produces: `completed_keys(path: Path) -> set[tuple[str, str, str]]`, `pending(tasks, arms, models, done) -> list[tuple]`, `sweep(...) -> float` (total USD spent), and a `python -m dce.runner` CLI.

- [ ] **Step 1: Write the failing test**

`experiments/dabstep-contract-eval/tests/test_runner.py`:

```python
import json
from pathlib import Path

from dce.runner import completed_keys, pending, sweep

TASKS = [{"task_id": "1", "question": "q", "guidelines": "g", "level": "hard"}]


def test_completed_keys_reads_existing_rows(tmp_path: Path):
    path = tmp_path / "r.jsonl"
    path.write_text(
        json.dumps({"task_id": "1", "arm": "contract", "model": "m"}) + "\n"
    )
    assert completed_keys(path) == {("1", "contract", "m")}


def test_completed_keys_on_missing_file_is_empty(tmp_path: Path):
    assert completed_keys(tmp_path / "absent.jsonl") == set()


def test_pending_skips_completed_work():
    done = {("1", "schema_only", "m")}
    todo = pending(TASKS, ("schema_only", "contract"), ("m",), done)
    assert todo == [("1", "contract", "m")]


def test_sweep_stops_before_exceeding_max_spend(tmp_path: Path):
    calls = []

    def fake_run(task, arm, model, *a, **k):
        calls.append(arm)
        return {"task_id": task["task_id"], "arm": arm, "model": model,
                "usd": 0.40, "verdict": "correct"}

    tasks = [{"task_id": str(i), "question": "q", "guidelines": "g", "level": "hard"}
             for i in range(10)]
    spent = sweep(
        tasks, ("schema_only",), ("z-ai/glm-5.3-flash",), {"1": "g"},
        out=tmp_path / "r.jsonl", db_path=tmp_path / "d", docs={},
        max_spend=1.00, run_task_fn=fake_run,
    )
    # Three calls cost 1.20, which overruns; the guard must stop at two.
    assert len(calls) == 2
    assert spent <= 1.00


def test_sweep_appends_rows_that_can_be_resumed(tmp_path: Path):
    out = tmp_path / "r.jsonl"

    def fake_run(task, arm, model, *a, **k):
        return {"task_id": task["task_id"], "arm": arm, "model": model,
                "usd": 0.01, "verdict": "correct"}

    sweep(TASKS, ("contract",), ("m",), {"1": "g"}, out=out,
          db_path=tmp_path / "d", docs={}, max_spend=1.0, run_task_fn=fake_run)
    assert completed_keys(out) == {("1", "contract", "m")}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd experiments/dabstep-contract-eval && uv run pytest tests/test_runner.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.runner'`

- [ ] **Step 3: Write the implementation**

`experiments/dabstep-contract-eval/dce/runner.py`:

```python
"""Sweep driver: resumable, budget-capped, one JSONL row per unit of work."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dce.agent import run_task
from dce.arms import ARMS
from dce.pricing import MODELS


def completed_keys(path: Path) -> set[tuple[str, str, str]]:
    if not path.exists():
        return set()
    keys = set()
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        keys.add((row["task_id"], row["arm"], row["model"]))
    return keys


def pending(tasks, arms, models, done) -> list[tuple[str, str, str]]:
    return [
        (task["task_id"], arm, model)
        for task in tasks
        for arm in arms
        for model in models
        if (task["task_id"], arm, model) not in done
    ]


def sweep(
    tasks, arms, models, golds, *, out: Path, db_path: Path, docs,
    max_spend: float, run_task_fn=run_task, per_task_usd: float = 0.25,
) -> float:
    by_id = {t["task_id"]: t for t in tasks}
    todo = pending(tasks, arms, models, completed_keys(out))
    out.parent.mkdir(parents=True, exist_ok=True)

    spent = 0.0
    last = per_task_usd
    with out.open("a") as fh:
        for task_id, arm, model in todo:
            # Reserve the worst case before calling, so the cap cannot be
            # overrun by a single expensive task.
            if spent + last > max_spend:
                print(f"stopping: {spent:.2f} + {last:.2f} would exceed {max_spend}")
                break
            if task_id not in golds:
                continue
            row = run_task_fn(
                by_id[task_id], arm, model, db_path, docs, golds[task_id],
                per_task_usd=per_task_usd,
            )
            fh.write(json.dumps(row) + "\n")
            fh.flush()
            spent += row["usd"]
            last = max(row["usd"], 0.01)
    return spent


def main() -> None:
    parser = argparse.ArgumentParser(prog="dce.runner")
    parser.add_argument("--n", type=int, default=0, help="0 = all tasks")
    parser.add_argument("--arms", nargs="+", default=list(ARMS))
    parser.add_argument("--models", nargs="+", default=["z-ai/glm-5.3-flash"])
    parser.add_argument("--max-spend", type=float, required=True)
    parser.add_argument("--out", type=Path, default=Path("results/results.jsonl"))
    parser.add_argument("--db", type=Path, default=Path("data/dabstep.duckdb"))
    parser.add_argument("--golds", type=Path, default=Path("data/golds.json"))
    parser.add_argument("--tasks", type=Path, default=Path("data/tasks.json"))
    args = parser.parse_args()

    for model in args.models:
        if model not in MODELS:
            raise SystemExit(f"unpinned or unknown model: {model}")

    golds = json.loads(args.golds.read_text())
    tasks = [t for t in json.loads(args.tasks.read_text()) if t["task_id"] in golds]
    if args.n:
        tasks = tasks[: args.n]

    docs = {
        "manual": Path("data/hf/data/context/manual.md").read_text(),
        "payments_readme": Path("data/hf/data/context/payments-readme.md").read_text(),
    }

    spent = sweep(
        tasks, tuple(args.arms), tuple(args.models), golds,
        out=args.out, db_path=args.db, docs=docs, max_spend=args.max_spend,
    )
    print(f"spent ${spent:.2f}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd experiments/dabstep-contract-eval && uv run pytest -v`
Expected: 41 passed

- [ ] **Step 5: Commit**

```bash
git add experiments/dabstep-contract-eval/dce/runner.py experiments/dabstep-contract-eval/tests/test_runner.py
git commit -m "experiment: resumable, budget-capped sweep runner"
```

---

### Task 9: Statistics

**Files:**
- Create: `experiments/dabstep-contract-eval/dce/stats.py`
- Test: `experiments/dabstep-contract-eval/tests/test_stats.py`

**Interfaces:**
- Consumes: nothing (reads JSONL).
- Produces: `wilson(successes: int, n: int, z: float = 1.96) -> tuple[float, float]`, `mcnemar(rows_a: dict[str, bool], rows_b: dict[str, bool]) -> dict`, `accuracy_by(rows: list[dict], key: str) -> dict`, `report(path: Path) -> str`.

- [ ] **Step 1: Write the failing test**

`experiments/dabstep-contract-eval/tests/test_stats.py`:

```python
import pytest

from dce.stats import accuracy_by, mcnemar, wilson


def test_wilson_interval_brackets_the_point_estimate():
    lo, hi = wilson(80, 100)
    assert lo < 0.80 < hi
    assert 0.70 < lo and hi < 0.88


def test_wilson_handles_a_perfect_score_without_a_zero_width_interval():
    # At the ceiling a naive normal interval collapses to [1.0, 1.0] and
    # would imply certainty the data does not support.
    lo, hi = wilson(100, 100)
    assert lo < 1.0
    assert hi == pytest.approx(1.0)


def test_mcnemar_reports_discordant_pairs():
    a = {"1": True, "2": False, "3": True, "4": False}
    b = {"1": True, "2": True, "3": True, "4": True}
    result = mcnemar(a, b)
    assert result["b_only"] == 2   # b right where a wrong
    assert result["a_only"] == 0
    assert result["discordant"] == 2


def test_mcnemar_with_no_discordant_pairs_is_not_significant():
    a = {"1": True, "2": False}
    result = mcnemar(a, dict(a))
    assert result["discordant"] == 0
    assert result["p_value"] == 1.0


def test_mcnemar_ignores_tasks_missing_from_either_arm():
    a = {"1": True, "2": False}
    b = {"1": False}
    assert mcnemar(a, b)["n_paired"] == 1


def test_accuracy_by_stratum():
    rows = [
        {"level": "easy", "verdict": "correct"},
        {"level": "easy", "verdict": "incorrect"},
        {"level": "hard", "verdict": "correct"},
    ]
    assert accuracy_by(rows, "level") == {"easy": (1, 2), "hard": (1, 1)}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd experiments/dabstep-contract-eval && uv run pytest tests/test_stats.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'dce.stats'`

- [ ] **Step 3: Write the implementation**

`experiments/dabstep-contract-eval/dce/stats.py`:

```python
"""Offline statistics over the results JSONL.

McNemar is the right test because every arm sees every task: the comparison is
paired. Its discordant-pair count is reported alongside every p-value, because
near the ceiling discordant pairs get scarce and a non-significant result then
means "could not tell", not "the arms are equal".
"""

from __future__ import annotations

import json
from collections import defaultdict
from math import sqrt
from pathlib import Path


def wilson(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    p = successes / n
    denom = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / denom
    half = z * sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def mcnemar(rows_a: dict[str, bool], rows_b: dict[str, bool]) -> dict:
    from scipy.stats import binomtest

    shared = sorted(set(rows_a) & set(rows_b))
    a_only = sum(1 for t in shared if rows_a[t] and not rows_b[t])
    b_only = sum(1 for t in shared if rows_b[t] and not rows_a[t])
    discordant = a_only + b_only

    p = 1.0 if discordant == 0 else binomtest(b_only, discordant, 0.5).pvalue
    return {
        "n_paired": len(shared),
        "a_only": a_only,
        "b_only": b_only,
        "discordant": discordant,
        "p_value": float(p),
    }


def accuracy_by(rows: list[dict], key: str) -> dict[str, tuple[int, int]]:
    out: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for row in rows:
        bucket = out[row.get(key, "unknown")]
        bucket[1] += 1
        bucket[0] += row["verdict"] == "correct"
    return {k: (v[0], v[1]) for k, v in out.items()}


def load(path: Path) -> list[dict]:
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def report(path: Path) -> str:
    rows = load(path)
    lines: list[str] = []

    for model in sorted({r["model"] for r in rows}):
        lines.append(f"\n## {model}")
        subset = [r for r in rows if r["model"] == model]

        for arm in sorted({r["arm"] for r in subset}):
            arm_rows = [r for r in subset if r["arm"] == arm]
            ok = sum(r["verdict"] == "correct" for r in arm_rows)
            lo, hi = wilson(ok, len(arm_rows))
            usd = sum(r["usd"] for r in arm_rows)
            tok = sum(r["input_tokens"] for r in arm_rows)
            cached = sum(r.get("cached_tokens", 0) for r in arm_rows)
            limits = sum(r["verdict"] == "hit_limit" for r in arm_rows)
            lines.append(
                f"{arm:14s} {ok:4d}/{len(arm_rows):<4d} "
                f"[{lo:.3f},{hi:.3f}]  ${usd:6.2f}  "
                f"in_tok/q={tok / max(len(arm_rows), 1):8.0f}  "
                f"cached={cached}  hit_limit={limits}"
            )

        def verdicts(arm: str) -> dict[str, bool]:
            return {
                r["task_id"]: r["verdict"] == "correct"
                for r in subset
                if r["arm"] == arm
            }

        for left in ("schema_only", "manual_prompt"):
            result = mcnemar(verdicts(left), verdicts("contract"))
            lines.append(
                f"McNemar {left} vs contract: p={result['p_value']:.4f} "
                f"discordant={result['discordant']} "
                f"(contract_only={result['b_only']}, {left}_only={result['a_only']})"
            )

    return "\n".join(lines)


if __name__ == "__main__":
    import sys

    print(report(Path(sys.argv[1] if len(sys.argv) > 1 else "results/results.jsonl")))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd experiments/dabstep-contract-eval && uv run pytest -v`
Expected: 47 passed

- [ ] **Step 5: Commit**

```bash
git add experiments/dabstep-contract-eval/dce/stats.py experiments/dabstep-contract-eval/tests/test_stats.py
git commit -m "experiment: Wilson intervals, paired McNemar, and stratified accuracy"
```

---

### Task 10: Smoke run and budget re-estimate

**This task spends real money (~$2). Stop for human review at the end.**

**Files:**
- Create: `experiments/dabstep-contract-eval/README.md`
- Modify: none

- [ ] **Step 1: Write the README**

`experiments/dabstep-contract-eval/README.md`:

```markdown
# DABStep Contract-Context Eval

Three-arm ablation measuring whether contract-delivered context beats a
manual-in-prompt baseline and a schema-only floor on the DABStep benchmark.
See `FINDINGS.md` for method, results, and conclusion, and
`docs/superpowers/specs/2026-08-30-dabstep-contract-eval-design.md` for the design.

## Setup

```bash
cd experiments/dabstep-contract-eval
uv sync
uv run pytest -q                     # deterministic, offline
uv run python -m dce.prepare         # download DABStep, build DuckDB, reconstruct golds
```

## Run (spends OpenRouter credit)

The key is read from `OPENROUTER_API_KEY`; source it from a `.env` that lives
**outside** this repo. Point `LENS_ENV_FILE` at that file:

```bash
set -a; . "$LENS_ENV_FILE"; set +a
uv run python -m dce.runner --n 12 --max-spend 2.00 --models z-ai/glm-5.3-flash
uv run python -m dce.stats results/results.jsonl
```

Flags: `--n` (0 = all), `--arms`, `--models`, `--max-spend`, `--out`.
Runs are resumable — a completed `(task, arm, model)` row is skipped.

## Arms

| Arm | Context |
|---|---|
| `schema_only` | table/column names + ungoverned SQL execution |
| `manual_prompt` | the above, plus DABStep's `manual.md` in the system prompt |
| `contract` | the library's nine tools over a frozen contract; no manual |
```

- [ ] **Step 2: Run the smoke sweep**

```bash
cd experiments/dabstep-contract-eval
set -a; . "$LENS_ENV_FILE"; set +a
uv run python -m dce.runner --n 12 --max-spend 2.00 \
  --models z-ai/glm-5.3-flash --out results/smoke12.jsonl
uv run python -m dce.stats results/smoke12.jsonl
```

- [ ] **Step 3: Answer the four questions the spec requires before a full sweep**

1. **Measured tokens per question, per arm.** Replaces the ~30k/2k estimate. Recompute the full-sweep cost from these numbers and write it into `FINDINGS.md`.
2. **Is arm B being prompt-cached?** Check the `cached` column in the stats report. A non-zero `cached_tokens` for `manual_prompt` means the cost comparison is confounded and must be reported with that caveat.
3. **Ceiling check.** If all three arms score near-identically on 12 tasks, the spec's ceiling risk is live — re-cut against the `hard` stratum only rather than spending the full sweep.
4. **Cap trips.** Any `hit_limit` means `max_tool_calls` (25) or `per_task_usd` (0.25) is too tight. Raise it and re-run the smoke *before* the full sweep, because a cap that binds unevenly across arms is a confound.

- [ ] **Step 4: Commit results and the recalculated budget**

```bash
git add experiments/dabstep-contract-eval/README.md experiments/dabstep-contract-eval/results/smoke12.jsonl
git commit -m "experiment: smoke run, measured per-arm token costs"
```

**STOP. Report the four answers and the revised budget to the human before Task 11.**

---

### Task 11: Full sweep, Sol subset, and FINDINGS

**This task spends the bulk of the budget (~$49 at the spec's estimate, revised by Task 10).**

**Files:**
- Create: `experiments/dabstep-contract-eval/FINDINGS.md`
- Create: `experiments/dabstep-contract-eval/data/sol_subset.json` (committed, exception to the `data/` gitignore rule — add `!data/sol_subset.json`)

- [ ] **Step 1: Freeze the Sol subset before any Sol run**

```bash
cd experiments/dabstep-contract-eval
uv run python -c "
import json, random
from pathlib import Path
golds = json.loads(Path('data/golds.json').read_text())
tasks = [t for t in json.loads(Path('data/tasks.json').read_text())
         if t['task_id'] in golds and t.get('level') == 'hard']
random.Random(0).shuffle(tasks)
ids = sorted(t['task_id'] for t in tasks[:60])
Path('data/sol_subset.json').write_text(json.dumps(ids, indent=2))
print(len(ids), 'tasks frozen')
"
git add -f experiments/dabstep-contract-eval/data/sol_subset.json
git commit -m "experiment: freeze the gpt-5.6-sol hard subset before running it"
```

Selecting this subset after seeing which tasks the other models missed would turn a ceiling check into a search for a favourable slice. Freeze it first.

- [ ] **Step 2: Run the three full sweeps**

```bash
set -a; . "$LENS_ENV_FILE"; set +a
cd experiments/dabstep-contract-eval
for m in deepseek/deepseek-v4-flash-0731 z-ai/glm-5.3-flash deepseek/deepseek-v4-pro-0813; do
  uv run python -m dce.runner --max-spend 40.00 --models "$m" --out results/full.jsonl
done
```

Resumable: re-run the same command after an interruption and completed rows are skipped.

- [ ] **Step 3: Run the Sol ceiling check**

```bash
uv run python -m dce.runner --max-spend 15.00 \
  --models openai/gpt-5.6-sol --arms manual_prompt contract \
  --tasks data/sol_subset_tasks.json --out results/sol_hard.jsonl
```

Build `data/sol_subset_tasks.json` by filtering `data/tasks.json` to the frozen ids.

- [ ] **Step 4: If the primary comparison lands near significance, repeat it**

The spec's documented response to a borderline primary result is a 3-sample
repeat of that one pair on the weak model — not a reinterpretation of the
single-sample numbers. Trigger it when the pre-registered B-vs-C McNemar on
`deepseek-v4-pro-0813` returns `0.01 < p < 0.10`:

```bash
for seed in 1 2 3; do
  uv run python -m dce.runner --max-spend 4.00 \
    --models deepseek/deepseek-v4-flash-0731 --arms manual_prompt contract \
    --out "results/repeat_seed${seed}.jsonl"
done
```

Vary `ModelSettings(seed=...)` per repeat via an env var read in
`_default_agent_factory`; record the seed in each row. ~$9. If `p` is clearly
above or below that band, skip this step and say in `FINDINGS.md` that it was
not triggered.

- [ ] **Step 5: Write FINDINGS.md**

Required contents, per the spec:
- **The pre-registered primary result first:** arm B vs arm C on `deepseek-v4-pro-0813`, paired McNemar, with its discordant-pair count. Everything else is labelled secondary and exploratory.
- Accuracy per arm per model with Wilson intervals, quoted against **"the reconstructed-gold subset (n=…)"** — never against "DABStep" unqualified.
- Gold coverage by `level` and template, and the statement that the scorable subset is easier than the full 450, so absolute numbers are biased upward and are **not** comparable to MotherDuck's 99.5%.
- Tokens and USD per question per arm, plus always-on prompt size per arm, and whether prompt caching was detected for arm B.
- `inspect_query` rejection counts, presented as descriptive instrumentation of arm C's behaviour, **not** as a governance claim.
- Cap-trip and error rates per arm.
- The contract digest, the commit sha, and the pinned model ids.
- If the arms tie: report the tie as a tie. If discordant pairs are scarce: report "this design could not tell", not "the arms are equal".

- [ ] **Step 6: Link from the main README and commit**

Add one line to the repo README pointing at `experiments/dabstep-contract-eval/FINDINGS.md`. Do not put the accuracy number in the main README — promotion is a separate decision the human makes after reading the results.

```bash
git add experiments/dabstep-contract-eval/ README.md
git commit -m "experiment: DABStep contract-context results and findings"
```

- [ ] **Step 7: Remove the process scaffolding**

Per the repo's doc lifecycle convention, once the work ships:

```bash
git rm docs/superpowers/specs/2026-08-30-dabstep-contract-eval-design.md \
       docs/superpowers/plans/2026-08-30-dabstep-contract-eval.md
git commit -m "chore: remove DABStep eval design and plan artifacts"
```

`FINDINGS.md` is the durable record; the spec and plan remain in git history.
