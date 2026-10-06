"""Each DABStep arm's system prompt and tool surface, as JSON.

The equivalence check for the Benchmark refactor: run it before the
refactor and after, and diff the two outputs. Every arm is built on its own
throwaway copy of the database, never on `data/dabstep.duckdb` itself (see
`dce.tools`' module docstring for why an arm must not open the pristine
file).

Run:  uv run python analysis/arm_surface.py > arm_surface.json
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

CONTEXT = ROOT / "data" / "hf" / "data" / "context"
PRISTINE = ROOT / "data" / "dabstep.duckdb"


def _builder():
    """`(arms, build)`, where `build(arm, db)` returns an `ArmSetup`. Built
    through `DABStep.build_arm`, the path the runner takes."""
    from dce.benchmarks.dabstep import DABStep, load_docs

    bench = DABStep(db=PRISTINE, golds={}, golds_hash="", docs=load_docs(CONTEXT))
    task = DABStep.task({"task_id": "0", "question": "", "level": "hard"})
    return (
        DABStep.arms + DABStep.extra_arms,
        lambda arm, db: bench.build_arm(arm, task, db),
    )


def surface(setup) -> dict:
    return {
        "system_prompt": setup.system_prompt,
        "tools": [
            {
                "name": tool.tool_def.name,
                "description": tool.tool_def.description,
                "parameters": tool.tool_def.parameters_json_schema,
            }
            for tool in setup.tools
        ],
    }


def main() -> None:
    arms, build = _builder()
    out = {}
    with tempfile.TemporaryDirectory() as tmp:
        for arm in arms:
            working = Path(tmp) / f"{arm}.duckdb"
            shutil.copyfile(PRISTINE, working)
            setup = build(arm, working)
            try:
                out[arm] = surface(setup)
            finally:
                setup.close()
    print(json.dumps(out, indent=1, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    main()
