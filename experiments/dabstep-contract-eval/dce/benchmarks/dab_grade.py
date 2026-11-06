"""Experiment 7's grader: each query's own `validate.py`, as DataAgentBench
grades (`common_scaffold/validate/validate.py` at the pinned commit).

The validators are third-party code, so each runs in a fresh `python -I`
subprocess, from an empty working directory, with a minimal environment and
a timeout, and only after a static check that it imports nothing but the
modules the 40 phase-1 validators use (`re`, `difflib`, and DAB's own
pure-Python `common_scaffold.validate.levenshtein`, pinned in
`dab_fetch.HELPER_FILES`) and calls no `open`, `exec`, `eval` or
`__import__`. As in DAB, an empty answer fails without running the
validator.

DAB's validators check that the gold appears in the answer, so a long answer
listing many candidates can pass. Every grade therefore carries `overlong`:
a pass whose answer is more than `OVERLONG_CHARS` characters longer than the
gold string (`ground_truth.csv` as DAB reads it). The validator's reason
string is never kept: it often quotes the gold.
"""

from __future__ import annotations

import ast
import subprocess
import sys
import tempfile
from pathlib import Path

from dce.benchmark import Grade

#: Names the grading rules on every row. Bump it whenever what counts as
#: correct, or what `overlong` means, changes.
SCORER = "dab-validate-py/1"

#: Characters an answer may exceed the gold string by before a pass is
#: flagged overlong (spec, "Models, runs, measure").
OVERLONG_CHARS = 200

#: Per validator run. DAB sets none; this only ends a runaway. The fuzzy
#: validators' edit-distance scans grow with the answer's length, so a long
#: wrong answer can take minutes.
VALIDATOR_TIMEOUT_S = 600

ALLOWED_IMPORTS: frozenset[str] = frozenset(
    {"re", "difflib", "common_scaffold.validate.levenshtein"}
)
_FORBIDDEN_CALLS = frozenset({"open", "exec", "eval", "compile", "__import__"})

_RUNNER = (
    "import importlib.util, json, sys\n"
    "path, helper = sys.argv[1], sys.argv[2]\n"
    "if helper:\n"
    "    sys.path.insert(0, helper)\n"
    "spec = importlib.util.spec_from_file_location('validate', path)\n"
    "module = importlib.util.module_from_spec(spec)\n"
    "spec.loader.exec_module(module)\n"
    "answer = sys.stdin.buffer.read().decode('utf-8')\n"
    "ok, _ = module.validate(answer)\n"
    "sys.stdout.write('\\n' + json.dumps(bool(ok)))\n"
)


class ValidatorRefused(RuntimeError):
    """A validator that imports or calls something outside the allowed set."""


def gold_string(text: str) -> str:
    """`ground_truth.csv` as DAB reads it: its non-empty lines, stripped,
    joined by newlines."""
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def check_validator(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in _FORBIDDEN_CALLS
        ):
            raise ValidatorRefused(f"{path}: calls {node.func.id}()")
        else:
            continue
        refused = [n for n in names if n not in ALLOWED_IMPORTS]
        if refused:
            raise ValidatorRefused(f"{path}: imports {refused}")


def run_validator(
    path: Path,
    answer: str,
    *,
    helper_root: Path | None = None,
    timeout: float = VALIDATOR_TIMEOUT_S,
) -> bool:
    """The validator's verdict on `answer`. Raises `subprocess.TimeoutExpired`
    or `RuntimeError` (the validator raised, or printed no verdict)."""
    with tempfile.TemporaryDirectory(prefix="dab-validate-") as cwd:
        proc = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                _RUNNER,
                str(path.resolve()),
                str(helper_root.resolve()) if helper_root else "",
            ],
            input=answer.encode("utf-8"),
            capture_output=True,
            timeout=timeout,
            cwd=cwd,
            env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
            check=False,
        )
    if proc.returncode != 0:
        tail = proc.stderr.decode("utf-8", "replace").strip().splitlines()[-1:]
        raise RuntimeError(f"validator exited {proc.returncode}: {tail}")
    last = proc.stdout.decode("utf-8", "replace").rstrip("\n").rsplit("\n", 1)[-1]
    if last not in ("true", "false"):
        raise RuntimeError("validator printed no verdict")
    return last == "true"


def grade_answer(
    query_dir: Path,
    answer: str,
    *,
    helper_root: Path | None = None,
    timeout: float = VALIDATOR_TIMEOUT_S,
) -> Grade:
    validate_py = query_dir / "validate.py"
    check_validator(validate_py)
    gold = gold_string((query_dir / "ground_truth.csv").read_text(encoding="utf-8"))
    if answer == "":
        return Grade("incorrect", fields={"overlong": False})
    try:
        ok = run_validator(
            validate_py, answer, helper_root=helper_root, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        return Grade(
            "incorrect", failure="validator_timeout", fields={"overlong": False}
        )
    except RuntimeError:
        return Grade("incorrect", failure="validator_error", fields={"overlong": False})
    overlong = ok and len(answer) > len(gold) + OVERLONG_CHARS
    return Grade("correct" if ok else "incorrect", fields={"overlong": overlong})
