"""Assert every declared dependency floor installs as a wheel on this Python.

`test-lowest-floors` resolves the project at `--resolution lowest-direct`, but
uv resolves *universally*: a transitive requirement can float a package above
its own declared floor, so the floor is never exercised. `pydantic>=2.0` sat
untrue for the life of the project that way — pydantic 2.0 pins
pydantic-core==2.0.1, whose wheels stop at cp311, so the declared floor meant a
PyO3 source build on both supported interpreters.

This checks the other half directly: pin each declared floor to `==` and ask uv
whether it resolves using wheels alone. A floor with no wheel for the running
interpreter is the recurring bug — `duckdb>=1.1.0` (no cp313),
`snowflake-connector-python>=3.6.0` (no cp312 or cp313),
`psycopg2-binary>=2.9.9` (no cp313) were all found this way.

Floors are read from pyproject rather than listed here, so a new dependency is
covered without editing this file. Only `>=` constraints are checked; anything
else is reported and skipped rather than silently ignored.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from pathlib import Path

_REQ = re.compile(
    r"^\s*([A-Za-z0-9._-]+)\s*(\[[^\]]*\])?\s*>=\s*([0-9][0-9A-Za-z.*+!-]*)\s*$"
)


def _floors(pyproject: dict) -> tuple[list[tuple[str, str]], list[str]]:
    """Return ([(requirement, source)], [unchecked]) for every `>=` floor."""
    project = pyproject["project"]
    groups: list[tuple[str, list[str]]] = [("dependencies", project["dependencies"])]
    # Extras are checked too, including the driver-only ones this package never
    # imports. They exist so a user can `pip install ...[snowflake]` before
    # writing their own DatabaseAdapter, which is exactly why an uninstallable
    # floor defeats their only purpose — even though no test exercises them.
    for name, reqs in project.get("optional-dependencies", {}).items():
        groups.append((f"extra:{name}", reqs))

    pinned: list[tuple[str, str]] = []
    unchecked: list[str] = []
    for source, reqs in groups:
        for req in reqs:
            # Self-referential extras (`agentic-data-contracts[duckdb]`) resolve
            # to this project; their contents are checked via their own group.
            if req.startswith(project["name"]):
                continue
            m = _REQ.match(req)
            if m is None:
                unchecked.append(f"{source}: {req}")
                continue
            name, extras, version = m.groups()
            pinned.append((f"{name}{extras or ''}=={version}", source))
    return pinned, unchecked


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    pyproject = tomllib.loads((root / "pyproject.toml").read_text())
    pinned, unchecked = _floors(pyproject)

    py = f"{sys.version_info.major}.{sys.version_info.minor}"
    print(f"Checking {len(pinned)} declared floors for wheels on Python {py}\n")
    for note in unchecked:
        print(f"  SKIP (not a >= floor)  {note}")

    failures: list[tuple[str, str, str]] = []
    for requirement, source in pinned:
        proc = subprocess.run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                sys.executable,
                "--dry-run",
                "--only-binary",
                ":all:",
                requirement,
            ],
            capture_output=True,
            text=True,
        )
        ok = proc.returncode == 0
        print(f"  {'ok  ' if ok else 'FAIL'}  {requirement:52} ({source})")
        if not ok:
            failures.append(
                (
                    requirement,
                    source,
                    proc.stderr.strip().splitlines()[-1] if proc.stderr.strip() else "",
                )
            )

    if failures:
        print(f"\n{len(failures)} declared floor(s) have no wheel on Python {py}:\n")
        for requirement, source, detail in failures:
            print(f"  {requirement}  [{source}]")
            if detail:
                print(f"      {detail}")
        print(
            "\nA floor with no wheel forces a source build for anyone installing"
            "\nat it. Raise the floor to the first version publishing a wheel for"
            "\nevery interpreter in `requires-python`."
        )
        return 1

    print(f"\nAll {len(pinned)} floors install as wheels on Python {py}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
