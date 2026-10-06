"""What the harness needs from a benchmark, and nothing more.

The runner, `run_task` and the analysis talk to a benchmark only through
`Benchmark`. DABStep (`dce.benchmarks.dabstep`) is the first implementation;
the hardened runner it grew up with -- the budget ledger, the retry pass,
the lock, torn-tail repair, construction retries -- is what every later
benchmark reuses.

Class attributes carry everything the analysis reads, so `dce.stats` can
resolve a row's benchmark (`benchmark_class(benchmark_of(row))`) without
loading golds or tasks. The three static hooks at the end exist for the same
reason: re-grading and the end-to-end view work from a stored row, and the
analysis must not import any one benchmark's scorer to do it.
`add_arguments`/`from_args` let `dce.runner` build whichever benchmark
`--benchmark` names; each adds only its own flags, so flag names must not
collide across benchmarks.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from dce.tools import ArmSetup

#: The arms the analysis compares, by role rather than by name.
#: `manual_plus` is the manual plus whatever knowledge the contract adds,
#: delivered as prose -- `manual_resolved` on DABStep -- so
#: (manual_plus - manual) / (contract - manual) is the share of the
#: contract's lead that is knowledge rather than delivery.
ROLES: tuple[str, ...] = ("baseline", "manual", "manual_plus", "contract")

BENCHMARK_NAMES: tuple[str, ...] = ("dabstep",)

#: What a row with no `benchmark` field is: every row written before the
#: field existed is a DABStep row.
DEFAULT_BENCHMARK = "dabstep"


@dataclass(frozen=True)
class Task:
    task_id: str
    prompt: str  # the full user message
    group: str  # stratum for smoke sampling and per-group stats
    meta: Mapping[str, Any] = field(default_factory=dict)
    subset: str | None = None  # a separately reported task set, if any


@dataclass(frozen=True)
class Grade:
    verdict: str  # "correct" | "incorrect" | "ungraded"
    failure: str | None = None  # grading-side failure category, if any


@runtime_checkable
class Benchmark(Protocol):
    name: str
    arms: tuple[str, ...]
    extra_arms: tuple[str, ...]
    governed_arms: frozenset[str]
    roles: Mapping[str, str]
    primary: tuple[str, str, str] | None
    group_label: str
    excluded_tasks: frozenset[str]
    subsets: tuple[str, ...]  # separately reported task sets, pre-registered first
    task_set_label: str  # how the report names the task set it scores
    excluded_reason: str  # why `excluded_tasks` are dropped, for the report
    excluded_source: str  # where the excluded list is kept

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None: ...
    @classmethod
    def from_args(cls, args: argparse.Namespace) -> Benchmark: ...

    def tasks(self) -> list[Task]: ...
    def pristine_db(self, task: Task) -> Path: ...
    def build_arm(self, arm: str, task: Task, db: Path) -> ArmSetup: ...
    def arm_digest(self, arm: str, task: Task) -> str: ...
    def gold_ref(self, task: Task) -> str | None: ...
    def grade(self, task: Task, answer: str) -> Grade: ...
    def normalize(self, answer: str) -> str: ...
    def golds_hash(self) -> str: ...
    def row_fields(self, task: Task) -> dict: ...

    @staticmethod
    def scorer() -> str: ...
    @staticmethod
    def rescore(row: dict) -> str | None: ...
    @staticmethod
    def e2e_correct(row: dict) -> bool: ...


def benchmark_class(name: str) -> type[Benchmark]:
    """The class registered under `name`. Imported lazily, so reading one
    benchmark's rows never imports another's scorer."""
    if name == "dabstep":
        from dce.benchmarks.dabstep import DABStep

        return DABStep
    raise ValueError(f"unknown benchmark: {name!r}; expected one of {BENCHMARK_NAMES}")


def benchmark_of(row: Mapping) -> str:
    return row.get("benchmark") or DEFAULT_BENCHMARK


def group_of(row: Mapping) -> str:
    """The row's stratum. `group` when the row has one; otherwise `level`,
    which is what DABStep rows written before `group` existed carry."""
    group = row.get("group")
    if group is None:
        group = row.get("level", "unknown")
    return group


def role_arms(benchmark: type[Benchmark], *roles: str) -> tuple[str, ...]:
    """The arms `benchmark` gives `roles`, in that order. An analysis that
    needs a role the benchmark does not define cannot run on it, so it stops
    with the role's name rather than a bare `KeyError`."""
    missing = [role for role in roles if role not in benchmark.roles]
    if missing:
        raise SystemExit(
            f"{benchmark.name} defines no arm for role(s) {missing}; "
            f"its roles are {dict(benchmark.roles)}"
        )
    return tuple(benchmark.roles[role] for role in roles)
