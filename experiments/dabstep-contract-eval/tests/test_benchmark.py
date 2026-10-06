from dataclasses import FrozenInstanceError

import pytest
from dce.benchmark import (
    DEFAULT_BENCHMARK,
    ROLES,
    Grade,
    Task,
    benchmark_class,
    benchmark_of,
    group_of,
)


def test_task_is_frozen_and_meta_defaults_empty():
    task = Task("1", "q", "hard")
    assert task.meta == {}
    with pytest.raises(FrozenInstanceError):
        task.prompt = "other"  # type: ignore[misc]


def test_grade_failure_defaults_to_none():
    assert Grade("correct").failure is None


def test_roles_are_the_four_the_analysis_reads():
    assert ROLES == ("baseline", "manual", "manual_plus", "contract")


def test_unknown_benchmark_is_refused():
    with pytest.raises(ValueError, match="unknown benchmark"):
        benchmark_class("nope")


def test_a_row_without_a_benchmark_field_is_dabstep():
    assert DEFAULT_BENCHMARK == "dabstep"
    assert benchmark_of({}) == "dabstep"
    assert benchmark_of({"benchmark": None}) == "dabstep"
    assert benchmark_of({"benchmark": "livesqlbench"}) == "livesqlbench"


def test_group_falls_back_to_level_when_absent_or_null():
    assert group_of({"group": "db1", "level": "hard"}) == "db1"
    assert group_of({"level": "hard"}) == "hard"
    # A salvage envelope or an older row can carry `group: null`; it must
    # read as the level, not as a `None` stratum.
    assert group_of({"group": None, "level": "easy"}) == "easy"
    assert group_of({}) == "unknown"


def test_role_arms_names_the_arms_a_benchmark_gives_its_roles():
    from dce.benchmark import role_arms
    from dce.benchmarks.dabstep import DABStep

    assert role_arms(DABStep, "manual", "manual_plus", "contract") == (
        "manual_prompt",
        "manual_resolved",
        "contract",
    )


def test_a_missing_role_is_refused_by_name():
    from dce.benchmark import role_arms
    from dce.benchmarks.dabstep import DABStep

    # DABStep has no `baseline` role: `schema_only` is an arm, not a role.
    with pytest.raises(SystemExit, match="baseline"):
        role_arms(DABStep, "baseline", "contract")
