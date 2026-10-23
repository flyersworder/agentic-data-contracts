import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import knowledge_reuse as kr  # noqa: E402

# db "d": 2 builds on 1, 1 builds on 0; 3 stands alone.
CHILDREN = {("d", 0): [], ("d", 1): [0], ("d", 2): [1], ("d", 3): []}


def test_closure_follows_children_transitively():
    assert kr.closure("d", [2], CHILDREN) == {0, 1, 2}
    assert kr.closure("d", [3], CHILDREN) == {3}
    assert kr.closure("d", [], CHILDREN) == set()


def test_closure_survives_a_cycle_and_a_dangling_id():
    children = {("d", 0): [1], ("d", 1): [0, 9]}
    assert kr.closure("d", [0], children) == {0, 1, 9}


def test_reuse_counts_tasks_per_entry():
    uses = {"t1": ("d", {0, 1}), "t2": ("d", {1}), "t3": ("e", {1})}
    assert kr.reuse(uses) == {("d", 0): 1, ("d", 1): 2, ("e", 1): 1}


def test_bin_of():
    assert [kr.bin_of(n) for n in (0, 1, 2, 3, 4, 5, 9)] == [
        "none",
        "1",
        "2",
        "3-4",
        "3-4",
        "5+",
        "5+",
    ]


def test_hard_tasks_are_those_of_a_reused_entry_the_manual_fails():
    uses = {
        "t1": ("d", {0, 1}),
        "t2": ("d", {0}),
        "t3": ("d", {0, 2}),
        "t4": ("d", {2}),
    }
    count = kr.reuse(uses)  # entry 0 serves three tasks, entry 2 two
    manual = {"t1": 0, "t2": 1, "t3": 0, "t4": 0}
    score = {"manual_prompt": manual}
    assert kr.hard_tasks(uses, count, score) == {"t1", "t2", "t3"}  # 1 of 9
    manual["t2"] = 2  # 2 of 9 runs: above a fifth
    assert kr.hard_tasks(uses, count, score) == set()
