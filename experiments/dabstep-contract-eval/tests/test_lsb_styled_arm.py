"""Experiment 4's `manual_styled`: `manual_prompt` reading the same
knowledge written in a manual's style, from `LSB_DATA/styled/kb/`."""

import json

import pytest
from dce.benchmarks.livesqlbench import LiveSQLBench


def _styled(lsb_data):
    kb = lsb_data / "hf-full-v1" / "shop" / "shop_kb.jsonl"
    rows = [json.loads(x) for x in kb.read_text(encoding="utf-8").splitlines() if x]
    rows[0]["definition"] = "As our guide explains, a missing value counts as zero."
    folder = lsb_data / "styled" / "kb"
    folder.mkdir(parents=True)
    (folder / "shop_kb.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )
    return LiveSQLBench.from_data(lsb_data)


def test_it_is_an_ungoverned_extra_arm():
    assert "manual_styled" in LiveSQLBench.extra_arms
    assert "manual_styled" not in LiveSQLBench.arms
    assert "manual_styled" not in LiveSQLBench.governed_arms


def test_it_is_the_manual_with_the_styled_kb(lsb_data):
    bench = _styled(lsb_data)
    task = next(t for t in bench.tasks() if t.task_id == "shop_1")
    db = lsb_data / "duckdb" / "shop.duckdb"
    manual = bench.build_arm("manual_prompt", task, db)
    styled = bench.build_arm("manual_styled", task, db)
    try:
        assert "As our guide explains" in styled.system_prompt
        assert "As our guide explains" not in manual.system_prompt
        assert [t.name for t in styled.tools] == [t.name for t in manual.tools]
    finally:
        manual.close()
        styled.close()


def test_its_digest_records_the_styled_kb(lsb_data):
    bench = _styled(lsb_data)
    task = next(t for t in bench.tasks() if t.task_id == "shop_1")
    assert bench.arm_digest("manual_styled", task) != bench.arm_digest(
        "manual_prompt", task
    )


def test_check_arms_needs_the_styled_kb(lsb_data):
    bench = _styled(lsb_data)
    tasks = bench.tasks()
    bench.check_arms(("manual_styled",), tasks)
    (lsb_data / "styled" / "kb" / "shop_kb.jsonl").unlink()
    with pytest.raises(SystemExit, match="manual_styled"):
        bench.check_arms(("manual_styled",), tasks)
