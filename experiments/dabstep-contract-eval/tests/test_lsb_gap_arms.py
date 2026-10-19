"""Experiment 4b's arms (docs/superpowers/specs/2026-10-19-experiment-4b-
design.md). `manual_gap` is the manual over the gapped KB; `manual_full` the
KB with each gap's correction (`LSB_DATA/full/kb/`); `contract_gap` the
governed arm over the authored contract (draw 1); `manual_noted` the gapped
KB plus the author's resolutions as notes (the contract's `notes.md`); and
`contract_text` the authored contract's two files in the prompt over plain
tools."""

import json

import pytest
from dce.benchmarks import livesqlbench as lsb
from dce.benchmarks.livesqlbench import LiveSQLBench

from tests.test_lsb_extra_arms import _compiled


def _bench(lsb_data, tmp_path):
    _, roots = _compiled(lsb_data, tmp_path)
    gap_root = roots["contract_authored"]
    (gap_root / "shop" / "notes.md").write_text(
        "A tier stored as 'G' means gold.", encoding="utf-8"
    )
    kb = lsb_data / "hf-full-v1" / "shop" / "shop_kb.jsonl"
    rows = [json.loads(x) for x in kb.read_text(encoding="utf-8").splitlines() if x]
    rows[0]["definition"] += " Gold tier is stored as 'G'."
    folder = lsb_data / "full" / "kb"
    folder.mkdir(parents=True)
    (folder / "shop_kb.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )
    bench = LiveSQLBench.from_data(
        lsb_data,
        contracts=roots["contract"],
        extra_contracts={
            **{a: roots[a] for a in roots if a != "contract"},
            "contract_gap": gap_root,
        },
    )
    task = next(t for t in bench.tasks() if t.task_id == "shop_1")
    return bench, task, lsb_data / "duckdb" / "shop.duckdb", gap_root


def test_the_arms_and_their_kinds():
    for arm in lsb.GAP_ARMS:
        assert arm in LiveSQLBench.extra_arms
        assert arm not in LiveSQLBench.governed_arms
    assert "contract_gap" in LiveSQLBench.governed_arms
    root = lsb.DEFAULT_CONTRACTS.parent.parent
    assert lsb.EXTRA_CONTRACTS["contract_gap"] == (
        root / "contracts-procedures4b" / "draw-1" / "livesqlbench"
    )


def test_manual_gap_is_the_manual_and_manual_full_adds_the_corrections(
    lsb_data, tmp_path
):
    bench, task, db, _ = _bench(lsb_data, tmp_path)
    arms = {
        a: bench.build_arm(a, task, db)
        for a in ("manual_prompt", "manual_gap", "manual_full")
    }
    try:
        assert arms["manual_gap"].system_prompt == arms["manual_prompt"].system_prompt
        assert "stored as 'G'" in arms["manual_full"].system_prompt
        assert "stored as 'G'" not in arms["manual_gap"].system_prompt
    finally:
        for a in arms.values():
            a.close()


def test_manual_noted_is_manual_gap_plus_the_notes(lsb_data, tmp_path):
    bench, task, db, _ = _bench(lsb_data, tmp_path)
    gap = bench.build_arm("manual_gap", task, db)
    noted = bench.build_arm("manual_noted", task, db)
    try:
        assert noted.system_prompt == (
            gap.system_prompt + "\n\n## Data notes\n\nA tier stored as 'G' means gold."
        )
        assert [t.name for t in noted.tools] == [t.name for t in gap.tools]
    finally:
        gap.close()
        noted.close()


def test_contract_text_carries_the_authored_contract_verbatim_over_plain_tools(
    lsb_data, tmp_path
):
    bench, task, db, gap_root = _bench(lsb_data, tmp_path)
    text = bench.build_arm("contract_text", task, db)
    gap = bench.build_arm("manual_gap", task, db)
    try:
        for name in ("contract.yml", "semantic.yml"):
            body = (gap_root / "shop" / name).read_text(encoding="utf-8").strip()
            assert body in text.system_prompt
        assert "## Knowledge base" not in text.system_prompt
        assert [t.name for t in text.tools] == [t.name for t in gap.tools]
    finally:
        text.close()
        gap.close()


def test_digests_record_what_each_arm_reads(lsb_data, tmp_path):
    bench, task, _, gap_root = _bench(lsb_data, tmp_path)
    digests = {a: bench.arm_digest(a, task) for a in lsb.GAP_ARMS}
    assert len(set(digests.values())) == len(digests)
    (gap_root / "shop" / "notes.md").write_text("changed", encoding="utf-8")
    lsb._sha256.cache_clear()  # files are hashed once per sweep, by design
    assert bench.arm_digest("manual_noted", task) != digests["manual_noted"]


def test_check_arms_needs_the_full_kb_and_the_notes(lsb_data, tmp_path):
    bench, task, _, gap_root = _bench(lsb_data, tmp_path)
    tasks = bench.tasks()
    bench.check_arms(lsb.GAP_ARMS, tasks)
    (gap_root / "shop" / "notes.md").unlink()
    with pytest.raises(SystemExit, match="manual_noted"):
        bench.check_arms(("manual_noted",), tasks)
    (lsb_data / "full" / "kb" / "shop_kb.jsonl").unlink()
    with pytest.raises(SystemExit, match="manual_full"):
        bench.check_arms(("manual_full",), tasks)
