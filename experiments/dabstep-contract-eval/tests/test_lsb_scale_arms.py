"""Experiment 6's arms (docs/superpowers/specs/2026-10-20-experiment-6-
disclosure-scale-design.md): the governed arm, the contract's files in the
prompt, and the prose, over a contract diluted with other databases'."""

import json
import shutil

import pytest
import yaml
from dce import scale
from dce.benchmarks import livesqlbench as lsb
from dce.benchmarks.livesqlbench import LiveSQLBench

from tests.test_lsb_extra_arms import _compiled


def _load(folder):
    return (
        yaml.safe_load((folder / "contract.yml").read_text(encoding="utf-8")),
        yaml.safe_load((folder / "semantic.yml").read_text(encoding="utf-8")),
    )


def _bench(lsb_data, tmp_path):
    _, roots = _compiled(lsb_data, tmp_path)
    base = _load(roots["contract"] / "shop")
    root = tmp_path / "scale"
    scale.write(root / "merged" / "1x" / "shop", *scale.merge(base, []))
    pad = scale.namespace(*base, "other")
    scale.write(root / "merged" / "2x" / "shop", *scale.merge(base, [pad]))
    scale.write(root / "first" / "2x" / "shop", *scale.merge(base, [pad], first=True))
    (root / "levels.json").write_text(
        json.dumps({"shop": {"1x": [], "2x": ["other"]}}), encoding="utf-8"
    )
    kb = lsb_data / "hf-full-v1" / "shop" / "shop_kb.jsonl"
    (lsb_data / "padding" / "other").mkdir(parents=True)
    rows = [json.loads(x) for x in kb.read_text(encoding="utf-8").splitlines() if x]
    rows[0]["knowledge"] = "Other padding knowledge"
    (lsb_data / "padding" / "other" / "other_kb.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )
    bench = LiveSQLBench.from_data(lsb_data, contracts=roots["contract"], scale=root)
    task = next(t for t in bench.tasks() if t.task_id == "shop_1")
    return bench, task, lsb_data / "duckdb" / "shop.duckdb", root


def test_the_arms_and_their_kinds():
    assert lsb.SCALE_LEVELS == ("1x", "2x", "4x", "8x", "all")
    for level in lsb.SCALE_LEVELS:
        for kind in lsb.SCALE_KINDS:
            arm = f"{kind}_{level}"
            assert arm in lsb.SCALE_ARMS
            assert arm in LiveSQLBench.extra_arms
            assert (arm in LiveSQLBench.governed_arms) == (kind == "contract_scaled")


def test_the_governed_arm_reads_the_level_and_carries_the_scale_guard(
    lsb_data, tmp_path
):
    bench, task, db, _ = _bench(lsb_data, tmp_path)
    one = bench.build_arm("contract_scaled_1x", task, db)
    two = bench.build_arm("contract_scaled_2x", task, db)
    try:
        assert "other." not in one.system_prompt
        assert "other." in two.system_prompt
        for setup in (one, two):
            assert setup.token_budget == lsb.SCALE_TOKEN_BUDGET
            assert setup.min_input_tokens == len(setup.system_prompt) // 6
        assert "lookup_metric" in [t.name for t in two.tools]
    finally:
        one.close()
        two.close()


def test_the_text_arms_put_the_level_files_in_the_prompt(lsb_data, tmp_path):
    bench, task, db, root = _bench(lsb_data, tmp_path)
    mid = bench.build_arm("contract_text_scaled_2x", task, db)
    first = bench.build_arm("contract_text_first_2x", task, db)
    try:
        for setup, folder in ((mid, "merged"), (first, "first")):
            for f in ("contract.yml", "semantic.yml"):
                text = (root / folder / "2x" / "shop" / f).read_text(encoding="utf-8")
                assert text.strip() in setup.system_prompt
            assert setup.token_budget == lsb.SCALE_TOKEN_BUDGET
            assert "execute_sql" in [t.name for t in setup.tools]
        assert mid.system_prompt != first.system_prompt
    finally:
        mid.close()
        first.close()


def test_the_prose_arm_carries_each_database_under_its_name(lsb_data, tmp_path):
    bench, task, db, _ = _bench(lsb_data, tmp_path)
    one = bench.build_arm("manual_scaled_1x", task, db)
    two = bench.build_arm("manual_scaled_2x", task, db)
    manual = bench.build_arm("manual_prompt", task, db)
    try:
        assert "## Knowledge base: shop" in one.system_prompt
        assert "## Knowledge base: other" not in one.system_prompt
        assert "## Knowledge base: other" in two.system_prompt
        assert "Other padding knowledge" in two.system_prompt
        # The base's KB is the manual's, whatever surrounds it.
        kb = manual.system_prompt.split("## Knowledge base\n\n", 1)[1]
        assert kb in one.system_prompt and kb in two.system_prompt
        assert two.min_input_tokens == len(two.system_prompt) // 6
    finally:
        for s in (one, two, manual):
            s.close()


def test_each_level_has_its_own_digest(lsb_data, tmp_path):
    bench, task, _, _ = _bench(lsb_data, tmp_path)
    for kind in lsb.SCALE_KINDS:
        if kind == "contract_text_first":
            continue
        assert bench.arm_digest(f"{kind}_1x", task) != bench.arm_digest(
            f"{kind}_2x", task
        )


def test_a_missing_level_is_refused_before_any_spend(lsb_data, tmp_path):
    bench, task, _, root = _bench(lsb_data, tmp_path)
    bench.check_arms(("contract_scaled_2x", "manual_scaled_2x"), [task])
    shutil.rmtree(root / "merged" / "2x")
    with pytest.raises(SystemExit, match="contract_scaled_2x"):
        bench.check_arms(("contract_scaled_2x",), [task])
    with pytest.raises(SystemExit, match="manual_scaled_4x"):
        bench.check_arms(("manual_scaled_4x",), [task])
