import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import knowledge_delivery as kd  # noqa: E402


def test_livesqlbench_reads_four_roles_on_the_primary_subset():
    config = kd.CONFIG["livesqlbench"]
    assert config["subset"] == "primary"
    assert config["roles"] == ("manual", "manual_plus", "contract")
    assert ("contract", "baseline") in config["pairs"]
    assert kd.arms_of("livesqlbench") == {
        "manual": "manual_prompt",
        "manual_plus": "manual_compiled",
        "contract": "contract",
        "baseline": "schema_only",
    }
    assert config["group"]({"high_level": True}) == "high_level"
    assert config["group"]({"high_level": False}) == "standard"
    assert config["runs"]["gpt-6-luna"](2) == ["livesqlbench/luna-r2"]


def test_dabstep_is_unchanged():
    assert kd.arms_of("dabstep") == {
        "manual": "manual_prompt",
        "manual_plus": "manual_resolved",
        "contract": "contract",
    }


def test_a_model_runs_only_when_every_repeat_file_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(kd, "ROOT", tmp_path)
    folder = tmp_path / "results" / "livesqlbench"
    folder.mkdir(parents=True)
    for i in (1, 2):
        (folder / f"luna-r{i}.jsonl").write_text("", encoding="utf-8")
    assert not kd.has_runs("livesqlbench", "gpt-6-luna")
    (folder / "luna-r3.jsonl").write_text("", encoding="utf-8")
    assert kd.has_runs("livesqlbench", "gpt-6-luna")
    assert not kd.has_runs("livesqlbench", "gpt-6-sol")
