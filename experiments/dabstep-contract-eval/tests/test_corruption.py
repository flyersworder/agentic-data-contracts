import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import corruption  # noqa: E402


def test_the_governed_arms_are_the_contract_arms_behind_the_governed_tools():
    assert corruption.governed("contract")
    assert corruption.governed("contract_hollow")
    assert corruption.governed("contract_compiled_opus_joins")
    assert not corruption.governed("contract_text")
    assert not corruption.governed("contract_text_noted")
    assert not corruption.governed("manual_prompt")
    assert not corruption.governed("schema_only")


def test_smoke_files_are_not_runs():
    assert not corruption.is_run("sonnet5-smoke12")
    assert not corruption.is_run("exp5c-smoke")
    assert corruption.is_run("qwen38-panel-r1")
