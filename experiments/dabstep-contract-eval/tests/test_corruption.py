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


def test_the_paper_counts_only_its_seven_models():
    assert corruption.in_paper("gpt-6-sol")
    assert corruption.in_paper("qwen3.8-27b")
    assert corruption.in_paper("z-ai/glm-5.3-flash")
    assert corruption.in_paper("openai/gpt-5.6-sol")
    assert not corruption.in_paper("qwen3.6-27b")
    assert not corruption.in_paper("deepseek/deepseek-v4-pro-0813")


def test_the_rules_are_stated_by_every_contract_arm_and_no_other():
    # The governed arms render the contract's constraints into the prompt;
    # the contract_text arms paste contract.yml, forbidden_operations and
    # all. The schema and manual arms state no rule.
    for arm in ("contract", "contract_hollow", "contract_compiled_opus"):
        assert corruption.states_rules(arm)
    for arm in ("contract_text", "contract_text_noted"):
        assert corruption.states_rules(arm)
    for arm in ("schema_only", "manual_prompt", "manual_resolved", "manual_summed"):
        assert not corruption.states_rules(arm)


def test_the_upper_bound_is_exact_and_the_zero_cell_also_gets_the_rule_of_three():
    lo, hi = corruption.bound(0, 300)
    assert lo == 0.0
    assert 0.0099 < hi < 0.0125
    assert corruption.bound(0, 0) == (0.0, 1.0)
