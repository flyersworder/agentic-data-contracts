"""DABStep experiment 1's arms: `contract` with the contract claudeopus5.5
compiled, without (`contract_compiled_opus`) and with declared joins
(`contract_compiled_opus_joins`). Each differs from `contract_compiled` only
in which compiled contract it loads."""

import duckdb
import pytest
from dce import frozen
from dce.benchmarks.dabstep import (
    ALL_ARMS,
    ARMS,
    EXTRA_ARMS,
    GOVERNED_ARMS,
    arm_digest,
    build_arm,
)

OPUS_ARMS = ("contract_compiled_opus", "contract_compiled_opus_joins")
DOCS = {"manual": "m", "payments_readme": "r"}


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "d.duckdb"
    duckdb.connect(str(path)).close()
    return path


def test_they_are_extra_governed_arms():
    for arm in OPUS_ARMS:
        assert arm in EXTRA_ARMS and arm in ALL_ARMS and arm not in ARMS
        assert arm in GOVERNED_ARMS


@pytest.mark.parametrize(
    ("arm", "directory"),
    [
        ("contract_compiled", "contracts"),
        ("contract_compiled_opus", "contracts-opus"),
        ("contract_compiled_opus_joins", "contracts-opus-joins"),
    ],
)
def test_each_loads_its_own_compile(arm, directory):
    path = frozen.compiled_contract_path(arm)
    assert path.parts[-3:] == (directory, "dabstep", "contract.yml")
    assert path.is_file()


def test_the_three_compiled_contracts_have_distinct_digests():
    digests = {arm_digest(a) for a in ("contract_compiled", *OPUS_ARMS)}
    assert len(digests) == 3


def test_the_joins_contract_carries_its_declared_joins():
    from agentic_data_contracts.semantic.yaml_source import YamlSource

    path = frozen.compiled_contract_path("contract_compiled_opus_joins")
    metrics = YamlSource(path.parent / "semantic.yml").get_metrics()
    assert any(m.from_sql for m in metrics)


@pytest.mark.parametrize("arm", OPUS_ARMS)
def test_the_arm_matches_contract_compiled_but_for_the_contract(db, arm):
    base = build_arm("contract_compiled", db, DOCS)
    opus = build_arm(arm, db, DOCS)
    try:
        assert [t.name for t in opus.tools] == [t.name for t in base.tools]
        assert opus.system_prompt.replace(
            frozen.load_compiled_contract(arm).name, "X"
        ) == base.system_prompt.replace(frozen.load_compiled_contract().name, "X")
    finally:
        base.close()
        opus.close()
