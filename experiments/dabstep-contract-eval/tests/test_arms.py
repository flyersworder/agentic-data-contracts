from pathlib import Path

import pytest
from dce.arms import ARMS, build_arm

DOCS = {"manual": "FEE RULE ALPHA: match on card_scheme.", "payments_readme": "cols"}


@pytest.fixture
def db(tmp_path: Path) -> Path:
    import duckdb

    path = tmp_path / "t.duckdb"
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE payments AS SELECT 1 AS psp_reference")
    con.close()
    return path


def test_three_arms_with_the_spec_names():
    assert ARMS == ("schema_only", "manual_prompt", "contract")


def test_schema_only_prompt_contains_no_manual_text(db):
    setup = build_arm("schema_only", db, DOCS)
    assert "FEE RULE ALPHA" not in setup.system_prompt


def test_manual_prompt_contains_the_manual_verbatim(db):
    setup = build_arm("manual_prompt", db, DOCS)
    assert "FEE RULE ALPHA: match on card_scheme." in setup.system_prompt


def test_contract_arm_prompt_contains_no_verbatim_manual_text(db):
    # Arm C must carry the manual's knowledge as structure, not as prose.
    setup = build_arm("contract", db, DOCS)
    assert "FEE RULE ALPHA" not in setup.system_prompt


def test_only_the_contract_arm_gets_the_governed_tools(db):
    names = {t.name for t in build_arm("contract", db, DOCS).tools}
    assert {"inspect_query", "run_query", "lookup_metric"} <= names

    for arm in ("schema_only", "manual_prompt"):
        ungoverned = {t.name for t in build_arm(arm, db, DOCS).tools}
        assert "inspect_query" not in ungoverned
        assert "lookup_metric" not in ungoverned


def test_ungoverned_arms_still_execute_sql(db):
    setup = build_arm("schema_only", db, DOCS)
    assert "execute_sql" in {t.name for t in setup.tools}


def test_only_the_contract_arm_carries_a_session(db):
    assert build_arm("contract", db, DOCS).session is not None
    assert build_arm("schema_only", db, DOCS).session is None


def test_unknown_arm_raises(db):
    with pytest.raises(ValueError):
        build_arm("some_other_arm", db, DOCS)
