"""Experiment 4b's gap proposer: a session of another model family that sees
a database's documentation, schema and data -- never a question, gold,
reference SQL or result -- and proposes ranked candidate gaps."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "prep" / "procedures4b"))

import propose as pp  # noqa: E402

G2 = {
    "table": "returns",
    "column": "return_details",
    "json_path": "shipping.fee",
    "stored": "v * 100",
    "recover": "v / 100",
    "kind": "unit",
    "named_by_author_prompt": True,
    "silent_because": "a fee 100 times too large is still a number",
    "data_shows": "fees run 500-9000, far above any item value",
}


def _menu(**over):
    menu = {
        "g1": [{"sentinel": s, "why": "x"} for s in ("ANY", "*", "", "ALL")],
        "g2": [
            G2,
            {**G2, "column": "a", "kind": "offset", "named_by_author_prompt": False},
            {
                **G2,
                "column": "b",
                "kind": "reversed scale",
                "stored": "11 - v",
                "recover": "11 - v",
                "named_by_author_prompt": False,
            },
            {**G2, "column": "c"},
        ],
    }
    menu.update(over)
    return menu


def test_the_rule_tables_and_their_text_criteria():
    assert pp.RULE_TABLES["reverse_logistics"] == (
        "return_charge_rules",
        (
            "customer_segment",
            "return_channel",
            "item_category",
            "primary_reason",
            "item_condition",
        ),
    )
    assert pp.RULE_TABLES["labor_certification_applications"][0] == "review_schedule"
    assert (
        "monthly_volume_band" in pp.RULE_TABLES["labor_certification_applications"][1]
    )
    assert pp.RULE_TABLES["solar_panel"][1][-1] == "vol_tier"


def test_the_proposer_is_not_the_authors_family():
    from dce.author import AUTHOR_MODEL

    assert pp.PROPOSER_MODEL == "gpt-6-sol"
    assert pp.PROPOSER_MODEL != AUTHOR_MODEL


def test_a_good_menu_passes():
    assert pp.validate_menu(_menu()) == []


def test_g1_needs_four_distinct_sentinels():
    problems = pp.validate_menu(_menu(g1=[{"sentinel": "ANY", "why": "x"}] * 4))
    assert any("distinct" in p for p in problems)
    problems = pp.validate_menu(_menu(g1=[{"sentinel": "ANY", "why": "x"}]))
    assert any("four" in p for p in problems)


def test_g2_needs_two_kinds_the_author_prompt_does_not_name():
    named = [{**G2, "column": str(i)} for i in range(4)]
    assert any("not name" in p for p in pp.validate_menu(_menu(g2=named)))


def test_g2_transformations_must_invert():
    bad = {**G2, "recover": "v / 10"}
    problems = pp.validate_menu(_menu(g2=[bad, *_menu()["g2"][1:]]))
    assert any("invert" in p for p in problems)


@pytest.mark.parametrize("expr", ["__import__('os')", "v.real", "abs(v)", "x + 1"])
def test_g2_expressions_are_arithmetic_in_v_only(expr):
    bad = {**G2, "stored": expr}
    problems = pp.validate_menu(_menu(g2=[bad, *_menu()["g2"][1:]]))
    assert any("arithmetic" in p for p in problems)


def test_evaluate_is_plain_arithmetic():
    assert pp.evaluate("11 - v", 3) == 8
    assert pp.evaluate("(v + 1) * 2 / 4", 3) == 2


def test_the_prompt_carries_documentation_schema_and_pieces_but_no_question():
    from dce.compile.inputs import CompileInputs, KBEntry

    inputs = CompileInputs(
        benchmark="livesqlbench",
        db_name="reverse_logistics",
        entries=(
            KBEntry(id=30, kind="calculation", name="Schedule", text="rules"),
            KBEntry(id=36, kind="calculation", name="IRV", text="value"),
        ),
        ddl="CREATE TABLE returns (casenum text);",
        column_meanings="returns.casenum: the case",
        dialect_note="",
        db_path=Path("x.duckdb"),
        init_sql=(),
        sources=(),
        out_subdir="x",
    )
    text = pp.user_prompt(inputs, r_ids={30}, c_ids={36})
    assert "[30] (R) Schedule: rules" in text
    assert "[36] (C) IRV: value" in text
    assert "CREATE TABLE returns" in text
    assert "return_charge_rules" in text
    for word in ("question", "gold", "reference"):
        assert word not in text.lower()
