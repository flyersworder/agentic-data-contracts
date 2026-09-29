"""The uninterpreted contract is a control, so its diff from the real one must
be exactly what it claims to remove and nothing else.

It removes the contract's own reading of the manual: every `INTERPRETATION`
sentence, and the one reading that goes beyond the manual in code as well as
prose -- the empty list as a wildcard for list-typed `fees` fields -- reverted
to the manual's literal "null means all". Everything else must be
byte-identical, or the arm measures some unlisted difference instead.
"""

import copy
import re
from pathlib import Path

import yaml
from dce import uninterpreted

from agentic_data_contracts import DataContract

ROOT = Path(__file__).parent.parent
REAL = ROOT / "contract"
UNINTERPRETED = ROOT / "contract_uninterpreted"

LIST_FIELDS = ("aci", "account_type", "merchant_category_code")


def _load(d: Path) -> tuple[dict, dict]:
    return (
        yaml.safe_load((d / "contract.yml").read_text(encoding="utf-8")),
        yaml.safe_load((d / "semantic.yml").read_text(encoding="utf-8")),
    )


def test_generation_is_idempotent(tmp_path: Path):
    first = uninterpreted.build(REAL, tmp_path / "a")
    second = uninterpreted.build(REAL, tmp_path / "b")
    for name in ("contract.yml", "semantic.yml"):
        assert (first / name).read_text(encoding="utf-8") == (second / name).read_text(
            encoding="utf-8"
        )


def test_committed_artifact_matches_the_generator(tmp_path: Path):
    fresh = uninterpreted.build(REAL, tmp_path / "fresh")
    for name in ("contract.yml", "semantic.yml"):
        assert (fresh / name).read_text(encoding="utf-8") == (
            UNINTERPRETED / name
        ).read_text(encoding="utf-8"), (
            f"{name} is stale -- re-run `python -m dce.uninterpreted`"
        )


def test_the_real_contract_has_seven_interpretations():
    """The paper names seven. If the frozen contract ever carries a different
    number, the arm's definition has drifted from what the paper describes."""
    _, real_s = _load(REAL)
    n = sum("INTERPRETATION:" in m.get("description", "") for m in real_s["metrics"])
    assert n == 7


def test_no_interpretation_survives():
    uc, us = _load(UNINTERPRETED)
    text = yaml.safe_dump(uc) + yaml.safe_dump(us)
    assert "INTERPRETATION" not in text


def test_the_empty_list_wildcard_is_gone_from_code_and_prose():
    _, us = _load(UNINTERPRETED)
    for metric in us["metrics"]:
        assert not re.search(r"len\s*\(", metric.get("sql_expression", "")), metric[
            "name"
        ]
    fees = next(t for t in us["tables"] if t["table"] == "fees")
    for column in fees["columns"]:
        assert "Empty" not in column.get("description", ""), column["name"]


def test_list_fields_keep_the_manuals_null_wildcard():
    """Reverted to the manual's reading, not deleted: a rule with a NULL list
    still applies to everything, as the manual says."""
    _, us = _load(UNINTERPRETED)
    sql = next(
        m["sql_expression"]
        for m in us["metrics"]
        if m["name"] == "fee_rule_matches_transaction"
    )
    for field in LIST_FIELDS:
        assert f"f.{field} IS NULL OR list_contains(f.{field}," in " ".join(sql.split())


def test_nothing_else_differs():
    """Undo the documented edits on the real contract and the two must be
    identical. Any other difference fails here, named."""
    real_c, real_s = _load(REAL)
    uc, us = _load(UNINTERPRETED)

    assert uc["name"] == f"{real_c['name']}-uninterpreted"
    expected_c = copy.deepcopy(real_c)
    expected_c["name"] = uc["name"]
    assert uc == expected_c

    expected_s = uninterpreted.uninterpret_semantic(copy.deepcopy(real_s))
    assert us == expected_s
    real_metrics = {m["name"]: m for m in real_s["metrics"]}
    for metric in us["metrics"]:
        real = real_metrics[metric["name"]]
        assert set(metric) == set(real), metric["name"]
        for key in metric:
            if key in ("description", "sql_expression"):
                continue
            assert metric[key] == real[key], (metric["name"], key)
        if "INTERPRETATION:" not in real.get("description", ""):
            assert metric.get("description") == real.get("description"), metric["name"]
        if metric["name"] != "fee_rule_matches_transaction":
            assert metric.get("sql_expression") == real.get("sql_expression"), metric[
                "name"
            ]
    assert us["relationships"] == real_s["relationships"]
    for table, real_table in zip(us["tables"], real_s["tables"], strict=True):
        assert table["table"] == real_table["table"]
        for column, real_col in zip(
            table["columns"], real_table["columns"], strict=True
        ):
            if table["table"] == "fees" and column["name"] in LIST_FIELDS:
                continue
            assert column == real_col, (table["table"], column["name"])


def test_the_text_before_each_interpretation_is_kept():
    """Only the INTERPRETATION sentence goes; the manual's own words before it
    stay, or the arm would also lose what the manual states outright."""
    _, real_s = _load(REAL)
    _, us = _load(UNINTERPRETED)
    stripped = {m["name"]: m["description"] for m in us["metrics"]}
    for metric in real_s["metrics"]:
        desc = metric.get("description", "")
        if "INTERPRETATION:" in desc:
            kept = desc.split("INTERPRETATION:")[0].strip()
            assert stripped[metric["name"]].strip() == kept, metric["name"]
            assert kept


def test_it_loads_as_a_contract():
    contract = DataContract.from_yaml(UNINTERPRETED / "contract.yml")
    prompt = contract.to_system_prompt()
    assert "INTERPRETATION" not in prompt
