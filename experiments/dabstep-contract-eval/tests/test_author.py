"""`dce.author`: one session writes a database's whole contract.

Unlike the compiler (one KB entry at a time), the author sees the schema, the
column meanings and the whole KB, explores the data read-only, and submits a
complete contract.yml and semantic.yml. A submission is accepted only when it
loads, every metric runs, every declared join passes the library's gate,
every table is allowed, the standard operations stay forbidden, and every KB
entry is mapped to where the contract holds it.
"""

import gzip
import json

import pytest
import yaml
from dce import author
from dce.benchmarks import livesqlbench
from dce.compile.session import DriverResult

USAGE = {"input_tokens": 10, "output_tokens": 5, "cached_tokens": 0, "usd": 0.01}

CONTRACT = """
version: "1.0"
name: shop-authored
semantic:
  source: {type: yaml, path: ./semantic.yml}
  allowed_tables:
    - schema: main
      tables: [customers, orders]
  forbidden_operations: [DELETE, DROP, TRUNCATE, UPDATE, INSERT, CREATE, ALTER,
    MERGE, GRANT, REVOKE, COPY]
  domains:
    - name: reference
      summary: Value meanings
      description: "Customer Tier: values are 'gold' and 'silver'."
  rules: []
"""

SEMANTIC = """
metrics:
  - name: shipped_order
    description: An order whose status is 'shipped'.
    sql_expression: "status = 'shipped'"
    source_model: main.orders
  - name: shipped_revenue
    description: Sum of amount over shipped orders.
    sql_expression: "SUM(CASE WHEN status = 'shipped' THEN amount END)"
    source_model: main.orders
  - name: basket_size
    description: Total qty divided by the number of orders, integer division.
    sql_expression: "SUM(qty) // COUNT(*)"
    source_model: main.orders
  - name: gold_revenue
    description: Revenue of gold customers' orders.
    sql_expression: "SUM(o.amount) FILTER (WHERE c.tier = 'gold')"
    source_model: main.orders
    from_sql: "main.orders o JOIN main.customers c ON c.custid = o.custid"
    grain: one row per order, with its customer
tables:
  - schema: main
    table: orders
    description: "Order Status: values are 'pending', 'shipped' and 'returned'."
    columns:
      - {name: status, description: Lifecycle state}
"""

COVERAGE = {
    "1": "domain:reference",
    "2": "metric:shipped_order",
    "3": "metric:shipped_revenue",
    "4": "metric:basket_size",
    "5": "metric:gold_revenue",
    "6": "table:orders",
}


def scripted(*calls):
    replies = []

    def driver(*, system_prompt, user_prompt, tools, **_):
        by_name = {t.__name__: t for t in tools}
        assert set(by_name) == {"run_sql", "submit_contract"}
        driver.system_prompt, driver.user_prompt = system_prompt, user_prompt
        for name, kwargs in calls:
            replies.append(by_name[name](**kwargs))
        return DriverResult(messages=[{"scripted": True}], usage=dict(USAGE))

    driver.replies = replies
    return driver


def _submit(**overrides):
    kwargs = {
        "contract_yml": CONTRACT,
        "semantic_yml": SEMANTIC,
        "kb_coverage": COVERAGE,
    }
    kwargs.update(overrides)
    return ("submit_contract", kwargs)


@pytest.fixture
def inputs(lsb_data):
    return livesqlbench.compile_inputs(lsb_data, "shop")


def test_a_valid_contract_is_written_with_its_log(inputs, tmp_path):
    driver = scripted(
        ("run_sql", {"sql": "SELECT DISTINCT status FROM orders"}), _submit()
    )
    out = tmp_path / "shop"
    status, spent = author.author_database(inputs, out, driver, spend_cap=1.0)
    assert (status, spent) == ("done", 0.01)
    assert driver.replies[1] == "Accepted."
    semantic = yaml.safe_load((out / "semantic.yml").read_text(encoding="utf-8"))
    assert [m["name"] for m in semantic["metrics"]][-1] == "gold_revenue"
    assert (out / "contract.yml").read_text(encoding="utf-8").startswith("# Authored")
    lines = [
        json.loads(x)
        for x in gzip.decompress((out / author.LOG).read_bytes()).splitlines()
    ]
    assert lines[0]["type"] == "header" and lines[0]["model"] == author.AUTHOR_MODEL
    assert lines[1]["accepted"] and lines[1]["queries"][0]["sql"].startswith("SELECT")


def test_the_contract_loads_and_its_join_passes_the_gate(inputs, tmp_path):
    from agentic_data_contracts import DataContract
    from agentic_data_contracts.semantic.yaml_source import YamlSource
    from agentic_data_contracts.validation import validate_metric_from_sql

    out = tmp_path / "shop"
    author.author_database(inputs, out, scripted(_submit()), spend_cap=1.0)
    contract = DataContract.from_yaml(out / "contract.yml")
    metrics = YamlSource(out / "semantic.yml").get_metrics()
    assert validate_metric_from_sql(contract, metrics, dialect="duckdb") == []


@pytest.mark.parametrize(
    ("override", "expect"),
    [
        ({"kb_coverage": {k: v for k, v in COVERAGE.items() if k != "4"}}, "4"),
        ({"kb_coverage": {**COVERAGE, "4": "metric:nope"}}, "nope"),
        ({"semantic_yml": SEMANTIC.replace("SUM(qty)", "SUM(qtyx)")}, "basket_size"),
        (
            {"contract_yml": CONTRACT.replace("[customers, orders]", "[orders]")},
            "customers",
        ),
        ({"contract_yml": CONTRACT.replace("DELETE, ", "")}, "DELETE"),
        (
            {
                "semantic_yml": SEMANTIC.replace(
                    'o.custid"', "o.custid WHERE c.tier = 'gold'\""
                )
            },
            "only a FROM",
        ),
        ({"contract_yml": "name: ["}, "contract.yml"),
    ],
)
def test_a_bad_submission_is_refused_with_the_reason(
    inputs, tmp_path, override, expect
):
    driver = scripted(_submit(**override), _submit())
    status, _ = author.author_database(inputs, tmp_path / "shop", driver, spend_cap=1.0)
    assert driver.replies[0].startswith("ERROR:") and expect in driver.replies[0]
    assert driver.replies[1] == "Accepted." and status == "done"


def test_no_accepted_submission_writes_no_contract(inputs, tmp_path):
    out = tmp_path / "shop"
    status, _ = author.author_database(
        inputs, out, scripted(_submit(contract_yml="name: [")), spend_cap=1.0
    )
    assert status == "failed"
    assert not (out / "contract.yml").exists()
    assert (out / author.LOG).exists()  # the spend is still recorded


def test_the_submissions_are_bounded(inputs, tmp_path):
    bad = _submit(contract_yml="name: [")
    driver = scripted(*[bad] * (author.MAX_SUBMISSIONS + 1))
    author.author_database(inputs, tmp_path / "shop", driver, spend_cap=1.0)
    assert "No submissions left" in driver.replies[-1]


def test_a_finished_database_is_skipped_and_the_cap_is_respected(inputs, tmp_path):
    out = tmp_path / "shop"
    author.author_database(inputs, out, scripted(_submit()), spend_cap=1.0)
    assert (
        author.author_database(inputs, out, scripted(), spend_cap=1.0)[0] == "skipped"
    )
    assert (
        author.author_database(
            inputs, tmp_path / "other", scripted(), spend_cap=1.0, spent=1.0
        )[0]
        == "capped"
    )


def test_the_prompt_carries_the_whole_kb_and_no_task(inputs, tmp_path):
    driver = scripted()
    author.author_database(inputs, tmp_path / "shop", driver, spend_cap=1.0)
    text = driver.system_prompt + driver.user_prompt
    for entry in inputs.entries:
        assert entry.name in text
    assert "shop_1" not in text  # no task id, question or gold
    assert inputs.ddl in text and inputs.column_meanings in text
