"""The compiler with `joins` on: a metric may declare the FROM it is written
against (`from_sql`, library 0.62) and what one row of it is (`grain`).

With `joins` off the compiler is the frozen one, prompt and tool schemas
unchanged, so the committed contracts stay reproducible. With it on, a
metric over several tables is accepted only when the library's gate passes
(only a FROM clause, governed tables, reads its source_model) and the query
`SELECT <expr> FROM <from_sql>` runs.
"""

import dataclasses
import inspect
import json

import pytest
from dce.benchmarks import livesqlbench
from dce.compile import run as run_module
from dce.compile.db import CompileDB
from dce.compile.emit import build, render_compiled, write
from dce.compile.entries import KBEntry
from dce.compile.session import (
    DriverResult,
    EntryRecord,
    compile_entry,
)

USAGE = {"input_tokens": 10, "output_tokens": 5, "cached_tokens": 0, "usd": 0.001}
JOIN = "main.customers c JOIN main.orders o ON o.custid = c.custid"
GRAIN = "one row per order, with its customer"


def scripted(*calls):
    replies = []

    def driver(*, system_prompt, user_prompt, tools):
        by_name = {t.__name__: t for t in tools}
        driver.tools = by_name
        driver.system_prompt = system_prompt
        driver.user_prompt = user_prompt
        for name, kwargs in calls:
            replies.append(by_name[name](**kwargs))
        return DriverResult(messages=[], usage=dict(USAGE))

    driver.replies = replies
    return driver


@pytest.fixture
def ctx(lsb_data):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    db = CompileDB(inputs.db_path, init_sql=inputs.init_sql)
    yield inputs, db, db.tables(), {e.id: e for e in inputs.entries}
    db.close()


def _joins(inputs):
    return dataclasses.replace(inputs, joins=True)


def _submit(ctx, entry_id, *submissions, joins=True):
    inputs, db, tables, by_id = ctx
    inputs = _joins(inputs) if joins else inputs
    driver = scripted(*(("submit_entry", s) for s in submissions))
    record = compile_entry(by_id[entry_id], inputs, db, driver, prior={}, tables=tables)
    return record, driver


def test_off_the_prompt_and_tool_are_the_frozen_compilers(ctx):
    inputs = ctx[0]
    assert dataclasses.replace(inputs).joins is False
    _, driver = _submit(ctx, 3, joins=False)
    assert "from_sql" not in driver.system_prompt
    assert "from_sql" not in inspect.signature(driver.tools["submit_entry"]).parameters
    assert "scalar subqueries" in driver.system_prompt


def test_on_the_prompt_explains_from_sql_and_grain(ctx):
    _, driver = _submit(ctx, 3)
    params = inspect.signature(driver.tools["submit_entry"]).parameters
    assert {"from_sql", "grain"} <= set(params)
    assert "from_sql" in driver.system_prompt and "grain" in driver.system_prompt


def test_a_calculation_over_a_declared_join_is_recorded(ctx):
    record, driver = _submit(
        ctx,
        3,
        {
            "sql_expression": "SUM(o.amount) FILTER (WHERE c.tier = 'gold')",
            "source_model": "orders",
            "from_sql": JOIN,
            "grain": GRAIN,
        },
    )
    assert driver.replies == ["Recorded."]
    [metric] = record.metrics
    assert metric["from_sql"] == JOIN
    assert metric["grain"] == GRAIN
    assert metric["source_model"] == "main.orders"


def test_a_one_table_metric_still_needs_no_join(ctx):
    record, driver = _submit(
        ctx, 3, {"sql_expression": "SUM(amount)", "source_model": "orders"}
    )
    assert driver.replies == ["Recorded."]
    assert "from_sql" not in record.metrics[0]


@pytest.mark.parametrize(
    ("from_sql", "grain", "expect"),
    [
        (JOIN, "", "grain"),
        (JOIN + " WHERE c.tier = 'gold'", GRAIN, "only a FROM clause"),
        ("main.customers c", GRAIN, "source_model"),
        ("main.orders o JOIN main.secret s ON s.id = o.orderid", GRAIN, "secret"),
    ],
)
def test_a_join_the_gate_refuses_is_an_error(ctx, from_sql, grain, expect):
    record, driver = _submit(
        ctx,
        3,
        {
            "sql_expression": "SUM(o.amount)",
            "source_model": "orders",
            "from_sql": from_sql,
            "grain": grain,
        },
    )
    assert driver.replies[0].startswith("ERROR:")
    assert expect in driver.replies[0]
    assert record.status == "untranslated"


def test_a_join_that_does_not_run_is_an_error(ctx):
    _, driver = _submit(
        ctx,
        3,
        {
            "sql_expression": "SUM(x.amount)",
            "source_model": "orders",
            "from_sql": JOIN,
            "grain": GRAIN,
        },
    )
    assert driver.replies[0].startswith("ERROR:")


def test_a_predicate_over_a_join_must_be_boolean(ctx):
    bad = {
        "sql_expression": "o.amount",
        "source_model": "orders",
        "from_sql": JOIN,
        "grain": GRAIN,
    }
    good = {**bad, "sql_expression": "o.status = 'shipped' AND c.tier = 'gold'"}
    record, driver = _submit(ctx, 2, bad, good)
    assert "BOOLEAN" in driver.replies[0]
    assert driver.replies[1] == "Recorded."
    assert record.metrics[0]["from_sql"] == JOIN


def test_a_section_metric_may_declare_a_join(ctx):
    inputs, db, tables, _ = ctx
    section = KBEntry(9, "section", "5 Fees", "## 5. Fees\nfee = amount * 2")
    driver = scripted(
        (
            "submit_entry",
            {
                "metrics": [
                    {
                        "name": "gold fee",
                        "kind": "calculation",
                        "sql_expression": "o.amount * 2",
                        "source_model": "main.orders",
                        "description": "The fee on one order.",
                        "from_sql": JOIN,
                        "grain": GRAIN,
                    }
                ]
            },
        )
    )
    record = compile_entry(section, _joins(inputs), db, driver, prior={}, tables=tables)
    assert driver.replies == ["Recorded."]
    assert record.metrics[0]["from_sql"] == JOIN
    assert record.metrics[0]["grain"] == GRAIN


def test_a_child_over_a_join_is_shown_with_it(ctx):
    inputs, db, tables, by_id = ctx
    child = EntryRecord(
        2,
        "predicate",
        "Shipped Order",
        "definitions",
        "metric",
        [
            {
                "name": "shipped_order",
                "description": "d",
                "sql_expression": "o.status = 'shipped'",
                "source_model": "main.orders",
                "from_sql": JOIN,
                "grain": GRAIN,
            }
        ],
        [],
        "",
        1,
        None,
    )
    driver = scripted()
    compile_entry(by_id[3], _joins(inputs), db, driver, prior={2: child}, tables=tables)
    assert JOIN in driver.user_prompt


def _record_with_join():
    return EntryRecord(
        3,
        "calculation",
        "Shipped Revenue",
        "calculations",
        "metric",
        [
            {
                "name": "shipped_revenue",
                "description": "Revenue.",
                "sql_expression": "SUM(o.amount)",
                "source_model": "main.orders",
                "from_sql": JOIN,
                "grain": GRAIN,
            }
        ],
        [],
        "",
        1,
        None,
    )


def test_emit_carries_the_join_and_manual_compiled_shows_it(ctx, tmp_path):
    inputs, _, tables, _ = ctx
    contract, semantic = build(inputs, [_record_with_join()], tables)
    [metric] = semantic["metrics"]
    assert (metric["from_sql"], metric["grain"]) == (JOIN, GRAIN)
    write(tmp_path, contract, semantic, inputs)
    text = render_compiled(tmp_path)
    assert JOIN in text and GRAIN in text


def test_the_emitted_join_passes_the_librarys_gate(ctx, tmp_path):
    from agentic_data_contracts import DataContract
    from agentic_data_contracts.semantic.yaml_source import YamlSource
    from agentic_data_contracts.validation import validate_metric_from_sql

    inputs, _, tables, _ = ctx
    contract, semantic = build(inputs, [_record_with_join()], tables)
    write(tmp_path, contract, semantic, inputs)
    loaded = DataContract.from_yaml(tmp_path / "contract.yml")
    source = YamlSource(tmp_path / "semantic.yml")
    metrics = source.get_metrics()
    assert metrics[0].from_sql == JOIN
    assert validate_metric_from_sql(loaded, metrics, dialect="duckdb") == []


def test_the_log_records_joins_and_a_resume_cannot_switch_it(ctx, tmp_path):
    inputs = ctx[0]
    header = run_module._header(_joins(inputs))
    assert header["joins"] is True
    assert "joins" not in run_module._header(inputs)  # frozen logs unchanged
    out = tmp_path / "shop"
    out.mkdir()
    (out / run_module.LOG).write_text(json.dumps(header) + "\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="joins"):
        run_module.compile_database(inputs, out, scripted(), spend_cap=1.0)


@pytest.mark.parametrize("joins", [False, True])
def test_the_section_tool_schema_lists_the_join_fields_only_when_on(ctx, joins):
    from pydantic_ai import Tool

    inputs, db, tables, _ = ctx
    inputs = _joins(inputs) if joins else inputs
    section = KBEntry(9, "section", "5 Fees", "## 5. Fees\nfee = amount * 2")
    driver = scripted()
    compile_entry(section, inputs, db, driver, prior={}, tables=tables)
    schema = json.dumps(Tool(driver.tools["submit_entry"]).function_schema.json_schema)
    assert ("from_sql" in schema) is joins
    assert ("grain" in schema) is joins


def test_the_joins_rule_offers_any_join_condition_and_derived_tables(ctx):
    _, driver = _submit(ctx, 3)
    prompt = " ".join(driver.system_prompt.split())
    assert "not only key equality" in prompt
    assert "derived table" in prompt


@pytest.mark.parametrize(
    "from_sql",
    [
        # a join on a condition, with NULL meaning "any value"
        "main.orders o JOIN main.customers c"
        " ON (c.tier IS NULL OR c.tier = 'gold') AND o.amount > 100",
        # a derived table for an aggregate the expression needs
        "main.orders o JOIN (SELECT custid, SUM(amount) AS total"
        " FROM main.orders GROUP BY custid) t ON t.custid = o.custid",
    ],
)
def test_a_condition_join_or_a_derived_table_is_accepted(ctx, from_sql):
    _, driver = _submit(
        ctx,
        3,
        {
            "sql_expression": "SUM(o.amount)",
            "source_model": "orders",
            "from_sql": from_sql,
            "grain": "one row per order",
        },
    )
    assert driver.replies == ["Recorded."]
