import pytest
from dce.benchmarks import livesqlbench
from dce.compile.db import CompileDB
from dce.compile.entries import KBEntry
from dce.compile.session import (
    MAX_ATTEMPTS,
    NOTE_MARKER,
    DriverResult,
    EntryRecord,
    compile_entry,
    system_prompt,
)

USAGE = {"input_tokens": 10, "output_tokens": 5, "cached_tokens": 0, "usd": 0.001}


def scripted(*calls):
    """A driver that makes `calls` -- (tool name, kwargs) -- in order and
    keeps each tool's reply, as a model would see them."""
    replies = []

    def driver(*, system_prompt, user_prompt, tools):
        by_name = {t.__name__: t for t in tools}
        assert set(by_name) == {"run_sql", "submit_entry"}
        driver.user_prompt = user_prompt
        for name, kwargs in calls:
            replies.append(by_name[name](**kwargs))
        return DriverResult(messages=[{"scripted": True}], usage=dict(USAGE))

    driver.replies = replies
    return driver


@pytest.fixture
def ctx(lsb_data):
    inputs = livesqlbench.compile_inputs(lsb_data, "shop")
    db = CompileDB(inputs.db_path, init_sql=inputs.init_sql)
    yield inputs, db, db.tables(), {e.id: e for e in inputs.entries}
    db.close()


def test_a_calculation_validates_logs_queries_and_carries_the_text(ctx):
    inputs, db, tables, by_id = ctx
    prior = {
        2: EntryRecord(
            2,
            "predicate",
            "Shipped Order",
            "definitions",
            "metric",
            [
                {
                    "name": "shipped_order",
                    "description": "d",
                    "sql_expression": "status = 'shipped'",
                    "source_model": "main.orders",
                }
            ],
            [],
            "",
            1,
            None,
            [],
            {},
            [],
        )
    }
    driver = scripted(
        ("run_sql", {"sql": "SELECT DISTINCT status FROM orders"}),
        ("submit_entry", {"sql_expression": "SUM(nope)", "source_model": "orders"}),
        (
            "submit_entry",
            {
                "sql_expression": "SUM(CASE WHEN status = 'shipped' THEN amount END)",
                "source_model": "orders",
                "note": "Status values are lower case.\n\nChecked.",
            },
        ),
    )
    record = compile_entry(by_id[3], inputs, db, driver, prior=prior, tables=tables)
    assert (
        driver.replies[1].startswith("ERROR:")
        and "2 attempt(s) left" in driver.replies[1]
    )
    assert driver.replies[2] == "Recorded."
    assert "shipped_order over main.orders" in driver.user_prompt  # its child
    assert record.status == "metric" and record.attempts == 2
    [metric] = record.metrics
    assert metric["name"] == "shipped_revenue"
    assert metric["source_model"] == "main.orders"
    assert metric["description"] == (
        "Revenue a customer has realised.\n\n"
        "The sum of amount over the customer's Shipped Orders.\n\n"
        "Built on: shipped_order.\n\n"
        f"{NOTE_MARKER}Status values are lower case. Checked."
    )
    assert record.queries == [
        {"sql": "SELECT DISTINCT status FROM orders", "error": False}
    ]
    assert record.usage == USAGE and record.domain == "calculations"


def test_a_predicate_is_checked_as_a_where_clause(ctx):
    inputs, db, tables, by_id = ctx
    driver = scripted(
        (
            "submit_entry",
            {"sql_expression": "tier = 'gold'", "source_model": "customers"},
        )
    )
    record = compile_entry(by_id[5], inputs, db, driver, prior={}, tables=tables)
    assert record.status == "metric" and record.domain == "definitions"
    assert record.metrics[0]["description"].endswith("Built on: Customer Tier.")


def test_a_metric_that_never_validates_is_kept_as_untranslated_prose(ctx):
    inputs, db, tables, by_id = ctx
    bad = ("submit_entry", {"sql_expression": "SUM(qty) +", "source_model": "orders"})
    driver = scripted(*[bad] * (MAX_ATTEMPTS + 1))
    record = compile_entry(by_id[4], inputs, db, driver, prior={}, tables=tables)
    assert "No attempts left" in driver.replies[MAX_ATTEMPTS - 1]
    assert driver.replies[MAX_ATTEMPTS] == (
        "No attempts left; the entry is kept as prose. Stop."
    )
    assert record.status == "untranslated" and record.attempts == MAX_ATTEMPTS
    [metric] = record.metrics
    assert metric["sql_expression"] == "" and metric["untranslated"]
    assert metric["description"].startswith("Items per order.")


def test_no_submission_is_untranslated_too(ctx):
    inputs, db, tables, by_id = ctx
    record = compile_entry(by_id[4], inputs, db, scripted(), prior={}, tables=tables)
    assert record.status == "untranslated"
    assert record.metrics[0]["untranslated"] == "no submission"


def test_an_entry_that_cannot_be_encoded_is_untranslated_with_the_reason(ctx):
    inputs, db, tables, by_id = ctx
    driver = scripted(
        (
            "submit_entry",
            {
                "sql_expression": "",
                "source_model": "",
                "cannot_encode": "the database records no budget",
                "note": "Checked every column for cost or budget.",
            },
        )
    )
    record = compile_entry(by_id[5], inputs, db, driver, prior={}, tables=tables)
    assert driver.replies == ["Recorded."]
    assert record.status == "untranslated" and record.attempts == 1
    [metric] = record.metrics
    assert metric["sql_expression"] == "" and metric["source_model"] == ""
    assert metric["untranslated"] == "the database records no budget"
    assert metric["description"].endswith(
        NOTE_MARKER + "Checked every column for cost or budget."
    )


def test_the_prompt_offers_cannot_encode_instead_of_a_placeholder(ctx):
    inputs, *_ = ctx
    assert "cannot_encode" in system_prompt(inputs)


def test_an_unknown_source_table_is_an_error_not_a_metric(ctx):
    inputs, db, tables, by_id = ctx
    driver = scripted(
        ("submit_entry", {"sql_expression": "COUNT(*)", "source_model": "nowhere"}),
        ("submit_entry", {"sql_expression": "COUNT(*)", "source_model": "MAIN.Orders"}),
    )
    record = compile_entry(by_id[4], inputs, db, driver, prior={}, tables=tables)
    assert "nowhere" in driver.replies[0]
    assert record.metrics[0]["source_model"] == "main.orders"


def test_an_illustration_lands_on_its_column_or_the_reference_domain(ctx):
    inputs, db, tables, by_id = ctx
    driver = scripted(
        ("submit_entry", {"target": "customers.nope"}),
        ("submit_entry", {"target": "Customers.Tier", "note": "Only two values."}),
    )
    record = compile_entry(by_id[1], inputs, db, driver, prior={}, tables=tables)
    assert record.status == "prose" and record.metrics == []
    assert record.prose == [
        {
            "target": "column",
            "table": "customers",
            "column": "tier",
            "text": "Customer Tier: The loyalty tiers customers belong to.\n\n"
            "Values are 'gold' and 'silver'.\n\n"
            f"{NOTE_MARKER}Only two values.",
        }
    ]
    lost = compile_entry(by_id[6], inputs, db, scripted(), prior={}, tables=tables)
    assert lost.prose[0]["target"] == "domain" and lost.domain == "reference"


def test_a_section_yields_named_metrics_and_always_its_domain_text(ctx):
    inputs, db, tables, _ = ctx
    section = KBEntry(9, "section", "5 Fees", "## 5. Fees\nfee = qty * 2")
    taken = {
        1: EntryRecord(
            1,
            "section",
            "1 Intro",
            "1_intro",
            "metric",
            [
                {
                    "name": "fee",
                    "description": "",
                    "sql_expression": "1",
                    "source_model": "main.orders",
                }
            ],
            [],
            "",
            1,
            None,
            [],
            {},
            [],
        )
    }
    metric = {
        "name": "Order Fee",
        "kind": "calculation",
        "sql_expression": "qty * 2",
        "source_model": "orders",
        "description": "The fee on one order.",
    }
    driver = scripted(
        ("submit_entry", {"metrics": [{**metric, "name": "Fee"}]}),
        ("submit_entry", {"metrics": [metric], "note": "qty is never null."}),
    )
    record = compile_entry(section, inputs, db, driver, prior=taken, tables=tables)
    assert "already used" in driver.replies[0]
    assert record.domain == "5_fees" and record.status == "metric"
    assert record.metrics == [
        {
            "name": "order_fee",
            "description": 'The fee on one order.\n\nFrom the manual section "5 Fees".',
            "sql_expression": "qty * 2",
            "source_model": "main.orders",
        }
    ]
    assert record.prose == [
        {
            "target": "domain",
            "table": None,
            "column": None,
            "text": f"## 5. Fees\nfee = qty * 2\n\n{NOTE_MARKER}qty is never null.",
        }
    ]
    empty = compile_entry(
        section,
        inputs,
        db,
        scripted(("submit_entry", {"metrics": []})),
        prior={},
        tables=tables,
    )
    assert empty.status == "prose" and empty.prose[0]["text"].startswith("## 5. Fees")


def test_records_round_trip_and_the_system_prompt_carries_the_context(ctx):
    inputs, db, tables, by_id = ctx
    record = compile_entry(by_id[4], inputs, db, scripted(), prior={}, tables=tables)
    assert EntryRecord.from_dict(record.to_dict()) == record
    prompt = system_prompt(inputs)
    assert inputs.ddl in prompt and inputs.column_meanings in prompt
    assert "PostgreSQL" in prompt and str(MAX_ATTEMPTS) in prompt
