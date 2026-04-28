import json
from pathlib import Path

import pytest

from agentic_data_contracts.adapters.duckdb import DuckDBAdapter
from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.tools.factory import create_tools


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "principals_contract.yml")


@pytest.fixture
def adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE SCHEMA IF NOT EXISTS hr;
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS sealed;
        CREATE TABLE analytics.orders (id INTEGER, amount DECIMAL(10,2));
        INSERT INTO analytics.orders VALUES (1, 10.00);
        CREATE TABLE hr.salaries (id INTEGER, salary DECIMAL(10,2));
        INSERT INTO hr.salaries VALUES (1, 100000.00);
        CREATE TABLE raw.audit_log (id INTEGER, event VARCHAR);
        INSERT INTO raw.audit_log VALUES (1, 'login');
        CREATE TABLE sealed.top_secret (id INTEGER, payload VARCHAR);
        INSERT INTO sealed.top_secret VALUES (1, 'classified');
        """
    )
    return db


def _tool(tools: list, name: str):
    return next(t for t in tools if t.name == name).callable


@pytest.mark.asyncio
class TestInspectQueryForwarding:
    async def test_alice_inspect_passes(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="alice@co.com")
        inspect = _tool(tools, "inspect_query")
        body = json.loads(
            (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0][
                "text"
            ]
        )
        assert body["valid"] is True

    async def test_bob_inspect_blocks_with_caller_in_message(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="bob@co.com")
        inspect = _tool(tools, "inspect_query")
        body = json.loads(
            (await inspect({"sql": "SELECT salary FROM hr.salaries"}))["content"][0][
                "text"
            ]
        )
        assert body["valid"] is False
        assert any("caller: 'bob@co.com'" in v for v in body["violations"])


@pytest.mark.asyncio
class TestDescribeTable:
    async def test_allowed_principal_succeeds(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="alice@co.com")
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        body = json.loads(text)
        assert body["schema"] == "hr"
        assert body["table"] == "salaries"

    async def test_restricted_for_other_principal(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="bob@co.com")
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        assert "restricted" in text
        assert "caller: 'bob@co.com'" in text

    async def test_restricted_for_unidentified(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter)
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        assert "caller: '<no caller identified>'" in text

    async def test_undeclared_table_unchanged_message(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="alice@co.com")
        describe = _tool(tools, "describe_table")
        text = (await describe({"schema": "nope", "table": "nothing"}))["content"][0][
            "text"
        ]
        assert "not in the allowed tables list" in text


@pytest.mark.asyncio
class TestPreviewTable:
    async def test_allowed_principal_succeeds(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="alice@co.com")
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        body = json.loads(text)
        # DuckDB returns Decimal; json.dumps(..., default=str) renders it as a string.
        assert body["rows"][0]["salary"] == "100000.00"

    async def test_restricted_for_other_principal(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        tools = create_tools(contract, adapter=adapter, caller_principal="bob@co.com")
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "hr", "table": "salaries"}))["content"][0][
            "text"
        ]
        assert "restricted" in text
        assert "caller: 'bob@co.com'" in text

    # Regression for issue #20: preview_table must honour per-rule
    # blocked_columns gates (incl. v0.14 per-principal scoping). Without
    # this, anyone allowed at the table level could read every column via
    # SELECT * — defeating the per-principal column policy.
    async def test_blocked_columns_rule_in_scope_refuses_preview(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        # Alice is in scope of `alice_only_column_block` (allowed_principals=[alice])
        # which blocks `pii_email` on analytics.orders. SELECT * would expose it.
        tools = create_tools(contract, adapter=adapter, caller_principal="alice@co.com")
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert text.startswith("BLOCKED")
        assert "alice_only_column_block" in text
        assert "pii_email" in text
        assert "alice@co.com" in text

    async def test_blocked_columns_rule_out_of_scope_allows_preview(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        # The intern is exempted from `non_intern_column_block`
        # (blocked_principals=[intern]) and is not allowed by
        # `alice_only_column_block` — neither rule applies, so preview runs.
        tools = create_tools(
            contract, adapter=adapter, caller_principal="intern@co.com"
        )
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        body = json.loads(text)
        assert body["schema"] == "analytics"
        assert body["table"] == "orders"
        assert isinstance(body["rows"], list)

    async def test_blocked_columns_rule_partial_scope(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        # Bob is outside `alice_only_column_block` but inside
        # `non_intern_column_block` (he's not the intern). Preview must refuse
        # and cite the rule that actually applies to him.
        tools = create_tools(contract, adapter=adapter, caller_principal="bob@co.com")
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert text.startswith("BLOCKED")
        assert "non_intern_column_block" in text
        assert "audit_payload" in text
        # Rule that does NOT apply to Bob must not be cited.
        assert "alice_only_column_block" not in text


def _preview_contract(extra_rules: str = "") -> DataContract:
    """Tiny inline contract whose only allowed table is analytics.orders.

    Test-local helper so per-edge-case rule shapes (wildcard table, table=None,
    warn/log enforcement, result_check, required_filter_values) don't pollute
    the shared principals_contract.yml fixture, which is exercised by many
    other test modules.
    """
    return DataContract.from_yaml_string(
        f"""
version: "1.0"
name: preview_edge_cases
semantic:
  allowed_tables:
    - schema: analytics
      tables: [orders]
  forbidden_operations: [DELETE, DROP, INSERT, UPDATE]
  rules:
{extra_rules}
""".rstrip()
        + "\n"
    )


@pytest.fixture
def preview_adapter() -> DuckDBAdapter:
    db = DuckDBAdapter(":memory:")
    db.connection.execute(
        """
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE analytics.orders (id INTEGER, amount DECIMAL(10,2));
        INSERT INTO analytics.orders VALUES (1, 10.00);
        """
    )
    return db


@pytest.mark.asyncio
class TestPreviewTableEdgeCases:
    """Edge cases for the per-rule data-visibility gating in preview_table.

    Documents the contract:
    - HONOURS rules that gate which DATA an in-scope caller may see:
      blocked_columns, required_filter_values.
    - BYPASSES rules that gate QUERY SHAPE (no_select_star, required_filter,
      require_limit, max_joins) — preview synthesises its own SELECT *.
    - SKIPS result_check rules — preview executes no result-check pipeline.
    - Surfaces warn/log enforcement as preambles, mirroring run_query.
    """

    async def test_wildcard_table_rule_applies(
        self, preview_adapter: DuckDBAdapter
    ) -> None:
        # T1: rule with `table: "*"` must apply to any in-scope preview.
        contract = _preview_contract(
            """
    - name: global_pii_block
      description: blocked column applies to all tables
      enforcement: block
      table: "*"
      query_check:
        blocked_columns: [secret]
"""
        )
        tools = create_tools(
            contract, adapter=preview_adapter, caller_principal="bob@co.com"
        )
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert text.startswith("BLOCKED")
        assert "global_pii_block" in text
        assert "secret" in text

    async def test_omitted_table_rule_applies(
        self, preview_adapter: DuckDBAdapter
    ) -> None:
        # T2: rule with no `table:` (None) is treated as global — same path
        # as wildcard but a distinct branch in the matching predicate.
        contract = _preview_contract(
            """
    - name: implicit_global_block
      description: no table key — applies everywhere
      enforcement: block
      query_check:
        blocked_columns: [pii]
"""
        )
        tools = create_tools(
            contract, adapter=preview_adapter, caller_principal="bob@co.com"
        )
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert text.startswith("BLOCKED")
        assert "implicit_global_block" in text

    async def test_unidentified_caller_blocked_by_unscoped_rule(
        self, preview_adapter: DuckDBAdapter
    ) -> None:
        # T3: an UNSCOPED block rule applies to everyone, including anonymous
        # callers — the message must use the "<no caller identified>" label.
        # (Per `principal_in_scope` semantics, scoped rules SKIP anonymous
        # callers — that's covered separately to document the distinction.)
        contract = _preview_contract(
            """
    - name: unscoped_block
      description: applies to every caller
      enforcement: block
      table: analytics.orders
      query_check:
        blocked_columns: [ssn]
"""
        )
        tools = create_tools(contract, adapter=preview_adapter)  # no caller
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert text.startswith("BLOCKED")
        assert "<no caller identified>" in text
        assert "unscoped_block" in text

    async def test_unidentified_caller_skips_principal_scoped_rule(
        self, preview_adapter: DuckDBAdapter
    ) -> None:
        # T3 (companion): a rule with `allowed_principals` set does not gate
        # anonymous callers — `principal_in_scope` returns False, the rule is
        # skipped, and preview proceeds. Documents existing semantics shared
        # with Validator: combine table-level allowed_principals if you want
        # anonymous fail-closed.
        contract = _preview_contract(
            """
    - name: alice_scoped_block
      description: only Alice is gated
      enforcement: block
      table: analytics.orders
      allowed_principals: [alice@co.com]
      query_check:
        blocked_columns: [pii_email]
"""
        )
        tools = create_tools(contract, adapter=preview_adapter)
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert "BLOCKED" not in text
        body = json.loads(text)
        assert body["table"] == "orders"

    async def test_warn_enforcement_surfaces_preamble(
        self, preview_adapter: DuckDBAdapter
    ) -> None:
        # T4: warn-level blocked_columns must NOT block preview but must
        # surface a WARNINGS preamble — symmetric with run_query.
        contract = _preview_contract(
            """
    - name: soft_audit_columns
      description: pre-launch audit, will tighten to block later
      enforcement: warn
      table: analytics.orders
      query_check:
        blocked_columns: [legacy_col]
"""
        )
        tools = create_tools(
            contract, adapter=preview_adapter, caller_principal="bob@co.com"
        )
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert text.startswith("WARNINGS:")
        assert "soft_audit_columns" in text
        # Body must still be present — preview was NOT blocked.
        json_start = text.index("{")
        body = json.loads(text[json_start:])
        assert body["table"] == "orders"

    async def test_log_enforcement_surfaces_preamble(
        self, preview_adapter: DuckDBAdapter
    ) -> None:
        # T4 companion: log-level surfaces a LOG preamble; preview proceeds.
        contract = _preview_contract(
            """
    - name: telemetry_only
      description: track but do not block
      enforcement: log
      table: analytics.orders
      query_check:
        blocked_columns: [tracked_col]
"""
        )
        tools = create_tools(
            contract, adapter=preview_adapter, caller_principal="bob@co.com"
        )
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert text.startswith("LOG:")
        assert "telemetry_only" in text

    async def test_result_check_rule_does_not_block(
        self, preview_adapter: DuckDBAdapter
    ) -> None:
        # T5: result_check rules guard query OUTPUT (post-execution), not
        # what columns/rows preview synthesises. They must not gate preview.
        contract = _preview_contract(
            """
    - name: row_cap
      description: result-side guard
      enforcement: block
      table: analytics.orders
      result_check:
        max_rows: 0
"""
        )
        tools = create_tools(
            contract, adapter=preview_adapter, caller_principal="bob@co.com"
        )
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert "BLOCKED" not in text
        body = json.loads(text)
        assert body["table"] == "orders"

    async def test_required_filter_values_in_scope_blocks_preview(
        self, preview_adapter: DuckDBAdapter
    ) -> None:
        # C2: v0.15 per-principal value allowlist on a WHERE filter is the
        # same class of bug as #20 — preview's filter-less SELECT * leaks rows
        # the rule was meant to gate. Caller IS keyed in values_by_principal,
        # so the rule applies and preview must refuse.
        contract = _preview_contract(
            """
    - name: tenant_scoped_to_alice
      description: alice may only see her tenants
      enforcement: block
      table: analytics.orders
      query_check:
        required_filter_values:
          column: tenant_id
          values_by_principal:
            alice@co.com: [acme]
"""
        )
        tools = create_tools(
            contract, adapter=preview_adapter, caller_principal="alice@co.com"
        )
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert text.startswith("BLOCKED")
        assert "tenant_scoped_to_alice" in text
        assert "tenant_id" in text

    async def test_required_filter_values_unmapped_caller_passes(
        self, preview_adapter: DuckDBAdapter
    ) -> None:
        # C2 companion: callers absent from values_by_principal fall through
        # the rule (mirrors RequiredFilterValuesChecker's documented contract,
        # checkers.py:339-340). Preview proceeds.
        contract = _preview_contract(
            """
    - name: tenant_scoped_to_alice
      description: only Alice is value-restricted
      enforcement: block
      table: analytics.orders
      query_check:
        required_filter_values:
          column: tenant_id
          values_by_principal:
            alice@co.com: [acme]
"""
        )
        tools = create_tools(
            contract, adapter=preview_adapter, caller_principal="bob@co.com"
        )
        preview = _tool(tools, "preview_table")
        text = (await preview({"schema": "analytics", "table": "orders"}))["content"][
            0
        ]["text"]
        assert "BLOCKED" not in text
        body = json.loads(text)
        assert body["table"] == "orders"


@pytest.mark.asyncio
class TestSemanticToolsUnaffected:
    """Explicit negative tests: metric/domain tools ignore caller_principal."""

    async def test_list_metrics_unaffected(
        self, contract: DataContract, adapter: DuckDBAdapter
    ) -> None:
        # Same call, different principals → same output.
        for principal in ["alice@co.com", "bob@co.com", None]:
            tools = create_tools(contract, adapter=adapter, caller_principal=principal)
            list_metrics = _tool(tools, "list_metrics")
            text = (await list_metrics({}))["content"][0]["text"]
            # principals_contract.yml has no semantic source → this exact reply.
            assert text == "No semantic source configured."


def test_create_tools_accepts_callable_principal(
    contract: DataContract, adapter: DuckDBAdapter
) -> None:
    # Must accept a zero-arg callable (Webex pattern) without raising.
    tools = create_tools(
        contract, adapter=adapter, caller_principal=lambda: "alice@co.com"
    )
    assert len(tools) == 9
