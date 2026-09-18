from pathlib import Path

import pytest

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.core.schema import (
    AllowedTable,
    DataContractSchema,
    Enforcement,
    QueryCheck,
    ResultCheck,
    SemanticConfig,
    SemanticRule,
)
from agentic_data_contracts.semantic.yaml_source import YamlSource
from agentic_data_contracts.validation.explain import ExplainResult
from agentic_data_contracts.validation.validator import Validator


class FakeExplainAdapter:
    def __init__(self, result: ExplainResult) -> None:
        self._result = result

    def explain(self, sql: str) -> ExplainResult:
        return self._result


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "valid_contract.yml")


@pytest.fixture
def validator(contract: DataContract) -> Validator:
    return Validator(contract)


def test_valid_query_passes(validator: Validator) -> None:
    result = validator.validate(
        "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
    )
    assert not result.blocked
    assert result.reasons == []


def test_forbidden_table_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM raw.payments WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("raw.payments" in r for r in result.reasons)


def test_select_star_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT * FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("SELECT *" in r for r in result.reasons)


def test_missing_filter_blocks(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM analytics.orders")
    assert result.blocked
    assert any("tenant_id" in r for r in result.reasons)


def test_delete_blocks(validator: Validator) -> None:
    result = validator.validate("DELETE FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("DELETE" in r for r in result.reasons)


def test_multiple_violations_all_reported(validator: Validator) -> None:
    result = validator.validate("SELECT * FROM raw.payments")
    assert result.blocked
    assert len(result.reasons) >= 2


def test_warnings_returned(validator: Validator) -> None:
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert not result.blocked


def test_minimal_contract_permissive(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    validator = Validator(dc)
    result = validator.validate("SELECT * FROM public.users")
    assert not result.blocked


def test_explain_cost_exceeds_limit(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=10.0, estimated_rows=100, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("cost" in r.lower() for r in result.reasons)


def test_explain_rows_exceeds_limit(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=1.0, estimated_rows=2000000, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked
    assert any("rows" in r.lower() for r in result.reasons)


def test_explain_schema_invalid_blocks(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(
            estimated_cost_usd=None,
            estimated_rows=None,
            schema_valid=False,
            errors=["Column not found"],
        )
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked


def test_explain_within_limits_passes(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=1.0, estimated_rows=500, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert not result.blocked


def test_explain_cost_passed_through(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(estimated_cost_usd=2.5, estimated_rows=500, schema_valid=True)
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert not result.blocked
    assert result.estimated_cost_usd == 2.5


def test_explain_fields_populated_when_schema_valid(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(
            estimated_cost_usd=0.05,
            estimated_rows=1500,
            schema_valid=True,
            errors=[],
        )
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.estimated_cost_usd == 0.05
    assert result.estimated_rows == 1500
    assert result.schema_valid is True
    assert result.explain_errors == []


def test_explain_fields_populated_when_schema_invalid(contract: DataContract) -> None:
    adapter = FakeExplainAdapter(
        ExplainResult(
            estimated_cost_usd=None,
            estimated_rows=None,
            schema_valid=False,
            errors=["column foo not found"],
        )
    )
    validator = Validator(contract, explain_adapter=adapter)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.blocked is True
    assert result.schema_valid is False
    assert result.explain_errors == ["column foo not found"]


def test_explain_fields_default_when_no_adapter(contract: DataContract) -> None:
    validator = Validator(contract, explain_adapter=None)
    result = validator.validate("SELECT id FROM analytics.orders WHERE tenant_id = 'x'")
    assert result.schema_valid is True
    assert result.explain_errors == []
    assert result.estimated_rows is None
    assert result.estimated_cost_usd is None


def test_table_scoped_query_check() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders", "customers"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="orders_filter",
                    description="Orders must filter by tenant_id",
                    enforcement=Enforcement.BLOCK,
                    table="analytics.orders",
                    query_check=QueryCheck(required_filter="tenant_id"),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate("SELECT id FROM analytics.orders")
    assert result.blocked

    result = validator.validate("SELECT id FROM analytics.customers")
    assert not result.blocked


def test_required_filter_tautology_is_blocked_end_to_end() -> None:
    """End-to-end: a blocking required_filter must reject `col = col` bypasses.

    This catches regressions where the low-level checker is hardened but the
    Validator wiring is missed (or vice versa).
    """
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["orders"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="tenant_isolation",
                    description="Orders must filter by tenant_id",
                    enforcement=Enforcement.BLOCK,
                    table="analytics.orders",
                    query_check=QueryCheck(required_filter="tenant_id"),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate(
        "SELECT id FROM analytics.orders WHERE tenant_id = tenant_id"
    )
    assert result.blocked, "Tautological predicate must not satisfy a blocking rule"


def test_global_query_check() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["a", "b"]}),
            ],
            rules=[
                SemanticRule(
                    name="no_star",
                    description="No select star",
                    enforcement=Enforcement.BLOCK,
                    query_check=QueryCheck(no_select_star=True),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate("SELECT * FROM public.a")
    assert result.blocked

    result = validator.validate("SELECT id FROM public.a")
    assert not result.blocked


def test_validate_results_blocks() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["metrics"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="wau_sanity",
                    description="WAU sanity",
                    enforcement=Enforcement.BLOCK,
                    result_check=ResultCheck(column="wau", max_value=8_000_000_000),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate_results(
        "SELECT wau FROM analytics.metrics",
        columns=["wau"],
        rows=[(12_000_000_000,)],
    )
    assert result.blocked
    assert any("wau" in r for r in result.reasons)


def test_validate_results_passes() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["metrics"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="wau_sanity",
                    description="WAU sanity",
                    enforcement=Enforcement.BLOCK,
                    result_check=ResultCheck(column="wau", max_value=8_000_000_000),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate_results(
        "SELECT wau FROM analytics.metrics",
        columns=["wau"],
        rows=[(1_000_000,)],
    )
    assert not result.blocked


def test_validate_results_table_scoping() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate(
                    {"schema": "analytics", "tables": ["metrics", "other"]}
                ),
            ],
            rules=[
                SemanticRule(
                    name="wau_sanity",
                    description="WAU sanity",
                    enforcement=Enforcement.BLOCK,
                    table="analytics.metrics",
                    result_check=ResultCheck(column="wau", max_value=100),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate_results(
        "SELECT wau FROM analytics.other",
        columns=["wau"],
        rows=[(999,)],
    )
    assert not result.blocked


def test_validate_results_warns() -> None:
    schema = DataContractSchema(
        name="test",
        semantic=SemanticConfig(
            allowed_tables=[
                AllowedTable.model_validate({"schema": "public", "tables": ["t"]}),
            ],
            rules=[
                SemanticRule(
                    name="empty_check",
                    description="Warn if empty",
                    enforcement=Enforcement.WARN,
                    result_check=ResultCheck(min_rows=1),
                ),
            ],
        ),
    )
    dc = DataContract(schema)
    validator = Validator(dc)

    result = validator.validate_results(
        "SELECT id FROM public.t",
        columns=["id"],
        rows=[],
    )
    assert not result.blocked
    assert len(result.warnings) == 1


def test_malformed_sql_blocks(validator: Validator) -> None:
    result = validator.validate("NOT VALID SQL AT ALL !!!")
    assert result.blocked
    assert any("parse error" in r.lower() for r in result.reasons)


def test_parse_error_sets_flag(validator: Validator) -> None:
    result = validator.validate("SELECT * FROM (")
    assert result.blocked
    assert result.parse_error


class TestMultipleStatements:
    """One query string may carry exactly one statement.

    A second statement is not analysed at all by a single-statement parse:
    sqlglot's ``parse_one`` either wraps the statements in a container node
    whose root no checker recognises, or (at the declared floor) silently keeps
    only the FIRST statement. Either way a forbidden operation smuggled behind
    a harmless ``SELECT`` reached the adapter unchecked.
    """

    def test_select_then_delete_is_blocked(self, validator: Validator) -> None:
        result = validator.validate(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'; "
            "DELETE FROM analytics.orders WHERE tenant_id = 'acme'"
        )
        assert result.blocked
        assert any("multiple statements" in r for r in result.reasons)
        # A policy block, not an unreadable query.
        assert not result.parse_error

    def test_two_selects_are_blocked(self, validator: Validator) -> None:
        result = validator.validate(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'; "
            "SELECT amount FROM analytics.orders WHERE tenant_id = 'acme'"
        )
        assert result.blocked
        assert any("multiple statements" in r for r in result.reasons)

    def test_a_trailing_semicolon_is_one_statement(self, validator: Validator) -> None:
        result = validator.validate(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'acme';"
        )
        assert not result.blocked
        assert result.reasons == []

    @pytest.mark.parametrize("tail", ["-- note", "/* note */"])
    def test_a_comment_after_the_final_semicolon_is_one_statement(
        self, validator: Validator, tail: str
    ) -> None:
        # sqlglot parses a comment-only tail as an `exp.Semicolon` node, not
        # None, so counting non-None entries mistook it for a second statement
        # and refused a query agents plausibly write, with a misleading reason.
        result = validator.validate(
            f"SELECT id FROM analytics.orders WHERE tenant_id = 'acme'; {tail}"
        )
        assert not result.blocked
        assert result.reasons == []

    def test_a_comment_does_not_hide_a_second_statement(
        self, validator: Validator
    ) -> None:
        # The exclusion above must drop ONLY the comment-only tail: a real
        # statement written after a comment is still a second statement.
        result = validator.validate(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'; "
            "/* note */ DELETE FROM analytics.orders WHERE tenant_id = 'acme'"
        )
        assert result.blocked
        assert any("multiple statements" in r for r in result.reasons)

    def test_validate_results_blocks_multiple_statements(
        self, validator: Validator
    ) -> None:
        result = validator.validate_results(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'; "
            "DELETE FROM analytics.orders WHERE tenant_id = 'acme'",
            ["id"],
            [(1,)],
        )
        assert result.blocked
        assert any("multiple statements" in r for r in result.reasons)


class TestTokenizerErrors:
    """An unterminated literal fails in the tokenizer, not the parser.

    ``sqlglot.errors.TokenError`` is not a ``ParseError`` subclass, so catching
    only ``ParseError`` let it escape ``validate`` as a raw exception.
    """

    def test_unterminated_literal_is_a_parse_error(self, validator: Validator) -> None:
        result = validator.validate("SELECT 'abc")
        assert result.blocked
        assert result.parse_error
        assert any("parse error" in r.lower() for r in result.reasons)

    def test_validate_results_survives_a_tokenizer_error(
        self, validator: Validator
    ) -> None:
        result = validator.validate_results("SELECT 'abc", ["x"], [(1,)])
        assert not result.blocked


def test_valid_query_has_no_parse_error(validator: Validator) -> None:
    result = validator.validate(
        "SELECT id, amount FROM analytics.orders WHERE tenant_id = 'acme'"
    )
    assert not result.parse_error


class TestValidatorWithSemanticSource:
    """Tests Validator integration with SemanticSource for relationship checking."""

    def test_validator_without_semantic_source_works(self, fixtures_dir: Path) -> None:
        contract = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
        validator = Validator(contract)
        result = validator.validate(
            "SELECT id FROM analytics.orders WHERE tenant_id = 'acme'"
        )
        assert not result.blocked

    def test_validator_with_semantic_source_emits_warnings(
        self, fixtures_dir: Path
    ) -> None:
        contract = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
        source = YamlSource(fixtures_dir / "relationships_checker.yml")
        validator = Validator(contract, semantic_source=source)
        # Join orders -> customers without required filter (status)
        result = validator.validate(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.tenant_id = 'acme'"
        )
        assert not result.blocked  # warnings only, never blocks
        assert any("status" in w for w in result.warnings)

    def test_validator_with_semantic_source_no_warnings_when_correct(
        self, fixtures_dir: Path
    ) -> None:
        contract = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
        source = YamlSource(fixtures_dir / "relationships_checker.yml")
        validator = Validator(contract, semantic_source=source)
        result = validator.validate(
            "SELECT o.id, c.name FROM analytics.orders o"
            " JOIN analytics.customers c ON o.customer_id = c.id"
            " WHERE o.tenant_id = 'acme' AND o.status != 'cancelled'"
        )
        assert not result.blocked
        assert result.warnings == []
