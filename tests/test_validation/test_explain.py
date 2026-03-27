from agentic_data_contracts.validation.explain import ExplainAdapter, ExplainResult


def test_explain_result_creation() -> None:
    result = ExplainResult(
        estimated_cost_usd=1.50,
        estimated_rows=50000,
        schema_valid=True,
        errors=[],
    )
    assert result.estimated_cost_usd == 1.50
    assert result.estimated_rows == 50000
    assert result.schema_valid
    assert result.errors == []


def test_explain_result_with_errors() -> None:
    result = ExplainResult(
        estimated_cost_usd=None,
        estimated_rows=None,
        schema_valid=False,
        errors=["Column 'foo' not found"],
    )
    assert not result.schema_valid
    assert len(result.errors) == 1


def test_explain_adapter_is_protocol() -> None:
    class FakeAdapter:
        def explain(self, sql: str) -> ExplainResult:
            return ExplainResult(
                estimated_cost_usd=0.01,
                estimated_rows=100,
                schema_valid=True,
                errors=[],
            )

    adapter: ExplainAdapter = FakeAdapter()
    result = adapter.explain("SELECT 1")
    assert result.schema_valid
