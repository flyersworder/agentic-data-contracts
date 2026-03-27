from pathlib import Path

from agentic_data_contracts.core.contract import DataContract


def test_from_yaml(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    assert dc.name == "revenue-analysis"
    assert len(dc.schema.semantic.allowed_tables) == 2
    assert dc.schema.resources is not None
    assert dc.schema.resources.cost_limit_usd == 5.00


def test_from_yaml_minimal(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "minimal_contract.yml")
    assert dc.name == "basic-query"
    assert dc.schema.resources is None


def test_from_yaml_string(fixtures_dir: Path) -> None:
    text = (fixtures_dir / "valid_contract.yml").read_text()
    dc = DataContract.from_yaml_string(text)
    assert dc.name == "revenue-analysis"


def test_to_system_prompt(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    prompt = dc.to_system_prompt()
    assert "analytics.orders" in prompt
    assert "analytics.customers" in prompt
    assert "DELETE" in prompt
    assert "tenant_isolation" in prompt
    assert "no_select_star" in prompt
    assert "cost_limit_usd" in prompt or "5.0" in prompt


def test_to_system_prompt_composable(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    user_prompt = "You are an analytics assistant."
    combined = f"{user_prompt}\n\n{dc.to_system_prompt()}"
    assert combined.startswith("You are an analytics assistant.")
    assert "analytics.orders" in combined


def test_allowed_table_names(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    names = dc.allowed_table_names()
    assert "analytics.orders" in names
    assert "analytics.customers" in names
    assert "analytics.subscriptions" in names
    assert not any(n.startswith("raw.") for n in names)


def test_block_rules(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    block_rules = dc.block_rules()
    assert len(block_rules) == 2
    names = [r.name for r in block_rules]
    assert "tenant_isolation" in names
    assert "no_select_star" in names


def test_warn_rules(fixtures_dir: Path) -> None:
    dc = DataContract.from_yaml(fixtures_dir / "valid_contract.yml")
    warn_rules = dc.warn_rules()
    assert len(warn_rules) == 1
    assert warn_rules[0].name == "use_approved_metrics"
