from pathlib import Path
from typing import cast

import pytest
import sqlglot

from agentic_data_contracts.core.contract import DataContract
from agentic_data_contracts.validation.checkers import TableAllowlistChecker


def _parse(sql: str) -> sqlglot.exp.Expression:
    return cast(sqlglot.exp.Expression, sqlglot.parse_one(sql))


@pytest.fixture
def contract(fixtures_dir: Path) -> DataContract:
    return DataContract.from_yaml(fixtures_dir / "principals_contract.yml")


def _checker(principal: str | None) -> TableAllowlistChecker:
    return TableAllowlistChecker(principal_resolver=lambda: principal)


class TestOpenTable:
    def test_no_principal_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        assert _checker(None).check_ast(ast, contract).passed

    def test_any_principal_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM analytics.orders")
        assert _checker("anyone@co.com").check_ast(ast, contract).passed


class TestAllowlist:
    def test_match_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT salary FROM hr.salaries")
        assert _checker("alice@co.com").check_ast(ast, contract).passed

    def test_miss_named_caller(self, contract: DataContract) -> None:
        ast = _parse("SELECT salary FROM hr.salaries")
        result = _checker("bob@co.com").check_ast(ast, contract)
        assert not result.passed
        assert "restricted to other principals" in result.message
        assert "caller: 'bob@co.com'" in result.message
        assert "hr.salaries" in result.message

    def test_miss_no_caller(self, contract: DataContract) -> None:
        ast = _parse("SELECT salary FROM hr.salaries")
        result = _checker(None).check_ast(ast, contract)
        assert not result.passed
        assert "caller: '<no caller identified>'" in result.message


class TestBlocklist:
    def test_non_blocked_passes(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM raw.audit_log")
        assert _checker("alice@co.com").check_ast(ast, contract).passed

    def test_blocked_caller_denied(self, contract: DataContract) -> None:
        ast = _parse("SELECT id FROM raw.audit_log")
        result = _checker("intern@co.com").check_ast(ast, contract)
        assert not result.passed
        assert "caller: 'intern@co.com'" in result.message


class TestUndeclared:
    def test_undeclared_denied(self, contract: DataContract) -> None:
        ast = _parse("SELECT * FROM nowhere.nothing")
        result = _checker("alice@co.com").check_ast(ast, contract)
        assert not result.passed
        assert "Tables not in allowlist" in result.message
        assert "nowhere.nothing" in result.message


class TestMixedErrors:
    def test_undeclared_and_restricted_both_reported(
        self, contract: DataContract
    ) -> None:
        ast = _parse(
            "SELECT s.salary FROM hr.salaries s JOIN nowhere.nothing n ON s.id = n.id"
        )
        result = _checker("bob@co.com").check_ast(ast, contract)
        assert not result.passed
        assert "Tables not in allowlist: nowhere.nothing" in result.message
        assert "restricted to other principals" in result.message
        assert "hr.salaries" in result.message


class TestEmptyAllowlist:
    def test_empty_allowlist_denies_everyone(self, contract: DataContract) -> None:
        ast = _parse("SELECT * FROM sealed.top_secret")
        for principal in [None, "alice@co.com", "bob@co.com"]:
            result = _checker(principal).check_ast(ast, contract)
            assert not result.passed


class TestBackwardsCompat:
    def test_no_resolver_behaves_as_before(self, contract: DataContract) -> None:
        """Constructing without a resolver = resolver always returns None.

        Restricted tables are then denied (fail-closed), open tables allowed.
        """
        checker = TableAllowlistChecker()
        assert checker.check_ast(
            _parse("SELECT id FROM analytics.orders"), contract
        ).passed
        assert not checker.check_ast(
            _parse("SELECT salary FROM hr.salaries"), contract
        ).passed
