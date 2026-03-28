"""Tests for fuzzy metric search across all semantic sources."""

from pathlib import Path

import pytest

from agentic_data_contracts.semantic.cube import CubeSource
from agentic_data_contracts.semantic.dbt import DbtSource
from agentic_data_contracts.semantic.yaml_source import YamlSource


@pytest.fixture
def yaml_source(fixtures_dir: Path) -> YamlSource:
    return YamlSource(fixtures_dir / "semantic_source.yml")


@pytest.fixture
def dbt_source(fixtures_dir: Path) -> DbtSource:
    return DbtSource(fixtures_dir / "sample_dbt_manifest.json")


@pytest.fixture
def cube_source(fixtures_dir: Path) -> CubeSource:
    return CubeSource(fixtures_dir / "sample_cube_schema.yml")


class TestYamlSourceSearch:
    def test_exact_name_found(self, yaml_source: YamlSource) -> None:
        results = yaml_source.search_metrics("total_revenue")
        assert len(results) >= 1
        assert results[0].name == "total_revenue"

    def test_fuzzy_by_description(self, yaml_source: YamlSource) -> None:
        results = yaml_source.search_metrics("completed orders revenue")
        assert len(results) >= 1
        names = [m.name for m in results]
        assert "total_revenue" in names

    def test_fuzzy_partial_match(self, yaml_source: YamlSource) -> None:
        results = yaml_source.search_metrics("customers")
        assert len(results) >= 1
        names = [m.name for m in results]
        assert "active_customers" in names

    def test_no_match_returns_empty(self, yaml_source: YamlSource) -> None:
        results = yaml_source.search_metrics("xyznonexistent")
        assert results == []


class TestDbtSourceSearch:
    def test_fuzzy_search(self, dbt_source: DbtSource) -> None:
        results = dbt_source.search_metrics("revenue")
        assert len(results) >= 1
        assert results[0].name == "total_revenue"

    def test_no_match(self, dbt_source: DbtSource) -> None:
        results = dbt_source.search_metrics("xyznonexistent")
        assert results == []


class TestCubeSourceSearch:
    def test_fuzzy_search(self, cube_source: CubeSource) -> None:
        results = cube_source.search_metrics("revenue")
        assert len(results) >= 1
        assert results[0].name == "total_revenue"

    def test_no_match(self, cube_source: CubeSource) -> None:
        results = cube_source.search_metrics("xyznonexistent")
        assert results == []
