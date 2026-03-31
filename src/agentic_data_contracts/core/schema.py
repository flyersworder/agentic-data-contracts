"""Pydantic models for YAML data contract validation."""

from __future__ import annotations

from enum import StrEnum
from typing import Self

from pydantic import BaseModel, Field, model_validator


class Enforcement(StrEnum):
    BLOCK = "block"
    WARN = "warn"
    LOG = "log"


class SemanticSource(BaseModel):
    type: str  # dbt | cube | yaml | custom
    path: str


class AllowedTable(BaseModel):
    schema_: str = Field(alias="schema")
    tables: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class QueryCheck(BaseModel):
    required_filter: str | None = None
    no_select_star: bool | None = None
    blocked_columns: list[str] | None = None
    require_limit: bool | None = None
    max_joins: int | None = None


class ResultCheck(BaseModel):
    column: str | None = None
    min_value: float | None = None
    max_value: float | None = None
    not_null: bool | None = None
    min_rows: int | None = None
    max_rows: int | None = None


class SemanticRule(BaseModel):
    name: str
    description: str
    enforcement: Enforcement
    table: str | None = None
    query_check: QueryCheck | None = None
    result_check: ResultCheck | None = None

    @model_validator(mode="after")
    def at_most_one_check(self) -> Self:
        if self.query_check is not None and self.result_check is not None:
            raise ValueError("Rule must not have both query_check and result_check")
        return self


class SemanticConfig(BaseModel):
    source: SemanticSource | None = None
    allowed_tables: list[AllowedTable] = Field(default_factory=list)
    forbidden_operations: list[str] = Field(default_factory=list)
    rules: list[SemanticRule] = Field(default_factory=list)
    domains: dict[str, list[str]] = Field(default_factory=dict)


class ResourceConfig(BaseModel):
    cost_limit_usd: float | None = None
    max_query_time_seconds: float | None = None
    max_retries: int | None = None
    max_rows_scanned: int | None = None
    token_budget: int | None = None


class TemporalConfig(BaseModel):
    max_duration_seconds: float | None = None


class SuccessCriterionConfig(BaseModel):
    name: str
    weight: float = Field(ge=0.0, le=1.0, default=1.0)


class DataContractSchema(BaseModel):
    version: str = "1.0"
    name: str
    semantic: SemanticConfig
    resources: ResourceConfig | None = None
    temporal: TemporalConfig | None = None
    success_criteria: list[SuccessCriterionConfig] = Field(default_factory=list)
