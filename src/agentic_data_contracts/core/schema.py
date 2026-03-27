"""Pydantic models for YAML data contract validation."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


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


class SemanticRule(BaseModel):
    name: str
    description: str
    enforcement: Enforcement
    filter_column: str | None = None  # explicit column for required filter rules


class SemanticConfig(BaseModel):
    source: SemanticSource | None = None
    allowed_tables: list[AllowedTable] = Field(default_factory=list)
    forbidden_operations: list[str] = Field(default_factory=list)
    rules: list[SemanticRule] = Field(default_factory=list)


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
