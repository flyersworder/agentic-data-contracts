"""Pydantic models for YAML data contract validation."""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, Field, field_validator, model_validator


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
    description: str | None = None
    preferred: bool = False
    allowed_principals: list[str] | None = None
    blocked_principals: list[str] | None = None

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def principals_mutually_exclusive(self) -> Self:
        if self.allowed_principals is not None and self.blocked_principals is not None:
            raise ValueError(
                f"AllowedTable for schema '{self.schema_}' cannot set both "
                f"allowed_principals and blocked_principals — pick one"
            )
        return self


class RequiredFilterValues(BaseModel):
    """Per-principal allowlist of literal values for a WHERE-clause column.

    Pairs with ``QueryCheck.required_filter_values``: when a query references
    ``column``, every literal in the predicate must be a subset of the values
    keyed under the resolved principal. Principals absent from the map fall
    through (the rule does not apply to them) — pair with ``allowed_principals``
    on the rule for a hard deny on unknown callers.
    """

    model_config = {"extra": "forbid"}

    column: str
    values_by_principal: dict[str, list[str | int | float]]

    @field_validator("values_by_principal")
    @classmethod
    def values_non_empty(
        cls, v: dict[str, list[str | int | float]]
    ) -> dict[str, list[str | int | float]]:
        for principal, values in v.items():
            if not values:
                raise ValueError(
                    f"values_by_principal[{principal!r}] must be non-empty; "
                    f"omit the key entirely to deny that principal"
                )
        return v


class QueryCheck(BaseModel):
    required_filter: str | None = None
    required_filter_values: RequiredFilterValues | None = None
    no_select_star: bool | None = None
    blocked_columns: list[str] | None = None
    require_limit: bool | None = None
    max_joins: int | None = None

    @model_validator(mode="after")
    def at_most_one_filter(self) -> Self:
        if self.required_filter is not None and self.required_filter_values is not None:
            raise ValueError(
                "QueryCheck must not set both required_filter and "
                "required_filter_values — pick one (they target the same column)"
            )
        return self


class ResultCheck(BaseModel):
    column: str | None = None
    min_value: float | None = None
    max_value: float | None = None
    not_null: bool | None = None
    min_rows: int | None = None
    max_rows: int | None = None


class SemanticRule(BaseModel):
    model_config = {"extra": "forbid"}

    name: str
    description: str
    enforcement: Enforcement
    table: str | None = None
    allowed_principals: list[str] | None = None
    blocked_principals: list[str] | None = None
    query_check: QueryCheck | None = None
    result_check: ResultCheck | None = None

    @field_validator("table")
    @classmethod
    def table_must_be_qualified(cls, v: str | None) -> str | None:
        if v is not None and v != "*" and "." not in v:
            raise ValueError(
                f"table must be fully qualified as 'schema.table', got '{v}'"
            )
        return v

    @model_validator(mode="after")
    def at_most_one_check(self) -> Self:
        if self.query_check is not None and self.result_check is not None:
            raise ValueError("Rule must not have both query_check and result_check")
        return self

    @model_validator(mode="after")
    def principals_mutually_exclusive(self) -> Self:
        if self.allowed_principals is not None and self.blocked_principals is not None:
            raise ValueError(
                f"Rule '{self.name}' cannot set both allowed_principals "
                f"and blocked_principals — pick one"
            )
        return self


class Domain(BaseModel):
    name: str
    summary: str
    description: str
    metrics: list[str] = Field(default_factory=list)
    tables: list[str] = Field(default_factory=list)
    last_reviewed: date | None = None


class SemanticConfig(BaseModel):
    source: SemanticSource | None = None
    allowed_tables: list[AllowedTable] = Field(default_factory=list)
    forbidden_operations: list[str] = Field(default_factory=list)
    rules: list[SemanticRule] = Field(default_factory=list)
    domains: list[Domain] = Field(default_factory=list)


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
