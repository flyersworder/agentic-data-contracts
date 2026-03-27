"""DataContract — loads YAML, provides accessors and system prompt generation."""

from __future__ import annotations

from pathlib import Path

import yaml

from agentic_data_contracts.core.schema import (
    DataContractSchema,
    Enforcement,
    SemanticRule,
)


class DataContract:
    """Main entry point: load a YAML data contract and interact with it."""

    def __init__(self, schema: DataContractSchema) -> None:
        self.schema = schema

    @property
    def name(self) -> str:
        return self.schema.name

    @classmethod
    def from_yaml(cls, path: str | Path) -> DataContract:
        text = Path(path).read_text()
        return cls.from_yaml_string(text)

    @classmethod
    def from_yaml_string(cls, text: str) -> DataContract:
        raw = yaml.safe_load(text)
        schema = DataContractSchema.model_validate(raw)
        return cls(schema=schema)

    def allowed_table_names(self) -> list[str]:
        names: list[str] = []
        for entry in self.schema.semantic.allowed_tables:
            for table in entry.tables:
                names.append(f"{entry.schema_}.{table}")
        return names

    def block_rules(self) -> list[SemanticRule]:
        return [
            r for r in self.schema.semantic.rules if r.enforcement == Enforcement.BLOCK
        ]

    def warn_rules(self) -> list[SemanticRule]:
        return [
            r for r in self.schema.semantic.rules if r.enforcement == Enforcement.WARN
        ]

    def log_rules(self) -> list[SemanticRule]:
        return [
            r for r in self.schema.semantic.rules if r.enforcement == Enforcement.LOG
        ]

    def to_system_prompt(self) -> str:
        sections: list[str] = []
        sections.append("## Data Contract: " + self.name)

        # Allowed tables
        table_names = self.allowed_table_names()
        if table_names:
            sections.append("\n### Allowed Tables\nYou may ONLY query these tables:")
            for name in table_names:
                sections.append(f"- {name}")

        # Forbidden operations
        if self.schema.semantic.forbidden_operations:
            ops = ", ".join(self.schema.semantic.forbidden_operations)
            sections.append(f"\n### Forbidden Operations\nYou must NEVER use: {ops}")

        # Rules
        block = self.block_rules()
        warn = self.warn_rules()
        if block or warn:
            sections.append("\n### Governance Rules")
            for rule in block:
                line = (
                    f"- **MUST** [{rule.name}]"
                    f" (violation blocks execution): {rule.description}"
                )
                sections.append(line)
            for rule in warn:
                line = (
                    f"- **SHOULD** [{rule.name}]"
                    f" (violation produces warning): {rule.description}"
                )
                sections.append(line)

        # Resource limits
        res = self.schema.resources
        if res:
            sections.append("\n### Resource Limits")
            if res.cost_limit_usd is not None:
                sections.append(f"- Max cost: ${res.cost_limit_usd:.2f}")
            if res.max_retries is not None:
                sections.append(f"- Max retries: {res.max_retries}")
            if res.token_budget is not None:
                sections.append(f"- Token budget: {res.token_budget:,}")
            if res.max_query_time_seconds is not None:
                sections.append(f"- Max query time: {res.max_query_time_seconds}s")
            if res.max_rows_scanned is not None:
                sections.append(f"- Max rows scanned: {res.max_rows_scanned:,}")

        # Temporal limits
        if self.schema.temporal and self.schema.temporal.max_duration_seconds:
            dur = self.schema.temporal.max_duration_seconds
            sections.append(f"\n### Time Limit\n- Max session duration: {dur}s")

        # Semantic source
        if self.schema.semantic.source:
            src = self.schema.semantic.source
            line = (
                f"\n### Semantic Source\nConsult {src.path} ({src.type})"
                " for metric definitions before computing metrics."
            )
            sections.append(line)

        return "\n".join(sections)
