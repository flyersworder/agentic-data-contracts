"""DataContract — loads YAML, provides accessors and system prompt generation."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from agentic_data_contracts.core.schema import (
    DataContractSchema,
    Domain,
    Enforcement,
    SemanticRule,
)

if TYPE_CHECKING:
    from datetime import date

    from agentic_data_contracts.adapters.base import DatabaseAdapter
    from agentic_data_contracts.core.prompt import PromptRenderer
    from agentic_data_contracts.core.staleness import StaleFinding
    from agentic_data_contracts.semantic.base import SemanticSource


class DataContract:
    """Main entry point: load a YAML data contract and interact with it."""

    def __init__(self, schema: DataContractSchema) -> None:
        self.schema = schema
        self._tables_resolved: bool = False
        self._source_dir: Path | None = None

    @property
    def name(self) -> str:
        return self.schema.name

    @classmethod
    def from_yaml(cls, path: str | Path) -> DataContract:
        resolved = Path(path).resolve()
        text = resolved.read_text()
        contract = cls.from_yaml_string(text)
        contract._source_dir = resolved.parent
        return contract

    @classmethod
    def from_yaml_string(cls, text: str) -> DataContract:
        raw = yaml.safe_load(text)
        schema = DataContractSchema.model_validate(raw)
        return cls(schema=schema)

    def has_wildcard_tables(self) -> bool:
        """Check if any schema uses wildcard ('*') for tables."""
        return any("*" in entry.tables for entry in self.schema.semantic.allowed_tables)

    def resolve_tables(self, adapter: DatabaseAdapter, *, force: bool = False) -> None:
        """Expand wildcard tables using the database adapter.

        Replaces ["*"] entries with actual table names from the database.
        Results are cached — subsequent calls are no-ops unless force=True.
        """
        if self._tables_resolved and not force:
            return
        for entry in self.schema.semantic.allowed_tables:
            if "*" in entry.tables:
                entry.tables = adapter.list_tables(entry.schema_)
        self._tables_resolved = True

    def allowed_table_names(self) -> list[str]:
        names: list[str] = []
        for entry in self.schema.semantic.allowed_tables:
            for table in entry.tables:
                if table == "*":
                    continue  # unresolved wildcard — skip
                names.append(f"{entry.schema_}.{table}")
        return names

    def allowed_table_names_for(self, principal: str | None) -> set[str]:
        """Return the set of qualified table names the given principal may access.

        Rules:
        - Table with neither allowed_principals nor blocked_principals → open to all.
        - Table with either field set and principal=None or "" → denied (fail-closed).
        - Table with allowed_principals set → principal must be in the list.
        - Table with blocked_principals set → principal must not be in the list.
        """
        # Treat empty string as unauthenticated (same as None — fail-closed).
        resolved: str | None = principal if principal else None
        result: set[str] = set()
        for entry in self.schema.semantic.allowed_tables:
            restricted = (
                entry.allowed_principals is not None
                or entry.blocked_principals is not None
            )
            if restricted and resolved is None:
                continue
            if (
                entry.allowed_principals is not None
                and resolved not in entry.allowed_principals
            ):
                continue
            if (
                entry.blocked_principals is not None
                and resolved in entry.blocked_principals
            ):
                continue
            for table in entry.tables:
                if table == "*":
                    continue
                result.add(f"{entry.schema_}.{table}")
        return result

    def block_rules(self) -> list[SemanticRule]:
        return [
            r for r in self.schema.semantic.rules if r.enforcement == Enforcement.BLOCK
        ]

    def warn_rules(self) -> list[SemanticRule]:
        return [
            r for r in self.schema.semantic.rules if r.enforcement == Enforcement.WARN
        ]

    def get_domain(self, name: str) -> Domain | None:
        """Find a domain by exact name, or None."""
        for d in self.schema.semantic.domains:
            if d.name == name:
                return d
        return None

    def log_rules(self) -> list[SemanticRule]:
        return [
            r for r in self.schema.semantic.rules if r.enforcement == Enforcement.LOG
        ]

    def to_sdk_config(self) -> dict[str, object]:
        """Generate Claude Agent SDK configuration from contract limits.

        Returns a dict of SDK options derived from contract resource/temporal
        constraints, suitable for passing to ClaudeAgentOptions.
        """
        config: dict[str, object] = {}
        res = self.schema.resources
        if res:
            if res.token_budget is not None:
                config["task_budget"] = res.token_budget
            if res.max_retries is not None:
                config["max_turns"] = res.max_retries
        return config

    def load_semantic_source(self) -> SemanticSource | None:
        """Auto-load the semantic source from the contract's source config.

        Returns None if no source is configured.
        """
        source_config = self.schema.semantic.source
        if source_config is None:
            return None

        from agentic_data_contracts.semantic.cube import CubeSource
        from agentic_data_contracts.semantic.dbt import DbtSource
        from agentic_data_contracts.semantic.yaml_source import YamlSource

        source_type = source_config.type.lower()
        path = source_config.path
        if self._source_dir is not None and not Path(path).is_absolute():
            path = str(self._source_dir / path)

        loaders: dict[str, type] = {
            "yaml": YamlSource,
            "dbt": DbtSource,
            "cube": CubeSource,
        }

        loader_cls = loaders.get(source_type)
        if loader_cls is None:
            msg = (
                f"Unknown semantic source type: '{source_type}'."
                f" Supported: {list(loaders.keys())}"
            )
            raise ValueError(msg)

        return loader_cls(path)

    def find_stale(
        self,
        semantic_source: SemanticSource | None = None,
        *,
        threshold_days: int = 90,
        today: date | None = None,
    ) -> list[StaleFinding]:
        """Return governance artefacts whose review is missing or expired.

        Convenience entry point that pulls metric impacts from an optional
        ``SemanticSource`` and delegates to :func:`find_stale_reviews`.
        Pass no source to check only domain-level staleness.
        """
        from agentic_data_contracts.core.staleness import find_stale_reviews

        impacts = (
            semantic_source.get_metric_impacts() if semantic_source is not None else []
        )
        return find_stale_reviews(
            self, impacts, threshold_days=threshold_days, today=today
        )

    def to_system_prompt(
        self,
        semantic_source: SemanticSource | None = None,
        *,
        renderer: PromptRenderer | None = None,
    ) -> str:
        """Generate a formatted system prompt section for an AI agent.

        Args:
            semantic_source: Optional semantic source for metric/relationship data.
            renderer: Optional custom prompt renderer. Defaults to ClaudePromptRenderer.
        """
        if renderer is None:
            from agentic_data_contracts.core.prompt import ClaudePromptRenderer

            renderer = ClaudePromptRenderer()
        return renderer.render(self, semantic_source)
