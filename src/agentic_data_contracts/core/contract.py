"""DataContract — loads YAML, provides accessors and system prompt generation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from agentic_data_contracts.core.principal import principal_in_scope
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
    from agentic_data_contracts.core.schema import (
        SemanticSource as SemanticSourceConfig,
    )
    from agentic_data_contracts.core.staleness import StaleFinding
    from agentic_data_contracts.semantic.base import SemanticSource


class SemanticSourceUnavailableError(RuntimeError):
    """A contract declares a semantic source that could not be loaded.

    Deliberately **not** a subclass of ``FileNotFoundError`` so generic
    file-error handling in a calling application cannot silently swallow it.
    A declared-but-unavailable semantic source is a governance failure: the
    agent would otherwise run with relationship/metric enforcement and the
    discovery tools (``list_metrics``, ``lookup_metric``, ``lookup_relationships``)
    silently degraded. Enforcement construction fails closed instead.
    """


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

        Note: ``resolve_principal()`` passes ``""`` through unchanged; this method
        treats ``""`` as unauthenticated for policy decisions. The split keeps the
        resolver neutral and concentrates access-policy in
        :func:`principal_in_scope`, which is the single source of truth shared
        with per-rule principal scoping in the Validator.
        """
        result: set[str] = set()
        for entry in self.schema.semantic.allowed_tables:
            if not principal_in_scope(
                principal, entry.allowed_principals, entry.blocked_principals
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
        """Load the semantic source declared by the contract.

        Prefers an inline snapshot (a frozen, self-contained contract — see
        :meth:`freeze_semantic_source`) over the external ``path``, so a contract
        rehydrated on another machine enforces identically with no file access.
        Returns None if no source is configured.
        """
        source_config = self.schema.semantic.source
        if source_config is None:
            return None

        if source_config.inline is not None:
            from agentic_data_contracts.semantic.yaml_source import YamlSource

            return YamlSource.from_raw(source_config.inline)

        return self._load_semantic_source_from_file(source_config)

    def _load_semantic_source_from_file(
        self, source_config: SemanticSourceConfig
    ) -> SemanticSource:
        """Load a semantic source from its external ``path``, failing closed."""
        from agentic_data_contracts.semantic.cube import CubeSource
        from agentic_data_contracts.semantic.dbt import DbtSource
        from agentic_data_contracts.semantic.yaml_source import YamlSource

        source_type = source_config.type.lower()
        path = source_config.path
        if path is None:
            raise SemanticSourceUnavailableError(
                f"Contract {self.name!r} declares a semantic source with neither"
                " an inline snapshot nor a path."
            )
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

        # Fail closed: a declared source that cannot be loaded must raise a
        # governance-specific error, not a raw OSError/parse error an app might
        # catch as routine file handling and proceed under-enforcing. OSError
        # covers missing file, a directory path, and permission denial;
        # yaml.YAMLError a malformed YAML/Cube source; json.JSONDecodeError a
        # malformed dbt manifest. (Content errors past a successful parse — e.g. a
        # KeyError on a missing metric name — are out of scope: that is a
        # malformed-but-present source, which still fails loud, just not as this
        # type.)
        try:
            return loader_cls(path)
        except (OSError, yaml.YAMLError, json.JSONDecodeError) as exc:
            raise SemanticSourceUnavailableError(
                f"Contract {self.name!r} declares a {source_type!r} semantic"
                f" source at {path!r}, but it could not be loaded: {exc}."
                " Refusing to build enforcement without the declared semantic"
                " source (this would silently drop relationship/metric"
                " enforcement and the discovery tools). Provide the source file"
                " or remove `semantic.source` from the contract."
            ) from exc

    def freeze_semantic_source(self, *, force: bool = False) -> None:
        """Snapshot the semantic source inline so the contract is self-contained.

        After freezing, ``model_dump`` → rehydrate preserves relationship/metric
        enforcement with no filesystem access — the contract artifact carries its
        own semantics (the basis for publishing a portable, content-addressed
        contract, e.g. as an ARD ``data-contract`` attestation). Captures metrics,
        relationships, metric impacts, and table column-schemas.

        Clears the external ``path`` and normalizes ``type`` to ``"yaml"`` once
        frozen: the path is machine-specific (so it must not enter the content
        address or leak into a published catalog), and the inline snapshot always
        rehydrates as a :class:`YamlSource`. Idempotent; a no-op when no source is
        configured, when one is already frozen (unless ``force``), or when there
        is no ``path`` to (re)load from. Loads from the external source, so it
        fails closed (:class:`SemanticSourceUnavailableError`) if it is
        unavailable.
        """
        from agentic_data_contracts.semantic.base import dump_semantic_source

        source_config = self.schema.semantic.source
        if source_config is None:
            return
        if source_config.inline is not None and not force:
            return
        # Nothing to (re)load from — an inline-only contract is already its own
        # source; force cannot improve on it.
        if source_config.path is None:
            return
        loaded = self._load_semantic_source_from_file(source_config)
        source_config.inline = dump_semantic_source(loaded)
        source_config.path = None
        source_config.type = "yaml"

    def find_stale(
        self,
        semantic_source: SemanticSource | None = None,
        *,
        threshold_days: int = 90,
        today: date | None = None,
    ) -> list[StaleFinding]:
        """Return governance artefacts whose review is missing or expired.

        Convenience entry point that pulls metrics and metric impacts from an
        optional ``SemanticSource`` and delegates to :func:`find_stale_reviews`.
        Pass no source to check only domain-level staleness.
        """
        from agentic_data_contracts.core.staleness import find_stale_reviews

        impacts = (
            semantic_source.get_metric_impacts() if semantic_source is not None else []
        )
        metrics = semantic_source.get_metrics() if semantic_source is not None else []
        return find_stale_reviews(
            self, impacts, metrics=metrics, threshold_days=threshold_days, today=today
        )

    def to_system_prompt(
        self,
        semantic_source: SemanticSource | None = None,
        *,
        renderer: PromptRenderer | None = None,
        principal: str | None = None,
    ) -> str:
        """Generate a formatted system prompt section for an AI agent.

        Args:
            semantic_source: Optional semantic source for metric/relationship data.
            renderer: Optional custom prompt renderer. Defaults to ClaudePromptRenderer.
            principal: Resolved caller identity. Renderers use this to filter
                per-principal sections (e.g. `required_filter_values`) so the
                prompt only exposes policy that applies to this caller.
        """
        if renderer is None:
            from agentic_data_contracts.core.prompt import ClaudePromptRenderer

            renderer = ClaudePromptRenderer()
        return renderer.render(self, semantic_source, principal=principal)
