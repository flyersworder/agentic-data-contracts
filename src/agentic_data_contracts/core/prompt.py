"""PromptRenderer protocol and ClaudePromptRenderer implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.core.schema import SemanticRule
    from agentic_data_contracts.semantic.base import SemanticSource


@runtime_checkable
class PromptRenderer(Protocol):
    """Renders a DataContract into a string prompt.

    ``principal`` is the resolved caller identity. Renderers should use it to
    filter per-principal sections (e.g. `required_filter_values`) so callers
    only see policy that applies to themselves. Custom renderers may ignore it.
    """

    def render(
        self,
        contract: DataContract,
        semantic_source: SemanticSource | None = None,
        principal: str | None = None,
    ) -> str: ...


class ClaudePromptRenderer:
    """Renders a DataContract as XML-structured output for Claude agents."""

    # Max metrics to list individually before switching to compact summaries.
    METRIC_DETAIL_THRESHOLD = 20
    RELATIONSHIP_DETAIL_THRESHOLD = 30

    def render(
        self,
        contract: DataContract,
        semantic_source: SemanticSource | None = None,
        principal: str | None = None,
    ) -> str:
        lines: list[str] = []

        # Opening wrapper
        lines.append(f'<data_contract name="{contract.name}">')

        # 1. Allowed tables
        lines.extend(self._render_allowed_tables(contract))

        # 2. Domains (if defined) OR metrics OR semantic_source fallback
        domain_lines = self._render_domains(contract)
        if domain_lines:
            lines.extend(domain_lines)
        else:
            metrics_lines = self._render_metrics(contract, semantic_source)
            if metrics_lines:
                lines.extend(metrics_lines)
            elif contract.schema.semantic.source:
                lines.extend(self._render_semantic_source_fallback(contract))

        # 3. Table relationships
        rel_lines = self._render_relationships(semantic_source)
        if rel_lines:
            lines.extend(rel_lines)

        # 4. Resource limits (resources + temporal merged)
        resource_lines = self._render_resource_limits(contract)
        if resource_lines:
            lines.extend(resource_lines)

        # 5. Constraints (forbidden ops + rules)
        lines.extend(self._render_constraints(contract, principal=principal))

        # Closing wrapper
        lines.append("</data_contract>")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Section renderers
    # ------------------------------------------------------------------

    def _render_allowed_tables(self, contract: DataContract) -> list[str]:
        lines = ["<allowed_tables>"]
        for entry in contract.schema.semantic.allowed_tables:
            if entry.description is None and not entry.preferred:
                continue
            attrs = [f'name="{entry.schema_}"']
            if entry.preferred:
                attrs.append('preferred="true"')
            if entry.description is not None:
                attrs.append(f'description="{entry.description}"')
            lines.append(f"<schema {' '.join(attrs)} />")
        lines.append("Only query these tables:")
        for name in contract.allowed_table_names():
            lines.append(f"- {name}")
        lines.append("</allowed_tables>")
        return lines

    def _render_domains(
        self,
        contract: DataContract,
    ) -> list[str]:
        domains = contract.schema.semantic.domains
        if not domains:
            return []

        lines = ["<available_domains>"]
        for domain in domains:
            metric_count = len(domain.metrics)
            lines.append(
                f'  <domain name="{domain.name}"'
                f' summary="{domain.summary}"'
                f' metric_count="{metric_count}" />'
            )
        lines.append(
            '  <hint>Use lookup_domain("...") for business context,'
            ' then lookup_metric("...") for SQL definitions.</hint>'
        )
        lines.append("</available_domains>")
        return lines

    def _render_metrics(
        self,
        contract: DataContract,
        semantic_source: SemanticSource | None,
    ) -> list[str]:
        if semantic_source is None:
            return []

        metrics = semantic_source.get_metrics()
        if not metrics:
            return []

        lines: list[str] = ["<available_metrics>"]
        compact = len(metrics) > self.METRIC_DETAIL_THRESHOLD

        if compact:
            lines.append(f"  <count>{len(metrics)} metrics available.</count>")
            lines.append(
                "  <hint>Use list_metrics() to browse,"
                ' lookup_metric("...") to get SQL definitions.</hint>'
            )
        else:
            for m in metrics:
                lines.append(f'  <metric name="{m.name}">{m.description}</metric>')
            lines.append(
                "  <hint>Use lookup_metric tool to get the SQL definition"
                " before computing any KPI.</hint>"
            )

        lines.append("</available_metrics>")
        return lines

    def _render_semantic_source_fallback(self, contract: DataContract) -> list[str]:
        src = contract.schema.semantic.source
        assert src is not None
        lines = [
            "<semantic_source>",
            f"  <type>{src.type}</type>",
            f"  <path>{src.path}</path>",
            "  <hint>Consult this source for metric definitions"
            " before computing metrics.</hint>",
            "</semantic_source>",
        ]
        return lines

    def _render_relationships(
        self, semantic_source: SemanticSource | None
    ) -> list[str]:
        if semantic_source is None:
            return []
        rels = semantic_source.get_relationships()
        if not rels:
            return []

        lines = ["<table_relationships>"]

        if len(rels) > self.RELATIONSHIP_DETAIL_THRESHOLD:
            table_counts: dict[str, int] = {}
            for r in rels:
                from_table = r.from_.rsplit(".", 1)[0]
                to_table = r.to.rsplit(".", 1)[0]
                table_counts[from_table] = table_counts.get(from_table, 0) + 1
                if from_table != to_table:
                    table_counts[to_table] = table_counts.get(to_table, 0) + 1
            for table, count in sorted(table_counts.items()):
                lines.append(f'  <table name="{table}" join_count="{count}" />')
            lines.append(
                f"  <hint>{len(rels)} relationships defined."
                ' Use lookup_relationships(table="schema.table")'
                " to get join details and required filters.</hint>"
            )
        else:
            for r in rels:
                desc = r.description.strip()
                desc_attr = f' description="{desc}"' if desc else ""
                parts = [
                    f"<from>{r.from_}</from>",
                    f"<to>{r.to}</to>",
                ]
                if r.required_filter:
                    filt = r.required_filter.strip()
                    parts.append(f"<required_filter>{filt}</required_filter>")
                inner = "".join(parts)
                lines.append(
                    f'  <relationship type="{r.type}"{desc_attr}>{inner}</relationship>'
                )

        lines.append("</table_relationships>")
        return lines

    def _render_resource_limits(self, contract: DataContract) -> list[str]:
        res = contract.schema.resources
        temporal = contract.schema.temporal

        has_resources = res is not None and any(
            v is not None
            for v in [
                res.cost_limit_usd,
                res.max_query_time_seconds,
                res.max_retries,
                res.max_rows_scanned,
                res.token_budget,
            ]
        )
        has_temporal = (
            temporal is not None and temporal.max_duration_seconds is not None
        )

        if not has_resources and not has_temporal:
            return []

        lines = ["<resource_limits>"]
        if res is not None:
            if res.cost_limit_usd is not None:
                lines.append(
                    f"  <cost_limit_usd>{res.cost_limit_usd:.2f}</cost_limit_usd>"
                )
            if res.max_query_time_seconds is not None:
                val = res.max_query_time_seconds
                lines.append(
                    f"  <max_query_time_seconds>{val}</max_query_time_seconds>"
                )
            if res.max_retries is not None:
                lines.append(f"  <max_retries>{res.max_retries}</max_retries>")
            if res.max_rows_scanned is not None:
                lines.append(
                    f"  <max_rows_scanned>{res.max_rows_scanned}</max_rows_scanned>"
                )
            if res.token_budget is not None:
                lines.append(f"  <token_budget>{res.token_budget}</token_budget>")
        if has_temporal:
            assert temporal is not None
            dur = temporal.max_duration_seconds
            lines.append(f"  <max_duration_seconds>{dur}</max_duration_seconds>")
        lines.append("</resource_limits>")
        return lines

    def _render_constraints(
        self, contract: DataContract, principal: str | None = None
    ) -> list[str]:
        forbidden = contract.schema.semantic.forbidden_operations
        block_rules = contract.block_rules()
        warn_rules = contract.warn_rules()
        if not forbidden and not block_rules and not warn_rules:
            return []

        lines = ["<constraints>"]

        # Forbidden operations
        if forbidden:
            ops = ", ".join(forbidden)
            lines.append(f"Forbidden operations: {ops}")

        # Block rules
        if block_rules:
            lines.append("")
            lines.append("Rules (violations block execution):")
            for rule in block_rules:
                lines.append(f"- [{rule.name}] {rule.description}")
                lines.extend(self._render_rule_detail(rule, principal))

        # Warn rules
        if warn_rules:
            lines.append("")
            lines.append("Rules (violations produce warnings):")
            for rule in warn_rules:
                lines.append(f"- [{rule.name}] {rule.description}")
                lines.extend(self._render_rule_detail(rule, principal))

        lines.append("</constraints>")
        return lines

    def _render_rule_detail(
        self, rule: SemanticRule, principal: str | None
    ) -> list[str]:
        """Render rule-level detail that depends on the caller's identity.

        Currently inlines ``required_filter_values`` for the calling principal
        only — other principals' allowed value lists are not exposed in the
        prompt. Returns an empty list when there's nothing to add (the common
        case for rules without a per-principal detail to surface).
        """
        if principal is None or rule.query_check is None:
            return []
        rfv = rule.query_check.required_filter_values
        if rfv is None:
            return []
        values = rfv.values_by_principal.get(principal)
        if not values:
            return []
        rendered = ", ".join(repr(v) for v in values)
        return [
            f"  Allowed values for {rfv.column} (you): {rendered}",
        ]
