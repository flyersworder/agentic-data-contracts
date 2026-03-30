"""PromptRenderer protocol and ClaudePromptRenderer implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.semantic.base import SemanticSource


@runtime_checkable
class PromptRenderer(Protocol):
    """Renders a DataContract into a string prompt."""

    def render(
        self,
        contract: DataContract,
        semantic_source: SemanticSource | None = None,
    ) -> str: ...


class ClaudePromptRenderer:
    """Renders a DataContract as XML-structured output for Claude agents."""

    # Max metrics to list individually before switching to compact summaries.
    METRIC_DETAIL_THRESHOLD = 20

    def render(
        self,
        contract: DataContract,
        semantic_source: SemanticSource | None = None,
    ) -> str:
        lines: list[str] = []

        # Opening wrapper
        lines.append(f'<data_contract name="{contract.name}">')

        # 1. Allowed tables
        lines.extend(self._render_allowed_tables(contract))

        # 2. Available metrics or semantic_source fallback
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
        lines.extend(self._render_constraints(contract))

        # Closing wrapper
        lines.append("</data_contract>")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Section renderers
    # ------------------------------------------------------------------

    def _render_allowed_tables(self, contract: DataContract) -> list[str]:
        lines = ["<allowed_tables>"]
        for name in contract.allowed_table_names():
            lines.append(f"  <table>{name}</table>")
        lines.append("</allowed_tables>")
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
        domains = contract.schema.semantic.domains
        compact = len(metrics) > self.METRIC_DETAIL_THRESHOLD

        if compact and domains:
            # Large metric set with domains — show counts only
            metric_names = {m.name for m in metrics}
            for domain, names in domains.items():
                count = sum(1 for n in names if n in metric_names)
                if count:
                    lines.append(f'  <domain name="{domain}" count="{count}" />')
            lines.append(
                '  <hint>Use list_metrics(domain="...") to browse,'
                ' lookup_metric("...") to get SQL definitions.</hint>'
            )
        elif domains:
            # Small metric set with domains — list with descriptions
            metric_map = {m.name: m for m in metrics}
            for domain, names in domains.items():
                entries = [metric_map[n] for n in names if n in metric_map]
                if entries:
                    lines.append(f'  <domain name="{domain}">')
                    for m in entries:
                        lines.append(
                            f'    <metric name="{m.name}">{m.description}</metric>'
                        )
                    lines.append("  </domain>")
            lines.append(
                "  <hint>Use lookup_metric tool to get the SQL definition"
                " before computing any KPI.</hint>"
            )
        elif compact:
            # Large metric set without domains — just count
            lines.append(f"  <count>{len(metrics)} metrics available.</count>")
            lines.append(
                "  <hint>Use list_metrics() to browse,"
                ' lookup_metric("...") to get SQL definitions.</hint>'
            )
        else:
            # Small metric set without domains — list all
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
        for r in rels:
            lines.append(
                f'  <relationship type="{r.type}">'
                f"<from>{r.from_}</from>"
                f"<to>{r.to}</to>"
                "</relationship>"
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

    def _render_constraints(self, contract: DataContract) -> list[str]:
        lines = ["<constraints>"]

        # Forbidden operations
        forbidden = contract.schema.semantic.forbidden_operations
        if forbidden:
            lines.append("  <forbidden_operations>")
            for op in forbidden:
                lines.append(f"    <operation>{op}</operation>")
            lines.append("  </forbidden_operations>")

        # Block rules
        block_rules = contract.block_rules()
        if block_rules:
            lines.append("  <block_rules>")
            for rule in block_rules:
                lines.append(f'    <rule name="{rule.name}">{rule.description}</rule>')
            lines.append("  </block_rules>")

        # Warn rules
        warn_rules = contract.warn_rules()
        if warn_rules:
            lines.append("  <warn_rules>")
            for rule in warn_rules:
                lines.append(f'    <rule name="{rule.name}">{rule.description}</rule>')
            lines.append("  </warn_rules>")

        lines.append("</constraints>")
        return lines
