"""PromptRenderer protocol and XmlPromptRenderer implementation."""

from __future__ import annotations

import enum
import logging
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import yaml

if TYPE_CHECKING:
    from agentic_data_contracts.core.contract import DataContract
    from agentic_data_contracts.core.schema import SemanticRule
    from agentic_data_contracts.semantic.base import SemanticSource

logger = logging.getLogger(__name__)


#: Renders one extras section. Receives ``(section_name, payload)`` and returns
#: the **complete** section, its own enclosing tags included — the renderer
#: contributes placement and ordering only, never wrapping. Taking the name lets
#: one function serve several sections.
ExtraSectionRenderer = Callable[[str, Any], list[str]]


def _default_extra_section(name: str, payload: Any) -> list[str]:
    """Wrap an extras payload in an XML tag, body dumped back as YAML.

    The payload has arbitrary nesting and no schema, so a hand-rolled XML walker
    would be guessing at its shape. Dumping the author's own structure back is
    faithful, handles nesting for free, stays compact, and models read YAML. The
    XML tag keeps the section boundary consistent with the rest of the prompt.

    Authored content is not escaped, so prose containing ``</name>`` would break
    the enclosing tag. That matches the existing ``description="..."``
    attributes, which do not escape quotes either: contract content is trusted
    at one level throughout.
    """
    body = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True).rstrip("\n")
    return [f"<{name}>", body, f"</{name}>"]


def _metric_element(name: str, *, indent: int) -> str:
    """A names-only ``<metric>`` element.

    The name is written raw, like every other value in this renderer: the
    prompt is read as text, not parsed as XML, and ``lookup_metric`` matches
    the raw name, so an escaped ``&amp;`` would cost the agent a fuzzy-match
    hop.
    """
    return f'{" " * indent}<metric name="{name}" />'


class _Unset(enum.Enum):
    """Sentinel distinguishing "argument omitted" from an explicit ``None``.

    An enum rather than ``object()``: ty narrows enum members inside a union, so
    ``int | None | _Unset`` resolves to ``int | None`` after an ``is _UNSET``
    check without needing a cast at the use site.
    """

    TOKEN = 0


_UNSET = _Unset.TOKEN


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


class XmlPromptRenderer:
    """Renders a DataContract as XML-structured output for LLM agents.

    XML tags mark section boundaries. That convention originates in Anthropic's
    prompting guidance, but nothing here is Claude-specific — no SDK import, no
    model-specific handling — and every frontier model reads it. The name states
    the axis on which ``PromptRenderer`` implementations differ: output format,
    not vendor.
    """

    # Defaults for the constructor parameters below. Kept as class attributes so
    # an existing subclass that overrides them keeps working; an explicit
    # constructor argument takes precedence over the class attribute.
    METRIC_DETAIL_THRESHOLD = 20
    RELATIONSHIP_DETAIL_THRESHOLD = 30
    # Above this many metric index entries (a metric listed under three domains
    # counts three times) the names are dropped from the prompt. Names cost a
    # few tokens each; descriptions, which the detail threshold governs, cost a
    # paragraph.
    METRIC_INDEX_THRESHOLD = 200

    def __init__(
        self,
        *,
        extra_sections: Sequence[str] | Mapping[str, ExtraSectionRenderer | None] = (),
        metric_detail_threshold: int | None | _Unset = _UNSET,
        relationship_detail_threshold: int | None | _Unset = _UNSET,
        metric_index_threshold: int | None | _Unset = _UNSET,
    ) -> None:
        """``None`` for any threshold means "never degrade".

        ``metric_index_threshold`` left unset follows the detail threshold's
        opt-out: ``metric_detail_threshold=None`` also means "never drop metric
        names". It is resolved at render time, so the attribute can be changed
        after construction like the other two.

        These were class constants until v0.40.0, which made the only opt-out
        subclassing — so the consumer who knows their own prompt budget could not
        express it. The budget call belongs where the budget knowledge is.
        """
        # ``str`` satisfies ``Sequence[str]`` structurally, so a bare string
        # would silently iterate characters — eleven one-character section names,
        # none of them carried. Against a non-extensible source that failure
        # produces no warning at all, which is the exact class of silence this
        # release exists to remove.
        if isinstance(extra_sections, str):
            raise TypeError(
                "XmlPromptRenderer(extra_sections=...) takes a sequence or"
                " mapping of section names, not a bare string — pass"
                f" [{extra_sections!r}], not {extra_sections!r}."
            )
        # Opt-in by name in both directions: nothing renders unless named, so a
        # section authored as internal bookkeeping never leaks into a prompt.
        # A mapping value of None selects the default formatter.
        self.extra_sections: dict[str, ExtraSectionRenderer | None]
        if isinstance(extra_sections, Mapping):
            self.extra_sections = dict(extra_sections)
        else:
            self.extra_sections = dict.fromkeys(extra_sections)
        self.metric_detail_threshold: int | None = (
            self.METRIC_DETAIL_THRESHOLD
            if metric_detail_threshold is _UNSET
            else metric_detail_threshold
        )
        self.relationship_detail_threshold: int | None = (
            self.RELATIONSHIP_DETAIL_THRESHOLD
            if relationship_detail_threshold is _UNSET
            else relationship_detail_threshold
        )
        # The raw argument, resolved at render time; read the resolved value
        # through the ``metric_index_threshold`` property.
        self._metric_index_threshold_arg: int | None | _Unset = metric_index_threshold

    @property
    def metric_index_threshold(self) -> int | None:
        """The resolved index threshold (``None`` means never drop names)."""
        return self._resolve_metric_thresholds()[1]

    @metric_index_threshold.setter
    def metric_index_threshold(self, value: int | None) -> None:
        self._metric_index_threshold_arg = value

    def _resolve_metric_thresholds(self) -> tuple[int | None, int | None]:
        """``(detail, index)`` thresholds, read at render time.

        Read through ``getattr`` with the class constants as fallback: before
        v0.40.0 there was no ``__init__`` here, so a subclass could legally
        define one of its own and never call ``super().__init__()``. Such an
        instance has none of these attributes, and the class constants were
        kept precisely so those subclasses keep working.
        """
        detail: int | None = getattr(
            self, "metric_detail_threshold", self.METRIC_DETAIL_THRESHOLD
        )
        index = getattr(self, "_metric_index_threshold_arg", _UNSET)
        if index is _UNSET:
            # "Never degrade" on the detail threshold has meant "show every
            # metric" since v0.40.0; dropping names under it would break that.
            index = None if detail is None else self.METRIC_INDEX_THRESHOLD
        return detail, index

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
        domain_lines = self._render_domains(contract, semantic_source)
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

        # 3b. Consumer-authored extras (opt-in by name, in the order given)
        lines.extend(self._render_extras(semantic_source))

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
        semantic_source: SemanticSource | None,
    ) -> list[str]:
        domains = contract.schema.semantic.domains
        if not domains:
            return []

        # Membership is metric-first, so per-domain counts come from the source's
        # metrics — tallied in a single pass rather than re-scanning per domain.
        # Without a source we can't count; the index then advertises name +
        # summary only and the agent uses lookup_domain. Imported locally to keep
        # core decoupled from semantic at module scope.
        from agentic_data_contracts.semantic.base import domain_metric_counts

        metrics = semantic_source.get_metrics() if semantic_source is not None else []
        domain_counts = domain_metric_counts(metrics)
        lines = ["<available_domains>"]
        if not metrics:
            # No source, or a source with no metrics: today's index, whose hint
            # names the lookup tools. Pinned byte-for-byte by the golden tests.
            for domain in domains:
                attrs = f'name="{domain.name}" summary="{domain.summary}"'
                lines.append(f"  <domain {attrs} />")
            lines.append(
                '  <hint>Use lookup_domain("...") for business context,'
                ' then lookup_metric("...") for SQL definitions.</hint>'
            )
            lines.append("</available_domains>")
            return lines

        cataloged = {d.name for d in domains}
        members = {
            d.name: [m.name for m in metrics if d.name in m.domains] for d in domains
        }
        others = [m.name for m in metrics if not cataloged.intersection(m.domains)]
        entries = sum(len(names) for names in members.values()) + len(others)
        _, index_threshold = self._resolve_metric_thresholds()
        drop_names = index_threshold is not None and entries > index_threshold
        if drop_names:
            self._log_names_dropped(entries, index_threshold)

        for domain in domains:
            attrs = f'name="{domain.name}" summary="{domain.summary}"'
            if domain_counts[domain.name]:
                attrs += f' metric_count="{domain_counts[domain.name]}"'
            names = members[domain.name]
            if drop_names or not names:
                lines.append(f"  <domain {attrs} />")
                continue
            lines.append(f"  <domain {attrs}>")
            lines.extend(_metric_element(name, indent=4) for name in names)
            lines.append("  </domain>")
        if others and not drop_names:
            lines.append("  <other_metrics>")
            lines.extend(_metric_element(name, indent=4) for name in others)
            lines.append("  </other_metrics>")

        # The hints name no tool: a host may wire a subset, and a disabled tool
        # would leave a dangling reference. Routing lives in each tool's own
        # description. Both keep the "before computing" constraint.
        if drop_names:
            lines.append(
                f"  <hint>{len(metrics)} metrics have governed SQL definitions;"
                " their names are omitted here. Each domain's description holds"
                " rules that apply to its metrics. Look up the domain and the"
                " metric before computing it.</hint>"
            )
        else:
            lines.append(
                "  <hint>Each domain's description holds rules that apply to its"
                " metrics, and each listed metric has a governed SQL definition."
                " Look up the domain and the metric before computing it.</hint>"
            )
        lines.append("</available_domains>")
        return lines

    def _log_names_dropped(self, entries: int, threshold: int) -> None:
        logger.info(
            "XmlPromptRenderer: %d metric index entries exceed"
            " metric_index_threshold=%d — metric names omitted from the prompt."
            " Pass metric_index_threshold=None to list them.",
            entries,
            threshold,
        )

    def _render_metrics(
        self,
        contract: DataContract,
        semantic_source: SemanticSource | None,
    ) -> list[str]:
        """Render the no-domain metric index and its lookup-before-computing hint.

        Three levels, by size: names with descriptions; names only, above the
        detail threshold; a count line, above the index threshold. Every level
        keeps the "before computing" constraint.

        The same constraint is on the ``run_query`` / ``inspect_query`` tool
        descriptions (``_PROTOCOL_METRIC_ORDERING`` in ``tools.factory``). That
        is deliberate, not drift: this prompt is opt-in, while descriptions
        travel with the tools on every path. The hint names no tool, since a
        host may not wire ``lookup_metric``; keep the two consistent in intent,
        not in text.
        """
        if semantic_source is None:
            return []

        metrics = semantic_source.get_metrics()
        if not metrics:
            return []

        lines: list[str] = ["<available_metrics>"]
        detail_threshold, index_threshold = self._resolve_metric_thresholds()
        listed_hint = (
            "  <hint>Each listed metric has a governed SQL definition; look it up"
            " before computing the metric.</hint>"
        )

        if index_threshold is not None and len(metrics) > index_threshold:
            self._log_names_dropped(len(metrics), index_threshold)
            lines.append(
                f"  <count>{len(metrics)} metrics available; names omitted above"
                f" {index_threshold} index entries.</count>"
            )
            lines.append(
                f"  <hint>{len(metrics)} metrics have governed SQL definitions;"
                " their names are omitted here. Look a metric up before"
                " computing it.</hint>"
            )
        elif detail_threshold is not None and len(metrics) > detail_threshold:
            logger.info(
                "XmlPromptRenderer: %d metrics exceeds"
                " metric_detail_threshold=%d — descriptions omitted from the"
                " prompt; names are still listed. Pass"
                " metric_detail_threshold=None to render descriptions inline.",
                len(metrics),
                detail_threshold,
            )
            lines.extend(_metric_element(m.name, indent=2) for m in metrics)
            lines.append(listed_hint)
        else:
            for m in metrics:
                lines.append(f'  <metric name="{m.name}">{m.description}</metric>')
            lines.append(listed_hint)

        lines.append("</available_metrics>")
        return lines

    def _render_semantic_source_fallback(self, contract: DataContract) -> list[str]:
        src = contract.schema.semantic.source
        assert src is not None
        lines = [
            "<semantic_source>",
            f"  <type>{src.type}</type>",
        ]
        if src.path is not None:
            lines.append(f"  <path>{src.path}</path>")
            lines.append(
                "  <hint>Consult this source for metric definitions"
                " before computing metrics.</hint>"
            )
        else:
            # Frozen / inline-only contract: definitions travel with the contract.
            lines.append(
                "  <hint>Metric definitions are embedded in this contract.</hint>"
            )
        lines.append("</semantic_source>")
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

        # Same fallback as ``_render_metrics``: a pre-v0.40.0 subclass with its
        # own ``__init__`` and no ``super().__init__()`` has no instance
        # attribute here, only the class constant.
        threshold = getattr(
            self, "relationship_detail_threshold", self.RELATIONSHIP_DETAIL_THRESHOLD
        )
        if threshold is not None and len(rels) > threshold:
            logger.info(
                "XmlPromptRenderer: %d relationships exceeds"
                " relationship_detail_threshold=%d — per-relationship join keys"
                " omitted from the prompt; agents must call"
                " lookup_relationships to get them. Pass"
                " relationship_detail_threshold=None to render them inline.",
                len(rels),
                threshold,
            )
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
                f"  <hint>{len(rels)} relationships defined; per-relationship"
                f" join keys omitted above threshold {threshold}."
                ' Use lookup_relationships(table="schema.table")'
                " to get join details and required filters.</hint>"
            )
        else:
            for r in rels:
                desc = r.description.strip()
                attrs = [f'type="{r.type}"']
                if r.preferred:
                    attrs.append('preferred="true"')
                if desc:
                    attrs.append(f'description="{desc}"')
                parts = [
                    f"<from>{r.from_}</from>",
                    f"<to>{r.to}</to>",
                ]
                if r.required_filter:
                    filt = r.required_filter.strip()
                    parts.append(f"<required_filter>{filt}</required_filter>")
                inner = "".join(parts)
                lines.append(
                    f"  <relationship {' '.join(attrs)}>{inner}</relationship>"
                )

        lines.append("</table_relationships>")
        return lines

    def _render_extras(self, semantic_source: SemanticSource | None) -> list[str]:
        """Render consumer-authored sections the caller explicitly named.

        Naming a section the source does not carry warns rather than rendering
        nothing quietly — it is the same typo class as a top-level key the parser
        ignores, and gets the same treatment.

        A consumer renderer that raises is allowed to propagate. Naming a section
        you cannot render is a bug, and swallowing it would silently drop content
        — the exact failure this whole feature removes.
        """
        # Same fallback as the two thresholds, but with no class constant to fall
        # back to: a pre-v0.40.0 subclass with its own ``__init__`` and no
        # ``super().__init__()`` never opted any section in, so an empty mapping
        # is the correct reading of its intent.
        sections: dict[str, ExtraSectionRenderer | None] = getattr(
            self, "extra_sections", {}
        )
        if not sections or semantic_source is None:
            return []
        # Imported locally to keep core decoupled from semantic at module scope,
        # matching _render_domains.
        from agentic_data_contracts.semantic.base import ExtensibleSemanticSource

        if not isinstance(semantic_source, ExtensibleSemanticSource):
            return []
        extras = semantic_source.get_extras()
        lines: list[str] = []
        for name, render in sections.items():
            if name not in extras:
                logger.warning(
                    "XmlPromptRenderer: extra_sections names %r, which the"
                    " semantic source does not carry (available: %s) — nothing"
                    " rendered for it.",
                    name,
                    sorted(extras),
                )
                continue
            renderer = _default_extra_section if render is None else render
            lines.extend(renderer(name, extras[name]))
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
