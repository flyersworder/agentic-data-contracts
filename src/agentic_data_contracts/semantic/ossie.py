"""Apache Ossie (incubating) semantic model source.

Reads a model written against the Ossie core spec — the vendor-neutral
YAML/JSON format for metrics, dimensions and joins that began as Open
Semantic Interchange and is now in the Apache Incubator.

Scope, and why it is drawn here
-------------------------------
Ossie standardises what a metric *is*; this library enforces what an agent
may *do* with it. The spec is therefore a strict subset of our semantic
vocabulary: an Ossie ``Metric`` carries only ``name``, ``expression``,
``description``, ``datatype``, ``ai_context`` and ``custom_extensions``.
Ownership, review dates, tiers, decompositions, drill-by dimensions and the
metric-impact graph have no home in the spec (they sit on its roadmap as
discussions #40 and #53).

Every entity in ``ossie-schema.json`` (model, dataset, field, metric,
relationship) sets ``additionalProperties: false``, so those cannot be
smuggled in as extra keys. The spec's own escape hatch is
``custom_extensions``, a list of ``{vendor_name, data}`` where ``data`` is a
*JSON string*. We read our own model-level vendor block back into real
vocabulary and carry every other vendor's block, at any level, verbatim
through ``get_extras()`` — the same boundary ``YamlSource`` draws: the
framework carries extras and, on request, places them in the prompt, but
never interprets them.

Fields and columns
------------------
A dataset field is a name over an expression, and the table schema registered
for the dataset holds the *physical* columns those expressions read: a field
``customer_name`` whose expression is ``name`` registers the column ``name``,
and a computed field (``subtotal - discount``) registers none. Relationships
join on fields, so their endpoints are mapped to columns the same way.
Metric expressions are passed through unchanged; they name the model's
datasets and fields, and translating them to physical SQL is not done here.

Churn
-----
The spec is ``0.2.0.dev0`` with no tagged releases yet (apache/ossie#102),
and it has changed shape under this module: the ``semantic_model`` wrapper
was removed (#383), ``expression.sql`` was added (#617), and metrics may sit
on a dataset (#591). ``dialect`` is treated as an opaque string and never
validated, as the spec itself now accepts any non-empty string.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import sqlglot
import yaml
from sqlglot import exp
from sqlglot.errors import ParseError

from agentic_data_contracts.adapters.base import Column, TableSchema
from agentic_data_contracts.semantic.base import (
    Decomposition,
    DrillDimension,
    MetricDefinition,
    MetricImpact,
    Relationship,
    _apply_convention_default,
    _parse_convention_default,
    as_list,
    as_text,
    build_relationship_index,
    entry_list,
    entry_mapping,
    fuzzy_search_metrics,
    jsonify_extras,
    parse_review_date,
    require_text,
    validate_decompositions,
    validate_drill_by,
)

logger = logging.getLogger(__name__)

#: Vendor key this project claims in ``custom_extensions``. Ossie's ``Vendor``
#: is a free-form string, so a namespace is claimed by convention, not registry.
OSSIE_VENDOR = "AGENTIC_DATA_CONTRACTS"

#: The one dialect the spec defines, and the language of ``expression.sql``.
_OSSIE_SQL = "OSSIE_SQL_2026"

#: Dialect preference when the caller names none. ``ANSI_SQL`` comes first
#: because the expression is handed to an agent that runs it on a warehouse,
#: and ANSI is the declared dialect closest to that; Ossie SQL is the spec's
#: own portable language, which a consumer is expected to translate. Dialect
#: names are compared case-folded, so the proposal's ``Ossie_SQL_2026``
#: spelling and the spec's ``OSSIE_SQL_2026`` name one dialect.
_DEFAULT_DIALECT_PREFERENCE = ("ANSI_SQL", _OSSIE_SQL)

#: Dialects in Ossie's registry (extensions/dialects.md) that are formula or
#: query languages rather than SQL. The fallback never picks one: handed over
#: as SQL, a Tableau or DAX formula is a query the agent would run or "fix".
#: An unregistered dialect is still a fallback, since a vendor's own name
#: (``DENODO``, ``DUCKDB``) may well be SQL. Compared case-folded.
_NON_SQL_DIALECTS = frozenset(
    d.casefold()
    for d in (
        "TABLEAU",
        "DAX",
        "MDX",
        "MAQL",
        "SIGMA",
        "THOUGHTSPOT",
        "HOLISTICS_AQL",
        "SPARQL",
    )
)

#: An unquoted, optionally qualified column reference: ``amount`` or
#: ``source.amount``. Read without sqlglot, which parses some bare words
#: (``user``, ``current_date``) as functions rather than columns.
_BARE_COLUMN = re.compile(r"(?:[A-Za-z_][A-Za-z0-9_$]*\.)?([A-Za-z_][A-Za-z0-9_$]*)")

#: Cardinality by ``(from_columns are a key, to_columns are a key)``. Ossie
#: never states the join type, so it is read off which endpoints are unique.
_CARDINALITY: dict[tuple[bool, bool], str] = {
    (True, True): "one_to_one",
    (False, True): "many_to_one",
    (True, False): "one_to_many",
    (False, False): "many_to_many",
}


def _normalize_ai_context(value: Any) -> dict[str, Any] | None:
    """Coerce Ossie's ``ai_context`` union to its object form.

    The schema allows either a bare string or an object with ``instructions``,
    ``synonyms`` and ``examples``. Normalising at the edge means consumers
    reading ``get_extras()`` never have to branch on the shape.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return {"instructions": value}
    if isinstance(value, dict):
        return dict(value)
    return None


def _looks_like_query(source: str) -> bool:
    """Whether a ``dataset.source`` is a query rather than a table reference.

    Ossie allows either. Whitespace is the discriminator: a table reference is
    a dotted identifier path, and while a quoted identifier *may* contain a
    space, that is rare enough that treating it as a query only costs a
    debug-level log instead of a warning.
    """
    return any(c.isspace() for c in source.strip())


def _table_key(source: str) -> str | None:
    """Reduce an Ossie ``dataset.source`` to our two-part ``schema.table`` key.

    Ossie sources are ``database.schema.table`` or a query. Our keys are
    two-part because ``Relationship`` endpoints are ``schema.table.column``
    and ``build_relationship_index`` recovers the table with one ``rsplit``;
    carrying the database part would make every endpoint four deep and break
    that contract. The database qualifier is therefore dropped, which can
    collide if one model spans two databases with matching schema and table
    names — logged when it happens.

    A query-backed dataset has no physical table to key on and returns None.
    """
    candidate = source.strip()
    if not candidate or any(c.isspace() for c in candidate):
        return None
    parts = candidate.split(".")
    if len(parts) < 2:
        return None
    return ".".join(parts[-2:])


def _extension_blocks(entries: Any, *, where: str) -> dict[str, Any]:
    """Each vendor's ``custom_extensions`` payload, parsed, keyed by vendor.

    Ossie stores extension payloads as a JSON *string*, so a neighbouring
    vendor's malformed block is a real possibility. It is carried verbatim
    rather than raised on: another vendor's typo must not stop this library
    from enforcing a contract.

    Only string payloads are put through ``json.loads``. Authors do write the
    block as a YAML mapping despite the spec, and that value is already
    usable — parsing it would raise ``TypeError`` and produce a "not valid
    JSON" warning pointing at a non-problem.

    The schema does not forbid one vendor twice in a list. The later block
    wins, and the earlier one is named rather than dropped silently.
    """
    blocks: dict[str, Any] = {}
    for extension in entry_list(entries, where=f"{where} custom_extensions"):
        vendor = extension.get("vendor_name")
        if not vendor:
            continue
        payload: Any = extension.get("data", "")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except ValueError:
                logger.warning(
                    "OssieSource: custom_extensions payload for vendor %r is"
                    " not valid JSON; carried verbatim as a string.",
                    vendor,
                )
        if vendor in blocks:
            logger.warning(
                "OssieSource: %s declares vendor %r in custom_extensions more"
                " than once; the last block is kept and the earlier dropped.",
                where,
                vendor,
            )
        blocks[vendor] = payload
    return blocks


def _require_datasets(model: dict[str, Any], path: str | Path) -> None:
    """A model with no dataset is an empty source; the schema requires one."""
    if not entry_list(model.get("datasets"), where="datasets"):
        raise ValueError(
            f"Ossie model {path}: model {model.get('name')!r} has no datasets;"
            " the spec requires at least one."
        )


def _physical_column(expression: str) -> str | None:
    """The column a field's expression reads, or None for a computed field.

    A field's ``name`` is what the model calls it and its expression is what
    the table holds, so ``customer_name: name`` is the column ``name``. A
    computed field (``subtotal - discount``) is no column at all.
    """
    text = expression.strip()
    match = _BARE_COLUMN.fullmatch(text)
    if match:
        return match.group(1)
    try:
        node = sqlglot.parse_one(text)
    except ParseError:
        return None
    return node.name if isinstance(node, exp.Column) else None


def _models(raw: Any, path: str | Path) -> list[dict[str, Any]]:
    """The models an Ossie document holds, in either shape the spec has had.

    The current spec (apache/ossie#383) makes a document one model, with its
    ``name``, ``datasets`` and ``metrics`` at the root, and rejects the earlier
    ``semantic_model`` array. That array is still read, so files written before
    the change keep working, but it is named as legacy.

    Every way of finding no model is an error rather than an empty source. An
    empty source enforces nothing an agent can see, and nothing downstream
    tells it apart from a model that has no metrics: reading a current
    document through the old wrapper looked exactly like that.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"Ossie model {path}: expected a mapping with `datasets` at the"
            f" root, got {type(raw).__name__}."
        )
    flat, wrapped = "datasets" in raw, "semantic_model" in raw
    if flat and wrapped:
        raise ValueError(
            f"Ossie model {path} has both root `datasets` and a legacy"
            " `semantic_model` list; reading either would drop the other's"
            " models. Keep one model per document, at the root."
        )
    if flat:
        _require_datasets(raw, path)
        return [raw]
    if not wrapped:
        raise ValueError(
            f"Ossie model {path} has no `datasets` at the root. A current"
            " Ossie document is one model, with `name` and `datasets` at the"
            " root (the earlier `semantic_model` list is also read)."
        )
    models = entry_list(raw.get("semantic_model"), where="semantic_model")
    if not models:
        raise ValueError(
            f"Ossie model {path}: the `semantic_model` list holds no model."
        )
    for model in models:
        _require_datasets(model, path)
    logger.warning(
        "OssieSource: %s uses the `semantic_model` list, which the current"
        " Ossie spec no longer accepts (apache/ossie#383). It is still read;"
        " to migrate, write one model per document with its properties at the"
        " root.",
        path,
    )
    return models


class OssieSource:
    """Loads metric and table definitions from an Apache Ossie semantic model."""

    def __init__(
        self,
        path: str | Path,
        *,
        dialect: str | None = None,
        vendor: str = OSSIE_VENDOR,
    ) -> None:
        """Parse an Ossie model file.

        Args:
            path: Path to the Ossie YAML (or JSON — YAML is a superset).
            dialect: Preferred expression dialect, matched ignoring case. When
                a metric or field has no expression in it, selection falls
                back to ``ANSI_SQL``, then Ossie SQL (``expression.sql`` or an
                ``OSSIE_SQL_2026`` entry), then the first declared dialect.
            vendor: ``custom_extensions`` vendor name to read back as real
                vocabulary. Everything under any other vendor rides in extras.
        """
        raw = yaml.safe_load(Path(path).read_text()) or {}
        self._dialect_preference: tuple[str, ...] = tuple(
            d.casefold()
            for d in (
                (dialect, *_DEFAULT_DIALECT_PREFERENCE)
                if dialect
                else _DEFAULT_DIALECT_PREFERENCE
            )
        )
        self._vendor = vendor

        self._metrics: list[MetricDefinition] = []
        self._tables: dict[str, TableSchema] = {}
        self._relationships: list[Relationship] = []
        self._metric_impacts: list[MetricImpact] = []
        # All keyed by model name first. Ossie namespaces entity names per
        # model, and a legacy document holds a list of models, so two models
        # in one file may each declare a `customer` dataset or a block from
        # the same vendor. A flat key would silently drop the first.
        self._ai_context: dict[str, dict[str, dict[str, Any]]] = {}
        self._foreign_extensions: dict[str, dict[str, Any]] = {}
        self._entity_extensions: dict[str, dict[str, dict[str, Any]]] = {}

        for model in _models(raw, path):
            self._load_model(model)

        self._rel_index = build_relationship_index(self._relationships)
        validate_decompositions(self._metrics)
        validate_drill_by(self._metrics, self._tables)

    # -- parsing ----------------------------------------------------------

    def _load_model(self, model: dict[str, Any]) -> None:
        """Parse one model: the document itself, or a legacy list entry.

        Dataset names are resolved to table keys per model, not globally:
        Ossie scopes names inside a model, so two models in one file may each
        declare a ``customer`` dataset pointing at different tables.
        """
        model_name = str(model.get("name", ""))
        self._record_ai_context(model_name, "models", model_name, model)

        name_to_table: dict[str, str] = {}
        keys_by_dataset: dict[str, list[tuple[str, ...]]] = {}
        # Per dataset, each field's physical column, or None when computed.
        # Relationships join on fields, and their endpoints must be columns.
        columns_by_dataset: dict[str, dict[str, str | None]] = {}
        # Model-scoped metrics are named bare. A dataset-scoped one
        # (apache/ossie#591) is named `dataset.metric`, as the spec references
        # it, which also keeps it apart from a model-scoped one of that name.
        metrics: list[tuple[str, dict[str, Any]]] = []

        for dataset in entry_list(
            model.get("datasets"), where=f"{model_name} datasets"
        ):
            name = require_text(
                dataset.get("name"), where=f"Ossie model {model_name!r} dataset name"
            )
            self._record_ai_context(model_name, "datasets", name, dataset)
            self._record_entity_extensions(model_name, "datasets", name, dataset)
            keys_by_dataset[name] = self._unique_key_sets(dataset)
            columns, field_columns = self._read_fields(model_name, name, dataset)
            columns_by_dataset[name] = field_columns
            metrics.extend(
                self._named_metrics(
                    dataset.get("metrics"),
                    where=f"Ossie dataset {name!r} metrics",
                    prefix=f"{name}.",
                )
            )

            raw_source = str(dataset.get("source", ""))
            key = _table_key(raw_source)
            if key is None:
                # A query-defined dataset legitimately has no table to key on,
                # so that case stays at debug. A single-part or empty source is
                # an authoring error worth a warning: an undeclared table makes
                # `validate_drill_by` soft-skip, so the typo turns column
                # validation off rather than failing.
                log = logger.debug if _looks_like_query(raw_source) else logger.warning
                log(
                    "OssieSource: dataset %r has no resolvable schema.table in"
                    " its source %r, so no table schema is registered for it."
                    " Expected `database.schema.table` or `schema.table`.",
                    name,
                    raw_source,
                )
                continue
            if key in self._tables:
                logger.warning(
                    "OssieSource: dataset %r maps to table key %r, which is"
                    " already declared; the database qualifier is dropped from"
                    " %r, so two databases sharing a schema and table name"
                    " collide. The later dataset wins.",
                    name,
                    key,
                    dataset.get("source"),
                )
            name_to_table[name] = key
            self._tables[key] = TableSchema(
                columns=columns,
                # Beside the `fields` already read. An Ossie dataset's own
                # description is the table-level gloss #89 had nowhere to put.
                description=as_text(dataset.get("description")),
            )

        self._load_relationships(
            model, model_name, name_to_table, keys_by_dataset, columns_by_dataset
        )
        overlay = self._vendor_overlay(model, model_name)
        metrics.extend(
            self._named_metrics(
                model.get("metrics"), where=f"Ossie model {model_name!r} metrics"
            )
        )
        overlay_metrics = entry_mapping(
            overlay.get("metrics"), where="vendor block metrics"
        )
        self._warn_unmatched_overlay(overlay_metrics, [n for n, _ in metrics])
        # Scoped to the metrics this model contributes: ``self._metrics``
        # accumulates across a legacy document's models, so applying the
        # default to the whole list would stamp one model's house convention
        # onto another's. Ossie namespaces entities per model; so does this.
        first_metric = len(self._metrics)
        self._load_metrics(metrics, model_name, overlay_metrics)
        _apply_convention_default(
            self._metrics[first_metric:],
            _parse_convention_default(overlay.get("decomposition_convention")),
        )
        self._load_metric_impacts(
            entry_list(
                overlay.get("metric_impacts"), where="vendor block metric_impacts"
            )
        )

    def _read_fields(
        self, model_name: str, dataset_name: str, dataset: dict[str, Any]
    ) -> tuple[list[Column], dict[str, str | None]]:
        """A dataset's physical columns, and each field's column or None.

        A field names what the model calls a value and its expression names
        what the table holds, so the column is read off the expression. A
        computed field is no column; it is logged at debug, since the agent
        sees the table as the warehouse has it. A field written without an
        expression, which the spec does not allow, keeps its name as the
        column, as it did before fields were read this way.
        """
        columns: dict[str, Column] = {}
        field_columns: dict[str, str | None] = {}
        for f in entry_list(
            dataset.get("fields"), where=f"Ossie dataset {dataset_name!r} fields"
        ):
            field_name = require_text(
                f.get("name"), where=f"Ossie dataset {dataset_name!r} field name"
            )
            qualified = f"{dataset_name}.{field_name}"
            self._record_ai_context(model_name, "fields", qualified, f)
            self._record_entity_extensions(model_name, "fields", qualified, f)
            expression = self._choose_expression(f)
            # A BI formula is no evidence of a column, and its field's name is
            # what the BI tool calls it; it registers none, like a computed one.
            column = (
                _physical_column(expression)
                if expression
                else None
                if self._bi_formulas(f)
                else field_name
            )
            field_columns[field_name] = column
            if column is None:
                logger.debug(
                    "OssieSource: field %r is computed (%r), not a column of"
                    " its table; it is not registered as one.",
                    qualified,
                    expression,
                )
                continue
            columns.setdefault(
                column,
                Column(
                    name=column,
                    type=as_text(f.get("datatype")),
                    description=as_text(f.get("description")),
                ),
            )
        return list(columns.values()), field_columns

    @staticmethod
    def _named_metrics(
        entries: Any, *, where: str, prefix: str = ""
    ) -> list[tuple[str, dict[str, Any]]]:
        return [
            (prefix + require_text(m.get("name"), where=f"{where} name"), m)
            for m in entry_list(entries, where=where)
        ]

    @staticmethod
    def _warn_unmatched_overlay(
        overlay_metrics: dict[str, Any], names: list[str]
    ) -> None:
        """Name a vendor-block key that reaches no metric.

        Its owners, tier and decompositions would otherwise vanish. Moving a
        metric onto its dataset renames it to ``dataset.metric``, which is the
        likeliest way for a key to stop matching, so that form is suggested.
        """
        known = set(names)
        for key in overlay_metrics:
            if key in known:
                continue
            scoped = [n for n in names if n.endswith(f".{key}")]
            hint = f" Did you mean {', '.join(map(repr, scoped))}?" if scoped else ""
            logger.warning(
                "OssieSource: the %s block names metric %r, which this model"
                " does not declare; its settings apply to nothing.%s",
                OSSIE_VENDOR,
                key,
                hint,
            )

    @staticmethod
    def _unique_key_sets(dataset: dict[str, Any]) -> list[tuple[str, ...]]:
        """Every column set that uniquely identifies a row in *dataset*."""
        keys: list[tuple[str, ...]] = []
        primary = dataset.get("primary_key") or []
        if primary:
            keys.append(tuple(primary))
        for unique in dataset.get("unique_keys") or []:
            if unique:
                keys.append(tuple(unique))
        return keys

    def _load_relationships(
        self,
        model: dict[str, Any],
        model_name: str,
        name_to_table: dict[str, str],
        keys_by_dataset: dict[str, list[tuple[str, ...]]],
        columns_by_dataset: dict[str, dict[str, str | None]],
    ) -> None:
        """Translate Ossie relationships into single-column edges.

        Ossie relationships are dataset-to-dataset with parallel
        ``from_columns``/``to_columns`` lists, and cardinality is never
        written down — it is implied by which endpoints are keys, so it is
        derived from *both* sides here.

        The spec documents ``to`` as the one side, but nothing validates that,
        and trusting it is not free: ``RelationshipChecker._check_fan_out``
        fires only on ``one_to_many``, so reading a backwards-declared join as
        ``one_to_one`` silently disables the row-multiplication warning on an
        aggregate. When ``to_columns`` are demonstrably not a key of the
        ``to`` dataset, the join fans out and is reported as such.

        Keys are optional in Ossie, though, and absence of a key is not
        evidence of fan-out. When the ``to`` dataset declares no keys at all
        there is nothing to contradict the spec's declaration, so it stands —
        otherwise every model that simply omits keys would flood the fan-out
        checker with false positives.

        Composite joins are skipped rather than split. Our ``Relationship``
        has single-column endpoints, so emitting one edge per column pair
        would assert two independent joins that are individually wrong — a
        silent correctness bug in any path the join planner walks. Skipping
        loudly is the honest failure, matching ``CubeSource``.
        """
        for rel in entry_list(
            model.get("relationships"), where=f"{model_name} relationships"
        ):
            rel_name = rel.get("name", "<unnamed>")
            from_cols = rel.get("from_columns") or []
            to_cols = rel.get("to_columns") or []
            if len(from_cols) != 1 or len(to_cols) != 1:
                logger.warning(
                    "OssieSource: relationship %r joins on %d column(s);"
                    " Relationship endpoints are single columns, so this edge"
                    " is skipped rather than split into independently-wrong"
                    " pairs. Declare it in a YamlSource overlay if the agent"
                    " needs it.",
                    rel_name,
                    max(len(from_cols), len(to_cols)),
                )
                continue

            from_table = name_to_table.get(rel.get("from", ""))
            to_table = name_to_table.get(rel.get("to", ""))
            if from_table is None or to_table is None:
                logger.warning(
                    "OssieSource: relationship %r references a dataset with no"
                    " physical table (query-backed or undeclared); skipped.",
                    rel_name,
                )
                continue

            # Relationships join on fields; an endpoint is a physical column.
            # A field not declared is taken as the column it names.
            from_fields = columns_by_dataset.get(rel.get("from", ""), {})
            to_fields = columns_by_dataset.get(rel.get("to", ""), {})
            from_column = from_fields.get(from_cols[0], from_cols[0])
            to_column = to_fields.get(to_cols[0], to_cols[0])
            if from_column is None or to_column is None:
                logger.warning(
                    "OssieSource: relationship %r joins on a computed field,"
                    " which is no column to join on; skipped.",
                    rel_name,
                )
                continue

            self._record_ai_context(model_name, "relationships", rel_name, rel)
            self._record_entity_extensions(model_name, "relationships", rel_name, rel)
            ai = _normalize_ai_context(rel.get("ai_context")) or {}
            from_keys = keys_by_dataset.get(rel.get("from", ""), [])
            to_keys = keys_by_dataset.get(rel.get("to", ""), [])
            from_unique = tuple(from_cols) in {tuple(k) for k in from_keys}
            # No keys declared on the target: nothing contradicts the spec's
            # "``to`` is the one side", so take it at its word.
            to_unique = not to_keys or tuple(to_cols) in {tuple(k) for k in to_keys}

            self._relationships.append(
                Relationship(
                    from_=f"{from_table}.{from_column}",
                    to=f"{to_table}.{to_column}",
                    type=_CARDINALITY[(from_unique, to_unique)],
                    description=str(ai.get("instructions", "")),
                )
            )

    def _load_metrics(
        self,
        metrics: list[tuple[str, dict[str, Any]]],
        model_name: str,
        overlay: dict[str, Any],
    ) -> None:
        """Build each metric, refusing one with no expression.

        ``source_model`` comes only from our vendor block. Without a
        ``from_sql`` it says the expression reads that table alone, and an
        Ossie expression names the model's datasets and fields, which may be
        renamed or computed, so nothing in the spec supports the claim.
        """
        for name, metric in metrics:
            self._record_ai_context(model_name, "metrics", name, metric)
            self._record_entity_extensions(model_name, "metrics", name, metric)
            expression = self._choose_expression(metric)
            # Written only in BI formula languages: valid Ossie with no SQL to
            # run. Kept, with the formulas as the reason, as dbt and Cube
            # report a metric they cannot express (#123).
            formulas = [] if expression else self._bi_formulas(metric)
            untranslated = (
                "written only in non-SQL dialects, so there is no SQL"
                f" expression to run; its definition: {'; '.join(formulas)}"
                if formulas
                else None
            )
            if not expression and not formulas:
                raise ValueError(
                    f"Ossie metric {name!r} has no expression: expected"
                    " `expression.sql` or a non-empty `expression.dialects`."
                )
            if self.get_metric(name) is not None:
                logger.warning(
                    "OssieSource: duplicate metric name %r; lookups return the"
                    " first one declared.",
                    name,
                )
            extra = overlay.get(name, {})
            self._metrics.append(
                MetricDefinition(
                    name=name,
                    description=as_text(metric.get("description")),
                    sql_expression=expression,
                    untranslated=untranslated,
                    source_model=as_text(extra.get("source_model")),
                    filters=as_list(extra.get("filters")),
                    domains=as_list(extra.get("domains")),
                    tier=as_list(extra.get("tier")),
                    indicator_kind=extra.get("indicator_kind"),
                    business_owner=extra.get("business_owner"),
                    operational_owner=extra.get("operational_owner"),
                    last_reviewed=parse_review_date(extra.get("last_reviewed")),
                    decompositions=[
                        Decomposition(
                            operator=require_text(
                                d.get("operator"),
                                where="Ossie decompositions[] operator",
                            ),
                            operands=as_list(d.get("operands")),
                            convention=d.get("convention"),
                            convention_operand=d.get("convention_operand"),
                        )
                        for d in entry_list(
                            extra.get("decompositions"),
                            where=f"{name} decompositions",
                        )
                    ],
                    drill_by=[
                        DrillDimension(
                            dimension=require_text(
                                dd.get("dimension"), where="Ossie drill_by[] dimension"
                            ),
                            column=require_text(
                                dd.get("column"), where="Ossie drill_by[] column"
                            ),
                        )
                        for dd in entry_list(
                            extra.get("drill_by"), where=f"{name} drill_by"
                        )
                    ],
                )
            )

    def _load_metric_impacts(self, impacts: list[dict[str, Any]]) -> None:
        for impact in impacts:
            self._metric_impacts.append(
                MetricImpact(
                    from_metric=require_text(
                        impact.get("from"), where="Ossie metric_impacts[] from"
                    ),
                    to_metric=require_text(
                        impact.get("to"), where="Ossie metric_impacts[] to"
                    ),
                    direction=as_text(impact.get("direction"), "positive"),
                    confidence=as_text(impact.get("confidence"), "hypothesized"),
                    evidence=as_text(impact.get("evidence")),
                    description=as_text(impact.get("description")),
                    last_reviewed=parse_review_date(impact.get("last_reviewed")),
                )
            )

    def _choose_expression(self, entity: dict[str, Any]) -> str:
        """Choose one dialect's expression, deterministically; "" for none.

        The spec requires implementations to resolve multi-dialect
        expressions deterministically. Preference order is the caller's
        dialect, then the portable defaults, then the first declared entry —
        the last step chosen so a model written only in a vendor dialect
        still yields an expression. Dialect names are compared case-folded.
        An entity with no expression yields "", which a metric refuses.
        """
        expression = entity.get("expression")
        # `expression` authored as a bare list — a plausible slip, since its
        # payload is a list — has no dialects or `sql` to read, so it yields "".
        if not isinstance(expression, dict):
            return ""
        raw_dialects = expression.get("dialects")
        dialects = [d for d in (raw_dialects or []) if isinstance(d, dict)]
        by_dialect = {
            as_text(d.get("dialect")).casefold(): as_text(d.get("expression"))
            for d in dialects
            if d.get("dialect")
        }
        # `sql` is the spec's primary portable expression, written in Ossie SQL
        # and authoritative over a `dialects` entry for it (apache/ossie#617).
        portable = as_text(expression.get("sql"))
        if portable:
            by_dialect[_OSSIE_SQL.casefold()] = portable
        for preferred in self._dialect_preference:
            if preferred in by_dialect:
                return by_dialect[preferred]
        # The last resort is any declared entry *but* a BI formula: handed to
        # the agent as SQL, `SUM([Profit])` reads as a query to run or repair.
        for d in dialects:
            if as_text(d.get("dialect")).casefold() not in _NON_SQL_DIALECTS:
                return as_text(d.get("expression"))
        return ""

    @staticmethod
    def _bi_formulas(entity: dict[str, Any]) -> list[str]:
        """``DIALECT: formula`` for each non-SQL entry an entity declares."""
        expression = entity.get("expression")
        if not isinstance(expression, dict):
            return []
        return [
            f"{as_text(d.get('dialect'))}: {as_text(d.get('expression'))}"
            for d in expression.get("dialects") or []
            if isinstance(d, dict)
            and as_text(d.get("dialect")).casefold() in _NON_SQL_DIALECTS
        ]

    # -- extensions -------------------------------------------------------

    def _vendor_overlay(self, model: dict[str, Any], model_name: str) -> dict[str, Any]:
        """Split model-level ``custom_extensions`` into ours and foreign extras.

        Parsing, and the handling of malformed or repeated blocks, is
        :func:`_extension_blocks`'s.
        """
        blocks = _extension_blocks(
            model.get("custom_extensions"), where=f"model {model_name!r}"
        )
        payload = blocks.pop(self._vendor, {})
        if blocks:
            self._foreign_extensions[model_name] = blocks
        if isinstance(payload, dict):
            return payload
        logger.warning(
            "OssieSource: our own custom_extensions block (vendor %r) did not"
            " parse to an object; ignoring it.",
            self._vendor,
        )
        return {}

    def _record_entity_extensions(
        self, model_name: str, kind: str, name: str, entity: dict[str, Any]
    ) -> None:
        """Carry the vendor blocks a dataset, field, metric or relationship holds.

        The schema allows ``custom_extensions`` on each, and producers use
        them: NVIDIA Auto-Ontology puts a metric's physical SQL on the metric.
        Our own block is read only at the model level, where it is keyed by
        metric name, so one found here is named rather than half-applied.
        """
        blocks = _extension_blocks(
            entity.get("custom_extensions"), where=f"{kind} {name!r}"
        )
        if blocks.pop(self._vendor, None) is not None:
            logger.warning(
                "OssieSource: %s %r carries a %s block, which is read only at"
                " the model level; move it to the model-level block, keyed by"
                " metric name. It is ignored here.",
                kind,
                name,
                self._vendor,
            )
        if blocks:
            model = self._entity_extensions.setdefault(model_name, {})
            model.setdefault(kind, {})[name] = blocks

    def _record_ai_context(
        self, model_name: str, kind: str, name: str, entity: dict[str, Any]
    ) -> None:
        context = _normalize_ai_context(entity.get("ai_context"))
        if context is None or not name:
            return
        model = self._ai_context.setdefault(model_name, {})
        model.setdefault(kind, {})[name] = context

    # -- SemanticSource ---------------------------------------------------

    def get_metrics(self) -> list[MetricDefinition]:
        return list(self._metrics)

    def get_metric(self, name: str) -> MetricDefinition | None:
        for m in self._metrics:
            if m.name == name:
                return m
        return None

    def search_metrics(self, query: str) -> list[MetricDefinition]:
        return fuzzy_search_metrics(self._metrics, self.get_metric, query)

    def get_relationships(self) -> list[Relationship]:
        return list(self._relationships)

    def get_relationships_for_table(self, table: str) -> list[Relationship]:
        return list(self._rel_index.get(table, []))

    def get_table_schema(self, schema: str, table: str) -> TableSchema | None:
        return self._tables.get(f"{schema}.{table}")

    def get_table_schemas(self) -> dict[str, TableSchema]:
        return dict(self._tables)

    def get_metric_impacts(self) -> list[MetricImpact]:
        return list(self._metric_impacts)

    # -- ExtensibleSemanticSource -----------------------------------------

    def get_extras(self) -> dict[str, Any]:
        """Ossie content this library carries but does not interpret.

        Two sections, each keyed by semantic-model name first:

        ``ossie_ai_context``
            Every ``ai_context`` in the file, grouped by model then by entity
            kind. The spec's AI-grounding channel — synonyms, instructions,
            example questions — which has no counterpart in our vocabulary.
            Carried, not indexed: ``search_metrics`` still matches on name and
            description only, so Ossie synonyms never silently change
            retrieval.

        ``ossie_custom_extensions``
            Every model-level vendor block *except* ours, grouped by model
            then by vendor name, parsed from its JSON string where possible.

        ``ossie_entity_extensions``
            Every vendor block on a dataset, field, metric or relationship,
            grouped by model, then entity kind (``datasets``, ``fields``,
            ``metrics``, ``relationships``), then entity name, then vendor.
            Fields are named ``dataset.field`` and dataset-scoped metrics
            ``dataset.metric``.

        A section is omitted entirely when empty rather than emitted as ``{}``.
        Extras ride into the contract's inline snapshot and its canonical
        bytes, so an always-present empty dict would add a noise key to every
        Ossie contract's digest and would trip a contract that declares
        ``expected_extras`` on rehydrate.

        Reachable in a prompt only when named in
        ``XmlPromptRenderer(extra_sections=...)``, like any other extras.
        """
        extras: dict[str, Any] = {}
        if self._ai_context:
            extras["ossie_ai_context"] = self._ai_context
        if self._foreign_extensions:
            extras["ossie_custom_extensions"] = self._foreign_extensions
        if self._entity_extensions:
            extras["ossie_entity_extensions"] = self._entity_extensions
        # Vendor payloads and ai_context are author-controlled and reach
        # ``contract_canonical_bytes`` through ``json.dumps``, so they get the
        # same coercion YamlSource applies to its own extras. This also
        # deep-copies, keeping ``get_extras`` a non-aliasing read like
        # ``get_metrics`` and ``get_relationships``.
        return jsonify_extras(extras, source="OssieSource")
