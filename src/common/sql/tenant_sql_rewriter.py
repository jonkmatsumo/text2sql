"""Conservative SQL tenant predicate rewrite for non-RLS providers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

import sqlglot
from sqlglot import exp

from common.sql.dialect import normalize_sqlglot_dialect

SUPPORTED_SQL_REWRITE_PROVIDERS = {"sqlite", "duckdb"}
_SET_OPERATION_TYPES = (exp.Union, exp.Intersect, exp.Except)


class TenantSQLRewriteError(ValueError):
    """Raised when tenant rewrite cannot be applied safely."""


@dataclass(frozen=True)
class TenantSQLRewriteResult:
    """Result payload for deterministic tenant rewrite operations."""

    rewritten_sql: str
    params: list[int]
    tables_rewritten: list[str]
    tenant_predicates_added: int


def rewrite_tenant_scoped_sql(
    sql: str,
    *,
    provider: str,
    tenant_id: int,
    tenant_column: str = "tenant_id",
    global_table_allowlist: set[str] | None = None,
    table_columns: Mapping[str, Sequence[str]] | None = None,
) -> TenantSQLRewriteResult:
    """Rewrite SQL to enforce tenant scoping via injected predicates.

    v1 scope is intentionally narrow:
    - single SELECT statement only
    - no nested SELECTs/subqueries
    - one predicate per non-global table in FROM/JOIN set
    """
    normalized_provider = (provider or "").strip().lower()
    if normalized_provider not in SUPPORTED_SQL_REWRITE_PROVIDERS:
        raise TenantSQLRewriteError("Provider does not support tenant SQL rewrite.")

    tenant_column_name = (tenant_column or "").strip()
    if not tenant_column_name:
        raise TenantSQLRewriteError("Tenant column name is required.")

    dialect = normalize_sqlglot_dialect(normalized_provider)
    try:
        expressions = sqlglot.parse(sql, read=dialect)
    except Exception as exc:
        raise TenantSQLRewriteError("SQL parse failed for tenant rewrite.") from exc

    if not expressions or len(expressions) != 1 or expressions[0] is None:
        raise TenantSQLRewriteError("Tenant rewrite requires a single SELECT statement.")

    expression = expressions[0]
    _assert_rewrite_eligible(expression)
    assert isinstance(expression, exp.Select)

    allowlist = {entry.strip().lower() for entry in (global_table_allowlist or set()) if entry}
    normalized_columns = _normalize_table_columns(table_columns)

    rewritten_tables: list[str] = []
    predicates: list[exp.Expression] = []

    for table in expression.find_all(exp.Table):
        table_keys = _table_keys(table)
        if not table_keys:
            raise TenantSQLRewriteError("Tenant rewrite could not resolve table identity.")
        if any(key in allowlist for key in table_keys):
            continue

        columns = _lookup_columns_for_table(table_keys, normalized_columns)
        if columns is not None and tenant_column_name.lower() not in columns:
            raise TenantSQLRewriteError("Tenant column missing for table rewrite.")

        reference = (table.alias_or_name or table.name or "").strip()
        if not reference:
            raise TenantSQLRewriteError("Tenant rewrite could not resolve table alias.")

        rewritten_tables.append(table_keys[0])
        predicates.append(
            exp.EQ(
                this=exp.column(tenant_column_name, table=reference),
                expression=exp.Placeholder(),
            )
        )

    if not predicates:
        raise TenantSQLRewriteError("Tenant rewrite produced no predicates.")

    combined_predicate = predicates[0]
    for predicate in predicates[1:]:
        combined_predicate = exp.and_(combined_predicate, predicate)

    existing_where = expression.args.get("where")
    if existing_where is None:
        expression.set("where", exp.Where(this=combined_predicate))
    else:
        expression.set(
            "where",
            exp.Where(this=exp.and_(existing_where.this, combined_predicate)),
        )

    return TenantSQLRewriteResult(
        rewritten_sql=expression.sql(dialect=dialect),
        params=[tenant_id] * len(predicates),
        tables_rewritten=rewritten_tables,
        tenant_predicates_added=len(predicates),
    )


def _normalize_table_columns(
    table_columns: Mapping[str, Sequence[str]] | None,
) -> dict[str, set[str]]:
    if not table_columns:
        return {}
    normalized: dict[str, set[str]] = {}
    for table_name, columns in table_columns.items():
        key = (table_name or "").strip().lower()
        if not key:
            continue
        normalized[key] = {
            col.strip().lower() for col in columns if isinstance(col, str) and col.strip()
        }
    return normalized


def _lookup_columns_for_table(
    table_keys: Sequence[str], table_columns: Mapping[str, set[str]]
) -> set[str] | None:
    if not table_columns:
        return None
    for key in table_keys:
        if key in table_columns:
            return table_columns[key]
    return None


def _table_keys(table: exp.Table) -> list[str]:
    """Return normalized lookup keys for table metadata and allowlist checks."""
    table_name = (table.name or "").strip().lower()
    if not table_name:
        return []

    db = (table.db or "").strip().lower()
    catalog = (table.catalog or "").strip().lower()
    keys = [table_name]
    if db:
        keys.insert(0, f"{db}.{table_name}")
    if catalog:
        if db:
            keys.insert(0, f"{catalog}.{db}.{table_name}")
        else:
            keys.insert(0, f"{catalog}.{table_name}")
    return keys


def _assert_rewrite_eligible(expression: exp.Expression) -> None:
    """Reject SQL shapes that cannot be scoped deterministically by the v1 rewriter."""
    if _contains_set_operation(expression):
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support set operations.")

    if not isinstance(expression, exp.Select):
        raise TenantSQLRewriteError("Tenant rewrite supports SELECT statements only.")

    if expression.args.get("with_") is not None:
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support CTEs.")

    if any(True for _ in expression.find_all(exp.Window)):
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support window functions.")

    if _has_nested_from_subquery(expression):
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support nested SELECTs in FROM.")

    if _has_correlated_subquery(expression):
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support correlated subqueries.")

    if _has_nested_select(expression):
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support subqueries.")


def _contains_set_operation(expression: exp.Expression) -> bool:
    if isinstance(expression, _SET_OPERATION_TYPES):
        return True
    return any(True for _ in expression.find_all(exp.SetOperation))


def _has_nested_select(expression: exp.Select) -> bool:
    for select in expression.find_all(exp.Select):
        if select is not expression:
            return True
    return False


def _has_nested_from_subquery(expression: exp.Select) -> bool:
    for subquery in expression.find_all(exp.Subquery):
        if isinstance(subquery.parent, (exp.From, exp.Join)):
            return True
    return False


def _has_correlated_subquery(expression: exp.Select) -> bool:
    outer_aliases = _top_level_table_aliases(expression)
    if not outer_aliases:
        return False

    for select in expression.find_all(exp.Select):
        if select is expression:
            continue
        for column in select.find_all(exp.Column):
            table_name = (column.table or "").strip().lower()
            if table_name and table_name in outer_aliases:
                return True
    return False


def _top_level_table_aliases(expression: exp.Select) -> set[str]:
    aliases: set[str] = set()
    for table in _top_level_tables(expression):
        alias_or_name = (table.alias_or_name or table.name or "").strip().lower()
        if alias_or_name:
            aliases.add(alias_or_name)
    return aliases


def _top_level_tables(expression: exp.Select) -> list[exp.Table]:
    tables: list[exp.Table] = []

    from_clause = expression.args.get("from_")
    if isinstance(from_clause, exp.From):
        from_this = from_clause.args.get("this")
        if isinstance(from_this, exp.Table):
            tables.append(from_this)

    joins = expression.args.get("joins") or []
    for join in joins:
        if not isinstance(join, exp.Join):
            continue
        join_this = join.args.get("this")
        if isinstance(join_this, exp.Table):
            tables.append(join_this)

    return tables
