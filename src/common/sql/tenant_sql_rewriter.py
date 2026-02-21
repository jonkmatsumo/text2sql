"""Conservative SQL tenant predicate rewrite for non-RLS providers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Sequence

import sqlglot
from sqlglot import exp

from common.sql.dialect import normalize_sqlglot_dialect

SUPPORTED_SQL_REWRITE_PROVIDERS = {"sqlite", "duckdb"}
_SET_OPERATION_TYPES = (exp.Union, exp.Intersect, exp.Except)


class TenantSQLRewriteError(ValueError):
    """Raised when tenant rewrite cannot be applied safely."""


class CTEClassification(Enum):
    """Classification of CTE query safety for tenant rewrite."""

    SAFE_SIMPLE_CTE = "SAFE_SIMPLE_CTE"
    UNSUPPORTED_CTE = "UNSUPPORTED_CTE"


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
    classification = _assert_rewrite_eligible(expression)
    assert isinstance(expression, exp.Select)

    allowlist = {entry.strip().lower() for entry in (global_table_allowlist or set()) if entry}
    normalized_columns = _normalize_table_columns(table_columns)

    total_rewritten_tables: list[str] = []
    total_predicates_count = 0

    if classification == CTEClassification.SAFE_SIMPLE_CTE:
        with_ = expression.args.get("with_")
        assert with_ is not None
        cte_names = {cte.alias_or_name.lower() for cte in with_.expressions if cte.alias_or_name}

        # 1. Injection in CTE bodies
        for cte in with_.expressions:
            assert isinstance(cte.this, exp.Select)
            rewritten, count = _inject_into_select(
                cte.this,
                tenant_id,
                tenant_column_name,
                allowlist,
                normalized_columns,
                cte_names,
            )
            total_rewritten_tables.extend(rewritten)
            total_predicates_count += count

        # 2. Injection in final SELECT
        rewritten, count = _inject_into_select(
            expression,
            tenant_id,
            tenant_column_name,
            allowlist,
            normalized_columns,
            cte_names,
        )
        total_rewritten_tables.extend(rewritten)
        total_predicates_count += count
    else:
        # Simple non-CTE path
        rewritten, count = _inject_into_select(
            expression,
            tenant_id,
            tenant_column_name,
            allowlist,
            normalized_columns,
            set(),
        )
        total_rewritten_tables.extend(rewritten)
        total_predicates_count += count

    if total_predicates_count == 0:
        # We don't check rewrite_targets here anymore as it's handled inside helper
        # If no predicates were added, but there were targets, it means they were allowlisted.
        # However, the original code had:
        # if not predicates: if not rewrite_targets: raise ... else return result
        # I'll keep that logic by checking if we have any targets at all.
        all_targets = _rewrite_target_tables(expression)
        if classification == CTEClassification.SAFE_SIMPLE_CTE:
            with_ = expression.args.get("with_")
            for cte in with_.expressions:
                all_targets.extend(_rewrite_target_tables(cte.this))

        if not all_targets:
            raise TenantSQLRewriteError("Tenant rewrite produced no predicates.")

        return TenantSQLRewriteResult(
            rewritten_sql=expression.sql(dialect=dialect),
            params=[],
            tables_rewritten=[],
            tenant_predicates_added=0,
        )

    return TenantSQLRewriteResult(
        rewritten_sql=expression.sql(dialect=dialect),
        params=[tenant_id] * total_predicates_count,
        tables_rewritten=total_rewritten_tables,
        tenant_predicates_added=total_predicates_count,
    )


def _inject_into_select(
    expression: exp.Select,
    tenant_id: int,
    tenant_column_name: str,
    allowlist: set[str],
    normalized_columns: dict[str, set[str]],
    cte_names: set[str],
) -> tuple[list[str], int]:
    """Inject tenant predicates into a single Select expression."""
    rewrite_targets = _rewrite_target_tables(expression)
    rewritten_tables: list[str] = []
    predicates: list[exp.Expression] = []

    for table in rewrite_targets:
        table_keys = _table_keys(table)
        if not table_keys:
            raise TenantSQLRewriteError("Tenant rewrite could not resolve table identity.")

        # Skip if it's a CTE reference
        if any(key in cte_names for key in table_keys):
            continue

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
        return [], 0

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

    return rewritten_tables, len(predicates)


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


def _assert_rewrite_eligible(expression: exp.Expression) -> CTEClassification | None:
    """Reject SQL shapes that cannot be scoped deterministically by the v1 rewriter."""
    if _contains_set_operation(expression):
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support set operations.")

    if not isinstance(expression, exp.Select):
        raise TenantSQLRewriteError("Tenant rewrite supports SELECT statements only.")

    if expression.args.get("with_") is not None:
        classification = classify_cte_query(expression)
        if classification == CTEClassification.UNSUPPORTED_CTE:
            raise TenantSQLRewriteError("Tenant rewrite v1 does not support unsupported CTEs.")
        return classification

    if any(True for _ in expression.find_all(exp.Window)):
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support window functions.")

    if _has_nested_from_subquery(expression):
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support nested SELECTs in FROM.")

    if _has_correlated_subquery(expression):
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support correlated subqueries.")

    if _has_nested_select(expression):
        raise TenantSQLRewriteError("Tenant rewrite v1 does not support subqueries.")

    return None


def classify_cte_query(expression: exp.Expression) -> CTEClassification:
    """Classify if a CTE query is safe for conservative tenant rewrite.

    Rules for SAFE_SIMPLE_CTE (tight and conservative):
    - Single WITH clause (already checked by caller usually, but enforced here).
    - No recursive WITH.
    - Each CTE body is a simple SELECT over base tables (no nested SELECT, no set ops, etc).
    - Final query selects FROM base tables or direct references to CTE names.
    """
    if not isinstance(expression, exp.Select):
        return CTEClassification.UNSUPPORTED_CTE

    with_ = expression.args.get("with_")
    if not with_:
        return CTEClassification.UNSUPPORTED_CTE

    if with_.recursive:
        return CTEClassification.UNSUPPORTED_CTE

    # Track CTE names to distinguish them from base tables
    cte_names = {cte.alias_or_name.lower() for cte in with_.expressions if cte.alias_or_name}

    # 1. Check CTE bodies
    for cte in with_.expressions:
        this = cte.this
        if not isinstance(this, exp.Select):
            return CTEClassification.UNSUPPORTED_CTE

        if _contains_set_operation(this):
            return CTEClassification.UNSUPPORTED_CTE

        if _has_nested_select(this):
            return CTEClassification.UNSUPPORTED_CTE

        # Ensure CTE body only selects from base tables (non-CTE references)
        # Note: In simple v1, we don't support CTEs referencing other CTEs yet?
        # The prompt says "Each CTE body is a simple SELECT over base tables".
        # This implies we should reject if a CTE references another CTE?
        # Let's be conservative as requested.
        for table in this.find_all(exp.Table):
            if table.name.lower() in cte_names:
                return CTEClassification.UNSUPPORTED_CTE

    # 2. Check the final SELECT (the expression itself without the WITH clause)
    # sqlglot expression for SELECT ... WITH ... will have the WITH in its args.
    # We need to check the SELECT body.
    if _contains_set_operation(expression):
        return CTEClassification.UNSUPPORTED_CTE

    # We want to allow SELECT from base tables or CTE names, but no other nesting.
    # _has_nested_select checks if find_all(exp.Select) has anything other than 'expression'.
    # However, 'expression' is the top-level Select which *contains* the CTE bodies in its
    # 'with' arg. We should check if there are any *other* Selects outside of the 'with'
    # definitions.

    # Check for subqueries in FROM/JOIN
    if _has_nested_from_subquery(expression):
        return CTEClassification.UNSUPPORTED_CTE

    # Check for subqueries in SELECT/WHERE etc.
    # We can't use _has_nested_select easily because it will find the CTE bodies.
    for node in expression.find_all(exp.Select):
        if node is expression:
            continue
        # If this select is NOT one of the CTE definition bodies, then it's an unsupported
        # nested select.
        is_cte_body = False
        for cte in with_.expressions:
            if node is cte.this:
                is_cte_body = True
                break
        if not is_cte_body:
            return CTEClassification.UNSUPPORTED_CTE

    # 3. Final query SELECT FROM base tables or CTE names only
    # _top_level_tables finds Table nodes in FROM and JOIN.
    for table in _top_level_tables(expression):
        # This is fine. It's either a base table or a CTE reference.
        pass

    return CTEClassification.SAFE_SIMPLE_CTE


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


def _rewrite_target_tables(expression: exp.Select) -> list[exp.Table]:
    """Return tenant-scopeable base tables for rewrite or fail closed."""
    tables: list[exp.Table] = []

    from_clause = expression.args.get("from_")
    if isinstance(from_clause, exp.From):
        from_this = from_clause.args.get("this")
        if isinstance(from_this, exp.Table):
            tables.append(from_this)
        elif from_this is not None:
            raise TenantSQLRewriteError(
                "Tenant rewrite v1 does not support non-table FROM sources."
            )

    joins = expression.args.get("joins") or []
    for join in joins:
        if not isinstance(join, exp.Join):
            continue
        join_this = join.args.get("this")
        if isinstance(join_this, exp.Table):
            tables.append(join_this)
        elif join_this is not None:
            raise TenantSQLRewriteError(
                "Tenant rewrite v1 does not support non-table JOIN sources."
            )

    return tables
