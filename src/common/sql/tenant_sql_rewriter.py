"""Conservative SQL tenant predicate rewrite for non-RLS providers."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Sequence

import sqlglot
from sqlglot import exp

from common.config.env import get_env_bool, get_env_int
from common.sql.dialect import normalize_sqlglot_dialect

SUPPORTED_SQL_REWRITE_PROVIDERS = {"sqlite", "duckdb"}
_SET_OPERATION_TYPES = (exp.Union, exp.Intersect, exp.Except)

MAX_TENANT_REWRITE_TARGETS = int(os.environ.get("MAX_TENANT_REWRITE_TARGETS", "25"))
MAX_TENANT_REWRITE_PARAMS = int(os.environ.get("MAX_TENANT_REWRITE_PARAMS", "50"))
MAX_SQL_AST_NODES = int(os.environ.get("MAX_SQL_AST_NODES", "1000"))


@dataclass(frozen=True)
class TenantRewriteSettings:
    """Runtime feature controls for tenant rewrite behavior."""

    enabled: bool
    strict_mode: bool
    max_targets: int
    max_params: int
    max_ast_nodes: int
    warn_ms: int
    hard_timeout_ms: int
    assert_invariants: bool

    @classmethod
    def from_env(cls) -> TenantRewriteSettings:
        """Load tenant rewrite settings from environment with fail-safe defaults."""
        warn_ms = _safe_env_non_negative_int("TENANT_REWRITE_WARN_MS", 50)
        hard_timeout_ms = _safe_env_non_negative_int("TENANT_REWRITE_HARD_TIMEOUT_MS", 200)
        return cls(
            enabled=_safe_env_bool("TENANT_REWRITE_ENABLED", True),
            strict_mode=_safe_env_bool("TENANT_REWRITE_STRICT_MODE", True),
            max_targets=_safe_env_int("TENANT_REWRITE_MAX_TARGETS", MAX_TENANT_REWRITE_TARGETS),
            max_params=_safe_env_int("TENANT_REWRITE_MAX_PARAMS", MAX_TENANT_REWRITE_PARAMS),
            max_ast_nodes=_safe_env_int("MAX_SQL_AST_NODES", MAX_SQL_AST_NODES),
            warn_ms=warn_ms,
            hard_timeout_ms=max(hard_timeout_ms, warn_ms),
            assert_invariants=_safe_env_bool("TENANT_REWRITE_ASSERT_INVARIANTS", False),
        )


class TenantSQLRewriteError(ValueError):
    """Raised when tenant rewrite cannot be applied safely."""

    def __init__(self, message: str, reason_code: str | None = None) -> None:
        """Initialize error with detailed message and internal reason code."""
        super().__init__(message)
        self.reason_code = reason_code or "UNKNOWN_ERROR"


class CTEClassification(Enum):
    """Classification of CTE query safety for tenant rewrite."""

    SAFE_SIMPLE_CTE = "SAFE_SIMPLE_CTE"
    UNSUPPORTED_CTE = "UNSUPPORTED_CTE"


class SubqueryClassification(Enum):
    """Classification of subquery safety for tenant rewrite."""

    SAFE_SIMPLE_SUBQUERY = "SAFE_SIMPLE_SUBQUERY"
    UNSUPPORTED_SUBQUERY = "UNSUPPORTED_SUBQUERY"


class TransformerErrorKind(str, Enum):
    """Transformer-local bounded failure kinds for pure rewrite operations."""

    AST_COMPLEXITY_EXCEEDED = "AST_COMPLEXITY_EXCEEDED"
    COMPLETENESS_FAILED = "COMPLETENESS_FAILED"
    CTE_UNSUPPORTED_SHAPE = "CTE_UNSUPPORTED_SHAPE"
    INVALID_REQUEST = "INVALID_REQUEST"
    MISSING_TENANT_COLUMN = "MISSING_TENANT_COLUMN"
    MISSING_TENANT_COLUMN_CONFIG = "MISSING_TENANT_COLUMN_CONFIG"
    NOT_SELECT_STATEMENT = "NOT_SELECT_STATEMENT"
    NOT_SINGLE_SELECT = "NOT_SINGLE_SELECT"
    NO_PREDICATES_PRODUCED = "NO_PREDICATES_PRODUCED"
    PARAM_LIMIT_EXCEEDED = "PARAM_LIMIT_EXCEEDED"
    PLACEHOLDER_PARAM_MISMATCH = "PLACEHOLDER_PARAM_MISMATCH"
    PARSE_ERROR = "PARSE_ERROR"
    PROVIDER_UNSUPPORTED = "PROVIDER_UNSUPPORTED"
    REWRITTEN_SQL_INVALID = "REWRITTEN_SQL_INVALID"
    SUBQUERY_UNSUPPORTED = "SUBQUERY_UNSUPPORTED"
    TARGET_LIMIT_EXCEEDED = "TARGET_LIMIT_EXCEEDED"
    UNRESOLVABLE_TABLE_ALIAS = "UNRESOLVABLE_TABLE_ALIAS"
    UNRESOLVABLE_TABLE_IDENTITY = "UNRESOLVABLE_TABLE_IDENTITY"


class TenantRewriteFailureReason(str, Enum):
    """Bounded transformer failure reason categories for policy and telemetry mapping."""

    UNSUPPORTED_SHAPE = "UNSUPPORTED_SHAPE"
    MISSING_TENANT_COLUMN = "MISSING_TENANT_COLUMN"
    TARGET_LIMIT_EXCEEDED = "TARGET_LIMIT_EXCEEDED"
    PARAM_LIMIT_EXCEEDED = "PARAM_LIMIT_EXCEEDED"
    AST_COMPLEXITY_EXCEEDED = "AST_COMPLEXITY_EXCEEDED"
    COMPLETENESS_FAILED = "COMPLETENESS_FAILED"
    DIALECT_UNSUPPORTED = "DIALECT_UNSUPPORTED"
    PARSE_FAILED = "PARSE_FAILED"
    NO_PREDICATES_PRODUCED = "NO_PREDICATES_PRODUCED"


class TenantSQLTransformerError(ValueError):
    """Raised when the pure SQL tenant transformer cannot safely transform input SQL."""

    def __init__(self, kind: TransformerErrorKind, message: str) -> None:
        """Capture a bounded transformer error kind with a safe diagnostic message."""
        super().__init__(message)
        self.kind = kind


@dataclass(frozen=True)
class RewriteTarget:
    """A specific table node eligible for tenant rewrite within a scope."""

    table: exp.Table
    scope_select: exp.Select
    cte_name: str | None = None
    appearance_index: int = 0
    scope_index: int = 0

    @property
    def effective_name(self) -> str:
        """Return the effective table name (alias or table name)."""
        return (self.table.alias_or_name or "").lower()

    @property
    def physical_name(self) -> str:
        """Return the physical base table name."""
        return (self.table.name or "").lower()


@dataclass(frozen=True)
class RewriteRequest:
    """Typed input contract for pure tenant SQL transformation."""

    sql: str
    provider: str
    tenant_id: int
    tenant_column: str = "tenant_id"
    global_table_allowlist: frozenset[str] = field(default_factory=frozenset)
    table_columns: Mapping[str, Sequence[str]] | None = None
    max_targets: int = MAX_TENANT_REWRITE_TARGETS
    max_params: int = MAX_TENANT_REWRITE_PARAMS
    max_ast_nodes: int = MAX_SQL_AST_NODES
    cte_classification: CTEClassification | None = None
    assert_invariants: bool = False
    skip_invariant_check: bool = False


@dataclass(frozen=True)
class RewriteSuccess:
    """Successful tenant SQL transformer output."""

    rewritten_sql: str
    params: list[int]
    tables_rewritten: list[str]
    tenant_predicates_added: int
    target_count: int = 0
    scope_depth: int = 1
    has_cte: bool = False
    has_subquery: bool = False


@dataclass(frozen=True)
class RewriteFailure:
    """Bounded transformer failure payload."""

    kind: TransformerErrorKind
    reason_code: TenantRewriteFailureReason
    message: str
    details: str | None = None

    def __post_init__(self) -> None:
        """Validate bounded failure fields at construction time."""
        if not isinstance(self.kind, TransformerErrorKind):
            raise TypeError("RewriteFailure.kind must be a TransformerErrorKind")
        if not isinstance(self.reason_code, TenantRewriteFailureReason):
            raise TypeError("RewriteFailure.reason_code must be a TenantRewriteFailureReason")
        if not isinstance(self.message, str) or not self.message.strip():
            raise ValueError("RewriteFailure.message must be a non-empty string")
        if self.details is not None:
            if not isinstance(self.details, str):
                raise TypeError("RewriteFailure.details must be a string when provided")
            sanitized = _sanitize_failure_details(self.details)
            object.__setattr__(self, "details", sanitized)


TenantSQLRewriteResult = RewriteSuccess


def _safe_env_bool(name: str, default: bool) -> bool:
    try:
        value = get_env_bool(name, default=default)
    except ValueError:
        return default
    return default if value is None else value


def _safe_env_int(name: str, default: int) -> int:
    try:
        value = get_env_int(name, default=default)
    except ValueError:
        return default
    if value is None or value < 1:
        return default
    return value


def _safe_env_non_negative_int(name: str, default: int) -> int:
    try:
        value = get_env_int(name, default=default)
    except ValueError:
        return default
    if value is None or value < 0:
        return default
    return value


def load_tenant_rewrite_settings() -> TenantRewriteSettings:
    """Load tenant rewrite settings from environment."""
    return TenantRewriteSettings.from_env()


def load_tenant_rewrite_config() -> TenantRewriteSettings:
    """Backward-compatible alias for legacy imports."""
    return load_tenant_rewrite_settings()


def _sanitize_failure_details(details: str | None) -> str | None:
    if not details:
        return None
    normalized = " ".join(part for part in details.splitlines() if part.strip()).strip()
    if not normalized:
        return None
    return normalized[:120]


def _failure_reason_for_kind(kind: TransformerErrorKind) -> TenantRewriteFailureReason:
    if kind == TransformerErrorKind.MISSING_TENANT_COLUMN:
        return TenantRewriteFailureReason.MISSING_TENANT_COLUMN
    if kind == TransformerErrorKind.MISSING_TENANT_COLUMN_CONFIG:
        return TenantRewriteFailureReason.MISSING_TENANT_COLUMN
    if kind == TransformerErrorKind.TARGET_LIMIT_EXCEEDED:
        return TenantRewriteFailureReason.TARGET_LIMIT_EXCEEDED
    if kind == TransformerErrorKind.PARAM_LIMIT_EXCEEDED:
        return TenantRewriteFailureReason.PARAM_LIMIT_EXCEEDED
    if kind == TransformerErrorKind.PLACEHOLDER_PARAM_MISMATCH:
        return TenantRewriteFailureReason.PARAM_LIMIT_EXCEEDED
    if kind == TransformerErrorKind.AST_COMPLEXITY_EXCEEDED:
        return TenantRewriteFailureReason.AST_COMPLEXITY_EXCEEDED
    if kind == TransformerErrorKind.COMPLETENESS_FAILED:
        return TenantRewriteFailureReason.COMPLETENESS_FAILED
    if kind == TransformerErrorKind.PROVIDER_UNSUPPORTED:
        return TenantRewriteFailureReason.DIALECT_UNSUPPORTED
    if kind == TransformerErrorKind.PARSE_ERROR:
        return TenantRewriteFailureReason.PARSE_FAILED
    if kind == TransformerErrorKind.REWRITTEN_SQL_INVALID:
        return TenantRewriteFailureReason.PARSE_FAILED
    if kind == TransformerErrorKind.NO_PREDICATES_PRODUCED:
        return TenantRewriteFailureReason.NO_PREDICATES_PRODUCED
    return TenantRewriteFailureReason.UNSUPPORTED_SHAPE


def _build_rewrite_failure(
    *,
    kind: TransformerErrorKind,
    message: str,
    details: str | None = None,
    reason_code: TenantRewriteFailureReason | None = None,
) -> RewriteFailure:
    return RewriteFailure(
        kind=kind,
        reason_code=reason_code or _failure_reason_for_kind(kind),
        message=message,
        details=details,
    )


def transform_tenant_scoped_sql(request: RewriteRequest) -> RewriteSuccess | RewriteFailure:
    """Pure tenant SQL transformer with typed request and bounded result/failure output."""
    validation_failure = _validate_transform_request(request)
    if validation_failure is not None:
        return validation_failure

    try:
        result = _transform_tenant_scoped_sql_impl(request)
    except TenantSQLTransformerError as exc:
        return _build_rewrite_failure(
            kind=exc.kind,
            message=str(exc),
            details=str(exc),
        )

    result_failure = _validate_transform_result(request, result)
    if result_failure is not None:
        return result_failure
    return result


def _validate_transform_request(request: object) -> RewriteFailure | None:
    if not isinstance(request, RewriteRequest):
        return _build_rewrite_failure(
            kind=TransformerErrorKind.INVALID_REQUEST,
            message="Tenant transformer requires a RewriteRequest input.",
        )
    if not isinstance(request.sql, str) or not request.sql.strip():
        return _build_rewrite_failure(
            kind=TransformerErrorKind.INVALID_REQUEST,
            message="RewriteRequest.sql must be a non-empty SQL string.",
        )
    if not isinstance(request.provider, str) or not request.provider.strip():
        return _build_rewrite_failure(
            kind=TransformerErrorKind.INVALID_REQUEST,
            message="RewriteRequest.provider must be a non-empty provider string.",
        )
    if not isinstance(request.tenant_id, int) or isinstance(request.tenant_id, bool):
        return _build_rewrite_failure(
            kind=TransformerErrorKind.INVALID_REQUEST,
            message="RewriteRequest.tenant_id must be an integer.",
        )
    if not isinstance(request.tenant_column, str) or not request.tenant_column.strip():
        return _build_rewrite_failure(
            kind=TransformerErrorKind.INVALID_REQUEST,
            message="RewriteRequest.tenant_column must be a non-empty string.",
        )
    if not isinstance(request.max_targets, int) or request.max_targets < 1:
        return _build_rewrite_failure(
            kind=TransformerErrorKind.INVALID_REQUEST,
            message="RewriteRequest.max_targets must be >= 1.",
        )
    if not isinstance(request.max_params, int) or request.max_params < 1:
        return _build_rewrite_failure(
            kind=TransformerErrorKind.INVALID_REQUEST,
            message="RewriteRequest.max_params must be >= 1.",
        )
    if not isinstance(request.max_ast_nodes, int) or request.max_ast_nodes < 1:
        return _build_rewrite_failure(
            kind=TransformerErrorKind.INVALID_REQUEST,
            message="RewriteRequest.max_ast_nodes must be >= 1.",
        )
    if request.cte_classification is not None and not isinstance(
        request.cte_classification, CTEClassification
    ):
        return _build_rewrite_failure(
            kind=TransformerErrorKind.INVALID_REQUEST,
            message="RewriteRequest.cte_classification must be a CTEClassification.",
        )
    return None


def _validate_transform_result(
    request: RewriteRequest, result: RewriteSuccess
) -> RewriteFailure | None:
    if not isinstance(result.rewritten_sql, str) or not result.rewritten_sql.strip():
        return _build_rewrite_failure(
            kind=TransformerErrorKind.REWRITTEN_SQL_INVALID,
            message="Transformer produced an empty rewritten SQL statement.",
        )

    dialect = normalize_sqlglot_dialect(request.provider.strip().lower())
    try:
        expressions = sqlglot.parse(result.rewritten_sql, read=dialect)
    except Exception:
        return _build_rewrite_failure(
            kind=TransformerErrorKind.REWRITTEN_SQL_INVALID,
            message="Transformer produced SQL that could not be parsed.",
        )
    if not expressions or len(expressions) != 1 or expressions[0] is None:
        return _build_rewrite_failure(
            kind=TransformerErrorKind.REWRITTEN_SQL_INVALID,
            message="Transformer produced an invalid rewritten SQL statement.",
        )

    try:
        original_expressions = sqlglot.parse(request.sql, read=dialect)
    except Exception:
        original_expressions = []

    original_placeholder_count = 0
    if original_expressions and original_expressions[0] is not None:
        original_placeholder_count = sum(
            1 for _ in original_expressions[0].find_all(exp.Placeholder)
        )
    rewritten_placeholder_count = sum(1 for _ in expressions[0].find_all(exp.Placeholder))
    added_placeholder_count = rewritten_placeholder_count - original_placeholder_count

    if added_placeholder_count != len(result.params):
        return _build_rewrite_failure(
            kind=TransformerErrorKind.PLACEHOLDER_PARAM_MISMATCH,
            message=(
                "Transformer produced mismatched placeholders and bound params "
                f"(added {added_placeholder_count} placeholders, {len(result.params)} params)."
            ),
        )
    if len(result.params) > request.max_params:
        return _build_rewrite_failure(
            kind=TransformerErrorKind.PARAM_LIMIT_EXCEEDED,
            message=(
                "Transformer produced params beyond max_params limit "
                f"({len(result.params)} > {request.max_params})."
            ),
        )
    return None


def _transform_tenant_scoped_sql_impl(request: RewriteRequest) -> RewriteSuccess:
    sql = request.sql
    provider = request.provider
    tenant_id = request.tenant_id
    tenant_column = request.tenant_column
    global_table_allowlist = request.global_table_allowlist
    table_columns = request.table_columns
    max_targets = request.max_targets
    max_params = request.max_params
    max_ast_nodes = request.max_ast_nodes
    cte_classification = request.cte_classification
    assert_invariants = request.assert_invariants
    skip_invariant_check = request.skip_invariant_check

    normalized_provider = (provider or "").strip().lower()
    if normalized_provider not in SUPPORTED_SQL_REWRITE_PROVIDERS:
        raise TenantSQLTransformerError(
            TransformerErrorKind.PROVIDER_UNSUPPORTED,
            "Provider does not support tenant SQL rewrite.",
        )

    tenant_column_name = (tenant_column or "").strip()
    if not tenant_column_name:
        raise TenantSQLTransformerError(
            TransformerErrorKind.MISSING_TENANT_COLUMN_CONFIG,
            "Tenant column name is required.",
        )

    dialect = normalize_sqlglot_dialect(normalized_provider)
    try:
        expressions = sqlglot.parse(sql, read=dialect)
    except Exception as exc:
        raise TenantSQLTransformerError(
            TransformerErrorKind.PARSE_ERROR,
            "SQL parse failed for tenant rewrite.",
        ) from exc

    if not expressions or len(expressions) != 1 or expressions[0] is None:
        raise TenantSQLTransformerError(
            TransformerErrorKind.NOT_SINGLE_SELECT,
            "Tenant rewrite requires a single SELECT statement.",
        )

    expression = expressions[0]
    if not isinstance(expression, exp.Select):
        raise TenantSQLTransformerError(
            TransformerErrorKind.NOT_SELECT_STATEMENT,
            "Tenant rewrite supports SELECT statements only.",
        )

    ast_node_count = _sql_ast_node_count(expression)
    if ast_node_count > max_ast_nodes:
        raise TenantSQLTransformerError(
            TransformerErrorKind.AST_COMPLEXITY_EXCEEDED,
            (
                "Tenant rewrite AST complexity exceeded the maximum allowed node count "
                f"({max_ast_nodes})."
            ),
        )

    classification = cte_classification
    if expression.args.get("with_") is not None and classification is None:
        classification = classify_cte_query(expression)
        if classification == CTEClassification.UNSUPPORTED_CTE:
            raise TenantSQLTransformerError(
                TransformerErrorKind.CTE_UNSUPPORTED_SHAPE,
                "Tenant rewrite v1 does not support unsupported CTEs.",
            )

    rewrite_has_cte = expression.args.get("with_") is not None
    rewrite_has_subquery = _has_subquery(expression)
    rewrite_scope_depth = _scope_depth(expression)

    allowlist = {entry.strip().lower() for entry in (global_table_allowlist or set()) if entry}
    normalized_columns = _normalize_table_columns(table_columns)

    targets = _collect_all_rewrite_targets(expression, classification)
    if len(targets) > max_targets:
        raise TenantSQLTransformerError(
            TransformerErrorKind.TARGET_LIMIT_EXCEEDED,
            f"Tenant rewrite exceeded maximum allowed targets ({max_targets}).",
        )

    sorted_targets = sorted(
        targets,
        key=lambda t: (
            t.cte_name or "",
            t.effective_name,
            t.physical_name,
            t.scope_index,
            t.appearance_index,
        ),
    )

    rewritten_tables: list[str] = []
    rewritten_target_keys: set[tuple] = set()
    total_predicates_count = 0

    with_ = expression.args.get("with_")
    cte_names = (
        {cte.alias_or_name.lower() for cte in with_.expressions if cte.alias_or_name}
        if with_
        else set()
    )

    for target in sorted_targets:
        target_key = (
            target.cte_name or "",
            target.effective_name,
            target.physical_name,
            target.scope_index,
            target.appearance_index,
        )
        if target_key in rewritten_target_keys:
            continue

        table_keys = _table_keys(target.table)
        if not table_keys:
            raise TenantSQLTransformerError(
                TransformerErrorKind.UNRESOLVABLE_TABLE_IDENTITY,
                "Tenant rewrite could not resolve table identity.",
            )

        if any(key in cte_names for key in table_keys):
            continue
        if any(key in allowlist for key in table_keys):
            continue

        columns = _lookup_columns_for_table(table_keys, normalized_columns)
        if columns is not None and tenant_column_name.lower() not in columns:
            raise TenantSQLTransformerError(
                TransformerErrorKind.MISSING_TENANT_COLUMN,
                "Tenant column missing for table rewrite.",
            )

        reference = (target.table.alias_or_name or target.table.name or "").strip()
        if not reference:
            raise TenantSQLTransformerError(
                TransformerErrorKind.UNRESOLVABLE_TABLE_ALIAS,
                "Tenant rewrite could not resolve table alias.",
            )

        if total_predicates_count >= max_params:
            raise TenantSQLTransformerError(
                TransformerErrorKind.PARAM_LIMIT_EXCEEDED,
                f"Tenant rewrite exceeded maximum allowed parameters ({max_params}).",
            )

        predicate = exp.EQ(
            this=exp.column(tenant_column_name, table=reference),
            expression=exp.Placeholder(),
        )

        existing_where = target.scope_select.args.get("where")
        if existing_where is None:
            target.scope_select.set("where", exp.Where(this=predicate))
        else:
            existing_condition = existing_where.this
            existing_condition.pop()
            existing_where.set("this", exp.and_(existing_condition, predicate, copy=False))

        rewritten_tables.append(table_keys[0])
        rewritten_target_keys.add(target_key)
        total_predicates_count += 1

    try:
        _assert_completeness(
            expression, classification, cte_names, allowlist, rewritten_target_keys
        )
    except TenantSQLRewriteError as exc:
        raise TenantSQLTransformerError(
            TransformerErrorKind.COMPLETENESS_FAILED,
            str(exc),
        ) from exc

    if total_predicates_count == 0:
        if not targets:
            raise TenantSQLTransformerError(
                TransformerErrorKind.NO_PREDICATES_PRODUCED,
                "Tenant rewrite produced no predicates.",
            )

        result = RewriteSuccess(
            rewritten_sql=expression.sql(dialect=dialect),
            params=[],
            tables_rewritten=[],
            tenant_predicates_added=0,
            target_count=len(targets),
            scope_depth=rewrite_scope_depth,
            has_cte=rewrite_has_cte,
            has_subquery=rewrite_has_subquery,
        )
        if assert_invariants and not skip_invariant_check:
            assert_rewrite_invariants(
                sql,
                result.rewritten_sql,
                result.params,
                provider=normalized_provider,
                tenant_column=tenant_column_name,
                global_table_allowlist=allowlist,
                table_columns=normalized_columns,
                strict_mode=True,
                tenant_id=tenant_id,
            )
        return result

    params = [tenant_id] * total_predicates_count
    assert len(params) == total_predicates_count

    result = RewriteSuccess(
        rewritten_sql=expression.sql(dialect=dialect),
        params=params,
        tables_rewritten=rewritten_tables,
        tenant_predicates_added=total_predicates_count,
        target_count=len(targets),
        scope_depth=rewrite_scope_depth,
        has_cte=rewrite_has_cte,
        has_subquery=rewrite_has_subquery,
    )
    if assert_invariants and not skip_invariant_check:
        assert_rewrite_invariants(
            sql,
            result.rewritten_sql,
            result.params,
            provider=normalized_provider,
            tenant_column=tenant_column_name,
            global_table_allowlist=allowlist,
            table_columns=normalized_columns,
            strict_mode=True,
            tenant_id=tenant_id,
        )
    return result


def rewrite_tenant_scoped_sql(
    sql: str,
    *,
    provider: str,
    tenant_id: int,
    tenant_column: str = "tenant_id",
    global_table_allowlist: set[str] | None = None,
    table_columns: Mapping[str, Sequence[str]] | None = None,
    _skip_invariant_check: bool = False,
) -> TenantSQLRewriteResult:
    """Rewrite SQL to enforce tenant scoping via injected predicates.

    v1 scope is intentionally narrow:
    - single SELECT statement only
    - no nested SELECTs/subqueries
    - one predicate per non-global table in FROM/JOIN set
    """
    settings = load_tenant_rewrite_settings()
    if not settings.enabled:
        raise TenantSQLRewriteError(
            "Tenant SQL rewrite is disabled by feature flag.", reason_code="REWRITE_DISABLED"
        )

    normalized_provider = (provider or "").strip().lower()
    if normalized_provider not in SUPPORTED_SQL_REWRITE_PROVIDERS:
        raise TenantSQLRewriteError(
            "Provider does not support tenant SQL rewrite.", reason_code="PROVIDER_UNSUPPORTED"
        )

    tenant_column_name = (tenant_column or "").strip()
    if not tenant_column_name:
        raise TenantSQLRewriteError(
            "Tenant column name is required.", reason_code="MISSING_TENANT_COLUMN_CONFIG"
        )

    dialect = normalize_sqlglot_dialect(normalized_provider)
    try:
        expressions = sqlglot.parse(sql, read=dialect)
    except Exception as exc:
        raise TenantSQLRewriteError(
            "SQL parse failed for tenant rewrite.", reason_code="PARSE_ERROR"
        ) from exc

    if not expressions or len(expressions) != 1 or expressions[0] is None:
        raise TenantSQLRewriteError(
            "Tenant rewrite requires a single SELECT statement.", reason_code="NOT_SINGLE_SELECT"
        )

    expression = expressions[0]
    ast_node_count = _sql_ast_node_count(expression)
    if ast_node_count > settings.max_ast_nodes:
        raise TenantSQLRewriteError(
            "Tenant rewrite AST complexity exceeded the maximum allowed node count "
            f"({settings.max_ast_nodes}).",
            reason_code="AST_COMPLEXITY_EXCEEDED",
        )
    classification = _assert_rewrite_eligible(expression, strict_mode=settings.strict_mode)
    assert isinstance(expression, exp.Select)
    rewrite_has_cte = expression.args.get("with_") is not None
    rewrite_has_subquery = _has_subquery(expression)
    rewrite_scope_depth = _scope_depth(expression)

    allowlist = {entry.strip().lower() for entry in (global_table_allowlist or set()) if entry}
    normalized_columns = _normalize_table_columns(table_columns)

    # 1. Collect all rewrite targets across all scopes
    targets = _collect_all_rewrite_targets(expression, classification)

    if len(targets) > settings.max_targets:
        raise TenantSQLRewriteError(
            f"Tenant rewrite exceeded maximum allowed targets ({settings.max_targets}).",
            reason_code="TARGET_LIMIT_EXCEEDED",
        )

    # 2. Sort targets for determinism
    # Sort key: (cte_name_or_empty, table_effective_name, table_physical_name,
    # scope_index, appearance_index)
    sorted_targets = sorted(
        targets,
        key=lambda t: (
            t.cte_name or "",
            t.effective_name,
            t.physical_name,
            t.scope_index,
            t.appearance_index,
        ),
    )

    # 3. Apply rewrites in sorted order
    rewritten_tables: list[str] = []
    rewritten_target_keys: set[tuple] = set()
    total_predicates_count = 0

    # CTE names to avoid rewriting CTE references
    with_ = expression.args.get("with_")
    cte_names = (
        {cte.alias_or_name.lower() for cte in with_.expressions if cte.alias_or_name}
        if with_
        else set()
    )

    for target in sorted_targets:
        target_key = (
            target.cte_name or "",
            target.effective_name,
            target.physical_name,
            target.scope_index,
            target.appearance_index,
        )
        if target_key in rewritten_target_keys:
            continue

        table_keys = _table_keys(target.table)
        if not table_keys:
            raise TenantSQLRewriteError(
                "Tenant rewrite could not resolve table identity.",
                reason_code="UNRESOLVABLE_TABLE_IDENTITY",
            )

        # Skip if it's a CTE reference
        if any(key in cte_names for key in table_keys):
            continue

        if any(key in allowlist for key in table_keys):
            continue

        columns = _lookup_columns_for_table(table_keys, normalized_columns)
        if columns is not None and tenant_column_name.lower() not in columns:
            raise TenantSQLRewriteError(
                "Tenant column missing for table rewrite.", reason_code="MISSING_TENANT_COLUMN"
            )

        reference = (target.table.alias_or_name or target.table.name or "").strip()
        if not reference:
            raise TenantSQLRewriteError(
                "Tenant rewrite could not resolve table alias.",
                reason_code="UNRESOLVABLE_TABLE_ALIAS",
            )

        if total_predicates_count >= settings.max_params:
            raise TenantSQLRewriteError(
                "Tenant rewrite exceeded maximum allowed parameters " f"({settings.max_params}).",
                reason_code="PARAM_LIMIT_EXCEEDED",
            )

        # Inject predicate into the target's scope_select
        predicate = exp.EQ(
            this=exp.column(tenant_column_name, table=reference),
            expression=exp.Placeholder(),
        )

        existing_where = target.scope_select.args.get("where")
        if existing_where is None:
            target.scope_select.set("where", exp.Where(this=predicate))
        else:
            existing_condition = existing_where.this
            existing_condition.pop()
            existing_where.set("this", exp.and_(existing_condition, predicate, copy=False))

        rewritten_tables.append(table_keys[0])
        rewritten_target_keys.add(target_key)
        total_predicates_count += 1

    # 4. Post-condition check: Ensure every eligible table reference has a predicate
    _assert_completeness(expression, classification, cte_names, allowlist, rewritten_target_keys)

    if total_predicates_count == 0:
        if not targets:
            raise TenantSQLRewriteError(
                "Tenant rewrite produced no predicates.", reason_code="NO_PREDICATES_PRODUCED"
            )

        result = TenantSQLRewriteResult(
            rewritten_sql=expression.sql(dialect=dialect),
            params=[],
            tables_rewritten=[],
            tenant_predicates_added=0,
            target_count=len(targets),
            scope_depth=rewrite_scope_depth,
            has_cte=rewrite_has_cte,
            has_subquery=rewrite_has_subquery,
        )
        if _should_run_invariant_checks(settings) and not _skip_invariant_check:
            assert_rewrite_invariants(
                sql,
                result.rewritten_sql,
                result.params,
                provider=normalized_provider,
                tenant_column=tenant_column_name,
                global_table_allowlist=allowlist,
                table_columns=normalized_columns,
                strict_mode=settings.strict_mode,
                tenant_id=tenant_id,
            )
        return result

    params = [tenant_id] * total_predicates_count
    assert (
        len(params) == total_predicates_count
    ), "One param per injected predicate invariant violated."

    result = TenantSQLRewriteResult(
        rewritten_sql=expression.sql(dialect=dialect),
        params=params,
        tables_rewritten=rewritten_tables,
        tenant_predicates_added=total_predicates_count,
        target_count=len(targets),
        scope_depth=rewrite_scope_depth,
        has_cte=rewrite_has_cte,
        has_subquery=rewrite_has_subquery,
    )
    if _should_run_invariant_checks(settings) and not _skip_invariant_check:
        assert_rewrite_invariants(
            sql,
            result.rewritten_sql,
            result.params,
            provider=normalized_provider,
            tenant_column=tenant_column_name,
            global_table_allowlist=allowlist,
            table_columns=normalized_columns,
            strict_mode=settings.strict_mode,
            tenant_id=tenant_id,
        )
    return result


def _collect_all_rewrite_targets(
    expression: exp.Select, classification: CTEClassification | None
) -> list[RewriteTarget]:
    targets: list[RewriteTarget] = []

    # 1. Collect from CTEs
    scope_idx = 0
    if classification == CTEClassification.SAFE_SIMPLE_CTE:
        with_ = expression.args.get("with_")
        if with_:
            for cte in with_.expressions:
                if isinstance(cte.this, exp.Select):
                    targets.extend(
                        _get_targets_in_select(
                            cte.this,
                            cte_name=cte.alias_or_name.lower() if cte.alias_or_name else None,
                            scope_index=scope_idx,
                        )
                    )
                    scope_idx += 1

    # 2. Collect from final SELECT and all its nested subqueries
    with_ = expression.args.get("with_")
    for select in expression.find_all(exp.Select):
        is_cte_body = False
        if with_:
            for cte in with_.expressions:
                if select is cte.this:
                    is_cte_body = True
                    break
        if is_cte_body:
            continue
        targets.extend(_get_targets_in_select(select, scope_index=scope_idx))
        scope_idx += 1

    return targets


def _get_targets_in_select(
    expression: exp.Select, cte_name: str | None = None, scope_index: int = 0
) -> list[RewriteTarget]:
    targets: list[RewriteTarget] = []
    appearance_index = 0

    from_clause = expression.args.get("from_")
    if isinstance(from_clause, exp.From):
        from_this = from_clause.args.get("this")
        if isinstance(from_this, exp.Table):
            targets.append(
                RewriteTarget(
                    table=from_this,
                    scope_select=expression,
                    cte_name=cte_name,
                    appearance_index=appearance_index,
                    scope_index=scope_index,
                )
            )
            appearance_index += 1
        elif from_this is not None:
            # Already checked by eligibility gate usually, but be safe
            pass

    joins = expression.args.get("joins") or []
    for join in joins:
        if not isinstance(join, exp.Join):
            continue
        join_this = join.args.get("this")
        if isinstance(join_this, exp.Table):
            targets.append(
                RewriteTarget(
                    table=join_this,
                    scope_select=expression,
                    cte_name=cte_name,
                    appearance_index=appearance_index,
                    scope_index=scope_index,
                )
            )
            appearance_index += 1

    return targets


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


def _sql_ast_node_count(expression: exp.Expression) -> int:
    """Count AST nodes to bound rewrite traversal complexity."""
    return sum(1 for _ in expression.walk())


def _scope_depth(expression: exp.Select) -> int:
    """Return max nested SELECT scope depth for rewrite diagnostics."""
    max_depth = 1
    for select in expression.find_all(exp.Select):
        depth = 1
        parent = select.parent
        while parent is not None:
            if isinstance(parent, exp.Select):
                depth += 1
            parent = parent.parent
        if depth > max_depth:
            max_depth = depth
    return max_depth


def _has_subquery(expression: exp.Select) -> bool:
    """Return True if query contains an explicit subquery expression."""
    return any(True for _ in expression.find_all(exp.Subquery))


def _should_run_invariant_checks(settings: TenantRewriteSettings | None = None) -> bool:
    """Run rewrite invariants only in debug/test modes."""
    rewrite_settings = settings or load_tenant_rewrite_settings()
    if rewrite_settings.assert_invariants:
        return True
    return bool(os.getenv("PYTEST_CURRENT_TEST"))


def _count_rewrite_predicates(expression: exp.Select, tenant_column: str) -> int:
    total = 0
    for select in expression.find_all(exp.Select):
        total += len(_collect_tenant_predicate_references(select, tenant_column))
    return total


def _tenant_predicate_reference(comparison: exp.Expression, tenant_column: str) -> str | None:
    if not isinstance(comparison, exp.EQ):
        return None

    tenant_column_normalized = tenant_column.strip().lower()
    if not tenant_column_normalized:
        return None

    left = comparison.this
    right = comparison.expression

    if isinstance(left, exp.Column) and isinstance(right, exp.Placeholder):
        column = left
    elif isinstance(right, exp.Column) and isinstance(left, exp.Placeholder):
        column = right
    else:
        return None

    if (column.name or "").strip().lower() != tenant_column_normalized:
        return None

    reference = (column.table or "").strip().lower()
    return reference or None


def _collect_tenant_predicate_references(select: exp.Select, tenant_column: str) -> list[str]:
    where = select.args.get("where")
    if where is None:
        return []
    references: list[str] = []
    for comparison in where.find_all(exp.EQ):
        if _nearest_select_ancestor(comparison) is not select:
            continue
        reference = _tenant_predicate_reference(comparison, tenant_column)
        if reference:
            references.append(reference)
    return references


def _scope_has_tenant_predicate(select: exp.Select, reference: str, tenant_column: str) -> bool:
    normalized_reference = (reference or "").strip().lower()
    if not normalized_reference:
        return False
    references = _collect_tenant_predicate_references(select, tenant_column)
    return normalized_reference in references


def _nearest_select_ancestor(node: exp.Expression) -> exp.Select | None:
    parent = node.parent
    while parent is not None:
        if isinstance(parent, exp.Select):
            return parent
        parent = parent.parent
    return None


def _assert_no_duplicate_tenant_predicates(expression: exp.Select, tenant_column: str) -> None:
    for select in expression.find_all(exp.Select):
        references = _collect_tenant_predicate_references(select, tenant_column)
        duplicates = {ref for ref in references if references.count(ref) > 1}
        if duplicates:
            duplicate = sorted(duplicates)[0]
            raise AssertionError(
                "Duplicate tenant predicate detected in the same scope for "
                f"reference '{duplicate}'."
            )


def _assert_predicates_within_intended_scope(expression: exp.Select, tenant_column: str) -> None:
    for select in expression.find_all(exp.Select):
        defined_names = _get_defined_names(select)
        for reference in _collect_tenant_predicate_references(select, tenant_column):
            if reference not in defined_names:
                raise AssertionError(
                    "Tenant predicate injected outside intended scope for "
                    f"reference '{reference}'."
                )


def _assert_all_eligible_targets_rewritten(
    expression: exp.Select,
    *,
    strict_mode: bool,
    tenant_column: str,
    global_table_allowlist: set[str],
) -> None:
    classification = _assert_rewrite_eligible(expression, strict_mode=strict_mode)
    with_ = expression.args.get("with_")
    cte_names = (
        {cte.alias_or_name.lower() for cte in with_.expressions if cte.alias_or_name}
        if with_
        else set()
    )
    targets = _collect_all_rewrite_targets(expression, classification)
    for target in targets:
        table_keys = _table_keys(target.table)
        if any(key in cte_names for key in table_keys):
            continue
        if any(key in global_table_allowlist for key in table_keys):
            continue

        reference = (target.table.alias_or_name or target.table.name or "").strip().lower()
        if not reference:
            raise AssertionError("Eligible rewrite target missing stable table reference.")

        if not _scope_has_tenant_predicate(target.scope_select, reference, tenant_column):
            raise AssertionError(
                "Eligible rewrite target missing tenant predicate for " f"reference '{reference}'."
            )


def assert_rewrite_invariants(
    sql: str,
    rewritten_sql: str,
    params: Sequence[int],
    *,
    provider: str = "sqlite",
    tenant_column: str = "tenant_id",
    global_table_allowlist: set[str] | None = None,
    table_columns: Mapping[str, set[str]] | None = None,
    strict_mode: bool = True,
    tenant_id: int | None = None,
) -> None:
    """Assert internal invariants for tenant rewrite correctness in debug/test modes."""
    del table_columns  # Invariants operate on SQL shape and predicate semantics only.

    normalized_provider = (provider or "").strip().lower()
    dialect = normalize_sqlglot_dialect(normalized_provider)
    original_expressions = sqlglot.parse(sql, read=dialect)
    rewritten_expressions = sqlglot.parse(rewritten_sql, read=dialect)

    if len(original_expressions) != 1 or original_expressions[0] is None:
        raise AssertionError("Invariant input must contain exactly one original SQL statement.")
    if len(rewritten_expressions) != 1 or rewritten_expressions[0] is None:
        raise AssertionError("Invariant input must contain exactly one rewritten SQL statement.")

    original_expression = original_expressions[0]
    rewritten_expression = rewritten_expressions[0]

    if not isinstance(original_expression, exp.Select):
        raise AssertionError("Invariant input original SQL must be a SELECT statement.")
    if not isinstance(rewritten_expression, exp.Select):
        raise AssertionError("Invariant input rewritten SQL must be a SELECT statement.")

    rewrite_predicate_count = _count_rewrite_predicates(rewritten_expression, tenant_column)
    if rewrite_predicate_count != len(params):
        raise AssertionError(
            "Placeholder count mismatch: "
            f"expected {rewrite_predicate_count}, received {len(params)} params."
        )

    _assert_no_duplicate_tenant_predicates(rewritten_expression, tenant_column)
    _assert_predicates_within_intended_scope(rewritten_expression, tenant_column)
    _assert_all_eligible_targets_rewritten(
        rewritten_expression,
        strict_mode=strict_mode,
        tenant_column=tenant_column,
        global_table_allowlist={
            entry.strip().lower() for entry in (global_table_allowlist or set())
        },
    )

    deterministic_tenant_id = tenant_id if tenant_id is not None else (params[0] if params else 0)
    second_pass = rewrite_tenant_scoped_sql(
        sql,
        provider=normalized_provider,
        tenant_id=int(deterministic_tenant_id),
        tenant_column=tenant_column,
        global_table_allowlist=global_table_allowlist,
        _skip_invariant_check=True,
    )
    if (second_pass.rewritten_sql, second_pass.params) != (rewritten_sql, list(params)):
        raise AssertionError("Tenant rewrite second pass is not deterministic.")


def _assert_rewrite_eligible(
    expression: exp.Expression, *, strict_mode: bool = True
) -> CTEClassification | None:
    """Reject SQL shapes that cannot be scoped deterministically by the v1 rewriter."""
    from common.security.tenant_enforcement_policy import TenantEnforcementPolicy, TenantSQLShape

    policy = TenantEnforcementPolicy(
        provider="sqlite",  # Provider doesn't matter for pure AST checks here
        mode="sql_rewrite",
        strict=strict_mode,
        max_targets=MAX_TENANT_REWRITE_TARGETS,
        max_params=MAX_TENANT_REWRITE_PARAMS,
        max_ast_nodes=MAX_SQL_AST_NODES,  # Ensure it doesn't fail on complexity inside classify_sql
        hard_timeout_ms=1000,
    )

    classification_shape = policy.classify_sql(expression.sql(dialect="sqlite"), provider="sqlite")

    if classification_shape == TenantSQLShape.UNSUPPORTED_SET_OPERATION:
        raise TenantSQLRewriteError(
            "Tenant rewrite v1 does not support set operations.",
            reason_code="SET_OPERATIONS_UNSUPPORTED",
        )
    if classification_shape == TenantSQLShape.UNSUPPORTED_STATEMENT_TYPE:
        raise TenantSQLRewriteError(
            "Tenant rewrite supports SELECT statements only.", reason_code="NOT_SELECT_STATEMENT"
        )
    if classification_shape == TenantSQLShape.UNSUPPORTED_CTE:
        raise TenantSQLRewriteError(
            "Tenant rewrite v1 does not support unsupported CTEs.",
            reason_code="CTE_UNSUPPORTED_SHAPE",
        )
    if classification_shape == TenantSQLShape.UNSUPPORTED_WINDOW_FUNCTION:
        raise TenantSQLRewriteError(
            "Tenant rewrite v1 does not support window functions.",
            reason_code="WINDOW_FUNCTIONS_UNSUPPORTED",
        )
    if classification_shape == TenantSQLShape.UNSUPPORTED_NESTED_FROM:
        raise TenantSQLRewriteError(
            "Tenant rewrite v1 does not support nested SELECTs in FROM.",
            reason_code="NESTED_FROM_UNSUPPORTED",
        )
    if classification_shape == TenantSQLShape.UNSUPPORTED_CORRELATED_SUBQUERY:
        raise TenantSQLRewriteError(
            "Tenant rewrite v1 does not support correlated subqueries.",
            reason_code="CORRELATED_SUBQUERY_UNSUPPORTED",
        )
    if classification_shape == TenantSQLShape.UNSUPPORTED_SUBQUERY:
        raise TenantSQLRewriteError(
            "Tenant rewrite v1 does not support this subquery shape.",
            reason_code="SUBQUERY_UNSUPPORTED",
        )
    if classification_shape == TenantSQLShape.SAFE_CTE_QUERY:
        return CTEClassification.SAFE_SIMPLE_CTE

    return None


def classify_subquery(expression: exp.Expression) -> SubqueryClassification:
    """Classify if a subquery is safe for conservative tenant rewrite."""
    if not isinstance(expression, exp.Select):
        return SubqueryClassification.UNSUPPORTED_SUBQUERY

    if expression.args.get("with_") is not None:
        return SubqueryClassification.UNSUPPORTED_SUBQUERY

    if _contains_set_operation(expression):
        return SubqueryClassification.UNSUPPORTED_SUBQUERY

    if _has_nested_select(expression):
        return SubqueryClassification.UNSUPPORTED_SUBQUERY

    if _is_scalar_aggregate_subquery(expression) and not _is_safe_scalar_aggregate_subquery(
        expression
    ):
        return SubqueryClassification.UNSUPPORTED_SUBQUERY

    return SubqueryClassification.SAFE_SIMPLE_SUBQUERY


def _is_scalar_aggregate_subquery(expression: exp.Select) -> bool:
    projections = expression.expressions or []
    if not projections:
        return False
    return any(_projection_contains_aggregate(projection) for projection in projections)


def _projection_contains_aggregate(expression: exp.Expression) -> bool:
    candidate = expression.this if isinstance(expression, exp.Alias) else expression
    if isinstance(candidate, exp.AggFunc):
        return True
    return any(isinstance(node, exp.AggFunc) for node in candidate.find_all(exp.AggFunc))


def _is_safe_scalar_aggregate_subquery(expression: exp.Select) -> bool:
    projections = expression.expressions or []
    if len(projections) != 1:
        return False

    if not _projection_contains_aggregate(projections[0]):
        return False

    if expression.args.get("group") is not None:
        return False
    if expression.args.get("having") is not None:
        return False
    if expression.args.get("distinct") is not None:
        return False

    limit = expression.args.get("limit")
    limit_value = _literal_limit_value(limit)
    if limit is not None and (limit_value is None or limit_value > 1):
        return False

    if expression.args.get("order") is not None and limit_value != 1:
        return False

    return True


def _literal_limit_value(limit: exp.Expression | None) -> int | None:
    if not isinstance(limit, exp.Limit):
        return None
    expression = limit.args.get("expression")
    if not isinstance(expression, exp.Literal) or not expression.is_int:
        return None
    try:
        value = int(expression.this)
    except Exception:
        return None
    if value < 0:
        return None
    return value


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

    # 1. Check CTE bodies
    for cte in with_.expressions:
        this = cte.this
        if not isinstance(this, exp.Select):
            return CTEClassification.UNSUPPORTED_CTE

        if _contains_set_operation(this):
            return CTEClassification.UNSUPPORTED_CTE

        for node in this.find_all(exp.Select):
            if node is this:
                continue
            if classify_subquery(node) == SubqueryClassification.UNSUPPORTED_SUBQUERY:
                return CTEClassification.UNSUPPORTED_CTE

        # Ensure CTE body is a simple SELECT.
        # v1.1: Allow CTEs to reference previously defined CTEs.
        # The rewrite logic will correctly skip them if they are in cte_names.
        pass

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
        # If this select is NOT one of the CTE definition bodies, check if it's safe.
        is_cte_body = False
        for cte in with_.expressions:
            if node is cte.this:
                is_cte_body = True
                break
        if not is_cte_body:
            if classify_subquery(node) == SubqueryClassification.UNSUPPORTED_SUBQUERY:
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


def _get_defined_names(select: exp.Select) -> set[str]:
    names: set[str] = set()
    for table in _top_level_tables(select):
        alias = (table.alias_or_name or "").strip().lower()
        if alias:
            names.add(alias)
        physical = (table.name or "").strip().lower()
        if physical:
            names.add(physical)
    return names


def _get_cte_names(select: exp.Select) -> set[str]:
    with_ = select.args.get("with_")
    if not with_:
        return set()
    return {cte.alias_or_name.lower() for cte in with_.expressions if cte.alias_or_name}


def _has_correlated_subquery(expression: exp.Select, *, strict_mode: bool) -> bool:
    outer_visible_names = _get_defined_names(expression) | _get_cte_names(expression)
    if not outer_visible_names:
        return False

    with_ = expression.args.get("with_")

    for select in expression.find_all(exp.Select):
        if select is expression:
            continue

        is_cte_body = False
        if with_:
            for cte in with_.expressions:
                if select is cte.this:
                    is_cte_body = True
                    break
        if is_cte_body:
            continue

        inner_defined_names = _get_defined_names(select) | _get_cte_names(select)
        has_ambiguous_names = bool(inner_defined_names & outer_visible_names)

        allow_ambiguous_unqualified = not strict_mode and _is_relaxed_single_from_scope(select)

        for column in select.find_all(exp.Column):
            col_table = (column.table or "").strip().lower()
            if col_table:
                if col_table in inner_defined_names:
                    continue
                if col_table in outer_visible_names:
                    return True
            else:
                if has_ambiguous_names and not allow_ambiguous_unqualified:
                    return True

    return False


def _is_relaxed_single_from_scope(select: exp.Select) -> bool:
    """Return True for the narrowly safe relaxed shape when strict mode is disabled."""
    from_clause = select.args.get("from_")
    if not isinstance(from_clause, exp.From):
        return False
    if not isinstance(from_clause.args.get("this"), exp.Table):
        return False
    joins = select.args.get("joins") or []
    return len(joins) == 0


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


def _assert_completeness(
    expression: exp.Select,
    classification: CTEClassification | None,
    cte_names: set[str],
    allowlist: set[str],
    rewritten_target_keys: set[tuple],
) -> None:
    """Ensure every eligible base table node has been rewritten."""
    all_targets = _collect_all_rewrite_targets(expression, classification)
    for target in all_targets:
        table_keys = _table_keys(target.table)
        if any(key in cte_names for key in table_keys):
            continue
        if any(key in allowlist for key in table_keys):
            continue

        target_key = (
            target.cte_name or "",
            target.effective_name,
            target.physical_name,
            target.scope_index,
            target.appearance_index,
        )
        if target_key not in rewritten_target_keys:
            raise TenantSQLRewriteError(
                f"Tenant predicate injection incomplete for table: {target.effective_name}",
                reason_code="COMPLETENESS_FAILED",
            )
