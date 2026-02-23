"""Policy enforcer for SQL validation.

Validates SQL against security policies using AST analysis.
"""

import logging
import threading
import time
from typing import Optional, Set

import sqlglot
from sqlglot import exp

from agent.audit import AuditEventSource, AuditEventType, emit_audit_event
from common.config.env import get_env_bool
from common.models.error_metadata import ErrorCategory
from common.policy.sql_policy import is_sensitive_column_name
from common.sql.comments import strip_sql_comments

logger = logging.getLogger(__name__)

_TABLE_CACHE_LOCK = threading.Lock()
_TABLE_CACHE_VALUE: Optional[Set[str]] = None
_TABLE_CACHE_FETCHED_AT: Optional[float] = None


def _get_db_url() -> str:
    """Build database connection URL from environment variables."""
    from common.config.env import get_env_str

    host = get_env_str("DB_HOST", "localhost")
    port = get_env_str("DB_PORT", "5432")
    from common.config.dataset import get_default_db_name

    db_name = get_env_str("DB_NAME", get_default_db_name())
    user = get_env_str("DB_USER", "text2sql_ro")
    password = get_env_str("DB_PASS", "secure_agent_pass")
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def _introspect_allowed_tables() -> Set[str]:
    """Introspect allowed tables from information_schema.

    Returns tables from the public schema only.
    Caches result to avoid per-query overhead.

    Cache is process-local with TTL and assumes eventual schema stability.
    Call clear_table_cache() if schema changes at runtime (e.g. migrations).

    Returns:
        Set of lowercase table names allowed for querying.
    """
    now = time.monotonic()
    cached = _get_cached_tables(now=now)
    if cached is not None:
        return cached

    try:
        import psycopg2

        db_url = _get_db_url()
        conn = psycopg2.connect(db_url)
        try:
            with conn.cursor() as cur:
                # Only allow tables from public schema
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_type = 'BASE TABLE'
                """
                )
                tables = {row[0].lower() for row in cur.fetchall()}
                _set_cached_tables(tables, fetched_at=now)
                logger.info(f"Introspected {len(tables)} tables from public schema")
                return tables
        finally:
            conn.close()
    except Exception as e:
        logger.warning("Failed to introspect tables: %s. Using empty allowlist.", e)
        logger.exception("PolicyEnforcer introspection failed")
        empty_tables: Set[str] = set()
        _set_cached_tables(empty_tables, fetched_at=now)
        return empty_tables


def clear_table_cache() -> None:
    """Clear the cached table introspection for testing or schema changes."""
    global _TABLE_CACHE_VALUE
    global _TABLE_CACHE_FETCHED_AT
    with _TABLE_CACHE_LOCK:
        _TABLE_CACHE_VALUE = None
        _TABLE_CACHE_FETCHED_AT = None


def _get_table_cache_ttl_seconds() -> int:
    """Return allowlist cache TTL in seconds."""
    from common.config.env import get_env_int

    default_ttl_seconds = 300
    try:
        raw_ttl = get_env_int("POLICY_ALLOWED_TABLES_CACHE_TTL_SECONDS", default_ttl_seconds)
    except ValueError:
        return default_ttl_seconds
    if raw_ttl is None:
        return default_ttl_seconds
    return max(1, int(raw_ttl))


def _get_cached_tables(*, now: float) -> Optional[Set[str]]:
    """Return a copy of cached allowlist when TTL is still valid."""
    ttl_seconds = _get_table_cache_ttl_seconds()
    with _TABLE_CACHE_LOCK:
        if _TABLE_CACHE_VALUE is None or _TABLE_CACHE_FETCHED_AT is None:
            return None
        if now - _TABLE_CACHE_FETCHED_AT >= ttl_seconds:
            return None
        return set(_TABLE_CACHE_VALUE)


def _set_cached_tables(tables: Set[str], *, fetched_at: float) -> None:
    """Store allowlist cache snapshot."""
    global _TABLE_CACHE_VALUE
    global _TABLE_CACHE_FETCHED_AT
    with _TABLE_CACHE_LOCK:
        _TABLE_CACHE_VALUE = set(tables)
        _TABLE_CACHE_FETCHED_AT = float(fetched_at)


class PolicyEnforcer:
    """Enforces security policies on SQL AST."""

    from common.policy.sql_policy import ALLOWED_STATEMENT_TYPES, BLOCKED_FUNCTIONS

    # Unified policy
    ALLOWED_STATEMENT_TYPES = ALLOWED_STATEMENT_TYPES
    BLOCKED_FUNCTIONS = BLOCKED_FUNCTIONS

    # Optional static allowlist for testing/injection (if set, overrides introspection)
    _static_allowed_tables: Optional[Set[str]] = None

    @classmethod
    def get_allowed_tables(cls) -> Set[str]:
        """Get the set of allowed tables.

        Uses static allowlist if set (for testing), otherwise introspects from DB.
        """
        if cls._static_allowed_tables is not None:
            return cls._static_allowed_tables
        return _introspect_allowed_tables()

    @classmethod
    def set_allowed_tables(cls, tables: Optional[Set[str]]) -> None:
        """Set static allowed tables for testing. Pass None to use introspection."""
        cls._static_allowed_tables = tables

    @classmethod
    def validate_sql(cls, sql: str) -> bool:
        """Validate SQL string against security policies.

        Args:
            sql: The SQL string to validate.

        Returns:
            True if valid, raises ValueError if invalid.
        """
        stripped_sql = strip_sql_comments(sql)
        try:
            # Parse SQL to AST
            parsed = sqlglot.parse(stripped_sql)
        except Exception as e:
            cls._emit_policy_rejection(
                reason="invalid_sql_syntax",
                details={"error_type": type(e).__name__},
            )
            raise ValueError(f"Invalid SQL syntax: {e}")

        allowed_tables = cls.get_allowed_tables()

        for statement in parsed:
            sensitive_columns: set[str] = set()

            # 1. Enforce specific statement types
            if statement.key not in cls.ALLOWED_STATEMENT_TYPES:
                # Allow specific SET commands if needed for session config, but generally block
                cls._emit_policy_rejection(
                    reason="statement_type_not_allowed",
                    details={"statement_type": type(statement).__name__},
                )
                raise ValueError(
                    f"Statement type not allowed: {type(statement).__name__}. "
                    f"Only {', '.join(sorted([t.upper() for t in cls.ALLOWED_STATEMENT_TYPES]))} "
                    "are allowed."
                )

            # 2. Walk the AST to check all nodes
            for node in statement.walk():
                # Check for accessed tables
                if isinstance(node, exp.Table):
                    table_name = node.name.lower()

                    # Check for cross-schema access (anything not public)
                    if node.db:  # Has explicit schema specified
                        schema = (
                            node.db.lower() if isinstance(node.db, str) else str(node.db).lower()
                        )
                        if schema != "public":
                            cls._emit_policy_rejection(
                                reason="cross_schema_access",
                                details={"schema": schema, "table": table_name},
                            )
                            raise ValueError(
                                f"Cross-schema access not allowed: {schema}.{table_name}"
                            )

                    if table_name not in allowed_tables:
                        # Allow CTE references (which appear as tables)
                        # Helper: check if it's a CTE defined in the query
                        if not cls._is_cte(node, statement):
                            cls._emit_policy_rejection(
                                reason="table_not_allowed",
                                details={"table": table_name},
                            )
                            raise ValueError(f"Access to table '{table_name}' is not allowed.")

                # Check for functions
                if isinstance(node, exp.Func):
                    # Robustly extract name(s) to check against denylist.
                    # sqlglot uses different attributes depending on the function type.
                    func_names: set[str] = {node.sql_name().lower()}
                    if isinstance(node, exp.Anonymous) and node.this:
                        func_names.add(str(node.this).lower())
                    if hasattr(node, "name") and node.name:
                        func_names.add(str(node.name).lower())

                    for name in func_names:
                        if name in cls.BLOCKED_FUNCTIONS:
                            cls._emit_policy_rejection(
                                reason="blocked_function",
                                details={"function": name},
                            )
                            raise ValueError(f"Function '{name}' is restricted.")

                if isinstance(node, exp.Column):
                    column_name = node.name.lower() if node.name else ""
                    if is_sensitive_column_name(column_name):
                        sensitive_columns.add(column_name)

            if sensitive_columns:
                sensitive_list = ", ".join(sorted(sensitive_columns))
                message = f"Sensitive column reference detected: {sensitive_list}."
                if get_env_bool("AGENT_BLOCK_SENSITIVE_COLUMNS", False):
                    cls._emit_policy_rejection(
                        reason="sensitive_column_reference",
                        details={"columns": sensitive_list},
                    )
                    raise ValueError(message)
                logger.warning(
                    "%s Query allowed because AGENT_BLOCK_SENSITIVE_COLUMNS=false.",
                    message,
                )

        return True

    @staticmethod
    def _is_cte(table_node: exp.Table, root: exp.Expression) -> bool:
        """Check if a table reference corresponds to a Common Table Expression (CTE)."""
        # Search for CTE definitions in the root statement
        ctes = root.find(exp.With)
        if ctes:
            for cte in ctes.expressions:
                if cte.alias == table_node.name:
                    return True
        return False

    @staticmethod
    def _emit_policy_rejection(*, reason: str, details: Optional[dict[str, str]] = None) -> None:
        emit_audit_event(
            AuditEventType.POLICY_REJECTION,
            source=AuditEventSource.AGENT,
            error_category=ErrorCategory.INVALID_REQUEST,
            metadata={
                "reason_code": reason,
                "decision": "reject",
                **(details or {}),
            },
        )
