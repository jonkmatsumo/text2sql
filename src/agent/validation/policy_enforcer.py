"""Policy enforcer for SQL validation.

Validates SQL against security policies using AST analysis.
"""

import logging
from functools import lru_cache
from typing import Optional, Set

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


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


@lru_cache(maxsize=1)
def _introspect_allowed_tables() -> Set[str]:
    """Introspect allowed tables from information_schema.

    Returns tables from the public schema only.
    Caches result to avoid per-query overhead.

    Cache is process-lifetime (lru_cache) and assumes schema stability.
    Call clear_table_cache() if schema changes at runtime (e.g. migrations).

    Returns:
        Set of lowercase table names allowed for querying.
    """
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
                logger.info(f"Introspected {len(tables)} tables from public schema")
                return tables
        finally:
            conn.close()
    except Exception as e:
        logger.warning("Failed to introspect tables: %s. Using empty allowlist.", e)
        logger.exception("PolicyEnforcer introspection failed")
        return set()


def clear_table_cache() -> None:
    """Clear the cached table introspection for testing or schema changes."""
    _introspect_allowed_tables.cache_clear()


class PolicyEnforcer:
    """Enforces security policies on SQL AST."""

    # Dangerous functions that should be blocked
    BLOCKED_FUNCTIONS = {
        "pg_read_file",
        "pg_ls_dir",
        "pg_stat_file",
        "current_setting",
        "set_config",
        "current_user",
        "session_user",
        "version",
        "pg_sleep",
    }

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
        try:
            # Parse SQL to AST
            parsed = sqlglot.parse(sql)
        except Exception as e:
            raise ValueError(f"Invalid SQL syntax: {e}")

        allowed_tables = cls.get_allowed_tables()

        for statement in parsed:
            # 1. Enforce specific statement types (SELECT, UNION, INTERSECT, EXCEPT)
            if not isinstance(statement, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
                # Allow specific SET commands if needed for session config, but generally block
                raise ValueError(
                    f"Statement type not allowed: {type(statement).__name__}. "
                    "Only SELECT, UNION, INTERSECT, EXCEPT are allowed."
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
                            raise ValueError(
                                f"Cross-schema access not allowed: {schema}.{table_name}"
                            )

                    if table_name not in allowed_tables:
                        # Allow CTE references (which appear as tables)
                        # Helper: check if it's a CTE defined in the query
                        if not cls._is_cte(node, statement):
                            raise ValueError(f"Access to table '{table_name}' is not allowed.")

                # Check for functions
                if isinstance(node, exp.Func):
                    # sqlglot represents function calls as nodes inheriting from Func
                    # The function name is usually the class name or sql_name()
                    # For Anonymous functions, check node.name instead
                    func_name = node.sql_name().lower()
                    if func_name in cls.BLOCKED_FUNCTIONS:
                        raise ValueError(f"Function '{func_name}' is restricted.")

                    # Handle Anonymous functions (e.g., pg_read_file)
                    if hasattr(node, "name") and node.name:
                        actual_name = node.name.lower()
                        if actual_name in cls.BLOCKED_FUNCTIONS:
                            raise ValueError(f"Function '{actual_name}' is restricted.")

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
