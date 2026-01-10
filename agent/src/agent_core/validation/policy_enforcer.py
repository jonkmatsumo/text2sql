"""Policy enforcer for SQL validation.

Validates SQL against security policies using AST analysis.
"""

import logging

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


class PolicyEnforcer:
    """Enforces security policies on SQL AST."""

    # Tables that are allowed to be queried
    ALLOWED_TABLES = {
        "customer",
        "rental",
        "payment",
        "staff",
        "inventory",
        "film",
        "actor",
        "address",
        "city",
        "country",
        "category",
        "language",
        "film_actor",
        "film_category",
        "store",
    }

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

        for statement in parsed:
            # 1. Enforce specific statement types (SELECT only, plus UNION)
            if not isinstance(statement, (exp.Select, exp.Union)):
                # Allow specific SET commands if needed for session config, but generally block
                raise ValueError(
                    f"Statement type not allowed: {type(statement).__name__}. "
                    "Only SELECT is allowed."
                )

            # 2. Walk the AST to check all nodes
            for node in statement.walk():
                # Check for accessed tables
                if isinstance(node, exp.Table):
                    table_name = node.name.lower()
                    if table_name not in cls.ALLOWED_TABLES:
                        # Allow CTE references (which appear as tables)
                        # Helper: check if it's a CTE defined in the query
                        if not cls._is_cte(node, statement):
                            raise ValueError(f"Access to table '{table_name}' is not allowed.")

                # Check for functions
                if isinstance(node, exp.Func):
                    # sqlglot represents function calls as nodes inheriting from Func
                    # The function name is usually the class name or sql_name()
                    func_name = node.sql_name().lower()
                    if func_name in cls.BLOCKED_FUNCTIONS:
                        raise ValueError(f"Function '{func_name}' is restricted.")

                    # Also check generic Command/Identifier if sqlglot parses them differently

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
