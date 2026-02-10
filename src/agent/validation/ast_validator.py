"""SQL AST validation using sqlglot for security checks and metadata extraction.

This module provides:
- SQL parsing with dialect support (PostgreSQL default)
- Security validation (blocked tables, forbidden commands)
- Metadata extraction for audit logging (table lineage, column usage, join complexity)
- Structured error objects for the healer loop
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import sqlglot
from sqlglot import exp

from agent.utils.sql_ast import count_joins, extract_columns, extract_tables, normalize_sql
from common.config.env import get_env_bool, get_env_int
from common.constants.reason_codes import ValidationRefusalReason
from common.policy.sql_policy import is_sensitive_column_name
from common.sql.dialect import normalize_sqlglot_dialect

logger = logging.getLogger(__name__)


class ViolationType(str, Enum):
    """Types of security violations detected during AST validation."""

    RESTRICTED_TABLE = "restricted_table"
    FORBIDDEN_COMMAND = "forbidden_command"
    DANGEROUS_PATTERN = "dangerous_pattern"
    SYNTAX_ERROR = "syntax_error"
    COMPLEXITY_LIMIT = "complexity_limit"
    SENSITIVE_COLUMN = "sensitive_column"
    COLUMN_ALLOWLIST = "column_allowlist"


@dataclass
class SecurityViolation:
    """Structured security violation for healer loop consumption."""

    violation_type: ViolationType
    message: str
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for state storage."""
        return {
            "violation_type": self.violation_type.value,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class SQLMetadata:
    """Extracted metadata from SQL AST for audit logging."""

    table_lineage: list[str] = field(default_factory=list)
    column_usage: list[str] = field(default_factory=list)
    join_complexity: int = 0
    has_aggregation: bool = False
    has_subquery: bool = False
    has_window_function: bool = False

    def to_dict(self) -> dict:
        """Convert to dictionary for state storage."""
        return {
            "table_lineage": self.table_lineage,
            "column_usage": self.column_usage,
            "join_complexity": self.join_complexity,
            "has_aggregation": self.has_aggregation,
            "has_subquery": self.has_subquery,
            "has_window_function": self.has_window_function,
        }


@dataclass
class ASTValidationResult:
    """Complete AST validation result including security status and metadata."""

    is_valid: bool
    violations: list[SecurityViolation] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: Optional[SQLMetadata] = None
    parsed_sql: Optional[str] = None  # Normalized/transpiled SQL

    def to_dict(self) -> dict:
        """Convert to dictionary for state storage."""
        return {
            "is_valid": self.is_valid,
            "violations": [v.to_dict() for v in self.violations],
            "warnings": self.warnings,
            "metadata": self.metadata.to_dict() if self.metadata else None,
            "parsed_sql": self.parsed_sql,
        }


# Security configuration
RESTRICTED_TABLES = frozenset(
    {
        "payroll",
        "credentials",
        "audit_logs",
        "user_secrets",
        "password_history",
        "api_keys",
    }
)

RESTRICTED_TABLE_PREFIXES = ("pg_", "information_schema.")

FORBIDDEN_COMMANDS = frozenset(
    {
        exp.Drop,
        exp.Delete,
        exp.Insert,
        exp.Update,
        exp.Alter,  # Base Alter class covers all ALTER operations
        exp.Grant,
        exp.Create,
        exp.Command,  # Generic database commands
    }
)


def parse_sql(
    sql: str, dialect: str = "postgres"
) -> tuple[Optional[exp.Expression], Optional[str]]:
    """
    Parse SQL string into AST.

    Args:
        sql: SQL query string
        dialect: SQL dialect (default: postgres)

    Returns:
        Tuple of (parsed AST, error message if parse failed)
    """
    dialect = normalize_sqlglot_dialect(dialect)
    try:
        expressions = sqlglot.parse(sql, dialect=dialect)

        if not expressions:
            return None, "Empty SQL query"

        if len(expressions) > 1:
            return (
                None,
                "Security Violation: SQL chaining detected. Multiple statements are not allowed.",
            )

        return expressions[0], None
    except sqlglot.errors.ParseError as e:
        return None, f"SQL syntax error: {str(e)}"


def validate_security(
    ast: exp.Expression, allowed_tables: Optional[set[str]] = None
) -> list[SecurityViolation]:
    """
    Validate SQL AST for security violations.

    Checks:
    - Restricted table access (payroll, credentials, etc.)
    - System table access (pg_*, information_schema)
    - Forbidden commands (DROP, DELETE, INSERT, UPDATE, etc.)
    - Dangerous patterns (UNION with subqueries in certain contexts)

    Args:
        ast: Parsed SQL AST

    Returns:
        List of SecurityViolation objects (empty if valid)
    """
    violations = []
    normalized_allowed_tables = _normalize_allowed_tables(allowed_tables)
    cte_names = _extract_cte_names(ast)

    # 1. Enforce strict Root Node Policy (SELECT / CTE / UNION)
    # Only allow read-only query structures at the root level
    ALLOWED_ROOTS = (exp.Select, exp.Union, exp.With, exp.Paren, exp.Subquery)
    if not isinstance(ast, ALLOWED_ROOTS):
        violations.append(
            SecurityViolation(
                violation_type=ViolationType.FORBIDDEN_COMMAND,
                message=(
                    f"Invalid Root Statement: {type(ast).__name__}. "
                    "Only SELECT, WITH (CTE), or UNION are allowed."
                ),
                details={"root_type": type(ast).__name__},
            )
        )

    # 2. Recursive Forbidden Command Check (Deep Walk)
    # Traverses the entire tree to find nested forbidden commands
    for node in ast.walk():
        if isinstance(node, tuple(FORBIDDEN_COMMANDS)):
            violations.append(
                SecurityViolation(
                    violation_type=ViolationType.FORBIDDEN_COMMAND,
                    message=(
                        f"Forbidden command detected: {type(node).__name__}. "
                        "Read-only access only."
                    ),
                    details={"command": type(node).__name__},
                )
            )

    # Check for restricted table access
    for table in ast.find_all(exp.Table):
        table_name = table.name.lower() if table.name else ""
        schema_name = table.db.lower() if table.db else ""
        full_name = f"{schema_name}.{table_name}" if schema_name else table_name

        if table_name in cte_names:
            continue

        if normalized_allowed_tables and not _is_table_in_allowlist(
            table, normalized_allowed_tables
        ):
            violations.append(
                SecurityViolation(
                    violation_type=ViolationType.RESTRICTED_TABLE,
                    message=(
                        f"Security Violation: Access to table '{full_name}' is not allowed. "
                        "Please use only tables from the approved allowlist."
                    ),
                    details={"table": full_name, "reason": "table_not_allowlisted"},
                )
            )

        # Check exact matches
        if table_name in RESTRICTED_TABLES:
            violations.append(
                SecurityViolation(
                    violation_type=ViolationType.RESTRICTED_TABLE,
                    message=(
                        f"Security Violation: Access to table '{table_name}' is restricted. "
                        "Please reformulate the query using only permitted tables."
                    ),
                    details={"table": table_name, "reason": "restricted_table"},
                )
            )

        # Check prefix matches (system tables)
        for prefix in RESTRICTED_TABLE_PREFIXES:
            if full_name.startswith(prefix) or table_name.startswith(prefix):
                violations.append(
                    SecurityViolation(
                        violation_type=ViolationType.RESTRICTED_TABLE,
                        message=(
                            f"Security Violation: Access to system table "
                            f"'{full_name}' is forbidden. Only user-defined "
                            "tables in the public schema are permitted."
                        ),
                        details={"table": full_name, "reason": "system_table"},
                    )
                )
                break

    # Check for dangerous UNION patterns with subqueries
    # (these can sometimes be used for SQL injection via UNION-based attacks)
    unions = list(ast.find_all(exp.Union))
    if len(unions) > 2:  # Allow simple unions, flag complex ones
        subqueries = list(ast.find_all(exp.Subquery))
        if subqueries:
            violations.append(
                SecurityViolation(
                    violation_type=ViolationType.DANGEROUS_PATTERN,
                    message=(
                        "Warning: Complex UNION with subqueries detected. "
                        "Please simplify the query or use CTEs instead."
                    ),
                    details={"union_count": len(unions), "subquery_count": len(subqueries)},
                )
            )

    # Enforce allowlist for UNION / INTERSECT / EXCEPT branches when provided.
    violations.extend(_validate_set_operation_allowlist(ast, allowed_tables))

    return violations


def _normalize_allowed_tables(allowed_tables: Optional[set[str]]) -> set[str]:
    if not allowed_tables:
        return set()
    return {str(table).strip().lower() for table in allowed_tables if str(table).strip()}


def _extract_cte_names(ast: exp.Expression) -> set[str]:
    cte_names: set[str] = set()
    for cte in ast.find_all(exp.CTE):
        alias = cte.alias_or_name
        if isinstance(alias, str) and alias.strip():
            cte_names.add(alias.strip().lower())
    return cte_names


def _is_table_in_allowlist(table: exp.Table, allowed_tables: set[str]) -> bool:
    if not allowed_tables:
        return True

    table_name = table.name.lower() if table.name else ""
    schema_name = table.db.lower() if table.db else ""
    full_name = f"{schema_name}.{table_name}" if schema_name and table_name else table_name

    return table_name in allowed_tables or full_name in allowed_tables


def _disallowed_tables_in_branch(
    branch: Optional[exp.Expression], allowed_tables: set[str], cte_names: set[str]
) -> list[str]:
    if branch is None or not allowed_tables:
        return []

    disallowed = set()
    for table in branch.find_all(exp.Table):
        table_name = table.name.lower() if table.name else ""
        if table_name in cte_names:
            continue
        if not _is_table_in_allowlist(table, allowed_tables):
            schema_name = table.db.lower() if table.db else ""
            full_name = f"{schema_name}.{table_name}" if schema_name and table_name else table_name
            if full_name:
                disallowed.add(full_name)
    return sorted(disallowed)


def _validate_set_operation_allowlist(
    ast: exp.Expression,
    allowed_tables: Optional[set[str]],
) -> list[SecurityViolation]:
    """Block set-operation branches that reference non-allowlisted tables."""
    normalized_allowed = _normalize_allowed_tables(allowed_tables)
    if not normalized_allowed:
        return []
    cte_names = _extract_cte_names(ast)

    violations: list[SecurityViolation] = []
    set_operations = tuple(ast.find_all((exp.Union, exp.Intersect, exp.Except)))
    for set_op in set_operations:
        operation = type(set_op).__name__.upper()
        branches = {"left": set_op.left, "right": set_op.right}
        for branch_name, branch_ast in branches.items():
            disallowed_tables = _disallowed_tables_in_branch(
                branch_ast, normalized_allowed, cte_names
            )
            if not disallowed_tables:
                continue
            violations.append(
                SecurityViolation(
                    violation_type=ViolationType.RESTRICTED_TABLE,
                    message=(
                        f"Security Violation: {operation} branch references tables "
                        f"outside allowed set: {', '.join(disallowed_tables)}."
                    ),
                    details={
                        "operation": operation,
                        "branch": branch_name,
                        "tables": disallowed_tables,
                        "reason": "set_operation_disallowed_table",
                    },
                )
            )
    return violations


def _extract_sensitive_columns(ast: exp.Expression) -> list[str]:
    sensitive: set[str] = set()
    for column in ast.find_all(exp.Column):
        column_name = column.name.lower() if column.name else ""
        if is_sensitive_column_name(column_name):
            sensitive.add(column_name)
    return sorted(sensitive)


def _normalize_allowed_columns(
    allowed_columns: Optional[dict[str, set[str]]],
) -> dict[str, set[str]]:
    normalized: dict[str, set[str]] = {}
    if not allowed_columns:
        return normalized

    for table_name, columns in allowed_columns.items():
        normalized_table = str(table_name).strip().lower()
        if not normalized_table:
            continue
        normalized_columns = {
            str(column_name).strip().lower()
            for column_name in (columns or set())
            if str(column_name).strip()
        }
        if normalized_columns:
            normalized[normalized_table] = normalized_columns
    return normalized


def _extract_table_alias_map(ast: exp.Expression, cte_names: set[str]) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    for table in ast.find_all(exp.Table):
        table_name = table.name.lower() if table.name else ""
        if not table_name or table_name in cte_names:
            continue
        alias = table.alias_or_name.lower() if table.alias_or_name else ""
        if alias:
            alias_map[alias] = table_name
    return alias_map


def _validate_column_allowlist(
    ast: exp.Expression,
    allowed_columns: Optional[dict[str, set[str]]],
    mode: str,
) -> tuple[list[SecurityViolation], list[str]]:
    """Validate selected columns against an allowlist.

    This guard is intentionally scoped to projection columns to avoid false
    positives on predicates and join conditions.
    """
    normalized_allowed_columns = _normalize_allowed_columns(allowed_columns)
    if not normalized_allowed_columns or mode == "off":
        return [], []

    cte_names = _extract_cte_names(ast)
    alias_map = _extract_table_alias_map(ast, cte_names)

    violations: list[SecurityViolation] = []
    warnings: list[str] = []

    for select_node in ast.find_all(exp.Select):
        for projection in select_node.expressions or []:
            projected_columns: list[exp.Column] = []
            if isinstance(projection, exp.Star):
                message = (
                    "Column allowlist warning: wildcard projection (*) is not explicitly "
                    "allowlisted."
                )
                details = {
                    "reason": "column_not_allowlisted",
                    "column": "*",
                    "table": None,
                }
                violation = SecurityViolation(
                    violation_type=ViolationType.COLUMN_ALLOWLIST,
                    message=message,
                    details=details,
                )
                if mode == "block":
                    violations.append(violation)
                else:
                    warnings.append(message)
                continue

            if isinstance(projection, exp.Column):
                projected_columns = [projection]
            else:
                projected_columns = list(projection.find_all(exp.Column))

            for column in projected_columns:
                column_name = column.name.lower() if column.name else ""
                if not column_name:
                    continue

                # Skip unqualified columns to avoid alias/CTE ambiguity.
                table_ref = column.table.lower() if column.table else ""
                if not table_ref:
                    continue

                table_name = alias_map.get(table_ref, table_ref)
                allowed_for_table = normalized_allowed_columns.get(table_name)
                # Unknown table mapping is treated as non-enforceable to avoid
                # false positives on derived tables/CTEs.
                if allowed_for_table is None:
                    continue
                if column_name in allowed_for_table:
                    continue

                message = (
                    f"Column allowlist violation: column '{table_name}.{column_name}' "
                    "is not allowed."
                )
                details = {
                    "reason": "column_not_allowlisted",
                    "table": table_name,
                    "column": column_name,
                }
                violation = SecurityViolation(
                    violation_type=ViolationType.COLUMN_ALLOWLIST,
                    message=message,
                    details=details,
                )
                if mode == "block":
                    violations.append(violation)
                else:
                    warnings.append(message)

    return violations, warnings


def extract_metadata(ast: exp.Expression) -> SQLMetadata:
    """
    Extract metadata from SQL AST for audit logging and complexity analysis.

    Extracts:
    - Table lineage (all tables referenced)
    - Column usage (all columns referenced)
    - Join complexity (count of join operations)
    - Aggregation presence
    - Subquery presence
    - Window function presence

    Args:
        ast: Parsed SQL AST

    Returns:
        SQLMetadata object with extracted information
    """
    metadata = SQLMetadata()

    # Extract table lineage
    metadata.table_lineage = sorted(extract_tables(ast))

    # Extract column usage
    metadata.column_usage = sorted(extract_columns(ast))

    # Count join complexity
    metadata.join_complexity = count_joins(ast)

    # Check for aggregation
    agg_funcs = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max, exp.AggFunc)
    metadata.has_aggregation = any(ast.find_all(agg_funcs))

    # Check for subqueries
    metadata.has_subquery = bool(list(ast.find_all(exp.Subquery)))

    # Check for window functions
    metadata.has_window_function = bool(list(ast.find_all(exp.Window)))

    return metadata


def validate_complexity(ast: exp.Expression) -> list[SecurityViolation]:
    """
    Validate SQL AST for complexity limits.

    Checks:
    - Join complexity

    Args:
        ast: Parsed SQL AST

    Returns:
        List of SecurityViolation objects (empty if valid)
    """
    violations = []

    # Join Complexity
    # Default to 10 as per conservative default
    max_joins = get_env_int("AGENT_MAX_JOIN_COMPLEXITY", 10)
    join_count = count_joins(ast)
    if join_count > max_joins:
        violations.append(
            SecurityViolation(
                violation_type=ViolationType.COMPLEXITY_LIMIT,
                message=(
                    f"Complexity Limit: Query contains {join_count} joins, "
                    f"exceeding the limit of {max_joins}. Please simplify the query."
                ),
                details={
                    "join_count": join_count,
                    "limit": max_joins,
                    "reason_code": ValidationRefusalReason.JOIN_COMPLEXITY_EXCEEDED.value,
                },
            )
        )
    return violations


def validate_sql(
    sql: str,
    dialect: str = "postgres",
    allowed_tables: Optional[set[str]] = None,
    allowed_columns: Optional[dict[str, set[str]]] = None,
    column_allowlist_mode: str = "warn",
) -> ASTValidationResult:
    """
    Complete SQL validation: parse, security check, complexity check, and metadata extraction.

    This is the main entry point for AST validation.

    Args:
        sql: SQL query string
        dialect: SQL dialect (default: postgres)
        allowed_tables: Optional allowlist used for set-operation branch checks.
        allowed_columns: Optional table->columns allowlist used for projection checks.
        column_allowlist_mode: "warn" (default), "block", or "off".

    Returns:
        ASTValidationResult with validation status, violations, and metadata
    """
    # Parse SQL
    ast, parse_error = parse_sql(sql, dialect)

    if parse_error:
        return ASTValidationResult(
            is_valid=False,
            violations=[
                SecurityViolation(
                    violation_type=ViolationType.SYNTAX_ERROR,
                    message=parse_error,
                    details={"sql": sql[:200]},  # Truncate for safety
                )
            ],
        )

    # Validate security
    violations = validate_security(ast, allowed_tables=allowed_tables)
    warnings: list[str] = []

    column_mode = (column_allowlist_mode or "warn").strip().lower()
    if column_mode not in {"warn", "block", "off"}:
        column_mode = "warn"

    # Validate complexity
    complexity_violations = validate_complexity(ast)
    violations.extend(complexity_violations)

    # Optional column-level allowlist
    column_violations, column_warnings = _validate_column_allowlist(
        ast,
        allowed_columns=allowed_columns,
        mode=column_mode,
    )
    violations.extend(column_violations)
    warnings.extend(column_warnings)

    # Optional sensitive-column guardrail
    sensitive_columns = _extract_sensitive_columns(ast)
    if sensitive_columns:
        sensitive_message = (
            "Sensitive column reference detected: " + ", ".join(sensitive_columns) + "."
        )
        if get_env_bool("AGENT_BLOCK_SENSITIVE_COLUMNS", False):
            violations.append(
                SecurityViolation(
                    violation_type=ViolationType.SENSITIVE_COLUMN,
                    message=sensitive_message,
                    details={
                        "columns": sensitive_columns,
                        "reason": "sensitive_column_reference",
                    },
                )
            )
        else:
            warnings.append(sensitive_message)
            logger.warning(
                "%s Query allowed because AGENT_BLOCK_SENSITIVE_COLUMNS=false.",
                sensitive_message,
            )

    # Extract metadata (even if there are violations, for audit purposes)
    metadata = extract_metadata(ast)

    # Transpile to normalized form (useful for caching and comparison)
    try:
        parsed_sql = normalize_sql(ast, dialect=dialect)
    except Exception:
        parsed_sql = sql  # Fallback to original

    return ASTValidationResult(
        is_valid=len(violations) == 0,
        violations=violations,
        warnings=warnings,
        metadata=metadata,
        parsed_sql=parsed_sql,
    )
