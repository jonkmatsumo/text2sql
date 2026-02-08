"""SQL AST validation using sqlglot for security checks and metadata extraction.

This module provides:
- SQL parsing with dialect support (PostgreSQL default)
- Security validation (blocked tables, forbidden commands)
- Metadata extraction for audit logging (table lineage, column usage, join complexity)
- Structured error objects for the healer loop
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import sqlglot
from sqlglot import exp

from agent.utils.sql_ast import count_joins, extract_columns, extract_tables, normalize_sql
from common.config.env import get_env_int
from common.constants.reason_codes import ValidationRefusalReason
from common.sql.dialect import normalize_sqlglot_dialect


class ViolationType(str, Enum):
    """Types of security violations detected during AST validation."""

    RESTRICTED_TABLE = "restricted_table"
    FORBIDDEN_COMMAND = "forbidden_command"
    DANGEROUS_PATTERN = "dangerous_pattern"
    SYNTAX_ERROR = "syntax_error"
    COMPLEXITY_LIMIT = "complexity_limit"


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
    metadata: Optional[SQLMetadata] = None
    parsed_sql: Optional[str] = None  # Normalized/transpiled SQL

    def to_dict(self) -> dict:
        """Convert to dictionary for state storage."""
        return {
            "is_valid": self.is_valid,
            "violations": [v.to_dict() for v in self.violations],
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


def validate_security(ast: exp.Expression) -> list[SecurityViolation]:
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

    return violations


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


def validate_sql(sql: str, dialect: str = "postgres") -> ASTValidationResult:
    """
    Complete SQL validation: parse, security check, complexity check, and metadata extraction.

    This is the main entry point for AST validation.

    Args:
        sql: SQL query string
        dialect: SQL dialect (default: postgres)

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
    violations = validate_security(ast)

    # Validate complexity
    complexity_violations = validate_complexity(ast)
    violations.extend(complexity_violations)

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
        metadata=metadata,
        parsed_sql=parsed_sql,
    )
