"""Shared SQL AST utilities using sqlglot."""

from typing import Optional, Set

import sqlglot
from sqlglot import exp


def parse_sql(sql: str, dialect: str = "postgres") -> Optional[exp.Expression]:
    """Parse SQL string into AST expression.

    Returns None if parsing fails.
    """
    try:
        expressions = sqlglot.parse(sql, dialect=dialect)
        if not expressions:
            return None
        return expressions[0]
    except Exception:
        return None


def extract_tables(ast: exp.Expression) -> Set[str]:
    """Extract fully qualified table names from AST.

    Handles catalog.db.table, db.table, and table.
    """
    tables = set()
    for table in ast.find_all(exp.Table):
        parts = []
        if table.catalog:
            parts.append(table.catalog)
        if table.db:
            parts.append(table.db)
        if table.this:
            # table.this is the table name (as an Identifier or string)
            parts.append(table.name)

        if parts:
            tables.add(".".join(parts))
    return tables


def extract_columns(ast: exp.Expression) -> Set[str]:
    """Extract qualified column names from AST.

    Handles table.column and column.
    """
    columns = set()
    for column in ast.find_all(exp.Column):
        parts = []
        if column.table:
            parts.append(column.table)
        if column.this:
            parts.append(column.name)

        if parts:
            columns.add(".".join(parts))
    return columns


def count_joins(ast: exp.Expression) -> int:
    """Count number of JOIN clauses in AST."""
    return len(list(ast.find_all(exp.Join)))


def normalize_sql(ast: exp.Expression, dialect: str = "postgres") -> str:
    """Generate normalized SQL string from AST."""
    try:
        return ast.sql(dialect=dialect)
    except Exception:
        return ""
