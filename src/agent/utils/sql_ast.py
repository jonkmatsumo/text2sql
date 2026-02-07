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
    """Extract fully qualified table names from AST."""
    tables = set()
    for table in ast.find_all(exp.Table):
        if table.name:
            table_name = table.name
            if table.db:
                table_name = f"{table.db}.{table_name}"
            tables.add(table_name)
    return tables


def extract_columns(ast: exp.Expression) -> Set[str]:
    """Extract qualified column names from AST."""
    columns = set()
    for column in ast.find_all(exp.Column):
        if column.name:
            col_ref = column.name
            if column.table:
                col_ref = f"{column.table}.{col_ref}"
            columns.add(col_ref)
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
