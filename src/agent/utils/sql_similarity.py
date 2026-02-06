"""Utility for computing structural similarity between SQL queries."""

import logging
from typing import Set

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


def _extract_tables(expression: exp.Expression) -> Set[str]:
    """Extract table names from AST."""
    tables = set()
    for table in expression.find_all(exp.Table):
        tables.add(table.name.lower())
    return tables


def _extract_columns(expression: exp.Expression) -> Set[str]:
    """Extract column names from AST."""
    columns = set()
    for col in expression.find_all(exp.Column):
        columns.add(col.name.lower())
    return columns


def compute_sql_similarity(sql1: str, sql2: str) -> float:
    """
    Compute structural similarity between two SQL queries (0.0 to 1.0).

    Uses Jaccard similarity of tables (primary) and columns (secondary).
    Returns 0.0 if parsing fails for either.
    """
    if not sql1 or not sql2:
        return 0.0

    if sql1.strip() == sql2.strip():
        return 1.0

    try:
        ast1 = sqlglot.parse_one(sql1)
        ast2 = sqlglot.parse_one(sql2)
    except Exception:
        # Fallback to 0.0 on parse error to fail safe (or maybe 1.0 to fail open?
        # Request said "treat parse failure as 0 similarity unless same raw string")
        return 0.0

    tables1 = _extract_tables(ast1)
    tables2 = _extract_tables(ast2)

    if not tables1 and not tables2:
        # No tables in either? Logic check.
        # If both are 'SELECT 1', they are similar.
        pass

    # Table Jaccard
    u_tables = tables1.union(tables2)
    if not u_tables:
        table_sim = 1.0
    else:
        table_sim = len(tables1.intersection(tables2)) / len(u_tables)

    # Column Jaccard
    cols1 = _extract_columns(ast1)
    cols2 = _extract_columns(ast2)

    u_cols = cols1.union(cols2)
    if not u_cols:
        col_sim = 1.0
    else:
        col_sim = len(cols1.intersection(cols2)) / len(u_cols)

    # Weighted score: Tables are more important for "structural drift"
    score = (0.7 * table_sim) + (0.3 * col_sim)
    return score
