"""AST-based schema drift detection utilities."""

import logging
from typing import Any, Dict, List, Set, Tuple

from agent.utils.sql_ast import extract_columns, extract_tables, parse_sql
from common.constants.reason_codes import DriftDetectionMethod
from dal.error_patterns import extract_missing_identifiers

logger = logging.getLogger(__name__)


def _build_schema_set(raw_schema_context: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    """Build a map of table name to set of column names from raw schema context."""
    schema_map: Dict[str, Set[str]] = {}
    if not raw_schema_context:
        return schema_map

    for node in raw_schema_context:
        if not isinstance(node, dict):
            continue
        if node.get("type") == "Table":
            name = node.get("name")
            if name:
                schema_map.setdefault(str(name).lower(), set())

    for node in raw_schema_context:
        if not isinstance(node, dict):
            continue
        if node.get("type") != "Column":
            continue
        table = node.get("table")
        name = node.get("name")
        if table and name:
            table_key = str(table).lower()
            if table_key in schema_map:
                schema_map[table_key].add(str(name).lower())

    return schema_map


def detect_schema_drift(
    sql: str,
    error_message: str,
    provider: str,
    raw_schema_context: List[Dict[str, Any]],
) -> Tuple[List[str], DriftDetectionMethod]:
    """
    Detect missing identifiers (tables/columns) using AST analysis or regex fallback.

    Returns:
        Tuple of (list of missing identifiers, method used)
    """
    missing_identifiers: List[str] = []
    method = DriftDetectionMethod.AST

    # 1. Best-effort AST analysis
    ast = parse_sql(sql)
    if ast:
        schema_map = _build_schema_set(raw_schema_context)
        if schema_map:
            # Check tables
            referenced_tables = extract_tables(ast)
            missing_tables = []
            for table in referenced_tables:
                if table.lower() not in schema_map:
                    missing_identifiers.append(table)
                    missing_tables.append(table.lower())

            # Check columns
            referenced_columns = extract_columns(ast)
            for col_ref in referenced_columns:
                if "." in col_ref:
                    table, col = col_ref.rsplit(".", 1)
                    table_key = table.lower()
                    if table_key in schema_map:
                        if col.lower() not in schema_map[table_key]:
                            missing_identifiers.append(col_ref)
                    elif table_key not in missing_tables:
                        # Table itself is missing (already added to missing_identifiers)
                        pass
                else:
                    # Unqualified column
                    # Check if it exists in ANY of the tables referenced in the query
                    found = False
                    # Only check tables that we actually have schema for
                    valid_referenced_tables = [
                        t.lower() for t in referenced_tables if t.lower() in schema_map
                    ]

                    if not valid_referenced_tables:
                        # If no valid tables found in schema, we can't really validate
                        # the column via AST
                        continue

                    for table_key in valid_referenced_tables:
                        if col_ref.lower() in schema_map[table_key]:
                            found = True
                            break

                    if not found:
                        missing_identifiers.append(col_ref)

    # 2. If AST found nothing or parse failed, or if we want to be aggressive (Hybrid)
    # The error message is often the most authoritative source of what's EXACTLY missing.
    regex_identifiers = extract_missing_identifiers(provider, error_message)

    if not missing_identifiers and regex_identifiers:
        missing_identifiers = regex_identifiers
        method = DriftDetectionMethod.REGEX_FALLBACK if not ast else DriftDetectionMethod.HYBRID
    elif missing_identifiers and regex_identifiers:
        # Merge results for Hybrid approach
        for ri in regex_identifiers:
            if ri not in missing_identifiers:
                missing_identifiers.append(ri)
        method = DriftDetectionMethod.HYBRID
    elif not ast:
        method = DriftDetectionMethod.REGEX_FALLBACK

    return missing_identifiers, method
