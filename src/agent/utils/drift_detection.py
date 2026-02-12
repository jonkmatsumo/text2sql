"""AST-based schema drift detection utilities."""

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Set, Tuple

from agent.utils.sql_ast import extract_columns, extract_tables, parse_sql
from common.constants.reason_codes import DriftDetectionMethod
from dal.error_patterns import extract_missing_identifiers

logger = logging.getLogger(__name__)

_STRUCTURED_DRIFT_SQLSTATE_CODES = {
    "42P01",  # undefined_table (Postgres/Redshift)
    "42703",  # undefined_column (Postgres/Redshift)
    "3F000",  # invalid_schema_name (Postgres/Redshift)
    "42S02",  # table not found (MySQL/ODBC)
    "42S22",  # column not found (MySQL/ODBC)
}
_STRUCTURED_DRIFT_CODE_FRAGMENTS = (
    "UNDEFINED_TABLE",
    "UNDEFINED_COLUMN",
    "TABLE_NOT_FOUND",
    "COLUMN_NOT_FOUND",
    "OBJECT_DOES_NOT_EXIST",
    "NOT_FOUND",
    "MISSING_COLUMN",
    "MISSING_TABLE",
)
_SQLSTATE_IN_TEXT_RE = re.compile(r"(?:sqlstate|sql state)\s*[:=]?\s*([0-9A-Z]{5})", re.IGNORECASE)


@dataclass(frozen=True)
class DriftDetectionResult:
    """Detailed schema drift detection result."""

    missing_identifiers: List[str]
    method: DriftDetectionMethod
    source: str


def _iter_code_candidates(value: Any) -> list[str]:
    """Collect string code-like values from nested metadata structures."""
    candidates: list[str] = []

    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            candidates.append(stripped)
        return candidates

    if isinstance(value, Mapping):
        for key, nested_value in value.items():
            key_normalized = str(key).strip().lower()
            if key_normalized in {
                "code",
                "sql_state",
                "sqlstate",
                "reason",
                "reason_code",
                "error_code",
                "provider_code",
            }:
                candidates.extend(_iter_code_candidates(nested_value))
                continue

            # Some providers include relevant codes under nested maps in details.
            if key_normalized in {"details_safe", "details_debug", "details"}:
                candidates.extend(_iter_code_candidates(nested_value))

    if isinstance(value, list):
        for nested in value:
            candidates.extend(_iter_code_candidates(nested))

    return candidates


def _extract_structured_sqlstates(
    error_message: str, error_metadata: Mapping[str, Any] | None
) -> set[str]:
    """Extract SQLSTATE codes from structured metadata and explicit SQLSTATE text."""
    sqlstates: set[str] = set()

    if error_metadata:
        for key in ("code", "sql_state", "sqlstate"):
            candidate = error_metadata.get(key)
            if isinstance(candidate, str):
                normalized = candidate.strip().upper()
                if len(normalized) == 5 and normalized.isalnum():
                    sqlstates.add(normalized)

    for match in _SQLSTATE_IN_TEXT_RE.findall(error_message or ""):
        normalized = match.strip().upper()
        if len(normalized) == 5 and normalized.isalnum():
            sqlstates.add(normalized)

    return sqlstates


def _has_structured_drift_signal(
    error_message: str, error_metadata: Mapping[str, Any] | None
) -> bool:
    """Return True when metadata includes structured drift indicators."""
    sqlstates = _extract_structured_sqlstates(error_message, error_metadata)
    if sqlstates.intersection(_STRUCTURED_DRIFT_SQLSTATE_CODES):
        return True

    if not error_metadata:
        return False

    for raw_code in _iter_code_candidates(error_metadata):
        code_value = raw_code.strip().upper()
        if not code_value:
            continue
        if code_value in _STRUCTURED_DRIFT_SQLSTATE_CODES:
            return True
        if any(fragment in code_value for fragment in _STRUCTURED_DRIFT_CODE_FRAGMENTS):
            return True

    return False


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
    error_metadata: Dict[str, Any] | None = None,
) -> Tuple[List[str], DriftDetectionMethod]:
    """
    Detect missing identifiers (tables/columns) using AST analysis or regex fallback.

    Returns:
        Tuple of (list of missing identifiers, method used)
    """
    result = detect_schema_drift_details(
        sql=sql,
        error_message=error_message,
        provider=provider,
        raw_schema_context=raw_schema_context,
        error_metadata=error_metadata,
    )
    return result.missing_identifiers, result.method


def detect_schema_drift_details(
    sql: str,
    error_message: str,
    provider: str,
    raw_schema_context: List[Dict[str, Any]],
    error_metadata: Dict[str, Any] | None = None,
) -> DriftDetectionResult:
    """Detect schema drift with source details for telemetry."""
    missing_identifiers: List[str] = []
    method = DriftDetectionMethod.AST
    source = "regex"

    # Normalize inputs
    sql = (sql or "").strip()
    error_message = (error_message or "").strip()

    # 1. Best-effort AST analysis
    ast = None
    if sql:
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
                        # the column via AST alone
                        continue

                    for table_key in valid_referenced_tables:
                        if col_ref.lower() in schema_map[table_key]:
                            found = True
                            break

                    if not found:
                        missing_identifiers.append(col_ref)
        else:
            # SQL is present and parsed, but we have no local schema context to compare against.
            # We will rely entirely on regex in this case.
            pass

    # 2. If AST found nothing or parse failed, or if we want to be aggressive (Hybrid)
    # The error message is often the most authoritative source of what's EXACTLY missing.
    structured_signal = _has_structured_drift_signal(
        error_message=error_message,
        error_metadata=error_metadata if isinstance(error_metadata, dict) else None,
    )
    regex_identifiers = extract_missing_identifiers(provider, error_message)

    if not missing_identifiers and regex_identifiers:
        missing_identifiers = regex_identifiers
        method = DriftDetectionMethod.REGEX_FALLBACK if not ast else DriftDetectionMethod.HYBRID
    elif missing_identifiers and regex_identifiers:
        # Merge results for Hybrid approach
        # Regex identifiers are high-confidence (confirmed by DB)
        # AST identifiers are suspected (missing from our schema context)
        for ri in regex_identifiers:
            if ri not in missing_identifiers:
                missing_identifiers.append(ri)
        method = DriftDetectionMethod.HYBRID
    elif missing_identifiers and not regex_identifiers:
        # We suspect drift via AST, but DB error doesn't explicitly name the identifier
        # or we don't have a regex for it yet.
        method = DriftDetectionMethod.AST
    elif not ast:
        method = DriftDetectionMethod.REGEX_FALLBACK

    if structured_signal and missing_identifiers:
        source = "structured"

    return DriftDetectionResult(
        missing_identifiers=missing_identifiers,
        method=method,
        source=source,
    )
