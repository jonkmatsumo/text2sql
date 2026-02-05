"""Provider-specific error patterns for schema drift detection."""

from __future__ import annotations

import re

_IDENTIFIER_GROUP = "name"

_GENERIC_PATTERNS = [
    re.compile(r'relation "(?P<name>[^"]+)" does not exist', re.IGNORECASE),
    re.compile(r'table "(?P<name>[^"]+)" does not exist', re.IGNORECASE),
    re.compile(r'column "(?P<name>[^"]+)" does not exist', re.IGNORECASE),
    re.compile(r"no such table: (?P<name>[\w\.]+)", re.IGNORECASE),
    re.compile(r"no such column: (?P<name>[\w\.]+)", re.IGNORECASE),
]

_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    "postgres": [
        re.compile(r'relation "(?P<name>[^"]+)" does not exist', re.IGNORECASE),
        re.compile(r'table "(?P<name>[^"]+)" does not exist', re.IGNORECASE),
        re.compile(r'column "(?P<name>[^"]+)" does not exist', re.IGNORECASE),
    ],
    "redshift": [
        re.compile(r'relation "(?P<name>[^"]+)" does not exist', re.IGNORECASE),
        re.compile(r'table "(?P<name>[^"]+)" does not exist', re.IGNORECASE),
        re.compile(r'column "(?P<name>[^"]+)" does not exist', re.IGNORECASE),
    ],
    "sqlite": [
        re.compile(r"no such table: (?P<name>[\w\.]+)", re.IGNORECASE),
        re.compile(r"no such column: (?P<name>[\w\.]+)", re.IGNORECASE),
    ],
    "bigquery": [
        re.compile(r"Not found: Table (?P<name>[\w\-\.\:]+)", re.IGNORECASE),
        re.compile(r"Not found: Dataset (?P<name>[\w\-\.\:]+)", re.IGNORECASE),
        re.compile(r"Unrecognized name: (?P<name>[\w\.\-]+)", re.IGNORECASE),
    ],
    "snowflake": [
        re.compile(
            r"Object '(?P<name>[^']+)' does not exist(?! or not authorized)",
            re.IGNORECASE,
        ),
        re.compile(r"SQL compilation error: Unknown column '(?P<name>[^']+)'", re.IGNORECASE),
    ],
    "databricks": [
        re.compile(r"Table or view not found: (?P<name>[\w\.\-]+)", re.IGNORECASE),
        re.compile(r"cannot resolve '`?(?P<name>[^`']+)`?'", re.IGNORECASE),
    ],
    "spark": [
        re.compile(r"Table or view not found: (?P<name>[\w\.\-]+)", re.IGNORECASE),
        re.compile(r"cannot resolve '`?(?P<name>[^`']+)`?'", re.IGNORECASE),
    ],
    "athena": [
        re.compile(r"Table '(?P<name>[^']+)' does not exist", re.IGNORECASE),
        re.compile(r"Column '(?P<name>[^']+)' cannot be resolved", re.IGNORECASE),
    ],
    "presto": [
        re.compile(r"Table '(?P<name>[^']+)' does not exist", re.IGNORECASE),
        re.compile(r"Column '(?P<name>[^']+)' cannot be resolved", re.IGNORECASE),
    ],
    "trino": [
        re.compile(r"Table '(?P<name>[^']+)' does not exist", re.IGNORECASE),
        re.compile(r"Column '(?P<name>[^']+)' cannot be resolved", re.IGNORECASE),
    ],
    "clickhouse": [
        re.compile(
            r"DB::Exception: Table (?P<name>[\w\.]+) doesn't exist",
            re.IGNORECASE,
        ),
        re.compile(r"Unknown column: (?P<name>[\w\.]+)", re.IGNORECASE),
    ],
}


def get_schema_drift_patterns(provider: str) -> list[re.Pattern[str]]:
    """Return provider-specific regex patterns for missing identifiers."""
    normalized = (provider or "").lower()
    return _PATTERNS.get(normalized, _GENERIC_PATTERNS)


def extract_missing_identifiers(provider: str, error_message: str) -> list[str]:
    """Extract missing identifiers from an error message for a provider."""
    if not error_message:
        return []
    patterns = get_schema_drift_patterns(provider)
    identifiers: list[str] = []
    for pattern in patterns:
        for match in pattern.finditer(error_message):
            name = match.groupdict().get(_IDENTIFIER_GROUP)
            if name and name not in identifiers:
                identifiers.append(name)
    return identifiers
