import re
from typing import List

_ARRAY_PATTERNS = [
    re.compile(r"\bARRAY\s*\[", re.IGNORECASE),
    re.compile(r"\bARRAY\s*\(", re.IGNORECASE),
    re.compile(r"\bANY\s*\(", re.IGNORECASE),
    re.compile(r"::\s*\w*\[\]", re.IGNORECASE),
]

_JSONB_PATTERNS = [
    re.compile(r"::\s*jsonb\b", re.IGNORECASE),
    re.compile(r"->>\s*", re.IGNORECASE),
    re.compile(r"->\s*", re.IGNORECASE),
    re.compile(r"\bjsonb_set\s*\(", re.IGNORECASE),
    re.compile(r"\bjsonb_extract_path_text\s*\(", re.IGNORECASE),
    re.compile(r"\bjsonb_extract_path\s*\(", re.IGNORECASE),
    re.compile(r"\bto_jsonb\s*\(", re.IGNORECASE),
    re.compile(r"\bjsonb_build_object\s*\(", re.IGNORECASE),
]


def validate_redshift_query(sql: str) -> List[str]:
    """Return a list of Redshift incompatibility errors for a SQL string."""
    errors = []

    if any(pattern.search(sql) for pattern in _ARRAY_PATTERNS):
        errors.append(
            "This query uses ARRAY syntax or array operators, which are not supported on Redshift."
        )

    if any(pattern.search(sql) for pattern in _JSONB_PATTERNS):
        errors.append(
            "This query uses JSONB operators/functions (->, ->>, jsonb_*), "
            "which are not supported on Redshift."
        )

    return errors
