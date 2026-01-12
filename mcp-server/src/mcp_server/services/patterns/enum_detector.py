import re
from dataclasses import dataclass
from typing import List, Optional

from mcp_server.models import ColumnDef


@dataclass
class EnumColumnSpec:
    """Specification for an enum-like column."""

    table: str
    column: str
    source: str  # "native_enum" or "low_cardinality_scan"
    values: List[str]


class EnumLikeColumnDetector:
    """Detects enum-like columns based on metadata, rules, and cardinality."""

    def __init__(
        self,
        allowlist: Optional[List[str]] = None,
        denylist: Optional[List[str]] = None,
        threshold: int = 10,
    ):
        """Initialize detector with configuration."""
        self.allowlist = self._parse_patterns(allowlist or [])
        self.denylist = self._parse_patterns(denylist or [])
        self.threshold = threshold

        # PII / High Churn identifiers
        # Based on specs: email, phone, ssn, etc.
        self.pii_tokens = {
            "email",
            "e-mail",
            "phone",
            "mobile",
            "ssn",
            "social",
            "address",
            "street",
            "zip",
            "postal",
            "dob",
            "birth",
            "first_name",
            "last_name",
            "full_name",
            "name",
            "ip",
            "device",
            "imei",
            "advertising_id",
            "gaid",
            "idfa",
            "token",
            "secret",
            "password",
        }

        # Excluded types
        self.excluded_types = {"uuid", "json", "jsonb", "blob", "bytea", "binary"}

    def _parse_patterns(self, patterns: List[str]) -> List[re.Pattern]:
        """Convert wildcard patterns (e.g. table.*) to regex."""
        compiled = []
        for p in patterns:
            # Escape regex chars, then replace wildcard * with .*
            # pattern: 'schema.table.col' -> ^schema\.table\.col$
            # pattern: '*.status' -> ^.*\.status$
            safe_p = re.escape(p)
            # Python < 3.12 disallows backslash in f-string expressions
            replaced = safe_p.replace(r"\*", ".*")
            regex_str = f"^{replaced}$"
            try:
                compiled.append(re.compile(regex_str, re.IGNORECASE))
            except re.error:
                pass  # logging.warning?
        return compiled

    def _matches_any(self, table: str, column: str, patterns: List[re.Pattern]) -> bool:
        """Check if table.column matches any pattern."""
        target = f"{table}.{column}"
        return any(p.match(target) for p in patterns)

    def is_pii_or_excluded_by_heuristic(self, column: ColumnDef) -> bool:
        """Check PII, ID, and type exclusions."""
        name_lower = column.name.lower()

        # 1. Type Exclusion
        # Check broad matching for data_type (e.g. contains 'json')
        # But instructions say: "uuid", "json/jsonb", "blob", or long free text.
        dtype = column.data_type.lower()
        if dtype in self.excluded_types or "json" in dtype:
            return True

        # 2. ID Exclusion
        # Primary key check (not passed in ColumnDef usually, but name heuristic works)
        # "ends with _id"
        if name_lower == "id" or name_lower.endswith("_id"):
            return True

        # 3. PII Name Matching
        # Split by underscore and check tokens
        tokens = set(name_lower.split("_"))
        # Check intersection
        if not self.pii_tokens.isdisjoint(tokens):
            return True

        return False

    def is_candidate(self, table: str, column: ColumnDef) -> bool:
        """Determine if a column is a candidate for enum detection.

        Precedence:
        1. Explicitly ALLOWED -> True (overrides PII/ID checks)
        2. Explicitly DENIED -> False
        3. PII/ID/Type Heuristics -> False
        4. Default -> True (subject to cardinality check later)
        """
        # Allowlist overrides everything
        if self._matches_any(table, column.name, self.allowlist):
            return True

        # Denylist
        if self._matches_any(table, column.name, self.denylist):
            return False

        # Heuristics
        if self.is_pii_or_excluded_by_heuristic(column):
            return False

        return True

    def canonicalize_values(self, values: List[str]) -> List[str]:
        """Clean, deduplicate, and sort values."""
        clean = set()
        for v in values:
            if v is None:
                continue
            # Handle potential non-string
            s = str(v).strip()
            if s:
                clean.add(s)
        return sorted(list(clean))
