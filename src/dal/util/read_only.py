"""Read-only SQL enforcement helpers for query-target connections."""

import re

import sqlglot
from opentelemetry import trace
from sqlglot import exp

from common.models.error_metadata import ErrorCategory
from common.policy.sql_policy import ALLOWED_STATEMENT_TYPES
from common.sql.dialect import normalize_sqlglot_dialect

_FALLBACK_MUTATION_PREFIX = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "MERGE",
    "GRANT",
    "REVOKE",
}
_SQL_COMMENT_RE = re.compile(r"(--[^\n]*|/\*.*?\*/)", flags=re.DOTALL)
_MUTATION_KEYWORD_RE = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|CALL)\b",
    flags=re.IGNORECASE,
)


def is_mutating_sql(sql: str, provider: str) -> bool:
    """Best-effort detection of mutating SQL statements."""
    if not isinstance(sql, str) or not sql.strip():
        return True

    dialect = normalize_sqlglot_dialect(provider)
    # Strip comments to ensure consistent parsing behavior (aligns with AST validator)
    stripped = _SQL_COMMENT_RE.sub(" ", sql).strip()
    try:
        expressions = sqlglot.parse(stripped, read=dialect)
    except Exception:
        expressions = None

    if expressions:
        if len(expressions) != 1:
            return True
        expression = expressions[0]
        if expression is None:
            return True
        if expression.key not in ALLOWED_STATEMENT_TYPES:
            return True

        forbidden_nodes = (
            exp.Insert,
            exp.Update,
            exp.Delete,
            exp.Drop,
            exp.Alter,
            exp.Create,
            exp.Command,
            exp.Grant,
            exp.Merge,
            exp.TruncateTable,
        )
        for node in expression.walk():
            if isinstance(node, forbidden_nodes):
                return True
        return False

    # Fallback lexical guard for parser failures.
    stripped = _SQL_COMMENT_RE.sub(" ", sql).lstrip()
    if not stripped:
        return True
    first_token = stripped.split(maxsplit=1)[0].upper()
    return first_token in _FALLBACK_MUTATION_PREFIX


def validate_no_mutation_keywords(sql: str) -> None:
    """Reject SQL text that contains mutation keywords (comments ignored)."""
    if not isinstance(sql, str):
        raise PermissionError("Read-only enforcement blocked non-string SQL payload.")

    stripped = _SQL_COMMENT_RE.sub("", sql)
    match = _MUTATION_KEYWORD_RE.search(stripped)
    if not match:
        return

    blocked_keyword = match.group(0).upper()
    raise PermissionError(
        "Read-only enforcement blocked mutation keyword " f"'{blocked_keyword}' in rendered SQL."
    )


def enforce_read_only_sql(sql: str, provider: str, read_only: bool) -> None:
    """Raise PermissionError when a mutating SQL is issued on read-only connection."""
    if not read_only:
        return
    if is_mutating_sql(sql, provider):
        from agent.audit import AuditEventSource, AuditEventType, emit_audit_event

        emit_audit_event(
            AuditEventType.READONLY_VIOLATION,
            source=AuditEventSource.DAL,
            error_category=ErrorCategory.INVALID_REQUEST,
            metadata={
                "provider": provider,
                "read_only": bool(read_only),
                "reason_code": "mutating_sql_blocked",
                "decision": "reject",
            },
        )

        # Emit telemetry for blocked mutation
        span = trace.get_current_span()
        if span and span.is_recording():
            # Best-effort extraction of statement type for telemetry
            try:
                stripped = _SQL_COMMENT_RE.sub(" ", sql).lstrip()
                statement_type = stripped.split(maxsplit=1)[0].upper() if stripped else "UNKNOWN"
            except Exception:
                statement_type = "UNKNOWN"

            span.add_event(
                "dal.read_only.blocked",
                attributes={
                    "provider": provider,
                    "category": ErrorCategory.MUTATION_BLOCKED.value,
                    "statement_type": statement_type,
                },
            )
        raise PermissionError(
            f"Read-only enforcement blocked non-SELECT statement for provider '{provider}'."
        )
