"""Authoritative provider/mode matrix for tenant enforcement policy conformance tests."""

from __future__ import annotations

TENANT_ENFORCEMENT_PROVIDER_MODE_MATRIX: dict[str, str] = {
    "duckdb": "sql_rewrite",
    "postgres": "rls_session",
    "sqlite": "sql_rewrite",
}


def tenant_enforcement_provider_mode_rows() -> list[tuple[str, str]]:
    """Return sorted provider/mode rows for parametrized conformance tests."""
    return sorted(TENANT_ENFORCEMENT_PROVIDER_MODE_MATRIX.items())
