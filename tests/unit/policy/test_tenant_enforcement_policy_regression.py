"""Integrated regression lock for tenant enforcement policy behavior."""

from __future__ import annotations

import pytest
import sqlglot

from common.security.tenant_enforcement_policy import TenantEnforcementPolicy, TenantSQLShape
from common.sql.tenant_sql_rewriter import load_tenant_rewrite_settings


def _normalize_sql(sql: str) -> str:
    return sqlglot.parse_one(sql, read="sqlite").sql(dialect="sqlite", pretty=False)


async def _schema_snapshot_loader(table_names: list[str], tenant_id: int) -> dict[str, set[str]]:
    del tenant_id
    known_columns = {
        "orders": {"id", "customer_id", "tenant_id", "status"},
        "customers": {"id", "tenant_id", "name", "is_active"},
    }
    snapshot: dict[str, set[str]] = {}
    for table_name in table_names:
        normalized = (table_name or "").strip().lower()
        if not normalized:
            continue
        short_name = normalized.split(".")[-1]
        columns = known_columns.get(normalized) or known_columns.get(short_name) or {"tenant_id"}
        snapshot[normalized] = set(columns)
        snapshot[short_name] = set(columns)
    return snapshot


@pytest.mark.asyncio
async def test_tenant_enforcement_policy_regression_sql_rewrite_cte_join(monkeypatch):
    """Lock integrated policy behavior for a production-like CTE + JOIN query."""
    monkeypatch.setenv("TENANT_REWRITE_STRICT_MODE", "true")
    monkeypatch.setenv("TENANT_REWRITE_MAX_TARGETS", "25")
    monkeypatch.setenv("TENANT_REWRITE_MAX_PARAMS", "50")
    monkeypatch.setenv("MAX_SQL_AST_NODES", "1000")
    monkeypatch.setenv("TENANT_REWRITE_WARN_MS", "50")
    monkeypatch.setenv("TENANT_REWRITE_HARD_TIMEOUT_MS", "200")

    settings = load_tenant_rewrite_settings()
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=settings.strict_mode,
        max_targets=settings.max_targets,
        max_params=settings.max_params,
        max_ast_nodes=settings.max_ast_nodes,
        hard_timeout_ms=settings.hard_timeout_ms,
        warn_ms=settings.warn_ms,
    )

    sql = """
    WITH scoped_orders AS (
        SELECT o.id, o.customer_id, o.tenant_id
        FROM orders o
        WHERE o.status = 'open'
    )
    SELECT so.id, c.name
    FROM scoped_orders so
    JOIN customers c ON c.id = so.customer_id
    WHERE c.is_active = 1
    """

    shape = policy.classify_sql(sql, provider="sqlite")
    assert shape == TenantSQLShape.SAFE_CTE_QUERY

    decision_one = await policy.evaluate(
        sql=sql,
        tenant_id=7,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        schema_snapshot_loader=_schema_snapshot_loader,
    )
    decision_two = await policy.evaluate(
        sql=sql,
        tenant_id=7,
        params=[],
        tenant_column="tenant_id",
        global_table_allowlist=set(),
        schema_snapshot_loader=_schema_snapshot_loader,
    )

    assert decision_one.should_execute is True
    assert decision_one.result.mode == "sql_rewrite"
    assert decision_one.result.outcome == "APPLIED"
    assert decision_one.result.applied is True
    assert decision_one.result.reason_code is None

    metadata = decision_one.envelope_metadata
    assert metadata["tenant_enforcement_applied"] is True
    assert metadata["tenant_enforcement_mode"] == "sql_rewrite"
    assert metadata["tenant_rewrite_outcome"] == "APPLIED"
    assert metadata.get("tenant_rewrite_reason_code") is None

    telemetry = decision_one.telemetry_attributes
    assert telemetry["tenant.enforcement.mode"] == "sql_rewrite"
    assert telemetry["tenant.enforcement.outcome"] == "APPLIED"
    assert telemetry["tenant.enforcement.applied"] is True
    assert telemetry["rewrite.target_count"] >= 1
    assert telemetry["rewrite.param_count"] >= 1
    assert isinstance(telemetry["rewrite.duration_ms"], float)
    assert telemetry["rewrite.duration_ms"] >= 0
    assert isinstance(telemetry["rewrite.duration_warn_exceeded"], bool)

    normalized_sql = _normalize_sql(decision_one.sql_to_execute).lower()
    assert "join" in normalized_sql
    assert "tenant_id = ?" in normalized_sql
    assert decision_one.params_to_bind

    assert decision_two.result == decision_one.result
    assert decision_two.envelope_metadata == decision_one.envelope_metadata
    assert decision_two.metric_attributes == decision_one.metric_attributes
    assert decision_two.params_to_bind == decision_one.params_to_bind
    assert _normalize_sql(decision_two.sql_to_execute) == _normalize_sql(
        decision_one.sql_to_execute
    )
