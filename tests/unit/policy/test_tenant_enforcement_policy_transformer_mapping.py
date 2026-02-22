"""Policy mapping tests for pure tenant SQL transformer errors."""

from unittest.mock import patch

import pytest

from common.security.tenant_enforcement_policy import TenantEnforcementPolicy
from common.sql.tenant_sql_rewriter import TenantSQLTransformerError, TransformerErrorKind


@pytest.mark.asyncio
async def test_policy_maps_transformer_param_limit_to_rejected_limit():
    """Transformer param limit should map to REJECTED_LIMIT with bounded reason code."""
    policy = TenantEnforcementPolicy(
        provider="sqlite",
        mode="sql_rewrite",
        strict=True,
        max_targets=25,
        max_params=50,
        max_ast_nodes=1000,
        hard_timeout_ms=200,
        warn_ms=50,
    )

    with patch(
        "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
        side_effect=TenantSQLTransformerError(
            TransformerErrorKind.PARAM_LIMIT_EXCEEDED,
            "Exceeded parameter budget.",
        ),
    ):
        decision = await policy.evaluate(
            sql="SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id",
            tenant_id=7,
            params=[],
            tenant_column="tenant_id",
            global_table_allowlist=set(),
            schema_snapshot_loader=None,
        )

    assert decision.should_execute is False
    assert decision.result.outcome == "REJECTED_LIMIT"
    assert decision.result.reason_code == "PARAM_LIMIT_EXCEEDED"
    assert decision.bounded_reason_code == "tenant_rewrite_param_limit_exceeded"
    assert decision.envelope_metadata["tenant_rewrite_outcome"] == "REJECTED_LIMIT"
    assert (
        decision.envelope_metadata["tenant_rewrite_reason_code"]
        == "tenant_rewrite_param_limit_exceeded"
    )
