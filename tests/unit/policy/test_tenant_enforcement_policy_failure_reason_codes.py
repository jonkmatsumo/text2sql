"""Policy mapping tests for bounded tenant rewrite failure reasons."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from common.sql.tenant_sql_rewriter import (
    RewriteFailure,
    TenantRewriteFailureReason,
    TransformerErrorKind,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_reason", "expected_outcome", "expected_policy_reason", "should_execute"),
    [
        (
            TenantRewriteFailureReason.UNSUPPORTED_SHAPE,
            "REJECTED_UNSUPPORTED",
            "SUBQUERY_UNSUPPORTED",
            False,
        ),
        (
            TenantRewriteFailureReason.MISSING_TENANT_COLUMN,
            "REJECTED_UNSUPPORTED",
            "MISSING_TENANT_COLUMN",
            False,
        ),
        (
            TenantRewriteFailureReason.TARGET_LIMIT_EXCEEDED,
            "REJECTED_LIMIT",
            "TARGET_LIMIT_EXCEEDED",
            False,
        ),
        (
            TenantRewriteFailureReason.PARAM_LIMIT_EXCEEDED,
            "REJECTED_LIMIT",
            "PARAM_LIMIT_EXCEEDED",
            False,
        ),
        (
            TenantRewriteFailureReason.AST_COMPLEXITY_EXCEEDED,
            "REJECTED_LIMIT",
            "AST_COMPLEXITY_EXCEEDED",
            False,
        ),
        (
            TenantRewriteFailureReason.COMPLETENESS_FAILED,
            "REJECTED_UNSUPPORTED",
            "COMPLETENESS_FAILED",
            False,
        ),
        (
            TenantRewriteFailureReason.DIALECT_UNSUPPORTED,
            "REJECTED_UNSUPPORTED",
            "PROVIDER_UNSUPPORTED",
            False,
        ),
        (
            TenantRewriteFailureReason.PARSE_FAILED,
            "REJECTED_UNSUPPORTED",
            "PARSE_ERROR",
            False,
        ),
        (
            TenantRewriteFailureReason.NO_PREDICATES_PRODUCED,
            "SKIPPED_NOT_REQUIRED",
            None,
            True,
        ),
    ],
)
async def test_policy_maps_bounded_transformer_failure_reason_to_stable_outcome(
    policy_factory,
    example_sql,
    failure_reason: TenantRewriteFailureReason,
    expected_outcome: str,
    expected_policy_reason: str | None,
    should_execute: bool,
) -> None:
    """Each rewrite failure reason should map deterministically to one policy result."""
    policy = policy_factory()
    with patch(
        "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
        return_value=RewriteFailure(
            kind=TransformerErrorKind.SUBQUERY_UNSUPPORTED,
            reason_code=failure_reason,
            message="synthetic test failure",
        ),
    ):
        decision = await policy.evaluate(
            sql=example_sql["safe_join"],
            tenant_id=7,
            params=[],
            tenant_column="tenant_id",
            global_table_allowlist=set(),
            schema_snapshot_loader=None,
        )

    assert decision.should_execute is should_execute
    assert decision.result.outcome == expected_outcome
    assert decision.result.reason_code == expected_policy_reason
    assert decision.telemetry_attributes["tenant_rewrite.failure_reason_category"] == (
        f"tenant_rewrite_failure_{failure_reason.value.lower()}"
    )
