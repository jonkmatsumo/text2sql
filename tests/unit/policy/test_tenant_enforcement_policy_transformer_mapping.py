"""Policy mapping tests for pure tenant SQL transformer errors."""

from unittest.mock import patch

import pytest

from common.sql.tenant_sql_rewriter import (
    RewriteFailure,
    RewriteRequest,
    TenantRewriteFailureReason,
    TransformerErrorKind,
    transform_tenant_scoped_sql,
)


@pytest.mark.asyncio
async def test_policy_maps_transformer_param_limit_to_rejected_limit(
    policy_factory,
    transformer_failure,
    example_sql,
):
    """Transformer param limit should map to REJECTED_LIMIT with bounded reason code."""
    policy = policy_factory()

    with patch(
        "common.sql.tenant_sql_rewriter.transform_tenant_scoped_sql",
        return_value=transformer_failure(
            kind=TransformerErrorKind.SUBQUERY_UNSUPPORTED,
            reason_code=TenantRewriteFailureReason.PARAM_LIMIT_EXCEEDED,
            message="Exceeded parameter budget.",
        ),
    ) as mock_transform:
        decision = await policy.evaluate(
            sql=example_sql["safe_join"],
            tenant_id=7,
            params=[],
            tenant_column="tenant_id",
            global_table_allowlist=set(),
            schema_snapshot_loader=None,
        )

    mock_transform.assert_called_once()
    rewrite_request = mock_transform.call_args.args[0]
    assert isinstance(rewrite_request, RewriteRequest)
    assert rewrite_request.provider == "sqlite"
    assert rewrite_request.tenant_id == 7
    assert rewrite_request.max_params == 50

    assert decision.should_execute is False
    assert decision.result.outcome == "REJECTED_LIMIT"
    assert decision.result.reason_code == "PARAM_LIMIT_EXCEEDED"
    assert decision.bounded_reason_code == "tenant_rewrite_param_limit_exceeded"
    assert decision.envelope_metadata["tenant_rewrite_outcome"] == "REJECTED_LIMIT"
    assert (
        decision.envelope_metadata["tenant_rewrite_reason_code"]
        == "tenant_rewrite_param_limit_exceeded"
    )


def test_transformer_rejects_invalid_request_shape_with_bounded_failure_kind():
    """Invalid request payloads should return bounded INVALID_REQUEST failures."""
    failure = transform_tenant_scoped_sql(
        RewriteRequest(
            sql="SELECT * FROM orders",
            provider="sqlite",
            tenant_id=None,  # type: ignore[arg-type]
        )
    )
    assert isinstance(failure, RewriteFailure)
    assert failure.kind == TransformerErrorKind.INVALID_REQUEST
    assert failure.reason_code == TenantRewriteFailureReason.UNSUPPORTED_SHAPE
