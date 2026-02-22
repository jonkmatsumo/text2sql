"""Shared fixtures for tenant enforcement policy tests."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from common.security.tenant_enforcement_policy import TenantEnforcementPolicy
from common.sql.tenant_sql_rewriter import (
    RewriteFailure,
    TenantRewriteFailureReason,
    TransformerErrorKind,
)


@pytest.fixture
def policy_factory() -> Callable[..., TenantEnforcementPolicy]:
    """Build policy instances with deterministic default settings."""

    def _factory(
        *,
        provider: str = "sqlite",
        mode: str = "sql_rewrite",
        strict: bool = True,
        max_targets: int = 25,
        max_params: int = 50,
        max_ast_nodes: int = 1000,
        hard_timeout_ms: int = 200,
        warn_ms: int = 50,
        rewrite_enabled: bool = True,
    ) -> TenantEnforcementPolicy:
        return TenantEnforcementPolicy(
            provider=provider,
            mode=mode,
            strict=strict,
            max_targets=max_targets,
            max_params=max_params,
            max_ast_nodes=max_ast_nodes,
            hard_timeout_ms=hard_timeout_ms,
            warn_ms=warn_ms,
            rewrite_enabled=rewrite_enabled,
        )

    return _factory


@pytest.fixture
def transformer_failure() -> Callable[..., RewriteFailure]:
    """Create bounded transformer failures for policy mapping tests."""

    def _factory(
        *,
        kind: TransformerErrorKind = TransformerErrorKind.PARAM_LIMIT_EXCEEDED,
        reason_code: TenantRewriteFailureReason = TenantRewriteFailureReason.PARAM_LIMIT_EXCEEDED,
        message: str = "Transformer failed.",
    ) -> RewriteFailure:
        return RewriteFailure(kind=kind, reason_code=reason_code, message=message)

    return _factory


@pytest.fixture
def example_sql() -> dict[str, str]:
    """Provide representative stable SQL samples shared across policy tests."""
    return {
        "safe_join": "SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id",
        "safe_simple": "SELECT * FROM orders",
        "unsupported_correlated": (
            "SELECT u.id FROM users u "
            "WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)"
        ),
        "not_required": "SELECT 1 AS ok",
    }
