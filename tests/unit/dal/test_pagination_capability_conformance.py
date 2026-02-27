"""Conformance harness for pagination capability gating."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import patch

import pytest

from dal.capabilities import BackendCapabilities, capabilities_for_provider
from dal.util.env import PROVIDER_ALIASES
from mcp_server.tools.execute_sql_query import handler

_KNOWN_PROVIDERS = sorted({value for value in PROVIDER_ALIASES.values() if value != "memgraph"})
_FEDERATED_PROVIDER_ORDERING_MATRIX: dict[str, bool] = {}


@pytest.mark.parametrize(
    "provider, expected_server, expected_wrapper, expected_keyset, "
    "expected_keyset_containment, expected_topology, expected_federated_ordering",
    [
        ("postgres", False, True, True, True, "single_backend", False),
        ("sqlite", False, True, True, True, "single_backend", False),
        ("duckdb", False, True, True, True, "single_backend", False),
        ("bigquery", False, False, False, False, "single_backend", False),
    ],
)
def test_pagination_capability_matrix(
    provider: str,
    expected_server: bool,
    expected_wrapper: bool,
    expected_keyset: bool,
    expected_keyset_containment: bool,
    expected_topology: str,
    expected_federated_ordering: bool,
):
    """Provider capability matrix should expose deterministic pagination support flags."""
    caps = capabilities_for_provider(provider)
    assert bool(caps.supports_pagination) is expected_server
    assert bool(caps.supports_offset_pagination_wrapper) is expected_wrapper
    assert bool(caps.supports_keyset) is expected_keyset
    assert bool(caps.supports_keyset_with_containment) is expected_keyset_containment
    assert caps.execution_topology == expected_topology
    assert caps.supports_federated_deterministic_ordering is expected_federated_ordering


def test_unknown_provider_pagination_capability_defaults_fail_closed():
    """Unknown providers should not advertise pagination wrapper support."""
    caps = capabilities_for_provider("unknown-provider")
    assert caps.supports_pagination is False
    assert caps.supports_offset_pagination_wrapper is False
    assert caps.supports_query_wrapping_subselect is False
    assert caps.supports_keyset is False
    assert caps.supports_keyset_with_containment is False
    assert caps.execution_topology == "single_backend"
    assert caps.supports_federated_deterministic_ordering is False


def test_federated_topology_defaults_to_ordering_unsupported():
    """Federated capabilities should fail closed unless ordering support is explicit."""
    caps = BackendCapabilities(provider_name="federated-proxy", execution_topology="federated")
    assert caps.supports_federated_deterministic_ordering is False


def test_federated_topology_requires_explicit_conformance_declaration():
    """Any provider marked federated must declare deterministic-ordering support in matrix."""
    discovered_federated: dict[str, bool] = {}
    for provider in _KNOWN_PROVIDERS:
        caps = capabilities_for_provider(provider)
        if caps.execution_topology == "federated":
            discovered_federated[provider] = bool(caps.supports_federated_deterministic_ordering)

    assert discovered_federated == _FEDERATED_PROVIDER_ORDERING_MATRIX


@pytest.mark.asyncio
async def test_pagination_request_unsupported_provider_fails_closed():
    """Unsupported pagination capability should fail closed with stable metadata."""
    caps = replace(
        capabilities_for_provider("postgres"),
        supports_pagination=False,
        supports_offset_pagination_wrapper=False,
        supports_query_wrapping_subselect=False,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler("SELECT 1 AS id", tenant_id=1, page_size=10)

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert (
        result["error"]["details_safe"]["reason_code"]
        == "execution_pagination_unsupported_provider"
    )


@pytest.mark.asyncio
async def test_keyset_request_rejected_when_keyset_capability_missing():
    """Keyset requests should fail closed when provider lacks keyset capability."""
    caps = replace(
        capabilities_for_provider("postgres"),
        supports_keyset=False,
        supports_keyset_with_containment=False,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=2,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert (
        result["error"]["details_safe"]["reason_code"]
        == "execution_pagination_unsupported_provider"
    )
    assert result["error"]["details_safe"]["required_capability"] == "keyset_pagination"


@pytest.mark.asyncio
async def test_keyset_request_rejected_when_containment_capability_missing():
    """Keyset requests should fail closed when containment capability is missing."""
    caps = replace(
        capabilities_for_provider("postgres"),
        supports_keyset=True,
        supports_keyset_with_containment=False,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=2,
        )

    result = json.loads(payload)
    assert result["error"]["category"] == "invalid_request"
    assert (
        result["error"]["details_safe"]["reason_code"]
        == "execution_pagination_unsupported_provider"
    )
    assert result["error"]["details_safe"]["required_capability"] == "keyset_with_containment"


@pytest.mark.asyncio
async def test_keyset_request_allows_supported_provider_capabilities():
    """Keyset requests should proceed when keyset containment capabilities are present."""
    caps = replace(
        capabilities_for_provider("postgres"),
        supports_keyset=True,
        supports_keyset_with_containment=True,
    )

    class _Conn:
        async def fetch(self, sql, *params):
            _ = sql, params
            return [{"id": 1}, {"id": 2}, {"id": 3}]

    @asynccontextmanager
    async def _conn_ctx(*_args, **_kwargs):
        yield _Conn()

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="postgres",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_conn_ctx(),
        ),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.utils.auth.validate_role", return_value=None),
    ):
        payload = await handler(
            "SELECT id FROM users ORDER BY id ASC",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=2,
        )

    result = json.loads(payload)
    assert "error" not in result
