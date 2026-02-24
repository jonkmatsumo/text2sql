"""Conformance harness for pagination capability gating."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from unittest.mock import patch

import pytest

from dal.capabilities import capabilities_for_provider
from mcp_server.tools.execute_sql_query import handler


@pytest.mark.parametrize(
    "provider, expected_server, expected_wrapper",
    [
        ("postgres", False, True),
        ("sqlite", False, True),
        ("duckdb", False, True),
        ("bigquery", False, False),
    ],
)
def test_pagination_capability_matrix(provider: str, expected_server: bool, expected_wrapper: bool):
    """Provider capability matrix should expose deterministic pagination support flags."""
    caps = capabilities_for_provider(provider)
    assert bool(caps.supports_pagination) is expected_server
    assert bool(caps.supports_offset_pagination_wrapper) is expected_wrapper


def test_unknown_provider_pagination_capability_defaults_fail_closed():
    """Unknown providers should not advertise pagination wrapper support."""
    caps = capabilities_for_provider("unknown-provider")
    assert caps.supports_pagination is False
    assert caps.supports_offset_pagination_wrapper is False
    assert caps.supports_query_wrapping_subselect is False


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
