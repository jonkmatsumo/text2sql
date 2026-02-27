"""Unit tests for federated pagination guards in execute_sql_query."""

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dal.capabilities import BackendCapabilities
from mcp_server.tools.execute_sql_query import handler


class BaseMockConn:
    """Base mock connection for testing."""

    def __init__(self, partition_sig=None):
        """Initialize mock connection."""
        self.session_guardrail_metadata = {}
        self.partition_signature = partition_sig

    async def fetch(self, *args, **kwargs):
        """Mock fetch."""
        return [{"id": 1}]

    async def fetch_page(self, *args, **kwargs):
        """Mock fetch_page."""
        return [{"id": 1}], None

    async def fetch_page_with_columns(self, *args, **kwargs):
        """Mock fetch_page_with_columns."""
        return [{"id": 1}], [], None

    def __getattr__(self, name):
        """Mock getattr."""
        return MagicMock()


class SpecializedMockConn(BaseMockConn):
    """Specialized mock connection for testing."""

    pass


@pytest.mark.asyncio
async def test_keyset_pagination_rejected_on_federated_without_ordering():
    """Keyset pagination should be rejected on federated backends without deterministic ordering."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=False,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_pagination=True,
    )

    @asynccontextmanager
    async def _mock_conn(*args, **kwargs):
        yield BaseMockConn()

    mock_policy_inst = MagicMock()
    mock_policy_inst.evaluate = AsyncMock(
        return_value=MagicMock(
            should_execute=True,
            sql_to_execute="SELECT 1",
            params_to_bind=[],
            envelope_metadata={},
            telemetry_attributes={},
        )
    )
    mock_policy_inst.default_decision.return_value = MagicMock(
        telemetry_attributes={}, envelope_metadata={}
    )

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="federated-db",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection", return_value=_mock_conn()
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=False),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.tools.execute_sql_query._validate_sql_ast", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query._validate_sql_complexity", return_value=(None, {})
        ),
        patch("dal.util.read_only.enforce_read_only_sql", return_value=None),
        patch(
            "common.security.tenant_enforcement_policy.TenantEnforcementPolicy",
            return_value=mock_policy_inst,
        ),
    ):
        result_json = await handler(
            "SELECT id FROM users ORDER BY id",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )
        result = json.loads(result_json)

        assert "error" in result
        assert result["error"]["category"] == "invalid_request"
        assert result["error"]["error_code"] == "VALIDATION_ERROR"
        assert (
            result["error"]["details_safe"]["reason_code"] == "PAGINATION_FEDERATED_ORDERING_UNSAFE"
        )


@pytest.mark.asyncio
async def test_keyset_pagination_accepted_on_federated_with_ordering():
    """Keyset pagination should be accepted on federated backends with deterministic ordering."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_federated_deterministic_ordering=True,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_column_metadata=True,
        supports_pagination=True,
    )

    @asynccontextmanager
    async def _mock_conn(*args, **kwargs):
        yield BaseMockConn()

    mock_policy_inst = MagicMock()
    mock_policy_inst.evaluate = AsyncMock(
        return_value=MagicMock(
            should_execute=True,
            sql_to_execute="SELECT 1",
            params_to_bind=[],
            envelope_metadata={},
            telemetry_attributes={},
        )
    )
    mock_policy_inst.default_decision.return_value = MagicMock(
        telemetry_attributes={}, envelope_metadata={}
    )

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="federated-db",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection", return_value=_mock_conn()
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=False),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.tools.execute_sql_query._validate_sql_ast", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query._validate_sql_complexity", return_value=(None, {})
        ),
        patch("dal.util.read_only.enforce_read_only_sql", return_value=None),
        patch(
            "common.security.tenant_enforcement_policy.TenantEnforcementPolicy",
            return_value=mock_policy_inst,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.normalize_sqlglot_dialect", return_value="postgres"
        ),
        patch(
            "dal.keyset_pagination.apply_keyset_pagination",
            side_effect=lambda q, *args, **kwargs: q,
        ),
        patch(
            "dal.keyset_pagination.canonicalize_keyset_sql",
            side_effect=lambda q, *args, **kwargs: "SELECT 1",
        ),
        patch("dal.keyset_pagination.decode_keyset_cursor", return_value={"id": 1}),
    ):
        result_json = await handler(
            "SELECT id FROM users ORDER BY id",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )
        result = json.loads(result_json)

        if "error" in result:
            pytest.fail(f"Handler returned error: {result['error']}")
        assert result["metadata"]["pagination_mode_used"] == "keyset"


@pytest.mark.asyncio
async def test_offset_pagination_rejected_on_federated_when_env_enabled():
    """Offset pagination should be rejected on federated backends when env flag is set."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_pagination=True,
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    @asynccontextmanager
    async def _mock_conn(*args, **kwargs):
        yield BaseMockConn()

    mock_policy_inst = MagicMock()
    mock_policy_inst.evaluate = AsyncMock(
        return_value=MagicMock(
            should_execute=True,
            sql_to_execute="SELECT 1",
            params_to_bind=[],
            envelope_metadata={},
            telemetry_attributes={},
        )
    )
    mock_policy_inst.default_decision.return_value = MagicMock(
        telemetry_attributes={}, envelope_metadata={}
    )

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="federated-db",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection", return_value=_mock_conn()
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=False),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.tools.execute_sql_query._validate_sql_ast", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query._validate_sql_complexity", return_value=(None, {})
        ),
        patch("dal.util.read_only.enforce_read_only_sql", return_value=None),
        patch(
            "common.security.tenant_enforcement_policy.TenantEnforcementPolicy",
            return_value=mock_policy_inst,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.get_env_bool",
            side_effect=lambda key, default: (
                True if key == "PAGINATION_DISALLOW_FEDERATED_OFFSET" else default
            ),
        ),
    ):
        result_json = await handler(
            "SELECT id FROM users",
            tenant_id=1,
            pagination_mode="offset",
            page_size=10,
        )
        result = json.loads(result_json)

        assert "error" in result
        assert result["error"]["category"] == "invalid_request"
        assert result["error"]["error_code"] == "VALIDATION_ERROR"
        assert result["error"]["details_safe"]["reason_code"] == "PAGINATION_FEDERATED_UNSUPPORTED"


@pytest.mark.asyncio
async def test_offset_pagination_accepted_on_federated_when_env_disabled():
    """Offset pagination should be accepted on federated backends when env flag is not set."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_pagination=True,
        supports_offset_pagination_wrapper=True,
        supports_query_wrapping_subselect=True,
    )

    @asynccontextmanager
    async def _mock_conn(*args, **kwargs):
        yield BaseMockConn()

    mock_policy_inst = MagicMock()
    mock_policy_inst.evaluate = AsyncMock(
        return_value=MagicMock(
            should_execute=True,
            sql_to_execute="SELECT 1",
            params_to_bind=[],
            envelope_metadata={},
            telemetry_attributes={},
        )
    )
    mock_policy_inst.default_decision.return_value = MagicMock(
        telemetry_attributes={}, envelope_metadata={}
    )

    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="federated-db",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection", return_value=_mock_conn()
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=False),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.tools.execute_sql_query._validate_sql_ast", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query._validate_sql_complexity", return_value=(None, {})
        ),
        patch("dal.util.read_only.enforce_read_only_sql", return_value=None),
        patch(
            "common.security.tenant_enforcement_policy.TenantEnforcementPolicy",
            return_value=mock_policy_inst,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.get_env_bool",
            side_effect=lambda key, default: (
                False if key == "PAGINATION_DISALLOW_FEDERATED_OFFSET" else default
            ),
        ),
    ):
        result_json = await handler(
            "SELECT id FROM users",
            tenant_id=1,
            pagination_mode="offset",
            page_size=10,
        )
        result = json.loads(result_json)

        if "error" in result:
            pytest.fail(f"Handler returned error: {result['error']}")
        assert result["metadata"]["pagination_mode_used"] == "offset"


@pytest.mark.asyncio
async def test_cursor_rejected_on_backend_signature_mismatch():
    """Pagination cursor should be rejected if partition_signature changes."""
    caps = BackendCapabilities(
        provider_name="federated-db",
        execution_topology="federated",
        supports_pagination=True,
        supports_keyset=True,
        supports_keyset_with_containment=True,
        supports_column_metadata=True,
        supports_federated_deterministic_ordering=True,
    )

    @asynccontextmanager
    async def _mock_conn_factory(partition_sig):
        # Yield a class instance with methods defined in its type().__dict__
        class TestSpecificMockConn(SpecializedMockConn):
            async def fetch(self, *args, **kwargs):
                return [{"id": 1}, {"id": 2}]

            async def fetch_page(self, *args, **kwargs):
                return [{"id": 1}, {"id": 2}], "some-token"

            async def fetch_page_with_columns(self, *args, **kwargs):
                return [{"id": 1}, {"id": 2}], [], "some-token"

        yield TestSpecificMockConn(partition_sig)

    mock_policy_inst = MagicMock()
    mock_policy_inst.evaluate = AsyncMock(
        return_value=MagicMock(
            should_execute=True,
            sql_to_execute="SELECT id FROM users ORDER BY id",
            params_to_bind=[],
            envelope_metadata={},
            telemetry_attributes={},
        )
    )
    mock_policy_inst.default_decision.return_value = MagicMock(
        telemetry_attributes={}, envelope_metadata={}
    )

    # First call to generate a valid token with signature A
    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="federated-db",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_mock_conn_factory("sig-A"),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=False),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.tools.execute_sql_query._validate_sql_ast", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query._validate_sql_complexity", return_value=(None, {})
        ),
        patch("dal.util.read_only.enforce_read_only_sql", return_value=None),
        patch(
            "common.security.tenant_enforcement_policy.TenantEnforcementPolicy",
            return_value=mock_policy_inst,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.normalize_sqlglot_dialect", return_value="postgres"
        ),
    ):
        result_json = await handler(
            "SELECT id FROM users ORDER BY id",
            tenant_id=1,
            pagination_mode="keyset",
            page_size=1,
        )
        result = json.loads(result_json)
        if "error" in result:
            print(f"DEBUG Error 1 Trace: {result['error']}")
        assert "error" not in result
        assert "next_page_token" in result["metadata"]
        cursor = result["metadata"]["next_page_token"]
        assert cursor is not None

    # Second call using the cursor but with signature B
    with (
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_capabilities",
            return_value=caps,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_query_target_provider",
            return_value="federated-db",
        ),
        patch(
            "mcp_server.tools.execute_sql_query.Database.get_connection",
            return_value=_mock_conn_factory("sig-B"),
        ),
        patch("mcp_server.utils.auth.validate_role", return_value=False),
        patch("agent.validation.policy_enforcer.PolicyEnforcer.validate_sql", return_value=None),
        patch("mcp_server.tools.execute_sql_query._validate_sql_ast", return_value=None),
        patch(
            "mcp_server.tools.execute_sql_query._validate_sql_complexity", return_value=(None, {})
        ),
        patch("dal.util.read_only.enforce_read_only_sql", return_value=None),
        patch(
            "common.security.tenant_enforcement_policy.TenantEnforcementPolicy",
            return_value=mock_policy_inst,
        ),
        patch(
            "mcp_server.tools.execute_sql_query.normalize_sqlglot_dialect", return_value="postgres"
        ),
    ):
        result_json_2 = await handler(
            "SELECT id FROM users ORDER BY id",
            tenant_id=1,
            pagination_mode="keyset",
            keyset_cursor=cursor,
            page_size=1,
        )
        result_2 = json.loads(result_json_2)

        assert "error" in result_2
        # Fingerprint mismatch because of backend_signature change
        assert (
            result_2["error"]["details_safe"]["reason_code"]
            == "execution_pagination_keyset_cursor_invalid"
        )
