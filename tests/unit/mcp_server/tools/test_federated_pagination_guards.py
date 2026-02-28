"""Unit tests for federated pagination guards in execute_sql_query."""

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dal.capabilities import BackendCapabilities
from mcp_server.tools.execute_sql_query import handler

pytestmark = pytest.mark.pagination


class BaseMockConn:
    """Base mock connection for testing."""

    def __init__(self, partition_sig=None, backend_set=None):
        """Initialize mock connection."""
        self.session_guardrail_metadata = {}
        self.partition_signature = partition_sig
        self.backend_set = backend_set

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
        assert result["metadata"]["pagination.execution_topology"] == "federated"
        assert (
            result["metadata"]["pagination.reject_reason_code"]
            == "PAGINATION_FEDERATED_ORDERING_UNSAFE"
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
    """Pagination cursor should be rejected if backend-set membership changes."""
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
    async def _mock_conn_factory(backend_set):
        # Yield a class instance with methods defined in its type().__dict__
        class TestSpecificMockConn(SpecializedMockConn):
            async def fetch(self, *args, **kwargs):
                return [{"id": 1}, {"id": 2}]

            async def fetch_page(self, *args, **kwargs):
                return [{"id": 1}, {"id": 2}], "some-token"

            async def fetch_page_with_columns(self, *args, **kwargs):
                return [{"id": 1}, {"id": 2}], [], "some-token"

        yield TestSpecificMockConn(partition_sig=None, backend_set=backend_set)

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
    mock_span = MagicMock()
    mock_span.is_recording.return_value = True
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
            return_value=_mock_conn_factory(
                [
                    {"backend_id": "db-a", "region": "us-east-1", "role": "primary"},
                    {"backend_id": "db-b", "region": "us-east-1", "role": "replica"},
                ]
            ),
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
        assert "error" not in result
        assert "next_page_token" in result["metadata"]
        cursor = result["metadata"]["next_page_token"]
        assert cursor is not None

    # Second call using the cursor but with backend membership reduced to A-only.
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
            return_value=_mock_conn_factory(
                [{"backend_id": "db-a", "region": "us-east-1", "role": "primary"}]
            ),
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
        patch("mcp_server.tools.execute_sql_query.trace.get_current_span", return_value=mock_span),
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
        assert result_2["error"]["details_safe"]["reason_code"] == "PAGINATION_BACKEND_SET_CHANGED"
        metadata = result_2["metadata"]
        assert metadata["pagination.backend_set_sig_present"] is True
        assert metadata["pagination.backend_set_mismatch"] is True
        assert metadata["pagination.reject_reason_code"] == "PAGINATION_BACKEND_SET_CHANGED"

        attrs = {}
        for call in mock_span.set_attribute.call_args_list:
            key, value = call.args
            attrs[key] = value
        assert (
            attrs["pagination.backend_set_sig_present"]
            == metadata["pagination.backend_set_sig_present"]
        )
        assert (
            attrs["pagination.backend_set_mismatch"] == metadata["pagination.backend_set_mismatch"]
        )
        assert attrs["pagination.reject_reason_code"] == metadata["pagination.reject_reason_code"]


@pytest.mark.asyncio
async def test_federated_rejection_telemetry_parity_and_invariants():
    """Federated pagination rejection telemetry should stay bounded and SQL-safe."""
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
    mock_span = MagicMock()
    mock_span.is_recording.return_value = True
    sql = "SELECT id FROM users WHERE note = 'LEAK_SENTINEL_FED_777' ORDER BY id"

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
        patch("mcp_server.tools.execute_sql_query.trace.get_current_span", return_value=mock_span),
    ):
        result_json = await handler(
            sql,
            tenant_id=1,
            pagination_mode="keyset",
            page_size=10,
        )
        result = json.loads(result_json)

        reason_code = result["error"]["details_safe"]["reason_code"]
        assert reason_code == "PAGINATION_FEDERATED_ORDERING_UNSAFE"
        assert reason_code in {
            "PAGINATION_FEDERATED_ORDERING_UNSAFE",
            "PAGINATION_BACKEND_SET_CHANGED",
            "PAGINATION_FEDERATED_UNSUPPORTED",
        }
        metadata = result["metadata"]
        assert metadata["pagination.execution_topology"] == "federated"
        assert metadata["pagination.reject_reason_code"] == reason_code

        attrs = {}
        for call in mock_span.set_attribute.call_args_list:
            key, value = call.args
            attrs[key] = value
        assert attrs["pagination.execution_topology"] == metadata["pagination.execution_topology"]
        assert attrs["pagination.reject_reason_code"] == metadata["pagination.reject_reason_code"]
        assert attrs["pagination.federated.ordering_supported"] is False

        serialized = json.dumps(result)
        assert "LEAK_SENTINEL_FED_777" not in serialized
