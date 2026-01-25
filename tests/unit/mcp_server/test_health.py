"""Tests for MCP server health and initialization state tracking."""

from mcp_server.models.health import CheckStatus, InitializationState


class TestInitializationState:
    """Tests for InitializationState class."""

    def test_empty_state_not_ready(self):
        """Empty state with no checks should not be ready."""
        state = InitializationState()
        assert state.is_ready is False

    def test_all_required_ok_is_ready(self):
        """State with all required checks OK should be ready."""
        state = InitializationState()
        state.record_success("database", required=True)
        state.record_success("nlp_patterns", required=True)

        assert state.is_ready is True
        assert len(state.failed_checks) == 0

    def test_one_required_failure_not_ready(self):
        """State with one required failure should not be ready."""
        state = InitializationState()
        state.record_success("database", required=True)
        state.record_failure("nlp_patterns", ValueError("init failed"), required=True)

        assert state.is_ready is False
        assert len(state.failed_checks) == 1
        assert state.failed_checks[0].name == "nlp_patterns"

    def test_optional_failure_still_ready(self):
        """State with only optional failures should be ready."""
        state = InitializationState()
        state.record_success("database", required=True)
        state.record_failure("schema_embeddings", ValueError("not critical"), required=False)

        assert state.is_ready is True
        # Still recorded as failed
        assert len(state.failed_checks) == 1

    def test_record_failure_captures_error_details(self):
        """Record failure should capture error type and message."""
        state = InitializationState()
        exc = RuntimeError("connection refused")
        state.record_failure("database", exc, required=True)

        check = state.checks["database"]
        assert check.status == CheckStatus.FAILED
        assert check.error_type == "RuntimeError"
        assert check.error_message == "connection refused"
        assert check.timestamp is not None

    def test_record_success_sets_ok_status(self):
        """Record success should set OK status with timestamp."""
        state = InitializationState()
        state.record_success("database", required=True)

        check = state.checks["database"]
        assert check.status == CheckStatus.OK
        assert check.error_type is None
        assert check.timestamp is not None

    def test_record_skipped_sets_skipped_status(self):
        """Record skipped should set SKIPPED status."""
        state = InitializationState()
        state.record_skipped("optional_feature", reason="not configured")

        check = state.checks["optional_feature"]
        assert check.status == CheckStatus.SKIPPED
        assert check.error_message == "not configured"
        assert check.required is False

    def test_as_dict_includes_all_info(self):
        """As dict should include ready status, checks, and failed_checks."""
        state = InitializationState()
        state.start()
        state.record_success("database", required=True)
        state.record_failure("nlp", ValueError("test"), required=True)
        state.complete()

        result = state.as_dict()

        assert result["ready"] is False
        assert "database" in result["checks"]
        assert "nlp" in result["checks"]
        assert len(result["failed_checks"]) == 1
        assert result["failed_checks"][0]["name"] == "nlp"
        assert result["started_at"] is not None
        assert result["completed_at"] is not None

    def test_check_result_to_dict(self):
        """Verify CheckResult to_dict produces valid JSON structure."""
        state = InitializationState()
        state.record_failure("test", TypeError("bad type"), required=True)

        check_dict = state.checks["test"].to_dict()

        assert check_dict["name"] == "test"
        assert check_dict["status"] == "failed"
        assert check_dict["error_type"] == "TypeError"
        assert check_dict["error_message"] == "bad type"
        assert check_dict["required"] is True
        assert "timestamp" in check_dict


class TestHealthEndpointHandler:
    """Tests for health endpoint handler logic."""

    def test_health_returns_ready_true_when_all_ok(self):
        """Health endpoint returns ready=true when ready."""
        state = InitializationState()
        state.start()
        state.record_success("database", required=True)
        state.complete()

        status = state.as_dict()

        assert status["ready"] is True
        assert len(status["failed_checks"]) == 0

    def test_health_returns_ready_false_when_failure(self):
        """Health endpoint returns ready=false when not ready."""
        state = InitializationState()
        state.start()
        state.record_failure("database", RuntimeError("connection refused"), required=True)
        state.complete()

        status = state.as_dict()

        assert status["ready"] is False
        assert len(status["failed_checks"]) == 1
        assert status["failed_checks"][0]["name"] == "database"
        assert status["failed_checks"][0]["error_type"] == "RuntimeError"

    def test_health_includes_all_checks_in_response(self):
        """Health endpoint response includes all checks with details."""
        state = InitializationState()
        state.record_success("database", required=True)
        state.record_failure("nlp", ValueError("test"), required=False)
        state.record_skipped("optional", reason="not configured")

        status = state.as_dict()

        assert "database" in status["checks"]
        assert "nlp" in status["checks"]
        assert "optional" in status["checks"]
        assert status["checks"]["database"]["status"] == "ok"
        assert status["checks"]["nlp"]["status"] == "failed"
        assert status["checks"]["optional"]["status"] == "skipped"
