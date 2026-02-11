"""Tests for MCP client manager retry logic."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.mcp_client.manager import (
    DEFAULT_RETRY_CONFIG,
    McpClientManager,
    McpClientSession,
    RetryConfig,
    is_retryable_tool_error,
    is_transient_error,
)
from common.models.error_metadata import ToolError


class TestIsTransientError:
    """Tests for transient error classification."""

    def test_timeout_is_transient(self):
        """Timeout errors should be classified as transient."""
        assert is_transient_error(Exception("Connection timeout"))
        assert is_transient_error(Exception("Request timed out"))
        assert is_transient_error(TimeoutError("Operation timed out"))

    def test_connection_errors_are_transient(self):
        """Connection-related errors should be classified as transient."""
        assert is_transient_error(Exception("Connection refused"))
        assert is_transient_error(Exception("Connection reset by peer"))
        assert is_transient_error(Exception("Network is unreachable"))
        assert is_transient_error(Exception("DNS resolution failed"))
        assert is_transient_error(ConnectionError("Failed to connect"))

    def test_server_errors_are_transient(self):
        """5xx server errors should be classified as transient."""
        assert is_transient_error(Exception("502 Bad Gateway"))
        assert is_transient_error(Exception("503 Service Unavailable"))
        assert is_transient_error(Exception("504 Gateway Timeout"))

    def test_validation_errors_are_not_transient(self):
        """4xx client/validation errors should not be retried."""
        assert not is_transient_error(Exception("400 Bad Request"))
        assert not is_transient_error(Exception("401 Unauthorized"))
        assert not is_transient_error(Exception("404 Not Found"))
        assert not is_transient_error(Exception("422 Validation Error"))
        assert not is_transient_error(Exception("Invalid argument: foo"))


class TestRetryConfig:
    """Tests for retry configuration."""

    def test_default_config(self):
        """Default config should have reasonable defaults."""
        config = DEFAULT_RETRY_CONFIG
        assert config.max_retries == 3
        assert config.base_delay_seconds == 0.5
        assert config.max_delay_seconds == 10.0
        assert 0 < config.jitter_factor <= 0.5

    def test_exponential_backoff(self):
        """Delay should increase exponentially with attempts."""
        config = RetryConfig(base_delay_seconds=1.0, jitter_factor=0)
        assert config.get_delay(0) == 1.0  # 1.0 * 2^0 = 1.0
        assert config.get_delay(1) == 2.0  # 1.0 * 2^1 = 2.0
        assert config.get_delay(2) == 4.0  # 1.0 * 2^2 = 4.0

    def test_max_delay_cap(self):
        """Delay should be capped at max_delay_seconds."""
        config = RetryConfig(base_delay_seconds=1.0, max_delay_seconds=5.0, jitter_factor=0)
        # 2^10 = 1024, but should be capped at 5.0
        assert config.get_delay(10) == 5.0

    def test_jitter_adds_randomness(self):
        """Jitter should add some randomness to delay."""
        config = RetryConfig(base_delay_seconds=1.0, jitter_factor=0.25)
        delays = [config.get_delay(0) for _ in range(10)]
        # All delays should be >= base_delay
        assert all(d >= 1.0 for d in delays)
        # Should have some variation (not all identical)
        assert len(set(delays)) > 1


class TestMcpClientSession:
    """Tests for MCP client session with retry."""

    @pytest.mark.asyncio
    async def test_call_tool_success_first_attempt(self):
        """Successful call should return result immediately."""
        mock_mcp = MagicMock()
        mock_mcp.call_tool = AsyncMock(return_value={"result": "success"})

        session = McpClientSession(mock_mcp, RetryConfig(max_retries=3))
        result = await session.call_tool_with_retry("test_tool", {"arg": "value"})

        assert result == {"result": "success"}
        assert mock_mcp.call_tool.call_count == 1

    @pytest.mark.asyncio
    async def test_call_tool_retry_on_transient_error(self):
        """Transient errors should trigger retry."""
        mock_mcp = MagicMock()
        # Fail twice with transient error, then succeed
        mock_mcp.call_tool = AsyncMock(
            side_effect=[
                ConnectionError("Connection refused"),
                TimeoutError("Timed out"),
                {"result": "success"},
            ]
        )

        config = RetryConfig(max_retries=3, base_delay_seconds=0.01)
        session = McpClientSession(mock_mcp, config)
        result = await session.call_tool_with_retry("test_tool", {"arg": "value"})

        assert result == {"result": "success"}
        assert mock_mcp.call_tool.call_count == 3

    @pytest.mark.asyncio
    async def test_call_tool_no_retry_on_semantic_error(self):
        """Semantic errors should not be retried."""
        mock_mcp = MagicMock()
        mock_mcp.call_tool = AsyncMock(side_effect=Exception("404 Not Found"))

        config = RetryConfig(max_retries=3, base_delay_seconds=0.01)
        session = McpClientSession(mock_mcp, config)

        with pytest.raises(Exception, match="404 Not Found"):
            await session.call_tool_with_retry("test_tool", {"arg": "value"})

        # Should only be called once (no retries for semantic errors)
        assert mock_mcp.call_tool.call_count == 1

    @pytest.mark.asyncio
    async def test_call_tool_exhausts_retries(self):
        """Should raise after exhausting all retries."""
        mock_mcp = MagicMock()
        mock_mcp.call_tool = AsyncMock(side_effect=ConnectionError("Connection refused"))

        config = RetryConfig(max_retries=2, base_delay_seconds=0.01)
        session = McpClientSession(mock_mcp, config)

        with pytest.raises(ConnectionError, match="Connection refused"):
            await session.call_tool_with_retry("test_tool", {"arg": "value"})

        # Initial attempt + 2 retries = 3 calls
        assert mock_mcp.call_tool.call_count == 3

    @pytest.mark.asyncio
    async def test_call_tool_retries_structured_retryable_error(self):
        """Structured retryable tool errors should trigger retries."""
        mock_mcp = MagicMock()
        mock_mcp.call_tool = AsyncMock(
            side_effect=[
                {
                    "schema_version": "1.0",
                    "result": None,
                    "metadata": {"provider": "mcp_server"},
                    "error": {
                        "category": "timeout",
                        "code": "TIMEOUT",
                        "message": "Timed out.",
                        "retryable": True,
                    },
                },
                {"schema_version": "1.0", "result": {"ok": True}, "metadata": {}},
            ]
        )

        config = RetryConfig(max_retries=2, base_delay_seconds=0.01)
        session = McpClientSession(mock_mcp, config)
        result = await session.call_tool_with_retry("test_tool", {"arg": "value"})

        assert result["result"] == {"ok": True}
        assert mock_mcp.call_tool.call_count == 2

    @pytest.mark.asyncio
    async def test_call_tool_does_not_retry_structured_non_retryable_error(self):
        """Structured non-retryable tool errors should return immediately."""
        first_result = {
            "schema_version": "1.0",
            "result": None,
            "metadata": {"provider": "mcp_server"},
            "error": {
                "category": "invalid_request",
                "code": "INVALID",
                "message": "Bad input.",
                "retryable": False,
            },
        }
        mock_mcp = MagicMock()
        mock_mcp.call_tool = AsyncMock(
            side_effect=[
                first_result,
                {"schema_version": "1.0", "result": {"ok": True}, "metadata": {}},
            ]
        )

        config = RetryConfig(max_retries=2, base_delay_seconds=0.01)
        session = McpClientSession(mock_mcp, config)
        result = await session.call_tool_with_retry("test_tool", {"arg": "value"})

        assert result == first_result
        assert mock_mcp.call_tool.call_count == 1


class TestStructuredErrorRetryability:
    """Tests for structured ToolError retryability classification."""

    def test_structured_retryable_flag_true(self):
        """Explicit retryable=True should always be retried."""
        err = ToolError(
            category="invalid_request",
            code="X",
            message="x",
            retryable=True,
        )
        assert is_retryable_tool_error(err) is True

    def test_structured_retryable_flag_false(self):
        """Explicit retryable=False should never be retried."""
        err = ToolError(
            category="timeout",
            code="TIMEOUT",
            message="Timed out.",
            retryable=False,
        )
        assert is_retryable_tool_error(err) is False


class TestMcpClientManager:
    """Tests for MCP client manager."""

    @pytest.mark.asyncio
    async def test_manager_creates_client_once(self):
        """Manager should reuse client across calls."""
        with patch("agent.mcp_client.manager.MCPClient") as mock_client_class:
            mock_client = MagicMock()
            mock_mcp = MagicMock()
            mock_mcp.call_tool = AsyncMock(return_value={"result": "ok"})

            # Set up the mock chain
            mock_client.connect.return_value.__aenter__ = AsyncMock(return_value=mock_mcp)
            mock_client.connect.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_client_class.return_value = mock_client

            manager = McpClientManager(
                server_url="http://localhost:8000",
                transport="sse",
                retry_config=RetryConfig(max_retries=0),
            )

            # First call
            async with manager.connect() as session:
                await session.call_tool("test", {})

            # Second call - should reuse client
            async with manager.connect() as session:
                await session.call_tool("test", {})

            # MCPClient should only be instantiated once
            assert mock_client_class.call_count == 1
