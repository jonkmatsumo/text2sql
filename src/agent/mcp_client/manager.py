"""MCP Client Manager with connection reuse and retry logic.

Provides resilient MCP tool invocation with:
- Connection pooling and reuse (avoid per-call client creation)
- Exponential backoff retries for transient failures
- Transient vs semantic error classification
"""

import asyncio
import logging
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional

from agent.mcp_client.sdk_client import MCPClient
from agent.models.run_budget import RunBudgetExceededError, consume_tool_call_budget
from agent.telemetry import telemetry
from common.errors.error_codes import canonical_error_code_for_category
from common.models.error_metadata import ToolError

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_retries: int = 3
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 10.0
    jitter_factor: float = 0.25

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt with exponential backoff and jitter."""
        delay = min(self.base_delay_seconds * (2**attempt), self.max_delay_seconds)
        jitter = delay * self.jitter_factor * random.random()
        return delay + jitter


# Default retry configuration
DEFAULT_RETRY_CONFIG = RetryConfig()


def is_retryable_tool_error(err: ToolError) -> bool:
    """Decide retryability from structured ToolError fields only."""
    if isinstance(err.retryable, bool):
        return err.retryable
    category = str(err.category or "").strip().lower()
    # Conservative fallback when legacy payloads omit retryable.
    if category in {"timeout"}:
        return True
    if category in {
        "invalid_request",
        "unsupported_capability",
        "unauthorized",
        "auth",
        "tool_response_malformed",
        "not_found",
    }:
        return False
    return False


def _extract_tool_error(payload: Any) -> Optional[ToolError]:
    """Extract ToolError from an MCP tool payload envelope when present."""
    if not isinstance(payload, dict):
        return None
    error_obj = payload.get("error")
    if not error_obj:
        return None
    if isinstance(error_obj, dict):
        try:
            return ToolError.model_validate(error_obj)
        except Exception:
            return None
    if isinstance(error_obj, str):
        return ToolError(
            category="internal",
            code="UNSTRUCTURED_TOOL_ERROR",
            error_code=canonical_error_code_for_category("internal").value,
            message=error_obj,
            retryable=False,
            provider="mcp_server",
        )
    return None


def is_transient_error(exc: Exception) -> bool:
    """Classify if an exception is transient (retryable) or semantic (not retryable).

    Transient errors are typically network/infrastructure issues that may resolve
    on retry. Semantic errors are application-level issues that won't change.

    Args:
        exc: The exception to classify.

    Returns:
        True if the error is transient and should be retried.
    """
    exc_str = str(exc).lower()
    exc_type = type(exc).__name__.lower()

    # Transient: network, connection, timeout issues
    transient_keywords = [
        "timeout",
        "timed out",
        "connection",
        "connect",
        "refused",
        "reset",
        "closed",
        "eof",
        "dns",
        "resolve",
        "network",
        "unreachable",
        "unavailable",
        "temporary",
        "retry",
        "overloaded",
        "503",
        "502",
        "504",
    ]

    # Check exception message
    if any(kw in exc_str for kw in transient_keywords):
        return True

    # Check exception type names
    transient_types = [
        "timeout",
        "connection",
        "network",
        "eof",
        "closed",
        "reset",
    ]
    if any(t in exc_type for t in transient_types):
        return True

    # Semantic errors: 4xx, tool-level errors
    semantic_keywords = [
        "not found",
        "invalid",
        "validation",
        "400",
        "401",
        "403",
        "404",
        "422",
    ]
    if any(kw in exc_str for kw in semantic_keywords):
        return False

    # Default: treat as transient for safety (allow retry)
    return True


class McpClientManager:
    """Manages MCP client connections with reuse and retry capabilities.

    This class provides:
    - A single reusable connection per manager instance
    - Automatic retry with exponential backoff for transient failures
    - Proper cleanup on shutdown

    Example:
        manager = McpClientManager(server_url="http://localhost:8000/messages")
        async with manager.connect() as session:
            result = await session.call_tool_with_retry("my_tool", {"arg": "value"})
    """

    def __init__(
        self,
        server_url: str,
        transport: str = "sse",
        headers: Optional[dict] = None,
        retry_config: Optional[RetryConfig] = None,
    ):
        """Initialize the MCP client manager.

        Args:
            server_url: MCP server endpoint URL.
            transport: Transport protocol ("sse" or "streamable-http").
            headers: Optional headers for HTTP requests.
            retry_config: Optional retry configuration. Defaults to DEFAULT_RETRY_CONFIG.
        """
        self.server_url = server_url
        self.transport = transport
        self.headers = headers or {}
        self.retry_config = retry_config or DEFAULT_RETRY_CONFIG
        self._client: Optional[MCPClient] = None
        self._connected = False
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def connect(self):
        """Async context manager for establishing/reusing MCP session.

        Yields:
            McpClientSession with retry-enabled tool invocation.
        """
        async with self._lock:
            if self._client is None:
                self._client = MCPClient(
                    server_url=self.server_url,
                    transport=self.transport,
                    headers=self.headers,
                )

        # Use the client's connect context manager
        async with self._client.connect() as mcp:
            yield McpClientSession(mcp, self.retry_config)

    async def call_tool_with_retry(
        self,
        tool_name: str,
        arguments: dict,
        retry_config: Optional[RetryConfig] = None,
    ) -> Any:
        """Call an MCP tool with automatic retry for transient failures.

        This is a convenience method that handles connection and retry in one call.

        Args:
            tool_name: Name of the tool to invoke.
            arguments: Tool arguments.
            retry_config: Optional override for retry configuration.

        Returns:
            Tool result on success.

        Raises:
            Exception: On permanent failure (semantic error or retries exhausted).
        """
        config = retry_config or self.retry_config
        async with self.connect() as session:
            return await session.call_tool_with_retry(tool_name, arguments, config)


class McpClientSession:
    """Active MCP session with retry-enabled tool invocation."""

    def __init__(self, mcp_client: MCPClient, retry_config: RetryConfig):
        """Initialize session with connected MCP client.

        Args:
            mcp_client: Connected MCPClient instance.
            retry_config: Retry configuration for tool calls.
        """
        self._mcp = mcp_client
        self._retry_config = retry_config

    async def list_tools(self):
        """List available tools from the MCP server."""
        return await self._mcp.list_tools()

    async def call_tool(self, name: str, arguments: dict) -> Any:
        """Call a tool without retry (single attempt)."""
        return await self._mcp.call_tool(name, arguments)

    async def call_tool_with_retry(
        self,
        name: str,
        arguments: dict,
        retry_config: Optional[RetryConfig] = None,
    ) -> Any:
        """Call a tool with automatic retry for transient failures.

        Args:
            name: Tool name to invoke.
            arguments: Tool arguments.
            retry_config: Optional override for retry configuration.

        Returns:
            Tool result on success.

        Raises:
            Exception: On permanent failure (semantic error or retries exhausted).
        """
        config = retry_config or self._retry_config
        last_exception: Optional[Exception] = None

        for attempt in range(config.max_retries + 1):
            try:
                # The first attempt is consumed in MCPToolWrapper; retries are consumed here.
                if attempt > 0:
                    consume_tool_call_budget(1)
                # Record attempt identifiers in the current span
                span = telemetry.get_current_span()
                if span:
                    span.set_attribute("mcp.attempt_count", attempt + 1)
                    span.set_attribute("mcp.retry_count", attempt)

                result = await self._mcp.call_tool(name, arguments)
                structured_error = _extract_tool_error(result)
                if structured_error is None:
                    return result

                if not is_retryable_tool_error(structured_error):
                    logger.warning(
                        "MCP tool '%s' returned non-retryable structured error "
                        "(category=%s, code=%s).",
                        name,
                        structured_error.category,
                        structured_error.code,
                    )
                    return result

                if attempt >= config.max_retries:
                    logger.error(
                        "MCP tool '%s' returned retryable structured error after %d attempts.",
                        name,
                        attempt + 1,
                    )
                    return result

                delay = config.get_delay(attempt)
                logger.warning(
                    "MCP tool '%s' returned retryable structured error (attempt %d/%d), "
                    "retrying in %.2fs (category=%s, code=%s).",
                    name,
                    attempt + 1,
                    config.max_retries + 1,
                    delay,
                    structured_error.category,
                    structured_error.code,
                )
                await asyncio.sleep(delay)
                continue
            except RunBudgetExceededError:
                raise
            except Exception as exc:
                last_exception = exc

                # Don't retry semantic errors
                if not is_transient_error(exc):
                    logger.warning(
                        "MCP tool '%s' failed with semantic error (not retrying): %s",
                        name,
                        exc,
                    )
                    raise

                # Check if we have retries left
                if attempt >= config.max_retries:
                    logger.error(
                        "MCP tool '%s' failed after %d attempts: %s",
                        name,
                        attempt + 1,
                        exc,
                    )
                    raise

                # Calculate delay and retry
                delay = config.get_delay(attempt)
                logger.warning(
                    "MCP tool '%s' transient failure (attempt %d/%d), " "retrying in %.2fs: %s",
                    name,
                    attempt + 1,
                    config.max_retries + 1,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)

        # Should not reach here, but just in case
        if last_exception:
            raise last_exception
        raise RuntimeError("Unexpected retry loop exit")


def create_resilient_invoke_fn(
    server_url: str,
    transport: str,
    tool_name: str,
    headers: Optional[dict] = None,
    retry_config: Optional[RetryConfig] = None,
):
    """Create an async invoke function with retry support.

    This is a drop-in replacement for the original _create_invoke_fn
    that adds retry logic for transient failures.

    Args:
        server_url: MCP server URL.
        transport: Transport protocol.
        tool_name: Name of the tool.
        headers: Optional headers.
        retry_config: Optional retry configuration.

    Returns:
        Async function that invokes the tool with retry support.
    """
    config = retry_config or DEFAULT_RETRY_CONFIG

    async def invoke(arguments: dict) -> Any:
        manager = McpClientManager(
            server_url=server_url,
            transport=transport,
            headers=headers,
            retry_config=config,
        )
        return await manager.call_tool_with_retry(tool_name, arguments)

    return invoke
