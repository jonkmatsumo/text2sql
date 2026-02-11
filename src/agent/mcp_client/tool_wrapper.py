"""LangGraph-Compatible Tool Wrapper.

Wraps MCP tools in a LangGraph-compatible interface with:
- name: str
- description: str
- metadata: dict (schema info)
- ainvoke(input, config): async invocation
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional

from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope
from common.models.tool_errors import tool_error_invalid_request

logger = logging.getLogger(__name__)


@dataclass
class MCPToolWrapper:
    """LangGraph-compatible wrapper for MCP tools.

    Provides the interface expected by LangGraph nodes:
    - name: str
    - description: str
    - metadata: dict
    - ainvoke(input, config) -> Any

    Attributes:
        name: Tool name (e.g., "list_tables", "execute_sql_query").
        description: Human-readable tool description.
        metadata: Schema and other tool metadata.
    """

    name: str
    description: str
    input_schema: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    _invoke_fn: Optional[Callable[[dict], Coroutine[Any, Any, Any]]] = field(
        default=None, repr=False
    )
    _schema_validator: Optional[Callable[[dict], None]] = field(
        default=None, init=False, repr=False
    )
    _schema_validator_ready: bool = field(default=False, init=False, repr=False)

    def __post_init__(self):
        """Merge input_schema into metadata for compatibility."""
        if self.input_schema and "input_schema" not in self.metadata:
            self.metadata["input_schema"] = self.input_schema

    async def ainvoke(self, input: dict, config: Optional[dict] = None) -> Any:
        """Invoke the MCP tool asynchronously.

        Args:
            input: Dictionary of tool arguments.
            config: Optional RunnableConfig (for LangGraph compatibility).

        Returns:
            Normalized tool result.

        Raises:
            RuntimeError: If _invoke_fn not set (tool not bound to client).
        """
        if self._invoke_fn is None:
            raise RuntimeError(
                f"MCPToolWrapper '{self.name}' is not bound to an MCPClient. "
                "Use MCPClient.get_tools() to create bound wrappers."
            )

        validation_error = self._validate_input(input)
        if validation_error is not None:
            return ToolResponseEnvelope(
                result=None,
                metadata=GenericToolMetadata(provider="agent_mcp_client"),
                error=validation_error,
            ).model_dump(exclude_none=True)

        # Config is accepted for LangGraph compatibility but not used by MCP
        # Telemetry wrapping is handled by tools.py _wrap_tool()
        return await self._invoke_fn(input)

    def _validate_input(self, input_payload: Any):
        """Validate outgoing tool arguments against MCP input_schema."""
        if not isinstance(input_payload, dict):
            return tool_error_invalid_request(
                code="TOOL_INPUT_SCHEMA_VIOLATION",
                message=f"Tool '{self.name}' expects a JSON object input.",
                reason_code="tool_input_schema_violation",
                provider="agent_mcp_client",
            )

        schema = self.input_schema or self.metadata.get("input_schema")
        if not isinstance(schema, dict) or not schema:
            return None

        validator = self._get_schema_validator(schema)
        if validator is None:
            return None

        try:
            validator(input_payload)
            return None
        except Exception as exc:
            return tool_error_invalid_request(
                code="TOOL_INPUT_SCHEMA_VIOLATION",
                message=f"Tool input validation failed for '{self.name}'.",
                reason_code="tool_input_schema_violation",
                provider="agent_mcp_client",
                details_safe={"tool_name": self.name, "validation_error": str(exc)},
            )

    def _get_schema_validator(self, schema: dict) -> Optional[Callable[[dict], None]]:
        """Return a cached jsonschema validator callable for this wrapper."""
        if self._schema_validator_ready:
            return self._schema_validator

        self._schema_validator_ready = True
        try:
            from jsonschema import validators

            validator_cls = validators.validator_for(schema)
            validator_cls.check_schema(schema)
            compiled = validator_cls(schema)
            self._schema_validator = compiled.validate
        except Exception as exc:
            logger.warning("Failed to compile input schema for tool '%s': %s", self.name, exc)
            self._schema_validator = None

        return self._schema_validator

    # Provide _arun for backward compatibility with StructuredTool interface
    async def _arun(self, *args, config=None, **kwargs) -> Any:
        """Legacy StructuredTool-compatible async run method.

        Converts positional/keyword args to dict and delegates to ainvoke().
        """
        # Build input dict from args/kwargs
        input_dict = {}
        if args:
            # If single dict arg, use it directly
            if len(args) == 1 and isinstance(args[0], dict):
                input_dict = args[0]
            else:
                input_dict["args"] = args
        input_dict.update(kwargs)

        return await self.ainvoke(input_dict, config=config)


def create_tool_wrapper(
    name: str,
    description: str,
    input_schema: dict,
    invoke_fn: Callable[[dict], Coroutine[Any, Any, Any]],
) -> MCPToolWrapper:
    """Create a bound MCPToolWrapper.

    Args:
        name: Tool name.
        description: Tool description.
        input_schema: JSON schema for tool inputs.
        invoke_fn: Async function that invokes the tool.

    Returns:
        MCPToolWrapper bound to the invoke function.
    """
    return MCPToolWrapper(
        name=name,
        description=description,
        input_schema=input_schema,
        _invoke_fn=invoke_fn,
    )
