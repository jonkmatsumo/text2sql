"""LangGraph-Compatible Tool Wrapper (Phase 3 - Issue #166).

Wraps MCP tools in a LangGraph-compatible interface with:
- name: str
- description: str
- metadata: dict (schema info)
- ainvoke(input, config): async invocation with telemetry

Design Decisions (from adapter migration):
- Preserves tool.name and tool.description from MCP
- input_schema stored in metadata for downstream consumers
- ainvoke() signature matches LangGraph/LangChain tool expectations
- Telemetry wrapping delegated to tools.py _wrap_tool() (Phase 4)
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional

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

        # Config is accepted for LangGraph compatibility but not used by MCP
        # Telemetry wrapping is handled by tools.py _wrap_tool()
        return await self._invoke_fn(input)

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
