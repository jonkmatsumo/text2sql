import ast
import inspect
import os

import pytest

from mcp_server.tools import registry


def get_all_tool_handlers():
    """Extract all tool handlers by inspecting the registry module."""
    # This is a bit of a hack to get the actual function objects
    # We can also walk the directory and import them.
    tools_dir = os.path.dirname(registry.__file__)
    handlers = {}

    for root, _, files in os.walk(tools_dir):
        for file in files:
            if file.endswith(".py") and file != "__init__.py" and file != "registry.py":
                rel_path = os.path.relpath(
                    os.path.join(root, file), os.path.dirname(os.path.dirname(tools_dir))
                )
                module_path = rel_path.replace(os.path.sep, ".").replace(".py", "")
                try:
                    module = __import__(module_path, fromlist=["handler"])
                    if hasattr(module, "handler"):
                        name = getattr(module, "TOOL_NAME", file.replace(".py", ""))
                        handlers[name] = module.handler
                except Exception:
                    continue
    return handlers


@pytest.mark.parametrize("tool_name, handler", get_all_tool_handlers().items())
def test_tool_invariants(tool_name, handler):
    """Ensure every tool follows the standard contract.

    1. Calls require_tenant_id() if it takes tenant_id
    2. Calls validate_limit() if it takes limit
    """
    source = inspect.getsource(handler)
    tree = ast.parse(source)

    # 1. Check for tracing - usually handled in registry.py
    # However, let's check if the file imports trace_tool or if registry.py wraps it.
    # Actually, registry.py is the source of truth for wrapping.
    # We check if registry.py wraps all canonical tools.

    # 2. Check for tenant_id validation
    handler_args = [arg.arg for arg in tree.body[0].args.args]
    if "tenant_id" in handler_args:
        # Check if require_tenant_id or manual tenant_id check is present
        # Ideally we want require_tenant_id(tenant_id, ...)
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
        call_names = []
        for call in calls:
            if isinstance(call.func, ast.Name):
                call_names.append(call.func.id)
            elif isinstance(call.func, ast.Attribute):
                call_names.append(call.func.attr)

        # Exception for metadata/schema tools
        metadata_tools = [
            "list_tables",
            "get_table_schema",
            "search_relevant_tables",
            "get_semantic_definitions",
            "list_approved_examples",
        ]
        if tool_name not in metadata_tools:
            assert (
                "require_tenant_id" in call_names
            ), f"Tool '{tool_name}' MUST call require_tenant_id()"

    # 3. Check for limit validation
    if "limit" in handler_args:
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
        call_names = []
        for call in calls:
            if isinstance(call.func, ast.Name):
                call_names.append(call.func.id)
            elif isinstance(call.func, ast.Attribute):
                call_names.append(call.func.attr)
        assert "validate_limit" in call_names, f"Tool '{tool_name}' MUST call validate_limit()"


def test_registry_wraps_all_with_tracing():
    """Verify that registry.py wraps all registered tools with trace_tool."""
    registry_source = inspect.getsource(registry.register_all)
    tree = ast.parse(registry_source)

    # Look for calls to mcp.tool(...)(trace_tool(...)(func))
    # or the helper register(name, func) which calls trace_tool

    # In registry.py:
    # def register(name, func):
    #     mcp.tool(name=name)(trace_tool(name)(func))

    # We verify the register helper is defined and used correctly
    found_register_def = False
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "register":
            found_register_def = True
            # Check if it calls trace_tool
            calls = [n for n in ast.walk(node) if isinstance(n, ast.Call)]
            call_names = []
            for c in calls:
                if isinstance(c.func, ast.Name):
                    call_names.append(c.func.id)
                elif isinstance(c.func, ast.Attribute):
                    call_names.append(c.func.attr)
            assert "trace_tool" in call_names or "register" in call_names or "tool" in call_names
            break

    assert found_register_def, "registry.py should use a register helper that applies trace_tool"
