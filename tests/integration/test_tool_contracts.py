"""Integration tests for tool contracts and standardization."""

import importlib
import inspect
import os
from typing import List

import pytest

from common.models.tool_envelopes import ExecuteSQLQueryResponseEnvelope, ToolResponseEnvelope


def get_tool_modules() -> List[str]:
    """Discover all tool modules in mcp_server.tools package."""
    tool_modules = []
    base_path = "src/mcp_server/tools"

    # Core tools
    for filename in os.listdir(base_path):
        if filename.endswith(".py") and filename != "__init__.py" and filename != "registry.py":
            module_name = f"mcp_server.tools.{filename[:-3]}"
            tool_modules.append(module_name)

    # Admin tools
    admin_path = os.path.join(base_path, "admin")
    if os.path.exists(admin_path):
        for filename in os.listdir(admin_path):
            if filename.endswith(".py") and filename != "__init__.py":
                module_name = f"mcp_server.tools.admin.{filename[:-3]}"
                tool_modules.append(module_name)

    return tool_modules


@pytest.mark.parametrize("module_name", get_tool_modules())
def test_tool_contract_signatures(module_name):
    """Verify that all tools have a handler function returning str (JSON envelope)."""
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        pytest.fail(f"Failed to import {module_name}: {e}")

    # Check for TOOL_NAME
    assert hasattr(module, "TOOL_NAME"), f"{module_name} missing TOOL_NAME"
    assert isinstance(module.TOOL_NAME, str), f"{module_name}.TOOL_NAME must be a string"

    # Check for handler
    assert hasattr(module, "handler"), f"{module_name} missing handler function"
    handler = module.handler
    assert inspect.iscoroutinefunction(handler), f"{module_name}.handler must be async"

    # Check return type annotation
    sig = inspect.signature(handler)
    return_annotation = sig.return_annotation

    # Exceptions can represent legacy tools or specific cases
    # But generally we want str
    if module_name.endswith("execute_sql_query"):
        # this one might return str too now, let's check
        assert (
            return_annotation == "str" or return_annotation is str
        ), f"{module_name} handler should return str (JSON envelope)"
    elif module_name.endswith("interaction"):
        pass
    else:
        # Standard check for the ones I updated
        # Allow 'str' string or str type
        assert (
            return_annotation == "str" or return_annotation is str
        ), f"{module_name} handler return annotation {return_annotation} should be 'str'"


def test_envelope_structure_validity():
    """Verify ToolResponseEnvelope structure with sample data."""
    sample_json = """
    {
        "schema_version": "1.0",
        "result": {"foo": "bar"},
        "metadata": {
            "provider": "test",
            "execution_time_ms": 10.5
        }
    }
    """
    envelope = ToolResponseEnvelope.model_validate_json(sample_json)
    assert envelope.result == {"foo": "bar"}
    assert envelope.metadata.provider == "test"
    assert envelope.metadata.execution_time_ms == 10.5


def test_sql_envelope_structure_validity():
    """Verify ExecuteSQLQueryResponseEnvelope structure."""
    sample_json = """
    {
        "schema_version": "1.0",
        "rows": [{"id": 1}],
        "columns": [{"name": "id", "type": "integer"}],
        "metadata": {
            "rows_returned": 1,
            "execution_time_ms": 20.0,
            "is_truncated": false,
            "is_limited": false,
            "is_paginated": false
        }
    }
    """
    envelope = ExecuteSQLQueryResponseEnvelope.model_validate_json(sample_json)
    assert len(envelope.rows) == 1
    assert envelope.rows[0]["id"] == 1
    assert envelope.metadata.rows_returned == 1
