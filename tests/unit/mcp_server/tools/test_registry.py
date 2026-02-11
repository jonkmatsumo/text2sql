"""Tests for the MCP tools registry."""

from common.models.tool_versions import (
    DEFAULT_TOOL_VERSION,
    TOOL_VERSION_REGISTRY,
    get_tool_version,
)
from mcp_server.tools.registry import CANONICAL_TOOLS, get_all_tool_names, validate_tool_names


class TestRegistry:
    """Tests for the tools registry."""

    def test_canonical_tools_no_tool_suffix(self):
        """Verify no canonical tool names end with '_tool'."""
        invalid = [name for name in CANONICAL_TOOLS if name.endswith("_tool")]
        assert invalid == [], f"Tool names must not end with '_tool': {invalid}"

    def test_validate_tool_names_passes(self):
        """Verify validate_tool_names passes for current tools."""
        assert validate_tool_names() is True

    def test_get_all_tool_names_returns_sorted_list(self):
        """Verify get_all_tool_names returns sorted list."""
        names = get_all_tool_names()
        assert names == sorted(names)
        assert len(names) == len(CANONICAL_TOOLS)

    def test_canonical_tools_contains_expected_tools(self):
        """Verify all expected tools are in the canonical set."""
        expected = {
            "list_tables",
            "get_table_schema",
            "get_sample_data",
            "search_relevant_tables",
            "get_semantic_subgraph",
            "get_semantic_definitions",
            "execute_sql_query",
            "resolve_ambiguity",
            "lookup_cache",
            "update_cache",
            "get_few_shot_examples",
            "save_conversation_state",
            "load_conversation_state",
            "create_interaction",
            "update_interaction",
            "submit_feedback",
            "list_interactions",
            "get_interaction_details",
            "approve_interaction",
            "reject_interaction",
            "export_approved_to_fewshot",
            "list_approved_examples",
        }
        assert expected.issubset(CANONICAL_TOOLS)

    def test_tool_version_registry_covers_all_canonical_tools(self):
        """Version registry must stay aligned with canonical tool list."""
        assert set(TOOL_VERSION_REGISTRY.keys()) == set(CANONICAL_TOOLS)

    def test_tool_version_registry_defaults_to_v1_for_all_canonical_tools(self):
        """Current tool contracts are pinned at v1 unless explicitly version-bumped."""
        for tool_name in CANONICAL_TOOLS:
            assert get_tool_version(tool_name) == DEFAULT_TOOL_VERSION


class TestToolModuleStructure:
    """Tests for tool module structure."""

    def test_list_tables_module_exports(self):
        """Verify list_tables module exports TOOL_NAME and handler."""
        from mcp_server.tools.list_tables import TOOL_NAME, handler

        assert TOOL_NAME == "list_tables"
        assert callable(handler)

    def test_get_table_schema_module_exports(self):
        """Verify get_table_schema module exports TOOL_NAME and handler."""
        from mcp_server.tools.get_table_schema import TOOL_NAME, handler

        assert TOOL_NAME == "get_table_schema"
        assert callable(handler)

    def test_execute_sql_query_module_exports(self):
        """Verify execute_sql_query module exports TOOL_NAME and handler."""
        from mcp_server.tools.execute_sql_query import TOOL_NAME, handler

        assert TOOL_NAME == "execute_sql_query"
        assert callable(handler)

    def test_lookup_cache_module_exports(self):
        """Verify lookup_cache module exports TOOL_NAME and handler."""
        from mcp_server.tools.lookup_cache import TOOL_NAME, handler

        assert TOOL_NAME == "lookup_cache"
        assert callable(handler)

    def test_get_few_shot_examples_module_exports(self):
        """Verify get_few_shot_examples module exports TOOL_NAME and handler."""
        from mcp_server.tools.get_few_shot_examples import TOOL_NAME, handler

        assert TOOL_NAME == "get_few_shot_examples"
        assert callable(handler)

    def test_submit_feedback_module_exports(self):
        """Verify submit_feedback module exports TOOL_NAME and handler."""
        from mcp_server.tools.feedback.submit_feedback import TOOL_NAME, handler

        assert TOOL_NAME == "submit_feedback"
        assert callable(handler)

    def test_admin_tools_module_exports(self):
        """Verify admin tools export correctly."""
        from mcp_server.tools.admin.approve_interaction import TOOL_NAME as approve_name
        from mcp_server.tools.admin.list_interactions import TOOL_NAME as list_name
        from mcp_server.tools.admin.reject_interaction import TOOL_NAME as reject_name

        assert list_name == "list_interactions"
        assert approve_name == "approve_interaction"
        assert reject_name == "reject_interaction"

    def test_interaction_tools_module_exports(self):
        """Verify interaction tools export correctly."""
        from mcp_server.tools.interaction.create_interaction import TOOL_NAME as create_name
        from mcp_server.tools.interaction.update_interaction import TOOL_NAME as update_name

        assert create_name == "create_interaction"
        assert update_name == "update_interaction"

    def test_conversation_tools_module_exports(self):
        """Verify conversation tools export correctly."""
        from mcp_server.tools.conversation.load_conversation_state import TOOL_NAME as load_name
        from mcp_server.tools.conversation.save_conversation_state import TOOL_NAME as save_name

        assert save_name == "save_conversation_state"
        assert load_name == "load_conversation_state"
