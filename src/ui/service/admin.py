"""Admin Service for Streamlit app.

This service handles admin operations like reviewing interactions,
managing few-shot examples, and syncing with the registry.
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add agent to path (same as in agent.py)
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "agent" / "src"))


class AdminService:
    """Service for Admin Panel business logic."""

    @staticmethod
    async def list_pin_rules(tenant_id: int):
        """List all pin rules for a tenant."""
        from types import SimpleNamespace

        # Use MCP tool instead of direct DAL
        result = await AdminService._call_tool(
            "manage_pin_rules", {"operation": "list", "tenant_id": tenant_id}
        )

        if isinstance(result, dict) and "error" in result:
            raise Exception(result["error"])

        if not isinstance(result, list):
            return []

        # Convert dicts to SimpleNamespace for dot-notation access in UI
        return [SimpleNamespace(**r) for r in result]

    @staticmethod
    async def upsert_pin_rule(tenant_id: int, rule_id: Optional[str] = None, **kwargs):
        """Create or update a pin rule."""
        from types import SimpleNamespace

        payload = {"operation": "upsert", "tenant_id": tenant_id, "rule_id": rule_id, **kwargs}

        result = await AdminService._call_tool("manage_pin_rules", payload)

        if isinstance(result, dict) and "error" in result:
            raise Exception(result["error"])

        return SimpleNamespace(**result)

    @staticmethod
    async def delete_pin_rule(rule_id: str, tenant_id: int):
        """Delete a pin rule."""
        result = await AdminService._call_tool(
            "manage_pin_rules", {"operation": "delete", "rule_id": rule_id, "tenant_id": tenant_id}
        )

        if isinstance(result, dict) and "error" in result:
            raise Exception(result["error"])

        return result.get("success", False)

    @staticmethod
    async def _call_tool(tool_name: str, args: dict) -> Any:
        """Invoke an MCP admin tool safely.

        Handles ExceptionGroups from asyncio TaskGroups by unwrapping single-exception
        groups to surface meaningful error messages. Logs full traceback for debugging.
        """
        import logging

        from ui.utils.exceptions import is_single_exception_group, unwrap_single_exception_group

        logger = logging.getLogger(__name__)

        try:
            from agent.tools import mcp_tools_context, unpack_mcp_result

            async with mcp_tools_context() as tools:
                tool = next((t for t in tools if t.name == tool_name), None)
                if not tool:
                    return {"error": f"Tool {tool_name} not found"}

                result = await tool.ainvoke(args, config={})

            # Use central unpacking utility
            unpacked = unpack_mcp_result(result)

            # Robustness: Handle FastMCP/SDK wrappers like {"result": [...]}
            if (
                isinstance(unpacked, dict)
                and "result" in unpacked
                and len(unpacked) == 1
                and not tool_name == "lookup_cache"  # Preserve for specific tools if needed
            ):
                return unpacked["result"]

            return unpacked
        except Exception as e:
            # Handle ExceptionGroups from asyncio TaskGroups
            if is_single_exception_group(e):
                # Log the full traceback of the original exception group
                logger.exception(
                    "Tool '%s' raised ExceptionGroup with single root cause",
                    tool_name,
                )
                # Unwrap to get meaningful error message
                unwrapped = unwrap_single_exception_group(e)
                return {"error": str(unwrapped)}
            else:
                # Multi-exception groups or regular exceptions: preserve original behavior
                return {"error": str(e)}

    @classmethod
    async def list_interactions(
        cls, limit: int = 50, thumb_filter: str = "All", status_filter: str = "All"
    ) -> List[Dict]:
        """
        Fetch and filter recent interactions.

        Args:
            limit: Max number of interactions to fetch from backend
            thumb_filter: Filter by feedback ("All", "UP", "DOWN", "None")
            status_filter: Filter by status ("All", "PENDING", "APPROVED", "REJECTED")

        Returns:
            List of filtered interaction dictionaries
        """
        interactions = await cls._call_tool("list_interactions", {"limit": limit})

        if isinstance(interactions, dict) and "error" in interactions:
            # Propagate error as a list with one error item or handle in UI
            # For simplicity, returning empty list if error, or let UI handle?
            # Existing specific UI error handling suggests we might want to return the error dict.
            # But the signature says List[Dict].
            # Let's return the error dict wrapped in a list or raise.
            # The UI checks `isinstance(interactions, list)`.
            # If we return a dict, the UI code `if isinstance(interactions, list)` fails.
            # So if error, we probably want to return it or raise it.
            # Let's return the simplified list, but if error, maybe raise?
            # Or better, return the robust structure.
            # Let's return the raw result if it's an error dict so the UI can check.
            return interactions

        if not isinstance(interactions, list):
            return {"error": f"Unexpected response format: {interactions}"}

        # Filtering logic
        filtered = []
        for i in interactions:
            # Apply thumb filter
            thumb = i.get("thumb")
            if thumb_filter == "UP" and thumb != "UP":
                continue
            if thumb_filter == "DOWN" and thumb != "DOWN":
                continue
            if thumb_filter == "None" and (thumb or thumb == "UP" or thumb == "DOWN"):
                # "None" means empty, None, or "-"
                # The original logic: df["thumb"].isna() |
                # (df["thumb"] == "-") | (df["thumb"] == "")
                if thumb and thumb not in ["-", ""]:
                    continue

            # Apply status filter
            if status_filter != "All" and i.get("execution_status") != status_filter:
                continue

            filtered.append(i)

        # Sort by created_at desc (lexicographical sort works for ISO strings)
        filtered.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return filtered

    @classmethod
    async def get_interaction_details(cls, interaction_id: str) -> Dict:
        """Get full details for a single interaction."""
        return await cls._call_tool("get_interaction_details", {"interaction_id": interaction_id})

    @classmethod
    async def approve_interaction(
        cls,
        interaction_id: str,
        corrected_sql: str,
        original_sql: str,
        notes: str = "",
    ) -> str:
        """
        Approve an interaction, optionally fixing the SQL.

        Returns:
            "OK" on success, or error message.
        """
        resolution_type = (
            "APPROVED_AS_IS" if corrected_sql == original_sql else "APPROVED_WITH_SQL_FIX"
        )
        res = await cls._call_tool(
            "approve_interaction",
            {
                "interaction_id": interaction_id,
                "corrected_sql": corrected_sql,
                "resolution_type": resolution_type,
                "reviewer_notes": notes,
            },
        )
        return res

    @classmethod
    async def reject_interaction(
        cls, interaction_id: str, reason: str = "CANNOT_FIX", notes: str = ""
    ) -> str:
        """Reject an interaction."""
        res = await cls._call_tool(
            "reject_interaction",
            {
                "interaction_id": interaction_id,
                "reason": reason,
                "reviewer_notes": notes,
            },
        )
        return res

    @classmethod
    async def export_approved_to_fewshot(cls, limit: int = 50) -> Dict:
        """Sync approved examples to the few-shot registry."""
        return await cls._call_tool("export_approved_to_fewshot", {"limit": limit})

    @classmethod
    async def list_approved_examples(cls, limit: int = 100, search_query: str = None) -> List[Dict]:
        """
        List currently approved few-shot examples.

        Args:
            limit: Max examples to fetch
            search_query: Optional string to filter by distinct question or SQL

        Returns:
            List of approved examples
        """
        examples = await cls._call_tool("list_approved_examples", {"limit": limit})

        if not isinstance(examples, list):
            return examples

        if not search_query:
            return examples

        # Search filtering
        query = search_query.lower()
        filtered = [
            ex
            for ex in examples
            if query in ex.get("question", "").lower() or query in ex.get("sql_query", "").lower()
        ]
        return filtered

    @classmethod
    async def get_recommendations(
        cls, query: str, tenant_id: int, limit: int, enable_fallback: bool
    ) -> Dict:
        """
        Run recommendations for inspection.

        Returns the raw tool output including metadata.
        """
        return await cls._call_tool(
            "recommend_examples",
            {
                "query": query,
                "tenant_id": tenant_id,
                "limit": limit,
                "enable_fallback": enable_fallback,
            },
        )
