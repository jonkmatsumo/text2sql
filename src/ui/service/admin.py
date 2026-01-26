"""Admin Service for Streamlit app using HTTP backends."""

import os
from typing import Any, Dict, List, Optional

import httpx

UI_API_URL = os.getenv("UI_API_URL", "http://localhost:8082")


class AdminService:
    """Service for Admin Panel business logic."""

    @staticmethod
    async def _request(
        method: str,
        path: str,
        params: dict | None = None,
        json: dict | None = None,
    ) -> Any:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(method, f"{UI_API_URL}{path}", params=params, json=json)
            response.raise_for_status()
            return response.json()

    @staticmethod
    async def list_pin_rules(tenant_id: int):
        """List all pin rules for a tenant."""
        from types import SimpleNamespace

        result = await AdminService._request("GET", "/pins", params={"tenant_id": tenant_id})

        if isinstance(result, dict) and "error" in result:
            raise Exception(result["error"])

        if not isinstance(result, list):
            return []

        return [SimpleNamespace(**r) for r in result]

    @staticmethod
    async def upsert_pin_rule(tenant_id: int, rule_id: Optional[str] = None, **kwargs):
        """Create or update a pin rule."""
        from types import SimpleNamespace

        payload = {"tenant_id": tenant_id, **kwargs}
        if rule_id:
            result = await AdminService._request("PATCH", f"/pins/{rule_id}", json=payload)
        else:
            result = await AdminService._request("POST", "/pins", json=payload)

        if isinstance(result, dict) and "error" in result:
            raise Exception(result["error"])

        return SimpleNamespace(**result)

    @staticmethod
    async def delete_pin_rule(rule_id: str, tenant_id: int):
        """Delete a pin rule."""
        result = await AdminService._request(
            "DELETE", f"/pins/{rule_id}", params={"tenant_id": tenant_id}
        )

        if isinstance(result, dict) and "error" in result:
            raise Exception(result["error"])

        return result.get("success", False)

    @classmethod
    async def list_interactions(
        cls, limit: int = 50, thumb_filter: str = "All", status_filter: str = "All"
    ) -> List[Dict]:
        """Fetch recent interactions with optional filters."""
        interactions = await cls._request(
            "GET",
            "/interactions",
            params={
                "limit": limit,
                "thumb": thumb_filter,
                "status": status_filter,
            },
        )

        if isinstance(interactions, dict) and "error" in interactions:
            return interactions

        if not isinstance(interactions, list):
            return {"error": f"Unexpected response format: {interactions}"}

        return interactions

    @classmethod
    async def get_interaction_details(cls, interaction_id: str) -> Dict:
        """Get full details for a single interaction."""
        return await cls._request("GET", f"/interactions/{interaction_id}")

    @classmethod
    async def approve_interaction(
        cls,
        interaction_id: str,
        corrected_sql: str,
        original_sql: str,
        notes: str = "",
    ) -> str:
        """Approve an interaction, optionally fixing the SQL."""
        res = await cls._request(
            "POST",
            f"/interactions/{interaction_id}/approve",
            json={
                "corrected_sql": corrected_sql,
                "original_sql": original_sql,
                "notes": notes,
            },
        )
        return res

    @classmethod
    async def reject_interaction(
        cls, interaction_id: str, reason: str = "CANNOT_FIX", notes: str = ""
    ) -> str:
        """Reject an interaction."""
        res = await cls._request(
            "POST",
            f"/interactions/{interaction_id}/reject",
            json={"reason": reason, "notes": notes},
        )
        return res

    @classmethod
    async def export_approved_to_fewshot(cls, limit: int = 50) -> Dict:
        """Sync approved examples to the few-shot registry."""
        return await cls._request(
            "POST",
            "/registry/publish-approved",
            json={"limit": limit},
        )

    @classmethod
    async def list_approved_examples(cls, limit: int = 100, search_query: str = None) -> List[Dict]:
        """List approved few-shot examples."""
        examples = await cls._request("GET", "/registry/examples", params={"limit": limit})

        if not isinstance(examples, list):
            return examples

        if not search_query:
            return examples

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
        """Run recommendations for inspection."""
        return await cls._request(
            "POST",
            "/recommendations/run",
            json={
                "query": query,
                "tenant_id": tenant_id,
                "limit": limit,
                "enable_fallback": enable_fallback,
            },
        )
