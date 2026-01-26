"""Operations Service for Streamlit Admin Panel."""

import logging
import os
from typing import AsyncGenerator

import httpx

logger = logging.getLogger(__name__)

UI_API_URL = os.getenv("UI_API_URL", "http://localhost:8082")


class OpsService:
    """Bridge service for Admin Operations."""

    @staticmethod
    async def run_pattern_generation(dry_run: bool = False) -> AsyncGenerator[str, None]:
        """Run pattern generation via UI API and yield status."""
        yield "Requesting pattern generation from UI API..."

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{UI_API_URL}/ops/patterns/generate", json={"dry_run": dry_run}
            )
            response.raise_for_status()
            result = response.json()

        if isinstance(result, dict) and result.get("success"):
            yield f"✓ Pattern generation completed (Run ID: {result.get('run_id')})"
            metrics = result.get("metrics", {})
            if metrics:
                yield f"Generated: {metrics.get('generated_count', 0)}"
                yield f"Created: {metrics.get('created_count', 0)}"
                yield f"Updated: {metrics.get('updated_count', 0)}"
            yield "Patterns successfully saved."
        else:
            error = result.get("error") if isinstance(result, dict) else str(result)
            yield f"✗ Pattern generation failed: {error}"

    @staticmethod
    async def run_schema_hydration() -> AsyncGenerator[str, None]:
        """Run schema hydration and yield logs."""
        yield "Schema hydration migration pending (MCP tool route required)."
        yield "Error: Not yet implemented via MCP tool."

    @staticmethod
    async def run_cache_reindexing() -> AsyncGenerator[str, None]:
        """Run cache re-indexing and yield logs."""
        yield "Cache re-indexing migration pending (MCP tool route required)."
        yield "Error: Not yet implemented via MCP tool."

    @staticmethod
    async def reload_patterns() -> dict:
        """Trigger backend pattern reload via UI API and return result."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(f"{UI_API_URL}/ops/patterns/reload")
            response.raise_for_status()
            result = response.json()

        if isinstance(result, dict) and "error" in result and result["error"]:
            return {
                "success": False,
                "message": f"Reload failed: {result['error']}",
            }

        return {
            "success": result.get("success", False) if isinstance(result, dict) else False,
            "message": (
                "Patterns reloaded successfully." if result.get("success") else "Reload failed."
            ),
            "reload_id": result.get("reload_id"),
            "duration_ms": result.get("duration_ms"),
            "pattern_count": result.get("pattern_count"),
        }
