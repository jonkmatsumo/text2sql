from typing import Any, Dict, List, Optional
from uuid import UUID

from dal.factory import get_pattern_run_store


class PatternAuditingService:
    """Service for retrieving and comparing pattern generation runs."""

    @staticmethod
    async def list_runs(limit: int = 50) -> List[Dict[str, Any]]:
        """List recent runs."""
        return await get_pattern_run_store().list_runs(limit)

    @staticmethod
    async def get_run_details(run_id: UUID) -> Optional[Dict[str, Any]]:
        """Get run details including items."""
        store = get_pattern_run_store()
        run = await store.get_run(run_id)
        if not run:
            return None
        items = await store.get_run_items(run_id)
        return {"run": run, "items": items}

    @staticmethod
    async def compare_runs(run_id_a: UUID, run_id_b: UUID) -> Dict[str, Any]:
        """Compare two runs and return the delta (Added/Removed patterns)."""
        store = get_pattern_run_store()
        items_a = await store.get_run_items(run_id_a)
        items_b = await store.get_run_items(run_id_b)

        # Create sets of (label, pattern) -> item
        # Note: keys are (pattern_label, pattern_text)
        map_a = {(i["pattern_label"], i["pattern_text"]): i for i in items_a}
        map_b = {(i["pattern_label"], i["pattern_text"]): i for i in items_b}

        set_a = set(map_a.keys())
        set_b = set(map_b.keys())

        added = [map_b[k] for k in (set_b - set_a)]
        removed = [map_a[k] for k in (set_a - set_b)]
        common_count = len(set_a & set_b)

        return {
            "run_a_id": str(run_id_a),
            "run_b_id": str(run_id_b),
            "added_count": len(added),
            "removed_count": len(removed),
            "common_count": common_count,
            "added_items": added,
            "removed_items": removed,
        }
