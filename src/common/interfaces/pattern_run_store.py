from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from uuid import UUID


class PatternRunStore(ABC):
    """Interface for persisting NLP pattern generation runs."""

    @abstractmethod
    async def create_run(
        self,
        status: str,
        target_table: Optional[str] = None,
        config_snapshot: Optional[Dict[str, Any]] = None,
    ) -> UUID:
        """Create a new run record."""
        pass

    @abstractmethod
    async def update_run(
        self,
        run_id: UUID,
        status: str,
        completed_at: Optional[Any] = None,
        error_message: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update an existing run record."""
        pass

    @abstractmethod
    async def add_run_items(self, run_id: UUID, items: List[Dict[str, Any]]) -> None:
        """Add associated pattern items to a run.

        Args:
            run_id: The run UUID.
            items: List of dicts with keys:
                   - pattern_id: str (Canonical ID)
                   - label: str
                   - pattern: str
                   - action: str ('CREATED', 'UPDATED', 'UNCHANGED')
        """
        pass

    @abstractmethod
    async def list_runs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """List recent runs."""
        pass

    @abstractmethod
    async def get_run(self, run_id: UUID) -> Optional[Dict[str, Any]]:
        """Get run details."""
        pass

    @abstractmethod
    async def get_run_items(self, run_id: UUID) -> List[Dict[str, Any]]:
        """Get items associated with a run."""
        pass
