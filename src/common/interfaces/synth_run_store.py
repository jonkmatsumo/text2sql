from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID


class SynthRunStore(ABC):
    """Interface for synthetic data generation run storage."""

    @abstractmethod
    async def create_run(
        self,
        config_snapshot: Dict[str, Any],
        output_path: Optional[str] = None,
        status: str = "PENDING",
        job_id: Optional[UUID] = None,
    ) -> UUID:
        """Create a new synthetic generation run record."""
        pass

    @abstractmethod
    async def update_run(
        self,
        run_id: UUID,
        status: Optional[str] = None,
        completed_at: Optional[datetime] = None,
        manifest: Optional[Dict[str, Any]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None,
        ui_state: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update an existing synthetic generation run record."""
        pass

    @abstractmethod
    async def get_run(self, run_id: UUID) -> Optional[Dict[str, Any]]:
        """Fetch a specific run record by ID."""
        pass

    @abstractmethod
    async def list_runs(
        self, limit: int = 20, status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List recent synthetic generation runs."""
        pass
