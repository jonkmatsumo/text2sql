"""Evaluation Store Interface."""

from abc import ABC, abstractmethod
from typing import List, Optional

from schema.evaluation.models import EvaluationCaseResultCreate, EvaluationRun, EvaluationRunCreate


class EvaluationStore(ABC):
    """Abstract interface for storing evaluation runs and results."""

    @abstractmethod
    async def create_run(self, run: EvaluationRunCreate) -> EvaluationRun:
        """Create a new evaluation run record.

        Args:
            run: Creation payload.

        Returns:
            The created EvaluationRun with ID and timestamps.
        """
        ...

    @abstractmethod
    async def update_run(self, run: EvaluationRun) -> None:
        """Update an existing evaluation run (status, summary, etc).

        Args:
            run: The updated run object. ID must match an existing record.
        """
        ...

    @abstractmethod
    async def get_run(self, run_id: str) -> Optional[EvaluationRun]:
        """Retrieve a run by ID.

        Args:
            run_id: The run identifier.

        Returns:
            EvaluationRun if found, else None.
        """
        ...

    @abstractmethod
    async def save_case_results(self, results: List[EvaluationCaseResultCreate]) -> None:
        """Bulk save evaluation results.

        Args:
            results: List of result creation payloads.
        """
        ...
