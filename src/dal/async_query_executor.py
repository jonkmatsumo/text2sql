from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, Union, runtime_checkable


class QueryStatus(str, Enum):
    """Normalized async query lifecycle states."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@runtime_checkable
class AsyncQueryExecutor(Protocol):
    """Protocol for async/job-style query execution."""

    async def submit(
        self, sql: str, params: Optional[Union[Dict[str, Any], List[Any]]] = None
    ) -> str:
        """Submit a query for asynchronous execution and return a job ID."""
        ...

    async def poll(self, job_id: str) -> QueryStatus:
        """Poll the status of an in-flight query."""
        ...

    async def fetch(self, job_id: str, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch results for a completed query."""
        ...

    async def cancel(self, job_id: str) -> None:
        """Cancel a running query."""
        ...
