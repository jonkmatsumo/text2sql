from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional
from uuid import UUID


class QueryTargetConfigStatus(str, Enum):
    """Lifecycle status for query-target configuration."""

    INACTIVE = "inactive"
    PENDING = "pending"
    ACTIVE = "active"
    UNHEALTHY = "unhealthy"


@dataclass(frozen=True)
class QueryTargetConfigRecord:
    """Persisted query-target configuration metadata."""

    id: UUID
    provider: str
    metadata: Dict[str, Any]
    auth: Dict[str, Any]
    guardrails: Dict[str, Any]
    status: QueryTargetConfigStatus
    last_tested_at: Optional[str] = None
    last_test_status: Optional[str] = None
    last_error_code: Optional[str] = None
    last_error_message: Optional[str] = None
