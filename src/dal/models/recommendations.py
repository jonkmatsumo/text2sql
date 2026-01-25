from dataclasses import dataclass
from datetime import datetime
from typing import List
from uuid import UUID


@dataclass
class PinRule:
    """Dataclass for pinned recommendation rules."""

    id: UUID
    tenant_id: int
    match_type: str  # 'exact', 'contains'
    match_value: str
    registry_example_ids: List[str]  # List of UUIDs as strings
    priority: int
    enabled: bool
    created_at: datetime
    updated_at: datetime
