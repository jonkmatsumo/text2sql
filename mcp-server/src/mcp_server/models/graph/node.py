from typing import Any, Dict

from pydantic import BaseModel, Field


class Node(BaseModel):
    """Canonical graph node representation.

    All graph store implementations must convert their native node types
    to this canonical representation before returning to business logic.

    Attributes:
        id: Unique identifier (string for cross-backend compatibility).
        label: Node type/label (e.g., "Table", "Column").
        properties: Additional node properties.
    """

    id: str
    label: str
    properties: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": False}
