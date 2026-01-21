from typing import Any, Dict

from pydantic import BaseModel, Field


class Edge(BaseModel):
    """Canonical graph edge representation.

    All graph store implementations must convert their native edge types
    to this canonical representation before returning to business logic.

    Attributes:
        source_id: ID of the source node.
        target_id: ID of the target node.
        type: Relationship type (e.g., "HAS_COLUMN", "FOREIGN_KEY_TO").
        properties: Additional edge properties.
    """

    source_id: str
    target_id: str
    type: str
    properties: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": False}
