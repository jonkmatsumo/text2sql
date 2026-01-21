from typing import Any, Dict

from pydantic import BaseModel, Field


class CacheLookupResult(BaseModel):
    """Result from a cache lookup operation.

    Returned by CacheStore.lookup() when a cache hit is found.

    Attributes:
        cache_id: Internal database key for the cache entry.
        value: The cached value (e.g., generated SQL).
        similarity: Similarity score (0.0 to 1.0).
        metadata: Additional metadata about the cache entry.
    """

    cache_id: str
    value: str
    similarity: float = Field(ge=0.0, le=1.0)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": False}
