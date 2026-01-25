from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class TraceSummary(BaseModel):
    """Summary representation of a trace for list endpoints."""

    trace_id: str
    service_name: str
    start_time: datetime
    end_time: datetime
    duration_ms: int
    span_count: int
    status: str
    raw_blob_url: Optional[str] = None


class TraceDetail(TraceSummary):
    """Detailed trace representation with optional attributes."""

    resource_attributes: Optional[dict] = None
    trace_attributes: Optional[dict] = None


class SpanSummary(BaseModel):
    """Summary representation of a span."""

    span_id: str
    trace_id: str
    parent_span_id: Optional[str] = None
    name: str
    kind: str
    status_code: str
    status_message: Optional[str] = None
    start_time: datetime
    end_time: datetime
    duration_ms: int


class PaginatedTracesResponse(BaseModel):
    """Paginated response containing trace summaries."""

    items: List[TraceSummary]
    total: int
    next_offset: Optional[int] = None


class PaginatedSpansResponse(BaseModel):
    """Paginated response containing span summaries."""

    items: List[SpanSummary]
    total: int
    next_offset: Optional[int] = None
