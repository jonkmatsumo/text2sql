from datetime import datetime
from typing import Any, Dict, List, Optional

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
    error_count: Optional[int] = None
    raw_blob_url: Optional[str] = None


class TraceDetail(TraceSummary):
    """Detailed trace representation with optional attributes."""

    resource_attributes: Optional[dict] = None
    trace_attributes: Optional[dict] = None
    total_tokens: Optional[int] = None
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    model_name: Optional[str] = None
    estimated_cost_usd: Optional[float] = None


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
    self_time_ms: Optional[int] = None
    span_attributes: Optional[Dict[str, Any]] = None
    events: Optional[List[Dict[str, Any]]] = None


class SpanDetail(SpanSummary):
    """Detailed representation of a span with related data."""

    links: Optional[List[Dict[str, Any]]] = None
    payloads: Optional[List[Dict[str, Any]]] = None


class DurationHistogramBin(BaseModel):
    """Histogram bin for duration distribution."""

    start_ms: int
    end_ms: int
    count: int


class TraceAggregationFacets(BaseModel):
    """Facet counts returned from trace aggregations."""

    service: Dict[str, int]
    status: Dict[str, int]
    error: Dict[str, int]


class TraceAggregationPercentiles(BaseModel):
    """Duration percentiles for traces."""

    p50_ms: Optional[float] = None
    p95_ms: Optional[float] = None
    p99_ms: Optional[float] = None


class TraceAggregationSampling(BaseModel):
    """Sampling metadata for aggregations."""

    is_sampled: bool = False
    sample_rate: Optional[float] = None


class TraceAggregationTruncation(BaseModel):
    """Truncation metadata for aggregations."""

    is_truncated: bool = False
    limit: Optional[int] = None


class TraceAggregationsResponse(BaseModel):
    """Aggregated trace search metadata."""

    total_count: int
    facet_counts: TraceAggregationFacets
    duration_histogram: List[DurationHistogramBin]
    percentiles: TraceAggregationPercentiles
    sampling: TraceAggregationSampling
    truncation: TraceAggregationTruncation
    as_of: datetime
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None


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


class MetricsBucket(BaseModel):
    """Aggregated metrics for a single time bucket."""

    timestamp: datetime
    count: int
    error_count: int
    avg_duration: Optional[float] = None


class MetricsSummary(BaseModel):
    """Overall aggregated metrics for a time window."""

    total_count: int
    error_count: int
    avg_duration: Optional[float] = None
    p95_duration: Optional[float] = None


class MetricsPreviewResponse(BaseModel):
    """Response containing aggregated metrics for preview."""

    summary: MetricsSummary
    timeseries: List[MetricsBucket]
    window_minutes: int
    start_time: datetime
