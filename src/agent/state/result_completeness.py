from dataclasses import asdict, dataclass
from enum import Enum
from typing import Optional


class PartialReason(str, Enum):
    """Reasons why a result set may be partial."""

    TRUNCATED = "TRUNCATED"
    LIMITED = "LIMITED"
    PAGINATED = "PAGINATED"
    PROVIDER_CAP = "PROVIDER_CAP"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ResultCompleteness:
    """Normalized completeness metadata for query results."""

    rows_returned: int
    is_truncated: bool
    is_limited: bool
    row_limit: Optional[int] = None
    query_limit: Optional[int] = None
    next_page_token: Optional[str] = None
    page_size: Optional[int] = None
    partial_reason: Optional[str] = None
    cap_detected: bool = False
    cap_mitigation_applied: bool = False
    cap_mitigation_mode: Optional[str] = None
    auto_paginated: bool = False
    pages_fetched: int = 1
    auto_pagination_stopped_reason: Optional[str] = None
    prefetch_enabled: bool = False
    prefetch_scheduled: bool = False
    prefetch_reason: Optional[str] = None

    def to_dict(self) -> dict:
        """Serialize to a JSON-serializable dict."""
        return asdict(self)

    @staticmethod
    def from_parts(
        rows_returned: int,
        is_truncated: bool,
        is_limited: bool,
        row_limit: Optional[int],
        query_limit: Optional[int] = None,
        next_page_token: Optional[str] = None,
        page_size: Optional[int] = None,
        partial_reason: Optional[str] = None,
        cap_detected: bool = False,
        cap_mitigation_applied: bool = False,
        cap_mitigation_mode: Optional[str] = None,
        auto_paginated: bool = False,
        pages_fetched: int = 1,
        auto_pagination_stopped_reason: Optional[str] = None,
        prefetch_enabled: bool = False,
        prefetch_scheduled: bool = False,
        prefetch_reason: Optional[str] = None,
    ) -> "ResultCompleteness":
        """Construct a completeness model from raw metadata."""
        reason = None
        if partial_reason:
            try:
                reason = PartialReason(partial_reason).value
            except ValueError:
                reason = None

        if reason is None:
            if next_page_token:
                reason = PartialReason.PAGINATED.value
            elif is_truncated:
                reason = PartialReason.TRUNCATED.value
            elif is_limited:
                reason = PartialReason.LIMITED.value

        return ResultCompleteness(
            rows_returned=rows_returned,
            is_truncated=is_truncated,
            is_limited=is_limited,
            row_limit=row_limit,
            query_limit=query_limit,
            next_page_token=next_page_token,
            page_size=page_size,
            partial_reason=reason,
            cap_detected=bool(cap_detected),
            cap_mitigation_applied=bool(cap_mitigation_applied),
            cap_mitigation_mode=cap_mitigation_mode,
            auto_paginated=bool(auto_paginated),
            pages_fetched=max(1, int(pages_fetched)),
            auto_pagination_stopped_reason=auto_pagination_stopped_reason,
            prefetch_enabled=bool(prefetch_enabled),
            prefetch_scheduled=bool(prefetch_scheduled),
            prefetch_reason=prefetch_reason,
        )
