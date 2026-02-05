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
    next_page_token: Optional[str] = None
    partial_reason: Optional[str] = None

    def to_dict(self) -> dict:
        """Serialize to a JSON-serializable dict."""
        return asdict(self)

    @staticmethod
    def from_parts(
        rows_returned: int,
        is_truncated: bool,
        is_limited: bool,
        row_limit: Optional[int],
        next_page_token: Optional[str] = None,
    ) -> "ResultCompleteness":
        """Construct a completeness model from raw metadata."""
        if next_page_token:
            reason = PartialReason.PAGINATED.value
        elif is_truncated:
            reason = PartialReason.TRUNCATED.value
        elif is_limited:
            reason = PartialReason.LIMITED.value
        else:
            reason = None

        return ResultCompleteness(
            rows_returned=rows_returned,
            is_truncated=is_truncated,
            is_limited=is_limited,
            row_limit=row_limit,
            next_page_token=next_page_token,
            partial_reason=reason,
        )
