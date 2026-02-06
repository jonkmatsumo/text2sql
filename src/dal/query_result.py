from dataclasses import dataclass
from typing import Any, Dict, List, Optional

ColumnMeta = Dict[str, Any]


@dataclass
class QueryResult:
    """Container for query rows with optional column metadata."""

    rows: List[Dict[str, Any]]
    columns: Optional[List[ColumnMeta]] = None
    next_page_token: Optional[str] = None
    page_size: Optional[int] = None
    is_truncated: bool = False
    is_limited: bool = False
    partial_reason: Optional[str] = None
