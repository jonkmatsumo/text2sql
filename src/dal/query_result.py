from dataclasses import dataclass
from typing import Any, Dict, List, Optional

ColumnMeta = Dict[str, Any]


@dataclass
class QueryResult:
    """Container for query rows with optional column metadata."""

    rows: List[Dict[str, Any]]
    columns: Optional[List[ColumnMeta]] = None
