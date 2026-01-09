from typing import List


def _format_vector(embedding: List[float]) -> str:
    """Format Python list as PostgreSQL vector string."""
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"
