from typing import Any, Dict, Union

# Type alias for structured filtering.
# Supports operators like {"category": {"$eq": "billing"}}
# Adapters will transpile this to backend-specific syntax (SQL WHERE, Pinecone filter, etc.)
#
# Examples:
#   - Simple equality: {"category": "billing"}
#   - With operator: {"price": {"$gt": 100}}
#   - Multiple conditions: {"category": "billing", "status": {"$in": ["active", "pending"]}}
FilterCriteria = Dict[str, Union[str, int, float, bool, Dict[str, Any]]]
