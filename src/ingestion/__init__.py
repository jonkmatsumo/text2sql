"""Services for data ingestion and vector indexing."""

__version__ = "0.1.0"

# Expose vector_indexes for test patching and registry wiring.
from . import patterns  # noqa: F401
from . import vector_indexes  # noqa: F401
