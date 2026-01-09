from typing import List, Protocol, runtime_checkable

from mcp_server.models import Example


@runtime_checkable
class ExampleStore(Protocol):
    """Protocol for accessing few-shot learning examples.

    This abstracts the source of examples (Postgres, CSV, API, etc.)
    from the retrieval logic.
    """

    async def fetch_all_examples(self) -> List[Example]:
        """Fetch all available examples.

        Returns:
            List of canonical Example objects.
        """
        ...
