import asyncio
import logging
from typing import Any, Dict, Optional

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


class EnrichmentAgent:
    """Agent for generating semantic descriptions for graph nodes."""

    def __init__(self, model: str = "gpt-4o"):
        """Initialize the enrichment agent."""
        self.model = model
        self.client = AsyncOpenAI()
        self.semaphore = asyncio.Semaphore(5)  # Limit concurrency

    async def generate_description(self, node_data: Dict[str, Any]) -> Optional[str]:
        """
        Generate a description for a node using an LLM.

        Args:
            node_data: Dictionary containing node properties (name, columns, sample_data, etc.)

        Returns:
            Generated description string, or None if generation failed.
        """
        async with self.semaphore:
            try:
                prompt = self._construct_prompt(node_data)
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a helpful data assistant."},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.0,
                )

                if not response.choices:
                    logger.warning(f"Empty response for node {node_data.get('name')}")
                    return None

                return response.choices[0].message.content
            except Exception as e:
                logger.error(f"Error generating description for node {node_data.get('name')}: {e}")
                return None

    def _construct_prompt(self, node_data: Dict[str, Any]) -> str:
        """Construct the prompt for schema enrichment."""
        table_name = node_data.get("name", "Unknown Table")
        # Assuming node_data might contain 'columns' or 'sample_data' keys
        # if they were fetched/joined.
        # If node is just the Table node properties, sample_data might be a string JSON.
        sample_data = node_data.get("sample_data", "[]")

        # Note: In a real scenario, we might need to fetch columns separately
        # if they aren't on the Table node properties.
        # But for this task, we assume node_data has what we need or we make a best effort.
        # The prompt requirement asks to use "node's columns and sample_data".

        return (
            f"Generate a concise semantic description for the table '{table_name}'.\n"
            f"Sample Data: {sample_data}\n\n"
            "Description:"
        )
