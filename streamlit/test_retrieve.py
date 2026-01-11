import asyncio

from agent_core.nodes.retrieve import retrieve_context_node
from langchain_core.messages import HumanMessage


async def test_retrieve():
    """Test retrieval node capabilities."""
    print("Testing retrieval...")
    state = {
        "messages": [
            HumanMessage(
                content=(
                    "What is the average length of films with MPAA rating of PG "
                    "and originally Spanish?"
                )
            )
        ],
        "tenant_id": 1,
    }
    try:
        result = await retrieve_context_node(state)
        # Verify schema context
        raw_ctx = result.get("raw_schema_context", [])
        print(f"Raw Schema Context Len: {len(raw_ctx)}")
        if len(raw_ctx) == 0:
            print("FAILURE: raw_schema_context is empty.")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(test_retrieve())
