"""End-to-end agent workflow verification."""

import asyncio

from langchain_core.messages import HumanMessage

from agent_core.graph import app


async def test_agent():
    """Test the complete agent workflow."""
    # Test case 1: Simple query
    print("=" * 60)
    print("Test 1: Simple Count Query")
    print("=" * 60)

    inputs = {
        "messages": [HumanMessage(content="How many films are in the database?")],
        "schema_context": "",
        "current_sql": None,
        "query_result": None,
        "error": None,
        "retry_count": 0,
    }

    async for event in app.astream(inputs):
        for node_name, node_output in event.items():
            print(f"\n[Node: {node_name}]")
            if "current_sql" in node_output:
                print(f"  SQL: {node_output['current_sql']}")
            if "query_result" in node_output and node_output["query_result"]:
                print(f"  Results: {len(node_output['query_result'])} rows")
            if "error" in node_output and node_output["error"]:
                print(f"  Error: {node_output['error']}")
            if "retry_count" in node_output:
                print(f"  Retry Count: {node_output['retry_count']}")
            if "messages" in node_output and node_output["messages"]:
                last_message = node_output["messages"][-1]
                if hasattr(last_message, "content"):
                    print(f"  Response: {last_message.content[:100]}...")

    # Test case 2: Query that might fail initially (tests self-correction)
    print("\n" + "=" * 60)
    print("Test 2: Query with Potential Error (Self-Correction)")
    print("=" * 60)

    inputs2 = {
        "messages": [HumanMessage(content="Show me all actors")],
        "schema_context": "",
        "current_sql": None,
        "query_result": None,
        "error": None,
        "retry_count": 0,
    }

    async for event in app.astream(inputs2):
        for node_name, node_output in event.items():
            print(f"\n[Node: {node_name}]")
            if "current_sql" in node_output:
                print(f"  SQL: {node_output['current_sql']}")
            if "error" in node_output and node_output["error"]:
                print(f"  Error: {node_output['error']}")
            if "retry_count" in node_output:
                print(f"  Retry: {node_output['retry_count']}")
            if "messages" in node_output and node_output["messages"]:
                last_message = node_output["messages"][-1]
                if hasattr(last_message, "content"):
                    print(f"  Response: {last_message.content[:100]}...")


if __name__ == "__main__":
    asyncio.run(test_agent())
