"""Context retrieval node for RAG-based schema lookup with MLflow tracing."""

import mlflow
from agent_core.retriever import get_vector_store
from agent_core.state import AgentState


def retrieve_context_node(state: AgentState) -> dict:
    """
    Node 1: RetrieveContext.

    Queries vector store for relevant tables based on user question.
    Uses the schema_embeddings table from Phase 2.

    Args:
        state: Current agent state containing conversation messages

    Returns:
        dict: Updated state with schema_context populated
    """
    with mlflow.start_span(
        name="retrieve_context",
        span_type="RETRIEVER",
    ) as span:
        # Extract the last user message
        messages = state["messages"]
        user_query = messages[-1].content if messages else ""

        span.set_inputs({"user_query": user_query})

        # Get vector store connection
        vector_store = get_vector_store()

        # Retrieve top 5 most relevant table definitions
        # Uses cosine similarity search on schema_embeddings
        docs = vector_store.similarity_search(user_query, k=5)

        # Format into a context string for the LLM
        # Each doc contains schema_text from schema_embeddings table
        context_parts = []
        for doc in docs:
            context_parts.append(doc.page_content)

        context_str = "\n\n".join(context_parts)

        span.set_outputs(
            {
                "context_length": len(context_str),
                "tables_retrieved": len(context_parts),
            }
        )

        return {"schema_context": context_str}
