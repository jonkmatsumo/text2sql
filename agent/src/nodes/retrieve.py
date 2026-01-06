"""Context retrieval node for RAG-based schema lookup."""

from src.retriever import get_vector_store
from src.state import AgentState


def retrieve_context_node(state: AgentState) -> dict:
    """
    Node 2: RetrieveContext.

    Queries vector store for relevant tables based on user question.
    Uses the schema_embeddings table from Phase 2.

    Args:
        state: Current agent state containing conversation messages

    Returns:
        dict: Updated state with schema_context populated
    """
    # Extract the last user message
    last_message = state["messages"][-1]
    user_query = last_message.content

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

    return {"schema_context": context_str}
