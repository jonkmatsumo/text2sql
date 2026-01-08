"""Streamlit UI for Text 2 SQL Agent.

This is a thin UI layer that calls the tested business logic in app_logic.py.
All business logic is tested separately, so this file can be excluded from coverage.
"""

import asyncio
import os
import sys
from pathlib import Path

import nest_asyncio
from dotenv import load_dotenv

import streamlit as st

# Apply nest_asyncio to allow nested event loops (required for MCP SSE client)
nest_asyncio.apply()

# Add app_logic to path
sys.path.insert(0, str(Path(__file__).parent))

from app_logic import format_conversation_entry, run_agent, validate_tenant_id  # noqa: E402

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Text 2 SQL Agent",
    page_icon="🗄️",
    layout="wide",
)

# Initialize session state
if "conversation_history" not in st.session_state:
    st.session_state.conversation_history = []
if "tenant_id" not in st.session_state:
    st.session_state.tenant_id = int(os.getenv("DEFAULT_TENANT_ID", "1"))
if "thread_id" not in st.session_state:
    import uuid

    st.session_state.thread_id = str(uuid.uuid4())


def main():
    """Run the main Streamlit application."""
    st.title("🗄️ Text 2 SQL Agent")
    st.markdown("Ask questions in natural language and get SQL query results")

    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")

        # LLM Provider and Model selection
        st.subheader("LLM Settings")

        # Import get_available_providers and get_available_models
        try:
            from agent_core.llm_client import get_available_models, get_available_providers

            providers = get_available_providers()
        except ImportError:
            providers = ["openai", "anthropic", "google"]
            get_available_models = lambda p: []  # noqa: E731

        # Initialize session state for LLM settings
        if "llm_provider" not in st.session_state:
            st.session_state.llm_provider = os.getenv("LLM_PROVIDER", "openai")
        if "llm_model" not in st.session_state:
            st.session_state.llm_model = os.getenv("LLM_MODEL", "gpt-5.2")

        # Provider dropdown
        provider_index = (
            providers.index(st.session_state.llm_provider)
            if st.session_state.llm_provider in providers
            else 0
        )
        selected_provider = st.selectbox(
            "Provider",
            options=providers,
            index=provider_index,
            help="Select the LLM provider",
        )

        # Update provider in session state
        if selected_provider != st.session_state.llm_provider:
            st.session_state.llm_provider = selected_provider
            # Reset model when provider changes
            models = get_available_models(selected_provider)
            st.session_state.llm_model = models[0] if models else "gpt-5.2"

        # Model dropdown (based on selected provider)
        available_models = get_available_models(selected_provider)
        if not available_models:
            available_models = ["gpt-5.2"]  # Fallback

        model_index = (
            available_models.index(st.session_state.llm_model)
            if st.session_state.llm_model in available_models
            else 0
        )
        selected_model = st.selectbox(
            "Model",
            options=available_models,
            index=model_index,
            help="Select the model to use",
        )
        st.session_state.llm_model = selected_model

        st.markdown("---")

        # Tenant ID configuration
        tenant_id = st.number_input(
            "Tenant ID",
            min_value=1,
            value=st.session_state.tenant_id,
            help="Tenant identifier for multi-tenant scenarios",
        )
        st.session_state.tenant_id = validate_tenant_id(tenant_id)

        st.markdown("---")
        st.markdown("### System Status")
        mcp_url = os.getenv("MCP_SERVER_URL", "http://localhost:8000/sse")
        st.text(f"MCP Server: {mcp_url}")
        st.text(f"LLM: {st.session_state.llm_provider}/{st.session_state.llm_model}")

        if st.button("Clear History"):
            st.session_state.conversation_history = []
            st.rerun()

    # Main content area
    question = st.chat_input("Ask a question about the database...")

    # Display conversation history
    for entry in st.session_state.conversation_history:
        with st.chat_message("user"):
            st.write(entry["question"])

        with st.chat_message("assistant"):
            if entry.get("error"):
                st.error(f"Error: {entry['error']}")
            else:
                if entry.get("sql"):
                    with st.expander("View SQL Query"):
                        st.code(entry["sql"], language="sql")
                    if entry.get("from_cache"):
                        st.info("✓ Used cached SQL")

                if entry.get("result"):
                    st.dataframe(entry["result"], use_container_width=True)

                if entry.get("response"):
                    st.write(entry["response"])

    # Process new question
    if question:
        # Show user question
        with st.chat_message("user"):
            st.write(question)

        # Process with agent
        with st.chat_message("assistant"):
            with st.spinner("Processing your question..."):
                try:
                    # Call tested business logic
                    results = asyncio.run(
                        run_agent(
                            question,
                            st.session_state.tenant_id,
                            st.session_state.thread_id,
                        )
                    )

                    # Format and store entry
                    entry = format_conversation_entry(question, results)
                    st.session_state.conversation_history.append(entry)

                    # Display results
                    if results.get("error"):
                        st.error(f"Error: {results['error']}")
                    else:
                        if results.get("sql"):
                            with st.expander("View SQL Query"):
                                st.code(results["sql"], language="sql")
                            if results.get("from_cache"):
                                st.info("✓ Used cached SQL (similarity >= 0.95)")

                        if results.get("result"):
                            st.dataframe(results["result"], use_container_width=True)

                        if results.get("response"):
                            st.write(results["response"])

                except Exception as e:
                    st.error(f"Failed to process question: {str(e)}")
                    st.session_state.conversation_history.append(
                        {
                            "question": question,
                            "error": str(e),
                        }
                    )

        st.rerun()


if __name__ == "__main__":
    # Check for required environment variables
    if not os.getenv("OPENAI_API_KEY"):
        st.error("⚠️ OPENAI_API_KEY environment variable is required")
        st.stop()

    main()
