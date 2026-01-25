import asyncio

import streamlit as st

from ui.service.ops_service import OpsService

st.set_page_config(page_title="System Operations", page_icon="⚙️", layout="wide")

st.title("⚙️ System Operations")
st.write("Perform maintenance tasks and update system state.")

tab1, tab2, tab3, tab4 = st.tabs(
    ["🧩 NLP Patterns", "🗄️ Schema", "🧠 Sematic Cache", "📊 Observability"]
)


async def run_operation(name: str, coro_gen):
    """Run async generator operation and stream logs."""
    status_container = st.status(f"Running {name}...", expanded=True)
    try:
        async for log in coro_gen:
            status_container.write(log)
        status_container.update(label=f"{name} Complete", state="complete")
    except Exception as e:
        status_container.write(f"Error: {e}")
        status_container.update(label=f"{name} Failed", state="error")


# --- Tab 1: NLP Patterns ---
with tab1:
    st.header("NLP Patterns")
    st.info("Manage query understanding patterns.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Generate Patterns")
        st.caption("Generate new entity patterns from DB values + LLM synonyms.")
        if st.button("Run Generation"):
            asyncio.run(run_operation("Pattern Generation", OpsService.run_pattern_generation()))

    with col2:
        st.subheader("Reload Patterns")
        st.caption("Reload NLP patterns from database without restart.")
        if st.button("Reload"):
            with st.spinner("Reloading patterns..."):
                result = asyncio.run(OpsService.reload_patterns())
                if result.get("success"):
                    st.success(f"✅ {result.get('message')}")
                    st.metric("Patterns Loaded", result.get("pattern_count"))
                    st.caption(
                        f"Duration: {result.get('duration_ms')}ms "
                        f"(ID: {result.get('reload_id')})"
                    )
                else:
                    st.error(f"❌ {result.get('message')}")
                    if result.get("reload_id"):
                        st.caption(f"Error ID: {result.get('reload_id')}")

# --- Tab 2: Schema ---
with tab2:
    st.header("Schema Hydration")
    st.info("Sync Postgres schema to Memgraph.")

    if st.button("Hydrate Schema", disabled=True, help="Coming soon"):
        asyncio.run(run_operation("Schema Hydration", OpsService.run_schema_hydration()))

# --- Tab 3: Semantic Cache ---
with tab3:
    st.header("Semantic Cache")
    st.info("Re-index embeddings for cache/retrieval.")

    if st.button("Re-index Cache", disabled=True, help="Coming soon"):
        asyncio.run(run_operation("Cache Re-indexing", OpsService.run_cache_reindexing()))

# --- Tab 4: Observability ---
with tab4:
    st.header("Observability")
    st.write("Monitor system performance and investigate traces.")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Dashboards")
        st.info("View aggregated metrics (latency, errors) in Grafana.")
        st.link_button(
            "Open Grafana",
            "http://localhost:3001/d/text2sql-traces/text2sql-trace-metrics",
            type="primary",
        )

    with col2:
        st.subheader("Trace Lookup")
        st.info("Direct access to raw trace data via API.")
        trace_id = st.text_input("Trace ID", placeholder="Enter 32-char Trace ID")
        if trace_id:
            st.link_button(
                f"View Trace {trace_id[:8]}...",
                f"http://localhost:4320/api/v1/traces/{trace_id}",
            )
