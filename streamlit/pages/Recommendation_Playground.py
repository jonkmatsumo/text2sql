import streamlit as st

st.set_page_config(page_title="Recommendation Playground", page_icon="🧪")

st.markdown("# 🧪 Recommendation Playground")
st.markdown(
    """
    Test the recommendation engine deterministically.
    This tool uses the exact same `recommend_examples` tool as the agent.
    """
)

# --- Inputs ---
st.subheader("Configuration")
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    query = st.text_area(
        "Natural Language Query",
        height=100,
        placeholder="e.g. show me top sales by region",
        help="The query to run recommendations against.",
    )

with col2:
    tenant_id = st.number_input("Tenant ID", min_value=1, value=1, step=1)
    limit = st.number_input("Limit", min_value=1, max_value=20, value=3)

with col3:
    st.markdown("### Flags")
    enable_fallback = st.checkbox("Enable Fallback", value=True)

# --- Action ---
if st.button("Run Recommendations", type="primary"):
    if not query.strip():
        st.warning("Please enter a query.")
    else:
        # Wiring Phase 2: Call backend
        try:
            import asyncio

            from streamlit.service.admin import AdminService

            with st.spinner("Running recommendations..."):
                result = asyncio.run(
                    AdminService.get_recommendations(
                        query=query,
                        tenant_id=tenant_id,
                        limit=limit,
                        enable_fallback=enable_fallback,
                    )
                )

            # Defensive parsing
            if isinstance(result, dict) and "error" in result:
                st.error(f"Service Error: {result['error']}")
            elif isinstance(result, dict):
                # Ensure structure
                st.session_state.reco_result = {
                    "examples": result.get("examples", []),
                    "metadata": result.get("metadata", {}),
                    "fallback_used": result.get("fallback_used", False),
                }
                st.success(f"Found {len(st.session_state.reco_result['examples'])} examples.")
            else:
                st.error(f"Unexpected response format: {type(result)}")
                st.json(result)

        except Exception as e:
            st.error(f"Playground Error: {e}")

# --- Result Rendering (Placeholder for Phase 3) ---
if "reco_result" in st.session_state:
    st.divider()
    res = st.session_state.reco_result
    st.json(res)  # Temporary raw dump
