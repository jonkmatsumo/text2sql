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
        st.info("Wiring to backend not yet implemented.")
        # Placeholder for Phase 2
        st.code(
            f"""
            # Emulating call to recommend_examples:
            query='{query}'
            tenant_id={tenant_id}
            limit={limit}
            enable_fallback={enable_fallback}
            """,
            language="python",
        )
