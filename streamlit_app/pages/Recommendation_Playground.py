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

            from streamlit_app.service.admin import AdminService

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

# --- Result Rendering ---
if "reco_result" in st.session_state:
    st.divider()
    res = st.session_state.reco_result
    meta = res.get("metadata", {})
    examples = res.get("examples", [])

    # 1. Selection Summary
    st.markdown("### Selection Summary")

    # Metrics
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Selected", meta.get("count_total", 0))
    m2.metric("Verified", meta.get("count_approved", 0))
    m3.metric("Seeded", meta.get("count_seeded", 0))
    m4.metric("Fallback", meta.get("count_fallback", 0))
    m5.metric("Pinned", meta.get("pins_selected_count", 0))

    # Matched Rules
    matched_rules = meta.get("pins_matched_rules", [])
    if matched_rules:
        st.info(f"📌 Matched Pin Rules: {', '.join(matched_rules)}")

    # Flags
    flags = []
    if res.get("fallback_used"):
        flags.append("⚠️ Fallback Pool Used")
    if meta.get("truncated"):
        flags.append("✂️ Truncated by Limit")

    if flags:
        st.warning(" | ".join(flags))
    else:
        st.success("✅ Standard Selection Plan")

    st.divider()

    # 2. Example Cards
    st.markdown(f"### Selected Examples ({len(examples)})")

    if not examples:
        st.info("No examples returned.")
    else:
        # Enforce UI-side bounding (Safety #119 prep)
        safe_limit = 20
        rendered_count = 0

        for i, ex in enumerate(examples):
            if rendered_count >= safe_limit:
                st.caption(f"... {len(examples) - rendered_count} more hidden for safety ...")
                break

            rendered_count += 1
            ex_meta = ex.get("metadata", {})

            # Safe Preview (Bound & Sanitize)
            raw_q = ex.get("question", "") or ""
            safe_q = raw_q.replace("\n", " ").strip()
            # Remove control chars
            safe_q = "".join(ch for ch in safe_q if ch.isprintable())
            if len(safe_q) > 120:
                safe_q = safe_q[:117] + "..."

            cols = st.columns([1, 4])
            with cols[0]:
                st.caption(f"Rank {i+1}")
                st.code(ex_meta.get("fingerprint", "N/A")[:8], language="text")

            with cols[1]:
                # Metadata Badge Line
                source = ex.get("source", "unknown").upper()
                status = ex_meta.get("status", "unknown").upper()

                badges = f"**{source}** • *{status}*"

                if ex_meta.get("pinned"):
                    badges = f"📌 **PINNED** • {badges}"

                st.markdown(badges)
                st.text(safe_q)

            st.divider()
