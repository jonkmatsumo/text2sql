import asyncio

import streamlit as st

from streamlit_app.service.admin import AdminService

st.set_page_config(page_title="Recommendations", page_icon="✨", layout="wide")

st.title("✨ Recommendations")

tab1, tab2 = st.tabs(["📌 Pinned Rules", "🧪 Playground"])

# --- Tab 1: Pinned Rules ---
with tab1:
    st.header("Pinned Rules")
    st.markdown(
        """
        Define rules to forcefully include specific examples for certain queries.
        Pins are tenant-scoped and applied **before** standard ranking.
        """
    )

    # --- Sidebar for Context ---
    with st.sidebar:
        st.header("Context")
        tenant_id = st.number_input("Tenant ID", min_value=1, value=1, step=1)
        if st.button("Refresh Rules"):
            st.rerun()

    # --- Load Rules ---
    try:
        rules = asyncio.run(AdminService.list_pin_rules(tenant_id))
    except Exception as e:
        st.error(f"Failed to load rules: {e}")
        rules = []

    # --- Create New Rule ---
    with st.expander("➕ Create New Rule", expanded=False):
        with st.form("create_rule_form"):
            c1, c2 = st.columns(2)
            match_type = c1.selectbox("Match Type", ["exact", "contains"])
            match_value = c2.text_input("Match Value (e.g. 'refund', 'quarterly report')")

            sigs_input = st.text_area("Signature Keys (one per line or comma-separated)")

            c3, c4 = st.columns(2)
            priority = c3.number_input("Priority (Higher Wins)", value=0)
            enabled = c4.checkbox("Enabled", value=True)

            if st.form_submit_button("Create Rule"):
                if not match_value or not sigs_input:
                    st.error("Match Value and Signature Keys are required.")
                else:
                    # Parse signatures
                    sigs = [
                        s.strip() for s in sigs_input.replace(",", "\n").splitlines() if s.strip()
                    ]

                    try:
                        asyncio.run(
                            AdminService.upsert_pin_rule(
                                tenant_id=tenant_id,
                                match_type=match_type,
                                match_value=match_value,
                                registry_example_ids=sigs,
                                priority=priority,
                                enabled=enabled,
                            )
                        )
                        st.success("Rule created!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error creating rule: {e}")

    # --- List Rules ---
    st.divider()
    st.subheader(f"Existing Rules ({len(rules)})")

    if not rules:
        st.info("No pinned rules found for this tenant.")
    else:
        for rule in rules:
            with st.container(border=True):
                c1, c2, c3, c4, c5 = st.columns([1, 2, 2, 1, 1])

                c1.markdown(f"**{rule.match_type.upper()}**")
                c1.caption(f"Pri: {rule.priority}")

                c2.code(rule.match_value)

                c3.caption(f"Pins: {len(rule.registry_example_ids)}")
                with c3.popover("View Pins"):
                    for s in rule.registry_example_ids:
                        st.code(s, language="text")

                status_emoji = "✅" if rule.enabled else "❌"
                c4.write(f"Status: {status_emoji}")

                # Actions
                if c5.button("Delete", key=f"del_{rule.id}"):
                    try:
                        asyncio.run(AdminService.delete_pin_rule(str(rule.id), tenant_id))
                        st.success("Deleted")
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))

                # Toggle Enable (Quick Action)
                new_status = not rule.enabled
                btn_label = "Disable" if rule.enabled else "Enable"
                if c5.button(btn_label, key=f"toggle_{rule.id}"):
                    try:
                        asyncio.run(
                            AdminService.upsert_pin_rule(
                                tenant_id=tenant_id, rule_id=str(rule.id), enabled=new_status
                            )
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))


# --- Tab 2: Playground ---
with tab2:
    st.header("Recommendation Playground")
    st.markdown(
        """
        Test the recommendation engine deterministically.
        This tool uses the exact same `recommend_examples` tool as the agent.
        """
    )

    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        query = st.text_area(
            "Natural Language Query",
            height=100,
            placeholder="e.g. show me top sales by region",
            help="The query to run recommendations against.",
        )

    with col2:
        # Use tenant_id from sidebar if available, else default
        # But wait, tab scope sharing... sidebar is global to page.
        # Let's reuse tenant_id from sidebar if logical, or allow override?
        # Sidebar is page-level config.
        pass  # use tenant_id from global sidebar

        limit = st.number_input("Limit", min_value=1, max_value=20, value=3)

    with col3:
        enable_fallback = st.checkbox("Enable Fallback", value=True)

    # --- Action ---
    if st.button("Run Recommendations", type="primary"):
        if not query.strip():
            st.warning("Please enter a query.")
        else:
            try:
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
