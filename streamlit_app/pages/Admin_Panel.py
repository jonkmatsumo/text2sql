import asyncio
import json

import nest_asyncio
import pandas as pd

import streamlit as st
from streamlit_app.service.admin import AdminService  # noqa: E402

# Allow nested asyncio loops for MCP
nest_asyncio.apply()

st.set_page_config(page_title="Text2SQL Admin Review", layout="wide")


def main():
    """Run the Admin Review Streamlit app."""
    st.title("🛡️ Text2SQL Admin Review")
    st.sidebar.header("Navigation")

    view = st.sidebar.radio(
        "View", ["Recent Interactions", "Pending Publication", "Approved Examples", "Operations"]
    )

    # Optional Filters for Recent Interactions
    thumb_filter = "All"
    status_filter = "All"
    if view == "Recent Interactions":
        st.sidebar.divider()
        st.sidebar.subheader("Filters")
        thumb_filter = st.sidebar.selectbox("Feedback", ["All", "UP", "DOWN", "None"])
        status_filter = st.sidebar.selectbox(
            "Review Status", ["All", "PENDING", "APPROVED", "REJECTED"]
        )

    if view == "Recent Interactions":
        st.header("Recent Interactions")

        # Load interactions
        with st.spinner("Loading interactions..."):
            interactions = asyncio.run(
                AdminService.list_interactions(
                    limit=50, thumb_filter=thumb_filter, status_filter=status_filter
                )
            )

        if isinstance(interactions, list):
            # Display logic
            if interactions:
                # We still use DataFrame for display convenience
                df = pd.DataFrame(interactions)

                # Show list
                cols = st.columns([2, 1, 1, 1, 1])
                cols[0].write("**NLQ Text**")
                cols[1].write("**Status**")
                cols[2].write("**Thumb**")
                cols[3].write("**Created At**")
                cols[4].write("**Action**")

                for _, row in df.iterrows():
                    cols = st.columns([2, 1, 1, 1, 1])
                    cols[0].write(row.get("user_nlq_text", "Unknown Query"))
                    cols[1].write(row.get("execution_status", "UNKNOWN"))
                    thumb = row.get("thumb")
                    thumb = thumb if thumb else "-"
                    cols[2].write(thumb)
                    # Handle potential non-datetime timestamp if raw string
                    created_at = row.get("created_at", "")
                    # Simple display, or convert if needed.
                    # The Service does sort by string, which is fine for ISO.
                    cols[3].write(created_at[:16].replace("T", " "))

                    if cols[4].button("Review", key=row["id"]):
                        st.session_state.review_id = row["id"]
                        st.rerun()

                # Review Detail
                if "review_id" in st.session_state:
                    rid = st.session_state.review_id
                    st.divider()
                    st.subheader(f"Reviewing Interaction: {rid}")

                    detail = asyncio.run(AdminService.get_interaction_details(rid))

                    if "error" in detail:
                        st.error(detail["error"])
                    else:
                        col_a, col_b = st.columns(2)
                        with col_a:
                            st.markdown("**User Query:**")
                            st.info(detail["user_nlq_text"])
                            st.markdown("**Generated SQL:**")
                            st.code(detail["generated_sql"], language="sql")

                        with col_b:
                            st.markdown("**Response:**")
                            try:
                                payload = json.loads(detail["response_payload"])
                                st.write(payload.get("text", detail["response_payload"]))
                            except (ValueError, TypeError, json.JSONDecodeError):
                                st.write(detail["response_payload"])

                            st.markdown("**Metadata:**")
                            st.json(
                                {
                                    "model": detail.get("model_version"),
                                    "status": detail.get("execution_status"),
                                    "tables": detail.get("tables_used"),
                                }
                            )

                        # Feedback list
                        if detail.get("feedback"):
                            st.markdown("**User Feedback:**")
                            for f in detail["feedback"]:
                                feedback_text = (
                                    f"{f['thumb']}: "
                                    f"{f['comment'] if f['comment'] else '(No comment)'} "
                                    f"({f['created_at']})"
                                )
                                st.warning(feedback_text)

                        # Action Panel
                        st.subheader("Action")

                        # Guided Correction for DOWN votes
                        is_downvote = any(
                            f.get("thumb") == "DOWN" for f in detail.get("feedback", [])
                        )
                        if is_downvote:
                            st.warning(
                                "⚠️ **Fix Recommended**: User gave this query a thumbs-down. "
                                "Please review and correct the SQL below."
                            )

                        action_col1, action_col2, action_col3 = st.columns(3)

                        corrected_sql = st.text_area(
                            "Corrected SQL", value=detail["generated_sql"], height=200
                        )
                        notes = st.text_input(
                            "Reviewer Notes",
                            placeholder="Reason for correction or approval",
                        )

                        if action_col1.button("Approve", type="primary"):
                            res = asyncio.run(
                                AdminService.approve_interaction(
                                    interaction_id=rid,
                                    corrected_sql=corrected_sql,
                                    original_sql=detail["generated_sql"],
                                    notes=notes,
                                )
                            )
                            if res == "OK":
                                st.success("Approved!")
                                del st.session_state.review_id
                                st.rerun()
                            else:
                                st.error(f"Approval failed: {res}")

                        if action_col2.button("Reject"):
                            res = asyncio.run(
                                AdminService.reject_interaction(
                                    interaction_id=rid,
                                    reason="CANNOT_FIX",
                                    notes=notes,
                                )
                            )
                            if res == "OK":
                                st.info("Rejected.")
                                del st.session_state.review_id
                                st.rerun()
                            else:
                                st.error(f"Rejection failed: {res}")
            else:
                st.info("No interactions found matching filters.")
        else:
            st.error(f"Error loading interactions: {interactions}")

    elif view == "Pending Publication":
        st.header("Pending Publication")
        st.write("Approved interactions ready to be synced to the Few-Shot Registry.")

        if st.button("🚀 Sync All Approved to Few-Shot", type="primary"):
            with st.spinner("Syncing..."):
                res = asyncio.run(AdminService.export_approved_to_fewshot(limit=50))
                if "published" in res:
                    st.success(f"Successfully published {res['published']} examples!")
                    if res["errors"]:
                        st.warning(f"Encountered {len(res['errors'])} errors.")
                else:
                    st.error(f"Sync failed: {res.get('error')}")

    elif view == "Approved Examples":
        st.header("✅ Approved Few-Shot Examples")
        st.write("These examples are currently verified and indexable in the Few-Shot Registry.")

        # Search/Filter UI
        search = st.text_input("Search examples", placeholder="Filter by question or SQL...")

        with st.spinner("Loading examples..."):
            examples = asyncio.run(
                AdminService.list_approved_examples(limit=100, search_query=search)
            )

        if isinstance(examples, list) and examples:
            df_ex = pd.DataFrame(examples)
            st.dataframe(
                df_ex[["question", "sql_query", "status", "created_at"]],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No approved examples found.")

    elif view == "Operations":
        from streamlit_app.service.ops_service import OpsService

        st.header("⚙️ System Operations")
        st.write("Perform maintenance tasks and update system state.")

        col1, col2, col3 = st.columns(3)

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

        with col1:
            st.subheader("NLP Patterns")
            st.info("Generate new entity patterns from DB values + LLM synonyms.")
            if st.button("Generate Patterns"):
                asyncio.run(
                    run_operation("Pattern Generation", OpsService.run_pattern_generation())
                )

            st.divider()
            st.subheader("Reload Patterns")
            st.info("Reload NLP patterns from database without restart.")
            if st.button("Reload NLP Patterns"):
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

        with col2:
            st.subheader("Schema Hydration")
            st.info("Sync Postgres schema to Memgraph.")
            if st.button("Hydrate Schema", disabled=True, help="Coming soon"):
                asyncio.run(run_operation("Schema Hydration", OpsService.run_schema_hydration()))

        with col3:
            st.subheader("Semantic Cache")
            st.info("Re-index embeddings for cache/retrieval.")
            if st.button("Re-index Cache", disabled=True, help="Coming soon"):
                asyncio.run(run_operation("Cache Re-indexing", OpsService.run_cache_reindexing()))


if __name__ == "__main__":
    main()
