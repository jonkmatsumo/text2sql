import asyncio
import json

import nest_asyncio
import pandas as pd

import streamlit as st
from streamlit_app.service.admin import AdminService
from streamlit_app.service.observability_links import grafana_trace_detail_url

# Allow nested asyncio loops for MCP
nest_asyncio.apply()

st.set_page_config(page_title="Review & Curation", page_icon="📝", layout="wide")

st.title("📝 Review & Curation")

tab1, tab2, tab3 = st.tabs(["📥 Inbox", "🚀 Publication", "📚 Registry"])

# --- Tab 1: Inbox (Recent Interactions) ---
with tab1:
    st.header("Inbox")
    st.caption("Review recent interactions and approve/reject them.")

    # Filters
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        thumb_filter = st.selectbox("Feedback", ["All", "UP", "DOWN", "None"])
    with col_f2:
        status_filter = st.selectbox(
            "Status", ["All", "PENDING", "APPROVED", "REJECTED"], index=1
        )  # Default to PENDING for inbox

    # Load interactions
    with st.spinner("Loading inbox..."):
        interactions = asyncio.run(
            AdminService.list_interactions(
                limit=50, thumb_filter=thumb_filter, status_filter=status_filter
            )
        )

    if isinstance(interactions, list) and interactions:
        df = pd.DataFrame(interactions)

        # Show list
        cols_header = st.columns([2, 2, 0.5, 1, 0.5, 1, 1])
        cols_header[0].write("**NLQ Text**")
        cols_header[1].write("**SQL Preview**")
        cols_header[2].write("**Trace**")
        cols_header[3].write("**Status**")
        cols_header[4].write("**Thumb**")
        cols_header[5].write("**Created At**")
        cols_header[6].write("**Action**")

        for _, row in df.iterrows():
            cols = st.columns([2, 2, 0.5, 1, 0.5, 1, 1])
            cols[0].write(row.get("user_nlq_text", "Unknown Query"))

            # SQL Preview
            sql_prev = row.get("generated_sql_preview")
            if sql_prev:
                cols[1].code(sql_prev, language="sql")
            else:
                cols[1].write("-")

            # Trace
            trace_id = row.get("trace_id")
            if trace_id:
                cols[2].link_button(
                    "📊",
                    grafana_trace_detail_url(trace_id),
                    help=f"Trace: {trace_id}",
                )
            else:
                cols[2].write("-")

            cols[3].write(row.get("execution_status", "UNKNOWN"))
            thumb = row.get("thumb")
            cols[4].write(thumb if thumb else "-")

            created_at = row.get("created_at", "")
            cols[5].write(created_at[:16].replace("T", " "))

            if cols[6].button("Review", key=f"review_{row['id']}"):
                st.session_state.review_id = row["id"]
                st.rerun()

        # Review Detail Pane
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

                # Trace ID Obsevability
                trace_id = detail.get("trace_id")
                if trace_id:
                    st.markdown("**Trace Observability:**")
                    st.text(f"Trace ID: {trace_id}")
                    st.link_button("📊 View in Grafana", grafana_trace_detail_url(trace_id))

                # Feedback
                if detail.get("feedback"):
                    st.markdown("**User Feedback:**")
                    for f in detail["feedback"]:
                        st.warning(f"{f['thumb']}: {f['comment'] or '(No comment)'}")

                # Action Panel
                st.subheader("Action")
                action_col1, action_col2 = st.columns(2)

                corrected_sql = st.text_area(
                    "Corrected SQL", value=detail["generated_sql"], height=200
                )
                notes = st.text_input("Reviewer Notes")

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
                        st.error(f"Failed: {res}")

                if action_col2.button("Reject"):
                    res = asyncio.run(
                        AdminService.reject_interaction(
                            interaction_id=rid, reason="CANNOT_FIX", notes=notes
                        )
                    )
                    if res == "OK":
                        st.info("Rejected.")
                        del st.session_state.review_id
                        st.rerun()
                    else:
                        st.error(f"Failed: {res}")

    elif isinstance(interactions, list) and not interactions:
        st.info("No interactions found matching filters.")
    else:
        st.error(f"Error loading interactions: {interactions}")


# --- Tab 2: Publication (Pending Publication) ---
with tab2:
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


# --- Tab 3: Registry (Approved Examples) ---
with tab3:
    st.header("Registry")
    st.write("Verified examples indexable in the Few-Shot Registry.")

    search = st.text_input("Search examples", placeholder="Filter by question or SQL...")

    with st.spinner("Loading examples..."):
        examples = asyncio.run(AdminService.list_approved_examples(limit=100, search_query=search))

    if isinstance(examples, list) and examples:
        df_ex = pd.DataFrame(examples)
        st.dataframe(
            df_ex[["question", "sql_query", "status", "created_at"]],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No approved examples found.")
