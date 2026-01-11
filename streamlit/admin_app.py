import asyncio
import json
import sys
from pathlib import Path

import nest_asyncio
import pandas as pd

import streamlit as st

# Add agent src to path for tools
sys.path.insert(0, str(Path(__file__).parent.parent / "agent" / "src"))

from agent_core.tools import get_mcp_tools  # noqa: E402

# Allow nested asyncio loops for MCP
nest_asyncio.apply()

st.set_page_config(page_title="Text2SQL Admin Review", layout="wide")


async def call_admin_tool(tool_name: str, args: dict):
    """Invoke an MCP admin tool and handle exceptions."""
    try:
        tools = await get_mcp_tools()
        tool = next((t for t in tools if t.name == tool_name), None)
        if not tool:
            return {"error": f"Tool {tool_name} not found"}
        return await tool.ainvoke(args)
    except Exception as e:
        return {"error": str(e)}


def main():
    """Run the Admin Review Streamlit app."""
    st.title("🛡️ Text2SQL Admin Review")
    st.sidebar.header("Navigation")

    view = st.sidebar.radio(
        "View", ["Recent Interactions", "Pending Publication", "Approved Examples"]
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
            interactions = asyncio.run(call_admin_tool("list_interactions_tool", {"limit": 50}))

        if isinstance(interactions, list):
            df = pd.DataFrame(interactions)
            if not df.empty:
                # Format for display
                df["created_at"] = pd.to_datetime(df["created_at"])
                df = df.sort_values("created_at", ascending=False)

                # Apply Filters
                if thumb_filter == "UP":
                    df = df[df["thumb"] == "UP"]
                elif thumb_filter == "DOWN":
                    df = df[df["thumb"] == "DOWN"]
                elif thumb_filter == "None":
                    df = df[df["thumb"].isna() | (df["thumb"] == "-") | (df["thumb"] == "")]

                if status_filter != "All":
                    df = df[df["execution_status"] == status_filter]

                # Show list
                cols = st.columns([2, 1, 1, 1, 1])
                cols[0].write("**NLQ Text**")
                cols[1].write("**Status**")
                cols[2].write("**Thumb**")
                cols[3].write("**Created At**")
                cols[4].write("**Action**")

                for _, row in df.iterrows():
                    cols = st.columns([2, 1, 1, 1, 1])
                    cols[0].write(row["user_nlq_text"])
                    cols[1].write(row["execution_status"])
                    thumb = row["thumb"] if row["thumb"] else "-"
                    cols[2].write(thumb)
                    cols[3].write(row["created_at"].strftime("%Y-%m-%d %H:%M"))

                    if cols[4].button("Review", key=row["id"]):
                        st.session_state.review_id = row["id"]
                        st.rerun()

                # Review Detail
                if "review_id" in st.session_state:
                    rid = st.session_state.review_id
                    st.divider()
                    st.subheader(f"Reviewing Interaction: {rid}")

                    detail = asyncio.run(
                        call_admin_tool("get_interaction_details_tool", {"interaction_id": rid})
                    )

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
                            "Reviewer Notes", placeholder="Reason for correction or approval"
                        )

                        if action_col1.button("Approve", type="primary"):
                            res = asyncio.run(
                                call_admin_tool(
                                    "approve_interaction_tool",
                                    {
                                        "interaction_id": rid,
                                        "corrected_sql": corrected_sql,
                                        "resolution_type": (
                                            "APPROVED_AS_IS"
                                            if corrected_sql == detail["generated_sql"]
                                            else "APPROVED_WITH_SQL_FIX"
                                        ),
                                        "reviewer_notes": notes,
                                    },
                                )
                            )
                            if res == "OK":
                                st.success("Approved!")
                                del st.session_state.review_id
                                st.rerun()

                        if action_col2.button("Reject"):
                            res = asyncio.run(
                                call_admin_tool(
                                    "reject_interaction_tool",
                                    {
                                        "interaction_id": rid,
                                        "reason": "CANNOT_FIX",
                                        "reviewer_notes": notes,
                                    },
                                )
                            )
                            if res == "OK":
                                st.info("Rejected.")
                                del st.session_state.review_id
                                st.rerun()
            else:
                st.info("No interactions found.")
        else:
            st.error(f"Error loading interactions: {interactions}")

    elif view == "Pending Publication":
        st.header("Pending Publication")
        st.write("Approved interactions ready to be synced to the Few-Shot Registry.")

        if st.button("🚀 Sync All Approved to Few-Shot", type="primary"):
            with st.spinner("Syncing..."):
                res = asyncio.run(call_admin_tool("export_approved_to_fewshot_tool", {"limit": 50}))
                if "published" in res:
                    st.success(f"Successfully published {res['published']} examples!")
                    if res["errors"]:
                        st.warning(f"Encountered {len(res['errors'])} errors.")
                else:
                    st.error(f"Sync failed: {res.get('error')}")

    elif view == "Approved Examples":
        st.header("✅ Approved Few-Shot Examples")
        st.write("These examples are currently verified and indexable in the Few-Shot Registry.")

        with st.spinner("Loading examples..."):
            examples = asyncio.run(call_admin_tool("list_approved_examples_tool", {"limit": 100}))

        if isinstance(examples, list) and examples:
            df_ex = pd.DataFrame(examples)
            # Show search/filter
            search = st.text_input("Search examples", placeholder="Filter by question or SQL...")
            if search:
                mask = df_ex["question"].str.contains(search, case=False) | df_ex[
                    "sql_query"
                ].str.contains(search, case=False)
                df_ex = df_ex[mask]

            st.dataframe(
                df_ex[["question", "sql_query", "status", "created_at"]],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No approved examples found in the registry.")


if __name__ == "__main__":
    main()
