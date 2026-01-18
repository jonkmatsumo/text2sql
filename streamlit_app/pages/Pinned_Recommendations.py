import asyncio

import streamlit as st

from streamlit_app.service.admin import AdminService

st.set_page_config(page_title="Pinned Recommendations", layout="wide")

st.title("📌 Pinned Recommendations")
st.markdown(
    """
    Define rules to forcefully include specific examples for certain queries.
    Pins are tenant-scoped and applied **before** standard ranking.
    """
)

# --- Sidebar ---
with st.sidebar:
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
                sigs = [s.strip() for s in sigs_input.replace(",", "\n").splitlines() if s.strip()]

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
