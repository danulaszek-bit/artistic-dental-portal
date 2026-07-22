"""
auth_gate.py
=============
Simple shared-password gate for the manager dashboards (Fixed / Removable and
their settings pages). Not real authentication — a lightweight barrier so pay
data isn't one bookmark away for anyone on the network.

Passwords live in .streamlit/secrets.toml (gitignored — never committed):

    [dashboard_passwords]
    fixed     = "..."
    removable = "..."

Behavior:
  - Correct password → unlocked for the browser session (st.session_state).
  - No secrets entry configured → gate DENIES with a setup hint. Deny-by-
    default matters because the cloud copy of this app is public-URL; a
    missing cloud secret must never silently open the door.
"""
from __future__ import annotations

import streamlit as st


def require_password(dashboard_key: str, label: str) -> None:
    """Call at the top of a gated page. Stops rendering until unlocked."""
    state_key = f"auth_ok_{dashboard_key}"
    if st.session_state.get(state_key):
        return

    try:
        expected = st.secrets["dashboard_passwords"][dashboard_key]
    except (KeyError, FileNotFoundError):
        expected = None

    st.markdown(f"## 🔒 {label}")
    if expected is None:
        st.error(
            "No password configured for this dashboard. Add one to "
            "`.streamlit/secrets.toml` under `[dashboard_passwords]` "
            f"(key: `{dashboard_key}`) and reload."
        )
        st.stop()

    with st.form(f"pw_form_{dashboard_key}"):
        pw = st.text_input("Password", type="password")
        if st.form_submit_button("Unlock"):
            if pw == expected:
                st.session_state[state_key] = True
                st.rerun()
            else:
                st.error("Incorrect password.")
    st.stop()
