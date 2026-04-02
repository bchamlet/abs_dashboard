"""
Shared sidebar elements rendered on every page.
Call render_common_sidebar() inside a `with st.sidebar:` block (or at module level —
Streamlit will route it to the sidebar automatically if called outside a column/container).
"""
import streamlit as st


def render_common_sidebar() -> None:
    """Render the debug toggle (and any future shared controls) in the sidebar."""
    st.session_state.setdefault("debug_mode", False)
    with st.sidebar:
        st.divider()
        st.toggle("Debug mode", key="debug_mode")
