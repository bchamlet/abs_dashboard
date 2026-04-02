"""
ABS Interactive Dashboard — entry point.
Runs on every page navigation, rendering the shared sidebar before delegating to pg.run().
"""
import streamlit as st

st.set_page_config(
    page_title="ABS Data Explorer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Cache version bump: increment when cached data structure changes
_CACHE_VERSION = "v2"
if st.session_state.get("cache_version") != _CACHE_VERSION:
    from modules.cache import cache
    cache.clear_all()
    st.session_state["cache_version"] = _CACHE_VERSION
    st.session_state.pop("cache_warmed", None)

if "cache_warmed" not in st.session_state:
    with st.spinner("Loading ABS dataset catalogue..."):
        from modules.cache import cache
        from modules.metadata import warm_cache
        cache.clear_expired()
        warm_cache()
    st.session_state["cache_warmed"] = True

# Persistent sidebar — always rendered because Home.py runs on every page load
with st.sidebar:
    st.title("ABS Data Explorer")
    st.caption("Powered by the Australian Bureau of Statistics API")
    st.divider()

    if st.button("Refresh data cache", width='stretch'):
        from modules.cache import cache
        cache.clear_all()
        st.session_state.pop("cache_warmed", None)
        st.rerun()

from modules.sidebar import render_common_sidebar

pg = st.navigation([
    st.Page("pages/home_content.py", title="Home",    icon=":material/home:"),
    st.Page("pages/01_Explore.py",   title="Explore", icon=":material/search:"),
    st.Page("pages/02_Compare.py",   title="Compare", icon=":material/compare_arrows:"),
    st.Page("pages/03_Forecast.py",  title="Forecast", icon=":material/trending_up:"),
])
render_common_sidebar()
pg.run()
