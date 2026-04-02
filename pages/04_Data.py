"""
Data catalog — view cached datasets, refresh, and extend date ranges.
"""
from datetime import datetime

import streamlit as st

from config import V1_DATASETS, FETCH_YEARS_DEFAULT
from modules.cache import cache
from modules.abs_client import get_observations
from modules.metadata import get_dataflow_version, get_all_dataflow_summaries

st.title("Data")
st.caption("Manage the datasets cached locally. Refresh to pull the latest data from ABS, or extend the date range to access historical records.")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%-d %b %Y %H:%M")


def _catalog_by_dataflow(entries: list[dict]) -> dict[str, dict]:
    """Index catalog entries by dataflow_id (latest entry wins)."""
    result = {}
    for e in entries:
        result[e["dataflow_id"]] = e
    return result


def _render_dataset_row(entry: dict | None, dataflow_id: str, dataflow_name: str, is_warm: bool) -> None:
    """Render a single dataset row with metadata and action buttons."""
    with st.container(border=True):
        col_info, col_actions = st.columns([3, 1])

        with col_info:
            st.markdown(f"**{dataflow_name}**  `{dataflow_id}`")
            if entry:
                st.caption(
                    f"Date range: {entry['start_period']} – {entry['end_period']}  |  "
                    f"Rows: {entry['row_count']:,}  |  "
                    f"Last fetched: {_format_ts(entry['fetched_at'])}"
                )
            else:
                st.caption("Not yet fetched")

        with col_actions:
            version = get_dataflow_version(dataflow_id)

            if entry:
                if st.button("Refresh", key=f"refresh_{dataflow_id}", use_container_width=True):
                    with st.spinner(f"Re-fetching {dataflow_name}..."):
                        try:
                            get_observations(
                                dataflow_id, version,
                                start_period=entry["start_period"],
                                end_period=entry["end_period"],
                                dataflow_name=dataflow_name,
                                force_refresh=True,
                                is_warm_cache=is_warm,
                            )
                            st.success("Refreshed.")
                        except Exception as e:
                            st.error(f"Refresh failed: {e}")
                    st.rerun()
            else:
                if st.button("Fetch", key=f"fetch_{dataflow_id}", use_container_width=True):
                    current_year = datetime.now().year
                    with st.spinner(f"Fetching {dataflow_name}..."):
                        try:
                            get_observations(
                                dataflow_id, version,
                                dataflow_name=dataflow_name,
                                is_warm_cache=is_warm,
                            )
                            st.success("Fetched.")
                        except Exception as e:
                            st.error(f"Fetch failed: {e}")
                    st.rerun()

        # Extend date range (only if already fetched)
        if entry:
            with st.expander("Extend date range"):
                current_year = datetime.now().year
                col_ns, col_ne, col_nb = st.columns([2, 2, 1])
                new_start = int(col_ns.number_input(
                    "New start year", min_value=1950, max_value=current_year,
                    value=int(entry["start_period"]),
                    key=f"ext_start_{dataflow_id}",
                ))
                new_end = int(col_ne.number_input(
                    "New end year", min_value=1950, max_value=current_year,
                    value=current_year,
                    key=f"ext_end_{dataflow_id}",
                ))
                if col_nb.button("Fetch", key=f"extbtn_{dataflow_id}", use_container_width=True):
                    with st.spinner(f"Fetching extended range ({new_start}–{new_end})..."):
                        try:
                            get_observations(
                                dataflow_id, version,
                                start_period=str(new_start),
                                end_period=str(new_end),
                                dataflow_name=dataflow_name,
                                force_refresh=True,
                                is_warm_cache=is_warm,
                            )
                            st.success(f"Extended to {new_start}–{new_end}.")
                        except Exception as e:
                            st.error(f"Fetch failed: {e}")
                    st.rerun()


# ---------------------------------------------------------------------------
# Load catalog + dataflow names
# ---------------------------------------------------------------------------
catalog_entries = cache.catalog_list()
catalog_by_id = _catalog_by_dataflow(catalog_entries)

# Build a name lookup from the ABS dataflow list (already cached)
try:
    all_flows = get_all_dataflow_summaries()
    name_lookup = {f["id"]: f["name"] for f in all_flows}
except Exception:
    name_lookup = {}

# ---------------------------------------------------------------------------
# Section 1: Warm cache datasets (V1_DATASETS)
# ---------------------------------------------------------------------------
st.subheader("Warm cache datasets")
st.caption("Pre-defined datasets automatically available on startup.")

for ds_id in V1_DATASETS:
    ds_name = name_lookup.get(ds_id, ds_id)
    entry = catalog_by_id.get(ds_id)
    _render_dataset_row(entry, ds_id, ds_name, is_warm=True)

# ---------------------------------------------------------------------------
# Section 2: Session datasets
# ---------------------------------------------------------------------------
st.subheader("Session datasets")
st.caption("Additional datasets loaded during this session via Explore, Compare, or Forecast.")

session_entries = [e for e in catalog_entries if e["dataflow_id"] not in V1_DATASETS]

if not session_entries:
    st.info("No additional datasets loaded yet. Search for one on the Explore tab.")
else:
    for entry in session_entries:
        ds_id = entry["dataflow_id"]
        ds_name = entry["dataflow_name"] or name_lookup.get(ds_id, ds_id)
        _render_dataset_row(entry, ds_id, ds_name, is_warm=False)
