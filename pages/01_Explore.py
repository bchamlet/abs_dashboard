"""
Explore page — search ABS metadata, select dimensions, view chart.
"""
import streamlit as st
import pandas as pd

from modules.search import find_matching_datasets
from modules.metadata import get_dataflow_version
from modules.abs_client import get_structure, get_observations
from modules.analytics import detect_anomalies
from modules.charts import line_chart, to_csv_bytes, to_png_bytes

st.title("Explore ABS Data")
st.caption("Search for any dataset using plain English, then drill into dimensions and date ranges.")

# ---------------------------------------------------------------------------
# Step 1: Search
# ---------------------------------------------------------------------------
query = st.text_input(
    "What data are you looking for?",
    placeholder="e.g. inflation last 10 years, unemployment rate Sydney, housing costs",
)

if not query:
    st.stop()

with st.spinner("Searching ABS catalogue..."):
    matches = find_matching_datasets(query, top_n=5)

if not matches:
    st.warning("No matching datasets found. Try different keywords.")
    st.stop()

# ---------------------------------------------------------------------------
# Step 2: Dataset selection
# ---------------------------------------------------------------------------
st.subheader("Matching datasets")
options = {f"{m['name']} ({m['id']})": m for m in matches}
selected_label = st.radio(
    "Select a dataset:",
    list(options.keys()),
    captions=[m.get("reason", "") for m in matches],
)
selected = options[selected_label]
dataflow_id = selected["id"]
version = get_dataflow_version(dataflow_id)

# ---------------------------------------------------------------------------
# Step 3: Dimension selectors
# ---------------------------------------------------------------------------
with st.spinner("Loading dataset dimensions..."):
    try:
        structure = get_structure(dataflow_id, version)
    except Exception as e:
        st.error(f"Could not load dataset structure: {e}")
        st.stop()

dimensions = structure.get("dimensions", [])

st.subheader("Filter dimensions")
if not dimensions:
    st.info("No dimension filters available — fetching all data.")

dim_selections: dict[str, str] = {}
cols = st.columns(min(len(dimensions), 3)) if dimensions else []
for i, dim in enumerate(dimensions):
    codes = dim.get("codes", [])
    if not codes:
        continue
    col = cols[i % len(cols)] if cols else st.container()
    options_map = {c["name"]: c["id"] for c in codes}
    # Default to first option
    chosen_name = col.selectbox(dim["name"], list(options_map.keys()), key=f"dim_{dim['id']}")
    dim_selections[dim["id"]] = options_map[chosen_name]

# Build data key from selections (ordered by dimension position)
if dim_selections:
    data_key = ".".join(dim_selections[d["id"]] for d in dimensions if d["id"] in dim_selections)
else:
    data_key = "all"

# ---------------------------------------------------------------------------
# Step 4: Date range
# ---------------------------------------------------------------------------
st.subheader("Date range")
col_s, col_e = st.columns(2)
start_year = col_s.number_input("Start year", min_value=1950, max_value=2025, value=2010, step=1)
end_year = col_e.number_input("End year", min_value=1950, max_value=2025, value=2025, step=1)
start_period = str(int(start_year))
end_period = str(int(end_year))

# ---------------------------------------------------------------------------
# Step 5: Fetch & display
# ---------------------------------------------------------------------------
from config import ABS_BASE_URL

_obs_url = f"{ABS_BASE_URL}/rest/data/ABS,{dataflow_id},{version}/{data_key}"
_obs_params = {
    "detail": "full",
    "dimensionAtObservation": "TIME_PERIOD",
    "startPeriod": start_period,
    "endPeriod": end_period,
}

if st.session_state.get("debug_mode"):
    st.info(
        f"**Endpoint:** `{_obs_url}`\n\n"
        f"**Params:** `{_obs_params}`\n\n"
        f"**Full URL:** `{_obs_url}?{'&'.join(f'{k}={v}' for k, v in _obs_params.items())}`"
    )

with st.spinner("Fetching data from ABS..."):
    try:
        df = get_observations(dataflow_id, version, data_key, start_period, end_period)
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        st.stop()

if df.empty:
    st.warning("No data returned for this selection. Try adjusting the filters or date range.")
    st.stop()

# Sidebar options
with st.sidebar:
    st.subheader("Chart options")
    show_anomalies = st.toggle("Highlight anomalies", value=False)
    show_table = st.toggle("Show data table", value=False)

anomalies_df = detect_anomalies(df) if show_anomalies else None

chart_title = f"{selected['name']} ({start_period}–{end_period})"
fig = line_chart(df, title=chart_title, anomalies=anomalies_df)
st.plotly_chart(fig, width='stretch')

if show_anomalies and anomalies_df is not None:
    n_anomalies = int(anomalies_df["is_anomaly"].sum())
    if n_anomalies:
        st.caption(f"Detected {n_anomalies} anomalous observation(s) (Z-score > 2.5)")

if show_table:
    st.dataframe(df, width='stretch')

# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------
st.subheader("Export")
col_csv, col_png = st.columns(2)
with col_csv:
    st.download_button(
        "Download CSV",
        data=to_csv_bytes(df),
        file_name=f"{dataflow_id}_{start_period}_{end_period}.csv",
        mime="text/csv",
    )
with col_png:
    try:
        png = to_png_bytes(fig)
        st.download_button(
            "Download PNG",
            data=png,
            file_name=f"{dataflow_id}_{start_period}_{end_period}.png",
            mime="image/png",
        )
    except Exception:
        st.caption("PNG export requires `kaleido` — run `pip install kaleido`")
