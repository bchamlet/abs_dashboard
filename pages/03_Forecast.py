"""
Forecast page — project future trends with linear regression or LSTM.
"""
import streamlit as st

from modules.search import find_matching_datasets
from modules.metadata import get_dataflow_version
from modules.abs_client import get_structure, get_observations, filter_dataframe
from modules.analytics import forecast
from modules.charts import forecast_chart, to_csv_bytes, to_png_bytes

st.title("Forecast")
st.caption("Project future trends from any ABS time series using linear regression or an LSTM neural network.")

# ---------------------------------------------------------------------------
# Dataset search
# ---------------------------------------------------------------------------
query = st.text_input(
    "What data do you want to forecast?",
    placeholder="e.g. consumer price index, unemployment",
)
if not query:
    st.stop()

with st.spinner("Searching ABS catalogue..."):
    matches = find_matching_datasets(query, top_n=5)

if not matches:
    st.warning("No matching datasets found.")
    st.stop()

opts = {f"{m['name']} ({m['id']})": m for m in matches}
chosen_label = st.radio(
    "Select a dataset:",
    list(opts.keys()),
    captions=[m.get("reason", "") for m in matches],
)
chosen = opts[chosen_label]
dataflow_id = chosen["id"]
version = get_dataflow_version(dataflow_id)

# ---------------------------------------------------------------------------
# Dimension selectors
# ---------------------------------------------------------------------------
with st.spinner("Loading dimensions..."):
    try:
        structure = get_structure(dataflow_id, version)
    except Exception as e:
        st.error(f"Could not load structure: {e}")
        st.stop()

dimensions = structure.get("dimensions", [])
dim_selections: dict[str, str] = {}
if dimensions:
    cols = st.columns(min(len(dimensions), 3))
    for i, dim in enumerate(dimensions):
        codes = dim.get("codes", [])
        if not codes:
            continue
        options_map = {c["name"]: c["id"] for c in codes}
        chosen_code = cols[i % len(cols)].selectbox(
            dim["name"], list(options_map.keys()), key=f"dim_{dim['id']}"
        )
        dim_selections[dim["id"]] = options_map[chosen_code]

# ---------------------------------------------------------------------------
# Fetch full dataset (cached) then filter locally
# ---------------------------------------------------------------------------
with st.spinner("Fetching historical data..."):
    try:
        df_all = get_observations(dataflow_id, version, dataflow_name=chosen["name"])
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        st.stop()

if df_all.empty:
    st.warning("No data returned. Try a different dataset.")
    st.stop()

df_filtered = filter_dataframe(df_all, structure, dim_selections)

if df_filtered.empty:
    st.warning("No data matched the selected dimension combination. Try adjusting the filters.")
    st.stop()

# ---------------------------------------------------------------------------
# Forecast settings
# ---------------------------------------------------------------------------
st.subheader("Forecast settings")
cached_min = int(df_all["time_period"].dt.year.min())
cached_max = int(df_all["time_period"].dt.year.max())

col_s, col_e, col_p, col_m = st.columns(4)
start = int(col_s.number_input("Historical start year", cached_min, cached_max, cached_min, step=1))
end = int(col_e.number_input("Historical end year", cached_min, cached_max, cached_max, step=1))
periods = col_p.number_input("Periods to forecast", min_value=1, max_value=40, value=8)
method_label = col_m.selectbox("Method", ["Linear regression", "LSTM (neural network)"])
method = "linear" if "Linear" in method_label else "lstm"

st.caption(
    f"Cached data covers {cached_min}–{cached_max}. "
    "To extend the date range, visit the **Data** tab."
)

df = df_filtered[df_filtered["time_period"].dt.year.between(start, end)].copy()

if df.empty:
    st.warning("No data in the selected date range.")
    st.stop()

if method == "lstm" and len(df.dropna()) < 50:
    st.info(
        f"LSTM requires at least 50 observations; this series has {len(df.dropna())}. "
        "Switching to linear regression."
    )
    method = "linear"

# ---------------------------------------------------------------------------
# Run forecast
# ---------------------------------------------------------------------------
with st.spinner(f"Running {method_label} forecast..."):
    try:
        forecast_df = forecast(df, periods=int(periods), method=method)
    except Exception as e:
        st.error(f"Forecast error: {e}")
        st.stop()

if forecast_df.empty:
    st.warning("Could not generate forecast. Not enough data.")
    st.stop()

title = f"{chosen['name']} — {method_label} forecast (+{periods} periods)"
fig = forecast_chart(df, forecast_df, title=title)
st.plotly_chart(fig, width='stretch')

# Summary stats
last_val = df["value"].dropna().iloc[-1]
next_val = forecast_df["value"].iloc[0]
delta = next_val - last_val
c1, c2, c3 = st.columns(3)
c1.metric("Last observed value", f"{last_val:.2f}")
c2.metric("Next period forecast", f"{next_val:.2f}", delta=f"{delta:+.2f}")
c3.metric("Forecast horizon", f"{periods} periods")

# Export
st.subheader("Export")
combined = df[["time_period", "value"]].copy()
combined["type"] = "historical"
fc_export = forecast_df[["time_period", "value", "lower", "upper"]].copy()
fc_export["type"] = "forecast"

col_csv, col_png = st.columns(2)
with col_csv:
    import pandas as pd
    export_df = pd.concat([
        combined.rename(columns={"value": "value"}),
        fc_export,
    ], ignore_index=True)
    st.download_button(
        "Download forecast data (CSV)",
        data=to_csv_bytes(export_df),
        file_name=f"{dataflow_id}_forecast.csv",
        mime="text/csv",
    )
with col_png:
    try:
        st.download_button(
            "Download chart (PNG)",
            data=to_png_bytes(fig),
            file_name=f"{dataflow_id}_forecast.png",
            mime="image/png",
        )
    except Exception:
        st.caption("PNG export requires `kaleido`")
