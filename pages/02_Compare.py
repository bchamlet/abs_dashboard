"""
Compare page — side-by-side correlation of two ABS datasets.
"""
import streamlit as st

from modules.search import find_matching_datasets
from modules.metadata import get_dataflow_version
from modules.abs_client import get_structure, get_observations, filter_dataframe
from modules.analytics import correlate
from modules.charts import correlation_chart, to_csv_bytes, to_png_bytes

st.title("Compare Datasets")
st.caption("Search for two datasets to compare them on the same chart and calculate their correlation.")


def _dataset_selector(prefix: str, label: str):
    """Reusable widget: search → select → dimensions → return (df, name)."""
    st.subheader(label)
    query = st.text_input(
        "Search for a dataset",
        placeholder="e.g. inflation, unemployment",
        key=f"{prefix}_query",
    )
    if not query:
        return None, None

    with st.spinner("Searching..."):
        matches = find_matching_datasets(query, top_n=5)
    if not matches:
        st.warning("No matches found.")
        return None, None

    opts = {f"{m['name']} ({m['id']})": m for m in matches}
    chosen_label = st.selectbox("Select dataset", list(opts.keys()), key=f"{prefix}_select")
    chosen = opts[chosen_label]
    dataflow_id = chosen["id"]
    version = get_dataflow_version(dataflow_id)

    with st.spinner("Loading dimensions..."):
        try:
            structure = get_structure(dataflow_id, version)
        except Exception as e:
            st.error(f"Could not load structure: {e}")
            return None, None

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
                dim["name"], list(options_map.keys()), key=f"{prefix}_dim_{dim['id']}"
            )
            dim_selections[dim["id"]] = options_map[chosen_code]

    with st.spinner("Fetching data..."):
        try:
            df_all = get_observations(dataflow_id, version, dataflow_name=chosen["name"])
        except Exception as e:
            st.error(f"Error: {e}")
            return None, None

    if df_all.empty:
        st.warning("No data returned.")
        return None, None

    df = filter_dataframe(df_all, structure, dim_selections)

    if df.empty:
        st.warning("No data matched the selected dimension combination. Try adjusting the filters.")
        return None, None

    cached_min = int(df_all["time_period"].dt.year.min())
    cached_max = int(df_all["time_period"].dt.year.max())
    col_s, col_e = st.columns(2)
    start = int(col_s.number_input("Start year", cached_min, cached_max, cached_min, key=f"{prefix}_start"))
    end = int(col_e.number_input("End year", cached_min, cached_max, cached_max, key=f"{prefix}_end"))
    st.caption(f"Cached data covers {cached_min}–{cached_max}. Extend on the **Data** tab.")

    df = df[df["time_period"].dt.year.between(start, end)].copy()

    if df.empty:
        st.warning("No data in the selected date range.")
        return None, None

    return df, chosen["name"]


col_a, col_b = st.columns(2)
with col_a:
    df_a, name_a = _dataset_selector("a", "Dataset A")
with col_b:
    df_b, name_b = _dataset_selector("b", "Dataset B")

if df_a is None or df_b is None:
    st.stop()

# ---------------------------------------------------------------------------
# Correlation
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Correlation analysis")

result = correlate(df_a, df_b)
pearson = result.get("pearson")
spearman = result.get("spearman")
n = result.get("n_observations", 0)

if pearson is None:
    st.warning(f"Not enough overlapping data points to compute correlation (n={n}).")
else:
    c1, c2, c3 = st.columns(3)
    c1.metric("Pearson r", f"{pearson:.4f}")
    c2.metric("Spearman r", f"{spearman:.4f}")
    c3.metric("Observations", n)

    strength = abs(pearson)
    if strength >= 0.7:
        interpretation = "Strong"
    elif strength >= 0.4:
        interpretation = "Moderate"
    else:
        interpretation = "Weak"
    direction = "positive" if pearson >= 0 else "negative"
    st.caption(f"{interpretation} {direction} linear correlation (Pearson)")

fig = correlation_chart(df_a, df_b, labels=(name_a, name_b), title=f"{name_a} vs {name_b}")
st.plotly_chart(fig, width='stretch')

# Export
st.subheader("Export")
col_csv, col_png = st.columns(2)
merged_df = result.get("merged")
if merged_df is not None:
    with col_csv:
        st.download_button(
            "Download aligned data (CSV)",
            data=to_csv_bytes(merged_df),
            file_name="comparison.csv",
            mime="text/csv",
        )
with col_png:
    try:
        st.download_button(
            "Download chart (PNG)",
            data=to_png_bytes(fig),
            file_name="comparison.png",
            mime="image/png",
        )
    except Exception:
        st.caption("PNG export requires `kaleido`")
