"""
ABS Data Explorer — landing page content.
"""
import streamlit as st

st.title("Welcome to the ABS Data Explorer")
st.markdown("""
Use the pages in the sidebar to:

- **Explore** — Search for any ABS dataset using plain English and view interactive charts
- **Compare** — Overlay two datasets to explore correlations
- **Forecast** — Project future trends using linear regression or LSTM models
- **Data** — View cached datasets, refresh data, and extend date ranges

**Getting started:** Click **Explore** and type a question like _"inflation last 10 years"_ or _"unemployment rate"_.
""")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.info("**Explore**\nSearch + visualise any ABS dataset")
with col2:
    st.info("**Compare**\nCorrelation between two datasets")
with col3:
    st.info("**Forecast**\nTrend projection with ML")
with col4:
    st.info("**Data**\nManage cached datasets and date ranges")
