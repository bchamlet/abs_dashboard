# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the App

```bash
pip install -r requirements.txt
cp .env.example .env   # then add ANTHROPIC_API_KEY
streamlit run Home.py
```

App runs at `http://localhost:8501`. There are no tests.

## Architecture

### Entry Point & Navigation

`Home.py` is the **single entry point** and runs on every page navigation (Streamlit `st.navigation()` pattern). It handles:
- One-time cache warm-up (guarded by `st.session_state["cache_warmed"]`)
- Persistent sidebar elements (title, refresh button)
- Shared debug toggle via `modules/sidebar.render_common_sidebar()`
- Page routing: `Home → Explore → Compare → Forecast`

The four page files in `pages/` contain only their own content — no `st.set_page_config()` and no `render_common_sidebar()` calls (those belong in `Home.py` only).

### Data Flow

```
User query
  → search.find_matching_datasets()   # Claude Haiku or fuzzy fallback
  → abs_client.get_structure()         # DSD XML + serieskeysonly probe → dimension selectors
  → abs_client.get_observations()      # SDMX-JSON → pd.DataFrame
  → analytics.*                        # forecast / anomalies / correlation
  → charts.*                           # Plotly figures
```

### Caching (`modules/cache.py`)

All API calls go through a SQLite TTL cache (`data/cache.db`). The singleton `cache` instance is imported directly:

```python
from modules.cache import cache
cache.get(key)              # returns None if missing or expired
cache.set(key, value, ttl_hours)
cache.clear_all()           # triggered by sidebar button or _CACHE_VERSION bump
```

TTLs: metadata/structures = 24h, observations = 6h. When the structure of cached data changes (e.g. a parsing fix), bump `_CACHE_VERSION` in `Home.py` — this forces a full cache clear on next load.

### ABS API Client (`modules/abs_client.py`)

Two separate fetch functions because the ABS API serves different formats per endpoint:
- `_get_xml()` — metadata endpoints (`/rest/dataflow/`, `/rest/datastructure/`) return SDMX XML only
- `_get_json()` — data endpoint (`/rest/data/`) accepts `Accept: application/vnd.sdmx.data+json`

**SDMX-JSON version handling:** The ABS returns v2.0 format (`data.structures[]`) not v1.0 (`data.structure`). Both `_parse_observations` and `_filter_codes_by_data` handle this with:
```python
structure = data_obj.get("structure") or (data_obj.get("structures") or [{}])[0]
```

**Structure building is two-step:**
1. Fetch full DSD (XML) — gives dimension names, order, complete codelists
2. Probe `/rest/data/.../all?detail=serieskeysonly` — gives only dimension values that actually have data in this specific flow (prevents 404s from selecting codes with no observations)

**FREQ dimension** is always filtered through `_apply_freq_allowlist()` — this runs on every `get_structure()` return (both cache hit and fresh fetch) to restrict frequency options to: Daily, Weekly, Monthly, Quarterly, Semi-Annual, Annual.

**Period parsing** (`_parse_sdmx_period`): handles `2024-Q1`, `2024Q1`, `2024-S1`, `2024-01`, `2024-01-01`, `2024`. Rows with unparseable periods are dropped before charting.

**Observation caching:** `time_period` is serialised as ISO strings (`.isoformat()`) for JSON storage and restored with `pd.to_datetime()` on cache read.

### Search (`modules/search.py`)

Calls Claude `claude-haiku-4-5-20251001` with the full ABS dataflow catalogue as context. Falls back to `difflib.SequenceMatcher` fuzzy matching if `ANTHROPIC_API_KEY` is unset or the API call fails. The LLM response is expected as a raw JSON array — markdown fences are stripped with regex before parsing.

### Analytics (`modules/analytics.py`)

- **Forecast:** Linear regression (`sklearn`) for all series; LSTM (`tensorflow.keras`) only when `len(series) >= 50`. LSTM is imported lazily inside `_lstm_forecast()` to avoid slow startup. Confidence bands = predicted ± 1.96 × residual std.
- **Anomaly detection:** Z-score threshold (default 2.5), adds `is_anomaly` bool column.
- **Correlation:** Aligns two DataFrames on `time_period` via inner join, returns Pearson + Spearman + merged DataFrame.

### Session State Keys

| Key | Set by | Purpose |
|---|---|---|
| `debug_mode` | `sidebar.py` toggle | Show API URL on Explore page |
| `cache_warmed` | `Home.py` startup | Prevent repeated warm_cache() calls |
| `cache_version` | `Home.py` | Force cache clear on version bump |

### Streamlit Widget Conventions

- Widget `key=` arguments use `{prefix}_{element}` format (e.g. `dim_FREQ`, `a_query`, `b_select`) to avoid key collisions between the two dataset selectors on the Compare page.
- `st.session_state.setdefault("debug_mode", False)` in `sidebar.py` ensures the key exists before any page reads it. Never pass `value=` to a toggle/widget that already has a `key=` — it overrides the user's choice on every re-render.
