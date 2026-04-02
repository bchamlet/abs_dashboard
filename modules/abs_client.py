"""
ABS SDMX REST API client.

- Metadata endpoints (dataflows, structures) return XML — parsed with ElementTree.
- Data observation endpoint supports JSON via Accept: application/vnd.sdmx.data+json.
- Observations are always fetched with data_key=all and cached as Parquet files.
- All public methods check the SQLite/Parquet cache before making HTTP requests.
"""
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

import httpx
import pandas as pd

from config import ABS_BASE_URL, METADATA_TTL_HOURS, DATA_TTL_HOURS, FETCH_YEARS_DEFAULT
from modules.cache import cache

# SDMX 2.1 XML namespaces
_NS = {
    "message": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "structure": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "common": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}


def _get_xml(url: str, params: dict | None = None) -> ET.Element:
    """GET a metadata endpoint and return the parsed XML root."""
    resp = httpx.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return ET.fromstring(resp.content)


def _get_json(url: str, params: dict | None = None) -> dict:
    """GET a data endpoint requesting SDMX-JSON."""
    headers = {"Accept": "application/vnd.sdmx.data+json"}
    resp = httpx.get(url, params=params, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Dataflows
# ---------------------------------------------------------------------------

def list_dataflows() -> list[dict]:
    """
    Return all ABS dataflows as [{id, name, description, version}, ...].
    Cached for METADATA_TTL_HOURS.
    """
    cache_key = "dataflows:ABS"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{ABS_BASE_URL}/rest/dataflow/ABS"
    root = _get_xml(url, params={"detail": "allstubs"})
    flows = _parse_dataflows_xml(root)
    cache.set(cache_key, flows, METADATA_TTL_HOURS)
    return flows


def _parse_dataflows_xml(root: ET.Element) -> list[dict]:
    results = []
    ns_struct = _NS["structure"]
    ns_common = _NS["common"]

    for df_elem in root.iter(f"{{{ns_struct}}}Dataflow"):
        df_id = df_elem.get("id", "")
        version = df_elem.get("version", "1.0.0")
        if not df_id:
            continue

        name = ""
        description = ""
        for name_elem in df_elem.findall(f"{{{ns_common}}}Name"):
            lang = name_elem.get("{http://www.w3.org/XML/1998/namespace}lang", "")
            text = (name_elem.text or "").strip()
            if lang == "en" or not name:
                name = text
        for desc_elem in df_elem.findall(f"{{{ns_common}}}Description"):
            lang = desc_elem.get("{http://www.w3.org/XML/1998/namespace}lang", "")
            text = (desc_elem.text or "").strip()
            if lang == "en" or not description:
                description = text

        results.append({"id": df_id, "name": name, "description": description, "version": version})

    return results


# ---------------------------------------------------------------------------
# Structure (dimensions + codes)
# ---------------------------------------------------------------------------

def get_structure(dataflow_id: str, version: str = "1.0.0") -> dict:
    """
    Return parsed structure for a dataflow:
    {
        "dimensions": [
            {"id": "MEASURE", "name": "Measure", "codes": [{"id": "1", "name": "CPI All groups"}, ...]},
            ...
        ]
    }
    Codes are filtered to only those that exist in actual data (via serieskeysonly probe).
    Cached for METADATA_TTL_HOURS.
    """
    cache_key = f"structure:{dataflow_id}:{version}"
    cached = cache.get(cache_key)
    if cached is not None:
        return _apply_freq_allowlist(cached)

    dsd_url = f"{ABS_BASE_URL}/rest/datastructure/ABS/{dataflow_id}/{version}"
    root = _get_xml(dsd_url, params={"references": "all", "detail": "full"})
    structure = _parse_structure_xml(root)

    try:
        data_url = f"{ABS_BASE_URL}/rest/data/ABS,{dataflow_id},{version}/all"
        probe = _get_json(data_url, params={"detail": "serieskeysonly", "dimensionAtObservation": "TIME_PERIOD"})
        structure = _filter_codes_by_data(structure, probe)
    except Exception:
        structure = _apply_freq_allowlist(structure)

    cache.set(cache_key, structure, METADATA_TTL_HOURS)
    return structure


# Allowed FREQ codes with user-friendly display names.
_FREQ_ALLOWLIST: dict[str, str] = {
    "D": "Daily",
    "W": "Weekly",
    "M": "Monthly",
    "Q": "Quarterly",
    "S": "Semi-Annual",
    "A": "Annual",
}


def _filter_codes_by_data(structure: dict, probe_data: dict) -> dict:
    """
    Replace each dimension's code list with only the values that appear in the
    data, using the filtered dimension value arrays from the serieskeysonly response.
    Handles both SDMX-JSON v1.0 (data.structure) and v2.0 (data.structures[]).
    Also applies the FREQ allowlist to the FREQ dimension.
    """
    try:
        sdmx_structure = _sdmx_structure(probe_data)
        series_dims = sdmx_structure.get("dimensions", {}).get("series", [])
        if not series_dims:
            return _apply_freq_allowlist(structure)

        # Build lookup: dim_id -> {json_name, codes}
        # The JSON name is what _parse_observations uses for DataFrame columns,
        # so we must use it in the structure too to ensure filter_dataframe can match.
        live_dims: dict[str, dict] = {}
        for dim in series_dims:
            dim_id = dim.get("id", "")
            dim_name = _get_en_name(dim.get("name", {})) or dim_id
            values = dim.get("values", [])
            live_dims[dim_id] = {
                "name": dim_name,
                "codes": [{"id": v.get("id", ""), "name": _get_en_name(v.get("name", {}))} for v in values],
            }

        updated_dims = []
        for dim in structure["dimensions"]:
            if dim["id"] in live_dims and live_dims[dim["id"]]["codes"]:
                live = live_dims[dim["id"]]
                # Update both name (from JSON — matches DataFrame columns) and filtered codes
                updated_dims.append({**dim, "name": live["name"], "codes": live["codes"]})
            else:
                updated_dims.append(dim)
        return _apply_freq_allowlist({"dimensions": updated_dims})
    except Exception:
        return _apply_freq_allowlist(structure)


def _apply_freq_allowlist(structure: dict) -> dict:
    """
    For the FREQ dimension, keep only codes in _FREQ_ALLOWLIST and
    replace their display names with the user-friendly versions.
    """
    updated_dims = []
    for dim in structure.get("dimensions", []):
        if "FREQ" in dim.get("id", "").upper():
            filtered = [
                {"id": c["id"], "name": _FREQ_ALLOWLIST[c["id"]]}
                for c in dim.get("codes", [])
                if c["id"] in _FREQ_ALLOWLIST
            ]
            updated_dims.append({**dim, "codes": filtered})
        else:
            updated_dims.append(dim)
    return {"dimensions": updated_dims}


def _parse_structure_xml(root: ET.Element) -> dict:
    ns_struct = _NS["structure"]
    ns_common = _NS["common"]

    codelists: dict[str, list[dict]] = {}
    for cl_elem in root.iter(f"{{{ns_struct}}}Codelist"):
        cl_id = cl_elem.get("id", "")
        codes = []
        for code_elem in cl_elem.findall(f"{{{ns_struct}}}Code"):
            code_id = code_elem.get("id", "")
            code_name = _xml_en_name(code_elem, ns_common) or code_id
            codes.append({"id": code_id, "name": code_name})
        codelists[cl_id] = codes

    dimensions = []
    for dim_elem in root.iter(f"{{{ns_struct}}}Dimension"):
        dim_id = dim_elem.get("id", "")
        position = int(dim_elem.get("position", 99))
        dim_name = _xml_en_name(dim_elem, ns_common) or dim_id

        cl_ref_id = ""
        local_rep = dim_elem.find(f"{{{ns_struct}}}LocalRepresentation")
        if local_rep is not None:
            enum_elem = local_rep.find(f"{{{ns_struct}}}Enumeration")
            if enum_elem is not None:
                ref_elem = enum_elem.find("Ref")
                if ref_elem is not None:
                    cl_ref_id = ref_elem.get("id", "")

        codes = codelists.get(cl_ref_id, [])
        dimensions.append({"id": dim_id, "name": dim_name, "position": position, "codes": codes})

    dimensions.sort(key=lambda d: d["position"])
    for d in dimensions:
        d.pop("position", None)

    return {"dimensions": dimensions}


def _xml_en_name(elem: ET.Element, ns_common: str) -> str:
    """Extract English Name text from an element's child Name elements."""
    fallback = ""
    for name_elem in elem.findall(f"{{{ns_common}}}Name"):
        lang = name_elem.get("{http://www.w3.org/XML/1998/namespace}lang", "")
        text = (name_elem.text or "").strip()
        if lang == "en":
            return text
        if not fallback:
            fallback = text
    return fallback


# ---------------------------------------------------------------------------
# Observations (data)
# ---------------------------------------------------------------------------

def get_observations(
    dataflow_id: str,
    version: str,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    dataflow_name: Optional[str] = None,
    force_refresh: bool = False,
    is_warm_cache: bool = False,
) -> pd.DataFrame:
    """
    Fetch ALL observations for a dataflow and return as a Pandas DataFrame:
    [time_period, value, <dimension columns>]

    Always fetches data_key=all from the ABS API. Dimension filtering is done
    locally by the caller using filter_dataframe().

    Defaults to the last FETCH_YEARS_DEFAULT years if no period is specified.
    Cached as a Parquet file for DATA_TTL_HOURS.
    """
    current_year = datetime.now().year
    if start_period is None:
        start_period = str(current_year - FETCH_YEARS_DEFAULT)
    if end_period is None:
        end_period = str(current_year)

    cache_key = f"obs:{dataflow_id}:{version}:{start_period}:{end_period}"

    if not force_refresh:
        cached_df = cache.get_df(cache_key)
        if cached_df is not None:
            # Invalidate Parquet files built before dimension columns were added.
            # A valid file always has more columns than just time_period + value.
            if set(cached_df.columns) - {"time_period", "value"}:
                return cached_df
            cache.catalog_invalidate(cache_key)  # stale — re-fetch below

    params: dict = {
        "detail": "full",
        "dimensionAtObservation": "TIME_PERIOD",
        "startPeriod": start_period,
        "endPeriod": end_period,
    }
    url = f"{ABS_BASE_URL}/rest/data/ABS,{dataflow_id},{version}/all"
    data = _get_json(url, params=params)
    df = _parse_observations(data)

    meta = {
        "dataflow_id": dataflow_id,
        "dataflow_name": dataflow_name or dataflow_id,
        "version": version,
        "start_period": start_period,
        "end_period": end_period,
        "is_warm_cache": is_warm_cache,
    }
    cache.set_df(cache_key, df, DATA_TTL_HOURS, meta)
    return df


def filter_dataframe(
    df: pd.DataFrame,
    structure: dict,
    dim_selections: dict[str, str],
) -> pd.DataFrame:
    """
    Filter a full 'all' DataFrame to match the user's dimension selections.

    dim_selections: {dim_id → code_id}
    Matches against human-readable dimension name columns already present in df.
    Falls back to the dimension ID as column name when the structure name doesn't
    match (can happen if the serieskeysonly probe failed and XML names were used).
    """
    for dim in structure.get("dimensions", []):
        code_id = dim_selections.get(dim["id"])
        if not code_id:
            continue
        # Resolve column name: prefer structure name, fall back to dim ID
        col_name = dim.get("name", dim["id"])
        if col_name not in df.columns:
            col_name = dim["id"]
        if col_name not in df.columns:
            continue
        code_name = next(
            (c["name"] for c in dim.get("codes", []) if c["id"] == code_id), None
        )
        if code_name:
            df = df[df[col_name] == code_name]
    return df.reset_index(drop=True)


def _sdmx_structure(data: dict) -> dict:
    """
    Locate the SDMX structure object from a JSON data response.

    ABS SDMX-JSON v2.0 puts the structure at the TOP LEVEL as data["structures"][0].
    Older / other implementations put it inside data["data"]["structure"] (v1.0)
    or data["data"]["structures"][0] (v2.0 non-ABS).
    """
    data_obj = data.get("data", {})
    return (
        (data.get("structures") or [{}])[0]         # ABS v2.0 — top-level
        or data_obj.get("structure")                 # v1.0 — inside data
        or (data_obj.get("structures") or [{}])[0]  # v2.0 other — inside data
    )


def _parse_observations(data: dict) -> pd.DataFrame:
    """Parse SDMX-JSON data response into a tidy DataFrame.

    Handles ABS SDMX-JSON v2.0 (top-level structures[]), v1.0 (data.structure),
    and v2.0 non-ABS (data.structures[]).
    """
    rows = []
    try:
        data_obj = data.get("data", {})
        dataset = data_obj.get("dataSets", [{}])[0]
        structure = _sdmx_structure(data)
        dimensions = structure.get("dimensions", {}).get("series", [])
        obs_dims = structure.get("dimensions", {}).get("observation", [])
        series_data = dataset.get("series", {})

        for series_key_str, series_obj in series_data.items():
            key_parts = series_key_str.split(":")
            dim_values: dict[str, str] = {}
            for i, dim in enumerate(dimensions):
                if i < len(key_parts):
                    idx = int(key_parts[i])
                    dim_name = _get_en_name(dim.get("name", {})) or dim.get("id", f"dim{i}")
                    codes = dim.get("values", [])
                    code_name = _get_en_name(codes[idx].get("name", {})) if idx < len(codes) else str(idx)
                    dim_values[dim_name] = code_name

            observations = series_obj.get("observations", {})
            for obs_key_str, obs_values in observations.items():
                obs_idx = int(obs_key_str)
                time_values = obs_dims[0].get("values", []) if obs_dims else []
                time_period = time_values[obs_idx].get("id", "") if obs_idx < len(time_values) else obs_key_str
                value = obs_values[0] if obs_values else None
                rows.append({"time_period": time_period, "value": value, **dim_values})
    except Exception:
        pass

    df = pd.DataFrame(rows)
    if not df.empty and "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["time_period"] = df["time_period"].apply(_parse_sdmx_period)
        df.dropna(subset=["time_period"], inplace=True)
        df.sort_values("time_period", inplace=True)
        df.reset_index(drop=True, inplace=True)
    return df


def _parse_sdmx_period(period: str) -> pd.Timestamp:
    """
    Convert ABS/SDMX period strings to a Timestamp.
    Handles: 2024-01-01, 2024-Q1, 2024Q1, 2024-S1, 2024-01..2024-12, 2024
    """
    import re
    if not isinstance(period, str):
        return pd.NaT
    period = period.strip()
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", period)
    if m:
        return pd.Timestamp(year=int(m.group(1)), month=int(m.group(2)), day=int(m.group(3)))
    m = re.fullmatch(r"(\d{4})-Q([1-4])", period)
    if m:
        return pd.Timestamp(year=int(m.group(1)), month=(int(m.group(2)) - 1) * 3 + 1, day=1)
    m = re.fullmatch(r"(\d{4})Q([1-4])", period)
    if m:
        return pd.Timestamp(year=int(m.group(1)), month=(int(m.group(2)) - 1) * 3 + 1, day=1)
    m = re.fullmatch(r"(\d{4})-S([12])", period)
    if m:
        return pd.Timestamp(year=int(m.group(1)), month=1 if m.group(2) == "1" else 7, day=1)
    m = re.fullmatch(r"(\d{4})-(\d{2})", period)
    if m:
        return pd.Timestamp(year=int(m.group(1)), month=int(m.group(2)), day=1)
    m = re.fullmatch(r"(\d{4})", period)
    if m:
        return pd.Timestamp(year=int(m.group(1)), month=1, day=1)
    return pd.NaT


def _get_en_name(name_obj: dict | str) -> str:
    if isinstance(name_obj, str):
        return name_obj
    return name_obj.get("en") or name_obj.get("") or next(iter(name_obj.values()), "")
