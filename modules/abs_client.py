"""
ABS SDMX REST API client.

- Metadata endpoints (dataflows, structures) return XML — parsed with ElementTree.
- Data observation endpoint supports JSON via format=jsondata query param.
- All public methods check the SQLite cache before making HTTP requests.
"""
import json
import xml.etree.ElementTree as ET
import httpx
import pandas as pd
from typing import Optional

from config import ABS_BASE_URL, METADATA_TTL_HOURS, DATA_TTL_HOURS
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

        # Name: prefer xml:lang="en", fall back to first Name element text
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

    # Step 1: full DSD for dimension names, order, and complete codelists
    dsd_url = f"{ABS_BASE_URL}/rest/datastructure/ABS/{dataflow_id}/{version}"
    root = _get_xml(dsd_url, params={"references": "all", "detail": "full"})
    structure = _parse_structure_xml(root)

    # Step 2: probe the data endpoint with serieskeysonly to get the filtered
    # dimension value lists that actually have data in this specific flow.
    try:
        data_url = f"{ABS_BASE_URL}/rest/data/ABS,{dataflow_id},{version}/all"
        probe = _get_json(data_url, params={"detail": "serieskeysonly", "dimensionAtObservation": "TIME_PERIOD"})
        structure = _filter_codes_by_data(structure, probe)
    except Exception:
        structure = _apply_freq_allowlist(structure)  # still filter even without probe

    cache.set(cache_key, structure, METADATA_TTL_HOURS)
    return structure


# Allowed FREQ codes with user-friendly display names.
# Any code not in this map is excluded from the dimension selector.
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
        data_obj = probe_data.get("data", {})
        # v2.0 uses "structures" (list); v1.0 uses "structure" (dict)
        sdmx_structure = data_obj.get("structure") or (data_obj.get("structures") or [{}])[0]
        series_dims = sdmx_structure.get("dimensions", {}).get("series", [])
        if not series_dims:
            return _apply_freq_allowlist(structure)

        # Build lookup: dimension id -> [{id, name}, ...] (already filtered by API)
        live_codes: dict[str, list[dict]] = {}
        for dim in series_dims:
            dim_id = dim.get("id", "")
            values = dim.get("values", [])
            live_codes[dim_id] = [{"id": v.get("id", ""), "name": _get_en_name(v.get("name", {}))} for v in values]

        # Overwrite codes in structure where we have live data
        updated_dims = []
        for dim in structure["dimensions"]:
            if dim["id"] in live_codes and live_codes[dim["id"]]:
                updated_dims.append({**dim, "codes": live_codes[dim["id"]]})
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

    # 1. Build codelist lookup: {codelist_id: [{id, name}, ...]}
    codelists: dict[str, list[dict]] = {}
    for cl_elem in root.iter(f"{{{ns_struct}}}Codelist"):
        cl_id = cl_elem.get("id", "")
        codes = []
        for code_elem in cl_elem.findall(f"{{{ns_struct}}}Code"):
            code_id = code_elem.get("id", "")
            code_name = _xml_en_name(code_elem, ns_common) or code_id
            codes.append({"id": code_id, "name": code_name})
        codelists[cl_id] = codes

    # 2. Extract dimensions from DimensionList
    dimensions = []
    for dim_elem in root.iter(f"{{{ns_struct}}}Dimension"):
        dim_id = dim_elem.get("id", "")
        position = int(dim_elem.get("position", 99))
        dim_name = _xml_en_name(dim_elem, ns_common) or dim_id

        # Resolve codelist reference via LocalRepresentation > Enumeration > Ref
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
    # Remove internal position key before returning
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
    data_key: str = "all",
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch data observations and return as a Pandas DataFrame with columns:
    [time_period, value, <dimension columns>]

    Cached for DATA_TTL_HOURS.
    """
    cache_key = f"obs:{dataflow_id}:{version}:{data_key}:{start_period}:{end_period}"
    cached = cache.get(cache_key)
    if cached is not None:
        df = pd.DataFrame(cached)
        if "time_period" in df.columns:
            df["time_period"] = pd.to_datetime(df["time_period"], errors="coerce")
        return df

    params: dict = {"detail": "full", "dimensionAtObservation": "TIME_PERIOD"}
    if start_period:
        params["startPeriod"] = start_period
    if end_period:
        params["endPeriod"] = end_period

    url = f"{ABS_BASE_URL}/rest/data/ABS,{dataflow_id},{version}/{data_key}"
    data = _get_json(url, params=params)
    df = _parse_observations(data)

    # Serialise time_period as ISO strings so json.dumps can handle them
    serialisable = df.copy()
    if "time_period" in serialisable.columns:
        serialisable["time_period"] = serialisable["time_period"].apply(
            lambda t: t.isoformat() if pd.notna(t) else None
        )
    cache.set(cache_key, serialisable.to_dict(orient="records"), DATA_TTL_HOURS)
    return df


def _parse_observations(data: dict) -> pd.DataFrame:
    """Parse SDMX-JSON data response into a tidy DataFrame.

    Handles both SDMX-JSON v1.0 (data.structure) and v2.0 (data.structures[]).
    """
    rows = []
    try:
        data_obj = data.get("data", {})

        dataset = data_obj.get("dataSets", [{}])[0]

        # v2.0 uses "structures" (list); v1.0 uses "structure" (dict)
        structure = data_obj.get("structure") or (data_obj.get("structures") or [{}])[0]

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
                row = {"time_period": time_period, "value": value, **dim_values}
                rows.append(row)
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
    Handles: 2024-Q1, 2024Q1, 2024-S1, 2024-01..2024-12, 2024, 2024-01-01
    """
    import re
    if not isinstance(period, str):
        return pd.NaT
    period = period.strip()
    # Full date: 2024-01-01
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", period)
    if m:
        return pd.Timestamp(year=int(m.group(1)), month=int(m.group(2)), day=int(m.group(3)))
    # Quarterly with hyphen: 2024-Q1
    m = re.fullmatch(r"(\d{4})-Q([1-4])", period)
    if m:
        month = (int(m.group(2)) - 1) * 3 + 1
        return pd.Timestamp(year=int(m.group(1)), month=month, day=1)
    # Quarterly without hyphen: 2024Q1
    m = re.fullmatch(r"(\d{4})Q([1-4])", period)
    if m:
        month = (int(m.group(2)) - 1) * 3 + 1
        return pd.Timestamp(year=int(m.group(1)), month=month, day=1)
    # Half-yearly: 2024-S1 / 2024-S2
    m = re.fullmatch(r"(\d{4})-S([12])", period)
    if m:
        month = 1 if m.group(2) == "1" else 7
        return pd.Timestamp(year=int(m.group(1)), month=month, day=1)
    # Monthly: 2024-01
    m = re.fullmatch(r"(\d{4})-(\d{2})", period)
    if m:
        return pd.Timestamp(year=int(m.group(1)), month=int(m.group(2)), day=1)
    # Annual: 2024
    m = re.fullmatch(r"(\d{4})", period)
    if m:
        return pd.Timestamp(year=int(m.group(1)), month=1, day=1)
    return pd.NaT


def _get_en_name(name_obj: dict | str) -> str:
    if isinstance(name_obj, str):
        return name_obj
    return name_obj.get("en") or name_obj.get("") or next(iter(name_obj.values()), "")
