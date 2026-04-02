"""
Dataflow discovery and metadata warm-up.
"""
from config import V1_DATASETS
from modules.abs_client import list_dataflows, get_structure


def warm_cache() -> None:
    """
    Fetch all ABS dataflows (cached 24h) and pre-fetch structures
    for V1 datasets. Called once on app startup.
    """
    flows = list_dataflows()
    # Build a version lookup for V1 datasets
    version_map = {f["id"]: f["version"] for f in flows}
    for ds_id in V1_DATASETS:
        version = version_map.get(ds_id, "1.0.0")
        try:
            get_structure(ds_id, version)
        except Exception:
            pass  # Don't block startup on a single failed pre-fetch


def get_all_dataflow_summaries() -> list[dict]:
    """
    Return [{id, name, description}, ...] for all ABS dataflows.
    Used by search.py to build the catalogue sent to the LLM.
    """
    flows = list_dataflows()
    return [{"id": f["id"], "name": f["name"], "description": f["description"]} for f in flows]


def get_dataflow_version(dataflow_id: str) -> str:
    """Return the version string for a given dataflow ID, defaulting to '1.0.0'."""
    flows = list_dataflows()
    for f in flows:
        if f["id"] == dataflow_id:
            return f["version"]
    return "1.0.0"
