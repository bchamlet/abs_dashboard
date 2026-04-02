import os
from dotenv import load_dotenv

load_dotenv()

ABS_BASE_URL = "https://data.api.abs.gov.au"
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

METADATA_TTL_HOURS = 24
DATA_TTL_HOURS = 6

# Pre-cached datasets for v1; add IDs here to warm cache on startup
V1_DATASETS = ["CPI", "LF"]

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "cache.db")
