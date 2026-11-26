"""Backend config, all overridable by env vars."""
import os

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "api-ro")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "lake")
TRINO_SOURCE_SCHEMA = os.environ.get("TRINO_SOURCE_SCHEMA", "stableflow")
TRINO_MARTS_SCHEMA = os.environ.get("TRINO_MARTS_SCHEMA", "marts")
TRINO_HTTP_SCHEME = os.environ.get("TRINO_HTTP_SCHEME", "http")

# Empty means auth disabled (local dev). Set API_KEY to enforce X-API-Key.
API_KEY = os.environ.get("API_KEY", "")

CACHE_TTL_SHORT = int(os.environ.get("CACHE_TTL_SHORT", "60"))
CACHE_TTL_LONG = int(os.environ.get("CACHE_TTL_LONG", "600"))

QUERY_TIMEOUT_S = int(os.environ.get("QUERY_TIMEOUT_S", "60"))
MAX_LIMIT = 100
DEFAULT_LIMIT = 25


def table(name: str) -> str:
    """Fully qualified mart ref. This API only serves dbt marts."""
    return f"{TRINO_CATALOG}.{TRINO_MARTS_SCHEMA}.{name}"
