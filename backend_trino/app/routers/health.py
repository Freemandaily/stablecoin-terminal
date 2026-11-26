from fastapi import APIRouter, Depends

from app import auth, config, query_builder, trino_client

router = APIRouter()


@router.get(
    "",
    dependencies=[Depends(auth.require_key)],
    summary="Trino health",
    description="Returns ok when Trino is reachable, degraded otherwise.",
)
def health():
    try:
        rows = trino_client.run_query("SELECT 1 AS ok")
        return {"status": "ok", "trino": "reachable", "result": rows}
    except Exception as e:
        return {"status": "degraded", "trino": "unreachable", "detail": str(e)}


@router.get(
    "/tables",
    dependencies=[Depends(auth.require_key)],
    summary="Served marts",
    description="Which Iceberg marts this API reads, fully qualified.",
)
def tables():
    """Which marts the API serves and where they come from."""
    return {
        "catalog": config.TRINO_CATALOG,
        "schema": config.TRINO_MARTS_SCHEMA,
        "warehouse": "gs://term-flow/warehouse",
        "tables": [config.table(name) for name in sorted(query_builder.TABLES)],
    }
