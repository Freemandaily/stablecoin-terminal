"""Raw SQL endpoint, gated by sql_gate: full Trino SELECT, policy enforced."""
import time

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app import auth, cache, config, sql_gate, trino_client


class SqlBody(BaseModel):
    sql: str = Field(
        examples=[
            "SELECT date_trunc('month', block_timestamp) AS m, count(*) AS n "
            "FROM mart_issuer_events GROUP BY 1 ORDER BY 1 LIMIT 5"
        ]
    )


router = APIRouter(prefix="/sql", tags=["sql"])


@router.post(
    "",
    dependencies=[Depends(auth.require_key)],
    summary="Raw Trino SELECT",
    description=(
        "Full Trino SQL: date_trunc, EXTRACT, CASE, CTEs, JOINs, GROUP BY. "
        "Exactly one SELECT; tables and columns must belong to the mart "
        "allowlist; LIMIT is injected when missing and capped at 100. "
        "No DDL/DML."
    ),
)
def run_raw_sql(body: SqlBody):
    final = sql_gate.gate(body.sql)
    key = cache.make_key("sql", final)
    hit = cache.get(key)
    if hit is not None:
        return {"cached": True, "data": hit}

    t0 = time.time()
    rows = trino_client.run_query(final, ())
    print(f"sql took={time.time()-t0:.1f}s", flush=True)
    cache.set(key, rows, config.CACHE_TTL_SHORT)
    return {"cached": False, "data": rows}
