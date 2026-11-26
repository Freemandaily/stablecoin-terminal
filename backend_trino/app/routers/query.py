"""Flexible query endpoint: caller picks columns and nested AND/OR filters.

POST /query/{table} for any allowlisted dbt mart. Tables, columns and operators
are allowlisted; values stay bound parameters.
"""
import json
import time
from typing import Any, Union

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app import auth, cache, config, query_builder, trino_client


class Condition(BaseModel):
    column: str = Field(examples=["dev_bps"])
    operator: str = Field(examples=["=", ">", "IN", "LIKE", "IS NULL"])
    value: Any = Field(default=None, examples=[100, "USDT", ["USDT", "USDC"]])


class FilterGroup(BaseModel):
    op: str = Field(examples=["and"])
    conditions: list[Any] = Field(examples=[[]])


FilterNode = Union[Condition, FilterGroup]


class OrderBy(BaseModel):
    column: str = Field(examples=["date"])
    direction: str = Field(default="desc", examples=["desc"])


class QueryBody(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "columns": ["date", "asset_id", "dev_bps"],
                    "filters": {
                        "op": "and",
                        "conditions": [
                            {"column": "date", "operator": ">=", "value": "2026-11-01"},
                            {"column": "dev_abs_bps", "operator": ">", "value": 50},
                        ],
                    },
                    "order_by": {"column": "date", "direction": "desc"},
                    "limit": 25,
                    "offset": 0,
                }
            ]
        }
    )

    columns: Union[list[str], None] = None
    filters: Union[FilterNode, None] = None
    order_by: Union[OrderBy, None] = None
    aggregate: Union[str, None] = None
    limit: int = config.DEFAULT_LIMIT
    offset: int = 0


router = APIRouter(prefix="/query", tags=["query"])


@router.get(
    "/columns/{table}",
    dependencies=[Depends(auth.require_key)],
    summary="Table columns and operators",
    description="Allowlisted columns with types plus legal operators per type.",
)
def table_columns(table: str):
    cols = query_builder.TABLES.get(table)
    if cols is None:
        raise HTTPException(status_code=422, detail=f"unknown table '{table}'")
    return {"table": table, "columns": cols, "operators": query_builder.OPERATORS}


@router.post(
    "/{table}",
    dependencies=[Depends(auth.require_key)],
    summary="Flexible mart query",
    description=(
        "Pick columns and nested AND/OR filters. Tables, columns and operators "
        "are allowlisted per column type. Values stay bound parameters. "
        f"Tables: {', '.join(sorted(query_builder.TABLES))}."
    ),
)
def run_table_query(table: str, body: QueryBody):
    payload = body.model_dump()
    sql, params = query_builder.build_select(
        table=table,
        columns=payload["columns"],
        filters=payload["filters"],
        order_by=payload["order_by"],
        limit=payload["limit"],
        offset=payload["offset"],
        aggregate=payload.get("aggregate"),
    )

    key = cache.make_key("query", table, json.dumps(payload, sort_keys=True, default=str))
    hit = cache.get(key)
    if hit is not None:
        return {"cached": True, "data": hit}

    t0 = time.time()
    rows = trino_client.run_query(sql, params)
    print(f"query {table} limit={payload['limit']} took={time.time()-t0:.1f}s", flush=True)
    cache.set(key, rows, config.CACHE_TTL_SHORT)
    return {"cached": False, "data": rows}
