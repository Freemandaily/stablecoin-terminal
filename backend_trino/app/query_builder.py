"""Server-side query builder for the flexible /query endpoint.

The caller picks columns and nested AND/OR filters. Nothing user-supplied is
ever interpolated except allowlisted identifiers and validated ints. All
values stay bound parameters.
"""
from fastapi import HTTPException

from app import config

# Column -> type. Types gate which operators are legal. These are exactly the
# columns materialized by dbt_spark/models/marts/*.sql.
TABLES: dict[str, dict[str, str]] = {
    "mart_issuer_events": {
        "block_number": "number",
        "block_timestamp": "timestamp",
        "transaction_hash": "string",
        "log_index": "number",
        "contract_address": "string",
        "token": "string",
        "event_type": "string",
        "topic1": "string",
        "topic2": "string",
        "blacklisted_address": "string",
        "destroyed_amount_raw": "number",
        "destroyed_amount_usdt": "number",
        "amount_raw": "number",
        "amount_usd": "number",
        "entity_name": "string",
        "entity_category": "string",
        "address_type": "string",
        "explorer_url": "string",
        "labels": "string",
        "has_label": "boolean",
        "label_source": "string",
        "label_fetched_at": "timestamp",
    },
    "mart_mint_burn_daily": {
        "date": "date",
        "token": "string",
        "minted_usd": "number",
        "burned_usd": "number",
        "net_usd": "number",
        "mint_count": "number",
        "burn_count": "number",
        "large_mint_count": "number",
        "large_burn_count": "number",
        "largest_mint_usd": "number",
    },
    "mart_peg_daily": {
        "date": "date",
        "asset_id": "string",
        "price_usd": "number",
        "dev_bps": "number",
        "dev_abs_bps": "number",
        "rolling_30d_avg_bps": "number",
        "rolling_30d_max_bps": "number",
        "is_depeg_event": "boolean",
    },
    "mart_supply_daily": {
        "date": "date",
        "asset_id": "string",
        "total_usd": "number",
        "net_issuance_1d": "number",
        "net_issuance_7d": "number",
        "net_issuance_30d": "number",
        "pct_7d": "number",
        "pct_30d": "number",
        "share_of_market": "number",
    },
    "mart_supply_chain": {
        "date": "date",
        "asset_id": "string",
        "chain": "string",
        "amount_usd": "number",
        "chain_share": "number",
        "share_delta_30d": "number",
    },
    "mart_market_overview": {
        "date": "date",
        "total_supply_usd": "number",
        "supply_delta_7d": "number",
        "core7_supply_usd": "number",
        "coverage_pct": "number",
        "usdt_share": "number",
        "usdc_share": "number",
        "top3_share": "number",
        "eth_share": "number",
        "tron_share": "number",
    },
    "mart_blacklist_events": {
        "block_timestamp": "timestamp",
        "token": "string",
        "event_type": "string",
        "blacklisted_address": "string",
        "amount_usd": "number",
        "destroyed_amount_usdt": "number",
        "block_number": "number",
        "transaction_hash": "string",
        "log_index": "number",
        "balance_raw": "number",
        "balance_usd": "number",
        "balance_block": "number",
        "entity_name": "string",
        "entity_category": "string",
        "address_type": "string",
        "explorer_url": "string",
        "labels": "string",
        "has_label": "boolean",
        "is_currently_blacklisted": "boolean",
        "state_since": "timestamp",
    },
}

_OPS_BY_TYPE: dict[str, set[str]] = {
    "string": {"=", "!=", "IN", "LIKE", "IS NULL", "IS NOT NULL"},
    "number": {"=", "!=", ">", ">=", "<", "<=", "IN", "IS NULL", "IS NOT NULL"},
    "date": {"=", "!=", ">", ">=", "<", "<=", "IS NULL", "IS NOT NULL"},
    "timestamp": {"=", "!=", ">", ">=", "<", "<=", "IS NULL", "IS NOT NULL"},
    "boolean": {"=", "!=", "IS NULL", "IS NOT NULL"},
}

_MAX_FILTERS = 50
_MAX_DEPTH = 5
_MAX_IN_ITEMS = 100

# Public alias for the columns discovery endpoint.
OPERATORS: dict[str, list[str]] = {k: sorted(v) for k, v in _OPS_BY_TYPE.items()}


def _fail(msg: str) -> HTTPException:
    return HTTPException(status_code=422, detail=msg)


def _check_table(table: str) -> dict[str, str]:
    cols = TABLES.get(table)
    if cols is None:
        raise _fail(f"unknown table '{table}', expected one of {sorted(TABLES)}")
    return cols


def _check_columns(table: str, columns, cols: dict[str, str]) -> list[str]:
    if columns is None:
        return list(cols)
    if not isinstance(columns, list) or not columns or len(columns) > len(cols):
        raise _fail("columns must be a non-empty list")
    seen = set()
    for c in columns:
        if not isinstance(c, str) or c not in cols or c in seen:
            raise _fail(f"invalid column '{c}' for table '{table}'")
        seen.add(c)
    return list(seen)


def _check_scalar(value, what: str):
    if isinstance(value, bool) or isinstance(value, (str, int, float)) or value is None:
        return value
    raise _fail(f"{what} must be a string, number, boolean or null")


def _normalize_temporal(col_type: str, value):
    """Trino CAST(? AS TIMESTAMP) rejects ISO 'T' separator, normalize to space."""
    if isinstance(value, str) and col_type in ("timestamp", "date"):
        v = value.strip()
        if v.endswith("Z"):
            v = v[:-1]
        return v.replace("T", " ")
    return value


def _placeholder(col_type: str, value) -> str:
    """Date/timestamp strings need an explicit cast in Trino."""
    if isinstance(value, str) and col_type == "date":
        return "CAST(? AS DATE)"
    if isinstance(value, str) and col_type == "timestamp":
        return "CAST(? AS TIMESTAMP)"
    return "?"


def _condition(table: str, cols: dict[str, str], node: dict, params: list) -> str:
    try:
        column = node["column"]
        operator = node["operator"]
    except (KeyError, TypeError):
        raise _fail("each condition needs 'column' and 'operator'")
    if column not in cols:
        raise _fail(f"invalid column '{column}' for table '{table}'")
    col_type = cols[column]
    if not isinstance(operator, str):
        raise _fail(f"invalid operator for column '{column}'")
    operator = operator.upper()
    if operator not in _OPS_BY_TYPE[col_type]:
        raise _fail(f"operator '{operator}' not allowed on {col_type} column '{column}'")

    if operator in ("IS NULL", "IS NOT NULL"):
        return f"{column} {operator}"
    if "value" not in node:
        raise _fail(f"condition on '{column}' needs a 'value'")
    value = node["value"]

    if operator == "IN":
        if not isinstance(value, list) or not value or len(value) > _MAX_IN_ITEMS:
            raise _fail(f"IN on '{column}' needs a non-empty list (max {_MAX_IN_ITEMS})")
        parts = []
        for v in value:
            v = _check_scalar(v, f"IN item on '{column}'")
            if v is None:
                raise _fail(f"IN on '{column}' cannot contain null")
            v = _normalize_temporal(col_type, v)
            parts.append(_placeholder(col_type, v))
            params.append(v)
        return f"{column} IN ({', '.join(parts)})"

    value = _check_scalar(value, f"value on '{column}'")
    if value is None:
        raise _fail(f"operator '{operator}' on '{column}' cannot take null, use IS NULL")
    if operator == "LIKE" and not isinstance(value, str):
        raise _fail(f"LIKE on '{column}' needs a string")
    if col_type == "boolean" and not isinstance(value, bool):
        raise _fail(f"boolean column '{column}' needs true/false")
    if col_type == "number" and isinstance(value, str):
        raise _fail(f"numeric column '{column}' needs a number")
    if col_type == "string" and not isinstance(value, str):
        raise _fail(f"string column '{column}' needs a string")
    value = _normalize_temporal(col_type, value)
    params.append(value)
    return f"{column} {operator} {_placeholder(col_type, value)}"


def _group(table: str, cols: dict[str, str], node: dict, params: list, depth: int) -> str:
    if depth > _MAX_DEPTH:
        raise _fail(f"filter nesting deeper than {_MAX_DEPTH} is not allowed")
    if not isinstance(node, dict):
        raise _fail("each filter must be an object")
    if "column" in node:
        return _condition(table, cols, node, params)
    op = node.get("op")
    children = node.get("conditions")
    if not isinstance(op, str) or op.lower() not in ("and", "or"):
        raise _fail("each filter group needs 'op' of 'and'/'or' plus 'conditions'")
    if not isinstance(children, list) or not children:
        raise _fail("filter group 'conditions' must be a non-empty list")
    parts = [_group(table, cols, child, params, depth + 1) for child in children]
    if len(params) > _MAX_FILTERS * 2:
        raise _fail(f"too many filter values (max {_MAX_FILTERS} conditions)")
    return f"({f' {op.upper()} '.join(parts)})"


def build_select(
    table: str,
    columns,
    filters,
    order_by,
    limit: int,
    offset: int,
    aggregate=None,
) -> tuple[str, tuple]:
    """Validate everything, return (sql, params). Raises HTTPException(422)."""
    cols = _check_table(table)
    if aggregate is not None:
        if not isinstance(aggregate, str) or aggregate.lower() != "count":
            raise _fail("aggregate must be 'count' or omitted")
        select = ["COUNT(*) AS count"]
    else:
        select = _check_columns(table, columns, cols)

    if not isinstance(limit, int) or isinstance(limit, bool):
        raise _fail("limit must be an integer")
    if limit < 1 or limit > config.MAX_LIMIT:
        raise _fail(f"limit must be 1..{config.MAX_LIMIT}")
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise _fail("offset must be an integer >= 0")

    params: list = []
    where = ""
    if filters is not None:
        clause = _group(table, cols, filters, params, 0)
        where = f"WHERE {clause}"

    order = ""
    if order_by is not None:
        if aggregate is not None:
            raise _fail("order_by is not allowed with aggregate 'count'")
        if not isinstance(order_by, dict):
            raise _fail("order_by must be {'column': ..., 'direction': 'asc'/'desc'}")
        col = order_by.get("column")
        direction = str(order_by.get("direction", "desc")).lower()
        if col not in cols:
            raise _fail(f"invalid order_by column '{col}' for table '{table}'")
        if direction not in ("asc", "desc"):
            raise _fail("order_by direction must be 'asc' or 'desc'")
        order = f"ORDER BY {col} {direction.upper()}"

    sql = f"""
        SELECT {', '.join(select)}
        FROM {config.table(table)}
        {where}
        {order}
        OFFSET {offset} LIMIT {limit}
    """
    return sql, tuple(params)
