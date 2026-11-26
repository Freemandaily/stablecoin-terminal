import pytest
from fastapi import HTTPException

from app import query_builder, sql_gate


def test_build_select_qualifies_table_and_binds_values():
    sql, params = query_builder.build_select(
        "mart_peg_daily",
        ["date", "dev_bps"],
        {"column": "dev_abs_bps", "operator": ">", "value": 50},
        {"column": "date", "direction": "desc"},
        10,
        0,
    )
    assert "lake.marts.mart_peg_daily" in sql
    assert "dev_abs_bps > ?" in sql
    assert "ORDER BY date DESC" in sql
    assert "OFFSET 0 LIMIT 10" in sql
    assert params == (50,)


def test_build_select_nested_filters_and_date_cast():
    sql, params = query_builder.build_select(
        "mart_mint_burn_daily",
        None,
        {
            "op": "and",
            "conditions": [
                {"column": "date", "operator": ">=", "value": "2026-11-01"},
                {"column": "net_usd", "operator": "IN", "value": [1, 2]},
            ],
        },
        None,
        5,
        0,
    )
    assert "CAST(? AS DATE)" in sql
    assert "net_usd IN (?, ?)" in sql
    assert params == ("2026-11-01", 1, 2)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"table": "transfers"},
        {"table": "mart_peg_daily", "columns": ["secret_column"]},
        {"table": "mart_peg_daily", "limit": 101},
        {"table": "mart_peg_daily", "filters": {"column": "dev_bps", "operator": "DROP"}},
        {"table": "mart_peg_daily", "filters": {"column": "dev_bps", "operator": "=", "value": "x"}},
    ],
)
def test_builder_rejects_bad_input(kwargs):
    defaults = dict(columns=None, filters=None, order_by=None, limit=25, offset=0)
    defaults.update(kwargs)
    with pytest.raises(HTTPException) as exc:
        query_builder.build_select(**defaults)
    assert exc.value.status_code == 422


def test_gate_pins_table_and_injects_limit():
    out = sql_gate.gate("SELECT date, dev_bps FROM mart_peg_daily WHERE dev_bps > 10")
    unquoted = out.replace('"', "")
    assert "lake.marts.mart_peg_daily" in unquoted
    assert "LIMIT 100" in unquoted.upper()


def test_gate_caps_limit():
    out = sql_gate.gate("SELECT date FROM mart_peg_daily LIMIT 5000")
    assert "LIMIT 100" in out.upper()
    assert "LIMIT 5000" not in out.upper()


def test_gate_keeps_cte_and_joins():
    out = sql_gate.gate(
        "WITH d AS (SELECT date, asset_id FROM mart_peg_daily) "
        "SELECT d.date, s.total_usd FROM d "
        "JOIN mart_supply_daily s ON s.date = d.date"
    )
    unquoted = out.replace('"', "")
    assert "WITH" in unquoted.upper()
    assert "lake.marts.mart_peg_daily" in unquoted
    assert "lake.marts.mart_supply_daily" in unquoted


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM mart_peg_daily",
        "SELECT secret_column FROM mart_peg_daily",
        "SELECT date FROM raw_issuer_events",
        "SELECT 1; SELECT 2",
    ],
)
def test_gate_rejects_bad_sql(sql):
    with pytest.raises(HTTPException) as exc:
        sql_gate.gate(sql)
    assert exc.value.status_code == 422
