"""Gated raw-SQL access: parse with sqlglot, enforce policy, forward to Trino.

Users write full Trino SQL (date_trunc, EXTRACT, CASE, CTEs, JOINs, GROUP BY).
The gate guarantees structurally, before Trino ever sees the text:

- exactly one statement, and it is a SELECT (WITH...SELECT and UNION ok)
- every real table is allowlisted and pinned to the marts catalog/schema
- every column resolves against the allowlisted columns (unknown fails closed)
- SELECT * expands to allowlisted columns only
- a LIMIT is always present and capped at MAX_LIMIT

User values are inline SQL by design in this mode; safety comes from the
read-only Trino role plus these structural guarantees, not parameter binding.
"""
import sqlglot
from fastapi import HTTPException
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify

from app import config, query_builder

ALLOWED_TABLES = set(query_builder.TABLES)
_MAX_SQL_CHARS = 20000


def _fail(msg: str) -> HTTPException:
    return HTTPException(status_code=422, detail=msg)


def _schema() -> dict:
    # Types do not matter for resolution, only names.
    return {t: {c: "TEXT" for c in cols} for t, cols in query_builder.TABLES.items()}


def _pin_tables(root: exp.Expression) -> None:
    """Reject non-allowlisted tables, pin the rest to our catalog.schema."""
    ctes = set()
    for cte in root.find_all(exp.CTE):
        ctes.add(cte.alias.lower())
    for scope in root.walk_scopes() if hasattr(root, "walk_scopes") else []:
        for name in getattr(scope, "cte_sources", {}) or {}:
            ctes.add(name.lower())
    for t in root.find_all(exp.Table):
        name = (t.name or "").lower()
        if not name or name in ctes:
            continue
        if name not in ALLOWED_TABLES:
            raise _fail(f"unknown table '{t.name}', expected one of {sorted(ALLOWED_TABLES)}")
        db = (t.db or "").lower()
        catalog = (t.catalog or "").lower()
        want_schema = config.TRINO_MARTS_SCHEMA.lower()
        if db and db != want_schema:
            raise _fail(f"table '{name}' must be unqualified or in schema '{want_schema}'")
        if catalog and catalog != config.TRINO_CATALOG.lower():
            raise _fail(f"table '{name}' must be unqualified or in catalog '{config.TRINO_CATALOG}'")
        t.set("db", exp.to_identifier(config.TRINO_MARTS_SCHEMA))
        t.set("catalog", exp.to_identifier(config.TRINO_CATALOG))


def _enforce_limit(root: exp.Expression) -> None:
    limit = root.args.get("limit")
    fetch = root.args.get("fetch")
    if limit is not None:
        lit = limit.find(exp.Literal)
        if lit is None:
            raise _fail("LIMIT must be a plain integer")
        try:
            n = int(lit.name)
        except ValueError:
            raise _fail("LIMIT must be a plain integer")
        if n < 1:
            raise _fail(f"limit must be 1..{config.MAX_LIMIT}")
        if n > config.MAX_LIMIT:
            limit.set("expression", exp.Literal.number(config.MAX_LIMIT))
    elif fetch is not None:
        lit = fetch.find(exp.Literal)
        if lit is None:
            raise _fail("FETCH must carry a plain integer row count")
        try:
            n = int(lit.name)
        except ValueError:
            raise _fail("FETCH must carry a plain integer row count")
        if n < 1:
            raise _fail(f"limit must be 1..{config.MAX_LIMIT}")
        if n > config.MAX_LIMIT:
            lit.replace(exp.Literal.number(config.MAX_LIMIT))
    else:
        root.set("limit", exp.Limit(expression=exp.Literal.number(config.MAX_LIMIT)))


def gate(sql: str) -> str:
    """Validate raw SQL and return the rewritten, executable Trino SQL."""
    text = (sql or "").strip().rstrip(";").strip()
    if not text:
        raise _fail("sql must be a non-empty SELECT statement")
    if len(text) > _MAX_SQL_CHARS:
        raise _fail(f"sql too long (max {_MAX_SQL_CHARS} chars)")
    try:
        statements = [s for s in sqlglot.parse(text, read="trino") if s is not None]
    except Exception as e:
        raise _fail(f"could not parse sql: {e}")
    if len(statements) != 1:
        raise _fail("send exactly one SELECT statement")
    root = statements[0]
    inner = root.expression if isinstance(root, exp.With) else root
    if not isinstance(inner, (exp.Select, exp.Union)):
        raise _fail(f"only SELECT queries are allowed, got {type(root).__name__}")
    # Keep the WITH node intact: dropping it would silently discard the CTEs.
    _pin_tables(root)
    try:
        resolved = qualify(root, schema=_schema())
    except Exception as e:
        raise _fail(f"column check failed: {e}")
    target = resolved.expression if isinstance(resolved, exp.With) else resolved
    _enforce_limit(target)
    return resolved.sql(dialect="trino")
