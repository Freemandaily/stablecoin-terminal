"""Read-only Trino client. One short-lived connection per query."""
from trino.dbapi import connect
from trino.exceptions import TrinoQueryError

from app import config


def run_query(sql: str, params: tuple = ()):
    """Run parameterized SQL, return list of dicts. Raises RuntimeError on failure."""
    conn = connect(
        host=config.TRINO_HOST,
        port=config.TRINO_PORT,
        user=config.TRINO_USER,
        catalog=config.TRINO_CATALOG,
        schema=config.TRINO_MARTS_SCHEMA,
        http_scheme=config.TRINO_HTTP_SCHEME,
        request_timeout=config.QUERY_TIMEOUT_S,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except TrinoQueryError as e:
        raise RuntimeError(f"Trino query failed: {e}") from e
    finally:
        conn.close()
