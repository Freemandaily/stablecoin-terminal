"""Apply SQL migrations in order, tracked in schema_migrations.

Usage: make migrate  |  uv run python scripts/migrate.py
Files: migrations/NNN_name.sql (lexical order = numeric order).
Idempotent: already-applied versions are skipped.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS = ROOT / "migrations"


def conn() -> psycopg.Connection:
    load_dotenv(ROOT / ".env")
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "stableflow"),
        password=os.getenv("PGPASSWORD", "stableflow"),
        dbname=os.getenv("PGDATABASE", "stableflow"),
        autocommit=True,  # DDL: fail loudly, one statement at a time
    )


def main() -> int:
    files = sorted(MIGRATIONS.glob("*.sql"))
    if not files:
        print("no migration files found in migrations/")
        return 1

    try:
        with conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version    TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            done = {r[0] for r in c.execute("SELECT version FROM schema_migrations")}

            applied = 0
            for f in files:
                version = f.stem
                if version in done:
                    continue
                print(f"applying {version} ...")
                c.execute(f.read_text())
                c.execute(
                    "INSERT INTO schema_migrations (version) VALUES (%s)", (version,)
                )
                applied += 1

            print(f"done: {applied} applied, {len(files) - applied} already current")
            return 0
    except psycopg.OperationalError as e:
        print(f"cannot reach postgres — is it up?  (make up)\n{e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
