"""Postgres connection helper shared by ingestion jobs (autocommit, §3 rule 4)."""

from __future__ import annotations

import os

import psycopg
from dotenv import load_dotenv

from .config import ROOT


def get_conn() -> psycopg.Connection:
    load_dotenv(ROOT / ".env")
    return psycopg.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        user=os.getenv("PGUSER", "stableflow"),
        password=os.getenv("PGPASSWORD", "stableflow"),
        dbname=os.getenv("PGDATABASE", "stableflow"),
        autocommit=True,  # ingest loads: each upsert commits, job resumes after crash
    )
