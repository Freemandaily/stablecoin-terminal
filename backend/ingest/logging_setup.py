"""Job logging: console + logs/<job>.log (P1.6 requires file logs)."""

from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime

from .config import LOGS, ensure_dirs

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"


def get_logger(job: str) -> logging.Logger:
    """Logger for a job — writes to stdout and logs/<job>.log."""
    ensure_dirs()
    logger = logging.getLogger(job)
    if logger.handlers:  # idempotent setup on repeated calls
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(_FORMAT, datefmt="%Y-%m-%dT%H:%M:%SZ")
    fmt.converter = lambda *a: datetime.now(UTC).timetuple()  # type: ignore[attr-defined]

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(LOGS / f"{job}.log")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


def log_run(logger: logging.Logger, job: str, fn, *, conn=None):  # noqa: ANN001
    """Run fn() while recording the attempt in load_log (if a conn is given).

    Returns fn()'s result; failed runs are recorded with the error message.
    Expects an autocommit connection (same policy as backend/jobs/migrate.py).
    """
    cur = conn.cursor() if conn else None
    run_id = None
    if cur:
        cur.execute(
            "INSERT INTO load_log (job) VALUES (%s) RETURNING id", (job,)
        )
        run_id = cur.fetchone()[0]
    try:
        result = fn()
        if cur:
            cur.execute(
                "UPDATE load_log SET status='success', finished_at=now() WHERE id=%s",
                (run_id,),
            )
        return result
    except Exception as e:
        logger.error("%s failed: %s", job, e)
        if cur:
            cur.execute(
                "UPDATE load_log SET status='failed', finished_at=now(), detail=%s "
                "WHERE id=%s",
                (str(e)[:2000], run_id),
            )
        raise
